"""Swiss national tariff law — pure functions, no HA dependencies.

Floor and cap data live in ``data/tariffs.json`` (loaded via ``tariffs_db``);
the functions here just consume rule lists and apply the law's math. Adding
a future EnV revision means adding a record to ``federal_minimum`` — no code
change.

Legal references (all in force since 1.1.2026 via the Mantelerlass / Stromgesetz):
- EnG Art. 15 Abs. 1 + EnFV Art. 15:
    Basisvergütung = BFE Referenz-Marktpreis (quartalsweise publiziert).
- EnG Art. 15 Abs. 1bis + EnV Art. 12 Abs. 1bis Bst. a/b/c/d (SR 730.01, AS 2025 138):
    Mindestvergütung floor — 6.00 / 6.20 / 180/kW / 12.00 Rp/kWh.
- StromVV Art. 4 Abs. 3 Bst. e (SR 734.71): cap mechanism. The numerical
    ceilings (10.96 / 8.20 / 7.20 / 5.40) are utility-published derivations —
    only EKZ, Groupe E, Primeo enforce them as a payment cap on producers;
    others pay above (e.g. IWB 12.95 Rp/kWh).
"""

from __future__ import annotations

from datetime import datetime

from .quarters import ZURICH
from .tariffs_db import evaluate_federal_floor, find_rule

# Default cap_rules — utility-published cost-recovery ceilings (EKZ, Groupe E,
# Primeo converge on these 4 values). Cap mechanism per StromVV Art. 4 Abs. 3
# Bst. e. Kept as a constant so tests and edge-case callers have a canonical
# default to feed into anrechenbarkeitsgrenze_rp_kwh().
DEFAULT_CAP_RULES: list[dict] = [
    {"kw_min": 0,   "kw_max": 100,  "self_consumption": True,  "cap_rp_kwh": 10.96},
    {"kw_min": 0,   "kw_max": 100,  "self_consumption": False, "cap_rp_kwh":  8.20},
    {"kw_min": 100, "kw_max": None, "self_consumption": True,  "cap_rp_kwh":  7.20},
    {"kw_min": 100, "kw_max": None, "self_consumption": False, "cap_rp_kwh":  5.40},
]


def mindestverguetung_rp_kwh(
    rules: list[dict], kw: float, eigenverbrauch: bool
) -> float | None:
    """Federal Mindestvergütung floor in Rp/kWh.

    Source: EnV Art. 12 Abs. 1bis Bst. a/b/c/d (SR 730.01, AS 2025 138,
    in force since 1.1.2026). Returns None for ≥150 kW (no federal floor),
    6.00 for ≤30 kW, 180/kW (1.20–6.00) for 30–<150 kW mit Eigenverbrauch,
    6.20 for 30–<150 kW ohne Eigenverbrauch.
    """
    rule = find_rule(rules, kw, eigenverbrauch)
    return evaluate_federal_floor(rule, kw) if rule is not None else None


def anrechenbarkeitsgrenze_rp_kwh(
    cap_rules: list[dict], kw: float, eigenverbrauch: bool
) -> float | None:
    """Cost-recovery ceiling (Anrechenbarkeitsgrenze) in Rp/kWh.

    Cap mechanism per StromVV Art. 4 Abs. 3 Bst. e (SR 734.71). The numerical
    values come from utility-published derivations (EKZ, Groupe E, Primeo);
    not all are codified in federal text. Returns None when no cap rule
    covers (kw, ev).
    """
    rule = find_rule(cap_rules, kw, eigenverbrauch)
    return float(rule["cap_rp_kwh"]) if rule is not None else None


def effective_rp_kwh_breakdown(
    base_input_rp_kwh: float,
    hkn_rp_kwh: float = 0.0,
    *,
    federal_floor_rp_kwh: float | None,
    cap_rp_kwh: float | None,
) -> tuple[float, float, float]:
    """Decomposed effective rate: ``(rate, base_after_floor, applied_hkn)``.

    Same math as ``effective_rp_kwh`` but exposes the components so callers
    can show the user where the total came from. Invariant:
    ``rate == base_after_floor + applied_hkn``.

    ``federal_floor_rp_kwh`` is the *effective* floor that the caller has
    already resolved — typically ``max(federal_floor, utility_floor)``.
    The name is historical; this function does not distinguish the
    floor's source.

    Schema 1.5.0 (v0.22.0) — cap activation is signaled solely by
    ``cap_rp_kwh is not None``. The legacy ``cap_mode`` boolean was
    dropped; resolvers now derive the cap from a non-empty ``cap_rules``
    array at the rate-window level.
    """
    floor = federal_floor_rp_kwh or 0.0
    base = max(base_input_rp_kwh, floor)

    if cap_rp_kwh is None:
        return base + hkn_rp_kwh, base, hkn_rp_kwh
    if base >= cap_rp_kwh:
        return base, base, 0.0
    applied_hkn = min(hkn_rp_kwh, cap_rp_kwh - base)
    return base + applied_hkn, base, applied_hkn


def effective_rp_kwh(
    base_input_rp_kwh: float,
    hkn_rp_kwh: float = 0.0,
    *,
    federal_floor_rp_kwh: float | None,
    cap_rp_kwh: float | None,
) -> float:
    """Effective Rückliefervergütung in Rp/kWh.

    Caller is expected to have already resolved ``federal_floor_rp_kwh``
    (from ``mindestverguetung_rp_kwh``) and ``cap_rp_kwh`` (from
    ``anrechenbarkeitsgrenze_rp_kwh`` against the utility's cap_rules).

    The federal Mindestvergütung floor (EnV Art. 12 Abs. 1bis) is always
    applied to the base. Whether the cost-recovery ceiling (per StromVV Art. 4
    Abs. 3 Bst. e) acts as a payment cap is a per-utility commercial choice —
    most Swiss utilities pay base + HKN additively without enforcing the cap;
    only EKZ, Groupe E, and Primeo apply it strictly per their published
    2026 terms (those ship with a non-empty ``cap_rules`` array in
    ``tariffs.json``).

    When the cap is active (``cap_rp_kwh is not None``), the EKZ-style
    two-clause cap rule applies:
    - If base alone already meets/exceeds the cap → HKN forfeited entirely.
    - Otherwise → HKN reduced just enough to keep base + HKN ≤ cap.
    """
    rate, _, _ = effective_rp_kwh_breakdown(
        base_input_rp_kwh,
        hkn_rp_kwh,
        federal_floor_rp_kwh=federal_floor_rp_kwh,
        cap_rp_kwh=cap_rp_kwh,
    )
    return rate


def classify_ht(hour_utc: datetime, ht_window: dict | None) -> bool:
    """True iff ``hour_utc`` falls inside the utility's HT (Hochtarif) window.

    ``ht_window`` shape (per power_tier in tariffs.json):

        {"mofr": [start_h, end_h] | None,
         "sa":   [start_h, end_h] | None,
         "su":   [start_h, end_h] | None}

    None for any day type means all-NT for that day. Hour windows are
    half-open: ``start_h <= local_hour < end_h``. Day-of-week and hour are
    determined in Zurich local time (DST-safe via zoneinfo) so a window of
    07:00–20:00 means 07:00–20:00 wall-clock all year, regardless of UTC
    offset.

    ``ht_window=None`` (or empty) returns False — the caller should not be
    invoking ``classify_ht`` for a utility without an HT/NT structure.
    """
    if not ht_window:
        return False
    local = hour_utc.astimezone(ZURICH)
    weekday = local.weekday()  # 0=Mon, 6=Sun
    if weekday < 5:
        window = ht_window.get("mofr")
    elif weekday == 5:
        window = ht_window.get("sa")
    else:
        window = ht_window.get("su")
    if not window:
        return False
    start_h, end_h = window
    return start_h <= local.hour < end_h


def classify_season(
    hour_utc: datetime,
    summer_months: list[int],
    winter_months: list[int],
) -> str:
    """Return ``"summer"`` or ``"winter"`` for ``hour_utc`` per the
    utility's seasonal month split.

    Month is determined in Zurich local time (DST-safe via zoneinfo) so
    the answer matches what a Swiss utility would invoice for that wall-
    clock hour. Raises ValueError if the hour's month appears in neither
    list — the caller is responsible for ensuring summer_months and
    winter_months together cover all 12 months.
    """
    local = hour_utc.astimezone(ZURICH)
    if local.month in summer_months:
        return "summer"
    if local.month in winter_months:
        return "winter"
    raise ValueError(
        f"Month {local.month} is in neither summer_months nor "
        f"winter_months ({summer_months} / {winter_months})"
    )


def chf_per_mwh_to_rp_per_kwh(chf_per_mwh: float) -> float:
    """BFE publishes in CHF/MWh; HA dashboard thinks in Rp/kWh."""
    return chf_per_mwh / 10.0


def rp_per_kwh_to_chf_per_kwh(rp_per_kwh: float) -> float:
    return rp_per_kwh / 100.0
