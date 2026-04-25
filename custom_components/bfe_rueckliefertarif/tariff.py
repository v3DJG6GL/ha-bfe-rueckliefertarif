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
- StromVV Art. 4 Abs. 3 Bst. e i.V.m. EnG Art. 15 Abs. 1 (SR 734.71, AS 2025 139):
    Anrechenbarkeitsgrenze — cost-recovery ceiling on what utilities may charge
    captive customers via Grundversorgungstarif. Whether a utility *enforces*
    this ceiling as a payment cap on producers is a per-utility commercial
    choice (only EKZ, Groupe E, Primeo do — IWB pays 12.95 Rp/kWh, well above
    the 10.96 small-plant ceiling).
"""

from __future__ import annotations

from .tariffs_db import evaluate_federal_floor, find_rule

# Default cap_rules — StromVV Art. 4 Abs. 3 Bst. e cost-recovery ceiling per
# EKZ's published derivation (the reference Gestehungskosten are nationally
# uniform, so the cap-enforcing utilities — EKZ, Groupe E, Primeo — converge
# on these numbers). Kept as a constant so tests and edge-case callers have a
# canonical default to feed into anrechenbarkeitsgrenze_rp_kwh().
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
    """Anrechenbarkeitsgrenze cost-recovery ceiling in Rp/kWh.

    Legal source: StromVV Art. 4 Abs. 3 Bst. e (SR 734.71, AS 2025 139,
    in force since 1.1.2026). The article mandates the *formula* (max
    chargeback = Gestehungskosten of a reference plant minus subsidies);
    per-utility cap_rules carry the values that utility's published
    derivation produces. Returns None when no cap rule covers (kw, ev).
    """
    rule = find_rule(cap_rules, kw, eigenverbrauch)
    return float(rule["cap_rp_kwh"]) if rule is not None else None


def effective_rp_kwh(
    base_input_rp_kwh: float,
    hkn_rp_kwh: float = 0.0,
    *,
    federal_floor_rp_kwh: float | None,
    cap_rp_kwh: float | None,
    cap_mode: bool = False,
) -> float:
    """Effective Rückliefervergütung in Rp/kWh.

    Caller is expected to have already resolved ``federal_floor_rp_kwh``
    (from ``mindestverguetung_rp_kwh``) and ``cap_rp_kwh`` (from
    ``anrechenbarkeitsgrenze_rp_kwh`` against the utility's cap_rules).

    The federal Mindestvergütung floor (EnV Art. 12 Abs. 1bis) is always
    applied to the base. Whether the Anrechenbarkeitsgrenze (StromVV Art. 4
    Abs. 3 Bst. e) acts as a payment cap is a per-utility commercial choice —
    most Swiss utilities pay base + HKN additively without enforcing the cap;
    only EKZ, Groupe E, and Primeo apply it strictly per their published
    2026 terms (those ship with ``cap_mode=true`` in tariffs.json).

    When ``cap_mode`` is True, the EKZ-style two-clause cap rule applies:
    - If base alone already meets/exceeds the cap → HKN forfeited entirely.
    - Otherwise → HKN reduced just enough to keep base + HKN ≤ cap.
    """
    floor = federal_floor_rp_kwh or 0.0
    base = max(base_input_rp_kwh, floor)

    if not cap_mode or cap_rp_kwh is None:
        return base + hkn_rp_kwh
    if base >= cap_rp_kwh:
        return base
    return min(base + hkn_rp_kwh, cap_rp_kwh)


def chf_per_mwh_to_rp_per_kwh(chf_per_mwh: float) -> float:
    """BFE publishes in CHF/MWh; HA dashboard thinks in Rp/kWh."""
    return chf_per_mwh / 10.0


def rp_per_kwh_to_chf_per_kwh(rp_per_kwh: float) -> float:
    return rp_per_kwh / 100.0
