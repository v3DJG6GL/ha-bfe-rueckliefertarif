"""Retro-import orchestration: read export LTS → compute compensation → write LTS.

Split into two layers:
- `compute_quarter_plan(...)` — pure-function: takes export kWh data + BFE prices +
  tariff config, returns the list of (hour_utc, sum_chf) records to write. Testable
  without HA.
- `apply_plan(...)` — HA-facing: reads current anchors from recorder, applies the
  plan via `async_import_statistics`, handles transition-spike shift.

The pure layer guarantees: for any billing_mode, the sum of compensation over a
quarter equals Q_total_kWh × Q_quarterly_rate_CHF (invoice match).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from .bfe import BfePrice, PriceNotYetPublishedError
from .const import ABRECHNUNGS_RHYTHMUS_MONAT
from .quarters import Month, Quarter, hours_in_range, month_bounds_utc, quarter_bounds_utc
from .tariff import (
    chf_per_mwh_to_rp_per_kwh,
    effective_rp_kwh,
    effective_rp_kwh_breakdown,
    rp_per_kwh_to_chf_per_kwh,
)
from .tariffs_db import evaluate_when

if TYPE_CHECKING:
    from .tariffs_db import ResolvedTariff


@dataclass(frozen=True)
class TariffConfig:
    """Resolved tariff inputs for one quarter of math.

    Wraps a ``ResolvedTariff`` (utility-published values from tariffs.json)
    plus the user's personal inputs (kW, Eigenverbrauch yes/no, HKN opt-in
    yes/no). The ``hkn_rp_kwh_resolved`` value is the JSON's HKN multiplied
    by 0 or 1 depending on whether the user opted in. ``user_inputs``
    carries declared per-utility user toggles used by the per-hour resolver
    to evaluate hkn_cases / bonuses[].when.
    """

    eigenverbrauch_aktiviert: bool
    installierte_leistung_kwp: float
    hkn_aktiviert: bool
    hkn_rp_kwh_resolved: float          # JSON's HKN if opted in, else 0.0
    resolved: ResolvedTariff
    user_inputs: dict = field(default_factory=dict)


@dataclass(frozen=True)
class HourRecord:
    start: datetime          # UTC, hour-aligned
    kwh: float               # export kWh in this hour
    rate_rp_kwh: float       # effective tariff applied (= base + hkn + bonus)
    compensation_chf: float  # kwh × rate / 100
    base_rp_kwh: float       # base after federal floor (and cap when binding)
    hkn_rp_kwh: float        # HKN bonus actually applied (0 if not opted in or cap-forfeited)
    # Stable bucket key when a quarter spans multiple (config × season)
    # segments. ``None`` for legacy single-segment imports.
    seg_id: str | None = None
    # Bonuses applied per hour (additive_rp_kwh + multiplier_pct deltas).
    # Layered on top of the cap-binding (base + hkn); bonuses do not
    # participate in cap-forfeit. 0.0 when the rate window has no bonuses
    # or none match this hour.
    bonus_rp_kwh: float = 0.0


@dataclass(frozen=True)
class QuarterPlan:
    quarter: Quarter
    anchor_sum_chf: float           # compensation sum at quarter_start - 1h
    records: list[HourRecord]
    # new_sum[h] = anchor + cumsum(record.compensation_chf)
    # Final new_sum accessible via records[-1] cumulative, but stored here for clarity:
    final_sum_chf: float
    # Delta to shift post-quarter LTS records by (transition-spike fix).
    # Caller applies this to all existing compensation LTS at start >= quarter_end.
    post_quarter_delta_chf: float


def _effective_floor(rt: ResolvedTariff) -> float | None:
    """``max(federal_floor, utility_floor)`` — whichever binds first.

    Utility-level ``price_floor_rp_kwh`` (per StromVV Art. 4 Abs. 3 Bst. e
    derivations / supplier T&Cs) is treated equivalently to the federal
    Mindestvergütung: both clamp the base from below, the higher one wins.
    Returns None only if both are null.
    """
    fed = rt.federal_floor_rp_kwh
    utl = rt.price_floor_rp_kwh
    if fed is None and utl is None:
        return None
    return max(fed or 0.0, utl or 0.0)


def _apply_floor_cap_hkn(base_rp_kwh: float, cfg: TariffConfig) -> float:
    """Apply effective floor + HKN + Anrechenbarkeitsgrenze cap to a base rate."""
    rt = cfg.resolved
    return effective_rp_kwh(
        base_rp_kwh,
        cfg.hkn_rp_kwh_resolved,
        federal_floor_rp_kwh=_effective_floor(rt),
        cap_rp_kwh=rt.cap_rp_kwh,
    )


def _apply_floor_cap_hkn_breakdown(
    base_rp_kwh: float, cfg: TariffConfig
) -> tuple[float, float, float]:
    """Decomposed variant: returns ``(rate, base_after_floor, applied_hkn)``.

    No-hour path; used by the M3 closure derivation where the per-hour
    bonuses are layered on top separately.
    """
    rt = cfg.resolved
    return effective_rp_kwh_breakdown(
        base_rp_kwh,
        cfg.hkn_rp_kwh_resolved,
        federal_floor_rp_kwh=_effective_floor(rt),
        cap_rp_kwh=rt.cap_rp_kwh,
    )


def _resolve_hkn_for_hour(cfg: TariffConfig, season: str | None) -> float:
    """Per-hour HKN before cap. Honors ``hkn_aktiviert``, ``hkn_structure``,
    and ``hkn_cases`` first-match-wins. Falls through to the static
    ``rt.hkn_rp_kwh`` when no case matches.
    """
    if not cfg.hkn_aktiviert:
        return 0.0
    rt = cfg.resolved
    if rt.hkn_structure != "additive_optin":
        # Bundled / none — no opt-in HKN to apply on top.
        return 0.0
    if rt.hkn_cases:
        for case in rt.hkn_cases:
            if evaluate_when(
                case["when"], season=season, user_inputs=cfg.user_inputs
            ):
                return float(case["rp_kwh"])
    return rt.hkn_rp_kwh


def _resolve_bonuses_for_hour(
    cfg: TariffConfig,
    season: str | None,
    base_after_floor: float,
    applied_hkn: float,
) -> float:
    """Sum the per-hour bonus rp/kWh contribution.

    Applied additively on top of the cap-binding (base + hkn). Bonuses are
    utility-discretionary extras outside BFE settlement, so cap-forfeit
    semantics do not apply to them.

    Iterates ``rt.bonuses`` in declared order. For each:
    - ``applies_when="opt_in"`` with no ``when`` clause: skipped (no toggle
      to gate it on; the data repo should have used a ``user_inputs`` key).
    - ``when`` clause present: evaluated against (season, user_inputs);
      skipped on no-match.
    - ``kind="additive_rp_kwh"``: adds ``rate_rp_kwh`` to the accumulator.
    - ``kind="multiplier_pct"``: re-scales the running rate
      (``base + hkn + accumulator``) by ``multiplier_pct/100``; the delta
      is added to the accumulator. Order in the schema array matters.
    """
    total, _ = _resolve_bonuses_for_hour_detailed(
        cfg, season, base_after_floor, applied_hkn
    )
    return total


def _resolve_bonuses_for_hour_detailed(
    cfg: TariffConfig,
    season: str | None,
    base_after_floor: float,
    applied_hkn: float,
) -> tuple[float, list[dict]]:
    """Like ``_resolve_bonuses_for_hour`` but also returns per-bonus contribution
    detail. The detail list contains only bonuses that actually contributed
    (skipped opt-in / when-clause-mismatched bonuses are omitted).
    """
    rt = cfg.resolved
    # Iterate rate-level bonuses first, tier-level second; multiplier_pct
    # stacking compounds in that order (rate's +5% applied first, tier's
    # +3% multiplies the rate-modified base → +8.15% combined).
    all_bonuses = (rt.bonuses or ()) + (rt.tier_bonuses or ())
    if not all_bonuses:
        return 0.0, []
    accumulator = 0.0
    detail: list[dict] = []
    for b in all_bonuses:
        when_clause = b.get("when")
        applies_when = b.get("applies_when", "always")
        if applies_when == "opt_in" and not when_clause:
            continue
        if when_clause and not evaluate_when(
            when_clause, season=season, user_inputs=cfg.user_inputs
        ):
            continue
        kind = b.get("kind", "additive_rp_kwh")
        contribution = 0.0
        if kind == "additive_rp_kwh":
            contribution = float(b.get("rate_rp_kwh", 0.0))
        elif kind == "multiplier_pct":
            mp = float(b.get("multiplier_pct", 100.0))
            current = base_after_floor + applied_hkn + accumulator
            contribution = current * (mp / 100.0 - 1.0)
        accumulator += contribution
        detail.append(
            {
                "name": b.get("name", "—"),
                "kind": kind,
                "rp_kwh_contribution": round(contribution, 4),
            }
        )
    return accumulator, detail


def _apply_floor_cap_hkn_bonus_breakdown(
    base_rp_kwh: float, cfg: TariffConfig, season: str | None
) -> tuple[float, float, float, float]:
    """Per-hour 4-tuple: ``(rate, base_after_floor, applied_hkn, applied_bonus)``.

    ``rate == base_after_floor + applied_hkn + applied_bonus``. The cap
    binds only on (base + hkn); bonuses sit on top.
    """
    rt = cfg.resolved
    per_hkn = _resolve_hkn_for_hour(cfg, season)
    rate_billing, base_after, applied_hkn = effective_rp_kwh_breakdown(
        base_rp_kwh,
        per_hkn,
        federal_floor_rp_kwh=_effective_floor(rt),
        cap_rp_kwh=rt.cap_rp_kwh,
    )
    applied_bonus = _resolve_bonuses_for_hour(
        cfg, season, base_after, applied_hkn
    )
    return rate_billing + applied_bonus, base_after, applied_hkn, applied_bonus


def _season_at(rt: ResolvedTariff, hour_utc: datetime) -> str | None:
    """Hour's season per ``rt.seasonal``, or ``None`` when no seasonal block.

    For ``base_model == "fixed_seasonal"`` the resolver writes the tier's
    seasonal block into ``rt.seasonal``, so the same lookup transparently
    yields the tier-level calendar for those tiers.
    """
    if rt.seasonal is None:
        return None
    from .tariff import classify_season
    return classify_season(
        hour_utc,
        rt.seasonal["summer_months"],
        rt.seasonal["winter_months"],
    )


def _effective_rate(
    cfg: TariffConfig, reference_rp_kwh: float
) -> float:
    """Single-rate computation for one BFE reference price.

    Selects the right base depending on the utility's published `base_model`:
    - rmp_quartal/rmp_monat → use the BFE reference price the caller supplied
    - fixed_flat → use the JSON's `fixed_rp_kwh`
    - fixed_ht_nt → conservative HT-rate fallback for callers without hour
      context. Hour-aware callers should route through ``_effective_rate_at_hour``
      to pick HT or NT per the utility's ``ht_window``.

    Raises if the rate has a seasonal overlay — there is no sensible
    period-flat default for summer/winter splits; the caller must use
    ``_effective_rate_at_hour`` to pick the season per hour.
    """
    rt = cfg.resolved
    if rt.seasonal is not None:
        raise ValueError(
            f"{rt.utility_key}: seasonal evaluation requires hour context — "
            f"call _effective_rate_at_hour instead"
        )
    if rt.base_model in ("rmp_quartal", "rmp_monat"):
        base = reference_rp_kwh
    elif rt.base_model == "fixed_flat":
        if rt.fixed_rp_kwh is None:
            raise ValueError(f"{rt.utility_key}: fixed_flat requires fixed_rp_kwh")
        base = rt.fixed_rp_kwh
    elif rt.base_model == "fixed_ht_nt":
        base = rt.fixed_ht_rp_kwh if rt.fixed_ht_rp_kwh is not None else 0.0
    else:
        raise ValueError(f"Unknown base_model {rt.base_model!r}")
    return _apply_floor_cap_hkn(base, cfg)


def _effective_rate_breakdown(
    cfg: TariffConfig, reference_rp_kwh: float
) -> tuple[float, float, float]:
    """Decomposed variant of ``_effective_rate``: ``(rate, base, applied_hkn)``."""
    rt = cfg.resolved
    if rt.seasonal is not None:
        raise ValueError(
            f"{rt.utility_key}: seasonal evaluation requires hour context — "
            f"call _effective_rate_breakdown_at_hour instead"
        )
    if rt.base_model in ("rmp_quartal", "rmp_monat"):
        base = reference_rp_kwh
    elif rt.base_model == "fixed_flat":
        if rt.fixed_rp_kwh is None:
            raise ValueError(f"{rt.utility_key}: fixed_flat requires fixed_rp_kwh")
        base = rt.fixed_rp_kwh
    elif rt.base_model == "fixed_ht_nt":
        base = rt.fixed_ht_rp_kwh if rt.fixed_ht_rp_kwh is not None else 0.0
    else:
        raise ValueError(f"Unknown base_model {rt.base_model!r}")
    return _apply_floor_cap_hkn_breakdown(base, cfg)


def _effective_rate_at_hour(
    cfg: TariffConfig, reference_rp_kwh: float, hour_utc: datetime
) -> float:
    """Hour-aware effective rate.

    For ``fixed_ht_nt`` utilities, classifies ``hour_utc`` via the
    utility's ``ht_window`` and applies HT or NT. For ``fixed_flat`` or
    ``fixed_ht_nt`` utilities with a ``seasonal`` overlay, also picks
    the summer or winter rate by ``hour_utc``'s Zurich-local month. For
    all other cases, delegates to ``_effective_rate`` (hour ignored).

    Forward-compatible: the future hourly Day-Ahead spot work (gated on
    Bundesrat adoption of the EnV Art. 12 revision) slots in as a single
    additional ``elif`` branch — no other changes needed.
    """
    from .tariff import classify_ht, classify_season  # local import: avoid cycle

    rt = cfg.resolved
    season: str | None = None
    if rt.seasonal is not None:
        season = classify_season(
            hour_utc,
            rt.seasonal["summer_months"],
            rt.seasonal["winter_months"],
        )

    if rt.base_model == "fixed_ht_nt":
        is_ht = classify_ht(hour_utc, rt.ht_window)
        if season is not None:
            key = f"{season}_{'ht' if is_ht else 'nt'}_rp_kwh"
            base = rt.seasonal.get(key)
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_ht_nt × seasonal requires "
                    f"all 4 rates (missing {key})"
                )
        else:
            base = rt.fixed_ht_rp_kwh if is_ht else rt.fixed_nt_rp_kwh
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_ht_nt requires both "
                    f"fixed_ht_rp_kwh and fixed_nt_rp_kwh"
                )
        return _apply_floor_cap_hkn(base, cfg)

    if rt.base_model == "fixed_flat" and season is not None:
        key = f"{season}_rp_kwh"
        base = rt.seasonal.get(key)
        if base is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_flat × seasonal requires both "
                f"summer_rp_kwh and winter_rp_kwh (missing {key})"
            )
        return _apply_floor_cap_hkn(base, cfg)

    if rt.base_model == "fixed_seasonal":
        if season is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_seasonal requires a tier-level "
                f"seasonal block with summer_months/winter_months"
            )
        key = f"{season}_rp_kwh"
        base = (rt.seasonal or {}).get(key)
        if base is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_seasonal × {season} missing {key}"
            )
        return _apply_floor_cap_hkn(base, cfg)

    return _effective_rate(cfg, reference_rp_kwh)


def _effective_rate_breakdown_at_hour(
    cfg: TariffConfig, reference_rp_kwh: float, hour_utc: datetime
) -> tuple[float, float, float, float]:
    """4-tuple ``(rate, base, applied_hkn, applied_bonus)`` for one hour.

    Per-hour ``hkn_cases`` resolution, conditional ``bonuses[].when``
    evaluation, and ``bonuses[].kind`` math (additive / multiplier) all
    happen here. ``rate == base + applied_hkn + applied_bonus``.
    """
    from .tariff import classify_ht

    rt = cfg.resolved
    season = _season_at(rt, hour_utc)

    if rt.base_model == "fixed_ht_nt":
        is_ht = classify_ht(hour_utc, rt.ht_window)
        # Classify-only seasonal blocks (months but no rate keys) don't
        # override the per-season HT/NT rates; fall back to the
        # unconditional fixed_ht_rp_kwh / fixed_nt_rp_kwh.
        seasonal_rate_key = (
            f"{season}_{'ht' if is_ht else 'nt'}_rp_kwh" if season else None
        )
        if season is not None and seasonal_rate_key in (rt.seasonal or {}):
            base = rt.seasonal[seasonal_rate_key]
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_ht_nt × seasonal requires "
                    f"all 4 rates (missing {seasonal_rate_key})"
                )
        else:
            base = rt.fixed_ht_rp_kwh if is_ht else rt.fixed_nt_rp_kwh
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_ht_nt requires both "
                    f"fixed_ht_rp_kwh and fixed_nt_rp_kwh"
                )
    elif rt.base_model == "fixed_flat":
        # Classify-only seasonal blocks fall back to fixed_rp_kwh.
        seasonal_rate_key = f"{season}_rp_kwh" if season else None
        if season is not None and seasonal_rate_key in (rt.seasonal or {}):
            base = rt.seasonal[seasonal_rate_key]
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_flat × seasonal requires both "
                    f"summer_rp_kwh and winter_rp_kwh (missing {seasonal_rate_key})"
                )
        else:
            if rt.fixed_rp_kwh is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_flat requires fixed_rp_kwh"
                )
            base = rt.fixed_rp_kwh
    elif rt.base_model == "fixed_seasonal":
        if season is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_seasonal requires a tier-level "
                f"seasonal block with summer_months/winter_months"
            )
        key = f"{season}_rp_kwh"
        base = (rt.seasonal or {}).get(key)
        if base is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_seasonal × {season} missing {key}"
            )
    elif rt.base_model in ("rmp_quartal", "rmp_monat"):
        base = reference_rp_kwh
    else:
        raise ValueError(f"Unknown base_model {rt.base_model!r}")

    return _apply_floor_cap_hkn_bonus_breakdown(base, cfg, season)


def _resolve_base_at_hour(
    cfg: TariffConfig, reference_rp_kwh: float, hour_utc: datetime
) -> tuple[float, str, str | None, bool | None]:
    """Resolve raw base rate at one hour. Returns ``(base, label, season, is_ht)``.

    - ``base``: pre-floor, pre-HKN, pre-bonus base rate in Rp/kWh
    - ``label``: source descriptor (``"fixed_flat"`` / ``"fixed_flat_summer"`` /
      ``"fixed_ht_nt_ht"`` / ``"rmp_quartal"`` / etc.)
    - ``season``: ``"summer"`` / ``"winter"`` / ``None`` (no overlay)
    - ``is_ht``: True/False for fixed_ht_nt; ``None`` otherwise

    Mirrors the base-resolution branches of
    ``_effective_rate_breakdown_at_hour`` but exposes the metadata needed
    to render a full breakdown dict (live sensor + analysis service).
    """
    from .tariff import classify_ht

    rt = cfg.resolved
    season = _season_at(rt, hour_utc)
    is_ht: bool | None = None

    if rt.base_model == "fixed_ht_nt":
        is_ht = classify_ht(hour_utc, rt.ht_window)
        seasonal_rate_key = (
            f"{season}_{'ht' if is_ht else 'nt'}_rp_kwh" if season else None
        )
        if season is not None and seasonal_rate_key in (rt.seasonal or {}):
            base = rt.seasonal[seasonal_rate_key]
            label = f"fixed_ht_nt_{season}_{'ht' if is_ht else 'nt'}"
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_ht_nt × seasonal requires "
                    f"all 4 rates (missing {seasonal_rate_key})"
                )
        else:
            base = rt.fixed_ht_rp_kwh if is_ht else rt.fixed_nt_rp_kwh
            label = f"fixed_ht_nt_{'ht' if is_ht else 'nt'}"
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_ht_nt requires both "
                    f"fixed_ht_rp_kwh and fixed_nt_rp_kwh"
                )
    elif rt.base_model == "fixed_flat":
        seasonal_rate_key = f"{season}_rp_kwh" if season else None
        if season is not None and seasonal_rate_key in (rt.seasonal or {}):
            base = rt.seasonal[seasonal_rate_key]
            label = f"fixed_flat_{season}"
            if base is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_flat × seasonal requires both "
                    f"summer_rp_kwh and winter_rp_kwh (missing {seasonal_rate_key})"
                )
        else:
            if rt.fixed_rp_kwh is None:
                raise ValueError(
                    f"{rt.utility_key}: fixed_flat requires fixed_rp_kwh"
                )
            base = rt.fixed_rp_kwh
            label = "fixed_flat"
    elif rt.base_model == "fixed_seasonal":
        if season is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_seasonal requires a tier-level "
                f"seasonal block with summer_months/winter_months"
            )
        key = f"{season}_rp_kwh"
        base = (rt.seasonal or {}).get(key)
        if base is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_seasonal × {season} missing {key}"
            )
        label = f"fixed_seasonal_{season}"
    elif rt.base_model in ("rmp_quartal", "rmp_monat"):
        base = reference_rp_kwh
        label = rt.base_model
    else:
        raise ValueError(f"Unknown base_model {rt.base_model!r}")

    return base, label, season, is_ht


def compute_breakdown_at(
    cfg: TariffConfig, reference_rp_kwh: float, hour_utc: datetime
) -> dict:
    """Full tariff breakdown at one hour — superset of the per-hour 4-tuple.

    Returns the dict shape consumed by:
    - ``coordinator._tariff_breakdown`` (live sensor, ``hour_utc=now``)
    - ``services.get_breakdown`` (analysis service, arbitrary hour in the
      requested period — typically quarter midpoint)

    Includes all classic breakdown keys (utility, tariff_source, floor_label,
    base_input_rp_kwh, effective_rp_kwh, etc.) plus applied-factor breakdown:
    ``season_now``, ``ht_nt_now``, ``applied_bonus_rp_kwh``,
    ``bonuses_applied`` (per-bonus contribution detail), ``bonuses_advertised``
    (informational list of all rate-window bonuses regardless of opt-in).
    """
    rt = cfg.resolved
    base_input, base_label, season, is_ht = _resolve_base_at_hour(
        cfg, reference_rp_kwh, hour_utc
    )

    floor = _effective_floor(rt)
    floor_value = floor if floor is not None else 0.0
    # Cap activation = `cap_rp_kwh` set (resolver derived from non-empty
    # `cap_rules` array).
    cap = rt.cap_rp_kwh

    per_hkn = _resolve_hkn_for_hour(cfg, season)
    rate, base_after_floor, applied_hkn, applied_bonus = (
        _apply_floor_cap_hkn_bonus_breakdown(base_input, cfg, season)
    )
    _bonus_total, bonuses_applied = _resolve_bonuses_for_hour_detailed(
        cfg, season, base_after_floor, applied_hkn
    )

    # Display-side: advertise rate-level + tier-level bonuses together.
    # Iteration order matches `_resolve_bonuses_for_hour_detailed` so
    # per-bonus contributions in `bonuses_applied` align by index.
    bonuses_advertised: list[dict] = []
    for b in (rt.bonuses or ()) + (rt.tier_bonuses or ()):
        kind = b.get("kind", "additive_rp_kwh")
        if kind == "additive_rp_kwh":
            value = b.get("rate_rp_kwh")
        elif kind == "multiplier_pct":
            value = b.get("multiplier_pct")
        else:
            value = None
        bonuses_advertised.append(
            {
                "name": b.get("name", "—"),
                "kind": kind,
                "value": value,
                "applies_when": b.get("applies_when", "always"),
            }
        )

    theoretical_total = base_after_floor + per_hkn
    if cap is not None:
        obergrenze_aktiv = theoretical_total > cap
        hkn_gekuerzt_auf = (
            max(0.0, cap - base_after_floor)
            if obergrenze_aktiv and base_after_floor < cap
            else None
        )
    else:
        obergrenze_aktiv = False
        hkn_gekuerzt_auf = None

    tariff_source = (
        f"tariffs.json v{rt.tariffs_json_version} {rt.utility_key} "
        f"@ {rt.valid_from} ({rt.tariffs_json_source})"
    )
    return {
        "utility": rt.utility_key,
        "tariff_source": tariff_source,
        "floor_label": rt.federal_floor_label,
        "eigenverbrauch_aktiviert": cfg.eigenverbrauch_aktiviert,
        "hkn_aktiviert": cfg.hkn_aktiviert,
        "base_model": rt.base_model,
        "base_input_rp_kwh": round(base_input, 4),
        "base_source": base_label,
        "minimalverguetung_rp_kwh": round(floor_value, 4),
        "base_after_floor_rp_kwh": round(base_after_floor, 4),
        "hkn_verguetung_rp_kwh": round(per_hkn, 4),
        "theoretical_total_rp_kwh": round(theoretical_total, 4),
        "anrechenbarkeitsgrenze_rp_kwh": (
            round(cap, 4) if cap is not None else None
        ),
        "effective_rp_kwh": round(rate, 4),
        "effective_chf_kwh": round(rate / 100.0, 6),
        "obergrenze_aktiv": obergrenze_aktiv,
        "hkn_gekuerzt_auf": (
            round(hkn_gekuerzt_auf, 4)
            if hkn_gekuerzt_auf is not None
            else None
        ),
        # Additional applied-factor breakdown
        "season_now": season,
        "ht_nt_now": ("ht" if is_ht else "nt") if is_ht is not None else None,
        "applied_bonus_rp_kwh": round(applied_bonus, 4),
        "bonuses_applied": bonuses_applied,
        "bonuses_advertised": bonuses_advertised,
    }


def _rate_rp_kwh_at_hour(
    hour_utc: datetime,
    q: Quarter,
    quarterly_price: BfePrice,
    monthly_prices: dict[Month, BfePrice] | None,
    cfg: TariffConfig,
    billing_mode: str,
    kwh_per_month: dict[Month, float] | None,
) -> tuple[float, float, float, float]:
    """Effective Rp/kWh for a single hour: ``(rate, base, applied_hkn, applied_bonus)``.

    Quarterly mode: flat rate = tariff(quarterly_price) for every hour.
    Monthly mode: M1/M2 use monthly prices directly; M3 uses a derived rate
    such that the quarter total equals Q_kWh × Q_rate exactly. The closure
    invariant operates on ``rate_billing == base + applied_hkn`` — bonuses
    sit on top per-hour and are computed separately, since they are
    utility-discretionary extras outside BFE settlement.
    """
    q_rp = chf_per_mwh_to_rp_per_kwh(quarterly_price.chf_per_mwh)

    # Fixed-rate utilities don't track BFE monthly prices — they're either
    # truly flat (fixed_flat) or per-hour HT/NT (fixed_ht_nt, switched on
    # local time inside _effective_rate_at_hour). Only RMP-based base_models
    # need the M1/M2/M3 monthly-price decomposition.
    is_fixed_base = cfg.resolved.base_model in ("fixed_flat", "fixed_ht_nt", "fixed_seasonal")
    if is_fixed_base or billing_mode != ABRECHNUNGS_RHYTHMUS_MONAT:
        return _effective_rate_breakdown_at_hour(cfg, q_rp, hour_utc)

    assert monthly_prices is not None
    assert kwh_per_month is not None
    m1, m2, m3 = q.months()
    # Determine which month this hour belongs to (in local Zurich time).
    for m in (m1, m2, m3):
        ms, me = month_bounds_utc(m)
        if ms <= hour_utc < me:
            this_month = m
            break
    else:
        raise ValueError(f"Hour {hour_utc} not in any month of quarter {q}")

    season = _season_at(cfg.resolved, hour_utc)

    if this_month in (m1, m2):
        if this_month not in monthly_prices:
            raise PriceNotYetPublishedError(
                f"Monthly PV price for {this_month} not published"
            )
        m_rp = chf_per_mwh_to_rp_per_kwh(monthly_prices[this_month].chf_per_mwh)
        rate_b, base, hkn = _effective_rate_breakdown(cfg, m_rp)
        bonus = _resolve_bonuses_for_hour(cfg, season, base, hkn)
        return rate_b + bonus, base, hkn, bonus

    # Month 3: derive rate so quarter (base+hkn) sum matches exactly.
    # Σ(M1_kwh × r_m1_eff) + Σ(M2_kwh × r_m2_eff) + Σ(M3_kwh × r_m3_eff) = Q_kwh × r_q_eff
    # r_m3_eff = (Q_kwh × r_q_eff − M1_kwh × r_m1_eff − M2_kwh × r_m2_eff) / M3_kwh
    r_q_eff, _, q_hkn = _effective_rate_breakdown(cfg, q_rp)
    if m1 not in monthly_prices or m2 not in monthly_prices:
        raise PriceNotYetPublishedError(
            f"Need M1 and M2 monthly prices to derive M3 rate for {q}"
        )
    r_m1_eff = _effective_rate(
        cfg, chf_per_mwh_to_rp_per_kwh(monthly_prices[m1].chf_per_mwh)
    )
    r_m2_eff = _effective_rate(
        cfg, chf_per_mwh_to_rp_per_kwh(monthly_prices[m2].chf_per_mwh)
    )
    q_kwh = sum(kwh_per_month.get(m, 0.0) for m in (m1, m2, m3))
    m3_kwh = kwh_per_month.get(m3, 0.0)
    if m3_kwh <= 0:
        # All export happened in M1/M2. Use r_q_eff as a safe fallback.
        bonus = _resolve_bonuses_for_hour(cfg, season, r_q_eff - q_hkn, q_hkn)
        return r_q_eff + bonus, r_q_eff - q_hkn, q_hkn, bonus
    derived = (
        q_kwh * r_q_eff - kwh_per_month[m1] * r_m1_eff - kwh_per_month[m2] * r_m2_eff
    ) / m3_kwh
    # Hold HKN at the quarter's resolved value so the kWh-weighted HKN average
    # over the quarter equals the intended HKN. Base absorbs the residual.
    base_after = derived - q_hkn
    bonus = _resolve_bonuses_for_hour(cfg, season, base_after, q_hkn)
    return derived + bonus, base_after, q_hkn, bonus


@dataclass(frozen=True)
class QuarterSegment:
    """One contiguous (config × season) slice of a quarter.

    Splits a quarter into segments so OPT_CONFIG_HISTORY transitions that
    fall mid-quarter, and seasonal-month boundaries that fall mid-quarter,
    each yield their own per-hour rate rather than rounding to quarter-start.

    ``seg_id`` is a stable bucketing key (used by ``_aggregate_by_period`` to
    group sub-rows under each main period row). ``start_utc`` / ``end_utc``
    are the half-open UTC bounds of this segment.
    """

    seg_id: str
    start_utc: datetime
    end_utc: datetime
    cfg: TariffConfig


def compute_quarter_plan_segmented(
    q: Quarter,
    hourly_kwh: dict[datetime, float],
    quarterly_price: BfePrice,
    monthly_prices: dict[Month, BfePrice] | None,
    segments: list[QuarterSegment],
    billing_mode: str,
    anchor_sum_chf: float,
    old_post_quarter_first_sum_chf: float | None,
) -> QuarterPlan:
    """Segmented variant of :func:`compute_quarter_plan`. Each hour gets its
    rate from the segment whose ``[start_utc, end_utc)`` covers it.

    Single-segment lists reduce to the legacy single-cfg path bytewise.
    Multi-segment lists drive sub-row rendering downstream — see
    ``services._aggregate_by_period``. The ``billing_mode`` parameter is
    treated as a quarter-level constant (it's auto-derived from the
    quarter-start utility's ``settlement_period`` and stays uniform across
    segments by construction).
    """
    if not segments:
        raise ValueError("compute_quarter_plan_segmented requires >=1 segment")

    q_start_utc, q_end_utc = quarter_bounds_utc(q)

    # Pre-compute per-month kWh totals (needed for M3 derivation in monthly mode).
    m1, m2, m3 = q.months()
    kwh_per_month: dict[Month, float] = {m1: 0.0, m2: 0.0, m3: 0.0}
    for m in (m1, m2, m3):
        ms, me = month_bounds_utc(m)
        for h, kwh in hourly_kwh.items():
            if ms <= h < me:
                kwh_per_month[m] += kwh

    single_segment = len(segments) == 1

    records: list[HourRecord] = []
    running_sum = anchor_sum_chf
    seg_idx = 0
    for h in hours_in_range(q_start_utc, q_end_utc):
        # Advance the segment cursor; segments are pre-sorted contiguous.
        while seg_idx < len(segments) - 1 and h >= segments[seg_idx].end_utc:
            seg_idx += 1
        seg = segments[seg_idx]
        kwh = hourly_kwh.get(h, 0.0)
        if single_segment:
            # Preserve the legacy M3-derivation closure (Σ over Q == Q_kwh × Q_rate).
            rate_rp, base_rp, hkn_rp, bonus_rp = _rate_rp_kwh_at_hour(
                h, q, quarterly_price, monthly_prices, seg.cfg, billing_mode, kwh_per_month
            )
        else:
            # Multi-segment: rate per hour is derived directly from the
            # appropriate BFE price + this segment's cfg. The Σ-closure
            # invariant only holds within each segment's hours, not across
            # the whole quarter (different cfgs → different effective rates).
            q_rp = chf_per_mwh_to_rp_per_kwh(quarterly_price.chf_per_mwh)
            is_fixed_base = seg.cfg.resolved.base_model in ("fixed_flat", "fixed_ht_nt", "fixed_seasonal")
            if is_fixed_base or billing_mode != ABRECHNUNGS_RHYTHMUS_MONAT:
                rate_rp, base_rp, hkn_rp, bonus_rp = _effective_rate_breakdown_at_hour(
                    seg.cfg, q_rp, h
                )
            else:
                # Monthly RMP with multi-segment: use the month's own price
                # (M3 closure deliberately dropped — see docstring).
                if monthly_prices is None:
                    raise ValueError("monthly_prices required for monthly RMP mode")
                for m in (m1, m2, m3):
                    ms, me = month_bounds_utc(m)
                    if ms <= h < me:
                        this_month = m
                        break
                if this_month not in monthly_prices:
                    raise PriceNotYetPublishedError(
                        f"Monthly PV price for {this_month} not published"
                    )
                m_rp = chf_per_mwh_to_rp_per_kwh(monthly_prices[this_month].chf_per_mwh)
                rate_b, base_rp, hkn_rp = _effective_rate_breakdown(seg.cfg, m_rp)
                season = _season_at(seg.cfg.resolved, h)
                bonus_rp = _resolve_bonuses_for_hour(
                    seg.cfg, season, base_rp, hkn_rp
                )
                rate_rp = rate_b + bonus_rp
        chf = kwh * rp_per_kwh_to_chf_per_kwh(rate_rp)
        running_sum += chf
        records.append(
            HourRecord(
                start=h,
                kwh=kwh,
                rate_rp_kwh=rate_rp,
                compensation_chf=chf,
                base_rp_kwh=base_rp,
                hkn_rp_kwh=hkn_rp,
                seg_id=seg.seg_id,
                bonus_rp_kwh=bonus_rp,
            )
        )

    if old_post_quarter_first_sum_chf is None:
        post_delta = 0.0
    else:
        post_delta = running_sum - old_post_quarter_first_sum_chf

    return QuarterPlan(
        quarter=q,
        anchor_sum_chf=anchor_sum_chf,
        records=records,
        final_sum_chf=running_sum,
        post_quarter_delta_chf=post_delta,
    )


def compute_quarter_plan(
    q: Quarter,
    hourly_kwh: dict[datetime, float],
    quarterly_price: BfePrice,
    monthly_prices: dict[Month, BfePrice] | None,
    cfg: TariffConfig,
    billing_mode: str,
    anchor_sum_chf: float,
    old_post_quarter_first_sum_chf: float | None,
) -> QuarterPlan:
    """Build the quarter's compensation LTS records (single-config path).

    Thin wrapper around :func:`compute_quarter_plan_segmented` with one
    segment spanning the entire quarter. Existing callers and tests stay
    bytewise-compatible; the records returned have ``seg_id="single"``.
    """
    q_start_utc, q_end_utc = quarter_bounds_utc(q)
    segment = QuarterSegment(
        seg_id="single",
        start_utc=q_start_utc,
        end_utc=q_end_utc,
        cfg=cfg,
    )
    return compute_quarter_plan_segmented(
        q,
        hourly_kwh,
        quarterly_price,
        monthly_prices,
        [segment],
        billing_mode,
        anchor_sum_chf,
        old_post_quarter_first_sum_chf,
    )


def cumulative_sums(plan: QuarterPlan) -> list[float]:
    """Helper: running LTS sum values at each record's hour."""
    out: list[float] = []
    s = plan.anchor_sum_chf
    for r in plan.records:
        s += r.compensation_chf
        out.append(s)
    return out
