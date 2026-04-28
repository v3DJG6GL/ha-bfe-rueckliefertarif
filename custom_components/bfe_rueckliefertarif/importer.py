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

from dataclasses import dataclass
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

if TYPE_CHECKING:
    from .tariffs_db import ResolvedTariff


@dataclass(frozen=True)
class TariffConfig:
    """Resolved tariff inputs for one quarter of math.

    Wraps a ``ResolvedTariff`` (utility-published values from tariffs.json)
    plus the user's personal inputs (kW, Eigenverbrauch yes/no, HKN opt-in
    yes/no). The ``hkn_rp_kwh_resolved`` value is the JSON's HKN multiplied
    by 0 or 1 depending on whether the user opted in.
    """

    eigenverbrauch_aktiviert: bool
    installierte_leistung_kw: float
    hkn_aktiviert: bool
    hkn_rp_kwh_resolved: float          # JSON's HKN if opted in, else 0.0
    resolved: ResolvedTariff


@dataclass(frozen=True)
class HourRecord:
    start: datetime          # UTC, hour-aligned
    kwh: float               # export kWh in this hour
    rate_rp_kwh: float       # effective tariff applied (= base_rp_kwh + hkn_rp_kwh)
    compensation_chf: float  # kwh × rate / 100
    base_rp_kwh: float       # base after federal floor (and cap when binding)
    hkn_rp_kwh: float        # HKN bonus actually applied (0 if not opted in or cap-forfeited)
    # v0.9.9 — stable bucket key when a quarter spans multiple
    # (config × season) segments. ``None`` for legacy single-segment imports.
    seg_id: str | None = None


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
        cap_mode=rt.cap_mode,
    )


def _apply_floor_cap_hkn_breakdown(
    base_rp_kwh: float, cfg: TariffConfig
) -> tuple[float, float, float]:
    """Decomposed variant: returns ``(rate, base_after_floor, applied_hkn)``."""
    rt = cfg.resolved
    return effective_rp_kwh_breakdown(
        base_rp_kwh,
        cfg.hkn_rp_kwh_resolved,
        federal_floor_rp_kwh=_effective_floor(rt),
        cap_rp_kwh=rt.cap_rp_kwh,
        cap_mode=rt.cap_mode,
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

    return _effective_rate(cfg, reference_rp_kwh)


def _effective_rate_breakdown_at_hour(
    cfg: TariffConfig, reference_rp_kwh: float, hour_utc: datetime
) -> tuple[float, float, float]:
    """Decomposed variant of ``_effective_rate_at_hour``: ``(rate, base, applied_hkn)``."""
    from .tariff import classify_ht, classify_season

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
        return _apply_floor_cap_hkn_breakdown(base, cfg)

    if rt.base_model == "fixed_flat" and season is not None:
        key = f"{season}_rp_kwh"
        base = rt.seasonal.get(key)
        if base is None:
            raise ValueError(
                f"{rt.utility_key}: fixed_flat × seasonal requires both "
                f"summer_rp_kwh and winter_rp_kwh (missing {key})"
            )
        return _apply_floor_cap_hkn_breakdown(base, cfg)

    return _effective_rate_breakdown(cfg, reference_rp_kwh)


def _rate_rp_kwh_at_hour(
    hour_utc: datetime,
    q: Quarter,
    quarterly_price: BfePrice,
    monthly_prices: dict[Month, BfePrice] | None,
    cfg: TariffConfig,
    billing_mode: str,
    kwh_per_month: dict[Month, float] | None,
) -> tuple[float, float, float]:
    """Effective Rp/kWh for a single hour: ``(rate, base, applied_hkn)``.

    Quarterly mode: flat rate = tariff(quarterly_price) for every hour.
    Monthly mode: M1/M2 use monthly prices directly; M3 uses a derived rate
    such that the quarter total equals Q_kWh × Q_rate exactly. The breakdown
    holds the invariant ``rate == base + applied_hkn``; for the M3 derived
    case the HKN is held constant at the quarter's resolved HKN and the base
    absorbs the residual (so the quarterly HKN-weighted average is exact).
    """
    q_rp = chf_per_mwh_to_rp_per_kwh(quarterly_price.chf_per_mwh)

    # Fixed-rate utilities don't track BFE monthly prices — they're either
    # truly flat (fixed_flat) or per-hour HT/NT (fixed_ht_nt, switched on
    # local time inside _effective_rate_at_hour). Only RMP-based base_models
    # need the M1/M2/M3 monthly-price decomposition.
    is_fixed_base = cfg.resolved.base_model in ("fixed_flat", "fixed_ht_nt")
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

    if this_month in (m1, m2):
        if this_month not in monthly_prices:
            raise PriceNotYetPublishedError(
                f"Monthly PV price for {this_month} not published"
            )
        m_rp = chf_per_mwh_to_rp_per_kwh(monthly_prices[this_month].chf_per_mwh)
        return _effective_rate_breakdown(cfg, m_rp)

    # Month 3: derive rate so quarter sum matches exactly.
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
        # All export happened in M1/M2. Return r_q_eff as a safe fallback (M3 has no weight).
        return r_q_eff, r_q_eff - q_hkn, q_hkn
    derived = (
        q_kwh * r_q_eff - kwh_per_month[m1] * r_m1_eff - kwh_per_month[m2] * r_m2_eff
    ) / m3_kwh
    # Hold HKN at the quarter's resolved value so the kWh-weighted HKN average
    # over the quarter equals the intended HKN. Base absorbs the residual.
    return derived, derived - q_hkn, q_hkn


@dataclass(frozen=True)
class QuarterSegment:
    """One contiguous (config × season) slice of a quarter.

    v0.9.9 — splits a quarter into segments so OPT_CONFIG_HISTORY transitions
    that fall mid-quarter, and seasonal-month boundaries that fall mid-quarter,
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
            rate_rp, base_rp, hkn_rp = _rate_rp_kwh_at_hour(
                h, q, quarterly_price, monthly_prices, seg.cfg, billing_mode, kwh_per_month
            )
        else:
            # Multi-segment: rate per hour is derived directly from the
            # appropriate BFE price + this segment's cfg. The Σ-closure
            # invariant only holds within each segment's hours, not across
            # the whole quarter (different cfgs → different effective rates).
            q_rp = chf_per_mwh_to_rp_per_kwh(quarterly_price.chf_per_mwh)
            is_fixed_base = seg.cfg.resolved.base_model in ("fixed_flat", "fixed_ht_nt")
            if is_fixed_base or billing_mode != ABRECHNUNGS_RHYTHMUS_MONAT:
                rate_rp, base_rp, hkn_rp = _effective_rate_breakdown_at_hour(
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
                rate_rp, base_rp, hkn_rp = _effective_rate_breakdown(seg.cfg, m_rp)
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
