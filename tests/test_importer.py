"""Tests for importer pure-function layer.

These tests exercise the algorithmic core without HA: they verify that for any
billing mode, the quarter total compensation exactly equals Q_kWh × Q_rate_CHF,
and that the transition-spike delta is computed correctly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from custom_components.bfe_rueckliefertarif.bfe import BfePrice
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
)
from custom_components.bfe_rueckliefertarif.importer import (
    TariffConfig,
    compute_quarter_plan,
    cumulative_sums,
)
from custom_components.bfe_rueckliefertarif.quarters import (
    Month,
    Quarter,
    hours_in_range,
    month_bounds_utc,
    quarter_bounds_utc,
)
from custom_components.bfe_rueckliefertarif.tariff import (
    chf_per_mwh_to_rp_per_kwh,
    effective_rp_kwh,
    rp_per_kwh_to_chf_per_kwh,
)
from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff


def _make_resolved(
    *,
    base_model: str = "rmp_quartal",
    fixed_rp_kwh: float | None = None,
    hkn_rp_kwh: float = 0.0,
    cap_mode: bool = False,
    cap_rp_kwh: float | None = None,
    federal_floor_rp_kwh: float | None = 6.00,
) -> ResolvedTariff:
    """Build a ResolvedTariff for tests without going through tariffs.json."""
    return ResolvedTariff(
        utility_key="test",
        valid_from="2026-01-01",
        settlement_period="quartal",
        base_model=base_model,
        fixed_rp_kwh=fixed_rp_kwh,
        fixed_ht_rp_kwh=None,
        fixed_nt_rp_kwh=None,
        hkn_rp_kwh=hkn_rp_kwh,
        hkn_structure="additive_optin" if hkn_rp_kwh > 0 else "none",
        cap_mode=cap_mode,
        cap_rp_kwh=cap_rp_kwh,
        federal_floor_rp_kwh=federal_floor_rp_kwh,
        federal_floor_label="<30 kW",
        requires_naturemade_star=False,
        price_floor_rp_kwh=None,
        tariffs_json_version="1.0.0",
        tariffs_json_source="bundled",
    )


# --- test helpers ---

def uniform_hourly(q: Quarter, kwh_per_hour: float) -> dict[datetime, float]:
    """Build hourly kWh map with a constant rate (avoids synthetic spikes)."""
    s, e = quarter_bounds_utc(q)
    return {h: kwh_per_hour for h in hours_in_range(s, e)}


def realistic_hourly(q: Quarter, monthly_totals_kwh: dict[Month, float]) -> dict[datetime, float]:
    """Spread a given monthly total uniformly across that month's hours.

    Simulates the realistic case where export varies by month (more in March).
    """
    out: dict[datetime, float] = {}
    for m, total in monthly_totals_kwh.items():
        ms, me = month_bounds_utc(m)
        hrs = hours_in_range(ms, me)
        if not hrs:
            continue
        per_hour = total / len(hrs)
        for h in hrs:
            out[h] = per_hour
    return out


EKZ_CFG = TariffConfig(
    eigenverbrauch_aktiviert=True,
    installierte_leistung_kw=10.0,
    hkn_aktiviert=False,
    hkn_rp_kwh_resolved=0.0,
    resolved=_make_resolved(),  # rmp_quartal, ≤30 kW small-band floor 6.00
)

EKZ_Q1_2026_PRICE = BfePrice(chf_per_mwh=102.66, days=90, volume_mwh=683957.0)
EKZ_MONTHLY_Q1_2026 = {
    Month(2026, 1): BfePrice(chf_per_mwh=126.77, days=31, volume_mwh=101447.0),
    Month(2026, 2): BfePrice(chf_per_mwh=97.70, days=28, volume_mwh=185438.0),
    Month(2026, 3): BfePrice(chf_per_mwh=98.81, days=31, volume_mwh=397071.0),
}
Q = Quarter(2026, 1)


class TestQuarterlyMode:
    def test_uniform_kwh_produces_flat_compensation(self):
        kwh = uniform_hourly(Q, kwh_per_hour=1.0)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )
        # Every hour's rate should be the same flat quarterly effective rate
        rates = {r.rate_rp_kwh for r in plan.records}
        assert len(rates) == 1
        expected = effective_rp_kwh(
            chf_per_mwh_to_rp_per_kwh(102.66), 0.0,
            federal_floor_rp_kwh=6.00,  # ≤30 kW small-band floor
            cap_rp_kwh=None, cap_mode=False,
        )
        assert rates.pop() == pytest.approx(expected)

    def test_quarterly_sum_equals_kwh_times_quarter_rate(self):
        kwh_per_hour = 1.0
        kwh = uniform_hourly(Q, kwh_per_hour=kwh_per_hour)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )
        q_rate_rp = effective_rp_kwh(
            chf_per_mwh_to_rp_per_kwh(102.66), 0.0,
            federal_floor_rp_kwh=6.00,
            cap_rp_kwh=None, cap_mode=False,
        )
        total_kwh = sum(kwh.values())
        expected = total_kwh * rp_per_kwh_to_chf_per_kwh(q_rate_rp)
        assert plan.final_sum_chf == pytest.approx(expected, rel=1e-9)

    def test_anchor_is_applied(self):
        kwh = uniform_hourly(Q, kwh_per_hour=0.5)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=42.0, old_post_quarter_first_sum_chf=None,
        )
        assert plan.anchor_sum_chf == 42.0
        # First record's cumulative = anchor + first hour compensation
        sums = cumulative_sums(plan)
        assert sums[0] == pytest.approx(42.0 + plan.records[0].compensation_chf)


class TestMonthlyMode:
    def test_monthly_total_matches_quarterly_invoice(self):
        """Monthly mode with M3 true-up must produce the same quarter sum as quarterly mode."""
        monthly_totals = {
            Month(2026, 1): 50.0,   # low winter export
            Month(2026, 2): 120.0,  # rising
            Month(2026, 3): 300.0,  # high
        }
        kwh = realistic_hourly(Q, monthly_totals)
        plan_m = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, EKZ_MONTHLY_Q1_2026, EKZ_CFG,
            ABRECHNUNGS_RHYTHMUS_MONAT, 0.0, None,
        )
        plan_q = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG,
            ABRECHNUNGS_RHYTHMUS_QUARTAL, 0.0, None,
        )
        assert plan_m.final_sum_chf == pytest.approx(plan_q.final_sum_chf, rel=1e-9)

    def test_monthly_intra_quarter_variation(self):
        """In monthly mode, M1 and M2 hours use monthly rates (different from Q rate)."""
        monthly_totals = {
            Month(2026, 1): 100.0,
            Month(2026, 2): 100.0,
            Month(2026, 3): 100.0,
        }
        kwh = realistic_hourly(Q, monthly_totals)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, EKZ_MONTHLY_Q1_2026, EKZ_CFG,
            ABRECHNUNGS_RHYTHMUS_MONAT, 0.0, None,
        )
        # Separate hours by month
        m1_s, m1_e = month_bounds_utc(Month(2026, 1))
        m2_s, m2_e = month_bounds_utc(Month(2026, 2))
        m1_rates = {r.rate_rp_kwh for r in plan.records if m1_s <= r.start < m1_e}
        m2_rates = {r.rate_rp_kwh for r in plan.records if m2_s <= r.start < m2_e}
        assert len(m1_rates) == 1
        assert len(m2_rates) == 1
        # Different rates because Jan 126.77 ≠ Feb 97.70 CHF/MWh
        assert m1_rates.pop() != m2_rates.pop()

    def test_m3_rate_derived_to_match_quarter(self):
        """M3 rate must compensate for M1/M2 excess/shortfall."""
        # Monthly prices in Q1 2026 average (volume-weighted) to ~102.66 quarterly.
        # If we set M1/M2/M3 kWh exactly equal to BFE volumes, M3 rate should be close to Q rate.
        # But with imbalanced distribution (most in Mar), M3 rate differs from M3 monthly price.
        monthly_totals = {
            Month(2026, 1): 10.0,
            Month(2026, 2): 10.0,
            Month(2026, 3): 100.0,
        }
        kwh = realistic_hourly(Q, monthly_totals)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, EKZ_MONTHLY_Q1_2026, EKZ_CFG,
            ABRECHNUNGS_RHYTHMUS_MONAT, 0.0, None,
        )
        # Verify sum = Q_kWh × Q_rate exactly
        q_rate_rp = effective_rp_kwh(
            chf_per_mwh_to_rp_per_kwh(102.66), 0.0,
            federal_floor_rp_kwh=6.00,
            cap_rp_kwh=None, cap_mode=False,
        )
        total_kwh = sum(monthly_totals.values())
        expected = total_kwh * rp_per_kwh_to_chf_per_kwh(q_rate_rp)
        assert plan.final_sum_chf == pytest.approx(expected, rel=1e-9)


class TestTransitionSpike:
    def test_no_post_quarter_data_delta_zero(self):
        kwh = uniform_hourly(Q, kwh_per_hour=0.1)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )
        assert plan.post_quarter_delta_chf == 0.0

    def test_delta_computed_when_post_quarter_data_exists(self):
        kwh = uniform_hourly(Q, kwh_per_hour=1.0)
        # Imagine old compensation LTS had sum=50 at q_end
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=50.0,
        )
        # new final sum = 2159 hours × 1 kWh × 0.10266 CHF/kWh ≈ 221.64
        # delta = 221.64 − 50 ≈ 171.64
        assert plan.post_quarter_delta_chf == pytest.approx(plan.final_sum_chf - 50.0)


class TestFixedMode:
    def test_fixed_mode_rate_constant_across_hours(self):
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kw=10.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(
                base_model="fixed_flat", fixed_rp_kwh=10.96,  # SIG-style flat rate
            ),
        )
        kwh = uniform_hourly(Q, kwh_per_hour=1.0)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, EKZ_MONTHLY_Q1_2026, cfg,
            ABRECHNUNGS_RHYTHMUS_QUARTAL, 0.0, None,
        )
        rates = {r.rate_rp_kwh for r in plan.records}
        assert rates == {10.96}

    def test_iwb_fixed_paid_in_full_no_cap(self):
        # IWB is an additive utility (verguetungs_obergrenze=False) — fixed
        # 12.95 Rp/kWh including HKN, paid in full without Anrechenbarkeitsgrenze cap.
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kw=10.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(
                base_model="fixed_flat", fixed_rp_kwh=12.95,  # IWB 2026 (HKN bundled in)
            ),
        )
        kwh = uniform_hourly(Q, kwh_per_hour=1.0)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, cfg,
            ABRECHNUNGS_RHYTHMUS_QUARTAL, 0.0, None,
        )
        rates = {r.rate_rp_kwh for r in plan.records}
        assert len(rates) == 1
        assert rates.pop() == pytest.approx(12.95)  # paid in full

    def test_ekz_strict_cap_reduces_hkn(self):
        # EKZ-style: verguetungs_obergrenze=True. RMP base 7.0 + HKN 5.0 = 12.0
        # > cap 10.96 → HKN reduced so total lands at 10.96.
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kw=10.0,
            hkn_aktiviert=True,
            hkn_rp_kwh_resolved=5.0,
            resolved=_make_resolved(
                base_model="rmp_quartal",
                hkn_rp_kwh=5.0,
                cap_mode=True,
                cap_rp_kwh=10.96,  # ≤100 kW mit EV
            ),
        )
        kwh = uniform_hourly(Q, kwh_per_hour=1.0)
        # synthetic Q price: 70.0 CHF/MWh = 7.00 Rp/kWh
        synthetic = BfePrice(chf_per_mwh=70.0, days=90, volume_mwh=1.0)
        plan = compute_quarter_plan(
            Q, kwh, synthetic, None, cfg,
            ABRECHNUNGS_RHYTHMUS_QUARTAL, 0.0, None,
        )
        rates = {r.rate_rp_kwh for r in plan.records}
        assert rates == {10.96}  # capped (HKN reduced from 5.0 to 3.96)


class TestHourCoverage:
    def test_all_hours_covered(self):
        kwh = uniform_hourly(Q, kwh_per_hour=0.0)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            0.0, None,
        )
        # Q1 2026 with DST spring-forward = 2159 hours
        assert len(plan.records) == 2159

    def test_records_are_hour_aligned_and_ordered(self):
        kwh = uniform_hourly(Q, kwh_per_hour=0.0)
        plan = compute_quarter_plan(
            Q, kwh, EKZ_Q1_2026_PRICE, None, EKZ_CFG, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            0.0, None,
        )
        for i, r in enumerate(plan.records):
            assert r.start.minute == 0
            assert r.start.second == 0
            if i > 0:
                assert r.start - plan.records[i - 1].start == timedelta(hours=1)
        assert plan.records[0].start == datetime(2025, 12, 31, 23, 0, tzinfo=UTC)
