"""Tests for importer pure-function layer.

These tests exercise the algorithmic core without HA: they verify that for any
billing mode, the quarter total compensation exactly equals Q_kWh × Q_rate_CHF,
and that the transition-spike delta is computed correctly.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from custom_components.bfe_rueckliefertarif.bfe import BfePrice
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
)
from custom_components.bfe_rueckliefertarif.importer import (
    QuarterSegment,
    TariffConfig,
    _apply_floor_cap_hkn,
    _effective_rate,
    _effective_rate_at_hour,
    _effective_rate_breakdown_at_hour,
    _resolve_bonuses_for_hour_detailed,
    compute_breakdown_at,
    compute_quarter_plan,
    compute_quarter_plan_segmented,
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
    classify_ht,
    effective_rp_kwh,
    rp_per_kwh_to_chf_per_kwh,
)
from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff


def _make_resolved(
    *,
    base_model: str = "rmp_quartal",
    fixed_rp_kwh: float | None = None,
    fixed_ht_rp_kwh: float | None = None,
    fixed_nt_rp_kwh: float | None = None,
    ht_window: dict | None = None,
    seasonal: dict | None = None,
    hkn_rp_kwh: float = 0.0,
    cap_rp_kwh: float | None = None,
    federal_floor_rp_kwh: float | None = 6.00,
    price_floor_rp_kwh: float | None = None,
    bonuses: tuple[dict, ...] | None = None,
    tier_bonuses: tuple[dict, ...] | None = None,
) -> ResolvedTariff:
    """Build a ResolvedTariff for tests without going through tariffs.json.

    v0.23.0: ``tier_seasonal`` was dropped — for fixed_seasonal tiers,
    pass the tier's seasonal block via ``seasonal=`` directly (the
    resolver does the same routing).
    """
    return ResolvedTariff(
        utility_key="test",
        valid_from="2026-01-01",
        settlement_period="quartal",
        base_model=base_model,
        fixed_rp_kwh=fixed_rp_kwh,
        fixed_ht_rp_kwh=fixed_ht_rp_kwh,
        fixed_nt_rp_kwh=fixed_nt_rp_kwh,
        hkn_rp_kwh=hkn_rp_kwh,
        hkn_structure="additive_optin" if hkn_rp_kwh > 0 else "none",
        cap_rp_kwh=cap_rp_kwh,
        federal_floor_rp_kwh=federal_floor_rp_kwh,
        federal_floor_label="<30 kW",
        price_floor_rp_kwh=price_floor_rp_kwh,
        tariffs_json_version="1.0.0",
        tariffs_json_source="bundled",
        ht_window=ht_window,
        seasonal=seasonal,
        bonuses=bonuses,
        tier_bonuses=tier_bonuses,
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
    installierte_leistung_kwp=10.0,
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


def _zurich_utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    """Build a UTC datetime from a Zurich-local wall-clock time."""
    local = datetime(year, month, day, hour, minute, tzinfo=ZoneInfo("Europe/Zurich"))
    return local.astimezone(UTC)


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
            cap_rp_kwh=None,
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
            cap_rp_kwh=None,
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
            cap_rp_kwh=None,
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
            installierte_leistung_kwp=10.0,
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
            installierte_leistung_kwp=10.0,
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
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=True,
            hkn_rp_kwh_resolved=5.0,
            resolved=_make_resolved(
                base_model="rmp_quartal",
                hkn_rp_kwh=5.0,
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


class TestFixedHtNtMode:
    """Per-hour HT/NT switching for fixed_ht_nt utilities (e.g. EKZ pre-2026)."""

    # EKZ producer-side window (verified against ekz-rueckliefertarife-2025.pdf):
    # weekdays 07:00–20:00 = HT; Sa/Su all NT.
    EKZ_HT_WINDOW = {"mofr": [7, 20], "sa": None, "su": None}

    # Real EKZ 2025 producer rates (Rp/kWh).
    EKZ_HT_RP = 12.60
    EKZ_NT_RP = 11.60
    EKZ_HKN_RP = 3.00

    def _make_ekz_2025_cfg(self, *, hkn_opted_in: bool) -> TariffConfig:
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=hkn_opted_in,
            hkn_rp_kwh_resolved=self.EKZ_HKN_RP if hkn_opted_in else 0.0,
            resolved=_make_resolved(
                base_model="fixed_ht_nt",
                fixed_ht_rp_kwh=self.EKZ_HT_RP,
                fixed_nt_rp_kwh=self.EKZ_NT_RP,
                ht_window=self.EKZ_HT_WINDOW,
                hkn_rp_kwh=self.EKZ_HKN_RP,
                cap_rp_kwh=None,
                # Pre-2026: no federal floor existed. Test against the
                # math-only signal, not the AS 2025 138 reform.
                federal_floor_rp_kwh=None,
            ),
        )

    @pytest.mark.parametrize(
        "year,month,day,hour,expected_rp",
        [
            pytest.param(2025, 1, 15, 14, 15.60, id="weekday-midday-ht"),
            pytest.param(2025, 1, 15, 22, 14.60, id="weekday-night-nt"),
            pytest.param(2025, 1, 18, 14, 14.60, id="saturday-midday-nt"),
            pytest.param(2025, 1, 19, 12, 14.60, id="sunday-nt"),
        ],
    )
    def test_ht_nt_matrix_with_hkn(self, year, month, day, hour, expected_rp):
        cfg = self._make_ekz_2025_cfg(hkn_opted_in=True)
        rate = _effective_rate_at_hour(cfg, 0.0, _zurich_utc(year, month, day, hour))
        assert rate == pytest.approx(expected_rp)

    def test_no_hkn_drops_only_hkn_addend(self):
        cfg = self._make_ekz_2025_cfg(hkn_opted_in=False)
        # Wed 14:00 → HT 12.60 only
        assert _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 1, 15, 14)) == pytest.approx(12.60)
        # Wed 22:00 → NT 11.60 only
        assert _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 1, 15, 22)) == pytest.approx(11.60)

    def test_quarter_total_matches_hand_computed_sum(self):
        """compute_quarter_plan output equals Σ(kwh_h × rate_h) computed
        independently via classify_ht — invoice-grade invariant."""
        cfg = self._make_ekz_2025_cfg(hkn_opted_in=True)

        # Use a Q1 2025 quarter (matches the EKZ-2025 rate window).
        q = Quarter(2025, 1)
        kwh = uniform_hourly(q, kwh_per_hour=1.0)
        # BFE quarterly price is unused for fixed_ht_nt, but the function
        # signature requires one. Pass a sentinel.
        unused_price = BfePrice(chf_per_mwh=0.0, days=90, volume_mwh=1.0)
        plan = compute_quarter_plan(
            q, kwh, unused_price, None, cfg, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )

        # Hand-compute the expected total: HT hours × (12.60+3.00) + NT hours × (11.60+3.00)
        ht_kwh = sum(
            kwh[h] for h in kwh if classify_ht(h, self.EKZ_HT_WINDOW)
        )
        nt_kwh = sum(
            kwh[h] for h in kwh if not classify_ht(h, self.EKZ_HT_WINDOW)
        )
        expected_chf = (
            ht_kwh * (self.EKZ_HT_RP + self.EKZ_HKN_RP) / 100.0
            + nt_kwh * (self.EKZ_NT_RP + self.EKZ_HKN_RP) / 100.0
        )
        assert plan.final_sum_chf == pytest.approx(expected_chf, rel=1e-9)

        # Sanity: at least some HT and some NT hours present (would catch
        # a regression where classify_ht is always-True or always-False).
        assert ht_kwh > 0
        assert nt_kwh > 0

    def test_per_hour_records_carry_correct_rate_per_hour(self):
        """Each HourRecord's rate_rp_kwh reflects the hour's HT/NT classification."""
        cfg = self._make_ekz_2025_cfg(hkn_opted_in=True)
        q = Quarter(2025, 1)
        kwh = uniform_hourly(q, kwh_per_hour=1.0)
        unused_price = BfePrice(chf_per_mwh=0.0, days=90, volume_mwh=1.0)
        plan = compute_quarter_plan(
            q, kwh, unused_price, None, cfg, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )
        rates_seen = sorted({round(r.rate_rp_kwh, 6) for r in plan.records})
        # Exactly two distinct rates: NT+HKN (14.60) and HT+HKN (15.60)
        assert rates_seen == [pytest.approx(14.60), pytest.approx(15.60)]

    def test_missing_nt_rate_raises(self):
        """fixed_ht_nt requires both HT and NT rates — a record missing
        fixed_nt_rp_kwh should fail loudly when an NT hour is hit."""
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(
                base_model="fixed_ht_nt",
                fixed_ht_rp_kwh=12.60,
                fixed_nt_rp_kwh=None,  # broken record
                ht_window=self.EKZ_HT_WINDOW,
                federal_floor_rp_kwh=None,
            ),
        )
        # Wed 22:00 → NT hour; NT rate is None → raise
        with pytest.raises(ValueError, match="fixed_ht_nt requires both"):
            _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 1, 15, 22))


class TestSeasonalFixedFlat:
    """fixed_flat × seasonal — summer/winter rate switch by Zurich-local month.

    Hypothetical Bagnes-style flat seasonal tariff (numbers chosen to
    match the schema example in v0.5).
    """

    BAGNES_SEASONAL = {
        "summer_months": [4, 5, 6, 7, 8, 9],
        "winter_months": [10, 11, 12, 1, 2, 3],
        "summer_rp_kwh": 9.00,
        "winter_rp_kwh": 12.00,
    }

    def _make_cfg(self, *, seasonal: dict, hkn: float = 0.0) -> TariffConfig:
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=hkn > 0,
            hkn_rp_kwh_resolved=hkn,
            resolved=_make_resolved(
                base_model="fixed_flat",
                fixed_rp_kwh=None,  # seasonal supersedes the year-round flat
                seasonal=seasonal,
                federal_floor_rp_kwh=None,
            ),
        )

    @pytest.mark.parametrize(
        "year,month,day,hour,expected_rp",
        [
            pytest.param(2025, 7, 15, 12, 9.00, id="july-summer"),
            pytest.param(2025, 1, 15, 12, 12.00, id="january-winter"),
            pytest.param(2025, 4, 1, 0, 9.00, id="april-first-summer-boundary"),
        ],
    )
    def test_seasonal_rate_matrix(self, year, month, day, hour, expected_rp):
        cfg = self._make_cfg(seasonal=self.BAGNES_SEASONAL)
        rate = _effective_rate_at_hour(cfg, 0.0, _zurich_utc(year, month, day, hour))
        assert rate == pytest.approx(expected_rp)

    def test_summer_rate_plus_hkn(self):
        cfg = self._make_cfg(seasonal=self.BAGNES_SEASONAL, hkn=3.00)
        rate = _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 7, 15, 12))
        assert rate == pytest.approx(12.00)  # 9.00 + 3.00 HKN

    def test_missing_winter_rate_raises_in_winter(self):
        broken = {
            "summer_months": [4, 5, 6, 7, 8, 9],
            "winter_months": [10, 11, 12, 1, 2, 3],
            "summer_rp_kwh": 9.00,
            # winter_rp_kwh missing
        }
        cfg = self._make_cfg(seasonal=broken)
        with pytest.raises(ValueError, match="winter_rp_kwh"):
            _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 1, 15, 12))

    def test_period_fallback_raises_for_seasonal(self):
        # _effective_rate has no hour context → can't pick season → must raise.
        cfg = self._make_cfg(seasonal=self.BAGNES_SEASONAL)
        with pytest.raises(ValueError, match="seasonal evaluation requires hour"):
            _effective_rate(cfg, 0.0)


class TestComputeBreakdownAt:
    """compute_breakdown_at — single-hour breakdown dict for the live sensor
    and analysis service. Covers v0.19.0 schema additions (season_now,
    ht_nt_now, bonuses_applied/advertised)."""

    SEASONAL_FIXED_FLAT = {
        "summer_months": [4, 5, 6, 7, 8, 9],
        "winter_months": [10, 11, 12, 1, 2, 3],
        "summer_rp_kwh": 6.20,
        "winter_rp_kwh": 9.00,
    }

    def _make_seasonal_cfg(self) -> TariffConfig:
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=8.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(
                base_model="fixed_flat",
                fixed_rp_kwh=7.60,  # Jahresmittel — was incorrectly used in v0.18.x
                seasonal=self.SEASONAL_FIXED_FLAT,
                federal_floor_rp_kwh=6.00,
            ),
        )

    def test_dict_shape_has_expected_keys(self):
        cfg = self._make_seasonal_cfg()
        bd = compute_breakdown_at(cfg, 0.0, _zurich_utc(2026, 5, 15, 12))
        for key in (
            "utility", "tariff_source", "floor_label",
            "eigenverbrauch_aktiviert", "hkn_aktiviert", "base_model",
            "base_input_rp_kwh", "base_source", "minimalverguetung_rp_kwh",
            "base_after_floor_rp_kwh", "hkn_verguetung_rp_kwh",
            "theoretical_total_rp_kwh", "anrechenbarkeitsgrenze_rp_kwh",
            "effective_rp_kwh", "effective_chf_kwh", "obergrenze_aktiv",
            "hkn_gekuerzt_auf",
            # v0.19.0 additions
            "season_now", "ht_nt_now", "applied_bonus_rp_kwh",
            "bonuses_applied", "bonuses_advertised",
        ):
            assert key in bd, f"missing breakdown key: {key}"

    def test_summer_picks_seasonal_rate_not_jahresmittel(self):
        """Bug fix: v0.18.x live sensor returned 7.6 (Jahresmittel) for
        regio_energie_solothurn in May; should be 6.2 (summer)."""
        cfg = self._make_seasonal_cfg()
        bd = compute_breakdown_at(cfg, 0.0, _zurich_utc(2026, 5, 15, 12))
        assert bd["base_input_rp_kwh"] == pytest.approx(6.20)
        assert bd["effective_rp_kwh"] == pytest.approx(6.20)
        assert bd["effective_chf_kwh"] == pytest.approx(0.062)
        assert bd["season_now"] == "summer"
        assert bd["base_source"] == "fixed_flat_summer"
        assert bd["ht_nt_now"] is None  # not fixed_ht_nt

    def test_winter_picks_winter_rate(self):
        cfg = self._make_seasonal_cfg()
        bd = compute_breakdown_at(cfg, 0.0, _zurich_utc(2026, 1, 15, 12))
        assert bd["base_input_rp_kwh"] == pytest.approx(9.00)
        assert bd["effective_rp_kwh"] == pytest.approx(9.00)
        assert bd["season_now"] == "winter"
        assert bd["base_source"] == "fixed_flat_winter"

    def test_bonuses_applied_vs_advertised(self):
        """Opt-in bonuses without a when-clause are advertised but not applied
        (no toggle to gate them on). Always-on bonuses are both."""
        bonuses = [
            {
                "name": "TOP-40",
                "kind": "multiplier_pct",
                "multiplier_pct": 108.0,
                "applies_when": "opt_in",
                # No when clause — should be advertised but not applied.
            },
            {
                "name": "Eco surcharge",
                "kind": "additive_rp_kwh",
                "rate_rp_kwh": 0.5,
                "applies_when": "always",
            },
        ]
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=8.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(
                base_model="fixed_flat",
                fixed_rp_kwh=7.60,
                federal_floor_rp_kwh=6.00,
            ),
        )
        # Inject bonuses via dataclasses.replace
        cfg = replace(
            cfg, resolved=replace(cfg.resolved, bonuses=tuple(bonuses))
        )
        bd = compute_breakdown_at(cfg, 0.0, _zurich_utc(2026, 5, 15, 12))

        # Advertised: both bonuses present
        names_advertised = [b["name"] for b in bd["bonuses_advertised"]]
        assert names_advertised == ["TOP-40", "Eco surcharge"]

        # Applied: only the always-on Eco surcharge
        names_applied = [b["name"] for b in bd["bonuses_applied"]]
        assert names_applied == ["Eco surcharge"]
        assert bd["applied_bonus_rp_kwh"] == pytest.approx(0.5)
        assert bd["effective_rp_kwh"] == pytest.approx(7.60 + 0.5)


class TestSeasonalFixedHtNt:
    """fixed_ht_nt × seasonal — 4-rate matrix (summer/winter × HT/NT)."""

    # Hypothetical Samnaun-style schema example: high in winter, low in summer.
    SAMNAUN_SEASONAL = {
        "summer_months": [4, 5, 6, 7, 8, 9],
        "winter_months": [10, 11, 12, 1, 2, 3],
        "summer_ht_rp_kwh": 5.50,
        "summer_nt_rp_kwh": 4.50,
        "winter_ht_rp_kwh": 7.00,
        "winter_nt_rp_kwh": 6.50,
    }
    HT_WINDOW = {"mofr": [7, 20], "sa": None, "su": None}

    def _make_cfg(self, *, seasonal: dict | None = None, hkn: float = 0.0) -> TariffConfig:
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=hkn > 0,
            hkn_rp_kwh_resolved=hkn,
            resolved=_make_resolved(
                base_model="fixed_ht_nt",
                # tier rates serve as fallback when seasonal is None
                fixed_ht_rp_kwh=12.60,
                fixed_nt_rp_kwh=11.60,
                ht_window=self.HT_WINDOW,
                seasonal=seasonal,
                federal_floor_rp_kwh=None,
            ),
        )

    @pytest.mark.parametrize(
        "year,month,day,hour,expected_rp",
        [
            pytest.param(2025, 7, 16, 14, 5.50, id="summer-weekday-midday-ht"),
            pytest.param(2025, 7, 16, 22, 4.50, id="summer-weekday-night-nt"),
            pytest.param(2025, 1, 15, 14, 7.00, id="winter-weekday-midday-ht"),
            pytest.param(2025, 1, 18, 12, 6.50, id="winter-saturday-nt"),
            # Boundaries: 2025-10-01 00:00 Wed → winter NT (window 07-20);
            # 2025-04-01 14:00 Tue → summer HT.
            pytest.param(2025, 10, 1, 0, 6.50, id="october-first-winter-nt-boundary"),
            pytest.param(2025, 4, 1, 14, 5.50, id="april-first-summer-ht-boundary"),
        ],
    )
    def test_seasonal_ht_nt_matrix(self, year, month, day, hour, expected_rp):
        cfg = self._make_cfg(seasonal=self.SAMNAUN_SEASONAL)
        rate = _effective_rate_at_hour(cfg, 0.0, _zurich_utc(year, month, day, hour))
        assert rate == pytest.approx(expected_rp)

    def test_winter_ht_plus_hkn(self):
        cfg = self._make_cfg(seasonal=self.SAMNAUN_SEASONAL, hkn=2.00)
        rate = _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 1, 15, 14))
        assert rate == pytest.approx(9.00)  # 7.00 + 2.00

    def test_missing_summer_nt_raises_at_summer_nt_hour(self):
        broken = dict(self.SAMNAUN_SEASONAL)
        del broken["summer_nt_rp_kwh"]
        cfg = self._make_cfg(seasonal=broken)
        # Wed 22:00 in summer → should hit missing summer_nt_rp_kwh
        with pytest.raises(ValueError, match="missing summer_nt_rp_kwh"):
            _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 7, 16, 22))

    def test_missing_winter_ht_raises_at_winter_ht_hour(self):
        broken = dict(self.SAMNAUN_SEASONAL)
        del broken["winter_ht_rp_kwh"]
        cfg = self._make_cfg(seasonal=broken)
        with pytest.raises(ValueError, match="missing winter_ht_rp_kwh"):
            _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2025, 1, 15, 14))

    def test_period_fallback_raises_for_seasonal(self):
        cfg = self._make_cfg(seasonal=self.SAMNAUN_SEASONAL)
        with pytest.raises(ValueError, match="seasonal evaluation requires hour"):
            _effective_rate(cfg, 0.0)

    def test_quarter_total_invariant_across_seasonal_boundary(self):
        """Q2 2025 spans Apr-May-Jun (all summer for canonical CH split) —
        sum should match Σ(kwh_h × summer_ht_or_nt_rate) exactly."""
        cfg = self._make_cfg(seasonal=self.SAMNAUN_SEASONAL, hkn=0.0)
        q = Quarter(2025, 2)
        kwh = uniform_hourly(q, kwh_per_hour=1.0)
        unused = BfePrice(chf_per_mwh=0.0, days=91, volume_mwh=1.0)
        plan = compute_quarter_plan(
            q, kwh, unused, None, cfg, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )
        ht_kwh = sum(kwh[h] for h in kwh if classify_ht(h, self.HT_WINDOW))
        nt_kwh = sum(kwh[h] for h in kwh if not classify_ht(h, self.HT_WINDOW))
        expected = (
            ht_kwh * 5.50 / 100.0    # summer HT (whole quarter is summer)
            + nt_kwh * 4.50 / 100.0  # summer NT
        )
        assert plan.final_sum_chf == pytest.approx(expected, rel=1e-9)
        assert ht_kwh > 0 and nt_kwh > 0


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


class TestEffectiveFloorMaxOfFederalAndUtility:
    """#2 — utility-level price_floor_rp_kwh wired into the per-hour floor."""

    def _cfg(self, federal: float | None, utility: float | None) -> TariffConfig:
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(
                base_model="rmp_quartal",
                federal_floor_rp_kwh=federal,
                price_floor_rp_kwh=utility,
            ),
        )

    @pytest.mark.parametrize(
        "federal,utility,reference,expected",
        [
            pytest.param(6.0, 8.0, 4.0, 8.0, id="utility-dominates"),
            pytest.param(6.0, 4.0, 3.0, 6.0, id="federal-dominates"),
            pytest.param(None, None, 3.0, 3.0, id="no-floor-passthrough"),
            pytest.param(None, 7.0, 3.0, 7.0, id="utility-only"),
        ],
    )
    def test_floor_max_of_federal_and_utility(self, federal, utility, reference, expected):
        rp = _apply_floor_cap_hkn(reference, self._cfg(federal=federal, utility=utility))
        assert rp == pytest.approx(expected)


class TestSnapshotFloorSource:
    """#2 — snapshot stores floor_source so the report can render the right line."""

    def test_floor_source_reflects_dominant_floor(self):
        from custom_components.bfe_rueckliefertarif.services import _floor_source

        rt_fed = _make_resolved(federal_floor_rp_kwh=6.0, price_floor_rp_kwh=4.0)
        assert _floor_source(rt_fed) == "federal"

        rt_utl = _make_resolved(federal_floor_rp_kwh=6.0, price_floor_rp_kwh=8.0)
        assert _floor_source(rt_utl) == "utility"

        rt_tied = _make_resolved(federal_floor_rp_kwh=6.0, price_floor_rp_kwh=6.0)
        # Tie → federal (utility doesn't strictly dominate).
        assert _floor_source(rt_tied) == "federal"

        rt_none = _make_resolved(federal_floor_rp_kwh=None, price_floor_rp_kwh=None)
        assert _floor_source(rt_none) == "federal"


class TestSegmentedQuarterPlan:
    """v0.9.9 — compute_quarter_plan_segmented + legacy back-compat path."""

    def test_legacy_single_segment_records_carry_seg_id(self):
        # Sanity: single-segment legacy compute_quarter_plan now returns
        # records with seg_id="single".
        q = Quarter(2026, 1)
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True, installierte_leistung_kwp=10.0,
            hkn_aktiviert=False, hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(base_model="fixed_flat", fixed_rp_kwh=9.0),
        )
        plan = compute_quarter_plan(
            q=q,
            hourly_kwh=uniform_hourly(q, 1.0),
            quarterly_price=BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=600000.0),
            monthly_prices=None,
            cfg=cfg,
            billing_mode=ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0,
            old_post_quarter_first_sum_chf=None,
        )
        assert plan.records  # non-empty
        # Every record carries the wrapper's stable single-segment id.
        assert {r.seg_id for r in plan.records} == {"single"}

    def test_segmented_two_configs_split_at_mid_quarter_boundary(self):
        q = Quarter(2026, 1)
        q_start, q_end = quarter_bounds_utc(q)
        # Split mid-quarter at Zurich-local 2026-02-15 00:00.
        boundary = datetime(2026, 2, 15, tzinfo=ZoneInfo("Europe/Zurich")).astimezone(UTC)
        cfg_a = TariffConfig(
            eigenverbrauch_aktiviert=True, installierte_leistung_kwp=10.0,
            hkn_aktiviert=False, hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(base_model="fixed_flat", fixed_rp_kwh=8.0),
        )
        cfg_b = TariffConfig(
            eigenverbrauch_aktiviert=True, installierte_leistung_kwp=10.0,
            hkn_aktiviert=False, hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(base_model="fixed_flat", fixed_rp_kwh=10.0),
        )
        segments = [
            QuarterSegment(seg_id="2026-01-01", start_utc=q_start, end_utc=boundary, cfg=cfg_a),
            QuarterSegment(seg_id="2026-02-15", start_utc=boundary, end_utc=q_end, cfg=cfg_b),
        ]
        plan = compute_quarter_plan_segmented(
            q=q,
            hourly_kwh=uniform_hourly(q, 1.0),
            quarterly_price=BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=600000.0),
            monthly_prices=None,
            segments=segments,
            billing_mode=ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0,
            old_post_quarter_first_sum_chf=None,
        )
        # Both seg_ids appear in the records.
        assert {r.seg_id for r in plan.records} == {"2026-01-01", "2026-02-15"}
        # All A-segment records use 8.0 rate; all B-segment records use 10.0.
        a_rates = {r.rate_rp_kwh for r in plan.records if r.seg_id == "2026-01-01"}
        b_rates = {r.rate_rp_kwh for r in plan.records if r.seg_id == "2026-02-15"}
        assert a_rates == {8.0}
        assert b_rates == {10.0}

    def test_segmented_empty_segments_raises(self):
        q = Quarter(2026, 1)
        with pytest.raises(ValueError):
            compute_quarter_plan_segmented(
                q=q,
                hourly_kwh={},
                quarterly_price=BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=600000.0),
                monthly_prices=None,
                segments=[],
                billing_mode=ABRECHNUNGS_RHYTHMUS_QUARTAL,
                anchor_sum_chf=0.0,
                old_post_quarter_first_sum_chf=None,
            )


class TestBatchDPerHourBonusesAndHknCases:
    """v0.11.0 (Batch D) — per-hour bonus + hkn_cases evaluation by the
    decomposed `_effective_rate_breakdown_at_hour` helper. Asserts the
    4-tuple (rate, base, hkn, bonus) where rate = base + hkn + bonus.
    """

    # 2026-01-15 14:00 UTC — January is winter under standard
    # summer=[4..9]/winter=[10..3] split.
    _HOUR_WINTER = datetime(2026, 1, 15, 14, tzinfo=UTC)
    _HOUR_SUMMER = datetime(2026, 7, 15, 14, tzinfo=UTC)

    def _seasonal_classify_only(self):
        # Seasonal block with NO rate keys — purely month classification
        # for hkn_cases.when.season / bonuses.when.season.
        return {
            "summer_months": [4, 5, 6, 7, 8, 9],
            "winter_months": [10, 11, 12, 1, 2, 3],
        }

    def _cfg(self, *, rt, hkn_aktiviert=True, user_inputs=None):
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=hkn_aktiviert,
            hkn_rp_kwh_resolved=rt.hkn_rp_kwh if hkn_aktiviert else 0.0,
            resolved=rt,
            user_inputs=user_inputs or {},
        )

    def test_static_hkn_unchanged_when_no_hkn_cases(self):
        rt = _make_resolved(
            base_model="fixed_flat", fixed_rp_kwh=8.0,
            hkn_rp_kwh=2.0, federal_floor_rp_kwh=6.0,
        )
        cfg = self._cfg(rt=rt)
        rate, base, hkn, bonus = _effective_rate_breakdown_at_hour(
            cfg, 0.0, self._HOUR_WINTER
        )
        assert hkn == pytest.approx(2.0)
        assert bonus == 0.0
        assert rate == pytest.approx(base + hkn)

    def test_hkn_cases_first_match_wins_per_season(self):
        rt = _make_resolved(
            base_model="fixed_flat", fixed_rp_kwh=8.0,
            hkn_rp_kwh=1.0, federal_floor_rp_kwh=6.0,
            seasonal=self._seasonal_classify_only(),
        )
        # Patch in hkn_cases (frozen dataclass — recreate with replace).
        rt = replace(rt, hkn_cases=(
            {"when": {"season": "winter"}, "rp_kwh": 4.0},
            {"when": {"season": "summer"}, "rp_kwh": 1.5},
        ))
        cfg = self._cfg(rt=rt)
        # Winter hour → 4.0
        _, _, hkn_w, _ = _effective_rate_breakdown_at_hour(
            cfg, 0.0, self._HOUR_WINTER
        )
        # Summer hour → 1.5
        _, _, hkn_s, _ = _effective_rate_breakdown_at_hour(
            cfg, 0.0, self._HOUR_SUMMER
        )
        assert hkn_w == pytest.approx(4.0)
        assert hkn_s == pytest.approx(1.5)

    def test_hkn_cases_falls_through_to_static_on_no_match(self):
        rt = _make_resolved(
            base_model="fixed_flat", fixed_rp_kwh=8.0,
            hkn_rp_kwh=1.0, federal_floor_rp_kwh=6.0,
            seasonal=self._seasonal_classify_only(),
        )
        rt = replace(rt, hkn_cases=(
            {"when": {"user_inputs": {"supply_product": True}}, "rp_kwh": 4.0},
        ))
        cfg = self._cfg(rt=rt, user_inputs={"supply_product": False})
        _, _, hkn, _ = _effective_rate_breakdown_at_hour(
            cfg, 0.0, self._HOUR_WINTER
        )
        # No case matched — falls through to static rt.hkn_rp_kwh = 1.0.
        assert hkn == pytest.approx(1.0)

    def test_additive_bonus_adds_to_rate(self):
        rt = _make_resolved(
            base_model="fixed_flat", fixed_rp_kwh=8.0,
            hkn_rp_kwh=0.0, federal_floor_rp_kwh=6.0,
        )
        rt = replace(rt, bonuses=(
            {
                "kind": "additive_rp_kwh", "name": "TestBonus",
                "applies_when": "always", "rate_rp_kwh": 0.5,
            },
        ))
        cfg = self._cfg(rt=rt, hkn_aktiviert=False)
        rate, base, hkn, bonus = _effective_rate_breakdown_at_hour(
            cfg, 0.0, self._HOUR_WINTER
        )
        assert bonus == pytest.approx(0.5)
        assert rate == pytest.approx(base + hkn + bonus)

    def test_multiplier_pct_curtailment(self):
        # multiplier_pct=85 → rate scaled to 85%; bonus_delta = -15% × current.
        rt = _make_resolved(
            base_model="fixed_flat", fixed_rp_kwh=10.0,
            hkn_rp_kwh=0.0, federal_floor_rp_kwh=0.0,
        )
        rt = replace(rt, bonuses=(
            {
                "kind": "multiplier_pct", "name": "TOP-40 curtailment",
                "applies_when": "always", "multiplier_pct": 85.0,
            },
        ))
        cfg = self._cfg(rt=rt, hkn_aktiviert=False)
        rate, base, _, bonus = _effective_rate_breakdown_at_hour(
            cfg, 0.0, self._HOUR_WINTER
        )
        # base after floor = 10.0; bonus delta = 10.0 * (0.85 - 1) = -1.5.
        assert base == pytest.approx(10.0)
        assert bonus == pytest.approx(-1.5)
        assert rate == pytest.approx(8.5)

    def test_optin_bonus_without_when_clause_skipped(self):
        # opt_in bonuses without a when-clause cannot be gated → never apply.
        rt = _make_resolved(
            base_model="fixed_flat", fixed_rp_kwh=8.0,
            hkn_rp_kwh=0.0, federal_floor_rp_kwh=6.0,
        )
        rt = replace(rt, bonuses=(
            {
                "kind": "additive_rp_kwh", "name": "OptIn no clause",
                "applies_when": "opt_in", "rate_rp_kwh": 0.5,
            },
        ))
        cfg = self._cfg(rt=rt, hkn_aktiviert=False)
        _, _, _, bonus = _effective_rate_breakdown_at_hour(
            cfg, 0.0, self._HOUR_WINTER
        )
        assert bonus == 0.0

    def test_optin_bonus_with_when_user_inputs_applies_on_match(self):
        rt = _make_resolved(
            base_model="fixed_flat", fixed_rp_kwh=8.0,
            hkn_rp_kwh=0.0, federal_floor_rp_kwh=6.0,
        )
        rt = replace(rt, bonuses=(
            {
                "kind": "additive_rp_kwh", "name": "OptIn with toggle",
                "applies_when": "opt_in",
                "when": {"user_inputs": {"top40_enrolled": True}},
                "rate_rp_kwh": 1.0,
            },
        ))
        cfg_off = self._cfg(
            rt=rt, hkn_aktiviert=False, user_inputs={"top40_enrolled": False}
        )
        cfg_on = self._cfg(
            rt=rt, hkn_aktiviert=False, user_inputs={"top40_enrolled": True}
        )
        _, _, _, b_off = _effective_rate_breakdown_at_hour(
            cfg_off, 0.0, self._HOUR_WINTER
        )
        _, _, _, b_on = _effective_rate_breakdown_at_hour(
            cfg_on, 0.0, self._HOUR_WINTER
        )
        assert b_off == 0.0
        assert b_on == pytest.approx(1.0)


# ----- v0.23.0 — schema 1.6.0 fixed_seasonal + tier-level bonuses ---------


class TestBonusConcatRateThenTier:
    """v0.22.0 — `_resolve_bonuses_for_hour_detailed` iterates rate-level
    bonuses first, tier-level second. Multiplier_pct stacks compound in
    iteration order (rate-then-tier)."""

    def _cfg(self, *, bonuses=None, tier_bonuses=None, hkn=0.0):
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=hkn,
            resolved=_make_resolved(
                base_model="rmp_quartal",
                bonuses=bonuses,
                tier_bonuses=tier_bonuses,
            ),
        )

    def test_rate_only_bonuses_apply(self):
        cfg = self._cfg(
            bonuses=({"kind": "additive_rp_kwh", "name": "R", "rate_rp_kwh": 0.5},),
            tier_bonuses=None,
        )
        total, detail = _resolve_bonuses_for_hour_detailed(cfg, None, 10.0, 0.0)
        assert total == pytest.approx(0.5)
        assert len(detail) == 1
        assert detail[0]["name"] == "R"

    def test_tier_only_bonuses_apply(self):
        cfg = self._cfg(
            bonuses=None,
            tier_bonuses=({"kind": "additive_rp_kwh", "name": "T", "rate_rp_kwh": 0.3},),
        )
        total, detail = _resolve_bonuses_for_hour_detailed(cfg, None, 10.0, 0.0)
        assert total == pytest.approx(0.3)
        assert detail[0]["name"] == "T"

    def test_rate_and_tier_concat_in_order(self):
        cfg = self._cfg(
            bonuses=({"kind": "additive_rp_kwh", "name": "R", "rate_rp_kwh": 0.5},),
            tier_bonuses=({"kind": "additive_rp_kwh", "name": "T", "rate_rp_kwh": 0.3},),
        )
        total, detail = _resolve_bonuses_for_hour_detailed(cfg, None, 10.0, 0.0)
        assert total == pytest.approx(0.8)
        assert [d["name"] for d in detail] == ["R", "T"]   # rate first, tier after

    def test_multiplier_pct_stacks_multiplicatively(self):
        # Rate +5% then Tier +3% on a base of (10 + 0): expect ~+8.15%.
        # Iteration 1 (rate, mp=105): current=10, contribution=10*0.05=0.5; acc=0.5
        # Iteration 2 (tier, mp=103): current=10+0.5=10.5, contribution=10.5*0.03=0.315; acc=0.815
        cfg = self._cfg(
            bonuses=({"kind": "multiplier_pct", "name": "R", "multiplier_pct": 105.0},),
            tier_bonuses=({"kind": "multiplier_pct", "name": "T", "multiplier_pct": 103.0},),
        )
        total, _detail = _resolve_bonuses_for_hour_detailed(cfg, None, 10.0, 0.0)
        assert total == pytest.approx(0.815, rel=1e-6)

    def test_tier_bonuses_empty_handled(self):
        cfg = self._cfg(
            bonuses=({"kind": "additive_rp_kwh", "name": "R", "rate_rp_kwh": 0.5},),
            tier_bonuses=(),
        )
        total, detail = _resolve_bonuses_for_hour_detailed(cfg, None, 10.0, 0.0)
        assert total == pytest.approx(0.5)
        assert len(detail) == 1


# ----- v0.23.0 — schema 1.6.0 fixed_seasonal dispatch ---------------------


class TestFixedSeasonalRate:
    """v0.23.0 — base_model "fixed_seasonal" reads tier-level
    summer_rp_kwh / winter_rp_kwh per hour, classified by the tier-level
    summer_months / winter_months calendar (Q1 decision)."""

    _SEASONAL = {
        "summer_months": [4, 5, 6, 7, 8, 9],
        "winter_months": [10, 11, 12, 1, 2, 3],
        "summer_rp_kwh": 20.0,
        "winter_rp_kwh": 30.0,
    }

    def _cfg(self, *, hkn=0.0, bonuses=None, tier_bonuses=None):
        return TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=10.0,
            hkn_aktiviert=hkn > 0,
            hkn_rp_kwh_resolved=hkn,
            resolved=_make_resolved(
                base_model="fixed_seasonal",
                seasonal=self._SEASONAL,
                hkn_rp_kwh=hkn,
                federal_floor_rp_kwh=None,
                bonuses=bonuses,
                tier_bonuses=tier_bonuses,
            ),
        )

    def test_summer_uses_summer_rp_kwh(self):
        rate = _effective_rate_at_hour(self._cfg(), 0.0, _zurich_utc(2026, 7, 15, 12))
        assert rate == pytest.approx(20.0)

    def test_winter_uses_winter_rp_kwh(self):
        rate = _effective_rate_at_hour(self._cfg(), 0.0, _zurich_utc(2026, 1, 15, 12))
        assert rate == pytest.approx(30.0)

    def test_summer_with_hkn_optin(self):
        rate = _effective_rate_at_hour(self._cfg(hkn=15.0), 0.0, _zurich_utc(2026, 7, 15, 12))
        assert rate == pytest.approx(35.0)

    def test_winter_bonus_applies(self):
        # Winter-gated +15 Rp/kWh additive bonus on top of base+hkn.
        winter_bonus = (
            {"kind": "additive_rp_kwh", "name": "Spezialbonus",
             "rate_rp_kwh": 15.0, "when": {"season": "winter"}},
        )
        cfg = self._cfg(hkn=15.0, tier_bonuses=winter_bonus)
        rate, base, hkn, bonus = _effective_rate_breakdown_at_hour(
            cfg, 0.0, _zurich_utc(2026, 1, 15, 12)
        )
        assert rate == pytest.approx(60.0)
        assert base == pytest.approx(30.0)
        assert hkn == pytest.approx(15.0)
        assert bonus == pytest.approx(15.0)

    def test_summer_no_winter_bonus(self):
        winter_bonus = (
            {"kind": "additive_rp_kwh", "name": "Spezialbonus",
             "rate_rp_kwh": 15.0, "when": {"season": "winter"}},
        )
        cfg = self._cfg(hkn=15.0, tier_bonuses=winter_bonus)
        rate = _effective_rate_at_hour(cfg, 0.0, _zurich_utc(2026, 7, 15, 12))
        assert rate == pytest.approx(35.0)

    def test_season_at_uses_tier_seasonal_block(self):
        from custom_components.bfe_rueckliefertarif.importer import _season_at
        rt = _make_resolved(base_model="fixed_seasonal", seasonal=self._SEASONAL)
        # Resolver writes the tier's calendar into `rt.seasonal`, so
        # _season_at transparently classifies against tier-level months.
        assert _season_at(rt, _zurich_utc(2026, 7, 15, 12)) == "summer"
        assert _season_at(rt, _zurich_utc(2026, 1, 15, 12)) == "winter"

