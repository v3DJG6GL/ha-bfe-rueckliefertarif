"""Tests for tariff.py national-law tables and Rückliefervergütung formula."""

from __future__ import annotations

import pytest

from custom_components.bfe_rueckliefertarif.tariff import (
    Segment,
    anrechenbarkeitsgrenze_rp_kwh,
    chf_per_mwh_to_rp_per_kwh,
    effective_rp_kwh,
    mindestverguetung_rp_kwh,
)


class TestMindestverguetung:
    def test_small_mit_ev_is_6_rappen(self):
        assert mindestverguetung_rp_kwh(Segment.SMALL_MIT_EV, 10) == 6.00

    def test_small_ohne_ev_is_6_rappen(self):
        assert mindestverguetung_rp_kwh(Segment.SMALL_OHNE_EV, 10) == 6.00

    def test_mid_mit_ev_degressive_30kw(self):
        assert mindestverguetung_rp_kwh(Segment.MID_MIT_EV, 30) == 6.00

    def test_mid_mit_ev_degressive_60kw(self):
        assert mindestverguetung_rp_kwh(Segment.MID_MIT_EV, 60) == 3.00

    def test_mid_mit_ev_degressive_90kw(self):
        assert mindestverguetung_rp_kwh(Segment.MID_MIT_EV, 90) == 2.00

    def test_large_mit_ev_degressive_150kw_boundary(self):
        # 180/150 = 1.20, boundary of LARGE_MIT_EV
        assert mindestverguetung_rp_kwh(Segment.LARGE_MIT_EV, 149) == round(180 / 149, 4)

    def test_mid_ohne_ev_flat(self):
        assert mindestverguetung_rp_kwh(Segment.MID_OHNE_EV, 60) == 6.20

    def test_large_ohne_ev_flat(self):
        assert mindestverguetung_rp_kwh(Segment.LARGE_OHNE_EV, 120) == 6.20

    def test_xl_mit_ev_no_floor(self):
        assert mindestverguetung_rp_kwh(Segment.XL_MIT_EV, 200) is None

    def test_xl_ohne_ev_no_floor(self):
        assert mindestverguetung_rp_kwh(Segment.XL_OHNE_EV, 300) is None

    def test_zero_kw_rejected_in_degressive(self):
        with pytest.raises(ValueError):
            mindestverguetung_rp_kwh(Segment.MID_MIT_EV, 0)


class TestAnrechenbarkeitsgrenze:
    def test_small_mit_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.SMALL_MIT_EV) == 10.96

    def test_mid_mit_ev_still_under_100kw(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.MID_MIT_EV) == 10.96

    def test_large_mit_ev_at_or_over_100kw(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.LARGE_MIT_EV) == 7.20

    def test_xl_mit_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.XL_MIT_EV) == 7.20

    def test_small_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.SMALL_OHNE_EV) == 8.20

    def test_mid_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.MID_OHNE_EV) == 8.20

    def test_large_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.LARGE_OHNE_EV) == 5.40

    def test_xl_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(Segment.XL_OHNE_EV) == 5.40


class TestEffectiveOhneObergrenze:
    """Vergütungs-Obergrenze=False — base + HKN paid additively, no upper cap."""

    def test_normal_market_base_plus_hkn(self):
        # Q1 2026 BFE reference 10.266 + HKN 3 → 13.266 (no cap)
        rp = effective_rp_kwh(
            10.266, Segment.SMALL_MIT_EV, 10, 3.0, verguetungs_obergrenze=False
        )
        assert rp == pytest.approx(13.266)

    def test_low_reference_lifted_to_floor(self):
        # 3.0 < floor 6.0 → base raised to floor; HKN added
        rp = effective_rp_kwh(
            3.0, Segment.SMALL_MIT_EV, 10, 2.0, verguetungs_obergrenze=False
        )
        assert rp == pytest.approx(8.0)  # 6.0 + 2.0

    def test_iwb_fixed_paid_in_full(self):
        # IWB 12.95 (HKN bundled, hkn=0) — additive utility, no cap → 12.95
        rp = effective_rp_kwh(
            12.95, Segment.SMALL_MIT_EV, 10, 0.0, verguetungs_obergrenze=False
        )
        assert rp == pytest.approx(12.95)

    def test_ewz_base_plus_hkn_full(self):
        # ewz: fixed 7.91 + HKN 5.0 → 12.91, no cap
        rp = effective_rp_kwh(
            7.91, Segment.SMALL_MIT_EV, 10, 5.0, verguetungs_obergrenze=False
        )
        assert rp == pytest.approx(12.91)

    def test_aew_inclusive_flat(self):
        # AEW 8.20 includes HKN, hkn=0 → 8.20
        rp = effective_rp_kwh(
            8.2, Segment.SMALL_MIT_EV, 10, 0.0, verguetungs_obergrenze=False
        )
        assert rp == pytest.approx(8.2)

    def test_xl_no_floor_low_reference_stays_low(self):
        rp = effective_rp_kwh(
            3.0, Segment.XL_OHNE_EV, 200, 0.0, verguetungs_obergrenze=False
        )
        assert rp == pytest.approx(3.0)


class TestEffectiveMitObergrenze:
    """Vergütungs-Obergrenze=True — EKZ-style cap with two sub-rules."""

    def test_q1_2026_ekz_no_hkn_below_cap(self):
        # 10.266 < cap 10.96, no HKN → 10.266 (cap not engaged)
        rp = effective_rp_kwh(
            10.266, Segment.SMALL_MIT_EV, 10, 0.0, verguetungs_obergrenze=True
        )
        assert rp == pytest.approx(10.266)

    def test_q1_2026_ekz_with_hkn_hits_cap_hkn_reduced(self):
        # 10.266 + 3.0 = 13.266 > cap 10.96 → reduced to 10.96 (HKN-Vergütung effectively cut)
        rp = effective_rp_kwh(
            10.266, Segment.SMALL_MIT_EV, 10, 3.0, verguetungs_obergrenze=True
        )
        assert rp == 10.96

    def test_base_alone_at_cap_hkn_forfeited(self):
        # Base = cap exactly → HKN forfeited per EKZ clause 2 (no Anspruch on HKN)
        rp = effective_rp_kwh(
            10.96, Segment.SMALL_MIT_EV, 10, 3.0, verguetungs_obergrenze=True
        )
        assert rp == 10.96

    def test_base_alone_above_cap_hkn_forfeited_full_base_paid(self):
        # Base 12.0 > cap 10.96 → producer gets base 12.0 (NOT capped to 10.96), zero HKN
        rp = effective_rp_kwh(
            12.0, Segment.SMALL_MIT_EV, 10, 3.0, verguetungs_obergrenze=True
        )
        assert rp == pytest.approx(12.0)

    def test_xl_mit_ev_cap_binds_at_7_20(self):
        rp = effective_rp_kwh(
            7.0, Segment.XL_MIT_EV, 200, 3.0, verguetungs_obergrenze=True
        )
        # 7.0 + 3.0 = 10.0 > cap 7.20 → reduced to 7.20
        assert rp == 7.20

    def test_low_reference_lifted_to_floor_then_hkn(self):
        # 3.0 < floor 6.0 → base 6.0; 6.0 + 3.0 = 9.0 < cap 10.96 → 9.0
        rp = effective_rp_kwh(
            3.0, Segment.SMALL_MIT_EV, 10, 3.0, verguetungs_obergrenze=True
        )
        assert rp == pytest.approx(9.0)


class TestUnitConversion:
    def test_bfe_to_rappen(self):
        # Q1 2026 PV: 102.66 CHF/MWh → 10.266 Rp/kWh
        assert chf_per_mwh_to_rp_per_kwh(102.66) == pytest.approx(10.266)

    def test_zero(self):
        assert chf_per_mwh_to_rp_per_kwh(0.0) == 0.0
