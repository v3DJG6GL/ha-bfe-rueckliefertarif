"""Tests for tariff.py national-law tables and formulas."""

from __future__ import annotations

import pytest

from custom_components.bfe_rueckliefertarif.tariff import (
    Segment,
    anrechenbarkeitsgrenze_rp_kwh,
    chf_per_mwh_to_rp_per_kwh,
    effective_rp_kwh_fixed,
    effective_rp_kwh_rmp,
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


class TestEffectiveRmp:
    def test_q1_2026_ekz_small_mit_ev_no_hkn(self):
        # Q1 2026 BFE reference = 102.66 CHF/MWh = 10.266 Rp/kWh
        # SMALL_MIT_EV: floor 6.00, cap 10.96
        # max(10.266, 6.00) = 10.266; no HKN; min(10.266, 10.96) = 10.266
        rp = effective_rp_kwh_rmp(10.266, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=0.0)
        assert rp == pytest.approx(10.266)

    def test_q1_2026_ekz_small_mit_ev_with_hkn_hits_cap(self):
        # 10.266 + 3.0 = 13.266, but cap = 10.96 → 10.96
        rp = effective_rp_kwh_rmp(10.266, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=3.0)
        assert rp == 10.96

    def test_low_reference_hits_floor(self):
        # Reference 3.0 Rp/kWh, floor 6.00 for SMALL → raised to 6.00
        rp = effective_rp_kwh_rmp(3.0, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=0.0)
        assert rp == 6.00

    def test_xl_segment_no_floor_low_ref_stays_low(self):
        rp = effective_rp_kwh_rmp(3.0, Segment.XL_OHNE_EV, 200, hkn_bonus_rp_kwh=0.0)
        assert rp == 3.0  # no floor, no HKN, well under 5.40 cap

    def test_xl_mit_ev_cap_binds(self):
        rp = effective_rp_kwh_rmp(9.0, Segment.XL_MIT_EV, 200, hkn_bonus_rp_kwh=0.0)
        assert rp == 7.20  # XL_MIT_EV cap


class TestEffectiveFixed:
    def test_iwb_14_capped(self):
        # IWB pays 14.0 flat; SMALL_MIT_EV cap 10.96
        rp = effective_rp_kwh_fixed(14.0, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=0.0)
        assert rp == 10.96

    def test_sig_10_96_exactly_at_cap(self):
        rp = effective_rp_kwh_fixed(10.96, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=0.0)
        assert rp == 10.96

    def test_aew_8_2_inclusive(self):
        # AEW pays 8.2 HKN-inclusive → hkn_bonus 0
        rp = effective_rp_kwh_fixed(8.2, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=0.0)
        assert rp == pytest.approx(8.2)

    def test_ewz_12_91_capped(self):
        rp = effective_rp_kwh_fixed(12.91, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=0.0)
        assert rp == 10.96

    def test_fixed_below_floor_raised_to_floor(self):
        # Shouldn't happen in practice (utilities never pay below legal minimum), but covered
        rp = effective_rp_kwh_fixed(3.0, Segment.SMALL_MIT_EV, 10, hkn_bonus_rp_kwh=0.0)
        assert rp == 6.00


class TestUnitConversion:
    def test_bfe_to_rappen(self):
        # Q1 2026 PV: 102.66 CHF/MWh → 10.266 Rp/kWh
        assert chf_per_mwh_to_rp_per_kwh(102.66) == pytest.approx(10.266)

    def test_zero(self):
        assert chf_per_mwh_to_rp_per_kwh(0.0) == 0.0
