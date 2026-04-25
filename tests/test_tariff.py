"""Tests for tariff.py — federal floor + cap math via JSON-backed rule lookups."""

from __future__ import annotations

import pytest

from custom_components.bfe_rueckliefertarif.tariff import (
    DEFAULT_CAP_RULES,
    anrechenbarkeitsgrenze_rp_kwh,
    chf_per_mwh_to_rp_per_kwh,
    effective_rp_kwh,
    mindestverguetung_rp_kwh,
)

# Federal floor rules per EnV Art. 12 Abs. 1bis (AS 2025 138, in force
# 1.1.2026). Same shape as the records carried by tariffs.json's
# `federal_minimum.rules`. Hardcoded here so the tests aren't coupled to
# the bundled JSON file.
FED_RULES_2026 = [
    {"kw_min": 0,   "kw_max": 30,   "self_consumption": None,  "min_rp_kwh": 6.00},
    {"kw_min": 30,  "kw_max": 150,  "self_consumption": True,
     "formula": "180/kw",
     "min_rp_kwh_at_kw_min": 6.00, "min_rp_kwh_at_kw_max": 1.20},
    {"kw_min": 30,  "kw_max": 150,  "self_consumption": False, "min_rp_kwh": 6.20},
    {"kw_min": 150, "kw_max": None, "self_consumption": None,  "min_rp_kwh": None},
]


class TestMindestverguetung:
    def test_small_mit_ev_is_6_rappen(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=10, eigenverbrauch=True) == 6.00

    def test_small_ohne_ev_is_6_rappen(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=10, eigenverbrauch=False) == 6.00

    def test_mid_mit_ev_degressive_30kw(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=30, eigenverbrauch=True) == 6.00

    def test_mid_mit_ev_degressive_60kw(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=60, eigenverbrauch=True) == 3.00

    def test_mid_mit_ev_degressive_90kw(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=90, eigenverbrauch=True) == 2.00

    def test_large_mit_ev_degressive_149kw(self):
        # 180/149 ~ 1.2081 — still in the mid_mit_ev (30–<150 kW) band.
        assert (
            mindestverguetung_rp_kwh(FED_RULES_2026, kw=149, eigenverbrauch=True)
            == round(180 / 149, 4)
        )

    def test_mid_ohne_ev_flat(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=60, eigenverbrauch=False) == 6.20

    def test_large_ohne_ev_flat(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=120, eigenverbrauch=False) == 6.20

    def test_xl_mit_ev_no_floor(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=200, eigenverbrauch=True) is None

    def test_xl_ohne_ev_no_floor(self):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=300, eigenverbrauch=False) is None

    def test_zero_kw_in_full_ruleset_returns_small_band_floor(self):
        # 0 kW satisfies the <30 kW flat rule (kw_min=0, kw_max=30) and
        # returns 6.00. Divide-by-zero is only reachable when caller restricts
        # rules to the degressive band — see test_tariffs_db.test_degressive_zero_kw_raises.
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=0, eigenverbrauch=True) == 6.00


class TestAnrechenbarkeitsgrenze:
    def test_small_mit_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=10, eigenverbrauch=True) == 10.96

    def test_mid_mit_ev_still_under_100kw(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=60, eigenverbrauch=True) == 10.96

    def test_large_mit_ev_at_or_over_100kw(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=120, eigenverbrauch=True) == 7.20

    def test_xl_mit_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=200, eigenverbrauch=True) == 7.20

    def test_small_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=10, eigenverbrauch=False) == 8.20

    def test_mid_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=60, eigenverbrauch=False) == 8.20

    def test_large_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=120, eigenverbrauch=False) == 5.40

    def test_xl_ohne_ev(self):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=300, eigenverbrauch=False) == 5.40


def _floor(kw: float, ev: bool) -> float | None:
    return mindestverguetung_rp_kwh(FED_RULES_2026, kw, ev)


def _cap(kw: float, ev: bool) -> float | None:
    return anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw, ev)


class TestEffectiveOhneObergrenze:
    """cap_mode=False — base + HKN paid additively, no upper cap."""

    def test_normal_market_base_plus_hkn(self):
        # Q1 2026 BFE reference 10.266 + HKN 3 → 13.266 (no cap)
        rp = effective_rp_kwh(
            10.266, 3.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=None, cap_mode=False,
        )
        assert rp == pytest.approx(13.266)

    def test_low_reference_lifted_to_floor(self):
        # 3.0 < floor 6.0 → base raised to floor; HKN added
        rp = effective_rp_kwh(
            3.0, 2.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=None, cap_mode=False,
        )
        assert rp == pytest.approx(8.0)

    def test_iwb_fixed_paid_in_full(self):
        # IWB 12.95 (HKN bundled, hkn=0) — additive utility, no cap → 12.95
        rp = effective_rp_kwh(
            12.95, 0.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=None, cap_mode=False,
        )
        assert rp == pytest.approx(12.95)

    def test_ewz_base_plus_hkn_full(self):
        # ewz: fixed 7.91 + HKN 5.0 → 12.91, no cap
        rp = effective_rp_kwh(
            7.91, 5.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=None, cap_mode=False,
        )
        assert rp == pytest.approx(12.91)

    def test_aew_inclusive_flat(self):
        # AEW 8.20 includes HKN, hkn=0 → 8.20
        rp = effective_rp_kwh(
            8.2, 0.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=None, cap_mode=False,
        )
        assert rp == pytest.approx(8.2)

    def test_xl_no_floor_low_reference_stays_low(self):
        rp = effective_rp_kwh(
            3.0, 0.0,
            federal_floor_rp_kwh=_floor(200, False),
            cap_rp_kwh=None, cap_mode=False,
        )
        assert rp == pytest.approx(3.0)


class TestEffectiveMitObergrenze:
    """cap_mode=True — EKZ-style cap with two sub-rules."""

    def test_q1_2026_ekz_no_hkn_below_cap(self):
        # 10.266 < cap 10.96, no HKN → 10.266 (cap not engaged)
        rp = effective_rp_kwh(
            10.266, 0.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=_cap(10, True), cap_mode=True,
        )
        assert rp == pytest.approx(10.266)

    def test_q1_2026_ekz_with_hkn_hits_cap_hkn_reduced(self):
        # 10.266 + 3.0 = 13.266 > cap 10.96 → reduced to 10.96
        rp = effective_rp_kwh(
            10.266, 3.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=_cap(10, True), cap_mode=True,
        )
        assert rp == 10.96

    def test_base_alone_at_cap_hkn_forfeited(self):
        # Base = cap exactly → HKN forfeited per EKZ clause 2.
        rp = effective_rp_kwh(
            10.96, 3.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=_cap(10, True), cap_mode=True,
        )
        assert rp == 10.96

    def test_base_alone_above_cap_hkn_forfeited_full_base_paid(self):
        # Base 12.0 > cap 10.96 → producer gets base 12.0 (NOT capped to 10.96), zero HKN.
        rp = effective_rp_kwh(
            12.0, 3.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=_cap(10, True), cap_mode=True,
        )
        assert rp == pytest.approx(12.0)

    def test_xl_mit_ev_cap_binds_at_7_20(self):
        rp = effective_rp_kwh(
            7.0, 3.0,
            federal_floor_rp_kwh=_floor(200, True),
            cap_rp_kwh=_cap(200, True), cap_mode=True,
        )
        # 7.0 + 3.0 = 10.0 > cap 7.20 → reduced to 7.20.
        assert rp == 7.20

    def test_low_reference_lifted_to_floor_then_hkn(self):
        # 3.0 < floor 6.0 → base 6.0; 6.0 + 3.0 = 9.0 < cap 10.96 → 9.0.
        rp = effective_rp_kwh(
            3.0, 3.0,
            federal_floor_rp_kwh=_floor(10, True),
            cap_rp_kwh=_cap(10, True), cap_mode=True,
        )
        assert rp == pytest.approx(9.0)


class TestUnitConversion:
    def test_bfe_to_rappen(self):
        # Q1 2026 PV: 102.66 CHF/MWh → 10.266 Rp/kWh
        assert chf_per_mwh_to_rp_per_kwh(102.66) == pytest.approx(10.266)

    def test_zero(self):
        assert chf_per_mwh_to_rp_per_kwh(0.0) == 0.0
