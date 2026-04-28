"""Tests for tariff.py — federal floor + cap math via JSON-backed rule lookups."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from custom_components.bfe_rueckliefertarif.tariff import (
    DEFAULT_CAP_RULES,
    anrechenbarkeitsgrenze_rp_kwh,
    chf_per_mwh_to_rp_per_kwh,
    classify_ht,
    classify_season,
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


def _utc_for_zurich(year: int, month: int, day: int, hour: int) -> datetime:
    """Build a UTC timestamp matching the given Zurich local wall-clock hour.

    Zurich is UTC+1 in winter (CET) and UTC+2 in summer (CEST). Tests pass
    in local-clock terms and convert here so the assertions read naturally.
    """
    local = datetime(year, month, day, hour, tzinfo=ZoneInfo("Europe/Zurich"))
    return local.astimezone(UTC)


class TestClassifyHT:
    """Per-hour HT/NT classification for fixed_ht_nt utilities."""

    # EKZ producer-side (verified against ekz-rueckliefertarife-2025.pdf):
    # weekdays 07:00–20:00 = HT; Sa/Su all NT.
    EKZ_WINDOW = {"mofr": [7, 20], "sa": None, "su": None}

    # Hypothetical consumer-side window (Sat morning HT). Used to confirm
    # the function correctly walks the sa entry when present.
    SAT_WINDOW = {"mofr": [7, 20], "sa": [7, 13], "su": None}

    def test_weekday_inside_ht_window(self):
        # Wed 14:00 Zurich → HT
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 14), self.EKZ_WINDOW) is True

    def test_weekday_before_ht_window(self):
        # Wed 06:00 Zurich → NT (before HT start)
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 6), self.EKZ_WINDOW) is False

    def test_weekday_at_ht_start_inclusive(self):
        # Wed 07:00 Zurich → HT (start inclusive)
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 7), self.EKZ_WINDOW) is True

    def test_weekday_at_ht_end_exclusive(self):
        # Wed 20:00 Zurich → NT (end exclusive — half-open window)
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 20), self.EKZ_WINDOW) is False

    def test_weekday_after_ht(self):
        # Wed 22:00 Zurich → NT
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 22), self.EKZ_WINDOW) is False

    def test_friday_still_uses_mofr_window(self):
        # Fri 14:00 Zurich → HT (Mo-Fr covers Friday)
        assert classify_ht(_utc_for_zurich(2025, 1, 17, 14), self.EKZ_WINDOW) is True

    def test_saturday_all_nt_when_sa_is_none(self):
        # Sat 12:00 Zurich → NT (sa=None)
        assert classify_ht(_utc_for_zurich(2025, 1, 18, 12), self.EKZ_WINDOW) is False

    def test_saturday_morning_ht_when_sa_window_present(self):
        # Sat 10:00 Zurich → HT (sa=[7,13])
        assert classify_ht(_utc_for_zurich(2025, 1, 18, 10), self.SAT_WINDOW) is True

    def test_saturday_afternoon_nt_when_sa_window_ends_at_13(self):
        # Sat 14:00 Zurich → NT (after sa end)
        assert classify_ht(_utc_for_zurich(2025, 1, 18, 14), self.SAT_WINDOW) is False

    def test_sunday_always_nt(self):
        assert classify_ht(_utc_for_zurich(2025, 1, 19, 12), self.EKZ_WINDOW) is False

    def test_dst_spring_forward_local_hour_correct(self):
        # CH DST transitions land on Sundays (last Sun of Mar / Oct), so
        # the EKZ producer window (Sun all-NT) can't be used to probe DST
        # classification. Use a window that's HT on Sundays too — we're
        # testing the timezone math, not the day-of-week mapping.
        sun_ht = {"mofr": [7, 20], "sa": [7, 20], "su": [7, 20]}
        # 2025-03-30 CEST starts; 02:00 local skipped to 03:00. 14:00
        # local on the transition Sunday must still be HT.
        assert classify_ht(_utc_for_zurich(2025, 3, 30, 14), sun_ht) is True
        # And the Monday after (2025-03-31, weekday) using the EKZ window:
        assert classify_ht(_utc_for_zurich(2025, 3, 31, 14), self.EKZ_WINDOW) is True

    def test_dst_fall_back_local_hour_correct(self):
        sun_ht = {"mofr": [7, 20], "sa": [7, 20], "su": [7, 20]}
        # 2025-10-26 CET resumes; 03:00 local rolls back to 02:00. 14:00
        # local on the transition Sunday must still be HT.
        assert classify_ht(_utc_for_zurich(2025, 10, 26, 14), sun_ht) is True
        # And the Monday after (2025-10-27, weekday) using the EKZ window:
        assert classify_ht(_utc_for_zurich(2025, 10, 27, 14), self.EKZ_WINDOW) is True

    def test_winter_vs_summer_same_local_hour_same_classification(self):
        # 14:00 Zurich in January (UTC+1) and July (UTC+2) both HT —
        # confirms the function isn't accidentally using UTC hour.
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 14), self.EKZ_WINDOW) is True
        assert classify_ht(_utc_for_zurich(2025, 7, 15, 14), self.EKZ_WINDOW) is True

    def test_ht_window_none_returns_false(self):
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 14), None) is False

    def test_ht_window_empty_dict_returns_false(self):
        assert classify_ht(_utc_for_zurich(2025, 1, 15, 14), {}) is False

    def test_missing_day_key_treated_as_all_nt(self):
        # Window only specifies mofr; sa/su keys absent → those days NT.
        partial = {"mofr": [7, 20]}
        assert classify_ht(_utc_for_zurich(2025, 1, 18, 12), partial) is False  # Sat
        assert classify_ht(_utc_for_zurich(2025, 1, 19, 12), partial) is False  # Sun


class TestClassifySeason:
    """Summer/winter classification by Zurich-local month."""

    # Canonical Swiss split (Apr–Sep summer, Oct–Mar winter).
    SUMMER_CH = [4, 5, 6, 7, 8, 9]
    WINTER_CH = [10, 11, 12, 1, 2, 3]

    def test_july_midday_is_summer(self):
        assert (
            classify_season(_utc_for_zurich(2025, 7, 15, 12), self.SUMMER_CH, self.WINTER_CH)
            == "summer"
        )

    def test_january_is_winter(self):
        assert (
            classify_season(_utc_for_zurich(2025, 1, 15, 12), self.SUMMER_CH, self.WINTER_CH)
            == "winter"
        )

    def test_april_first_midnight_is_summer(self):
        # First instant of summer per the canonical split.
        assert (
            classify_season(_utc_for_zurich(2025, 4, 1, 0), self.SUMMER_CH, self.WINTER_CH)
            == "summer"
        )

    def test_september_last_evening_is_summer(self):
        assert (
            classify_season(_utc_for_zurich(2025, 9, 30, 23), self.SUMMER_CH, self.WINTER_CH)
            == "summer"
        )

    def test_october_first_midnight_is_winter(self):
        # First instant of winter — boundary belongs to the new season.
        assert (
            classify_season(_utc_for_zurich(2025, 10, 1, 0), self.SUMMER_CH, self.WINTER_CH)
            == "winter"
        )

    def test_dst_fall_back_day_is_winter(self):
        # 2025-10-26 is the last Sunday of October (CET resumes). Month is
        # 10 → winter. Confirms the function reads Zurich-local month, not UTC.
        assert (
            classify_season(_utc_for_zurich(2025, 10, 26, 12), self.SUMMER_CH, self.WINTER_CH)
            == "winter"
        )

    def test_month_in_neither_list_raises(self):
        # Asymmetric split that leaves several months uncovered.
        with pytest.raises(ValueError, match="neither"):
            classify_season(_utc_for_zurich(2025, 4, 1, 12), [5, 6, 7, 8], [11, 12, 1, 2])

    def test_alternate_split_works(self):
        # Hypothetical utility that uses Mar-Aug as summer.
        summer = [3, 4, 5, 6, 7, 8]
        winter = [9, 10, 11, 12, 1, 2]
        assert (
            classify_season(_utc_for_zurich(2025, 3, 15, 12), summer, winter) == "summer"
        )
        assert (
            classify_season(_utc_for_zurich(2025, 9, 15, 12), summer, winter) == "winter"
        )


class TestUnitConversion:
    def test_bfe_to_rappen(self):
        # Q1 2026 PV: 102.66 CHF/MWh → 10.266 Rp/kWh
        assert chf_per_mwh_to_rp_per_kwh(102.66) == pytest.approx(10.266)

    def test_zero(self):
        assert chf_per_mwh_to_rp_per_kwh(0.0) == 0.0
