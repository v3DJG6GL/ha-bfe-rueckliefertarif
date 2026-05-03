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
    @pytest.mark.parametrize(
        ("kw", "ev", "expected"),
        [
            pytest.param(10, True, 6.00, id="small_mit_ev"),
            pytest.param(10, False, 6.00, id="small_ohne_ev"),
            pytest.param(30, True, 6.00, id="mid_mit_ev_30kw"),
            pytest.param(60, True, 3.00, id="mid_mit_ev_60kw"),
            pytest.param(90, True, 2.00, id="mid_mit_ev_90kw"),
            # 180/149 ~ 1.2081 — still in the mid_mit_ev (30–<150 kW) band.
            pytest.param(149, True, round(180 / 149, 4), id="mid_mit_ev_149kw"),
            pytest.param(60, False, 6.20, id="mid_ohne_ev_flat"),
            pytest.param(120, False, 6.20, id="large_ohne_ev_flat"),
            pytest.param(200, True, None, id="xl_mit_ev_no_floor"),
            pytest.param(300, False, None, id="xl_ohne_ev_no_floor"),
            # 0 kW satisfies the <30 kW flat rule (kw_min=0, kw_max=30) and
            # returns 6.00. Divide-by-zero is only reachable when caller restricts
            # rules to the degressive band — see test_tariffs_db.test_degressive_zero_kw_raises.
            pytest.param(0, True, 6.00, id="zero_kw_in_full_ruleset"),
        ],
    )
    def test_mindestverguetung(self, kw, ev, expected):
        assert mindestverguetung_rp_kwh(FED_RULES_2026, kw=kw, eigenverbrauch=ev) == expected


class TestAnrechenbarkeitsgrenze:
    @pytest.mark.parametrize(
        ("kw", "ev", "expected"),
        [
            pytest.param(10, True, 10.96, id="small_mit_ev"),
            pytest.param(60, True, 10.96, id="mid_mit_ev_under_100kw"),
            pytest.param(120, True, 7.20, id="large_mit_ev_at_or_over_100kw"),
            pytest.param(200, True, 7.20, id="xl_mit_ev"),
            pytest.param(10, False, 8.20, id="small_ohne_ev"),
            pytest.param(60, False, 8.20, id="mid_ohne_ev"),
            pytest.param(120, False, 5.40, id="large_ohne_ev"),
            pytest.param(300, False, 5.40, id="xl_ohne_ev"),
        ],
    )
    def test_anrechenbarkeitsgrenze(self, kw, ev, expected):
        assert anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw=kw, eigenverbrauch=ev) == expected


def _floor(kw: float, ev: bool) -> float | None:
    return mindestverguetung_rp_kwh(FED_RULES_2026, kw, ev)


def _cap(kw: float, ev: bool) -> float | None:
    return anrechenbarkeitsgrenze_rp_kwh(DEFAULT_CAP_RULES, kw, ev)


class TestEffectiveOhneObergrenze:
    """cap_rp_kwh=None — base + HKN paid additively, no upper cap."""

    @pytest.mark.parametrize(
        ("base", "hkn", "floor_kw", "floor_ev", "expected"),
        [
            # Q1 2026 BFE reference 10.266 + HKN 3 → 13.266 (no cap)
            pytest.param(10.266, 3.0, 10, True, 13.266, id="q1_2026_base_plus_hkn"),
            # 3.0 < floor 6.0 → base raised to floor; HKN added
            pytest.param(3.0, 2.0, 10, True, 8.0, id="low_reference_lifted_to_floor"),
            # IWB 12.95 (HKN bundled, hkn=0) — additive utility, no cap → 12.95
            pytest.param(12.95, 0.0, 10, True, 12.95, id="iwb_fixed_paid_in_full"),
            # ewz: fixed 7.91 + HKN 5.0 → 12.91, no cap
            pytest.param(7.91, 5.0, 10, True, 12.91, id="ewz_base_plus_hkn_full"),
            # AEW 8.20 includes HKN, hkn=0 → 8.20
            pytest.param(8.2, 0.0, 10, True, 8.2, id="aew_inclusive_flat"),
            pytest.param(3.0, 0.0, 200, False, 3.0, id="xl_no_floor_low_reference_stays_low"),
        ],
    )
    def test_effective_no_cap(self, base, hkn, floor_kw, floor_ev, expected):
        rp = effective_rp_kwh(
            base, hkn,
            federal_floor_rp_kwh=_floor(floor_kw, floor_ev),
            cap_rp_kwh=None,
        )
        assert rp == pytest.approx(expected)


class TestEffectiveMitObergrenze:
    """cap_rp_kwh set (schema 1.5.0: non-empty cap_rules) — EKZ-style cap."""

    @pytest.mark.parametrize(
        ("base", "hkn", "kw", "ev", "expected"),
        [
            # 10.266 < cap 10.96, no HKN → 10.266 (cap not engaged)
            pytest.param(10.266, 0.0, 10, True, 10.266, id="q1_2026_ekz_no_hkn_below_cap"),
            # 10.266 + 3.0 = 13.266 > cap 10.96 → reduced to 10.96
            pytest.param(10.266, 3.0, 10, True, 10.96, id="q1_2026_ekz_with_hkn_hits_cap"),
            # Base = cap exactly → HKN forfeited per EKZ clause 2.
            pytest.param(10.96, 3.0, 10, True, 10.96, id="base_alone_at_cap_hkn_forfeited"),
            # Base 12.0 > cap 10.96 → producer gets base 12.0 (NOT capped to 10.96), zero HKN.
            pytest.param(12.0, 3.0, 10, True, 12.0, id="base_above_cap_hkn_forfeited_full_base_paid"),
            # 7.0 + 3.0 = 10.0 > cap 7.20 → reduced to 7.20.
            pytest.param(7.0, 3.0, 200, True, 7.20, id="xl_mit_ev_cap_binds_at_7_20"),
            # 3.0 < floor 6.0 → base 6.0; 6.0 + 3.0 = 9.0 < cap 10.96 → 9.0.
            pytest.param(3.0, 3.0, 10, True, 9.0, id="low_reference_lifted_to_floor_then_hkn"),
            # v0.22.0 schema 1.5.0: cap activation via cap_rp_kwh only — full HKN passes when cap not set elsewhere
            pytest.param(10.266, 3.0, 10, True, 10.96, id="v0_22_0_schema_1_5_0_cap_active_truncates_hkn"),
        ],
    )
    def test_effective_with_cap(self, base, hkn, kw, ev, expected):
        rp = effective_rp_kwh(
            base, hkn,
            federal_floor_rp_kwh=_floor(kw, ev),
            cap_rp_kwh=_cap(kw, ev),
        )
        assert rp == pytest.approx(expected)


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

    @pytest.mark.parametrize(
        ("date_args", "window_attr", "expected"),
        [
            # Wed 14:00 Zurich → HT
            pytest.param((2025, 1, 15, 14), "EKZ_WINDOW", True, id="weekday_inside_ht_window"),
            # Wed 06:00 Zurich → NT (before HT start)
            pytest.param((2025, 1, 15, 6), "EKZ_WINDOW", False, id="weekday_before_ht_window"),
            # Wed 07:00 Zurich → HT (start inclusive)
            pytest.param((2025, 1, 15, 7), "EKZ_WINDOW", True, id="weekday_at_ht_start_inclusive"),
            # Wed 20:00 Zurich → NT (end exclusive — half-open window)
            pytest.param((2025, 1, 15, 20), "EKZ_WINDOW", False, id="weekday_at_ht_end_exclusive"),
            # Wed 22:00 Zurich → NT
            pytest.param((2025, 1, 15, 22), "EKZ_WINDOW", False, id="weekday_after_ht"),
            # Fri 14:00 Zurich → HT (Mo-Fr covers Friday)
            pytest.param((2025, 1, 17, 14), "EKZ_WINDOW", True, id="friday_still_uses_mofr_window"),
            # Sat 12:00 Zurich → NT (sa=None)
            pytest.param((2025, 1, 18, 12), "EKZ_WINDOW", False, id="saturday_all_nt_when_sa_is_none"),
            # Sat 10:00 Zurich → HT (sa=[7,13])
            pytest.param((2025, 1, 18, 10), "SAT_WINDOW", True, id="saturday_morning_ht_when_sa_window_present"),
            # Sat 14:00 Zurich → NT (after sa end)
            pytest.param((2025, 1, 18, 14), "SAT_WINDOW", False, id="saturday_afternoon_nt_when_sa_window_ends_at_13"),
            pytest.param((2025, 1, 19, 12), "EKZ_WINDOW", False, id="sunday_always_nt"),
        ],
    )
    def test_classify_ht_weekday_windows(self, date_args, window_attr, expected):
        window = getattr(self, window_attr)
        assert classify_ht(_utc_for_zurich(*date_args), window) is expected

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

    @pytest.mark.parametrize(
        ("date_args", "expected_season"),
        [
            pytest.param((2025, 7, 15, 12), "summer", id="july_midday"),
            pytest.param((2025, 1, 15, 12), "winter", id="january"),
            # First instant of summer per the canonical split.
            pytest.param((2025, 4, 1, 0), "summer", id="april_first_midnight"),
            pytest.param((2025, 9, 30, 23), "summer", id="september_last_evening"),
            # First instant of winter — boundary belongs to the new season.
            pytest.param((2025, 10, 1, 0), "winter", id="october_first_midnight"),
            # 2025-10-26 is the last Sunday of October (CET resumes). Month is
            # 10 → winter. Confirms the function reads Zurich-local month, not UTC.
            pytest.param((2025, 10, 26, 12), "winter", id="dst_fall_back_day"),
        ],
    )
    def test_classify_season(self, date_args, expected_season):
        assert (
            classify_season(_utc_for_zurich(*date_args), self.SUMMER_CH, self.WINTER_CH)
            == expected_season
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


# v0.22.0 schema 1.5.0: cap activation flows through cap_rp_kwh is not None
# only (legacy cap_mode boolean dropped). Coverage folded into
# TestEffectiveOhneObergrenze (None case) and TestEffectiveMitObergrenze
# (cap-set case, see "v0_22_0_schema_1_5_0_..." param id).
