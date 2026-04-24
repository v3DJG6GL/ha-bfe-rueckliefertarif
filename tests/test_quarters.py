"""Tests for quarters.py — timezone and DST correctness."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from custom_components.bfe_rueckliefertarif.quarters import (
    Month,
    Quarter,
    ZURICH,
    hours_in_range,
    month_bounds_utc,
    quarter_bounds_utc,
    quarter_end_zurich,
    quarter_of,
    quarter_start_zurich,
)


class TestQuarterParsing:
    def test_parse_2026q1(self):
        assert Quarter.parse("2026Q1") == Quarter(2026, 1)

    def test_parse_lowercase(self):
        assert Quarter.parse("2026q4") == Quarter(2026, 4)

    def test_parse_invalid(self):
        with pytest.raises(ValueError):
            Quarter.parse("2026Q5")
        with pytest.raises(ValueError):
            Quarter.parse("not-a-quarter")

    def test_str_roundtrip(self):
        assert str(Quarter(2026, 1)) == "2026Q1"

    def test_ordering(self):
        assert Quarter(2026, 1) < Quarter(2026, 2)
        assert Quarter(2025, 4) < Quarter(2026, 1)


class TestMonthParsing:
    def test_parse_2026_01(self):
        assert Month.parse("2026-01") == Month(2026, 1)

    def test_parse_invalid_month(self):
        with pytest.raises(ValueError):
            Month.parse("2026-13")
        with pytest.raises(ValueError):
            Month.parse("2026-00")

    def test_str_roundtrip(self):
        assert str(Month(2026, 7)) == "2026-07"

    def test_month_to_quarter(self):
        assert Month(2026, 1).quarter() == Quarter(2026, 1)
        assert Month(2026, 3).quarter() == Quarter(2026, 1)
        assert Month(2026, 4).quarter() == Quarter(2026, 2)
        assert Month(2026, 12).quarter() == Quarter(2026, 4)


class TestQuarterBounds:
    def test_q1_2026_starts_jan_1_cet(self):
        start = quarter_start_zurich(Quarter(2026, 1))
        # CET = UTC+1
        assert start.year == 2026
        assert start.month == 1
        assert start.day == 1
        assert start.utcoffset() == timedelta(hours=1)

    def test_q2_2026_starts_apr_1_cest(self):
        # April 1 2026 is after the DST switch (last Sunday of March 2026 = 29 March)
        start = quarter_start_zurich(Quarter(2026, 2))
        assert start.month == 4
        # CEST = UTC+2
        assert start.utcoffset() == timedelta(hours=2)

    def test_q1_bounds_utc(self):
        start, end = quarter_bounds_utc(Quarter(2026, 1))
        # Zurich 2026-01-01 00:00 CET = 2025-12-31 23:00 UTC
        assert start == datetime(2025, 12, 31, 23, 0, tzinfo=UTC)
        # Zurich 2026-04-01 00:00 CEST = 2026-03-31 22:00 UTC
        assert end == datetime(2026, 3, 31, 22, 0, tzinfo=UTC)

    def test_q4_bounds_crosses_new_year(self):
        start, end = quarter_bounds_utc(Quarter(2026, 4))
        assert start == datetime(2026, 9, 30, 22, 0, tzinfo=UTC)
        assert end == datetime(2026, 12, 31, 23, 0, tzinfo=UTC)


class TestQuarterNavigation:
    def test_next_within_year(self):
        assert Quarter(2026, 1).next() == Quarter(2026, 2)

    def test_next_crosses_year(self):
        assert Quarter(2026, 4).next() == Quarter(2027, 1)

    def test_prev_crosses_year(self):
        assert Quarter(2026, 1).prev() == Quarter(2025, 4)

    def test_end_is_next_start(self):
        q = Quarter(2026, 1)
        assert quarter_end_zurich(q) == quarter_start_zurich(q.next())

    def test_months_of_quarter(self):
        q = Quarter(2026, 1)
        m1, m2, m3 = q.months()
        assert m1 == Month(2026, 1)
        assert m2 == Month(2026, 2)
        assert m3 == Month(2026, 3)


class TestQuarterOf:
    def test_mid_quarter_utc(self):
        assert quarter_of(datetime(2026, 4, 15, 12, 0, tzinfo=UTC)) == Quarter(2026, 2)

    def test_boundary_zurich_midnight(self):
        dt = datetime(2026, 4, 1, 0, 0, tzinfo=ZURICH)
        assert quarter_of(dt) == Quarter(2026, 2)


class TestMonthBoundsDst:
    def test_march_2026_spans_dst_switch(self):
        start, end = month_bounds_utc(Month(2026, 3))
        # Zurich 2026-03-01 00:00 CET = 2026-02-28 23:00 UTC
        assert start == datetime(2026, 2, 28, 23, 0, tzinfo=UTC)
        # Zurich 2026-04-01 00:00 CEST = 2026-03-31 22:00 UTC
        assert end == datetime(2026, 3, 31, 22, 0, tzinfo=UTC)

    def test_october_2026_spans_dst_switch(self):
        start, end = month_bounds_utc(Month(2026, 10))
        # Zurich 2026-10-01 00:00 CEST = 2026-09-30 22:00 UTC
        assert start == datetime(2026, 9, 30, 22, 0, tzinfo=UTC)
        # Zurich 2026-11-01 00:00 CET = 2026-10-31 23:00 UTC
        assert end == datetime(2026, 10, 31, 23, 0, tzinfo=UTC)


class TestHoursInRange:
    def test_one_hour(self):
        s = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        e = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
        assert hours_in_range(s, e) == [s]

    def test_q1_2026_hour_count_handles_dst(self):
        # Q1 2026: Jan(31)+Feb(28)+Mar(31) = 90 days
        # DST spring-forward on 2026-03-29 local → that day has 23 hours
        # Total hours: 90*24 - 1 = 2159
        s, e = quarter_bounds_utc(Quarter(2026, 1))
        assert len(hours_in_range(s, e)) == 90 * 24 - 1

    def test_q4_2026_hour_count_handles_dst(self):
        # Q4 2026: Oct(31)+Nov(30)+Dec(31) = 92 days
        # DST fall-back on 2026-10-25 local → that day has 25 hours
        # Total hours: 92*24 + 1 = 2209
        s, e = quarter_bounds_utc(Quarter(2026, 4))
        assert len(hours_in_range(s, e)) == 92 * 24 + 1
