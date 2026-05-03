"""Tests for bfe.py CSV parsing. Uses locally snapshot'd fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from custom_components.bfe_rueckliefertarif.bfe import (
    PriceNotYetPublishedError,
    get_month,
    get_quarter,
    parse_monatspreise,
    parse_quartalspreise,
)
from custom_components.bfe_rueckliefertarif.quarters import Month, Quarter

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def quarterly_csv() -> str:
    return (FIXTURES / "quartalspreise_sample.csv").read_text(encoding="utf-8")


@pytest.fixture
def monthly_csv() -> str:
    return (FIXTURES / "monatspreise_sample.csv").read_text(encoding="utf-8")


class TestQuarterlyParse:
    @pytest.mark.parametrize(
        ("quarter", "expected_chf_per_mwh", "days"),
        [
            # Q1 2026 = 102.66 CHF/MWh (verified against briefing)
            pytest.param(Quarter(2026, 1), 102.66, 90, id="q1_2026"),
            pytest.param(Quarter(2024, 1), 61.97, None, id="q1_2024"),
            pytest.param(Quarter(2025, 1), 103.80, None, id="q1_2025"),
        ],
    )
    def test_parses_quarterly_prices(self, quarterly_csv, quarter, expected_chf_per_mwh, days):
        prices = parse_quartalspreise(quarterly_csv)
        assert quarter in prices
        assert prices[quarter].chf_per_mwh == pytest.approx(expected_chf_per_mwh)
        if days is not None:
            assert prices[quarter].days == days

    def test_parses_multiple_years(self, quarterly_csv):
        prices = parse_quartalspreise(quarterly_csv)
        assert Quarter(2024, 1) in prices
        assert Quarter(2024, 2) in prices
        assert Quarter(2025, 1) in prices


class TestMonthlyParse:
    def test_parses_q1_2026_months(self, monthly_csv):
        prices = parse_monatspreise(monthly_csv)
        # Jan/Feb/Mar 2026 from the user's copy
        assert prices[Month(2026, 1)].chf_per_mwh == pytest.approx(126.77)
        assert prices[Month(2026, 2)].chf_per_mwh == pytest.approx(97.70)
        assert prices[Month(2026, 3)].chf_per_mwh == pytest.approx(98.81)

    def test_volume_weighted_avg_matches_quarterly(self, monthly_csv, quarterly_csv):
        """Volume-weighted monthly avg should match the quarterly figure."""
        m = parse_monatspreise(monthly_csv)
        q = parse_quartalspreise(quarterly_csv)
        jan, feb, mar = m[Month(2026, 1)], m[Month(2026, 2)], m[Month(2026, 3)]
        weighted = (
            jan.volume_mwh * jan.chf_per_mwh
            + feb.volume_mwh * feb.chf_per_mwh
            + mar.volume_mwh * mar.chf_per_mwh
        ) / (jan.volume_mwh + feb.volume_mwh + mar.volume_mwh)
        assert weighted == pytest.approx(q[Quarter(2026, 1)].chf_per_mwh, abs=0.02)


class TestPriceNotPublished:
    def test_get_quarter_missing_raises(self, quarterly_csv):
        prices = parse_quartalspreise(quarterly_csv)
        with pytest.raises(PriceNotYetPublishedError):
            get_quarter(prices, Quarter(2030, 1))

    def test_get_month_missing_raises(self, monthly_csv):
        prices = parse_monatspreise(monthly_csv)
        with pytest.raises(PriceNotYetPublishedError):
            get_month(prices, Month(2030, 1))

    def test_get_quarter_present_returns_price(self, quarterly_csv):
        prices = parse_quartalspreise(quarterly_csv)
        assert get_quarter(prices, Quarter(2026, 1)).chf_per_mwh == pytest.approx(102.66)


class TestMalformedRows:
    def test_empty_csv(self):
        assert parse_quartalspreise("") == {}
        assert parse_monatspreise("") == {}

    def test_header_only(self):
        header = "Year,Period,Days,Volume_pv_MWh,Price_pv_CHF_MWh\n"
        assert parse_quartalspreise(header) == {}

    def test_bad_quarter_value_skipped(self):
        csv_text = (
            "Year,Period,Days,Volume_pv_MWh,Price_pv_CHF_MWh,"
            "Volume_wasserkraft_MWh,Price_wasserkraft_CHF_MWh,"
            "Volume_windenergie_MWh,Price_windenergie_CHF_MWh,"
            "Volume_biomasse_MWh,Price_biomasse_CHF_MWh\n"
            "2026,Q5,90,1000,50.0,1,1,1,1,1,1\n"  # Q5 invalid
            "2026,Q1,90,1000,50.0,1,1,1,1,1,1\n"
        )
        prices = parse_quartalspreise(csv_text)
        assert set(prices.keys()) == {Quarter(2026, 1)}

    def test_bad_month_value_skipped(self):
        csv_text = (
            "Year,Month,Days,Volume_pv_MWh,Price_pv_CHF_MWh,"
            "Volume_wasserkraft_MWh,Price_wasserkraft_CHF_MWh,"
            "Volume_windenergie_MWh,Price_windenergie_CHF_MWh,"
            "Volume_biomasse_MWh,Price_biomasse_CHF_MWh\n"
            "2026,13,30,1000,50.0,1,1,1,1,1,1\n"  # month 13 invalid
            "2026,1,31,1000,50.0,1,1,1,1,1,1\n"
        )
        prices = parse_monatspreise(csv_text)
        assert set(prices.keys()) == {Month(2026, 1)}
