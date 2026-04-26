"""Tests for v0.7.0 recompute report machinery — pure functions only.

Covers ``_aggregate_monthly`` and ``_format_recompute_notification`` from
``services.py``, plus the snapshot's new ``monthly`` / ``total_*`` fields
asserted via ``compute_quarter_plan`` + ``_aggregate_monthly`` directly.

Coordinator-level staleness detection is exercised live during the
verification step (kW-change in options flow → notification appears);
unit-testing it would require either HA fixtures or extracting the
comparison into a pure helper, both deferred until the logic grows.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from custom_components.bfe_rueckliefertarif.bfe import BfePrice
from custom_components.bfe_rueckliefertarif.const import ABRECHNUNGS_RHYTHMUS_QUARTAL
from custom_components.bfe_rueckliefertarif.importer import (
    HourRecord,
    TariffConfig,
    compute_quarter_plan,
)
from custom_components.bfe_rueckliefertarif.quarters import (
    Quarter,
    hours_in_range,
    quarter_bounds_utc,
)
from custom_components.bfe_rueckliefertarif.services import (
    _RecomputeReport,
    _RecomputeReportRow,
    _aggregate_monthly,
    _format_recompute_notification,
)
from custom_components.bfe_rueckliefertarif.tariff import classify_ht
from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff


# ----- _aggregate_monthly ----------------------------------------------------


def _hour(year: int, month: int, day: int, hour: int) -> datetime:
    """Build a UTC hour matching the given Zurich-local wall-clock."""
    return datetime(
        year, month, day, hour, tzinfo=ZoneInfo("Europe/Zurich")
    ).astimezone(timezone.utc)


def _hr(start: datetime, kwh: float, rate_rp_kwh: float = 10.0) -> HourRecord:
    """Build a synthetic HourRecord with computed compensation."""
    return HourRecord(
        start=start,
        kwh=kwh,
        rate_rp_kwh=rate_rp_kwh,
        compensation_chf=kwh * rate_rp_kwh / 100.0,
    )


class TestAggregateMonthly:
    def test_full_quarter_produces_three_buckets(self):
        # Synthetic Q1 2026 with 1 kWh/hour at flat 10 Rp/kWh.
        q = Quarter(2026, 1)
        s, e = quarter_bounds_utc(q)
        records = [_hr(h, kwh=1.0, rate_rp_kwh=10.0) for h in hours_in_range(s, e)]
        out = _aggregate_monthly(records)
        # Three Zurich-local months: 2026-01, 2026-02, 2026-03
        months = [b["month"] for b in out]
        assert months == ["2026-01", "2026-02", "2026-03"]
        # Avg rate is flat 10.0 Rp/kWh in every bucket
        for b in out:
            assert b["rate_rp_kwh_avg"] == pytest.approx(10.0)

    def test_january_kwh_chf_match_hand_computed(self):
        q = Quarter(2026, 1)
        s, e = quarter_bounds_utc(q)
        records = [_hr(h, kwh=1.0, rate_rp_kwh=10.0) for h in hours_in_range(s, e)]
        out = _aggregate_monthly(records)
        jan = next(b for b in out if b["month"] == "2026-01")
        # January 2026 has 31 days × 24 hours = 744 hours, all in CET (no DST).
        assert jan["kwh"] == pytest.approx(744.0)
        assert jan["chf"] == pytest.approx(74.40)

    def test_zero_export_month_returns_none_rate(self):
        # One zero-export hour in February.
        feb_hour = _hour(2026, 2, 15, 12)
        records = [_hr(feb_hour, kwh=0.0, rate_rp_kwh=10.0)]
        out = _aggregate_monthly(records)
        assert out == [
            {"month": "2026-02", "kwh": 0.0, "chf": 0.0, "rate_rp_kwh_avg": None}
        ]

    def test_dst_spring_forward_buckets_to_zurich_local_month(self):
        # 2025-03-30 02:00 → 03:00 Zurich (CEST starts) — Sunday, last of March.
        # Q1 2025 has 2159 UTC hours total (DST steals one). When bucketed by
        # Zurich-local %Y-%m: Jan 744 + Feb 672 + Mar 743 = 2159. The DST loss
        # lands in March because the skipped local hour was a Mar 30 wall-clock
        # hour, not a Feb or Apr hour.
        q = Quarter(2025, 1)
        s, e = quarter_bounds_utc(q)
        records = [_hr(h, kwh=1.0, rate_rp_kwh=10.0) for h in hours_in_range(s, e)]
        out = _aggregate_monthly(records)
        buckets = {b["month"]: b for b in out}
        assert buckets["2025-01"]["kwh"] == pytest.approx(744.0)
        assert buckets["2025-02"]["kwh"] == pytest.approx(672.0)
        assert buckets["2025-03"]["kwh"] == pytest.approx(743.0)
        assert sum(b["kwh"] for b in out) == pytest.approx(2159.0)

    def test_ht_nt_month_avg_is_kwh_weighted_blend(self):
        # Synthetic January with 1 kWh/hour. EKZ-2025 producer window:
        # mofr 7-20 = HT (12.60), everything else = NT (11.60).
        ekz_window = {"mofr": [7, 20], "sa": None, "su": None}
        m = Quarter(2025, 1)
        s, e = quarter_bounds_utc(m)
        # Only iterate the January slice
        from custom_components.bfe_rueckliefertarif.quarters import month_bounds_utc, Month
        jan_s, jan_e = month_bounds_utc(Month(2025, 1))
        records = []
        for h in hours_in_range(jan_s, jan_e):
            is_ht = classify_ht(h, ekz_window)
            rate = 12.60 if is_ht else 11.60
            records.append(_hr(h, kwh=1.0, rate_rp_kwh=rate))
        out = _aggregate_monthly(records)
        jan = next(b for b in out if b["month"] == "2025-01")
        # Hand-compute expected weighted avg:
        ht_count = sum(1 for h in hours_in_range(jan_s, jan_e) if classify_ht(h, ekz_window))
        nt_count = sum(1 for h in hours_in_range(jan_s, jan_e) if not classify_ht(h, ekz_window))
        expected = (ht_count * 12.60 + nt_count * 11.60) / (ht_count + nt_count)
        assert jan["rate_rp_kwh_avg"] == pytest.approx(expected, rel=1e-4)


# ----- snapshot end-to-end via compute_quarter_plan -------------------------


def _make_resolved() -> ResolvedTariff:
    return ResolvedTariff(
        utility_key="test",
        valid_from="2026-01-01",
        settlement_period="quartal",
        base_model="rmp_quartal",
        fixed_rp_kwh=None,
        fixed_ht_rp_kwh=None,
        fixed_nt_rp_kwh=None,
        hkn_rp_kwh=0.0,
        hkn_structure="none",
        cap_mode=False,
        cap_rp_kwh=None,
        federal_floor_rp_kwh=6.00,
        federal_floor_label="<30 kW",
        requires_naturemade_star=False,
        price_floor_rp_kwh=None,
        tariffs_json_version="1.0.0",
        tariffs_json_source="bundled",
    )


class TestMonthlyAggregationViaPlan:
    """Snapshot-shape check: feeding compute_quarter_plan output into
    _aggregate_monthly produces 3 buckets per quarter that sum to the
    plan's totals."""

    def test_plan_records_aggregate_to_three_months(self):
        q = Quarter(2026, 1)
        s, e = quarter_bounds_utc(q)
        kwh = {h: 1.0 for h in hours_in_range(s, e)}
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kw=10.0,
            hkn_aktiviert=False,
            hkn_rp_kwh_resolved=0.0,
            resolved=_make_resolved(),
        )
        plan = compute_quarter_plan(
            q, kwh, BfePrice(chf_per_mwh=100.0, days=90, volume_mwh=1.0),
            None, cfg, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )
        out = _aggregate_monthly(plan.records)
        assert len(out) == 3
        assert sum(b["kwh"] for b in out) == pytest.approx(
            sum(r.kwh for r in plan.records)
        )
        assert sum(b["chf"] for b in out) == pytest.approx(
            sum(r.compensation_chf for r in plan.records)
        )


# ----- _format_recompute_notification ---------------------------------------


def _config_dict(**overrides) -> dict:
    base = {
        "utility_key": "ekz",
        "utility_name": "Elektrizitätswerke des Kantons Zürich (EKZ)",
        "base_model": "rmp_quartal",
        "settlement_period": "quartal",
        "kw": 25.0,
        "eigenverbrauch": True,
        "hkn_optin": True,
        "hkn_rp_kwh": 3.00,
        "billing": "quartal",
        "floor_label": "<30 kW",
        "floor_rp_kwh": 6.00,
        "cap_mode": True,
        "tariffs_version": "1.0.0",
        "tariffs_source": "bundled",
    }
    base.update(overrides)
    return base


def _row(month: str, rate: float, kwh: float, chf: float) -> _RecomputeReportRow:
    return _RecomputeReportRow(
        month=month,
        rate_rp_kwh_avg=rate,
        total_kwh=kwh,
        total_chf=chf,
    )


class TestFormatRecomputeNotification:
    def test_multi_quarter_title_and_totals(self):
        rows = [
            _row("2026-03", 13.27, 411.13, 54.55),
            _row("2026-02", 13.27, 412.04, 54.67),
            _row("2026-01", 13.27, 411.39, 54.61),
        ]
        report = _RecomputeReport(rows=rows, quarters_recomputed=2, config=_config_dict())
        title, body = _format_recompute_notification(report)
        assert "3 months across 2 quarters" in title
        # Header section
        assert "**Utility:** ekz — Elektrizitätswerke des Kantons Zürich (EKZ)" in body
        assert "**Tariff model:** rmp_quartal (settlement: quartal)" in body
        assert "**Installed power:** 25.0 kW" in body
        assert "**Eigenverbrauch (self-consumption):** Yes" in body
        assert "**HKN opt-in:** Yes (3.00 Rp/kWh additive)" in body
        assert "**Federal floor (Mindestvergütung):** <30 kW (6.00 Rp/kWh)" in body
        assert "**Cap mode (Anrechenbarkeitsgrenze):** Active" in body
        # Per-month table
        assert "| Month | Avg rate (Rp/kWh) | kWh exported | CHF |" in body
        assert "| 2026-03 | 13.2700 | 411.13 | 54.55 |" in body
        # Totals (only when n_q > 1)
        assert "Totals:" in body
        assert "3 months" in body

    def test_single_quarter_title_no_totals(self):
        rows = [
            _row("2026-03", 13.27, 411.13, 54.55),
            _row("2026-02", 13.27, 412.04, 54.67),
            _row("2026-01", 13.27, 411.39, 54.61),
        ]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        title, body = _format_recompute_notification(report)
        assert "1 quarter" in title
        assert "Totals:" not in body  # suppressed for single-quarter

    def test_truncation_at_24_months(self):
        rows = [_row(f"2026-{m:02d}", 10.0, 100.0, 10.0) for m in range(12, 0, -1)]
        # Add older year to push past 24 rows
        rows += [_row(f"2025-{m:02d}", 10.0, 100.0, 10.0) for m in range(12, 0, -1)]
        rows += [_row(f"2024-{m:02d}", 10.0, 100.0, 10.0) for m in range(12, 0, -1)]
        # 36 rows total
        report = _RecomputeReport(rows=rows, quarters_recomputed=12, config=_config_dict())
        _, body = _format_recompute_notification(report)
        assert "12 older month(s) not shown" in body
        # First shown row should be 2026-12, last 2025-01 (24 newest)
        assert "| 2026-12 |" in body
        assert "| 2025-01 |" in body
        assert "| 2024-12 |" not in body  # truncated

    def test_hkn_off_renders_no_rate_suffix(self):
        rows = [_row("2026-01", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(hkn_optin=False),
        )
        _, body = _format_recompute_notification(report)
        assert "**HKN opt-in:** No" in body

    def test_no_floor_renders_none_suffix(self):
        rows = [_row("2026-01", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(floor_label="≥150 kW", floor_rp_kwh=None),
        )
        _, body = _format_recompute_notification(report)
        assert "**Federal floor (Mindestvergütung):** ≥150 kW (none)" in body

    def test_missing_rate_renders_dash(self):
        rows = [_row("2026-02", None, None, None)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "| 2026-02 | — | — | — |" in body

    def test_remote_source_renders_in_header(self):
        rows = [_row("2026-01", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(tariffs_source="remote"),
        )
        _, body = _format_recompute_notification(report)
        assert "**Tariff data:** v1.0.0 (remote)" in body
