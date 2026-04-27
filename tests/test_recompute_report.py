"""Tests for v0.7.0 recompute report machinery — pure functions only.

Covers ``_aggregate_by_period`` and ``_format_recompute_notification`` from
``services.py``, plus the snapshot's per-period rows asserted via
``compute_quarter_plan`` + ``_aggregate_by_period`` directly.

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
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
)
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
    _aggregate_by_period,
    _format_recompute_notification,
)
from custom_components.bfe_rueckliefertarif.tariff import classify_ht
from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff


# ----- _aggregate_by_period --------------------------------------------------


def _hour(year: int, month: int, day: int, hour: int) -> datetime:
    """Build a UTC hour matching the given Zurich-local wall-clock."""
    return datetime(
        year, month, day, hour, tzinfo=ZoneInfo("Europe/Zurich")
    ).astimezone(timezone.utc)


def _hr(
    start: datetime,
    kwh: float,
    rate_rp_kwh: float = 10.0,
    base_rp_kwh: float | None = None,
    hkn_rp_kwh: float = 0.0,
) -> HourRecord:
    """Build a synthetic HourRecord with computed compensation.

    Defaults base_rp_kwh to (rate - hkn) so the breakdown invariant holds.
    """
    if base_rp_kwh is None:
        base_rp_kwh = rate_rp_kwh - hkn_rp_kwh
    return HourRecord(
        start=start,
        kwh=kwh,
        rate_rp_kwh=rate_rp_kwh,
        compensation_chf=kwh * rate_rp_kwh / 100.0,
        base_rp_kwh=base_rp_kwh,
        hkn_rp_kwh=hkn_rp_kwh,
    )


class TestAggregateByPeriod:
    def test_monthly_full_quarter_produces_three_buckets(self):
        # Synthetic Q1 2026 with 1 kWh/hour at flat 10 Rp/kWh.
        q = Quarter(2026, 1)
        s, e = quarter_bounds_utc(q)
        records = [_hr(h, kwh=1.0, rate_rp_kwh=10.0) for h in hours_in_range(s, e)]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_MONAT)
        # Three Zurich-local months: 2026-01, 2026-02, 2026-03
        periods = [b["period"] for b in out]
        assert periods == ["2026-01", "2026-02", "2026-03"]
        # Avg rate is flat 10.0 Rp/kWh in every bucket
        for b in out:
            assert b["rate_rp_kwh_avg"] == pytest.approx(10.0)

    def test_quarterly_full_quarter_produces_one_bucket(self):
        # Same data as above but quarterly aggregation collapses to one row.
        q = Quarter(2026, 1)
        s, e = quarter_bounds_utc(q)
        records = [_hr(h, kwh=1.0, rate_rp_kwh=10.0) for h in hours_in_range(s, e)]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_QUARTAL)
        assert len(out) == 1
        assert out[0]["period"] == "2026Q1"
        # Q1 2026 = 2159 hours (DST spring-forward in March eats one local hour)
        assert out[0]["kwh"] == pytest.approx(2159.0)
        assert out[0]["rate_rp_kwh_avg"] == pytest.approx(10.0)

    def test_january_kwh_chf_match_hand_computed(self):
        q = Quarter(2026, 1)
        s, e = quarter_bounds_utc(q)
        records = [_hr(h, kwh=1.0, rate_rp_kwh=10.0) for h in hours_in_range(s, e)]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_MONAT)
        jan = next(b for b in out if b["period"] == "2026-01")
        # January 2026 has 31 days × 24 hours = 744 hours, all in CET (no DST).
        assert jan["kwh"] == pytest.approx(744.0)
        assert jan["chf"] == pytest.approx(74.40)

    def test_zero_export_period_returns_none_rate(self):
        # One zero-export hour in February.
        feb_hour = _hour(2026, 2, 15, 12)
        records = [_hr(feb_hour, kwh=0.0, rate_rp_kwh=10.0)]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_MONAT)
        assert len(out) == 1
        row = out[0]
        assert row["period"] == "2026-02"
        assert row["kwh"] == 0.0
        assert row["chf"] == 0.0
        assert row["rate_rp_kwh_avg"] is None
        assert row["base_rp_kwh_avg"] is None
        assert row["hkn_rp_kwh_avg"] is None

    def test_dst_spring_forward_buckets_to_zurich_local_month(self):
        # 2025-03-30 02:00 → 03:00 Zurich (CEST starts) — Sunday, last of March.
        # Q1 2025 has 2159 UTC hours total (DST steals one). When bucketed by
        # Zurich-local %Y-%m: Jan 744 + Feb 672 + Mar 743 = 2159. The DST loss
        # lands in March because the skipped local hour was a Mar 30 wall-clock
        # hour, not a Feb or Apr hour.
        q = Quarter(2025, 1)
        s, e = quarter_bounds_utc(q)
        records = [_hr(h, kwh=1.0, rate_rp_kwh=10.0) for h in hours_in_range(s, e)]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_MONAT)
        buckets = {b["period"]: b for b in out}
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
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_MONAT)
        jan = next(b for b in out if b["period"] == "2025-01")
        # Hand-compute expected weighted avg:
        ht_count = sum(1 for h in hours_in_range(jan_s, jan_e) if classify_ht(h, ekz_window))
        nt_count = sum(1 for h in hours_in_range(jan_s, jan_e) if not classify_ht(h, ekz_window))
        expected = (ht_count * 12.60 + nt_count * 11.60) / (ht_count + nt_count)
        assert jan["rate_rp_kwh_avg"] == pytest.approx(expected, rel=1e-4)

    def test_breakdown_invariant_base_plus_hkn_equals_total(self):
        # 12.45 base + 0.82 HKN = 13.27 total, every hour.
        feb_hour = _hour(2026, 2, 15, 12)
        records = [
            _hr(feb_hour, kwh=10.0, rate_rp_kwh=13.27, base_rp_kwh=12.45, hkn_rp_kwh=0.82)
        ]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_MONAT)
        row = out[0]
        assert row["base_rp_kwh_avg"] == pytest.approx(12.45)
        assert row["hkn_rp_kwh_avg"] == pytest.approx(0.82)
        assert row["base_rp_kwh_avg"] + row["hkn_rp_kwh_avg"] == pytest.approx(
            row["rate_rp_kwh_avg"], abs=1e-4
        )

    def test_breakdown_hkn_zero_when_optout(self):
        feb_hour = _hour(2026, 2, 15, 12)
        records = [
            _hr(feb_hour, kwh=10.0, rate_rp_kwh=12.45, base_rp_kwh=12.45, hkn_rp_kwh=0.0)
        ]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_MONAT)
        row = out[0]
        assert row["hkn_rp_kwh_avg"] == pytest.approx(0.0)
        assert row["base_rp_kwh_avg"] == pytest.approx(row["rate_rp_kwh_avg"])


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
        price_floor_rp_kwh=None,
        tariffs_json_version="1.0.0",
        tariffs_json_source="bundled",
    )


class TestPeriodAggregationViaPlan:
    """Snapshot-shape check: feeding compute_quarter_plan output into
    _aggregate_by_period produces sensible buckets that sum to the plan's
    totals, with base + hkn invariant preserved.
    """

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
        out = _aggregate_by_period(plan.records, ABRECHNUNGS_RHYTHMUS_MONAT)
        assert len(out) == 3
        assert sum(b["kwh"] for b in out) == pytest.approx(
            sum(r.kwh for r in plan.records)
        )
        assert sum(b["chf"] for b in out) == pytest.approx(
            sum(r.compensation_chf for r in plan.records)
        )

    def test_quarterly_aggregation_collapses_plan_to_one_row(self):
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
        out = _aggregate_by_period(plan.records, ABRECHNUNGS_RHYTHMUS_QUARTAL)
        assert len(out) == 1
        assert out[0]["period"] == "2026Q1"
        assert out[0]["kwh"] == pytest.approx(sum(r.kwh for r in plan.records))

    def test_plan_records_carry_base_hkn_breakdown(self):
        # rmp_quartal with HKN opt-in active, no cap → base + hkn = rate per hour.
        rt = _make_resolved()
        # Override hkn_rp_kwh on a copy via dataclasses.replace would be cleaner;
        # since ResolvedTariff is frozen, build a fresh one with HKN.
        from dataclasses import replace
        rt_with_hkn = replace(rt, hkn_rp_kwh=2.50, hkn_structure="additive_optin")
        q = Quarter(2026, 1)
        s, e = quarter_bounds_utc(q)
        kwh = {h: 1.0 for h in hours_in_range(s, e)}
        cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kw=10.0,
            hkn_aktiviert=True,
            hkn_rp_kwh_resolved=2.50,
            resolved=rt_with_hkn,
        )
        plan = compute_quarter_plan(
            q, kwh, BfePrice(chf_per_mwh=100.0, days=90, volume_mwh=1.0),
            None, cfg, ABRECHNUNGS_RHYTHMUS_QUARTAL,
            anchor_sum_chf=0.0, old_post_quarter_first_sum_chf=None,
        )
        # Every hour: base_rp + hkn_rp == rate_rp; hkn_rp == 2.50 (no cap).
        for r in plan.records:
            assert r.base_rp_kwh + r.hkn_rp_kwh == pytest.approx(r.rate_rp_kwh, abs=1e-9)
            assert r.hkn_rp_kwh == pytest.approx(2.50)


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
        "cap_rp_kwh": 10.96,
        "tariffs_version": "1.0.0",
        "tariffs_source": "bundled",
    }
    base.update(overrides)
    return base


def _row(
    period: str,
    rate: float | None,
    kwh: float | None,
    chf: float | None,
    base: float | None = None,
    hkn: float | None = None,
    intended_hkn: float | None = None,
) -> _RecomputeReportRow:
    """Construct a row; defaults base/hkn so base+hkn=rate when both given."""
    if rate is not None and base is None and hkn is None:
        # No HKN by default → put everything in base.
        base, hkn = rate, 0.0
    return _RecomputeReportRow(
        period=period,
        rate_rp_kwh_avg=rate,
        base_rp_kwh_avg=base,
        hkn_rp_kwh_avg=hkn,
        intended_hkn_rp_kwh=intended_hkn,
        total_kwh=kwh,
        total_chf=chf,
    )


class TestFormatRecomputeNotification:
    def test_multi_quarter_title_and_totals(self):
        rows = [
            _row("2026-03", 13.27, 411.13, 54.55, base=10.27, hkn=3.00),
            _row("2026-02", 13.27, 412.04, 54.67, base=10.27, hkn=3.00),
            _row("2026-01", 13.27, 411.39, 54.61, base=10.27, hkn=3.00),
        ]
        report = _RecomputeReport(rows=rows, quarters_recomputed=2, config=_config_dict())
        title, body = _format_recompute_notification(report)
        assert "3 periods across 2 quarters" in title
        # Header section
        assert "**Utility:** ekz — Elektrizitätswerke des Kantons Zürich (EKZ)" in body
        assert "**Tariff model:** rmp_quartal (settlement: quartal)" in body
        assert "**Installed power:** 25.0 kW" in body
        assert "**Eigenverbrauch (self-consumption):** Yes" in body
        assert "**HKN opt-in:** Yes (3.00 Rp/kWh additive)" in body
        assert "**Federal floor (Mindestvergütung):** <30 kW (6.00 Rp/kWh)" in body
        # Cap value is now embedded in the line.
        assert (
            "**Cap mode (Anrechenbarkeitsgrenze):** Active — current cap "
            "10.96 Rp/kWh (25.0 kW, EV=Yes)"
        ) in body
        # Slim 6-column table headers + unit-disclaimer line.
        assert "_Rates in Rp/kWh; energy in kWh; CHF totals._" in body
        assert "| Period | Base | HKN | Total | kWh | CHF |" in body
        # 3-decimal rates, 2-decimal kWh/CHF.
        assert "| 2026-03 | 10.270 | 3.000 | 13.270 | 411.13 | 54.55 |" in body
        # Totals (only when n_q > 1)
        assert "Totals:" in body
        assert "3 periods" in body

    def test_single_quarter_single_period_title_grammar(self):
        # Single quarter + single period → "1 period" (no awkward "(1 periods)").
        rows = [_row("2026Q1", 13.27, 1233.56, 163.83, base=10.27, hkn=3.00)]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        title, body = _format_recompute_notification(report)
        assert title == "Tariff recomputed — 1 period"
        assert "Totals:" not in body

    def test_single_quarter_multi_period_title(self):
        # Single quarter with monthly billing → 3 monthly periods.
        rows = [
            _row("2026-03", 13.27, 411.13, 54.55, base=10.27, hkn=3.00),
            _row("2026-02", 13.27, 412.04, 54.67, base=10.27, hkn=3.00),
            _row("2026-01", 13.27, 411.39, 54.61, base=10.27, hkn=3.00),
        ]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        title, _ = _format_recompute_notification(report)
        assert title == "Tariff recomputed — 1 quarter (3 periods)"

    def test_quarterly_period_label_renders(self):
        # Quarterly billing → row period is "2026Q1" not "2026-01".
        rows = [_row("2026Q1", 13.27, 1233.56, 163.83, base=10.27, hkn=3.00)]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        _, body = _format_recompute_notification(report)
        assert "| 2026Q1 |" in body

    def test_cap_off_renders_simple_off(self):
        rows = [_row("2026Q1", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(cap_mode=False, cap_rp_kwh=None),
        )
        _, body = _format_recompute_notification(report)
        assert "**Cap mode (Anrechenbarkeitsgrenze):** Off" in body
        assert "current cap" not in body  # only emitted when active

    def test_truncation_at_24_periods(self):
        rows = [_row(f"2026-{m:02d}", 10.0, 100.0, 10.0) for m in range(12, 0, -1)]
        # Add older years to push past 24 rows
        rows += [_row(f"2025-{m:02d}", 10.0, 100.0, 10.0) for m in range(12, 0, -1)]
        rows += [_row(f"2024-{m:02d}", 10.0, 100.0, 10.0) for m in range(12, 0, -1)]
        # 36 rows total
        report = _RecomputeReport(rows=rows, quarters_recomputed=12, config=_config_dict())
        _, body = _format_recompute_notification(report)
        assert "12 older period(s) not shown" in body
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

    def test_utility_floor_dominates_renders_utility_line(self):
        # #2: utility_floor 8.0 > federal 6.0 → render Utility floor line,
        # cite federal value for context.
        rows = [_row("2026-01", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(
                floor_label="<30 kW", floor_rp_kwh=6.00,
                utility_floor_rp_kwh=8.00, floor_source="utility",
            ),
        )
        _, body = _format_recompute_notification(report)
        assert "**Utility floor:** 8.00 Rp/kWh" in body
        assert "federal 6.00 Rp/kWh" in body
        # The federal-floor line is suppressed when the utility line is shown.
        assert "**Federal floor (Mindestvergütung):**" not in body

    def test_federal_floor_dominates_renders_federal_line_only(self):
        # #2: federal 6.0 ≥ utility 4.0 → existing federal-floor line, no
        # utility line.
        rows = [_row("2026-01", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(
                floor_label="<30 kW", floor_rp_kwh=6.00,
                utility_floor_rp_kwh=4.00, floor_source="federal",
            ),
        )
        _, body = _format_recompute_notification(report)
        assert "**Federal floor (Mindestvergütung):** <30 kW (6.00 Rp/kWh)" in body
        assert "**Utility floor:**" not in body

    def test_missing_rate_renders_dash(self):
        rows = [_row("2026-02", None, None, None)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "| 2026-02 | — | — | — | — | — |" in body

    def test_remote_source_renders_in_header(self):
        rows = [_row("2026-01", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(tariffs_source="remote"),
        )
        _, body = _format_recompute_notification(report)
        assert "**Tariff data:** v1.0.0 (remote)" in body

    # --- HKN forfeit / slash-format visualization ---------------------------

    def test_hkn_forfeit_partial_renders_slash(self):
        # EV=Yes EKZ scenario: applied 0.694 < intended 3.00 → "applied / intended".
        rows = [_row("2026Q1", 10.96, 1000.0, 109.60,
                     base=10.266, hkn=0.694, intended_hkn=3.00)]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        _, body = _format_recompute_notification(report)
        assert "0.694 / 3.00" in body
        # Forfeit footnote present + period count.
        assert "HKN was reduced or forfeited in 1 period" in body
        assert "Published HKN: 3.00 Rp/kWh" in body

    def test_hkn_forfeit_full_renders_slash_zero(self):
        # EV=No EKZ scenario: applied 0.000 < intended 3.00.
        rows = [_row("2026Q1", 10.266, 1000.0, 102.66,
                     base=10.266, hkn=0.0, intended_hkn=3.00)]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        _, body = _format_recompute_notification(report)
        assert "0.000 / 3.00" in body
        assert "HKN was reduced or forfeited" in body

    def test_no_forfeit_renders_plain_hkn(self):
        # Applied == intended → no slash, no footnote.
        rows = [_row("2026Q1", 13.27, 1000.0, 132.70,
                     base=10.27, hkn=3.00, intended_hkn=3.00)]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        _, body = _format_recompute_notification(report)
        assert " 3.000 " in body
        assert " / 3.00" not in body
        assert "forfeited" not in body

    def test_hkn_optout_no_slash_no_footnote(self):
        # User did not opt in → intended is None or 0 → no forfeit signal.
        rows = [_row("2026Q1", 10.27, 1000.0, 102.70,
                     base=10.27, hkn=0.0, intended_hkn=0.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(hkn_optin=False),
        )
        _, body = _format_recompute_notification(report)
        assert " / " not in body
        assert "forfeited" not in body


# ----- v0.8.6: per-config grouping --------------------------------------------


def _row_with_meta(
    period: str,
    rate: float | None,
    kwh: float | None,
    chf: float | None,
    *,
    base: float | None = None,
    hkn: float | None = None,
    intended_hkn: float | None = None,
    utility_key: str | None = None,
    utility_name: str | None = None,
    kw: float | None = None,
    eigenverbrauch: bool | None = None,
    hkn_optin: bool | None = None,
    billing: str | None = None,
    base_model: str | None = None,
    cap_mode: bool | None = None,
    cap_rp_kwh: float | None = None,
    floor_label: str | None = None,
    floor_rp_kwh: float | None = None,
    tariffs_version: str | None = None,
    tariffs_source: str | None = None,
) -> _RecomputeReportRow:
    if rate is not None and base is None and hkn is None:
        base, hkn = rate, 0.0
    return _RecomputeReportRow(
        period=period,
        rate_rp_kwh_avg=rate,
        base_rp_kwh_avg=base,
        hkn_rp_kwh_avg=hkn,
        intended_hkn_rp_kwh=intended_hkn,
        total_kwh=kwh,
        total_chf=chf,
        utility_key_at_period=utility_key,
        utility_name_at_period=utility_name,
        kw_at_period=kw,
        eigenverbrauch_at_period=eigenverbrauch,
        hkn_optin_at_period=hkn_optin,
        billing_at_period=billing,
        base_model_at_period=base_model,
        cap_mode_at_period=cap_mode,
        cap_rp_kwh_at_period=cap_rp_kwh,
        floor_label_at_period=floor_label,
        floor_rp_kwh_at_period=floor_rp_kwh,
        tariffs_version_at_period=tariffs_version,
        tariffs_source_at_period=tariffs_source,
    )


class TestPerConfigGrouping:
    """v0.8.6: notification renders one section per distinct config used."""

    def test_single_config_matching_today_suppresses_per_group_heading(self):
        # When the only group's fingerprint matches today, the per-group
        # heading is suppressed — output looks like v0.8.5 (active-today
        # block + single table).
        rows = [_row_with_meta(
            "2026Q1", 13.27, 1000.0, 132.70, base=10.27, hkn=3.00,
            utility_key="ekz", utility_name="Elektrizitätswerke des Kantons Zürich (EKZ)",
            kw=25.0, eigenverbrauch=True, hkn_optin=True, billing="quartal",
        )]
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        _, body = _format_recompute_notification(report)
        assert "## Active configuration (today)" in body
        assert "## Per-period results" in body
        # v0.9.2: "Periods computed under" replaced by "Configuration in effect:".
        assert "## Periods computed under" not in body
        assert "## Configuration in effect" not in body

    def test_two_configs_emits_two_groups(self):
        rows = [
            _row_with_meta(
                "2026Q1", 10.96, 586.21, 64.25, base=10.266, hkn=0.694,
                intended_hkn=3.00,
                utility_key="ekz", utility_name="EKZ",
                kw=8.0, eigenverbrauch=True, hkn_optin=True, billing="quartal",
                base_model="rmp_quartal", cap_mode=True, cap_rp_kwh=10.96,
            ),
            _row_with_meta(
                "2025Q4", 8.0, 335.38, 26.83, base=8.0, hkn=0.0,
                utility_key="age_sa", utility_name="Acqua Gas Elettricità SA Chiasso",
                kw=105.0, eigenverbrauch=True, hkn_optin=False, billing="quartal",
                base_model="fixed_flat", cap_mode=False,
            ),
        ]
        report = _RecomputeReport(rows=rows, quarters_recomputed=2, config=_config_dict())
        _, body = _format_recompute_notification(report)
        # v0.9.2: heading is "## Configuration in effect: YYYY-MM-DD → YYYY-MM-DD"
        # with date ranges derived from the group's row periods.
        # 2026Q1 = 2026-01-01 → 2026-03-31; 2025Q4 = 2025-10-01 → 2025-12-31.
        ekz_idx = body.find("## Configuration in effect: 2026-01-01 → 2026-03-31")
        age_idx = body.find("## Configuration in effect: 2025-10-01 → 2025-12-31")
        assert ekz_idx != -1 and age_idx != -1, "both groups should render"
        assert ekz_idx < age_idx, "ekz (newer) should render before age_sa (older)"
        # Each group has its own table with the right rate.
        assert "| 2026Q1 | 10.266 | 0.694 / 3.00 | 10.960 | 586.21 | 64.25 |" in body
        assert "| 2025Q4 | 8.000 | 0.000 | 8.000 | 335.38 | 26.83 |" in body
        # Each group's bullet block carries its own utility identity.
        assert "Utility:** ekz — EKZ" in body
        assert "Utility:** age_sa — Acqua Gas Elettricità SA Chiasso" in body
        # Per-group sub-bullets reflect per-row metadata.
        assert "**Tariff model:** rmp_quartal" in body
        assert "**Tariff model:** fixed_flat" in body
        assert "Cap mode (Anrechenbarkeitsgrenze):** Active — cap 10.96 Rp/kWh" in body
        assert "Cap mode (Anrechenbarkeitsgrenze):** Off" in body

    def test_grouping_preserves_newest_first_order(self):
        rows = [
            _row_with_meta("2026Q1", 11.0, 100.0, 11.0,
                           utility_key="ekz", kw=8.0, eigenverbrauch=True,
                           hkn_optin=True, billing="quartal"),
            _row_with_meta("2025Q4", 8.0, 100.0, 8.0,
                           utility_key="age_sa", kw=105.0, eigenverbrauch=True,
                           hkn_optin=False, billing="quartal"),
            _row_with_meta("2025Q3", 8.0, 100.0, 8.0,
                           utility_key="age_sa", kw=105.0, eigenverbrauch=True,
                           hkn_optin=False, billing="quartal"),
        ]
        report = _RecomputeReport(rows=rows, quarters_recomputed=3, config=_config_dict())
        _, body = _format_recompute_notification(report)
        # v0.9.2: age_sa group spans 2025Q3 + 2025Q4 → 2025-07-01 → 2025-12-31.
        age_heading = "## Configuration in effect: 2025-07-01 → 2025-12-31"
        assert age_heading in body
        age_block = body[body.find(age_heading):]
        q4_idx = age_block.find("| 2025Q4 |")
        q3_idx = age_block.find("| 2025Q3 |")
        assert q4_idx != -1 and q3_idx != -1
        assert q4_idx < q3_idx

    def test_forfeit_footnote_per_group(self):
        # Group A (ekz, 2026Q1) has a forfeit; group B (age_sa) doesn't.
        # The footnote must appear within group A's section and reference
        # group A's published HKN — not the active-today header value.
        rows = [
            _row_with_meta("2026Q1", 10.96, 586.21, 64.25,
                           base=10.266, hkn=0.694, intended_hkn=3.00,
                           utility_key="ekz", kw=8.0, eigenverbrauch=True,
                           hkn_optin=True, billing="quartal"),
            _row_with_meta("2025Q4", 8.0, 335.38, 26.83,
                           base=8.0, hkn=0.0,
                           utility_key="age_sa", kw=105.0, eigenverbrauch=True,
                           hkn_optin=False, billing="quartal"),
        ]
        report = _RecomputeReport(rows=rows, quarters_recomputed=2, config=_config_dict())
        _, body = _format_recompute_notification(report)
        # Footnote text appears once.
        assert body.count("HKN was reduced or forfeited") == 1
        # And it references group A's published HKN (3.00, the intended
        # value carried on the forfeit row), not the today header value.
        assert "Published HKN: 3.00 Rp/kWh" in body

    def test_legacy_snapshot_without_metadata_falls_back_to_unknown_group(self):
        # Pre-v0.8.6 snapshots don't carry per-period metadata — all None
        # fields. Should render under a date-bounded heading + "(unknown)"
        # utility bullet without crashing.
        rows = [_row("2025Q3", 8.0, 100.0, 8.0)]  # bare _row, no meta
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=_config_dict())
        _, body = _format_recompute_notification(report)
        # v0.9.2: 2025Q3 → 2025-07-01 → 2025-09-30 heading.
        assert "## Configuration in effect: 2025-07-01 → 2025-09-30" in body
        # The bullet block falls back to "(unknown)" for the utility key.
        assert "Utility:** (unknown)" in body
        # Row still rendered.
        assert "| 2025Q3 |" in body


class TestNotesAndSeasonalConfigBlock:
    """v0.9.9 — config-block rendering for rate-window notes + seasonal markers."""

    def test_config_block_includes_notes_when_present(self):
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(
            notes_active=[
                {"severity": "warning", "text": {"en": "Self-attest naturemade-star."}}
            ],
            notes_lang="en",
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Notes:**" in body
        assert "*(warning)* Self-attest naturemade-star." in body

    def test_config_block_no_notes_when_empty(self):
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(notes_active=None, notes_lang="en")
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Notes:**" not in body

    def test_config_block_locale_fallback_to_de(self):
        # Note has only DE text; renderer asked for EN → falls back to DE.
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(
            notes_active=[
                {"severity": "info", "text": {"de": "Nur Deutsch verfügbar."}}
            ],
            notes_lang="en",
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "Nur Deutsch verfügbar." in body

    def test_config_block_locale_fallback_to_first_when_de_missing(self):
        # Neither user lang nor DE → falls back to first available.
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(
            notes_active=[
                {"severity": "info", "text": {"it": "Solo italiano."}}
            ],
            notes_lang="en",
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "Solo italiano." in body

    def test_config_block_seasonal_yes_renders_summer_winter_split(self):
        rows = [_row("2026Q2", 10.00, 100.0, 10.00)]
        cfg = _config_dict(
            seasonal={
                "summer_months": [4, 5, 6, 7, 8, 9],
                "winter_months": [10, 11, 12, 1, 2, 3],
            }
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Seasonal rates:** Yes" in body
        assert "summer: Apr–Sep" in body
        assert "winter: Oct–Mar" in body

    def test_config_block_seasonal_no_when_explicit_none(self):
        rows = [_row("2026Q2", 10.00, 100.0, 10.00)]
        cfg = _config_dict(seasonal=None)
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Seasonal rates:** No" in body


class TestAggregateBySegment:
    """v0.9.9 — `_aggregate_by_period` emits per-segment sub-buckets when the
    records carry distinct seg_id values."""

    def test_single_seg_id_no_sub_rows(self):
        # All hours tagged with the same seg_id → no sub_rows in output.
        records = [
            HourRecord(start=_hour(2026, 1, h, 0), kwh=1.0, rate_rp_kwh=8.0,
                       compensation_chf=0.08, base_rp_kwh=8.0, hkn_rp_kwh=0.0,
                       seg_id="single")
            for h in range(1, 5)
        ]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_QUARTAL)
        assert len(out) == 1
        assert "sub_rows" not in out[0]

    def test_two_seg_ids_emit_sub_rows_with_distinct_rates(self):
        # Two seg_ids inside one quarterly bucket → output carries sub_rows.
        records = [
            HourRecord(start=_hour(2026, 1, 5, 0), kwh=1.0, rate_rp_kwh=8.0,
                       compensation_chf=0.08, base_rp_kwh=8.0, hkn_rp_kwh=0.0,
                       seg_id="a"),
            HourRecord(start=_hour(2026, 1, 6, 0), kwh=1.0, rate_rp_kwh=8.0,
                       compensation_chf=0.08, base_rp_kwh=8.0, hkn_rp_kwh=0.0,
                       seg_id="a"),
            HourRecord(start=_hour(2026, 2, 20, 0), kwh=1.0, rate_rp_kwh=10.0,
                       compensation_chf=0.10, base_rp_kwh=10.0, hkn_rp_kwh=0.0,
                       seg_id="b"),
            HourRecord(start=_hour(2026, 2, 21, 0), kwh=1.0, rate_rp_kwh=10.0,
                       compensation_chf=0.10, base_rp_kwh=10.0, hkn_rp_kwh=0.0,
                       seg_id="b"),
        ]
        out = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_QUARTAL)
        assert len(out) == 1
        assert "sub_rows" in out[0]
        sub_map = {sr["seg_id"]: sr for sr in out[0]["sub_rows"]}
        assert set(sub_map) == {"a", "b"}
        assert sub_map["a"]["rate_rp_kwh_avg"] == pytest.approx(8.0)
        assert sub_map["b"]["rate_rp_kwh_avg"] == pytest.approx(10.0)
        # Sub-row kWh sums to top-level period kWh.
        assert (sub_map["a"]["kwh"] + sub_map["b"]["kwh"]) == pytest.approx(out[0]["kwh"])
        assert (sub_map["a"]["chf"] + sub_map["b"]["chf"]) == pytest.approx(out[0]["chf"], abs=1e-6)


class TestPeriodTableSubRowRendering:
    """v0.9.9 — `_render_period_table` emits ↳-prefixed sub-rows when set."""

    def test_period_table_renders_sub_rows_when_set(self):
        from custom_components.bfe_rueckliefertarif.services import (
            _PeriodSubRow,
            _render_period_table,
        )

        row = _RecomputeReportRow(
            period="2026Q1",
            rate_rp_kwh_avg=9.0,
            base_rp_kwh_avg=9.0,
            hkn_rp_kwh_avg=0.0,
            intended_hkn_rp_kwh=None,
            total_kwh=200.0,
            total_chf=18.0,
            sub_rows=(
                _PeriodSubRow(
                    label="Jan 1 – Feb 14 (Utility A)",
                    base_rp_kwh_avg=8.0, hkn_rp_kwh_avg=0.0,
                    rate_rp_kwh_avg=8.0, total_kwh=100.0, total_chf=8.0,
                ),
                _PeriodSubRow(
                    label="Feb 15 – Mar 31 (Utility B)",
                    base_rp_kwh_avg=10.0, hkn_rp_kwh_avg=0.0,
                    rate_rp_kwh_avg=10.0, total_kwh=100.0, total_chf=10.0,
                ),
            ),
        )
        lines, _ = _render_period_table([row])
        # Main row plus 2 sub-rows.
        assert any("| 2026Q1 |" in line for line in lines)
        assert any("↳ Jan 1 – Feb 14 (Utility A)" in line for line in lines)
        assert any("↳ Feb 15 – Mar 31 (Utility B)" in line for line in lines)

    def test_period_table_no_sub_rows_when_unset(self):
        from custom_components.bfe_rueckliefertarif.services import _render_period_table

        row = _row("2026Q1", 9.0, 100.0, 9.0)  # no sub_rows
        lines, _ = _render_period_table([row])
        # Header (3 lines) + one main row + nothing else (no estimate footer).
        non_meta = [line for line in lines if "↳" in line]
        assert non_meta == []

    def test_period_table_sub_row_handles_none_cells(self):
        from custom_components.bfe_rueckliefertarif.services import (
            _PeriodSubRow,
            _render_period_table,
        )

        row = _RecomputeReportRow(
            period="2026Q1",
            rate_rp_kwh_avg=9.0,
            base_rp_kwh_avg=9.0,
            hkn_rp_kwh_avg=0.0,
            intended_hkn_rp_kwh=None,
            total_kwh=100.0,
            total_chf=9.0,
            sub_rows=(
                _PeriodSubRow(
                    label="zero-export segment",
                    base_rp_kwh_avg=None, hkn_rp_kwh_avg=None,
                    rate_rp_kwh_avg=None, total_kwh=None, total_chf=None,
                ),
            ),
        )
        lines, _ = _render_period_table([row])
        sub_line = next(line for line in lines if "↳" in line)
        # 5 — markers (base, hkn, rate, kwh, chf) — exact 5 dashes.
        assert sub_line.count("—") == 5
