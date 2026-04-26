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
        requires_naturemade_star=False,
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
