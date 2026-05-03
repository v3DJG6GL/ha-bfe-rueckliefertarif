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

from datetime import UTC, datetime
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
    _aggregate_by_period,
    _canon_fingerprint,
    _format_recompute_notification,
    _RecomputeReport,
    _RecomputeReportRow,
    _render_bonuses_lines,
    _render_config_block,
    _render_tariff_model_lines,
    _render_when_summary,
)
from custom_components.bfe_rueckliefertarif.tariff import classify_ht
from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff


@pytest.fixture(autouse=True)
def _freeze_today(monkeypatch):
    """v0.18.0 (Issue 8.6): _should_emit_today_block now suppresses the
    'Active configuration (today)' block when today's date is outside all
    recomputed periods. Most existing tests use rows in 2026-Q1 / 2026-01..03;
    pin today to 2026-02-15 so the today block stays emitted (preserving
    historical assertions). Tests that specifically exercise the
    suppression rule override this fixture."""
    from datetime import date as _date

    real_date = _date

    class _FrozenDate(_date):
        @classmethod
        def today(cls):
            return real_date(2026, 1, 15)

    import custom_components.bfe_rueckliefertarif.services as _svc
    monkeypatch.setattr(_svc, "date", _FrozenDate, raising=False)


# ----- _aggregate_by_period --------------------------------------------------


def _hour(year: int, month: int, day: int, hour: int) -> datetime:
    """Build a UTC hour matching the given Zurich-local wall-clock."""
    return datetime(
        year, month, day, hour, tzinfo=ZoneInfo("Europe/Zurich")
    ).astimezone(UTC)


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
        # Only iterate the January slice
        from custom_components.bfe_rueckliefertarif.quarters import Month, month_bounds_utc
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
            installierte_leistung_kwp=10.0,
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
            installierte_leistung_kwp=10.0,
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
            installierte_leistung_kwp=10.0,
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
        "kwp": 25.0,
        "eigenverbrauch": True,
        "hkn_optin": True,
        "hkn_rp_kwh": 3.00,
        "billing": "quartal",
        "floor_label": "<30 kW",
        "floor_rp_kwh": 6.00,
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
        # v0.17.0 — utility line shows only the human-readable name.
        assert "**Utility:** Elektrizitätswerke des Kantons Zürich (EKZ)" in body
        # v0.17.0 — tariff model line uses localised label; rmp_quartal
        # encodes settlement in the model name itself.
        assert "**Tariff model:** Reference market price (quarterly)" in body
        # v0.17.1 — Installed power, Self-consumption, HKN opt-in are now
        # 4-space-indented sub-bullets under a Configuration parent.
        assert "- **Configuration:**" in body
        assert "    - **Installed power:** 25.0 kWp" in body
        assert "    - **Self-consumption:** Yes" in body
        assert "    - **HKN opt-in:** Yes (3.00 Rp/kWh additive)" in body
        assert "**Federal floor (Mindestvergütung):** <30 kW (6.00 Rp/kWh)" in body
        # Cap value is now embedded in the line.
        assert (
            "**Cap (Anrechenbarkeitsgrenze):** Active — current cap "
            "10.96 Rp/kWh (25.0 kWp, EV=Yes)"
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

    def test_cap_off_omitted_entirely(self):
        # v0.17.1 — Issue 8.4: when no cap is active, the line is dropped
        # entirely (mirrors HKN: don't echo state with no impact).
        # v0.22.0 — schema 1.5.0: cap activation = cap_rp_kwh present.
        rows = [_row("2026Q1", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(cap_rp_kwh=None),
        )
        _, body = _format_recompute_notification(report)
        assert "Cap (Anrechenbarkeitsgrenze):" not in body
        assert "current cap" not in body

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
    kwp: float | None = None,
    eigenverbrauch: bool | None = None,
    hkn_optin: bool | None = None,
    billing: str | None = None,
    base_model: str | None = None,
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
        kw_at_period=kwp,
        eigenverbrauch_at_period=eigenverbrauch,
        hkn_optin_at_period=hkn_optin,
        billing_at_period=billing,
        base_model_at_period=base_model,
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
            kwp=25.0, eigenverbrauch=True, hkn_optin=True, billing="quartal",
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
                kwp=8.0, eigenverbrauch=True, hkn_optin=True, billing="quartal",
                base_model="rmp_quartal", cap_rp_kwh=10.96,
            ),
            _row_with_meta(
                "2025Q4", 8.0, 335.38, 26.83, base=8.0, hkn=0.0,
                utility_key="age_sa", utility_name="Acqua Gas Elettricità SA Chiasso",
                kwp=105.0, eigenverbrauch=True, hkn_optin=False, billing="quartal",
                base_model="fixed_flat",
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
        # v0.17.0 — utility line dropped the slug prefix.
        assert "Utility:** EKZ" in body
        assert "Utility:** Acqua Gas Elettricità SA Chiasso" in body
        # Per-group sub-bullets reflect per-row metadata.
        # v0.17.0 — model labels are now localised.
        assert "**Tariff model:** Reference market price (quarterly)" in body
        assert "**Tariff model:** Fixed flat rate" in body
        assert "Cap (Anrechenbarkeitsgrenze):** Active — cap 10.96 Rp/kWh" in body
        # v0.22.0 — cap activation = `cap_rp_kwh` set. age_sa group has no
        # cap (cap_rp_kwh is None), so its bullet block emits no cap line.
        # The "today" block (from _config_dict default) has cap_rp_kwh=10.96
        # → one occurrence; ekz group has cap_rp_kwh=10.96 → another. age_sa = 0.
        assert body.count("Cap (Anrechenbarkeitsgrenze):") == 2

    def test_grouping_preserves_newest_first_order(self):
        rows = [
            _row_with_meta("2026Q1", 11.0, 100.0, 11.0,
                           utility_key="ekz", kwp=8.0, eigenverbrauch=True,
                           hkn_optin=True, billing="quartal"),
            _row_with_meta("2025Q4", 8.0, 100.0, 8.0,
                           utility_key="age_sa", kwp=105.0, eigenverbrauch=True,
                           hkn_optin=False, billing="quartal"),
            _row_with_meta("2025Q3", 8.0, 100.0, 8.0,
                           utility_key="age_sa", kwp=105.0, eigenverbrauch=True,
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
                           utility_key="ekz", kwp=8.0, eigenverbrauch=True,
                           hkn_optin=True, billing="quartal"),
            _row_with_meta("2025Q4", 8.0, 335.38, 26.83,
                           base=8.0, hkn=0.0,
                           utility_key="age_sa", kwp=105.0, eigenverbrauch=True,
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
    """v0.9.9 — config-block rendering for rate-window seasonal markers.
    v0.16.1 — Notes are now hidden from the recompute notification;
    only the no-notes-rendered assertion is kept here, plus seasonal."""

    def test_v0_16_1_notes_never_rendered_even_when_present(self):
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(
            notes_active=[
                {"severity": "warning", "text": {"en": "Self-attest naturemade-star."}}
            ],
            notes_lang="en",
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Notes:**" not in body
        assert "Self-attest naturemade-star." not in body

    def test_config_block_no_notes_when_empty(self):
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(notes_active=None, notes_lang="en")
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Notes:**" not in body

    def test_config_block_seasonal_main_bullet_dropped(self):
        # v0.17.1 — Issue 8.3: the "Seasonal rates: Yes (summer: ... ; winter: ...)"
        # main bullet is dropped. Seasonal info is encoded in the tariff-model
        # name itself ("Fixpreis (saisonal)" / "Fixed flat rate (seasonal)")
        # and Sommer/Winter rate sub-bullets under Tariff model.
        rows = [_row("2026Q2", 10.00, 100.0, 10.00)]
        cfg = _config_dict(
            seasonal={
                "summer_months": [4, 5, 6, 7, 8, 9],
                "winter_months": [10, 11, 12, 1, 2, 3],
            }
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Seasonal rates:**" not in body

    def test_config_block_seasonal_main_bullet_dropped_when_explicit_none(self):
        # v0.17.1 — same as above; rate window with no seasonal block also
        # produces no main bullet. (Pre-0.17.1 emitted "Seasonal rates: No".)
        rows = [_row("2026Q2", 10.00, 100.0, 10.00)]
        cfg = _config_dict(seasonal=None)
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Seasonal rates:**" not in body


class TestBonusesConfigBlock:
    """v0.10.0 #3 Phase 1 (Batch C) — display-only bonus rendering in the
    active-today config block. No conditional evaluation yet."""

    def test_config_block_renders_always_and_opt_in_bonuses(self):
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(
            bonuses_active=[
                {"name": "Eco", "rate_rp_kwh": 1.50, "applies_when": "always"},
                {"name": "Winter+", "rate_rp_kwh": 0.80, "applies_when": "opt_in"},
            ],
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "- **Bonuses:**" in body
        assert "Eco: 1.50 Rp/kWh (immer)" in body
        assert "Winter+: 0.80 Rp/kWh (opt-in)" in body

    def test_config_block_omits_bonuses_when_none(self):
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(bonuses_active=None)
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Bonuses:**" not in body

    def test_config_block_omits_bonuses_when_empty_list(self):
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(bonuses_active=[])
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "**Bonuses:**" not in body

    def test_config_block_does_not_append_bonus_note(self):
        # v0.16.1 — bonus.note is intentionally dropped from the
        # notification (long German tariff-sheet text from utility data
        # would dominate the body). Bonus name + value + applies_when +
        # when-clause is enough.
        rows = [_row("2026Q1", 9.20, 100.0, 9.20)]
        cfg = _config_dict(
            bonuses_active=[
                {"name": "Snow", "rate_rp_kwh": 2.00,
                 "applies_when": "always", "note": "winter only"},
            ],
        )
        report = _RecomputeReport(rows=rows, quarters_recomputed=1, config=cfg)
        _, body = _format_recompute_notification(report)
        assert "Snow: 2.00 Rp/kWh (immer)" in body
        assert "winter only" not in body


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


class TestPeriodTableBonusColumn:
    """v0.11.0 (Batch D) — optional Bonus column appears between HKN and
    Total when any row has a non-zero applied bonus. Otherwise the legacy
    6-column layout is preserved bytewise."""

    def test_bonus_column_collapses_when_all_rows_zero(self):
        from custom_components.bfe_rueckliefertarif.services import (
            _render_period_table,
        )

        # Default rows have bonus_rp_kwh_avg=None → column collapses.
        rows = [
            _row("2026-03", 13.27, 100.0, 13.27, base=10.27, hkn=3.00),
            _row("2026-02", 13.27, 100.0, 13.27, base=10.27, hkn=3.00),
        ]
        lines, _ = _render_period_table(rows)
        header = next(line for line in lines if line.startswith("| Period"))
        assert "Bonus" not in header
        # 6-column layout: 5 separators between cells.
        assert header.count("|") == 7  # leading + 6 separators + trailing

    def test_bonus_column_appears_when_any_row_nonzero(self):
        from custom_components.bfe_rueckliefertarif.services import (
            _render_period_table,
        )

        rows = [
            _RecomputeReportRow(
                period="2026-03",
                rate_rp_kwh_avg=13.77,
                base_rp_kwh_avg=10.27, hkn_rp_kwh_avg=3.00,
                intended_hkn_rp_kwh=None,
                total_kwh=100.0, total_chf=13.77,
                bonus_rp_kwh_avg=0.50,
            ),
            _RecomputeReportRow(
                period="2026-02",
                rate_rp_kwh_avg=13.27,
                base_rp_kwh_avg=10.27, hkn_rp_kwh_avg=3.00,
                intended_hkn_rp_kwh=None,
                total_kwh=100.0, total_chf=13.27,
                bonus_rp_kwh_avg=0.0,
            ),
        ]
        lines, _ = _render_period_table(rows)
        header = next(line for line in lines if line.startswith("| Period"))
        assert "Bonus" in header
        # 7-column layout.
        assert header.count("|") == 8
        # Row with nonzero bonus shows the value; row with zero bonus is blank.
        body_lines = [line for line in lines if line.startswith("| 2026-")]
        assert "0.500" in body_lines[0]  # 2026-03 row with bonus
        # 2026-02 row's bonus cell is blank — check that "0.000" doesn't appear.
        assert "0.000" not in body_lines[1]

    def test_subrow_bonus_triggers_column(self):
        # When the main row has zero bonus but a sub-row has nonzero, the
        # column still appears.
        from custom_components.bfe_rueckliefertarif.services import (
            _PeriodSubRow,
            _render_period_table,
        )

        row = _RecomputeReportRow(
            period="2026-Q1",
            rate_rp_kwh_avg=13.27,
            base_rp_kwh_avg=10.27, hkn_rp_kwh_avg=3.00,
            intended_hkn_rp_kwh=None,
            total_kwh=100.0, total_chf=13.27,
            bonus_rp_kwh_avg=0.0,
            sub_rows=(
                _PeriodSubRow(
                    label="winter",
                    base_rp_kwh_avg=10.27, hkn_rp_kwh_avg=3.00,
                    rate_rp_kwh_avg=13.77, total_kwh=50.0, total_chf=6.89,
                    bonus_rp_kwh_avg=0.50,
                ),
                _PeriodSubRow(
                    label="summer",
                    base_rp_kwh_avg=10.27, hkn_rp_kwh_avg=3.00,
                    rate_rp_kwh_avg=13.27, total_kwh=50.0, total_chf=6.64,
                    bonus_rp_kwh_avg=0.0,
                ),
            ),
        )
        lines, _ = _render_period_table([row])
        header = next(line for line in lines if line.startswith("| Period"))
        assert "Bonus" in header

    def test_bonus_aggregation_kwh_weighted(self):
        # Two records, kwh-weighted bonus average.
        from dataclasses import replace
        records = [
            _hr(_hour(2026, 1, 15, 10), 10.0, 13.0, base_rp_kwh=10.0, hkn_rp_kwh=3.0),
            _hr(_hour(2026, 1, 15, 11), 10.0, 13.0, base_rp_kwh=10.0, hkn_rp_kwh=3.0),
        ]
        # Stamp bonus + recompute rate_rp_kwh + compensation_chf.
        records = [
            replace(
                records[0],
                rate_rp_kwh=14.0,
                compensation_chf=records[0].kwh * 14.0 / 100.0,
                bonus_rp_kwh=1.0,
            ),
            replace(
                records[1],
                rate_rp_kwh=15.0,
                compensation_chf=records[1].kwh * 15.0 / 100.0,
                bonus_rp_kwh=2.0,
            ),
        ]
        periods = _aggregate_by_period(records, ABRECHNUNGS_RHYTHMUS_QUARTAL)
        assert len(periods) == 1
        # bonus = (10 * 1.0 + 10 * 2.0) / 20 = 1.5
        assert periods[0]["bonus_rp_kwh_avg"] == pytest.approx(1.5)


# ----- v0.16.0 — Issue 2: user_inputs / bonuses_active rendering -------------


class TestUserInputsRendering:
    """v0.16.0: ``Active user inputs:`` line surfaces the resolved
    user_inputs dict in the recompute notification's config block.
    """

    def test_config_block_renders_user_inputs_line(self):
        # v0.17.1: each user_input is its own sub-bullet under Configuration
        # (replaces the pre-0.17.1 collated "Active user inputs:" one-liner).
        cfg = _config_dict(user_inputs={
            "regio_top40_opted_in": True,
            "ekz_segment": "kleq30_mit_ev",
        })
        lines = _render_config_block(cfg)
        # Sorted alphabetically — ekz_segment first, regio_top40 second.
        assert any(
            line == "    - **ekz_segment:** kleq30_mit_ev" for line in lines
        )
        assert any(
            line == "    - **regio_top40_opted_in:** True" for line in lines
        )

    def test_config_block_omits_user_inputs_when_empty(self):
        cfg = _config_dict(user_inputs={})
        lines = _render_config_block(cfg)
        # No user_input sub-bullets when the dict is empty.
        assert not any("regio_top40" in line or "ekz_segment" in line for line in lines)

    def test_config_block_omits_user_inputs_when_missing_key(self):
        # Legacy snapshot — no `user_inputs` key at all.
        cfg = _config_dict()
        lines = _render_config_block(cfg)
        assert not any("regio_top40" in line or "ekz_segment" in line for line in lines)

    def test_config_block_omits_user_inputs_when_not_a_dict(self):
        # Defensive — corrupted snapshot stored a list.
        cfg = _config_dict(user_inputs=["broken"])
        lines = _render_config_block(cfg)
        assert not any("regio_top40" in line or "ekz_segment" in line for line in lines)


# ----- v0.17.1 — Issue 8.5: grouped Configuration layout ----------------------


class TestGroupedConfigurationLayout:
    """v0.17.1 — Issue 8.5: user-defined values (Installed power,
    Self-consumption, HKN opt-in, user_inputs) are grouped under a
    `**Configuration:**` parent at the TOP of the bullet list. Utility/
    tariff descriptors (Utility, Tariff model, Federal floor, Cap mode,
    Tariff data) follow as top-level bullets.
    """

    def test_configuration_parent_emitted_above_utility(self):
        cfg = _config_dict()
        lines = _render_config_block(cfg)
        body = "\n".join(lines)
        cfg_idx = body.find("- **Configuration:**")
        utility_idx = body.find("- **Utility:**")
        assert cfg_idx != -1, "Configuration parent missing"
        assert utility_idx != -1, "Utility line missing"
        assert cfg_idx < utility_idx, (
            "Configuration parent must precede Utility line"
        )

    def test_configuration_subs_use_4space_indent(self):
        cfg = _config_dict()
        lines = _render_config_block(cfg)
        # All sub-bullets under Configuration use 4-space indent (matches
        # _render_tariff_model_lines indentation; valid CommonMark nesting).
        sub_lines = [
            line for line in lines
            if line.startswith("    - **") and (
                "Installed power" in line
                or "Self-consumption" in line
                or "HKN opt-in" in line
            )
        ]
        assert len(sub_lines) >= 2, (
            f"Expected ≥2 indented sub-bullets, got: {sub_lines}"
        )

    def test_billing_period_main_bullet_dropped(self):
        # v0.17.1 — Issue 8.2: "Billing period: quartal" main bullet is gone.
        # The period info is in the Tariff-model sub-bullet (Abrechnungsperiode)
        # for fixed_* models or in the model name for rmp_*.
        cfg = _config_dict(billing="quartal", base_model="fixed_flat",
                           settlement_period="quartal")
        lines = _render_config_block(cfg)
        assert not any(line.startswith("- **Billing period:") for line in lines)

    def test_user_inputs_become_individual_subs_under_configuration(self):
        decls = [{"key": "regio_top40_opted_in", "type": "boolean",
                  "label_de": "Wahltarif TOP-40 abonniert"}]
        cfg = _config_dict(
            user_inputs={"regio_top40_opted_in": True},
            user_inputs_decl=decls,
            notes_lang="de",
        )
        lines = _render_config_block(cfg)
        # The pre-0.17.1 collated "Active user inputs:" line is gone.
        assert not any("Active user inputs:" in line for line in lines)
        # The localised label appears as its own sub-bullet under Configuration.
        assert any(
            line == "    - **Wahltarif TOP-40 abonniert:** Ja" for line in lines
        )

    def test_user_input_sub_appears_after_hkn_in_configuration(self):
        # Within Configuration, ordering is: Installed power, Self-consumption,
        # HKN opt-in/structure, then user_inputs (alphabetical by key).
        decls = [{"key": "regio_top40_opted_in", "type": "boolean",
                  "label_de": "Wahltarif TOP-40 abonniert"}]
        cfg = _config_dict(
            hkn_structure="additive_optin", hkn_optin=True, hkn_rp_kwh=4.0,
            user_inputs={"regio_top40_opted_in": True},
            user_inputs_decl=decls,
        )
        lines = _render_config_block(cfg)
        body = "\n".join(lines)
        kw_idx = body.find("**Installed power:**")
        hkn_idx = body.find("**HKN opt-in:**")
        ui_idx = body.find("**Wahltarif TOP-40 abonniert:**")
        utility_idx = body.find("- **Utility:**")
        assert kw_idx < hkn_idx < ui_idx < utility_idx

    def test_per_group_heading_includes_bonuses_from_sample_row(self):
        # Two groups, each with a sample bonus declaration. Both groups'
        # per-group blocks should render the Bonuses section.
        bonus_a = [{"kind": "additive_rp_kwh", "rate_rp_kwh": 0.5,
                    "name": "Boost A", "applies_when": "always"}]
        bonus_b = [{"kind": "multiplier_pct", "multiplier_pct": 108.0,
                    "name": "Boost B", "applies_when": "always"}]
        rows = [
            _RecomputeReportRow(
                period="2026Q1", rate_rp_kwh_avg=10.0, base_rp_kwh_avg=10.0,
                hkn_rp_kwh_avg=0.0, intended_hkn_rp_kwh=None,
                total_kwh=100.0, total_chf=10.0,
                utility_key_at_period="ekz", utility_name_at_period="EKZ",
                kw_at_period=8.0, eigenverbrauch_at_period=True,
                hkn_optin_at_period=False, billing_at_period="quartal",
                base_model_at_period="rmp_quartal",
                bonuses_active_at_period=bonus_a,
            ),
            _RecomputeReportRow(
                period="2025Q4", rate_rp_kwh_avg=8.0, base_rp_kwh_avg=8.0,
                hkn_rp_kwh_avg=0.0, intended_hkn_rp_kwh=None,
                total_kwh=80.0, total_chf=6.4,
                utility_key_at_period="regio_energie_solothurn",
                utility_name_at_period="Regio",
                kw_at_period=8.0, eigenverbrauch_at_period=True,
                hkn_optin_at_period=False, billing_at_period="quartal",
                base_model_at_period="fixed_flat",
                bonuses_active_at_period=bonus_b,
            ),
        ]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=2, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "Boost A" in body
        assert "Boost B" in body


# ----- v0.16.0 — Issue 3: EV-relevance annotation ----------------------------


class TestEigenverbrauchAnnotation:
    """v0.17.1 — Issue 8.1: Self-consumption line is suppressed entirely
    when ``self_consumption_relevant()`` returns False. Pre-v0.17.1 the line
    was always emitted with a "(no effect on rates)" annotation; that
    annotation is now gone (the absent line carries the same signal).

    Also v0.17.1: line label is now "Self-consumption" (no "Eigenverbrauch"
    prefix) and lives under the Configuration parent as a sub-bullet.
    """

    @staticmethod
    def _ev_line(lines):
        return next(
            (line for line in lines if "**Self-consumption:**" in line),
            None,
        )

    def test_ev_line_emitted_when_relevant(self):
        # EKZ at 50 kW falls in federal floor 30-150 kW band where rules
        # differ on self_consumption — predicate returns True → line emitted.
        cfg = _config_dict(
            utility_key="ekz", valid_from="2026-04-01", kwp=50.0,
            eigenverbrauch=True,
        )
        lines = _render_config_block(cfg)
        ev_line = self._ev_line(lines)
        assert ev_line is not None
        assert ev_line.endswith("Yes")
        # 4-space sub-bullet indent under Configuration
        assert ev_line.startswith("    - ")

    def test_ev_line_suppressed_when_irrelevant(self, monkeypatch):
        # Synthetic tariff: federal floor uses self_consumption=null AND
        # the utility has no cap_rules → predicate returns False → line
        # is dropped entirely (Issue 8.1).
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2025-01-01", "valid_to": None,
                "rules": [{"kw_min": 0, "kw_max": None,
                           "self_consumption": None, "min_rp_kwh": 4.0}],
            }],
            "utilities": {
                "syn": {"name_de": "Syn", "homepage": "https://example.test",
                        "rates": [{
                            "valid_from": "2025-01-01", "valid_to": None,
                            "settlement_period": "quartal",
                            "power_tiers": [{"kw_min": 0, "kw_max": None,
                                             "base_model": "fixed_flat",
                                             "fixed_rp_kwh": 8.0,
                                             "hkn_rp_kwh": 0.0,
                                             "hkn_structure": "none"}],
                        }]},
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        cfg = _config_dict(
            utility_key="syn", valid_from="2025-04-01", kwp=10.0,
            eigenverbrauch=True,
        )
        lines = _render_config_block(cfg)
        assert self._ev_line(lines) is None

    def test_ev_line_emitted_when_valid_from_missing(self):
        # Legacy snapshot lacks `valid_from` → relevance can't be computed →
        # permissive default keeps the line (no regression).
        cfg = _config_dict(eigenverbrauch=True)
        cfg.pop("valid_from", None)
        lines = _render_config_block(cfg)
        ev_line = self._ev_line(lines)
        assert ev_line is not None
        assert ev_line.endswith("Yes")

    def test_ev_line_suppressed_when_ev_is_none(self):
        # v0.17.1: ev=None means the user hasn't set EV; suppress entirely
        # rather than emit a "—" placeholder under Configuration.
        cfg = _config_dict(eigenverbrauch=None)
        lines = _render_config_block(cfg)
        assert self._ev_line(lines) is None


# ----- v0.16.0 — Issue 4.1: fingerprint canonicalization ---------------------


class TestFingerprintCoercion:
    """v0.16.0: today_fingerprint and per-row fingerprints both go through
    ``_canon_fingerprint``, so int-vs-float / bool-vs-int / None-vs-missing
    drifts collapse to the same key — single-period running-quarter
    notifications no longer duplicate the active-config block.
    """

    def test_int_kw_matches_float_kw_in_today_block(self):
        # Snapshot has kw_at_period=8 (int); header has kwp=8.0 (float).
        # Without canonicalizer, tuple eq passes (Python (8,)==(8.0,)),
        # but with bool-vs-int drift this still tests the canonicalizer
        # is in effect.
        rows = [_row_with_meta(
            "2026Q1", 6.20, 1000.0, 62.0,
            utility_key="ekz", utility_name="EKZ",
            kwp=8, eigenverbrauch=True, hkn_optin=False, billing="quartal",
            base_model="fixed_flat",
        )]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(
                utility_key="ekz", kwp=8.0, eigenverbrauch=True,
                hkn_optin=False, billing="quartal",
            ),
        )
        _, body = _format_recompute_notification(report)
        # Per-group heading suppressed → only the active-today block.
        assert "## Active configuration (today)" in body
        assert "## Configuration in effect" not in body

    def test_genuine_config_change_keeps_per_group_heading(self):
        # Snapshot's EV differs from today's → fingerprints DO differ →
        # per-group heading renders.
        rows = [_row_with_meta(
            "2026Q1", 6.20, 1000.0, 62.0,
            utility_key="ekz", utility_name="EKZ",
            kwp=8.0, eigenverbrauch=False, hkn_optin=False, billing="quartal",
            base_model="fixed_flat",
        )]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1,
            config=_config_dict(
                utility_key="ekz", kwp=8.0, eigenverbrauch=True,
                hkn_optin=False, billing="quartal",
            ),
        )
        _, body = _format_recompute_notification(report)
        assert "## Configuration in effect" in body

    def test_settlement_period_threaded_into_per_group_heading(self):
        # Multi-group report — per-group heading should now show
        # settlement suffix (was hard-coded None pre-v0.16.0).
        rows = [
            _RecomputeReportRow(
                period="2026Q1", rate_rp_kwh_avg=10.0, base_rp_kwh_avg=10.0,
                hkn_rp_kwh_avg=0.0, intended_hkn_rp_kwh=None,
                total_kwh=100.0, total_chf=10.0,
                utility_key_at_period="ekz", utility_name_at_period="EKZ",
                kw_at_period=8.0, eigenverbrauch_at_period=True,
                hkn_optin_at_period=False, billing_at_period="quartal",
                base_model_at_period="rmp_quartal",
                settlement_period_at_period="quartal",
            ),
            _RecomputeReportRow(
                period="2025Q4", rate_rp_kwh_avg=8.0, base_rp_kwh_avg=8.0,
                hkn_rp_kwh_avg=0.0, intended_hkn_rp_kwh=None,
                total_kwh=80.0, total_chf=6.4,
                utility_key_at_period="other", utility_name_at_period="Other",
                kw_at_period=8.0, eigenverbrauch_at_period=True,
                hkn_optin_at_period=False, billing_at_period="quartal",
                base_model_at_period="fixed_flat",
                settlement_period_at_period="quartal",
                fixed_rp_kwh_at_period=8.0,
            ),
        ]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=2, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        # v0.17.0 — per-group blocks render localised model labels and
        # settlement as a sub-bullet (for fixed_*; rmp_* encodes it in
        # the model name).
        assert "Tariff model:** Reference market price (quarterly)" in body
        assert "Tariff model:** Fixed flat rate" in body
        assert "    - Rate: 8.00 Rp/kWh" in body
        assert "    - Settlement period: Quarterly" in body


# ----- v0.16.0 — Issue 5: Tariff-model rate values ---------------------------


class TestTariffModelRateRendering:
    """v0.17.0: ``_render_tariff_model_lines`` emits a parent bullet plus
    localised sub-bullets for rates (and settlement period for fixed_*).
    Localised model labels — ``Fixpreis`` / ``Fixed flat rate`` etc.
    """

    def test_fixed_flat_plain_renders_rate_subbullet_de(self):
        lines = _render_tariff_model_lines({
            "base_model": "fixed_flat",
            "fixed_rp_kwh": 6.20,
            "settlement_period": "quartal",
            "notes_lang": "de",
        })
        assert lines == [
            "- **Tariff model:** Fixpreis",
            "    - Tarif: 6.20 Rp/kWh",
            "    - Abrechnungsperiode: Quartal",
        ]

    def test_fixed_flat_seasonal_renders_summer_winter_de(self):
        lines = _render_tariff_model_lines({
            "base_model": "fixed_flat",
            "fixed_rp_kwh": 7.6,
            "seasonal": {
                "summer_rp_kwh": 6.20,
                "winter_rp_kwh": 9.00,
            },
            "settlement_period": "quartal",
            "notes_lang": "de",
        })
        assert lines == [
            "- **Tariff model:** Fixpreis (saisonal)",
            "    - Sommer: 6.20 Rp/kWh",
            "    - Winter: 9.00 Rp/kWh",
            "    - Abrechnungsperiode: Quartal",
        ]

    def test_fixed_ht_nt_plain_de(self):
        lines = _render_tariff_model_lines({
            "base_model": "fixed_ht_nt",
            "fixed_ht_rp_kwh": 12.34,
            "fixed_nt_rp_kwh": 5.67,
            "settlement_period": "quartal",
            "notes_lang": "de",
        })
        assert lines == [
            "- **Tariff model:** Fixpreis (HT/NT)",
            "    - HT: 12.34 Rp/kWh",
            "    - NT: 5.67 Rp/kWh",
            "    - Abrechnungsperiode: Quartal",
        ]

    def test_fixed_ht_nt_seasonal_4_subbullets(self):
        lines = _render_tariff_model_lines({
            "base_model": "fixed_ht_nt",
            "fixed_ht_rp_kwh": 12.34,
            "fixed_nt_rp_kwh": 5.67,
            "seasonal": {
                "summer_ht_rp_kwh": 12.34,
                "winter_ht_rp_kwh": 14.00,
                "summer_nt_rp_kwh": 5.67,
                "winter_nt_rp_kwh": 6.50,
            },
            "settlement_period": "quartal",
            "notes_lang": "de",
        })
        assert lines == [
            "- **Tariff model:** Fixpreis (HT/NT, saisonal)",
            "    - HT Sommer: 12.34 Rp/kWh",
            "    - HT Winter: 14.00 Rp/kWh",
            "    - NT Sommer: 5.67 Rp/kWh",
            "    - NT Winter: 6.50 Rp/kWh",
            "    - Abrechnungsperiode: Quartal",
        ]

    def test_rmp_quartal_renders_only_model_line_no_subbullets(self):
        # rmp_*: settlement is encoded in the model name, no sub-bullets.
        lines = _render_tariff_model_lines({
            "base_model": "rmp_quartal",
            "settlement_period": "quartal",
            "notes_lang": "en",
        })
        assert lines == ["- **Tariff model:** Reference market price (quarterly)"]

    def test_legacy_snapshot_no_fixed_fields(self):
        # base_model known but no rate fields (pre-v0.16.0 cache) — should
        # gracefully render model + settlement only, no crash.
        lines = _render_tariff_model_lines({
            "base_model": "fixed_flat",
            "settlement_period": "stunde",
            "notes_lang": "de",
        })
        assert lines == [
            "- **Tariff model:** Fixpreis",
            "    - Abrechnungsperiode: Stunde",
        ]

    def test_returns_empty_when_base_model_missing(self):
        assert _render_tariff_model_lines({"settlement_period": "quartal"}) == []
        assert _render_tariff_model_lines({}) == []

    def test_unknown_lang_falls_back_to_english(self):
        lines = _render_tariff_model_lines({
            "base_model": "fixed_flat",
            "fixed_rp_kwh": 6.20,
            "settlement_period": "quartal",
            "notes_lang": "fr",
        })
        assert lines[0] == "- **Tariff model:** Fixed flat rate"
        assert "    - Rate: 6.20 Rp/kWh" in lines
        assert "    - Settlement period: Quarterly" in lines


# ----- v0.16.1 — user_inputs label translation -------------------------------


_REGIO_DECL = {
    "key": "regio_top40_opted_in",
    "type": "boolean",
    "default": False,
    "label_de": "Wahltarif TOP-40 abonniert",
    "label_en": "Wahltarif TOP-40 subscribed",
}

_ENUM_DECL = {
    "key": "ekz_segment",
    "type": "enum",
    "default": "kleq30_mit_ev",
    "label_de": "Segment",
    "label_en": "Segment",
    "values": ["kleq30_mit_ev", "kleq30_ohne_ev"],
    "value_labels_de": {
        "kleq30_mit_ev": "≤30 kW mit Eigenverbrauch",
        "kleq30_ohne_ev": "≤30 kW ohne Eigenverbrauch",
    },
    "value_labels_en": {
        "kleq30_mit_ev": "≤30 kW with self-consumption",
        "kleq30_ohne_ev": "≤30 kW without self-consumption",
    },
}


class TestUserInputsLabelTranslation:
    """v0.16.1 — Active user inputs line uses ``label_de`` /
    ``value_labels_de`` from the rate window's declarations.
    """

    def test_uses_label_de_and_yes_no_for_boolean(self):
        cfg = _config_dict(
            user_inputs={"regio_top40_opted_in": False},
            user_inputs_decl=[_REGIO_DECL],
            notes_lang="de",
        )
        lines = _render_config_block(cfg)
        # v0.17.1: each user_input is its own sub-bullet under Configuration
        # (replaces the pre-0.17.1 collated "Active user inputs:" line).
        assert any(
            line == "    - **Wahltarif TOP-40 abonniert:** Nein" for line in lines
        )

    def test_uses_label_en_and_yes_no_for_boolean(self):
        cfg = _config_dict(
            user_inputs={"regio_top40_opted_in": True},
            user_inputs_decl=[_REGIO_DECL],
            notes_lang="en",
        )
        lines = _render_config_block(cfg)
        assert any(
            line == "    - **Wahltarif TOP-40 subscribed:** Yes" for line in lines
        )

    def test_uses_value_labels_for_enum(self):
        cfg = _config_dict(
            user_inputs={"ekz_segment": "kleq30_mit_ev"},
            user_inputs_decl=[_ENUM_DECL],
            notes_lang="de",
        )
        lines = _render_config_block(cfg)
        assert any(
            line == "    - **Segment:** ≤30 kW mit Eigenverbrauch" for line in lines
        )

    def test_falls_back_to_key_when_no_decl(self):
        cfg = _config_dict(
            user_inputs={"unknown_key": True},
            user_inputs_decl=None,
        )
        lines = _render_config_block(cfg)
        # No decl → raw key, raw value (str(bool) → "True").
        assert any(line == "    - **unknown_key:** True" for line in lines)


# ----- v0.16.1 — HKN-structure-aware line ------------------------------------


class TestHknStructureGating:
    """v0.16.1 — Issue 3: HKN line uses ``hkn_structure`` to render the
    correct phrasing for utilities where opt-in isn't a user choice.
    """

    def test_additive_optin_yes(self):
        cfg = _config_dict(
            hkn_structure="additive_optin", hkn_optin=True, hkn_rp_kwh=3.00,
        )
        lines = _render_config_block(cfg)
        assert any(
            "HKN opt-in:** Yes (3.00 Rp/kWh additive)" in line
            for line in lines
        )

    def test_additive_optin_no(self):
        cfg = _config_dict(hkn_structure="additive_optin", hkn_optin=False)
        lines = _render_config_block(cfg)
        assert any("HKN opt-in:** No" in line for line in lines)

    def test_bundled_renders_dedicated_line(self):
        cfg = _config_dict(
            hkn_structure="bundled", hkn_optin=False, hkn_rp_kwh=None,
        )
        lines = _render_config_block(cfg)
        assert any(
            "HKN:** bundled in base rate (no opt-in available)" in line
            for line in lines
        )
        # The misleading "Yes/No" line must NOT appear for bundled.
        assert not any("HKN opt-in:**" in line for line in lines)

    def test_none_renders_dedicated_line(self):
        cfg = _config_dict(
            hkn_structure="none", hkn_optin=False, hkn_rp_kwh=None,
        )
        lines = _render_config_block(cfg)
        assert any("HKN:** not paid by utility" in line for line in lines)
        assert not any("HKN opt-in:**" in line for line in lines)

    def test_legacy_snapshot_falls_back_to_yes_no(self):
        # No `hkn_structure` key in cfg — pre-v0.16.1 snapshot path.
        cfg = _config_dict(hkn_optin=True, hkn_rp_kwh=4.00)
        cfg.pop("hkn_structure", None)
        lines = _render_config_block(cfg)
        assert any(
            "HKN opt-in:** Yes (4.00 Rp/kWh additive)" in line
            for line in lines
        )


# ----- v0.16.1 — Bonus % formatting + label-aware when-clause ----------------


class TestBonusPercentFormatting:
    """v0.16.1 — Issue 5: ``multiplier_pct=108`` renders as ``+8.00%``
    (uplift), ``multiplier_pct=85`` as ``−15.00%`` (curtailment); long
    ``note`` field is dropped; when-clause keys/values use labels.
    """

    def test_multiplier_pct_uplift_renders_as_plus_percent(self):
        lines = _render_bonuses_lines([
            {"kind": "multiplier_pct", "name": "TOP-40",
             "multiplier_pct": 108.0, "applies_when": "opt_in"},
        ])
        assert any("TOP-40: +8.00% (opt-in)" in line for line in lines)

    def test_multiplier_pct_curtailment_renders_as_minus_percent(self):
        lines = _render_bonuses_lines([
            {"kind": "multiplier_pct", "name": "Curtail-15",
             "multiplier_pct": 85.0, "applies_when": "always"},
        ])
        assert any("Curtail-15: −15.00% (immer)" in line for line in lines)

    def test_bonus_note_is_dropped(self):
        lines = _render_bonuses_lines([
            {"kind": "additive_rp_kwh", "name": "Snow",
             "rate_rp_kwh": 2.00, "applies_when": "always",
             "note": "long German note that should not appear"},
        ])
        rendered = "\n".join(lines)
        assert "Snow: 2.00 Rp/kWh (immer)" in rendered
        assert "long German note" not in rendered

    def test_bonus_when_clause_dropped_v0_17_0(self):
        # v0.17.0 — the when-clause is no longer rendered. The bonus's
        # current state is shown via the ``Active user inputs:`` line above
        # the Bonuses block; rendering the bonus's *condition* alongside
        # confused users (e.g. ``when ...=Ja`` while the user actually has
        # Nein in their config). Issue 2 / v0.17.0.
        lines = _render_bonuses_lines(
            [
                {"kind": "multiplier_pct", "name": "TOP-40",
                 "multiplier_pct": 108.0, "applies_when": "opt_in",
                 "when": {"user_inputs": {"regio_top40_opted_in": True}}},
            ],
            decls=[_REGIO_DECL],
            lang="de",
        )
        rendered = "\n".join(lines)
        # Bonus name + percent + applies_when annotation only.
        assert "TOP-40: +8.00% (opt-in)" in rendered
        # No when-clause leak.
        assert "when " not in rendered
        assert "Wahltarif TOP-40 abonniert" not in rendered

    def test_season_winter_only_renders_winter_label(self):
        # v0.23.3 — season-gated bonus without applies_when (e.g. AEW
        # Spezialbonus) renders ``(Winter)``, matching the JS card.
        lines = _render_bonuses_lines([
            {"kind": "additive_rp_kwh", "name": "Spezialbonus",
             "rate_rp_kwh": 15.00,
             "when": {"season": "winter"}},
        ])
        assert any("Spezialbonus: 15.00 Rp/kWh (Winter)" in line for line in lines)

    def test_season_summer_only_renders_summer_label(self):
        lines = _render_bonuses_lines([
            {"kind": "additive_rp_kwh", "name": "SommerPlus",
             "rate_rp_kwh": 3.00,
             "when": {"season": "summer"}},
        ])
        assert any("SommerPlus: 3.00 Rp/kWh (Sommer)" in line for line in lines)

    def test_user_inputs_only_renders_bedingt_label(self):
        # User-input-gated bonus without ``applies_when`` renders
        # ``(bedingt)`` — surfaces that the bonus is conditional without
        # leaking the user_inputs values themselves.
        lines = _render_bonuses_lines([
            {"kind": "additive_rp_kwh", "name": "Cond",
             "rate_rp_kwh": 1.00,
             "when": {"user_inputs": {"foo": "bar"}}},
        ])
        assert any("Cond: 1.00 Rp/kWh (bedingt)" in line for line in lines)

    def test_always_with_winter_renders_winter_label_only(self):
        # ``applies_when=always`` + season-gating: just ``(Winter)``.
        # Old behavior was ``(always)``; the season tag is more accurate.
        lines = _render_bonuses_lines([
            {"kind": "additive_rp_kwh", "name": "AutoWinter",
             "rate_rp_kwh": 5.00, "applies_when": "always",
             "when": {"season": "winter"}},
        ])
        assert any("AutoWinter: 5.00 Rp/kWh (Winter)" in line for line in lines)

    def test_opt_in_with_winter_combines_labels(self):
        # Opt-in + season → combined ``(opt-in, Winter)``. ``bedingt`` is
        # suppressed when ``opt-in`` is set (opt-in already implies a
        # user-input gate; doubling reads as noise).
        lines = _render_bonuses_lines([
            {"kind": "additive_rp_kwh", "name": "OptWinter",
             "rate_rp_kwh": 4.00, "applies_when": "opt_in",
             "when": {"season": "winter",
                      "user_inputs": {"foo": "bar"}}},
        ])
        assert any("OptWinter: 4.00 Rp/kWh (opt-in, Winter)" in line for line in lines)

    def test_when_summary_label_translates_enum_value(self):
        s = _render_when_summary(
            {"user_inputs": {"ekz_segment": "kleq30_mit_ev"}},
            decls=[_ENUM_DECL],
            lang="de",
        )
        assert s == "Segment=≤30 kW mit Eigenverbrauch"


# ----- v0.16.1 — Per-group suppression for multi-group reports ---------------


class TestPerGroupSuppression:
    """v0.16.1 — Issue 4.1: when one of multiple groups matches today's
    fingerprint, only its data table renders (no second config block);
    the active-today block already covered the config.
    """

    def test_two_groups_one_matches_today(self):
        # Group A: matches today's config (Regio).
        # Group B: different (EWZ).
        rows = [
            _row_with_meta(
                "2026Q2", 6.20, 100.0, 6.20,
                utility_key="regio_energie_solothurn",
                utility_name="Regio",
                kwp=8.0, eigenverbrauch=True, hkn_optin=False, billing="quartal",
                base_model="fixed_flat",
            ),
            _row_with_meta(
                "2026Q1", 9.50, 200.0, 19.00,
                utility_key="ewz", utility_name="EWZ",
                kwp=8.0, eigenverbrauch=True, hkn_optin=False, billing="quartal",
                base_model="fixed_ht_nt",
            ),
        ]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=2,
            config=_config_dict(
                utility_key="regio_energie_solothurn",
                utility_name="Regio Energie Solothurn",
                kwp=8.0, eigenverbrauch=True, hkn_optin=False, billing="quartal",
                base_model="fixed_flat",
            ),
        )
        _, body = _format_recompute_notification(report)
        # Active-today renders Regio once.
        assert "## Active configuration (today)" in body
        # Today-matching group merges into "Per-period results (active config)".
        assert "## Per-period results (active config)" in body
        # The today-matching group MUST NOT also render as
        # "Configuration in effect: ..." (the duplication v0.16.0 still had).
        # The EWZ group does still render its full config block.
        config_in_effect_count = body.count("## Configuration in effect:")
        assert config_in_effect_count == 1, (
            f"expected 1 'Configuration in effect' (EWZ only), got "
            f"{config_in_effect_count}; body:\n{body}"
        )

    def test_single_group_matching_today_keeps_legacy_per_period_results(self):
        # Single group case keeps the v0.16.0 heading (no "active config"
        # qualifier needed when there's only one group).
        rows = [_row_with_meta(
            "2026Q1", 13.27, 1000.0, 132.70, base=10.27, hkn=3.00,
            utility_key="ekz", utility_name="EKZ",
            kwp=25.0, eigenverbrauch=True, hkn_optin=True, billing="quartal",
        )]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "## Per-period results" in body
        assert "## Per-period results (active config)" not in body
        assert "## Configuration in effect:" not in body


# ----- v0.17.0 — Issue 1: canon fingerprint user_inputs ---------------------


class TestCanonFingerprintUserInputs:
    """v0.17.0 — Issue 1 fix on the report-grouping side: differing
    user_inputs (e.g. one period had TOP-40 opted-in, another opted-out)
    must produce different fingerprints so the per-period grouping can
    visualise the change.
    """

    def test_canon_fingerprint_includes_user_inputs(self):
        a = _canon_fingerprint(
            "u", 8.0, True, True, "quartal",
            {"regio_top40_opted_in": True},
        )
        b = _canon_fingerprint(
            "u", 8.0, True, True, "quartal",
            {"regio_top40_opted_in": False},
        )
        assert a != b

    def test_canon_fingerprint_user_inputs_order_invariant(self):
        a = _canon_fingerprint(
            "u", 8.0, True, True, "quartal",
            {"a": 1, "b": 2},
        )
        b = _canon_fingerprint(
            "u", 8.0, True, True, "quartal",
            {"b": 2, "a": 1},
        )
        assert a == b

    def test_canon_fingerprint_none_and_empty_dict_equal(self):
        # Backwards-compat with pre-v0.16.0 snapshots where user_inputs
        # field is absent: must match today's empty-dict config.
        a = _canon_fingerprint("u", 8.0, True, True, "quartal", None)
        b = _canon_fingerprint("u", 8.0, True, True, "quartal", {})
        assert a == b

    def test_canon_fingerprint_default_user_inputs_arg(self):
        # Old call sites that don't pass user_inputs still work and produce
        # the same canon as passing None.
        a = _canon_fingerprint("u", 8.0, True, True, "quartal")
        b = _canon_fingerprint("u", 8.0, True, True, "quartal", None)
        assert a == b


class TestTodayBlockSuppression:
    """v0.18.0 Issue 8.6 — _should_emit_today_block suppresses the
    'Active configuration (today)' header when today's date falls outside
    every recomputed period. Editing a past-quarter transition fires
    recompute over that quarter only; today's config is irrelevant noise."""

    def test_today_block_emitted_when_today_inside_range(self, monkeypatch):
        # autouse fixture sets today=2026-01-15; row 2026-Q1 covers it.
        rows = [_row("2026Q1", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "## Active configuration (today)" in body

    def test_today_block_suppressed_when_editing_past_quarter(
        self, monkeypatch
    ):
        # Override autouse fixture: today is well past the recomputed
        # range (user edited a past quarter; today is months later).
        from datetime import date as _date
        real_date = _date

        class _Today(_date):
            @classmethod
            def today(cls):
                return real_date(2026, 6, 15)

        import custom_components.bfe_rueckliefertarif.services as _svc
        monkeypatch.setattr(_svc, "date", _Today, raising=False)

        rows = [_row("2026Q1", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "## Active configuration (today)" not in body
        # Per-group heading still emits with the period's date range so
        # the user knows what was recomputed.
        assert "## Configuration in effect: 2026-01-01 → 2026-03-31" in body

    def test_today_block_suppressed_when_no_rows(self):
        # Defensive: empty rows shouldn't render today block either.
        report = _RecomputeReport(
            rows=[], quarters_recomputed=0, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "## Active configuration (today)" not in body

    def test_today_block_emitted_for_running_quarter(self, monkeypatch):
        # User edits the running quarter's transition → recompute fires
        # for the running quarter only → today is inside that quarter
        # → today block emitted (running estimate is useful context).
        from datetime import date as _date
        real_date = _date

        class _Today(_date):
            @classmethod
            def today(cls):
                return real_date(2026, 5, 1)  # in 2026-Q2

        import custom_components.bfe_rueckliefertarif.services as _svc
        monkeypatch.setattr(_svc, "date", _Today, raising=False)

        rows = [_row("2026Q2", 10.0, 100.0, 10.0)]
        report = _RecomputeReport(
            rows=rows, quarters_recomputed=1, config=_config_dict()
        )
        _, body = _format_recompute_notification(report)
        assert "## Active configuration (today)" in body
