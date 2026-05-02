"""v0.21.11 — running-quarter floor fallback in the chart history pipeline.

Targets the two pure helpers introduced in `services.py`:
- `_synthesize_fallback_prices` — per-base_model dispatch deciding when the
  federal Mindestvergütung floor stands in for missing BFE prices, and when
  that substitution counts as an estimate.
- `_quarter_from_period_string` — maps `_aggregate_by_period`'s period
  strings back to a `Quarter` so post-aggregation rows can be flagged.

End-to-end behaviour of `_compute_hour_records_for_quarters` is verified
manually on dev HA (per the v0.21.11 plan) — the fixture cost of mocking
`_cfg_for_entry` + `read_hourly_export` outweighs the regression-detection
value for what is a small surgical wrapper around already-tested helpers.
"""

from __future__ import annotations

import pytest

from custom_components.bfe_rueckliefertarif.bfe import BfePrice
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
)
from custom_components.bfe_rueckliefertarif.quarters import Month, Quarter
from custom_components.bfe_rueckliefertarif.services import (
    _quarter_from_period_string,
    _synthesize_fallback_prices,
)
from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff


def _resolved(base_model: str, *, floor: float = 6.00, hkn: float = 0.0) -> ResolvedTariff:
    """ResolvedTariff stub for the synthesis helper. Field defaults mirror
    `tests/test_recompute_report.py::_make_resolved` for consistency."""
    return ResolvedTariff(
        utility_key=f"test_{base_model}",
        valid_from="2026-01-01",
        settlement_period="quartal",
        base_model=base_model,
        fixed_rp_kwh=7.60 if base_model == "fixed_flat" else None,
        fixed_ht_rp_kwh=12.60 if base_model == "fixed_ht_nt" else None,
        fixed_nt_rp_kwh=11.60 if base_model == "fixed_ht_nt" else None,
        hkn_rp_kwh=hkn,
        hkn_structure="none",
        cap_rp_kwh=None,
        federal_floor_rp_kwh=floor,
        federal_floor_label="<30 kW",
        price_floor_rp_kwh=None,
        tariffs_json_version="1.0.0",
        tariffs_json_source="bundled",
    )


_Q = Quarter(2026, 2)  # the running quarter in our scenarios


class TestSynthesizeFallbackPricesUnpublishedQuarter:
    """Behaviour when the running quarter is missing from `coordinator.quarterly`."""

    def test_rmp_quartal_synthesizes_floor_and_flags_estimate(self):
        rt = _resolved("rmp_quartal")
        out_q, out_m, used = _synthesize_fallback_prices(
            rt, _Q, {}, None, ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )
        assert _Q in out_q
        # Floor 6.00 Rp/kWh ↔ 60 CHF/MWh.
        assert out_q[_Q].chf_per_mwh == pytest.approx(60.0)
        assert out_m is None
        assert used is True

    def test_rmp_monat_quartal_billed_synthesizes_quarterly_only(self):
        # rmp_monat × billing=quartal: the importer short-circuits on quarterly
        # price; monthly_prices is unused. Synthesis must NOT touch monthly.
        rt = _resolved("rmp_monat")
        out_q, out_m, used = _synthesize_fallback_prices(
            rt, _Q, {}, None, ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )
        assert _Q in out_q
        assert out_m is None
        assert used is True

    def test_rmp_monat_monat_billed_tops_up_all_three_months(self):
        # rmp_monat × billing=monat: importer reads monthly_prices for M1/M2,
        # derives M3. With ZERO published months, all three need a synthesized
        # entry or compute_quarter_plan_segmented raises PriceNotYetPublishedError.
        rt = _resolved("rmp_monat")
        out_q, out_m, used = _synthesize_fallback_prices(
            rt, _Q, {}, {}, ABRECHNUNGS_RHYTHMUS_MONAT,
        )
        assert _Q in out_q
        assert out_m is not None
        for m in _Q.months():
            assert m in out_m
            assert out_m[m].chf_per_mwh == pytest.approx(60.0)
        assert used is True

    def test_rmp_monat_monat_billed_preserves_published_months(self):
        # Partial: M1 published, M2/M3 missing. Real M1 price is preserved;
        # only M2/M3 get the floor. Whole quarter is still flagged as estimate
        # (any-fallback-in-quarter semantics — keeps the chart footnote logic
        # simple at the row-marking layer).
        rt = _resolved("rmp_monat")
        m1, m2, m3 = _Q.months()
        published_m1 = BfePrice(chf_per_mwh=88.5, days=30, volume_mwh=42.0)
        _out_q, out_m, used = _synthesize_fallback_prices(
            rt, _Q, {}, {m1: published_m1}, ABRECHNUNGS_RHYTHMUS_MONAT,
        )
        assert out_m[m1] is published_m1
        assert out_m[m2].chf_per_mwh == pytest.approx(60.0)
        assert out_m[m3].chf_per_mwh == pytest.approx(60.0)
        assert used is True

    def test_fixed_flat_synthesizes_but_does_not_flag_estimate(self):
        # fixed_flat utilities don't read the BFE quarterly price at all
        # (importer routes through fixed_rp_kwh). The synthesis just unblocks
        # the loop guard — flagging this as an estimate would be misleading.
        rt = _resolved("fixed_flat")
        out_q, _out_m, used = _synthesize_fallback_prices(
            rt, _Q, {}, None, ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )
        assert _Q in out_q
        assert used is False

    def test_fixed_ht_nt_synthesizes_but_does_not_flag_estimate(self):
        rt = _resolved("fixed_ht_nt")
        out_q, _out_m, used = _synthesize_fallback_prices(
            rt, _Q, {}, None, ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )
        assert _Q in out_q
        assert used is False


class TestSynthesizeFallbackPricesNoOp:
    """Behaviour when the running quarter IS already published — no
    substitution should happen and no estimate flag should fire."""

    def test_published_quarter_passes_through_unchanged_rmp_quartal(self):
        rt = _resolved("rmp_quartal")
        real = BfePrice(chf_per_mwh=84.2, days=92, volume_mwh=1234.5)
        out_q, _out_m, used = _synthesize_fallback_prices(
            rt, _Q, {_Q: real}, None, ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )
        assert out_q[_Q] is real
        assert used is False

    def test_published_quarter_and_months_passes_through_rmp_monat(self):
        rt = _resolved("rmp_monat")
        real_q = BfePrice(chf_per_mwh=84.2, days=92, volume_mwh=1234.5)
        m1, m2, m3 = _Q.months()
        real_months = {
            m1: BfePrice(chf_per_mwh=80.0, days=30, volume_mwh=400.0),
            m2: BfePrice(chf_per_mwh=85.0, days=31, volume_mwh=420.0),
            m3: BfePrice(chf_per_mwh=87.5, days=30, volume_mwh=415.0),
        }
        out_q, out_m, used = _synthesize_fallback_prices(
            rt, _Q, {_Q: real_q}, dict(real_months), ABRECHNUNGS_RHYTHMUS_MONAT,
        )
        assert out_q[_Q] is real_q
        for m in (m1, m2, m3):
            assert out_m[m] is real_months[m]
        assert used is False

    def test_input_dicts_not_mutated(self):
        # Defensive: caller passes the live coordinator dict; synthesis must
        # work on a copy (otherwise we'd persist a fake floor price into the
        # coordinator and pollute future calls).
        rt = _resolved("rmp_quartal")
        in_q: dict[Quarter, BfePrice] = {}
        in_m: dict[Month, BfePrice] = {}
        _synthesize_fallback_prices(rt, _Q, in_q, in_m, ABRECHNUNGS_RHYTHMUS_MONAT)
        assert in_q == {}
        assert in_m == {}


class TestSynthesizeFallbackPricesFloorEdgeCases:
    """Floor-derived BfePrice.chf_per_mwh sanity."""

    def test_zero_floor_yields_zero_chf_per_mwh(self):
        rt = _resolved("rmp_quartal", floor=0.0)
        out_q, _, used = _synthesize_fallback_prices(
            rt, _Q, {}, None, ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )
        assert out_q[_Q].chf_per_mwh == 0.0
        assert used is True

    def test_high_floor_e_g_180kw_category_rounds_correctly(self):
        # 180 kW category floor — currently 180 / kW per the EnFV table
        # but per-kWh equivalent is e.g. 12.00 Rp/kWh after Plant resolution.
        rt = _resolved("rmp_quartal", floor=12.00)
        out_q, _, used = _synthesize_fallback_prices(
            rt, _Q, {}, None, ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )
        assert out_q[_Q].chf_per_mwh == pytest.approx(120.0)
        assert used is True


class TestQuarterFromPeriodString:
    """Round-trip from `_aggregate_by_period`'s period strings."""

    @pytest.mark.parametrize(
        "period_str,expected",
        [
            ("2026Q1", Quarter(2026, 1)),
            ("2026Q4", Quarter(2026, 4)),
            ("2025Q3", Quarter(2025, 3)),
            ("2026q2", Quarter(2026, 2)),  # case-insensitive (Quarter.parse)
            ("2026-01", Quarter(2026, 1)),
            ("2026-02", Quarter(2026, 1)),
            ("2026-03", Quarter(2026, 1)),
            ("2026-04", Quarter(2026, 2)),
            ("2026-12", Quarter(2026, 4)),
            ("2026-01-15", Quarter(2026, 1)),
            ("2025-12-31", Quarter(2025, 4)),
            ("2026-01-01", Quarter(2026, 1)),
            ("2025-12-31 23:00", Quarter(2025, 4)),
            ("2026-01-01 00:00", Quarter(2026, 1)),
        ],
    )
    def test_parses_known_shapes(self, period_str, expected):
        assert _quarter_from_period_string(period_str) == expected

    def test_yearly_shape_returns_none(self):
        # `jahr` granularity rolls through `_aggregate_to_yearly`, which has
        # its own `is_current_estimate` rollup — period string is just "YYYY"
        # with no quarter context.
        assert _quarter_from_period_string("2026") is None

    def test_quarter_boundary_at_year_end_correct(self):
        # Regression guard: month → quarter formula must handle Dec correctly.
        assert _quarter_from_period_string("2025-12-31 23:00") == Quarter(2025, 4)
        assert _quarter_from_period_string("2026-01-01 00:00") == Quarter(2026, 1)
