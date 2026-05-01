"""Tests for v0.8.2: skipped-quarters notification recorder gating.

Covers ``BfeCoordinator._filter_skipped_to_quarters_with_export`` — the
recorder-presence check that prevents the "Older quarters skipped"
notification from claiming HA has grid-export records when it doesn't.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.bfe_rueckliefertarif.coordinator import BfeCoordinator


def _make_coordinator(statistic_id: str | None = "sensor.power_meter_exported"):
    """Build a coordinator with just enough state for the filter helper.

    Skips DataUpdateCoordinator.__init__ (needs a real hass) — the helper
    only touches ``self.hass``, ``self._config`` (post-A+: merge of
    ``self.entry.data`` entity wiring + history-resolved versioned fields),
    and ``self._earliest_export_hour``, so a partially-initialised instance
    is fine here.

    v0.9.0: ``entry.options`` must be present (even if empty) because
    ``_config`` reads it via ``_resolve_config_at``.
    """
    import asyncio

    coord = BfeCoordinator.__new__(BfeCoordinator)
    coord.hass = MagicMock()
    coord._earliest_export_hour = None
    coord._auto_import_lock = asyncio.Lock()
    entry_data = {"stromnetzeinspeisung_kwh": statistic_id} if statistic_id else {}
    coord.entry = SimpleNamespace(
        entry_id="test_entry", data=entry_data, options={}
    )
    return coord


class TestFilterSkippedQuarters:
    @pytest.mark.asyncio
    async def test_dismisses_when_sensor_has_no_export_records(self):
        coord = _make_coordinator()
        with patch(
            "custom_components.bfe_rueckliefertarif.ha_recorder.read_hourly_export",
            new=AsyncMock(return_value={}),
        ):
            result = await coord._filter_skipped_to_quarters_with_export(
                ["2023Q1", "2024Q4", "2025Q3"]
            )
        assert result == []

    @pytest.mark.asyncio
    async def test_dismisses_when_sensor_has_only_zero_rows(self):
        # Recorder returns rows but all zero — no real export data.
        coord = _make_coordinator()
        zero_rows = {datetime(2025, 9, 1, h, tzinfo=UTC): 0.0 for h in range(3)}
        with patch(
            "custom_components.bfe_rueckliefertarif.ha_recorder.read_hourly_export",
            new=AsyncMock(return_value=zero_rows),
        ):
            result = await coord._filter_skipped_to_quarters_with_export(["2025Q3"])
        assert result == []

    @pytest.mark.asyncio
    async def test_filters_quarters_ending_before_first_export(self):
        # User's first non-zero export is 2025-09-01. Quarters ending
        # *before* that (2023Q1..2025Q2) should drop. 2025Q3 spans
        # 2025-07..2025-09-30, ends after threshold → kept.
        coord = _make_coordinator()
        rows = {
            datetime(2025, 9, 1, 12, tzinfo=UTC): 1.5,
            datetime(2025, 9, 2, 12, tzinfo=UTC): 2.0,
        }
        with patch(
            "custom_components.bfe_rueckliefertarif.ha_recorder.read_hourly_export",
            new=AsyncMock(return_value=rows),
        ):
            result = await coord._filter_skipped_to_quarters_with_export(
                ["2023Q1", "2024Q4", "2025Q1", "2025Q2", "2025Q3", "2025Q4"]
            )
        assert result == ["2025Q3", "2025Q4"]

    @pytest.mark.asyncio
    async def test_passes_through_on_recorder_error(self):
        # Recorder hiccup → don't suppress a real notification.
        coord = _make_coordinator()
        with patch(
            "custom_components.bfe_rueckliefertarif.ha_recorder.read_hourly_export",
            new=AsyncMock(side_effect=RuntimeError("recorder offline")),
        ):
            result = await coord._filter_skipped_to_quarters_with_export(
                ["2025Q3", "2025Q4"]
            )
        assert result == ["2025Q3", "2025Q4"]

    @pytest.mark.asyncio
    async def test_passes_through_when_no_statistic_id_configured(self):
        coord = _make_coordinator(statistic_id=None)
        result = await coord._filter_skipped_to_quarters_with_export(["2025Q3"])
        assert result == ["2025Q3"]

    @pytest.mark.asyncio
    async def test_caches_earliest_hour_across_calls(self):
        # First call computes, second call reuses cached value (no second
        # recorder query).
        coord = _make_coordinator()
        rows = {datetime(2025, 9, 1, tzinfo=UTC): 1.0}
        mock_read = AsyncMock(return_value=rows)
        with patch(
            "custom_components.bfe_rueckliefertarif.ha_recorder.read_hourly_export",
            new=mock_read,
        ):
            await coord._filter_skipped_to_quarters_with_export(["2025Q3"])
            await coord._filter_skipped_to_quarters_with_export(["2025Q4"])
        assert mock_read.call_count == 1
        assert coord._earliest_export_hour == datetime(2025, 9, 1, tzinfo=UTC)


class TestCoordinatorAutoImportSkipsPreValidFrom:
    """v0.9.3 — _auto_import_newly_published must apply the same
    pre-valid_from quarter skip that _reimport_all_history does. Otherwise
    every 6-hourly coordinator refresh logs a "predates earliest record"
    WARNING for each pre-install BFE-published quarter."""

    @pytest.mark.asyncio
    async def test_skips_quarters_predating_earliest_history_record(self):
        from custom_components.bfe_rueckliefertarif.bfe import BfePrice
        from custom_components.bfe_rueckliefertarif.const import OPT_CONFIG_HISTORY
        from custom_components.bfe_rueckliefertarif.quarters import Quarter

        coord = _make_coordinator()
        # History anchored at 2025-04-01 (plant install).
        coord.entry = SimpleNamespace(
            entry_id="test_entry",
            data={"stromnetzeinspeisung_kwh": "sensor.export"},
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-04-01",
                        "valid_to": None,
                        "config": {
                            "energieversorger": "ekz",
                            "installierte_leistung_kw": 8.0,
                            "eigenverbrauch_aktiviert": True,
                            "hkn_aktiviert": True,
                            "abrechnungs_rhythmus": "quartal",
                        },
                    }
                ]
            },
        )
        # BFE has 2024Q4 + 2025Q1 + 2025Q2 + 2025Q3 published.
        coord.quarterly = {
            Quarter(2024, 4): BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
            Quarter(2025, 1): BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0),
            Quarter(2025, 2): BfePrice(chf_per_mwh=80.0, days=91, volume_mwh=0.0),
            Quarter(2025, 3): BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
        }
        coord._imported = {}

        async def _fake_notify(*args, **kwargs):
            return None

        coord._notify_skipped_quarters = _fake_notify

        called: list[Quarter] = []

        async def _fake_reimport(_hass, q):
            called.append(q)

        with patch(
            "custom_components.bfe_rueckliefertarif.services._reimport_quarter",
            new=_fake_reimport,
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._build_recompute_report",
            return_value=None,
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._notify_recompute",
        ):
            await coord._auto_import_newly_published()

        # Pre-valid_from quarters must NOT reach _reimport_quarter.
        assert sorted(str(q) for q in called) == ["2025Q2", "2025Q3"]

    @pytest.mark.asyncio
    async def test_no_history_means_no_filter_applied(self):
        # Defensive: when entry.options has no history, the coordinator
        # falls back to importing everything (legacy behavior, no regression).
        from custom_components.bfe_rueckliefertarif.bfe import BfePrice
        from custom_components.bfe_rueckliefertarif.quarters import Quarter

        coord = _make_coordinator()
        coord.entry = SimpleNamespace(
            entry_id="test_entry",
            data={"stromnetzeinspeisung_kwh": "sensor.export"},
            options={},
        )
        coord.quarterly = {
            Quarter(2024, 4): BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
            Quarter(2025, 2): BfePrice(chf_per_mwh=80.0, days=91, volume_mwh=0.0),
        }
        coord._imported = {}

        async def _fake_notify(*args, **kwargs):
            return None

        coord._notify_skipped_quarters = _fake_notify

        called: list[Quarter] = []

        async def _fake_reimport(_hass, q):
            called.append(q)

        with patch(
            "custom_components.bfe_rueckliefertarif.services._reimport_quarter",
            new=_fake_reimport,
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._build_recompute_report",
            return_value=None,
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._notify_recompute",
        ):
            await coord._auto_import_newly_published()

        # Both quarters reach _reimport_quarter — no history filter applied.
        assert sorted(str(q) for q in called) == ["2024Q4", "2025Q2"]


class TestRecomputeNotificationGate:
    """v0.16.0 — Issue 1: editing the *current active* tariff (running
    quarter only) used to produce no notification because the gate at
    `coordinator.py:385` was `if reimported:`. The fix extends the gate
    to also fire when only the running-quarter config changed.
    """

    @pytest.mark.asyncio
    async def test_fires_for_active_edit_only(self):
        # Setup: no past quarters published (so reimported=[]), but
        # `_running_q_config_changed` returns True and the running-quarter
        # estimate succeeds. Pre-v0.16.0 produced no notification; now it does.
        coord = _make_coordinator()
        coord.entry = SimpleNamespace(
            entry_id="test_entry",
            data={"stromnetzeinspeisung_kwh": "sensor.export"},
            options={},
        )
        coord.quarterly = {}  # No past quarters published.
        coord._imported = {}

        async def _fake_notify(*args, **kwargs):
            return None

        coord._notify_skipped_quarters = _fake_notify
        # Force "config changed" so the running-quarter estimate runs and
        # the new gate condition fires.
        coord._running_q_config_changed = MagicMock(return_value=True)

        async def _fake_estimate(*_args, **_kwargs):
            return None

        notify_calls: list[tuple] = []

        def _fake_notify_recompute(_hass, _entry_id, _report):
            notify_calls.append((_entry_id, _report))

        with patch(
            "custom_components.bfe_rueckliefertarif.services._import_running_quarter_estimate",
            new=_fake_estimate,
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._build_recompute_report",
            return_value="report-stub",
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._notify_recompute",
            new=_fake_notify_recompute,
        ):
            await coord._auto_import_newly_published(is_user_reload=True)

        assert len(notify_calls) == 1, (
            "v0.16.0: notification must fire even when only the running "
            "quarter changed (active-tariff edit case)"
        )
        assert notify_calls[0][0] == "test_entry"

    @pytest.mark.asyncio
    async def test_no_fire_when_nothing_changed(self):
        # Setup: no past quarters AND `_running_q_config_changed` returns
        # False. With is_user_reload=True the running-quarter estimate is
        # gated off (would only run on the kWh roll-forward path), so
        # `running_q_estimated=False` and no notification fires.
        coord = _make_coordinator()
        coord.entry = SimpleNamespace(
            entry_id="test_entry",
            data={"stromnetzeinspeisung_kwh": "sensor.export"},
            options={},
        )
        coord.quarterly = {}
        coord._imported = {}

        async def _fake_notify(*args, **kwargs):
            return None

        coord._notify_skipped_quarters = _fake_notify
        coord._running_q_config_changed = MagicMock(return_value=False)

        notify_calls: list[tuple] = []

        def _fake_notify_recompute(_hass, _entry_id, _report):
            notify_calls.append((_entry_id, _report))

        with patch(
            "custom_components.bfe_rueckliefertarif.services._import_running_quarter_estimate",
            new=AsyncMock(),
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._build_recompute_report",
            return_value="report-stub",
        ), patch(
            "custom_components.bfe_rueckliefertarif.services._notify_recompute",
            new=_fake_notify_recompute,
        ):
            await coord._auto_import_newly_published(is_user_reload=True)

        assert notify_calls == [], (
            "v0.16.0: notification must NOT fire when nothing changed"
        )
