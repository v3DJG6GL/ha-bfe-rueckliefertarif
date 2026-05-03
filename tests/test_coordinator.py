"""Tests for ``BfeCoordinator`` filter helpers and auto-import gating."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.bfe_rueckliefertarif.bfe import BfePrice
from custom_components.bfe_rueckliefertarif.const import OPT_CONFIG_HISTORY
from custom_components.bfe_rueckliefertarif.coordinator import BfeCoordinator
from custom_components.bfe_rueckliefertarif.quarters import Quarter


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


async def _async_noop(*args, **kwargs):
    return None


_ZERO_ROWS = {datetime(2025, 9, 1, h, tzinfo=UTC): 0.0 for h in range(3)}
_REAL_ROWS = {
    datetime(2025, 9, 1, 12, tzinfo=UTC): 1.5,
    datetime(2025, 9, 2, 12, tzinfo=UTC): 2.0,
}
_RECORDER_ERROR = RuntimeError("recorder offline")
_NO_PATCH = object()  # sentinel: skip recorder patch entirely (no statistic_id case)


class TestFilterSkippedQuarters:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("recorder_outcome", "statistic_id", "quarters", "expected"),
        [
            # Empty recorder → all quarters dropped.
            ({}, "sensor.power_meter_exported",
             ["2023Q1", "2024Q4", "2025Q3"], []),
            # Only zero rows → no real export data, all dropped.
            (_ZERO_ROWS, "sensor.power_meter_exported", ["2025Q3"], []),
            # First non-zero export 2025-09-01: quarters ending before
            # threshold drop, 2025Q3 (ends 2025-09-30) and 2025Q4 are kept.
            (_REAL_ROWS, "sensor.power_meter_exported",
             ["2023Q1", "2024Q4", "2025Q1", "2025Q2", "2025Q3", "2025Q4"],
             ["2025Q3", "2025Q4"]),
            # Recorder error → don't suppress a real notification.
            (_RECORDER_ERROR, "sensor.power_meter_exported",
             ["2025Q3", "2025Q4"], ["2025Q3", "2025Q4"]),
            # No statistic_id configured → passthrough, no recorder call.
            (_NO_PATCH, None, ["2025Q3"], ["2025Q3"]),
        ],
    )
    async def test_filter_skipped_quarters(
        self, recorder_outcome, statistic_id, quarters, expected,
    ):
        coord = _make_coordinator(statistic_id=statistic_id)
        if recorder_outcome is _NO_PATCH:
            result = await coord._filter_skipped_to_quarters_with_export(quarters)
        else:
            if isinstance(recorder_outcome, BaseException):
                mock = AsyncMock(side_effect=recorder_outcome)
            else:
                mock = AsyncMock(return_value=recorder_outcome)
            with patch(
                "custom_components.bfe_rueckliefertarif.ha_recorder.read_hourly_export",
                new=mock,
            ):
                result = await coord._filter_skipped_to_quarters_with_export(quarters)
        assert result == expected

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
                            "installierte_leistung_kwp": 8.0,
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

        coord._notify_skipped_quarters = _async_noop

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

        coord._notify_skipped_quarters = _async_noop

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

        coord._notify_skipped_quarters = _async_noop
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

        coord._notify_skipped_quarters = _async_noop
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


class TestUserInputsFingerprint:
    """v0.17.0 — Issue 1: toggling a user_input boolean (e.g.
    ``regio_top40_opted_in``) didn't fire the recompute notification
    because ``_running_q_config_changed`` and ``_snapshot_is_stale`` only
    checked utility/kw/ev/hkn/billing/version, not user_inputs. Fix: add
    a final equality check on user_inputs.
    """

    def _make_tariff_cfg(self, user_inputs: dict):
        # The gate only reads scalar attrs of ``tariff_cfg`` and a couple of
        # fields on ``tariff_cfg.resolved`` — SimpleNamespace duck-types
        # both since ResolvedTariff is a frozen dataclass.
        rt = SimpleNamespace(utility_key="u", tariffs_json_version="v1")
        return SimpleNamespace(
            installierte_leistung_kwp=8.0,
            eigenverbrauch_aktiviert=True,
            hkn_aktiviert=True,
            user_inputs=dict(user_inputs),
            resolved=rt,
        )

    def test_running_q_config_changed_detects_user_inputs_diff(self):
        coord = _make_coordinator()
        prior = {
            "utility_key": "u",
            "kwp": 8.0,
            "eigenverbrauch_aktiviert": True,
            "hkn_optin": True,
            "billing": "quartal",
            "tariffs_json_version": "v1",
            "user_inputs": {"regio_top40_opted_in": False},
        }
        tariff_cfg = self._make_tariff_cfg(
            {"regio_top40_opted_in": True},
        )
        cfg = {"abrechnungs_rhythmus": "quartal"}
        with patch(
            "custom_components.bfe_rueckliefertarif.services._cfg_for_entry",
            return_value=(cfg, tariff_cfg),
        ):
            assert coord._running_q_config_changed(prior, Quarter(2026, 2))

    def test_running_q_config_changed_unchanged_when_user_inputs_equal(self):
        coord = _make_coordinator()
        prior = {
            "utility_key": "u",
            "kwp": 8.0,
            "eigenverbrauch_aktiviert": True,
            "hkn_optin": True,
            "billing": "quartal",
            "tariffs_json_version": "v1",
            "user_inputs": {"regio_top40_opted_in": True},
        }
        tariff_cfg = self._make_tariff_cfg(
            {"regio_top40_opted_in": True},
        )
        cfg = {"abrechnungs_rhythmus": "quartal"}
        with patch(
            "custom_components.bfe_rueckliefertarif.services._cfg_for_entry",
            return_value=(cfg, tariff_cfg),
        ):
            assert not coord._running_q_config_changed(prior, Quarter(2026, 2))

    def test_running_q_config_changed_pre_v0_17_snapshot_no_false_positive(
        self,
    ):
        # Pre-v0.16.0 snapshot lacks "user_inputs" — must not flag stale
        # when today's tariff_cfg also has no user_inputs declared (e.g.
        # utility without rate-window user_inputs).
        coord = _make_coordinator()
        prior = {
            "utility_key": "u",
            "kwp": 8.0,
            "eigenverbrauch_aktiviert": True,
            "hkn_optin": True,
            "billing": "quartal",
            "tariffs_json_version": "v1",
        }  # no "user_inputs" key
        tariff_cfg = self._make_tariff_cfg({})  # empty dict
        cfg = {"abrechnungs_rhythmus": "quartal"}
        with patch(
            "custom_components.bfe_rueckliefertarif.services._cfg_for_entry",
            return_value=(cfg, tariff_cfg),
        ):
            assert not coord._running_q_config_changed(prior, Quarter(2026, 2))
