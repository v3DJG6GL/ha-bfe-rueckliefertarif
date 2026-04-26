"""Tests for v0.8.2: setup-time sentinel handling + OptionsFlow options preservation.

Covers:
- ``async_setup_entry`` first-setup synthesizes the 1970 sentinel from entry.data.
- ``async_setup_entry`` refuses to silently re-seed on empty history (logs error).
- The four OptionsFlow terminal returns now carry the current options dict
  (rather than ``data={}`` which HA would commit as a wipe).
"""

from __future__ import annotations

import logging
from types import MappingProxyType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from custom_components.bfe_rueckliefertarif import async_setup_entry
from custom_components.bfe_rueckliefertarif.config_flow import (
    BfeRuecklieferTarifOptionsFlow,
)
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    OPT_CONFIG_HISTORY,
)


def _entry_data(utility="ekz", kw=8.0):
    return {
        CONF_ENERGIEVERSORGER: utility,
        CONF_INSTALLIERTE_LEISTUNG_KW: kw,
        CONF_EIGENVERBRAUCH_AKTIVIERT: True,
        CONF_HKN_AKTIVIERT: True,
        CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
    }


def _mock_hass():
    """Hass mock that records async_update_entry(options=...) calls."""
    hass = MagicMock()
    hass.data = {}
    return hass


def _entry(*, options=None, data=None):
    """Build a fake config entry for setup tests."""
    return SimpleNamespace(
        entry_id="test_entry_id",
        data=data or _entry_data(),
        options=MappingProxyType(options) if options is not None else MappingProxyType({}),
    )


class TestSetupSentinelSynthesis:
    """Fix B verification — split first-setup vs pathological-empty-history."""

    @pytest.mark.asyncio
    async def test_first_setup_synthesizes_sentinel_from_entry_data(self):
        # Brand-new entry: options is empty (no OPT_CONFIG_HISTORY key at
        # all). Setup must synthesize the 1970 sentinel from entry.data.
        hass = _mock_hass()
        entry = _entry(options={})
        captured: dict = {}

        def _record(_entry, **kwargs):
            captured.update(kwargs)

        hass.config_entries.async_update_entry.side_effect = _record
        # Stub out the platform-forward + service register so setup completes.
        hass.config_entries.async_forward_entry_setups.return_value = None
        with patch(
            "custom_components.bfe_rueckliefertarif.services.async_register_services",
            new=_async_noop,
        ), patch(
            "custom_components.bfe_rueckliefertarif.data_coordinator."
            "TariffsDataCoordinator"
        ) as tdc_cls:
            tdc_cls.return_value.async_load = _async_noop
            hass.config_entries.async_forward_entry_setups = _async_noop
            await async_setup_entry(hass, entry)

        assert "options" in captured, "expected async_update_entry(options=...) call"
        history = captured["options"][OPT_CONFIG_HISTORY]
        assert len(history) == 1
        assert history[0]["valid_from"] == "1970-01-01"
        assert history[0]["valid_to"] is None
        assert history[0]["config"][CONF_ENERGIEVERSORGER] == "ekz"
        assert history[0]["config"][CONF_INSTALLIERTE_LEISTUNG_KW] == 8.0

    @pytest.mark.asyncio
    async def test_existing_history_is_left_alone(self):
        # Entry already has a populated history → setup must NOT touch it.
        existing_history = [
            {
                "valid_from": "1970-01-01",
                "valid_to": None,
                "config": _entry_data(utility="ekz"),
            }
        ]
        hass = _mock_hass()
        entry = _entry(options={OPT_CONFIG_HISTORY: existing_history})
        with patch(
            "custom_components.bfe_rueckliefertarif.services.async_register_services",
            new=_async_noop,
        ), patch(
            "custom_components.bfe_rueckliefertarif.data_coordinator."
            "TariffsDataCoordinator"
        ) as tdc_cls:
            tdc_cls.return_value.async_load = _async_noop
            hass.config_entries.async_forward_entry_setups = _async_noop
            await async_setup_entry(hass, entry)
        # No options write should have happened.
        assert not hass.config_entries.async_update_entry.called

    @pytest.mark.asyncio
    async def test_empty_history_logs_error_and_does_not_reseed(self, caplog):
        # Pathological state: OPT_CONFIG_HISTORY exists but is []. Setup
        # must log loudly and NOT silently re-seed (which would encode the
        # wrong utility from a possibly-mutated entry.data).
        hass = _mock_hass()
        entry = _entry(
            options={OPT_CONFIG_HISTORY: []},
            data=_entry_data(utility="age_sa"),  # mutated, not original
        )
        with caplog.at_level(
            logging.ERROR, logger="custom_components.bfe_rueckliefertarif"
        ), patch(
            "custom_components.bfe_rueckliefertarif.services.async_register_services",
            new=_async_noop,
        ), patch(
            "custom_components.bfe_rueckliefertarif.data_coordinator."
            "TariffsDataCoordinator"
        ) as tdc_cls:
            tdc_cls.return_value.async_load = _async_noop
            hass.config_entries.async_forward_entry_setups = _async_noop
            await async_setup_entry(hass, entry)
        assert any("OPT_CONFIG_HISTORY is empty" in r.message for r in caplog.records)
        # No reseed write happened.
        assert not hass.config_entries.async_update_entry.called


class TestOptionsFlowPreservesOptions:
    """Fix A verification — terminal returns carry current options through.

    The four sites that previously returned ``data={}`` must now return
    ``data=dict(self.config_entry.options or {})`` so HA's
    ``OptionsFlowManager.async_finish_flow`` doesn't wipe ``entry.options``.
    """

    def _make_flow(self, options):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow._options = MappingProxyType(options)

        # Mock self.config_entry — the property normally pulls from
        # self.handler/self.hass. Use property-on-instance trick: set on
        # the type via SimpleNamespace works since the property is on the
        # parent class. Easier: monkey-patch by attribute.
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data=_entry_data(),
            options=flow._options,
        )
        # Bypass the read-only property by stashing on the instance dict
        # via type(flow).__dict__ — actually simplest: use object.__setattr__
        # to override the lookup. Since OptionsFlow.config_entry is a
        # property, we can shadow it via __dict__ access only if the
        # property uses __get__ and isn't a data-descriptor... it IS a
        # data-descriptor. So we mock self.handler instead and the
        # hass.config_entries.async_get_entry to return our fake.
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow, flow_entry

    @pytest.mark.asyncio
    async def test_done_history_returns_current_options(self):
        existing_history = [
            {"valid_from": "1970-01-01", "valid_to": "2026-04-01",
             "config": _entry_data(utility="ekz")},
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": _entry_data(utility="age_sa")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing_history})
        result = await flow.async_step_done_history()
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        # The result's data must carry the current options through, not {}.
        assert result["data"] != {}
        assert OPT_CONFIG_HISTORY in result["data"]
        assert result["data"][OPT_CONFIG_HISTORY] == existing_history

    @pytest.mark.asyncio
    async def test_done_history_with_empty_options_returns_empty_not_none(self):
        # Edge case: options is empty (legitimately). Result still carries
        # an empty dict, not data={} that would behave the same way — but
        # we want defensive behaviour: never return None or {} that could
        # encode something other than "no change".
        flow, _ = self._make_flow({})
        result = await flow.async_step_done_history()
        # data is {} here because options is {} — that's correct: nothing
        # to preserve. The fix-A goal is "don't wipe what's there", and
        # there's nothing there.
        assert result["data"] == {}

    @pytest.mark.asyncio
    async def test_done_history_schedules_explicit_reload(self):
        # Fix C verification: OptionsFlowWithReload's auto-reload silently
        # skips when _edit_row pre-writes options (its diff check sees no
        # change), so we trigger the reload explicitly. Required so the
        # coordinator's tariff-breakdown sensor rebuilds with fresh entry
        # state.
        flow, entry = self._make_flow({OPT_CONFIG_HISTORY: []})
        await flow.async_step_done_history()
        # async_create_task(async_reload(entry_id)) must have been scheduled.
        assert flow.hass.async_create_task.called, (
            "async_step_done_history must schedule a reload"
        )


class TestFirstEntryDataLiveReads:
    """Fix A verification — _first_entry_data refreshes config/options
    from the live ConfigEntry on every call, not from the one-time
    snapshot written at async_setup_entry time."""

    def _setup_hass_with_entry(self, entry_data, entry_options):
        """Build a hass mock + a stored entry slot + a live ConfigEntry."""
        from custom_components.bfe_rueckliefertarif.const import DOMAIN

        hass = MagicMock()
        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=entry_data,
            options=entry_options,
        )
        # Stale snapshot written at startup:
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": {"energieversorger": "stale_old"},
                    "options": {OPT_CONFIG_HISTORY: []},
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)
        return hass, live_entry

    def test_returns_live_entry_data_after_mutation(self):
        from custom_components.bfe_rueckliefertarif.services import _first_entry_data

        hass, entry = self._setup_hass_with_entry(
            entry_data=_entry_data(utility="ekz"),
            entry_options={OPT_CONFIG_HISTORY: []},
        )
        # Mutate live entry.data after setup.
        entry.data = _entry_data(utility="age_sa")
        result = _first_entry_data(hass)
        assert result["config"][CONF_ENERGIEVERSORGER] == "age_sa"

    def test_returns_live_entry_options_after_mutation(self):
        from custom_components.bfe_rueckliefertarif.services import _first_entry_data

        new_history = [
            {"valid_from": "1970-01-01", "valid_to": "2026-01-01",
             "config": _entry_data(utility="ekz")},
            {"valid_from": "2026-01-01", "valid_to": None,
             "config": _entry_data(utility="age_sa")},
        ]
        hass, entry = self._setup_hass_with_entry(
            entry_data=_entry_data(),
            entry_options={OPT_CONFIG_HISTORY: []},
        )
        # Mutate live entry.options after setup (simulating an OptionsFlow
        # that wrote options inline via async_update_entry).
        entry.options = {OPT_CONFIG_HISTORY: new_history}
        result = _first_entry_data(hass)
        assert result["options"][OPT_CONFIG_HISTORY] == new_history


class TestCoordinatorConfigLiveReads:
    """Fix B verification — BfeCoordinator._config is a property that
    reads self.entry.data live, not a cached dict from __init__."""

    def test_config_property_reflects_live_entry_data(self):
        from custom_components.bfe_rueckliefertarif.coordinator import BfeCoordinator

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(data=_entry_data(utility="ekz"))
        assert coord._config[CONF_ENERGIEVERSORGER] == "ekz"

        # Mutate live entry.data after coordinator was built.
        coord.entry.data = _entry_data(utility="age_sa")
        assert coord._config[CONF_ENERGIEVERSORGER] == "age_sa"


async def _async_noop(*args, **kwargs):
    """Async stub that does nothing — for replacing service/setup calls."""
    return None
