"""Tests for v0.8.2: setup-time sentinel handling + OptionsFlow options preservation.

Covers:
- ``async_setup_entry`` first-setup synthesizes the 1970 sentinel from entry.data.
- ``async_setup_entry`` refuses to silently re-seed on empty history (logs error).
- The four OptionsFlow terminal returns now carry the current options dict
  (rather than ``data={}`` which HA would commit as a wipe).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.bfe_rueckliefertarif import async_setup_entry
from custom_components.bfe_rueckliefertarif import coordinator as coord_mod
from custom_components.bfe_rueckliefertarif import services as svc
from custom_components.bfe_rueckliefertarif.bfe import BfePrice
from custom_components.bfe_rueckliefertarif.config_flow import (
    BfeRuecklieferTarifFlow,
    BfeRuecklieferTarifOptionsFlow,
)
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KWP,
    CONF_NAMENSPRAEFIX,
    CONF_PLANT_NAME,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    CONF_VALID_FROM,
    DOMAIN,
    OPT_CONFIG_HISTORY,
)
from custom_components.bfe_rueckliefertarif.coordinator import BfeCoordinator
from custom_components.bfe_rueckliefertarif.importer import TariffConfig
from custom_components.bfe_rueckliefertarif.quarters import Quarter, quarter_of
from custom_components.bfe_rueckliefertarif.services import _first_entry_data
from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff


def _entry_data(utility="ekz", kw=8.0):
    return {
        CONF_ENERGIEVERSORGER: utility,
        CONF_INSTALLIERTE_LEISTUNG_KWP: kw,
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
        assert history[0]["config"][CONF_INSTALLIERTE_LEISTUNG_KWP] == 8.0

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
    """Terminal returns carry current options so HA doesn't wipe entry.options."""

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
        flow, _entry = self._make_flow({OPT_CONFIG_HISTORY: []})
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
        hass, entry = self._setup_hass_with_entry(
            entry_data=_entry_data(utility="ekz"),
            entry_options={OPT_CONFIG_HISTORY: []},
        )
        # Mutate live entry.data after setup.
        entry.data = _entry_data(utility="age_sa")
        result = _first_entry_data(hass)
        assert result["config"][CONF_ENERGIEVERSORGER] == "age_sa"

    def test_returns_live_entry_options_after_mutation(self):
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
    """v0.9.0 verification — BfeCoordinator._config is a property that merges
    entity-wiring from entry.data with history-resolved versioned fields from
    entry.options[OPT_CONFIG_HISTORY] live, not a cached dict from __init__."""

    def _open_record(self, utility="ekz", **overrides):
        cfg = _entry_data(utility=utility)
        cfg.update(overrides)
        return {
            "valid_from": "1970-01-01",
            "valid_to": None,
            "config": cfg,
        }

    def test_config_property_reflects_live_history_open_record(self):

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            data={CONF_STROMNETZEINSPEISUNG_KWH: "sensor.foo"},
            options={OPT_CONFIG_HISTORY: [self._open_record(utility="ekz")]},
        )
        assert coord._config[CONF_ENERGIEVERSORGER] == "ekz"
        # Entity wiring from entry.data merged in.
        assert coord._config[CONF_STROMNETZEINSPEISUNG_KWH] == "sensor.foo"

        # Mutate live history (e.g. via OptionsFlow apply_change).
        coord.entry.options = {
            OPT_CONFIG_HISTORY: [self._open_record(utility="age_sa")]
        }
        assert coord._config[CONF_ENERGIEVERSORGER] == "age_sa"

    def test_config_property_history_overrides_entry_data_versioned(self):
        # Pre-A+ entries may still carry versioned fields in entry.data.
        # Post-A+ those must be ignored — history wins.

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            data=_entry_data(utility="legacy_stale"),  # old/stale value
            options={OPT_CONFIG_HISTORY: [self._open_record(utility="ekz")]},
        )
        assert coord._config[CONF_ENERGIEVERSORGER] == "ekz"


async def _async_noop(*args, **kwargs):
    """Async stub that does nothing — for replacing service/setup calls."""
    return None


def _close_coro(coro):
    """For MagicMock hass: services fire-and-forget coros via
    ``hass.async_create_task``; close them so they don't leak as
    ``coroutine was never awaited`` warnings at test teardown."""
    coro.close()


class TestApplyChangeWizard:
    """OptionsFlow ``apply_change`` two-step wizard (Step 1 picks utility/date/kW,
    Step 2 captures EV/HKN/user_inputs)."""

    def _make_flow(self, options, data=None):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        # async_add_executor_job is awaited in _async_warm_cache.
        flow.hass.async_add_executor_job = AsyncMock(return_value=None)
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data=data or {CONF_STROMNETZEINSPEISUNG_KWH: "sensor.foo"},
            options=MappingProxyType(options),
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow, flow_entry

    async def _drive(self, flow, valid_from, utility, details):
        """Drive add_pick_row → add_new_row and synthesise a CREATE_ENTRY-shaped
        result from ``async_update_entry`` so callers can assert
        ``result["data"][OPT_CONFIG_HISTORY]`` directly."""
        kw = details.get(CONF_INSTALLIERTE_LEISTUNG_KWP, 10.0)
        step1_payload = {
            "valid_from": valid_from,
            CONF_ENERGIEVERSORGER: utility,
            CONF_INSTALLIERTE_LEISTUNG_KWP: kw,
        }
        step1 = await flow.async_step_add_pick_row(step1_payload)
        if step1.get("step_id") == "add_pick_row":
            # Step 1 re-rendered with errors — caller wants to see those.
            return step1
        step2_details = {
            k: v for k, v in details.items()
            if k != CONF_INSTALLIERTE_LEISTUNG_KWP
        }
        result = await flow.async_step_add_new_row(step2_details)
        # If save succeeded, async_update_entry was called with the new
        # options dict. Synthesise the v0.18.0 CREATE_ENTRY shape.
        upd = flow.hass.config_entries.async_update_entry
        if upd.called:
            new_options = upd.call_args.kwargs.get("options") or upd.call_args.args[1]
            return {
                "type": SimpleNamespace(name="CREATE_ENTRY"),
                "data": new_options,
                "step_id": result.get("step_id"),
            }
        return result

    @pytest.mark.asyncio
    async def test_creates_new_record_with_all_fields(self):
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="age_sa", kw=10.0)},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await self._drive(
            flow,
            valid_from="2026-04-01",
            utility="ekz",
            details={
                CONF_INSTALLIERTE_LEISTUNG_KWP: 12.5,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: True,
            },
        )
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        history = result["data"][OPT_CONFIG_HISTORY]
        assert len(history) == 2
        # Prior open record now closed at the new transition date.
        assert history[0]["valid_from"] == "1970-01-01"
        assert history[0]["valid_to"] == "2026-04-01"
        assert history[0]["config"][CONF_ENERGIEVERSORGER] == "age_sa"
        # New open record with all six fields from the form.
        assert history[1]["valid_from"] == "2026-04-01"
        assert history[1]["valid_to"] is None
        assert history[1]["config"][CONF_ENERGIEVERSORGER] == "ekz"
        assert history[1]["config"][CONF_INSTALLIERTE_LEISTUNG_KWP] == 12.5
        assert history[1]["config"][CONF_HKN_AKTIVIERT] is True

    @pytest.mark.asyncio
    async def test_invalid_valid_from_re_renders_form(self):
        # Step 1 catches an invalid date — never advances to Step 2.
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await flow.async_step_add_pick_row(
            {"valid_from": "garbage", CONF_ENERGIEVERSORGER: "ekz"}
        )
        assert result["type"].name in ("FORM", "form")
        assert result["errors"] == {"valid_from": "invalid_valid_from"}
        assert result["step_id"] == "add_pick_row"

    @pytest.mark.asyncio
    async def test_kw_zero_re_renders_step_two(self):
        # Step 1 advances OK; Step 2 catches kw=0.
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await self._drive(
            flow,
            valid_from="2026-04-01",
            utility="ekz",
            details={
                CONF_INSTALLIERTE_LEISTUNG_KWP: 0.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: False,
            },
        )
        assert result["type"].name in ("FORM", "form")
        assert result["errors"] == {CONF_INSTALLIERTE_LEISTUNG_KWP: "kw_required"}
        # v0.13.0 — kW lives on Step 1; the kw_required error re-renders Step 1.
        assert result["step_id"] == "add_pick_row"

    @pytest.mark.asyncio
    async def test_no_op_change_does_not_duplicate_record(self):
        # Submitting Step 2 with the exact same values for the open record's
        # date is a no-op — wizard returns CREATE_ENTRY without appending.
        existing = [
            {"valid_from": "2026-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz", kw=8.0)},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await self._drive(
            flow,
            valid_from="2026-01-01",
            utility="ekz",
            details={
                CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: True,
            },
        )
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        # Options unchanged, history is still 1 record.
        history = result["data"][OPT_CONFIG_HISTORY]
        assert len(history) == 1

    @pytest.mark.asyncio
    async def test_no_op_submit_still_triggers_reload(self):
        # v0.18.1: the no-op guard was removed. Submitting identical config
        # MUST still call async_reload so the recompute path runs and the
        # user gets a notification body containing "Recompute" — even if no
        # values changed. This verifies the reload trigger fires
        # unconditionally (the notification body assertion happens at the
        # services.py recompute layer, not here).
        existing = [
            {"valid_from": "2026-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz", kw=8.0)},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        await self._drive(
            flow,
            valid_from="2026-01-01",
            utility="ekz",
            details={
                CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: True,
            },
        )
        # async_reload MUST have been scheduled even though config is
        # unchanged — the user gets the recompute notification regardless.
        assert flow.hass.config_entries.async_update_entry.called
        assert flow.hass.async_create_task.called, (
            "async_create_task must be invoked to schedule async_reload"
        )

    @pytest.mark.asyncio
    async def test_user_inputs_persist_into_history_record(self):
        # AEW declares user_inputs.fixpreis_rmp (v1.2.0 bundled data).
        # The Step 2 form renders the dropdown for the utility chosen in
        # Step 1; the chosen value persists into the new record.
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await self._drive(
            flow,
            valid_from="2026-04-01",
            utility="aew",
            # v0.13.0 — kW=50 lands in AEW's upper tier (kw_min=30,
            # kw_max=3000) which is gated on "rmp"; the
            # O4 dry-run accepts this combination. (kW=10 + RMP would
            # be correctly rejected by the new no_matching_tier check.)
            details={
                CONF_INSTALLIERTE_LEISTUNG_KWP: 50.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: False,
                "fixpreis_rmp": "rmp",
            },
        )
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        history = result["data"][OPT_CONFIG_HISTORY]
        new_rec = history[-1]
        assert new_rec["config"]["energieversorger"] == "aew"
        assert new_rec["config"]["user_inputs"] == {
            "fixpreis_rmp": "rmp"
        }

    @pytest.mark.asyncio
    async def test_user_inputs_default_used_when_not_provided(self):
        # Submission omits the declared user_input — resolver falls back
        # to decl.default ("fixpreis").
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await self._drive(
            flow,
            valid_from="2026-04-01",
            utility="aew",
            details={
                CONF_INSTALLIERTE_LEISTUNG_KWP: 10.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: False,
                # fixpreis_rmp intentionally omitted.
            },
        )
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        history = result["data"][OPT_CONFIG_HISTORY]
        assert history[-1]["config"]["user_inputs"] == {
            "fixpreis_rmp": "fixpreis"
        }

    @pytest.mark.asyncio
    async def test_invalid_user_input_choice_re_renders_form(self):
        # Submitting an enum value not in the declared `values` list →
        # Step 2 form re-renders with a per-field error.
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await self._drive(
            flow,
            valid_from="2026-04-01",
            utility="aew",
            details={
                CONF_INSTALLIERTE_LEISTUNG_KWP: 10.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: False,
                "fixpreis_rmp": "bogus_value_not_in_enum",
            },
        )
        assert result["type"].name in ("FORM", "form")
        assert result["errors"].get("fixpreis_rmp") == "invalid_choice"
        assert result["step_id"] == "add_new_row"

    @pytest.mark.asyncio
    async def test_step1_renders_picker_only(self):
        # Initial render of apply_change is the utility/date picker —
        # it must not contain kW/EV/HKN/user_inputs fields.
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await flow.async_step_add_pick_row()
        assert result["type"].name in ("FORM", "form")
        assert result["step_id"] == "add_pick_row"
        assert result.get("last_step") is False
        # v0.13.0: kW now lives on Step 1 too (drives Step 2's EV gate
        # and find_active_rate_window validation). EV/HKN/user_inputs
        # remain on Step 2.
        rendered_keys = {str(k) for k in result["data_schema"].schema}
        assert "valid_from" in rendered_keys
        assert CONF_ENERGIEVERSORGER in rendered_keys
        assert CONF_INSTALLIERTE_LEISTUNG_KWP in rendered_keys
        assert CONF_EIGENVERBRAUCH_AKTIVIERT not in rendered_keys
        assert CONF_HKN_AKTIVIERT not in rendered_keys


class TestPerPeriodEditor:
    """v0.13.0 (Phase 2) — when a single user-history entry's effective
    span covers multiple rate windows with differing ``user_inputs[]``
    declarations, the Step 2 form renders one section per period
    (namespaced field keys ``period_<idx>_<key>``), and save splits
    into N history records — one per period, each carrying the
    period-scoped user_inputs."""

    def _make_flow(self, options, data=None):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow.hass.async_add_executor_job = AsyncMock(return_value=None)
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data=data or {CONF_STROMNETZEINSPEISUNG_KWH: "sensor.foo"},
            options=MappingProxyType(options),
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow, flow_entry

    def _patch_synthetic_db(self, monkeypatch, rates):
        """Install a synthetic db with one utility ('syn') and the given
        rate windows. Patches both tariffs_db.load_tariffs (used by db
        helpers internally) and config_flow.load_tariffs (re-bound at
        module import time)."""
        from custom_components.bfe_rueckliefertarif import config_flow as cf
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2024-01-01",
                "valid_to": None,
                "rules": [
                    {"kw_min": 0, "kw_max": None, "self_consumption": None,
                     "min_rp_kwh": 4.0},
                ],
            }],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "homepage": "https://example.test",
                    "rates": rates,
                }
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        monkeypatch.setattr(cf, "load_tariffs", lambda: synthetic)
        # Lookup helpers in tdb call load_tariffs internally; the
        # find_active_rate_window in cf is imported but uses its own
        # import-time-bound load_tariffs from tdb. Patch both so all
        # paths see the synthetic db.

    @pytest.mark.asyncio
    async def test_renders_namespaced_fields_when_decls_differ(self, monkeypatch):
        # Two rate windows with different user_inputs decls:
        # 2026 has key=old_key, 2027 has key=new_key.
        rates = [
            {
                "valid_from": "2026-01-01", "valid_to": "2027-01-01",
                "settlement_period": "quartal",
                "power_tiers": [{
                    "kw_min": 0, "kw_max": None,
                    "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                    "hkn_rp_kwh": 0.0, "hkn_structure": "none",
                }],
                "user_inputs": [{
                    "key": "old_key", "type": "enum", "default": "fix",
                    "values": ["fix", "rmp"], "label_de": "Old",
                }],
            },
            {
                "valid_from": "2027-01-01", "valid_to": None,
                "settlement_period": "quartal",
                "power_tiers": [{
                    "kw_min": 0, "kw_max": None,
                    "base_model": "fixed_flat", "fixed_rp_kwh": 9.0,
                    "hkn_rp_kwh": 0.0, "hkn_structure": "none",
                }],
                "user_inputs": [{
                    "key": "new_key", "type": "enum", "default": "fix",
                    "values": ["fix", "rmp"], "label_de": "New",
                }],
            },
        ]
        self._patch_synthetic_db(monkeypatch, rates)

        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: []})
        # Step 1: pick syn + 2026-04-01 + kW=10
        await flow.async_step_add_pick_row({
            "valid_from": "2026-04-01",
            CONF_ENERGIEVERSORGER: "syn",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 10.0,
        })
        # Render Step 2 (no user_input — initial render).
        result = await flow.async_step_add_new_row()
        assert result["type"].name in ("FORM", "form")
        rendered_keys = {str(k) for k in result["data_schema"].schema}
        # Period 0 (2026) namespaced field
        assert "period_0_old_key" in rendered_keys
        # Period 1 (2027) namespaced field
        assert "period_1_new_key" in rendered_keys
        # No bare unnamespaced keys (multi-period mode replaces them)
        assert "old_key" not in rendered_keys
        assert "new_key" not in rendered_keys

    @pytest.mark.asyncio
    async def test_save_splits_into_n_entries(self, monkeypatch):
        # Same setup as above; submit Step 2 → expect 2 history records.
        rates = [
            {
                "valid_from": "2026-01-01", "valid_to": "2027-01-01",
                "settlement_period": "quartal",
                "power_tiers": [{
                    "kw_min": 0, "kw_max": None,
                    "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                    "hkn_rp_kwh": 0.0, "hkn_structure": "none",
                }],
                "user_inputs": [{
                    "key": "old_key", "type": "enum", "default": "fix",
                    "values": ["fix", "rmp"], "label_de": "Old",
                }],
            },
            {
                "valid_from": "2027-01-01", "valid_to": None,
                "settlement_period": "quartal",
                "power_tiers": [{
                    "kw_min": 0, "kw_max": None,
                    "base_model": "fixed_flat", "fixed_rp_kwh": 9.0,
                    "hkn_rp_kwh": 0.0, "hkn_structure": "none",
                }],
                "user_inputs": [{
                    "key": "new_key", "type": "enum", "default": "fix",
                    "values": ["fix", "rmp"], "label_de": "New",
                }],
            },
        ]
        self._patch_synthetic_db(monkeypatch, rates)

        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: []})
        await flow.async_step_add_pick_row({
            "valid_from": "2026-04-01",
            CONF_ENERGIEVERSORGER: "syn",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 10.0,
        })
        # Submit Step 2 with explicit per-period values.
        await flow.async_step_add_new_row({
            "period_0_old_key": "rmp",
            "period_1_new_key": "fix",
        })
        upd = flow.hass.config_entries.async_update_entry
        assert upd.called
        new_options = upd.call_args.kwargs.get("options") or upd.call_args.args[1]
        history = new_options[OPT_CONFIG_HISTORY]
        # Filter out the 1970 sentinel that _append_history_record
        # injects when history was empty (the sentinel is fixture-data
        # noise; the test cares about user-owned records only).
        user_recs = [r for r in history if r["valid_from"] != "1970-01-01"]
        assert len(user_recs) == 2, f"got {len(user_recs)} user records, expected 2"
        # Period 0: user's chosen valid_from (2026-04-01) with old_key="rmp"
        rec0 = next(r for r in user_recs if r["valid_from"] == "2026-04-01")
        assert rec0["config"]["user_inputs"] == {"old_key": "rmp"}
        # Period 1: rate-window boundary (2027-01-01) with new_key="fix"
        rec1 = next(r for r in user_recs if r["valid_from"] == "2027-01-01")
        assert rec1["config"]["user_inputs"] == {"new_key": "fix"}

    @pytest.mark.asyncio
    async def test_pure_rate_change_keeps_single_entry(self, monkeypatch):
        # Two rate windows, identical user_inputs decls — only fixed rate
        # differs. Expect single history record (no split).
        common_decl = [{
            "key": "k", "type": "enum", "default": "x",
            "values": ["x", "y"], "label_de": "K",
        }]
        rates = [
            {
                "valid_from": "2026-01-01", "valid_to": "2027-01-01",
                "settlement_period": "quartal",
                "power_tiers": [{
                    "kw_min": 0, "kw_max": None,
                    "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                    "hkn_rp_kwh": 0.0, "hkn_structure": "none",
                }],
                "user_inputs": common_decl,
            },
            {
                "valid_from": "2027-01-01", "valid_to": None,
                "settlement_period": "quartal",
                "power_tiers": [{
                    "kw_min": 0, "kw_max": None,
                    "base_model": "fixed_flat", "fixed_rp_kwh": 9.5,
                    "hkn_rp_kwh": 0.0, "hkn_structure": "none",
                }],
                "user_inputs": common_decl,
            },
        ]
        self._patch_synthetic_db(monkeypatch, rates)

        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: []})
        await flow.async_step_add_pick_row({
            "valid_from": "2026-04-01",
            CONF_ENERGIEVERSORGER: "syn",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 10.0,
        })
        await flow.async_step_add_new_row({"k": "x"})
        upd = flow.hass.config_entries.async_update_entry
        assert upd.called
        new_options = upd.call_args.kwargs.get("options") or upd.call_args.args[1]
        history = new_options[OPT_CONFIG_HISTORY]
        # Filter out the 1970 sentinel injected when history was empty.
        user_recs = [r for r in history if r["valid_from"] != "1970-01-01"]
        assert len(user_recs) == 1


class TestRecomputeHistoryEstimate:
    """Recompute report flags estimate rows; notification renderer marks them."""

    def test_report_row_carries_is_current_estimate(self):
        # Simulate a coordinator with one estimate snapshot in _imported,
        # build the report, assert the row is flagged as estimate.
        from custom_components.bfe_rueckliefertarif.services import (
            _build_recompute_report,
        )

        snapshot = {
            "rate_rp_kwh": 7.91,
            "kwp": 8.0,
            "eigenverbrauch_aktiviert": True,
            "hkn_rp_kwh": 5.0,
            "hkn_optin": True,
            "cap_rp_kwh": None,
            "cap_applied": False,
            "total_kwh": 100.0,
            "total_chf": 7.91,
            "periods": [
                {
                    "period": "2026Q2",
                    "kwh": 100.0,
                    "chf": 7.91,
                    "rate_rp_kwh_avg": 7.91,
                    "base_rp_kwh_avg": None,
                    "hkn_rp_kwh_avg": None,
                    "intended_hkn_rp_kwh": None,
                }
            ],
            "utility_key": "ewz",
            "base_model": "fixpreis",
            "billing": ABRECHNUNGS_RHYTHMUS_QUARTAL,
            "floor_label": None,
            "floor_rp_kwh": None,
            "tariffs_json_version": "2026.01",
            "tariffs_json_source": "bundled",
            "is_current_estimate": True,
        }

        coordinator = MagicMock()
        coordinator._imported = {
            "2026Q2": {
                "q_price_chf_mwh": None,
                "imported_at": "2026-04-27T12:00:00+00:00",
                "snapshot": snapshot,
            }
        }

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.foo",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.bar",
            },
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "1970-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ewz"),
                    }
                ]
            },
        )
        hass = MagicMock()
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)

        report = _build_recompute_report(hass, [Quarter.parse("2026Q2")])
        assert len(report.rows) == 1
        row = report.rows[0]
        assert row.is_current_estimate is True
        assert row.utility_key_at_period == "ewz"

    def test_renderer_marks_estimate_row_visually(self):
        # v0.9.2: replaces the wide "(estimate · BASIS)" inline tag with a
        # compact `*` in the period cell + a single footnote line below the
        # table. The basis label is gone (fixed_flat utilities don't really
        # have a "floor" — sensor attributes still expose estimate_basis).
        from custom_components.bfe_rueckliefertarif.services import (
            _format_recompute_notification,
            _RecomputeReport,
            _RecomputeReportRow,
        )

        rows = [
            _RecomputeReportRow(
                period="2026Q2",
                rate_rp_kwh_avg=7.91,
                base_rp_kwh_avg=None,
                hkn_rp_kwh_avg=None,
                intended_hkn_rp_kwh=None,
                total_kwh=100.0,
                total_chf=7.91,
                utility_key_at_period="ewz",
                utility_name_at_period="ewz Zürich",
                kw_at_period=8.0,
                eigenverbrauch_at_period=True,
                hkn_optin_at_period=True,
                billing_at_period=ABRECHNUNGS_RHYTHMUS_QUARTAL,
                base_model_at_period="fixpreis",
                cap_rp_kwh_at_period=None,
                floor_label_at_period=None,
                floor_rp_kwh_at_period=None,
                tariffs_version_at_period="2026.01",
                tariffs_source_at_period="bundled",
                is_current_estimate=True,
            ),
        ]
        report = _RecomputeReport(
            rows=rows,
            quarters_recomputed=1,
            config={
                "utility_key": "ewz",
                "utility_name": "ewz Zürich",
                "base_model": "fixpreis",
                "settlement_period": "quartal",
                "kwp": 8.0,
                "eigenverbrauch": True,
                "hkn_optin": True,
                "hkn_rp_kwh": 5.0,
                "billing": ABRECHNUNGS_RHYTHMUS_QUARTAL,
                "floor_label": None,
                "floor_rp_kwh": None,
                "cap_rp_kwh": None,
                "tariffs_version": "2026.01",
                "tariffs_source": "bundled",
            },
        )
        _title, body = _format_recompute_notification(report)
        # Old wide decoration must be gone.
        assert "*(estimate · " not in body
        # Compact asterisk anchor on the period cell.
        assert "2026Q2 *" in body
        # Footnote line present.
        assert "* Estimated from today's kWh production" in body


class TestRecomputeHistoryStep:
    """OptionsFlow ``recompute_history`` step delegates to `_reimport_all_history`."""

    def _make_flow(self, options=None):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data=_entry_data(),
            options=MappingProxyType(options or {OPT_CONFIG_HISTORY: []}),
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow

    @pytest.mark.asyncio
    async def test_unconfirmed_renders_form(self):
        flow = self._make_flow()
        result = await flow.async_step_recompute_history(None)
        assert result["type"].name in ("FORM", "form")

    @pytest.mark.asyncio
    async def test_confirmed_invokes_full_reimport(self):
        flow = self._make_flow()
        with patch(
            "custom_components.bfe_rueckliefertarif.services._reimport_all_history",
            new=AsyncMock(return_value={
                "available": [],
                "imported": [],
                "skipped": [],
                "failed": [],
                "estimated": [],
            }),
        ) as mock_reimport:
            result = await flow.async_step_recompute_history({"confirm": True})
        assert mock_reimport.call_count == 1
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")


class TestRefreshUpstreamDataHelper:
    """`_refresh_upstream_data` refreshes both BFE prices and tariffs.json."""

    def _make_hass(self, *, tdc_refresh_returns=True, tdc_last_error=None):
        coordinator = MagicMock()
        coordinator._imported = {"2025Q4": {}, "2026Q1": {}}
        coordinator.async_refresh = AsyncMock(return_value=None)
        coordinator.quarterly = {"2026Q1": object(), "2025Q4": object()}

        tdc = MagicMock()
        tdc.async_refresh = AsyncMock(return_value=tdc_refresh_returns)
        tdc.last_error = tdc_last_error

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=_entry_data(),
            options={},
        )
        hass = MagicMock()
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                },
                "_tariffs_data": tdc,
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)

        async def _fake_executor(fn, *args, **kwargs):
            return {"schema_version": "1.0.1"}
        hass.async_add_executor_job = _fake_executor
        return hass, coordinator, tdc

    @pytest.mark.asyncio
    async def test_calls_both_coordinators_and_returns_tariffs_status(self):

        hass, coordinator, tdc = self._make_hass(tdc_refresh_returns=True)
        result = await svc._refresh_upstream_data(hass)

        coordinator.async_refresh.assert_awaited_once()
        tdc.async_refresh.assert_awaited_once()
        assert result["tariffs_refreshed"] is True
        assert result["tariffs_version"] == "1.0.1"
        assert result["tariffs_error"] is None

    @pytest.mark.asyncio
    async def test_continues_when_tariffs_refresh_fails(self):

        hass, coordinator, tdc = self._make_hass(
            tdc_refresh_returns=False,
            tdc_last_error="schema mismatch",
        )
        result = await svc._refresh_upstream_data(hass)

        # BFE poll still fired even though tariff refresh failed.
        coordinator.async_refresh.assert_awaited_once()
        tdc.async_refresh.assert_awaited_once()
        assert result["tariffs_refreshed"] is False
        assert result["tariffs_version"] is None
        assert result["tariffs_error"] == "schema mismatch"


class TestRefreshDataStep:
    """v0.9.6 — the OptionsFlow ``refresh_data`` step (renamed from
    ``refresh_prices``) delegates to ``_refresh_upstream_data`` after
    confirmation, which refreshes BOTH BFE prices AND the companion-repo
    tariffs.json."""

    def _make_flow(self, options=None):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data=_entry_data(),
            options=MappingProxyType(options or {OPT_CONFIG_HISTORY: []}),
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow

    @pytest.mark.asyncio
    async def test_confirmed_invokes_upstream_data_refresh(self):
        flow = self._make_flow()
        # The mocked return now includes the v0.9.6 tariffs_* keys.
        with patch(
            "custom_components.bfe_rueckliefertarif.services._refresh_upstream_data",
            new=AsyncMock(return_value={
                "available": [],
                "newly_imported": [],
                "tariffs_refreshed": True,
                "tariffs_version": "1.0.1",
                "tariffs_error": None,
            }),
        ) as mock_refresh:
            result = await flow.async_step_refresh_data({"confirm": True})
        assert mock_refresh.call_count == 1
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")


class TestPlatformsAfterAplus:
    """v0.9.0 — button platform removed entirely."""

    def test_platforms_only_sensor(self):
        from custom_components.bfe_rueckliefertarif import PLATFORMS

        assert PLATFORMS == ["sensor"]

    def test_button_module_deleted(self):
        # Importing button.py must raise — file is gone.
        import pytest as _pt

        with _pt.raises(ImportError):
            __import__(
                "custom_components.bfe_rueckliefertarif.button"
            )


class TestWarmCacheBootstrap:
    """v0.9.1 — _async_warm_cache lazy-inits TariffsDataCoordinator so the
    initial config flow's utility dropdown sees the live remote list (not
    just bundled). Without this, age_sa and any other remote-only utility
    is invisible until an entry exists."""

    @pytest.mark.asyncio
    async def test_lazy_inits_when_missing(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _async_warm_cache,
        )

        hass = MagicMock()
        hass.data = {}
        hass.async_add_executor_job = AsyncMock(return_value=None)
        with patch(
            "custom_components.bfe_rueckliefertarif.data_coordinator."
            "TariffsDataCoordinator"
        ) as tdc_cls:
            tdc_instance = tdc_cls.return_value
            tdc_instance.async_load = AsyncMock(return_value=None)
            await _async_warm_cache(hass)
        # Singleton was instantiated and stored in hass.data.
        assert tdc_cls.call_count == 1
        assert hass.data[DOMAIN]["_tariffs_data"] is tdc_instance
        tdc_instance.async_load.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reuses_existing_singleton(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _async_warm_cache,
        )

        hass = MagicMock()
        existing = MagicMock()
        hass.data = {DOMAIN: {"_tariffs_data": existing}}
        hass.async_add_executor_job = AsyncMock(return_value=None)
        with patch(
            "custom_components.bfe_rueckliefertarif.data_coordinator."
            "TariffsDataCoordinator"
        ) as tdc_cls:
            await _async_warm_cache(hass)
        # No second instantiation; the existing slot is preserved.
        assert tdc_cls.call_count == 0
        assert hass.data[DOMAIN]["_tariffs_data"] is existing


class TestInitialEntitiesStepPlantName:
    """v0.9.1 — initial config flow uses plant_name as the entry title and
    derives the namenspraefix default via slugify(plant_name)."""

    def _make_flow(self, energieversorger="ekz"):
        flow = BfeRuecklieferTarifFlow.__new__(BfeRuecklieferTarifFlow)
        flow._data = {
            CONF_ENERGIEVERSORGER: energieversorger,
            CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: True,
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }
        return flow

    @pytest.mark.asyncio
    async def test_uses_plant_name_as_title_and_derives_prefix(self):
        flow = self._make_flow()
        result = await flow.async_step_entities(
            {
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                CONF_PLANT_NAME: "Rooftop South",
                CONF_NAMENSPRAEFIX: "",
            }
        )
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        assert result["title"] == "Rooftop South"
        assert result["data"][CONF_PLANT_NAME] == "Rooftop South"
        assert result["data"][CONF_NAMENSPRAEFIX] == "rooftop_south_rueckliefertarif"

    @pytest.mark.asyncio
    async def test_explicit_namenspraefix_wins_over_derived(self):
        flow = self._make_flow()
        result = await flow.async_step_entities(
            {
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                CONF_PLANT_NAME: "Rooftop South",
                CONF_NAMENSPRAEFIX: "custom_pv",
            }
        )
        assert result["data"][CONF_NAMENSPRAEFIX] == "custom_pv"


class TestFirstTimeSetupSplitFlow:
    """v0.18.0 — first-time setup combines utility + active-since + kW into
    a single ``user`` step (Issue 6.4.1). Followed by ``tariff_details``
    (EV + HKN + user_inputs[]) and ``entities`` (HA wiring). Replaces the
    v0.14.0 menu + tariff_pick split."""

    def _make_flow(self, energieversorger="aew"):
        # _data populated with utility key for tests that call
        # tariff_details directly via _setup_pick; the user-step itself
        # accepts utility via the form payload now.
        flow = BfeRuecklieferTarifFlow.__new__(BfeRuecklieferTarifFlow)
        flow._data = {CONF_ENERGIEVERSORGER: energieversorger}
        flow.hass = MagicMock()
        # _async_warm_cache (called from the combined user step) reads
        # hass.config.language + awaits hass.async_add_executor_job; mock
        # both to avoid hitting the real coordinator load path.
        flow.hass.config = MagicMock()
        flow.hass.config.language = "de"
        flow.hass.config.path = MagicMock(return_value="/tmp")
        flow.hass.async_add_executor_job = AsyncMock(return_value=None)
        # Pre-seed hass.data[DOMAIN]['_tariffs_data'] so _async_warm_cache
        # short-circuits past the TariffsDataCoordinator init.
        tdc_stub = MagicMock()
        tdc_stub.async_load = AsyncMock(return_value=None)
        flow.hass.data = {DOMAIN: {"_tariffs_data": tdc_stub}}
        return flow

    @pytest.mark.asyncio
    async def test_pick_kw_required_rejected(self):
        flow = self._make_flow("aew")
        result = await flow.async_step_user({
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "2026-04-01",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 0.0,
        })
        # kw=0 → re-renders Step 1 with kw_required error.
        assert result["type"].name in ("FORM", "form")
        assert result["step_id"] == "user"
        assert result["errors"][CONF_INSTALLIERTE_LEISTUNG_KWP] == "kw_required"

    @pytest.mark.asyncio
    async def test_pick_invalid_date_rejected(self):
        flow = self._make_flow("aew")
        result = await flow.async_step_user({
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "not-a-date",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 10.0,
        })
        assert result["step_id"] == "user"
        assert result["errors"][CONF_VALID_FROM] == "invalid_valid_from"

    @pytest.mark.asyncio
    async def test_pick_no_active_rate_rejected(self):
        flow = self._make_flow("aew")
        # 1999 is before any AEW rate window in bundled data.
        result = await flow.async_step_user({
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "1999-04-01",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 10.0,
        })
        assert result["step_id"] == "user"
        assert result["errors"][CONF_VALID_FROM] == "no_active_rate"

    @pytest.mark.asyncio
    async def test_pick_advances_to_details(self):
        flow = self._make_flow("aew")
        result = await flow.async_step_user({
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "2026-04-01",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 15.0,
        })
        # On valid submit, pick is stashed and we render Step 2.
        assert flow._setup_pick == {
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "2026-04-01",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 15.0,
        }
        assert result["type"].name in ("FORM", "form")
        assert result["step_id"] == "tariff_details"

    @pytest.mark.asyncio
    async def test_details_aew_kw50_excludes_fixpreis_only(self):
        # v1.6.0 AEW: at kW=50 the fixed_flat fixpreis tier (2..30) is OUT,
        # but rmp_quartal (2..3000) AND both fixed_seasonal tiers (0..∞)
        # remain valid. The fixpreis_rmp dropdown should expose three
        # options — everything except "fixpreis".
        flow = self._make_flow("aew")
        flow._setup_pick = {
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "2026-04-01",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 50.0,
        }
        result = await flow.async_step_tariff_details(None)
        assert result["step_id"] == "tariff_details"
        for k, v in result["data_schema"].schema.items():
            if str(k) == "fixpreis_rmp":
                opts = [opt["value"] for opt in v.config["options"]]
                assert sorted(opts) == sorted(["rmp", "spezial", "spezialmitbonus"])
                break
        else:
            raise AssertionError("fixpreis_rmp not in schema")

    @pytest.mark.asyncio
    async def test_details_aew_kw15_offers_all_four_options(self):
        # v1.6.0 AEW data: at kW=15 all four tiers cover
        # (fixed_flat 2..30, rmp_quartal 2..3000, fixed_seasonal 0..∞ ×2),
        # so the kw-aware filter should expose all four enum options.
        flow = self._make_flow("aew")
        flow._setup_pick = {
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "2026-04-01",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 15.0,
        }
        result = await flow.async_step_tariff_details(None)
        for k, v in result["data_schema"].schema.items():
            if str(k) == "fixpreis_rmp":
                opts = [opt["value"] for opt in v.config["options"]]
                assert sorted(opts) == sorted([
                    "fixpreis", "rmp", "spezial", "spezialmitbonus"
                ])
                break
        else:
            raise AssertionError("fixpreis_rmp not in schema")

    @pytest.mark.asyncio
    async def test_details_save_stashes_history_for_create_entry(self):
        # Submitting tariff_details with valid user_input should advance
        # to entities AND stash the pre-built OPT_CONFIG_HISTORY records
        # in self._setup_history (consumed by async_step_entities).
        flow = self._make_flow("aew")
        flow._setup_pick = {
            CONF_ENERGIEVERSORGER: "aew",
            CONF_VALID_FROM: "2026-04-01",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 15.0,
        }
        result = await flow.async_step_tariff_details(
            {
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                "fixpreis_rmp": "fixpreis",
            }
        )
        # Advances to entities form (no user_input → form, not create_entry).
        assert result["type"].name in ("FORM", "form")
        assert result["step_id"] == "entities"
        # History was pre-built and stashed.
        assert hasattr(flow, "_setup_history")
        assert len(flow._setup_history) >= 1
        rec = flow._setup_history[-1]  # last (non-sentinel) record
        assert rec["valid_from"] == "2026-04-01"
        assert rec["config"][CONF_ENERGIEVERSORGER] == "aew"
        assert rec["config"][CONF_INSTALLIERTE_LEISTUNG_KWP] == 15.0
        assert rec["config"]["user_inputs"] == {
            "fixpreis_rmp": "fixpreis"
        }

    @pytest.mark.asyncio
    async def test_entities_passes_options_when_history_pre_built(self):
        # When tariff_details has stashed _setup_history, async_step_entities
        # passes it through to async_create_entry via the options kwarg
        # (bypasses __init__.py's history-synthesis path).
        flow = self._make_flow("aew")
        flow._data.update({
            CONF_INSTALLIERTE_LEISTUNG_KWP: 15.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        })
        flow._setup_history = [{
            "valid_from": "2026-04-01",
            "valid_to": None,
            "config": {
                CONF_ENERGIEVERSORGER: "aew",
                CONF_INSTALLIERTE_LEISTUNG_KWP: 15.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: False,
                CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
                "user_inputs": {"fixpreis_rmp": "fixpreis"},
            },
        }]
        result = await flow.async_step_entities({
            CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
            CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
            CONF_PLANT_NAME: "Rooftop South",
            CONF_NAMENSPRAEFIX: "",
        })
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        # options carries the pre-built history.
        assert OPT_CONFIG_HISTORY in result["options"]
        history = result["options"][OPT_CONFIG_HISTORY]
        # Last record (post-normalize) has the user_input.
        assert history[-1]["config"]["user_inputs"] == {
            "fixpreis_rmp": "fixpreis"
        }


class TestOptionsEntitiesStepUpdatesTitle:
    """v0.9.1 — OptionsFlow entities step lets users rename the entry by
    submitting a new plant_name."""

    @pytest.mark.asyncio
    async def test_submitting_new_plant_name_updates_entry_title(self):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                CONF_NAMENSPRAEFIX: "ekz_rueckliefertarif",
            },
            options=MappingProxyType({OPT_CONFIG_HISTORY: []}),
            title="EKZ (Elektrizitätswerke des Kantons Zürich)",
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry

        await flow.async_step_entities(
            {
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                CONF_PLANT_NAME: "Rooftop South",
                CONF_NAMENSPRAEFIX: "ekz_rueckliefertarif",
            }
        )
        # async_update_entry must have been called with title=new_plant_name.
        flow.hass.config_entries.async_update_entry.assert_called_once()
        kwargs = flow.hass.config_entries.async_update_entry.call_args.kwargs
        assert kwargs.get("title") == "Rooftop South"
        assert kwargs["data"][CONF_PLANT_NAME] == "Rooftop South"

    @pytest.mark.asyncio
    async def test_unchanged_plant_name_does_not_push_title_update(self):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                CONF_NAMENSPRAEFIX: "rooftop_south_rueckliefertarif",
                CONF_PLANT_NAME: "Rooftop South",
            },
            options=MappingProxyType({OPT_CONFIG_HISTORY: []}),
            title="Rooftop South",
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry

        await flow.async_step_entities(
            {
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                CONF_PLANT_NAME: "Rooftop South",
                CONF_NAMENSPRAEFIX: "rooftop_south_rueckliefertarif",
            }
        )
        # data is updated, but title is not pushed because it's unchanged.
        kwargs = flow.hass.config_entries.async_update_entry.call_args.kwargs
        assert "title" not in kwargs


class TestValidFromInitialFlow:
    """v0.9.2 — `valid_from` field on the initial tariff step replaces the
    artificial 1970-01-01 sentinel for fresh installs."""

    @pytest.mark.asyncio
    async def test_first_setup_uses_valid_from_as_sentinel_anchor(self):
        # entry.data carries a CONF_VALID_FROM picked by the user during the
        # initial flow → that becomes the synthesized sentinel's valid_from.
        hass = _mock_hass()
        data = {
            **_entry_data(utility="ekz"),
            CONF_VALID_FROM: "2024-06-15",
        }
        entry = _entry(options={}, data=data)
        captured: dict = {}

        def _record(_entry, **kwargs):
            captured.update(kwargs)

        hass.config_entries.async_update_entry.side_effect = _record
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

        history = captured["options"][OPT_CONFIG_HISTORY]
        assert len(history) == 1
        assert history[0]["valid_from"] == "2024-06-15"
        # CONF_VALID_FROM is NOT in CONFIG_HISTORY_FIELDS — must not appear in
        # the record's "config" dict (it's a per-entry anchor, not versioned).
        assert CONF_VALID_FROM not in history[0]["config"]

    @pytest.mark.asyncio
    async def test_first_setup_falls_back_to_1970_when_valid_from_missing(self):
        # Defensive: pre-v0.9.2 entries reaching this code path don't carry
        # CONF_VALID_FROM. The synthesized sentinel must still be valid (uses
        # the 1970 fallback so the existing-entry behavior is preserved).
        hass = _mock_hass()
        data = _entry_data(utility="ekz")  # no CONF_VALID_FROM
        entry = _entry(options={}, data=data)
        captured: dict = {}

        hass.config_entries.async_update_entry.side_effect = (
            lambda _e, **kw: captured.update(kw)
        )
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

        history = captured["options"][OPT_CONFIG_HISTORY]
        assert history[0]["valid_from"] == "1970-01-01"


class TestReimportClearsFirst:
    """`_reimport_all_history` wipes LTS + snapshot map before iterating quarters."""

    @pytest.mark.asyncio
    async def test_clear_via_recorder_task_queue_then_block_till_done(self):
        # v0.9.4: clear_statistics MUST be queued onto the recorder's main
        # thread via `Recorder.async_clear_statistics(...)` (which uses
        # `queue_task` internally). Calling the underlying sync
        # `clear_statistics` on ANY executor thread (DbWorker, hass exec)
        # trips HA's `_assert_in_recorder_thread` guard. After queueing, we
        # `await instance.async_block_till_done()` so anchor reads in the
        # subsequent quarter loop don't race with the queued clear.

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {"2025Q4": {"snapshot": {}}}
        coordinator._async_save_state = _async_noop

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                **_entry_data(utility="ekz"),
            },
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)

        order: list[str] = []

        # Synchronous @callback API — queues a ClearStatisticsTask onto the
        # recorder thread. We just need to record that it was called.
        def _fake_async_clear(stat_ids, **kwargs):
            order.append(f"clear:{stat_ids}")

        async def _fake_block_till_done():
            order.append("block_till_done")

        recorder_instance = MagicMock()
        recorder_instance.async_clear_statistics = _fake_async_clear
        recorder_instance.async_block_till_done = _fake_block_till_done

        # Trap any accidental dispatch via the WRONG executor pool — both
        # generic hass and the recorder's read-side DbWorker pool would
        # crash with "Detected unsafe call not in recorder thread".
        async def _wrong_hass_thread(fn, *args, **kwargs):
            order.append("WRONG_THREAD:hass_executor")
            return None

        async def _wrong_recorder_executor(fn, *args, **kwargs):
            order.append("WRONG_THREAD:recorder_executor")
            return None

        hass.async_add_executor_job = _wrong_hass_thread
        recorder_instance.async_add_executor_job = _wrong_recorder_executor

        async def _fake_reimport_quarter(_hass, q, **_kw):
            order.append(f"quarter:{q}")
            return 0.0

        async def _fake_estimate(_hass, **_kw):
            return {}

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "fetch_quarterly", new=_async_return({})), \
             patch("homeassistant.components.recorder.get_instance", return_value=recorder_instance):
            await svc._reimport_all_history(hass)

        # Order must be: clear queued → block_till_done awaited → (any
        # subsequent quarter work follows).
        assert order, "expected at least the clear call"
        assert order[0] == f"clear:['{live_entry.data[CONF_RUECKLIEFERVERGUETUNG_CHF]}']"
        assert order[1] == "block_till_done"
        # Recorder-thread invariant — no dispatch through any wrong pool.
        assert "WRONG_THREAD:hass_executor" not in order, (
            "clear dispatched on hass.async_add_executor_job (DbWorker), "
            "would crash with 'Detected unsafe call not in recorder thread'"
        )
        assert "WRONG_THREAD:recorder_executor" not in order, (
            "clear dispatched on instance.async_add_executor_job (DbWorker), "
            "would crash with 'Detected unsafe call not in recorder thread'"
        )
        # Snapshot map cleared.
        assert coordinator._imported == {}

    @pytest.mark.asyncio
    async def test_skips_quarters_predating_valid_from(self):
        # History anchor at 2025-04-01. Pretend BFE has 2024Q4 + 2025Q1 +
        # 2025Q2 + 2025Q3. Only 2025Q2 + 2025Q3 should be imported; 2024Q4
        # and 2025Q1 land in `before_active`.

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                **_entry_data(utility="ekz"),
            },
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-04-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)
        hass.async_add_executor_job = _async_noop

        imported_quarters: list[Quarter] = []

        async def _fake_reimport_quarter(_hass, q, **_kw):
            imported_quarters.append(q)
            return 0.0

        async def _fake_estimate(_hass, **_kw):
            return {}

        prices = {
            Quarter(2024, 4): BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
            Quarter(2025, 1): BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0),
            Quarter(2025, 2): BfePrice(chf_per_mwh=80.0, days=91, volume_mwh=0.0),
            Quarter(2025, 3): BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
        }

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "fetch_quarterly", new=_async_return(prices)), \
             patch("homeassistant.components.recorder.get_instance", return_value=_recorder_instance_mock()):
            result = await svc._reimport_all_history(hass)

        # 2024Q4 + 2025Q1 predate the 2025-04-01 anchor → before_active.
        assert sorted(str(q) for q in result["before_active"]) == ["2024Q4", "2025Q1"]
        # 2025Q2 + 2025Q3 went through.
        assert sorted(str(q) for q in imported_quarters) == ["2025Q2", "2025Q3"]
        # And the result dict carries the new key.
        assert "before_active" in result

    @pytest.mark.asyncio
    async def test_result_dict_always_has_before_active_key(self):
        # Even when no history exists or no quarters predate, the key is
        # always present so callers can rely on it.

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                **_entry_data(utility="ekz"),
            },
            options={},  # no history
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)
        hass.async_add_executor_job = _async_noop

        async def _fake_estimate(_hass, **_kw):
            return {}

        with patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "fetch_quarterly", new=_async_return({})), \
             patch("homeassistant.components.recorder.get_instance", return_value=_recorder_instance_mock()):
            result = await svc._reimport_all_history(hass)
        assert result["before_active"] == []


class TestAnchorThreading:
    """`_reimport_all_history` threads cumulative LTS sum through memory between quarters."""

    @pytest.mark.asyncio
    async def test_threads_returned_anchor_through_quarters(self):
        # Each `_reimport_quarter` mock returns a prescribed final sum.
        # The next call must receive that value via `anchor_override`.

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                **_entry_data(utility="ekz"),
            },
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)
        hass.async_add_executor_job = _async_noop

        # Prescribed per-quarter final sums, in the order they're called.
        quarter_finals = {
            Quarter(2025, 3): 14.28,
            Quarter(2025, 4): 54.53,
            Quarter(2026, 1): 114.71,
        }
        prices = {q: BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0) for q in quarter_finals}
        observed_overrides: list[tuple[str, float | None]] = []

        async def _fake_reimport_quarter(_hass, q, *, anchor_override=None, force_fresh=False):
            observed_overrides.append((str(q), anchor_override))
            return quarter_finals[q]

        estimate_anchor: list[float | None] = []

        async def _fake_estimate(_hass, *, anchor_override=None):
            estimate_anchor.append(anchor_override)
            return {}

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "fetch_quarterly", new=_async_return(prices)), \
             patch("homeassistant.components.recorder.get_instance", return_value=_recorder_instance_mock()):
            await svc._reimport_all_history(hass)

        # First quarter starts at 0; each subsequent receives the prior's final.
        assert observed_overrides[0] == ("2025Q3", 0.0)
        assert observed_overrides[1] == ("2025Q4", 14.28)
        assert observed_overrides[2] == ("2026Q1", 54.53)
        # Estimate path receives the post-loop cumulative sum.
        assert estimate_anchor == [114.71]

    @pytest.mark.asyncio
    async def test_failed_quarter_does_not_advance_cumulative(self):
        # If a quarter raises, the cumulative anchor should NOT advance,
        # so the next quarter still anchors at the prior good value.

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={
                CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
                CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
                **_entry_data(utility="ekz"),
            },
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)
        hass.async_add_executor_job = _async_noop

        prices = {
            Quarter(2025, 3): BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
            Quarter(2025, 4): BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
        }
        observed: list[tuple[str, float | None]] = []

        async def _fake_reimport_quarter(_hass, q, *, anchor_override=None, force_fresh=False):
            observed.append((str(q), anchor_override))
            if q == Quarter(2025, 3):
                # Q3 succeeds; advances anchor to 14.28.
                return 14.28
            # Q4 raises — anchor for any subsequent quarter (or the
            # estimate) should remain 14.28.
            raise RuntimeError("synthetic failure")

        estimate_anchor: list[float | None] = []

        async def _fake_estimate(_hass, *, anchor_override=None):
            estimate_anchor.append(anchor_override)
            return {}

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "fetch_quarterly", new=_async_return(prices)), \
             patch("homeassistant.components.recorder.get_instance", return_value=_recorder_instance_mock()):
            await svc._reimport_all_history(hass)

        assert observed[0] == ("2025Q3", 0.0)
        assert observed[1] == ("2025Q4", 14.28)
        # Estimate sees the last successful cumulative, not the failed Q4.
        assert estimate_anchor == [14.28]


class TestCoordinatorRefreshesRunningQuarter:
    """`_auto_import_newly_published` refreshes the running-quarter estimate."""

    def _make_coord(self, *, quarterly: dict, imported: dict | None = None,
                    history_valid_from: str = "2025-01-01"):

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=_entry_data(utility="ekz"),
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": history_valid_from,
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        coord.quarterly = quarterly
        coord._imported = imported or {}
        coord.hass = MagicMock()
        # v0.9.14 — `_auto_import_newly_published` wraps its body in this
        # lock; the BfeCoordinator(__new__) skip means we set it manually.
        coord._auto_import_lock = asyncio.Lock()
        # Sub-helpers that aren't relevant to these tests.
        coord._notify_skipped_quarters = _async_noop
        return coord

    @pytest.mark.asyncio
    async def test_refreshes_running_quarter_when_no_reimports(self):
        # No stale quarters → no `_reimport_quarter` calls → estimate
        # still runs (LTS-read path because no contiguous reimport).

        # Q1 2026 already in `_imported` and considered fresh.
        q1_2026 = Quarter(2026, 1)
        coord = self._make_coord(
            quarterly={
                q1_2026: BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0),
            },
            imported={"2026Q1": {"q_price_chf_mwh": 80.0, "snapshot": {}}},
        )
        # Force every prior to be 'fresh' (not stale).
        coord._snapshot_is_stale = MagicMock(return_value=False)

        estimate_calls: list[float | None] = []
        reimport_calls: list = []

        async def _fake_reimport_quarter(_hass, q):
            reimport_calls.append(q)
            return 0.0

        async def _fake_estimate(_hass, *, anchor_override=None):
            estimate_calls.append(anchor_override)
            return {}

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "_build_recompute_report", new=MagicMock()), \
             patch.object(svc, "_notify_recompute", new=MagicMock()):
            await coord._auto_import_newly_published()

        assert reimport_calls == []  # nothing was stale
        # Estimate fired anyway, with anchor_override=None (LTS-read path).
        assert estimate_calls == [None]

    @pytest.mark.asyncio
    async def test_chains_anchor_when_prev_quarter_was_just_reimported(self):
        # Q1 2026 reimported in this tick → running Q2 2026 should chain
        # its anchor through memory using Q1's returned final sum.

        q1_2026 = Quarter(2026, 1)
        coord = self._make_coord(
            quarterly={
                q1_2026: BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0),
            },
        )
        coord._snapshot_is_stale = MagicMock(return_value=True)

        estimate_calls: list[float | None] = []

        async def _fake_reimport_quarter(_hass, q):
            return 114.71  # Q1's final cumulative sum

        async def _fake_estimate(_hass, *, anchor_override=None):
            estimate_calls.append(anchor_override)
            return {}

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "_build_recompute_report", new=MagicMock()), \
             patch.object(svc, "_notify_recompute", new=MagicMock()):
            await coord._auto_import_newly_published()

        # Estimate received Q1's final sum as anchor_override (no LTS race).
        assert estimate_calls == [114.71]

    @pytest.mark.asyncio
    async def test_uses_lts_read_when_chain_not_contiguous(self):
        # Only Q3 2025 was reimported (e.g. utility change for that
        # quarter only). Running quarter Q2 2026 is NOT contiguous to
        # Q3 2025 → fall back to LTS-read path.

        q3_2025 = Quarter(2025, 3)
        coord = self._make_coord(
            quarterly={
                q3_2025: BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
            },
        )
        coord._snapshot_is_stale = MagicMock(return_value=True)

        estimate_calls: list[float | None] = []

        async def _fake_reimport_quarter(_hass, q):
            return 14.28

        async def _fake_estimate(_hass, *, anchor_override=None):
            estimate_calls.append(anchor_override)
            return {}

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "_build_recompute_report", new=MagicMock()), \
             patch.object(svc, "_notify_recompute", new=MagicMock()):
            await coord._auto_import_newly_published()

        # Q3 → Q4 → Q1 → Q2: not contiguous from Q3 → no override.
        assert estimate_calls == [None]

    @pytest.mark.asyncio
    async def test_skips_estimate_when_bfe_published_running_quarter(self):
        # If BFE has published the running quarter, the published-quarter
        # loop handles it via `_reimport_quarter`. The estimate must NOT
        # then overwrite that with a floor-based estimate.
        running_q = quarter_of(datetime.now(UTC))
        coord = self._make_coord(
            quarterly={
                running_q: BfePrice(chf_per_mwh=80.0, days=92, volume_mwh=0.0),
            },
        )
        coord._snapshot_is_stale = MagicMock(return_value=True)

        estimate_calls: list[float | None] = []

        async def _fake_reimport_quarter(_hass, q):
            return 100.0

        async def _fake_estimate(_hass, *, anchor_override=None):
            estimate_calls.append(anchor_override)
            return {}

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "_build_recompute_report", new=MagicMock()), \
             patch.object(svc, "_notify_recompute", new=MagicMock()):
            await coord._auto_import_newly_published()

        # Estimate did NOT fire — the published-quarter path handled it.
        assert estimate_calls == []


class TestFirstRefreshDefersAutoImport:
    """First refresh schedules `_auto_import_newly_published` as a background task."""

    def _make_coord(self, *, data):

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=_entry_data(utility="ekz"),
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        coord.quarterly = {}
        coord.monthly = {}
        coord._imported = {}
        coord.hass = MagicMock()
        coord.data = data
        coord._tariff_breakdown = MagicMock(return_value=None)
        return coord

    @pytest.mark.asyncio
    async def test_first_refresh_schedules_background_task(self):
        # `self.data is None` → schedule auto-import as a background task,
        # don't await it inline.

        coord = self._make_coord(data=None)
        background_tasks: list = []

        def _capture_bg(coro, *, name=None):
            background_tasks.append((coro, name))
            coro.close()  # don't actually run it
            return MagicMock()

        coord.hass.async_create_background_task = _capture_bg

        async def _fake_fetch_quarterly(_session):
            return {Quarter(2026, 1): BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0)}

        async def _fake_fetch_monthly(_session):
            return {}

        # Sentinel: if auto-import ran inline it would call this and fail.
        async def _fake_auto_import_inline(*, is_user_reload=False):
            raise AssertionError("auto-import must NOT run inline on first refresh")

        coord._auto_import_newly_published = _fake_auto_import_inline

        with patch.object(coord_mod, "fetch_quarterly", new=_fake_fetch_quarterly), \
             patch.object(coord_mod, "fetch_monthly", new=_fake_fetch_monthly):
            result = await coord._async_update_data()

        # Result returned promptly with BFE prices populated.
        assert "quarterly" in result
        # Auto-import was scheduled as a background task.
        assert len(background_tasks) == 1
        assert background_tasks[0][1].startswith("bfe_rueckliefertarif_initial_auto_import")

    @pytest.mark.asyncio
    async def test_subsequent_tick_runs_auto_import_inline(self):
        # `self.data` already populated → 6h tick path → run inline,
        # no background task.

        coord = self._make_coord(data={"already": "populated"})

        bg_called = []

        def _capture_bg(coro, *, name=None):
            bg_called.append((coro, name))
            coro.close()
            return MagicMock()

        coord.hass.async_create_background_task = _capture_bg

        async def _fake_fetch_quarterly(_session):
            return {Quarter(2026, 1): BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0)}

        async def _fake_fetch_monthly(_session):
            return {}

        inline_calls = []

        async def _fake_auto_import_inline(*, is_user_reload=False):
            inline_calls.append(True)

        coord._auto_import_newly_published = _fake_auto_import_inline

        with patch.object(coord_mod, "fetch_quarterly", new=_fake_fetch_quarterly), \
             patch.object(coord_mod, "fetch_monthly", new=_fake_fetch_monthly):
            await coord._async_update_data()

        # Inline path ran, background-task path did not.
        assert inline_calls == [True]
        assert bg_called == []


class TestAutoImportLock:
    """`_auto_import_newly_published` serializes concurrent invocations via lock."""

    @pytest.mark.asyncio
    async def test_lock_serializes_concurrent_invocations(self):
        # Two concurrent invocations of `_auto_import_newly_published`
        # must not interleave; the second waits until the first
        # releases the lock.
        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=_entry_data(utility="ekz"),
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        coord.quarterly = {}  # nothing to reimport
        coord._imported = {}
        coord.hass = MagicMock()
        coord._auto_import_lock = asyncio.Lock()
        coord._notify_skipped_quarters = _async_noop

        events: list[str] = []
        first_in_critical = asyncio.Event()
        let_first_finish = asyncio.Event()

        async def _slow_estimate(_hass, *, anchor_override=None):
            events.append("enter")
            if not first_in_critical.is_set():
                first_in_critical.set()
                await let_first_finish.wait()
            events.append("exit")
            return {}

        async def _fake_reimport_quarter(_hass, q):
            return 0.0

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_slow_estimate), \
             patch.object(svc, "_build_recompute_report", new=MagicMock()), \
             patch.object(svc, "_notify_recompute", new=MagicMock()):

            task_a = asyncio.create_task(coord._auto_import_newly_published())
            await first_in_critical.wait()
            # Second invocation must block on the lock — `task_b` will
            # not progress past `enter` until task_a releases.
            task_b = asyncio.create_task(coord._auto_import_newly_published())
            # Give task_b a chance to try (it should be lock-blocked).
            await asyncio.sleep(0)
            assert events == ["enter"], (
                "Second invocation entered critical section before lock release"
            )
            let_first_finish.set()
            await asyncio.gather(task_a, task_b)

        # Both invocations completed, in serialized order.
        assert events == ["enter", "exit", "enter", "exit"]


class TestApplyChangeNotificationIncludesRunningQuarter:
    """Auto-import recompute notifications include the running quarter."""

    @pytest.mark.asyncio
    async def test_running_quarter_appended_to_report_quarters(self):
        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=_entry_data(utility="ekz"),
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )

        running_q = quarter_of(datetime.now(UTC))
        # A published prior quarter that's stale (apply_change drift).
        stale_published = Quarter(running_q.year, running_q.q - 1) if running_q.q > 1 \
            else Quarter(running_q.year - 1, 4)

        coord.quarterly = {
            stale_published: BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0),
        }
        coord._imported = {}
        coord.hass = MagicMock()
        coord._auto_import_lock = asyncio.Lock()
        coord._notify_skipped_quarters = _async_noop
        coord._snapshot_is_stale = MagicMock(return_value=True)

        async def _fake_reimport_quarter(_hass, q):
            return 100.0

        async def _fake_estimate(_hass, *, anchor_override=None):
            return {}

        captured_quarters: list = []

        def _capture_report(_hass, quarters, **_kw):
            captured_quarters.append(list(quarters))
            return MagicMock()

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "_build_recompute_report", side_effect=_capture_report), \
             patch.object(svc, "_notify_recompute", new=MagicMock()):
            await coord._auto_import_newly_published()

        # Report builder received BOTH the stale-published quarter AND
        # the running quarter (regression test for "only previous
        # quarter is mentioned").
        assert len(captured_quarters) == 1
        assert stale_published in captured_quarters[0]
        assert running_q in captured_quarters[0]


class TestRunningQuarterStalenessGate:
    """apply_change reload re-runs the running-quarter estimate only when its config changed."""

    def _make_coord(self, *, quarterly: dict, imported: dict | None = None,
                    history_valid_from: str = "2025-01-01"):

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=_entry_data(utility="ekz"),
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": history_valid_from,
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        coord.quarterly = quarterly
        coord._imported = imported or {}
        coord.hass = MagicMock()
        coord._auto_import_lock = asyncio.Lock()
        coord._notify_skipped_quarters = _async_noop
        return coord

    @pytest.mark.parametrize(
        ("is_user_reload", "config_changed", "with_prior_snapshot",
         "expected_estimate_calls", "running_q_in_report"),
        [
            # apply_change reload, running config unchanged → estimate skipped.
            pytest.param(
                True, False, True, 0, False,
                id="apply_change_skips_when_running_q_unchanged",
            ),
            # apply_change reload, running config changed → estimate runs.
            pytest.param(
                True, True, True, 1, True,
                id="apply_change_runs_when_running_q_changed",
            ),
            # 6h tick (is_user_reload=False) → estimate runs unconditionally,
            # but running_q excluded from notification when config unchanged.
            pytest.param(
                False, False, True, 1, False,
                id="six_hour_tick_runs_estimate_unconditionally",
            ),
            # First-ever run (no prior snapshot) → estimate runs AND running_q
            # listed; let real `_running_q_config_changed` fire (returns True
            # for no-prior).
            pytest.param(
                True, None, False, 1, True,
                id="first_ever_estimate_runs_and_lists_running_quarter",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_running_quarter_estimate_gating(
        self, is_user_reload, config_changed, with_prior_snapshot,
        expected_estimate_calls, running_q_in_report,
    ):
        running_q = quarter_of(datetime.now(UTC))
        stale_q = Quarter(running_q.year - 1, 1)
        imported = (
            {str(running_q): {
                "q_price_chf_mwh": None,
                "snapshot": {"is_current_estimate": True},
            }}
            if with_prior_snapshot else {}
        )
        coord = self._make_coord(
            quarterly={stale_q: BfePrice(chf_per_mwh=80.0, days=90, volume_mwh=0.0)},
            imported=imported,
        )
        coord._snapshot_is_stale = MagicMock(return_value=True)
        if config_changed is not None:
            coord._running_q_config_changed = MagicMock(return_value=config_changed)
        # else: real impl returns True for no-prior-snapshot.

        estimate_calls: list = []
        captured_quarters: list = []

        async def _fake_reimport_quarter(_hass, q):
            return 100.0

        async def _fake_estimate(_hass, *, anchor_override=None):
            estimate_calls.append(anchor_override)
            return {}

        def _capture_report(_hass, quarters, **_kw):
            captured_quarters.append(list(quarters))
            return MagicMock()

        with patch.object(svc, "_reimport_quarter", new=_fake_reimport_quarter), \
             patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "_build_recompute_report", side_effect=_capture_report), \
             patch.object(svc, "_notify_recompute", new=MagicMock()):
            await coord._auto_import_newly_published(is_user_reload=is_user_reload)

        assert len(estimate_calls) == expected_estimate_calls
        assert len(captured_quarters) == 1
        assert stale_q in captured_quarters[0]
        assert (running_q in captured_quarters[0]) is running_q_in_report


class TestAsyncUpdateDataPassesIsUserReload:
    """`_async_update_data` passes `is_user_reload` correctly to background vs inline path."""

    def _make_coord(self, *, data):

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            entry_id="entry_xyz",
            data=_entry_data(utility="ekz"),
            options={
                OPT_CONFIG_HISTORY: [
                    {
                        "valid_from": "2025-01-01",
                        "valid_to": None,
                        "config": _entry_data(utility="ekz"),
                    }
                ]
            },
        )
        coord.quarterly = {}
        coord.monthly = {}
        coord._imported = {}
        coord.hass = MagicMock()
        coord.data = data
        coord._tariff_breakdown = MagicMock(return_value=None)
        return coord

    @pytest.mark.asyncio
    async def test_first_refresh_passes_is_user_reload_true(self):

        coord = self._make_coord(data=None)

        captured_kwargs: list = []

        async def _noop():
            return None

        def _fake_auto_import(**kwargs):
            captured_kwargs.append(kwargs)
            return _noop()

        coord._auto_import_newly_published = _fake_auto_import
        coord.hass.async_create_background_task = lambda coro, *, name=None: (coro.close(), MagicMock())[1]

        async def _fake_fetch_quarterly(_session):
            return {}

        async def _fake_fetch_monthly(_session):
            return {}

        with patch.object(coord_mod, "fetch_quarterly", new=_fake_fetch_quarterly), \
             patch.object(coord_mod, "fetch_monthly", new=_fake_fetch_monthly):
            await coord._async_update_data()

        assert captured_kwargs == [{"is_user_reload": True}]

    @pytest.mark.asyncio
    async def test_subsequent_tick_passes_is_user_reload_false(self):

        coord = self._make_coord(data={"already": "populated"})

        captured_kwargs: list = []

        async def _fake_auto_import(**kwargs):
            captured_kwargs.append(kwargs)

        coord._auto_import_newly_published = _fake_auto_import

        async def _fake_fetch_quarterly(_session):
            return {}

        async def _fake_fetch_monthly(_session):
            return {}

        with patch.object(coord_mod, "fetch_quarterly", new=_fake_fetch_quarterly), \
             patch.object(coord_mod, "fetch_monthly", new=_fake_fetch_monthly):
            await coord._async_update_data()

        assert captured_kwargs == [{"is_user_reload": False}]


def _async_return(value):
    async def _f(*args, **kwargs):
        return value
    return _f


def _recorder_instance_mock():
    """Recorder-instance mock for v0.9.4 dispatch.

    services._reimport_all_history calls the recorder's first-class API:
    `instance.async_clear_statistics(stat_ids)` (sync @callback, queues a
    ClearStatisticsTask on the recorder thread) followed by
    `await instance.async_block_till_done()` (drains the task queue).
    Tests patch `homeassistant.components.recorder.get_instance` to
    return this mock so both call sites succeed without a real recorder.
    """
    inst = MagicMock()
    inst.async_clear_statistics = MagicMock(return_value=None)

    async def _block_till_done():
        return None

    inst.async_block_till_done = _block_till_done
    return inst


class TestRunningQuarterEstimatePerHourRates:
    """`_import_running_quarter_estimate` resolves rate per hour via the rate-breakdown helper."""

    @pytest.mark.asyncio
    async def test_ht_nt_utility_applies_correct_rate_per_hour(self):
        # Synthetic EWZ-shaped fixed_ht_nt config: HT 10.50 / NT 6.45,
        # HKN 3.00 (opt-in Yes), Mo–Sa 06–22 HT window. Two synthetic
        # export hours land in 2026Q2 — one in HT (Tue 2026-04-07 09:00
        # UTC = 11:00 CEST → HT mofr 06–22) and one in NT (Tue 2026-04-07
        # 21:00 UTC = 23:00 CEST → after 22:00 → NT). Both 2 kWh.

        resolved = ResolvedTariff(
            utility_key="ewz",
            valid_from="2026-01-01",
            settlement_period="quartal",
            base_model="fixed_ht_nt",
            fixed_rp_kwh=None,
            fixed_ht_rp_kwh=10.50,
            fixed_nt_rp_kwh=6.45,
            hkn_rp_kwh=3.00,
            hkn_structure="additive_optin",
            cap_rp_kwh=None,
            federal_floor_rp_kwh=6.00,
            federal_floor_label="<30 kW",
            price_floor_rp_kwh=None,
            tariffs_json_version="1.0.0",
            tariffs_json_source="remote",
            ht_window={"mofr": [6, 22], "sa": [6, 22], "su": None},
            seasonal=None,
        )
        tariff_cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=8.0,
            hkn_aktiviert=True,
            hkn_rp_kwh_resolved=3.00,
            resolved=resolved,
        )
        cfg = {
            CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
            CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }

        hass = MagicMock()
        hass.async_create_task = _close_coro
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop
        coordinator.data = {"current_tariff_rp_kwh": 12.91}

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={**cfg, **_entry_data(utility="ewz")},
            options={},
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)

        async def _fake_cfg_for_entry(_hass, **_kw):
            return cfg, tariff_cfg

        # `_cfg_for_entry` is sync, not async — return a plain tuple.
        def _sync_cfg(_hass, **_kw):
            return cfg, tariff_cfg

        # Read-the-export-LTS mock: 2 kWh at HT, 2 kWh at NT.
        ht_hour = datetime(2026, 4, 7, 9, 0, tzinfo=UTC)   # 11:00 CEST → HT
        nt_hour = datetime(2026, 4, 7, 21, 0, tzinfo=UTC)  # 23:00 CEST → NT
        synthetic_kwh = {ht_hour: 2.0, nt_hour: 2.0}

        async def _fake_read_hourly_export(_hass, _stat_id, _start, _end):
            # Filter to within the requested window — caller passes
            # [q_start_utc, last_full_hour). Our two test hours are early
            # in 2026Q2; if `now` is later they fall inside.
            return {h: kwh for h, kwh in synthetic_kwh.items() if _start <= h < _end}

        async def _fake_read_anchor(_hass, _stat_id, _at):
            return 0.0

        async def _fake_import_statistics(*args, **kwargs):
            return None

        with patch.object(svc, "_cfg_for_entry", new=_sync_cfg), \
             patch.object(svc, "read_hourly_export", new=_fake_read_hourly_export), \
             patch.object(svc, "read_compensation_anchor", new=_fake_read_anchor), \
             patch.object(svc, "import_statistics", new=_fake_import_statistics):
            result = await svc._import_running_quarter_estimate(hass)

        # Effective per-hour rates:
        # HT: 10.50 base + 3.00 HKN = 13.50 Rp/kWh → 2 kWh × 13.50 / 100 = 0.27 CHF
        # NT:  6.45 base + 3.00 HKN =  9.45 Rp/kWh → 2 kWh ×  9.45 / 100 = 0.189 CHF
        # Total: 0.459 CHF, 4 kWh.
        assert result["hours_imported"] >= 2
        assert abs(result["chf_total"] - 0.459) < 1e-3, result
        # kWh-weighted avg rate = 0.459 × 100 / 4 = 11.475 Rp/kWh.
        assert abs(result["rate_rp_kwh"] - 11.475) < 1e-3

        # Snapshot's per-period entry now carries Base / HKN / intended_hkn
        # populated (NOT None like pre-v0.9.5).
        snap = coordinator._imported["2026Q2"]["snapshot"]
        period = snap["periods"][0]
        # Base = kWh-weighted avg of HT 10.50 and NT 6.45 over equal kWh:
        #   (10.50 + 6.45) / 2 = 8.475
        assert abs(period["base_rp_kwh_avg"] - 8.475) < 1e-3
        # HKN is flat 3.00 across both hours.
        assert abs(period["hkn_rp_kwh_avg"] - 3.000) < 1e-3
        # Intended HKN = published rate (3.00) since hkn_aktiviert=True.
        assert abs(period["intended_hkn_rp_kwh"] - 3.000) < 1e-3
        # Total rate matches kWh-weighted avg.
        assert abs(period["rate_rp_kwh_avg"] - 11.475) < 1e-3
        # is_current_estimate flag still set.
        assert snap["is_current_estimate"] is True
        # is_estimate=False since fixed_ht_nt produces exact rates (only
        # rmp_quartal/rmp_monat are real estimates).
        assert result["is_estimate"] is False

    @pytest.mark.asyncio
    async def test_fixed_flat_utility_unchanged_behavior(self):
        # Regression: a fixed_flat utility (no HT/NT split) should produce
        # constant per-hour rates equal to fixed_rp_kwh + HKN. Base/HKN
        # cells now light up but with the same value across all hours.

        resolved = ResolvedTariff(
            utility_key="age_sa",
            valid_from="2026-01-01",
            settlement_period="quartal",
            base_model="fixed_flat",
            fixed_rp_kwh=8.00,
            fixed_ht_rp_kwh=None,
            fixed_nt_rp_kwh=None,
            hkn_rp_kwh=4.00,
            hkn_structure="additive_optin",
            cap_rp_kwh=None,
            federal_floor_rp_kwh=6.00,
            federal_floor_label="<30 kW",
            price_floor_rp_kwh=None,
            tariffs_json_version="1.0.0",
            tariffs_json_source="remote",
            ht_window=None,
            seasonal=None,
        )
        tariff_cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=8.0,
            hkn_aktiviert=True,
            hkn_rp_kwh_resolved=4.00,
            resolved=resolved,
        )
        cfg = {
            CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
            CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }

        hass = MagicMock()
        hass.async_create_task = _close_coro
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop
        coordinator.data = {"current_tariff_rp_kwh": 12.0}
        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={**cfg, **_entry_data(utility="age_sa")},
            options={},
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)

        ht_hour = datetime(2026, 4, 7, 9, 0, tzinfo=UTC)
        nt_hour = datetime(2026, 4, 7, 21, 0, tzinfo=UTC)
        synthetic = {ht_hour: 2.0, nt_hour: 2.0}

        async def _fake_read_hourly_export(_hass, _stat_id, _start, _end):
            return {h: kwh for h, kwh in synthetic.items() if _start <= h < _end}

        async def _fake_read_anchor(*_args, **_kw):
            return 0.0

        async def _fake_import_statistics(*args, **kwargs):
            return None

        def _sync_cfg(_hass, **_kw):
            return cfg, tariff_cfg

        with patch.object(svc, "_cfg_for_entry", new=_sync_cfg), \
             patch.object(svc, "read_hourly_export", new=_fake_read_hourly_export), \
             patch.object(svc, "read_compensation_anchor", new=_fake_read_anchor), \
             patch.object(svc, "import_statistics", new=_fake_import_statistics):
            result = await svc._import_running_quarter_estimate(hass)

        # fixed_flat: base = 8.00, HKN = 4.00, total = 12.00 every hour.
        # 4 kWh × 12.00 / 100 = 0.48 CHF.
        assert abs(result["chf_total"] - 0.48) < 1e-3
        snap = coordinator._imported["2026Q2"]["snapshot"]
        period = snap["periods"][0]
        assert abs(period["base_rp_kwh_avg"] - 8.000) < 1e-3
        assert abs(period["hkn_rp_kwh_avg"] - 4.000) < 1e-3
        assert abs(period["rate_rp_kwh_avg"] - 12.000) < 1e-3


class TestRunningEstimateDuringFirstRefresh:
    """Running-quarter estimate runs even when `coordinator.data` is still None."""

    @pytest.mark.asyncio
    async def test_runs_when_coordinator_data_is_none(self):

        resolved = ResolvedTariff(
            utility_key="age_sa",
            valid_from="2026-01-01",
            settlement_period="quartal",
            base_model="fixed_flat",
            fixed_rp_kwh=8.00,
            fixed_ht_rp_kwh=None,
            fixed_nt_rp_kwh=None,
            hkn_rp_kwh=4.00,
            hkn_structure="additive_optin",
            cap_rp_kwh=None,
            federal_floor_rp_kwh=6.00,
            federal_floor_label="<30 kW",
            price_floor_rp_kwh=None,
            tariffs_json_version="1.0.0",
            tariffs_json_source="remote",
            ht_window=None,
            seasonal=None,
        )
        tariff_cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kwp=8.0,
            hkn_aktiviert=True,
            hkn_rp_kwh_resolved=4.00,
            resolved=resolved,
        )
        cfg = {
            CONF_STROMNETZEINSPEISUNG_KWH: "sensor.export",
            CONF_RUECKLIEFERVERGUETUNG_CHF: "sensor.compensation",
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }

        hass = MagicMock()
        hass.async_create_task = _close_coro
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop
        # First-refresh state: DataUpdateCoordinator.data is still None
        # while `_async_update_data` populates it. Pre-fix this raised
        # "Coordinator not ready — run 'Refresh prices from BFE' first".
        coordinator.data = None

        live_entry = SimpleNamespace(
            entry_id="entry_xyz",
            data={**cfg, **_entry_data(utility="age_sa")},
            options={},
        )
        hass.data = {
            DOMAIN: {
                "entry_xyz": {
                    "config": dict(live_entry.data),
                    "options": dict(live_entry.options),
                    "coordinator": coordinator,
                }
            }
        }
        hass.config_entries.async_get_entry = MagicMock(return_value=live_entry)

        ht_hour = datetime(2026, 4, 7, 9, 0, tzinfo=UTC)
        nt_hour = datetime(2026, 4, 7, 21, 0, tzinfo=UTC)
        synthetic = {ht_hour: 2.0, nt_hour: 2.0}

        async def _fake_read_hourly_export(_hass, _stat_id, _start, _end):
            return {h: kwh for h, kwh in synthetic.items() if _start <= h < _end}

        async def _fake_read_anchor(*_args, **_kw):
            return 0.0

        async def _fake_import_statistics(*args, **kwargs):
            return None

        def _sync_cfg(_hass, **_kw):
            return cfg, tariff_cfg

        with patch.object(svc, "_cfg_for_entry", new=_sync_cfg), \
             patch.object(svc, "read_hourly_export", new=_fake_read_hourly_export), \
             patch.object(svc, "read_compensation_anchor", new=_fake_read_anchor), \
             patch.object(svc, "import_statistics", new=_fake_import_statistics):
            # Must NOT raise "Coordinator not ready …".
            result = await svc._import_running_quarter_estimate(hass)

        # Estimate completed and wrote a snapshot for the running quarter.
        assert "2026Q2" in coordinator._imported
        assert result["chf_total"] >= 0.0


class TestRenderConfigBlockShared:
    """v0.9.2 — `_render_config_block` is the shared bullet-list renderer
    used by both the active-today block and each per-group "Configuration in
    effect" block. Same field labels in both places."""

    def test_shared_helper_emits_consistent_labels(self):
        from custom_components.bfe_rueckliefertarif.services import (
            _render_active_today_block,
            _render_config_block,
        )
        c = {
            "utility_key": "ekz",
            "utility_name": "EKZ",
            "base_model": "rmp_quartal",
            "settlement_period": "quartal",
            "kwp": 8.0,
            "eigenverbrauch": True,
            "hkn_optin": True,
            "hkn_rp_kwh": 3.0,
            "billing": ABRECHNUNGS_RHYTHMUS_QUARTAL,
            "floor_label": "<30 kW",
            "floor_rp_kwh": 6.0,
            "cap_rp_kwh": 10.96,
            "tariffs_version": "1.0.0",
            "tariffs_source": "remote",
        }
        today_block = "\n".join(_render_active_today_block(c))
        group_block = "\n".join(_render_config_block(c, is_today=False))

        # v0.17.1 — Both blocks contain the same set of label lines under
        # the new grouped layout (differing only on the "current cap" vs
        # "cap" wording, intentionally). "Billing period" + "Seasonal rates"
        # main bullets dropped (Issues 8.2 + 8.3); the period info now lives
        # in the Tariff-model sub-bullets.
        for label in (
            "**Utility:**",
            "**Tariff model:**",
            "- **Configuration:**",
            "    - **Installed power:**",
            "    - **Self-consumption:**",
            "    - **HKN opt-in:**",
            "**Federal floor (Mindestvergütung):**",
            "**Cap (Anrechenbarkeitsgrenze):**",
            "**Tariff data:**",
        ):
            assert label in today_block, f"{label} missing in today block"
            assert label in group_block, f"{label} missing in group block"

        # is_today flag controls the cap-line wording.
        assert "current cap" in today_block
        assert "current cap" not in group_block


class TestManageHistoryLabels:
    """Manage-history menu distinguishes active (`→ now`) vs future (`→ ...`) records."""

    def _make_flow(self, history):
        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data={CONF_STROMNETZEINSPEISUNG_KWH: "sensor.foo"},
            options=MappingProxyType({OPT_CONFIG_HISTORY: history}),
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow

    @pytest.mark.asyncio
    async def test_active_record_renders_now(self):
        # Single open record from 2024 — clearly in the past, currently active.
        history = [
            {"valid_from": "2024-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow = self._make_flow(history)
        with patch(
            "custom_components.bfe_rueckliefertarif.config_flow.date"
        ) as mock_date:
            mock_date.today.return_value.isoformat.return_value = "2026-04-27"
            result = await flow.async_step_manage_history()
        labels = result["menu_options"]
        assert "→ now" in labels["edit_pick_row_0"]
        assert "→ ..." not in labels["edit_pick_row_0"]

    @pytest.mark.asyncio
    async def test_future_record_renders_ellipsis(self):
        # Two-record chain: closed past record + future-scheduled open record.
        history = [
            {"valid_from": "2024-01-01", "valid_to": "2026-07-01",
             "config": _entry_data(utility="ekz")},
            {"valid_from": "2026-07-01", "valid_to": None,
             "config": _entry_data(utility="age_sa")},
        ]
        flow = self._make_flow(history)
        with patch(
            "custom_components.bfe_rueckliefertarif.config_flow.date"
        ) as mock_date:
            mock_date.today.return_value.isoformat.return_value = "2026-04-27"
            result = await flow.async_step_manage_history()
        labels = result["menu_options"]
        # Closed past record renders the explicit valid_to.
        assert "→ 2026-07-01" in labels["edit_pick_row_0"]
        # Future open record renders ... and NOT now.
        assert "→ ..." in labels["edit_pick_row_1"]
        assert "→ now" not in labels["edit_pick_row_1"]

    @pytest.mark.asyncio
    async def test_record_starting_today_renders_now(self):
        # Boundary: valid_from == today → still counts as active (<= today).
        history = [
            {"valid_from": "2026-04-27", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow = self._make_flow(history)
        with patch(
            "custom_components.bfe_rueckliefertarif.config_flow.date"
        ) as mock_date:
            mock_date.today.return_value.isoformat.return_value = "2026-04-27"
            result = await flow.async_step_manage_history()
        labels = result["menu_options"]
        assert "→ now" in labels["edit_pick_row_0"]
        assert "→ ..." not in labels["edit_pick_row_0"]
