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
    BfeRuecklieferTarifFlow,
    BfeRuecklieferTarifOptionsFlow,
)
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_NAMENSPRAEFIX,
    CONF_PLANT_NAME,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    CONF_VALID_FROM,
    DOMAIN,
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
        from custom_components.bfe_rueckliefertarif.coordinator import BfeCoordinator

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            data={"stromnetzeinspeisung_kwh": "sensor.foo"},
            options={OPT_CONFIG_HISTORY: [self._open_record(utility="ekz")]},
        )
        assert coord._config[CONF_ENERGIEVERSORGER] == "ekz"
        # Entity wiring from entry.data merged in.
        assert coord._config["stromnetzeinspeisung_kwh"] == "sensor.foo"

        # Mutate live history (e.g. via OptionsFlow apply_change).
        coord.entry.options = {
            OPT_CONFIG_HISTORY: [self._open_record(utility="age_sa")]
        }
        assert coord._config[CONF_ENERGIEVERSORGER] == "age_sa"

    def test_config_property_history_overrides_entry_data_versioned(self):
        # Pre-A+ entries may still carry versioned fields in entry.data.
        # Post-A+ those must be ignored — history wins.
        from custom_components.bfe_rueckliefertarif.coordinator import BfeCoordinator

        coord = BfeCoordinator.__new__(BfeCoordinator)
        coord.entry = SimpleNamespace(
            data=_entry_data(utility="legacy_stale"),  # old/stale value
            options={OPT_CONFIG_HISTORY: [self._open_record(utility="ekz")]},
        )
        assert coord._config[CONF_ENERGIEVERSORGER] == "ekz"


async def _async_noop(*args, **kwargs):
    """Async stub that does nothing — for replacing service/setup calls."""
    return None


class TestApplyChangeWizard:
    """v0.9.0 — the OptionsFlow ``apply_change`` wizard inlines what the
    deleted ``tariff`` step + ``_apply_config_change`` helper used to do."""

    def _make_flow(self, options, data=None):
        from unittest.mock import AsyncMock

        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        # async_add_executor_job is awaited in _async_warm_cache.
        flow.hass.async_add_executor_job = AsyncMock(return_value=None)
        flow_entry = SimpleNamespace(
            entry_id="test_entry_id",
            data=data or {"stromnetzeinspeisung_kwh": "sensor.foo"},
            options=MappingProxyType(options),
        )
        flow.handler = "test_entry_id"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow, flow_entry

    @pytest.mark.asyncio
    async def test_creates_new_record_with_all_fields(self):
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="age_sa", kw=10.0)},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await flow.async_step_apply_change(
            {
                "effective_date": "2026-04-01",
                CONF_ENERGIEVERSORGER: "ekz",
                CONF_INSTALLIERTE_LEISTUNG_KW: 12.5,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: True,
                CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
            }
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
        assert history[1]["config"][CONF_INSTALLIERTE_LEISTUNG_KW] == 12.5
        assert history[1]["config"][CONF_HKN_AKTIVIERT] is True

    @pytest.mark.asyncio
    async def test_invalid_effective_date_re_renders_form(self):
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await flow.async_step_apply_change(
            {
                "effective_date": "garbage",
                CONF_ENERGIEVERSORGER: "ekz",
                CONF_INSTALLIERTE_LEISTUNG_KW: 8.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: False,
                CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
            }
        )
        # Form re-renders with the error.
        assert result["type"].name in ("FORM", "form")
        assert result["errors"] == {"effective_date": "invalid_valid_from"}

    @pytest.mark.asyncio
    async def test_kw_zero_re_renders_form(self):
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz")},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await flow.async_step_apply_change(
            {
                "effective_date": "2026-04-01",
                CONF_ENERGIEVERSORGER: "ekz",
                CONF_INSTALLIERTE_LEISTUNG_KW: 0.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: False,
                CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
            }
        )
        assert result["type"].name in ("FORM", "form")
        assert result["errors"] == {CONF_INSTALLIERTE_LEISTUNG_KW: "kw_required"}

    @pytest.mark.asyncio
    async def test_no_op_change_does_not_duplicate_record(self):
        # Submitting the exact same values for the open record's date should
        # not create a duplicate — the wizard treats this as a no-op.
        existing = [
            {"valid_from": "2026-01-01", "valid_to": None,
             "config": _entry_data(utility="ekz", kw=8.0)},
        ]
        flow, _ = self._make_flow({OPT_CONFIG_HISTORY: existing})
        result = await flow.async_step_apply_change(
            {
                "effective_date": "2026-01-01",
                CONF_ENERGIEVERSORGER: "ekz",
                CONF_INSTALLIERTE_LEISTUNG_KW: 8.0,
                CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                CONF_HKN_AKTIVIERT: True,
                CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
            }
        )
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        # Options unchanged, history is still 1 record.
        history = result["data"][OPT_CONFIG_HISTORY]
        assert len(history) == 1


class TestRecomputeHistoryEstimate:
    """v0.9.0 — _reimport_all_history appends a running-quarter estimate row;
    the recompute report propagates `is_current_estimate` to its rows; the
    notification renderer marks them visually."""

    def test_report_row_carries_is_current_estimate(self):
        # Simulate a coordinator with one estimate snapshot in _imported,
        # build the report, assert the row is flagged as estimate.
        from custom_components.bfe_rueckliefertarif.const import DOMAIN
        from custom_components.bfe_rueckliefertarif.quarters import Quarter
        from custom_components.bfe_rueckliefertarif.services import (
            _build_recompute_report,
        )

        snapshot = {
            "rate_rp_kwh": 7.91,
            "kw": 8.0,
            "eigenverbrauch_aktiviert": True,
            "hkn_rp_kwh": 5.0,
            "hkn_optin": True,
            "cap_mode": False,
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
                "stromnetzeinspeisung_kwh": "sensor.foo",
                "rueckliefervergutung_chf": "sensor.bar",
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
            _RecomputeReport,
            _RecomputeReportRow,
            _format_recompute_notification,
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
                cap_mode_at_period=False,
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
                "kw": 8.0,
                "eigenverbrauch": True,
                "hkn_optin": True,
                "hkn_rp_kwh": 5.0,
                "billing": ABRECHNUNGS_RHYTHMUS_QUARTAL,
                "floor_label": None,
                "floor_rp_kwh": None,
                "cap_mode": False,
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
    """v0.9.0 — the OptionsFlow ``recompute_history`` step delegates to
    ``_reimport_all_history`` after confirmation."""

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
        from unittest.mock import AsyncMock

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
    """v0.9.6 — `_refresh_upstream_data` runs both fetch operations: BFE
    prices via `BfeCoordinator.async_refresh()` AND tariffs.json via
    `TariffsDataCoordinator.async_refresh()`. Returns a dict that surfaces
    both fetch statuses for the OptionsFlow renderer."""

    def _make_hass(self, *, tdc_refresh_returns=True, tdc_last_error=None):
        from unittest.mock import AsyncMock

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
        from custom_components.bfe_rueckliefertarif import services as svc

        hass, coordinator, tdc = self._make_hass(tdc_refresh_returns=True)
        result = await svc._refresh_upstream_data(hass)

        coordinator.async_refresh.assert_awaited_once()
        tdc.async_refresh.assert_awaited_once()
        assert result["tariffs_refreshed"] is True
        assert result["tariffs_version"] == "1.0.1"
        assert result["tariffs_error"] is None

    @pytest.mark.asyncio
    async def test_continues_when_tariffs_refresh_fails(self):
        from custom_components.bfe_rueckliefertarif import services as svc

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
        from unittest.mock import AsyncMock

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
        from unittest.mock import AsyncMock

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
        from unittest.mock import AsyncMock

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
            CONF_INSTALLIERTE_LEISTUNG_KW: 8.0,
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
    """v0.9.2 — _reimport_all_history wipes LTS + the in-memory snapshot map
    BEFORE iterating quarters, so the first run after a fresh install is
    idempotent (no stale rows from HA's energy-component auto-compensation
    poisoning the cumulative sum chain)."""

    @pytest.mark.asyncio
    async def test_clear_via_recorder_task_queue_then_block_till_done(self):
        # v0.9.4: clear_statistics MUST be queued onto the recorder's main
        # thread via `Recorder.async_clear_statistics(...)` (which uses
        # `queue_task` internally). Calling the underlying sync
        # `clear_statistics` on ANY executor thread (DbWorker, hass exec)
        # trips HA's `_assert_in_recorder_thread` guard. After queueing, we
        # `await instance.async_block_till_done()` so anchor reads in the
        # subsequent quarter loop don't race with the queued clear.
        from custom_components.bfe_rueckliefertarif import services as svc

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {"2025Q4": {"snapshot": {}}}
        coordinator._async_save_state = _async_noop_factory()

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

        async def _fake_reimport_quarter(_hass, q):
            order.append(f"quarter:{q}")

        async def _fake_estimate(_hass):
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
        from custom_components.bfe_rueckliefertarif import services as svc
        from custom_components.bfe_rueckliefertarif.bfe import BfePrice
        from custom_components.bfe_rueckliefertarif.quarters import Quarter

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop_factory()

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
        hass.async_add_executor_job = _async_noop_factory()

        imported_quarters: list[Quarter] = []

        async def _fake_reimport_quarter(_hass, q):
            imported_quarters.append(q)

        async def _fake_estimate(_hass):
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
        from custom_components.bfe_rueckliefertarif import services as svc

        hass = MagicMock()
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop_factory()

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
        hass.async_add_executor_job = _async_noop_factory()

        async def _fake_estimate(_hass):
            return {}

        with patch.object(svc, "_import_running_quarter_estimate", new=_fake_estimate), \
             patch.object(svc, "fetch_quarterly", new=_async_return({})), \
             patch("homeassistant.components.recorder.get_instance", return_value=_recorder_instance_mock()):
            result = await svc._reimport_all_history(hass)
        assert result["before_active"] == []


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


def _async_noop_factory():
    async def _f(*args, **kwargs):
        return None
    return _f


class TestRunningQuarterEstimatePerHourRates:
    """v0.9.5 — `_import_running_quarter_estimate` resolves the rate per hour
    via `_effective_rate_breakdown_at_hour` (was: one flat rate for every
    hour). For HT/NT utilities, the right rate is applied to each hour's
    kWh based on its Zurich-local time / day. The snapshot's per-period
    dict comes from `_aggregate_by_period` so Base / HKN / intended_hkn
    columns are populated with kWh-weighted averages."""

    @pytest.mark.asyncio
    async def test_ht_nt_utility_applies_correct_rate_per_hour(self):
        # Synthetic EWZ-shaped fixed_ht_nt config: HT 10.50 / NT 6.45,
        # HKN 3.00 (opt-in Yes), Mo–Sa 06–22 HT window. Two synthetic
        # export hours land in 2026Q2 — one in HT (Tue 2026-04-07 09:00
        # UTC = 11:00 CEST → HT mofr 06–22) and one in NT (Tue 2026-04-07
        # 21:00 UTC = 23:00 CEST → after 22:00 → NT). Both 2 kWh.
        from datetime import datetime, timezone

        from custom_components.bfe_rueckliefertarif import services as svc
        from custom_components.bfe_rueckliefertarif.importer import TariffConfig
        from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff

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
            cap_mode=False,
            cap_rp_kwh=None,
            federal_floor_rp_kwh=6.00,
            federal_floor_label="<30 kW",
            requires_naturemade_star=False,
            price_floor_rp_kwh=None,
            tariffs_json_version="1.0.0",
            tariffs_json_source="remote",
            ht_window={"mofr": [6, 22], "sa": [6, 22], "su": None},
            seasonal=None,
        )
        tariff_cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kw=8.0,
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
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop_factory()
        # `_import_running_quarter_estimate` only checks `coordinator.data`
        # is truthy now (the rate logic no longer reads `tariff_breakdown`).
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
        ht_hour = datetime(2026, 4, 7, 9, 0, tzinfo=timezone.utc)   # 11:00 CEST → HT
        nt_hour = datetime(2026, 4, 7, 21, 0, tzinfo=timezone.utc)  # 23:00 CEST → NT
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
        from datetime import datetime, timezone

        from custom_components.bfe_rueckliefertarif import services as svc
        from custom_components.bfe_rueckliefertarif.importer import TariffConfig
        from custom_components.bfe_rueckliefertarif.tariffs_db import ResolvedTariff

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
            cap_mode=False,
            cap_rp_kwh=None,
            federal_floor_rp_kwh=6.00,
            federal_floor_label="<30 kW",
            requires_naturemade_star=False,
            price_floor_rp_kwh=None,
            tariffs_json_version="1.0.0",
            tariffs_json_source="remote",
            ht_window=None,
            seasonal=None,
        )
        tariff_cfg = TariffConfig(
            eigenverbrauch_aktiviert=True,
            installierte_leistung_kw=8.0,
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
        coordinator = MagicMock()
        coordinator._imported = {}
        coordinator._async_save_state = _async_noop_factory()
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

        ht_hour = datetime(2026, 4, 7, 9, 0, tzinfo=timezone.utc)
        nt_hour = datetime(2026, 4, 7, 21, 0, tzinfo=timezone.utc)
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
            "kw": 8.0,
            "eigenverbrauch": True,
            "hkn_optin": True,
            "hkn_rp_kwh": 3.0,
            "billing": "quartal",
            "floor_label": "<30 kW",
            "floor_rp_kwh": 6.0,
            "cap_mode": True,
            "cap_rp_kwh": 10.96,
            "tariffs_version": "1.0.0",
            "tariffs_source": "remote",
        }
        today_block = "\n".join(_render_active_today_block(c))
        group_block = "\n".join(_render_config_block(c, is_today=False))

        # Both blocks contain the same set of label lines (differing only on
        # the "current cap" vs "cap" wording, intentionally).
        for label in (
            "**Utility:**",
            "**Tariff model:**",
            "**Installed power:**",
            "**Eigenverbrauch (self-consumption):**",
            "**HKN opt-in:**",
            "**Billing period:**",
            "**Federal floor (Mindestvergütung):**",
            "**Cap mode (Anrechenbarkeitsgrenze):**",
            "**Tariff data:**",
        ):
            assert label in today_block, f"{label} missing in today block"
            assert label in group_block, f"{label} missing in group block"

        # is_today flag controls the cap-line wording.
        assert "current cap" in today_block
        assert "current cap" not in group_block
