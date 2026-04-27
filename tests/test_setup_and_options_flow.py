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
            "estimate_basis": "fixpreis",
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
        assert row.estimate_basis == "fixpreis"
        assert row.utility_key_at_period == "ewz"

    def test_renderer_marks_estimate_row_visually(self):
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
                estimate_basis="fixpreis",
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
        # Estimate marker rendered in the period column.
        assert "*(estimate · fixpreis)*" in body


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


class TestRefreshPricesStep:
    """v0.9.0 — the OptionsFlow ``refresh_prices`` step delegates to
    ``_refresh_coordinator`` after confirmation."""

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
    async def test_confirmed_invokes_coordinator_refresh(self):
        from unittest.mock import AsyncMock

        flow = self._make_flow()
        with patch(
            "custom_components.bfe_rueckliefertarif.services._refresh_coordinator",
            new=AsyncMock(return_value={"available": [], "newly_imported": []}),
        ) as mock_refresh:
            result = await flow.async_step_refresh_prices({"confirm": True})
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
