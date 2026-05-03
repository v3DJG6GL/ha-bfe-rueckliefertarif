"""Tests for Phase 6: data_coordinator.py.

Covers the bits that don't need a running aiohttp loop — the validation
fallback path, override path resolution in tariffs_db, and that
``set_override_path`` round-trips correctly.

Network fetch is exercised in the live HA-side integration test (see
plan verification scenarios 9 & 10).
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from aioresponses import aioresponses

from custom_components.bfe_rueckliefertarif.data_coordinator import (
    REMOTE_SCHEMA_URL,
    REMOTE_URL,
    TariffsDataCoordinator,
    _scan_history_for_drift,
)
from custom_components.bfe_rueckliefertarif.repairs import TariffDriftRepairFlow
from custom_components.bfe_rueckliefertarif.tariffs_db import (
    _BUNDLED_DATA_PATH,
    _OVERRIDE_PATH,
    get_source,
    load_tariffs,
    set_override_path,
)


@pytest.fixture(autouse=True)
def reset_override():
    """Each test starts and ends with override cleared."""
    set_override_path(None)
    yield
    set_override_path(None)


class TestSetOverridePath:
    def test_initially_bundled(self):
        assert _OVERRIDE_PATH is None
        assert get_source() == "bundled"

    def test_override_path_changes_source(self, tmp_path):
        # Write a minimal valid tariffs.json into tmp_path
        cache = tmp_path / "tariffs.json"
        cache.write_text(json.dumps({
            "schema_version": "1.0.0",
            "last_updated": "2026-04-26",
            "federal_minimum": [{
                "valid_from": "2026-01-01", "valid_to": None,
                "rules": [{"kw_min": 0, "kw_max": 30, "self_consumption": None, "min_rp_kwh": 6.0}],
            }],
            "utilities": {"test": {
                "name_de": "Test",
                "homepage": "https://example.com",
                "rates": [{
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "power_tiers": [{
                        "kw_min": 0, "kw_max": None,
                        "base_model": "rmp_quartal",
                        "hkn_rp_kwh": 0.0,
                        "hkn_structure": "none",
                    }],
                }],
            }},
        }))
        set_override_path(cache)
        assert get_source() == "remote"
        loaded = load_tariffs()
        assert "test" in loaded["utilities"]

    def test_override_clear_reverts_to_bundled(self, tmp_path):
        set_override_path(tmp_path / "doesnt_exist.json")
        # Override pointer is set but file missing → load_tariffs falls back to bundled.
        loaded = load_tariffs()
        # bundled has 13 utilities including ekz
        assert "ekz" in loaded["utilities"]

        set_override_path(None)
        assert get_source() == "bundled"

    def test_override_to_missing_file_falls_back(self, tmp_path):
        set_override_path(tmp_path / "no.json")
        # The override path is "set" but file doesn't exist; load_tariffs
        # uses bundled. get_source() still returns "remote" since the pointer
        # is set — that's a known caveat (callers may want to verify isfile).
        loaded = load_tariffs()
        assert "ekz" in loaded["utilities"]


class TestLooseValidate:
    def test_accepts_minimal_valid(self):
        TariffsDataCoordinator._loose_validate({
            "schema_version": "1.0.0",
            "federal_minimum": [{}],
            "utilities": {"x": {}},
        })

    @pytest.mark.parametrize(
        "payload,match_pattern",
        [
            (
                {"federal_minimum": [{}], "utilities": {"x": {}}},
                "schema_version",
            ),
            (
                {"schema_version": "1.0.0",
                 "federal_minimum": [{}], "utilities": {}},
                "utilities",
            ),
            (
                {"schema_version": "1.0.0",
                 "federal_minimum": [], "utilities": {"x": {}}},
                "federal_minimum",
            ),
        ],
    )
    def test_rejects_invalid(self, payload, match_pattern):
        with pytest.raises(ValueError, match=match_pattern):
            TariffsDataCoordinator._loose_validate(payload)


class TestBundledStillWorks:
    """bundled tariffs.json must always be loadable."""

    def test_bundled_path_exists(self):
        assert _BUNDLED_DATA_PATH.is_file()

    def test_bundled_loads_with_no_override(self):
        loaded = load_tariffs()
        assert "ekz" in loaded["utilities"]


class TestScanHistoryForDrift:
    """Drift scanner. Walks one config entry's OPT_CONFIG_HISTORY against
    the current tariff schema; emits a descriptor per stale (entry × period)
    pair."""

    def _make_entry(self, history):
        from custom_components.bfe_rueckliefertarif.const import OPT_CONFIG_HISTORY
        return SimpleNamespace(
            entry_id="test_entry_id",
            options={OPT_CONFIG_HISTORY: history},
        )

    def _patch_db(self, monkeypatch, rates):
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2024-01-01",
                "valid_to": None,
                "rules": [{"kw_min": 0, "kw_max": None,
                           "self_consumption": None, "min_rp_kwh": 4.0}],
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

    def test_no_drift_when_stored_matches_current(self, monkeypatch):
        rates = [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [{
                "key": "k", "type": "enum", "default": "x",
                "values": ["x", "y"], "label_de": "K",
            }],
        }]
        self._patch_db(monkeypatch, rates)

        entry = self._make_entry([
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": {"energieversorger": "syn",
                        "user_inputs": {"k": "x"}}},
        ])
        assert _scan_history_for_drift(entry) == []

    def test_added_key_reported_as_missing(self, monkeypatch):
        rates = [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [
                {"key": "k", "type": "enum", "default": "x",
                 "values": ["x", "y"], "label_de": "K"},
                {"key": "new_opt", "type": "boolean", "default": False,
                 "label_de": "New"},
            ],
        }]
        self._patch_db(monkeypatch, rates)

        entry = self._make_entry([
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": {"energieversorger": "syn",
                        "user_inputs": {"k": "x"}}},
        ])
        descs = _scan_history_for_drift(entry)
        assert len(descs) == 1
        assert descs[0]["missing_keys"] == ["new_opt"]
        assert descs[0]["stale_values"] == []
        assert descs[0]["utility"] == "syn"

    def test_stale_value_reported(self, monkeypatch):
        rates = [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [{
                "key": "k", "type": "enum", "default": "new1",
                "values": ["new1", "new2"], "label_de": "K",
            }],
        }]
        self._patch_db(monkeypatch, rates)

        entry = self._make_entry([
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": {"energieversorger": "syn",
                        "user_inputs": {"k": "old"}}},
        ])
        descs = _scan_history_for_drift(entry)
        assert len(descs) == 1
        assert descs[0]["missing_keys"] == []
        assert descs[0]["stale_values"] == ["k"]

    def test_removed_key_does_not_fire(self, monkeypatch):
        # Stored has key "k"; current rate window declares NO user_inputs.
        # Stored value becomes inert noise; no repair issue should fire.
        rates = [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [],
        }]
        self._patch_db(monkeypatch, rates)

        entry = self._make_entry([
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": {"energieversorger": "syn",
                        "user_inputs": {"k": "x"}}},
        ])
        assert _scan_history_for_drift(entry) == []

    def test_pure_rate_change_does_not_fire(self, monkeypatch):
        # Same user_inputs decls across both windows; only fixed_rp_kwh differs.
        common_decl = [{
            "key": "k", "type": "enum", "default": "x",
            "values": ["x", "y"], "label_de": "K",
        }]
        rates = [
            {
                "valid_from": "2026-01-01", "valid_to": "2027-01-01",
                "settlement_period": "quartal",
                "power_tiers": [],
                "user_inputs": common_decl,
            },
            {
                "valid_from": "2027-01-01", "valid_to": None,
                "settlement_period": "quartal",
                "power_tiers": [],
                "user_inputs": common_decl,
            },
        ]
        self._patch_db(monkeypatch, rates)

        entry = self._make_entry([
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": {"energieversorger": "syn",
                        "user_inputs": {"k": "x"}}},
        ])
        assert _scan_history_for_drift(entry) == []

    def test_sentinel_record_skipped(self, monkeypatch):
        # 1970-01-01 sentinel records are skipped (they have None values).
        rates = [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [{
                "key": "k", "type": "enum", "default": "x",
                "values": ["x", "y"], "label_de": "K",
            }],
        }]
        self._patch_db(monkeypatch, rates)

        entry = self._make_entry([
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": {"energieversorger": None, "user_inputs": None}},
        ])
        assert _scan_history_for_drift(entry) == []

    def test_empty_history_returns_empty(self):
        entry = self._make_entry([])
        assert _scan_history_for_drift(entry) == []


class TestTariffDriftRepairFlow:
    """Repair flow that resolves a drift issue by appending a new history
    entry at period_from with the user's picks."""

    def _patch_db(self, monkeypatch, rates):
        from custom_components.bfe_rueckliefertarif import config_flow as cf
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2024-01-01",
                "valid_to": None,
                "rules": [{"kw_min": 0, "kw_max": None,
                           "self_consumption": None, "min_rp_kwh": 4.0}],
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

    @pytest.mark.asyncio
    async def test_save_appends_entry_with_patched_user_inputs(self, monkeypatch):
        from unittest.mock import MagicMock

        from custom_components.bfe_rueckliefertarif.const import (
            CONF_ENERGIEVERSORGER,
            CONF_USER_INPUTS,
            OPT_CONFIG_HISTORY,
        )

        # Synthetic rate window: declares a renamed key the stored entry doesn't have.
        rates = [{
            "valid_from": "2027-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [{
                "key": "renamed", "type": "enum", "default": "fix",
                "values": ["fix", "rmp"], "label_de": "K",
            }],
        }]
        self._patch_db(monkeypatch, rates)

        # Pre-existing history with a 2026 record carrying the OLD key.
        existing_history = [
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": {
                 CONF_ENERGIEVERSORGER: "syn",
                 "installierte_leistung_kwp": 10.0,
                 "eigenverbrauch_aktiviert": True,
                 "hkn_aktiviert": False,
                 "abrechnungs_rhythmus": "quartal",
                 CONF_USER_INPUTS: {"old_key": "fix"},
             }},
        ]
        entry = SimpleNamespace(
            entry_id="entry_42",
            data={"stromnetzeinspeisung_kwh": "sensor.foo"},
            options={OPT_CONFIG_HISTORY: existing_history},
        )

        descriptor = {
            "entry_id": "entry_42",
            "entry_idx": 0,
            "utility": "syn",
            "period_from": "2027-01-01",
            "period_to": None,
            "missing_keys": ["renamed"],
            "stale_values": [],
        }

        flow = TariffDriftRepairFlow(descriptor)
        flow.hass = MagicMock()
        flow.hass.config.language = "en"
        flow.hass.config_entries.async_get_entry.return_value = entry

        # Submit the form with the user's new pick.
        result = await flow.async_step_init({"renamed": "rmp"})

        # Flow completes via async_create_entry.
        assert result["type"].name in ("CREATE_ENTRY", "create_entry")
        # async_update_entry was called with new options containing a
        # new record at 2027-01-01 with renamed=rmp.
        flow.hass.config_entries.async_update_entry.assert_called_once()
        new_options = flow.hass.config_entries.async_update_entry.call_args.kwargs[
            "options"
        ]
        history = new_options[OPT_CONFIG_HISTORY]
        new_rec = next(r for r in history if r["valid_from"] == "2027-01-01")
        assert new_rec["config"][CONF_USER_INPUTS] == {
            "old_key": "fix",  # preserved from base
            "renamed": "rmp",  # newly picked
        }

    @pytest.mark.asyncio
    async def test_aborts_when_no_rate_window(self, monkeypatch):
        from unittest.mock import MagicMock

        # No rate window for the period we're trying to fix.
        self._patch_db(monkeypatch, [])

        descriptor = {
            "entry_id": "entry_42",
            "entry_idx": 0,
            "utility": "syn",
            "period_from": "2027-01-01",
            "period_to": None,
            "missing_keys": ["k"],
            "stale_values": [],
        }
        flow = TariffDriftRepairFlow(descriptor)
        flow.hass = MagicMock()

        result = await flow.async_step_init()
        assert result["type"].name in ("ABORT", "abort")
        assert result["reason"] == "no_rate_window"


class TestRemoteSchemaFetch:
    """Schema is fetched alongside tariffs.json. Schema-fetch failure is
    non-fatal: validation falls back to the bundled schema."""

    @pytest.fixture
    def minimal_tariffs(self):
        """Minimal v1.4.x-shape tariffs payload that validates against the
        canonical schema. Includes data_version + last_updated for the
        version-tracking assertions."""
        return {
            "schema_version": "1.4.0",
            "data_version": "0.0.2",
            "last_updated": "2026-05-01",
            "federal_minimum": [{
                "valid_from": "2026-01-01", "valid_to": None,
                "rules": [{
                    "kw_min": 0, "kw_max": 30,
                    "self_consumption": None, "min_rp_kwh": 6.0,
                }],
            }],
            "utilities": {"test": {
                "name_de": "Test",
                "homepage": "https://example.com",
                "rates": [{
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "power_tiers": [{
                        "kw_min": 0, "kw_max": None,
                        "base_model": "rmp_quartal",
                        "hkn_rp_kwh": 0.0,
                        "hkn_structure": "none",
                    }],
                }],
            }},
        }

    @pytest.fixture
    def canonical_schema(self):
        from pathlib import Path
        schema_path = (
            Path(__file__).parent.parent
            / "custom_components" / "bfe_rueckliefertarif"
            / "schemas" / "tariffs-v1.schema.json"
        )
        with open(schema_path, encoding="utf-8") as f:
            return json.load(f)

    def _make_coordinator(self, tmp_path):
        """Build a TariffsDataCoordinator with a minimal stub HA-side."""
        async def _exec(func, *args, **kwargs):
            return func(*args, **kwargs)

        config = SimpleNamespace(path=lambda *a: str(tmp_path))
        config_entries = SimpleNamespace(async_entries=lambda _domain: [])
        hass = SimpleNamespace(
            config=config,
            config_entries=config_entries,
            async_add_executor_job=_exec,
            data={},
        )
        return TariffsDataCoordinator(hass)

    @pytest.mark.asyncio
    async def test_fetch_schema_success_uses_remote_for_validation(
        self, tmp_path, canonical_schema, minimal_tariffs
    ):
        coord = self._make_coordinator(tmp_path)
        with aioresponses() as m:
            m.get(REMOTE_SCHEMA_URL, payload=canonical_schema)
            m.get(REMOTE_URL, payload=minimal_tariffs)
            ok = await coord.async_refresh()

        assert ok is True
        assert coord.last_schema_source == "remote"
        assert coord.last_schema_error is None
        assert coord._schema_cache_path.is_file()
        assert coord.last_data_version == "0.0.2"
        assert coord.last_data_updated == "2026-05-01"

    @pytest.mark.parametrize(
        "schema_mock_kwargs",
        [
            # HTTP 500 from the remote.
            {"status": 500},
            # Malformed JSON body.
            {"body": "<html>not json</html>", "content_type": "text/html"},
            # Valid JSON but not a valid Draft 2020-12 schema.
            {"payload": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "nonexistent",
            }},
        ],
        ids=["http_error", "malformed_json", "fails_meta_schema"],
    )
    @pytest.mark.asyncio
    async def test_fetch_schema_failure_falls_back_to_bundled(
        self, tmp_path, minimal_tariffs, schema_mock_kwargs
    ):
        coord = self._make_coordinator(tmp_path)
        with aioresponses() as m:
            m.get(REMOTE_SCHEMA_URL, **schema_mock_kwargs)
            m.get(REMOTE_URL, payload=minimal_tariffs)
            ok = await coord.async_refresh()

        assert ok is True
        assert coord.last_schema_source == "bundled"
        assert coord.last_schema_error is not None
        assert coord.last_error is None

    @pytest.mark.asyncio
    async def test_data_version_and_last_updated_recorded_on_refresh(
        self, tmp_path, canonical_schema, minimal_tariffs
    ):
        coord = self._make_coordinator(tmp_path)
        with aioresponses() as m:
            m.get(REMOTE_SCHEMA_URL, payload=canonical_schema)
            m.get(REMOTE_URL, payload=minimal_tariffs)
            await coord.async_refresh()

        # Meta file persists the version markers so async_load() can
        # populate state on cache-fresh restart without re-parsing tariffs.
        with open(coord._meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        assert meta["data_version"] == "0.0.2"
        assert meta["last_updated"] == "2026-05-01"

    @pytest.mark.asyncio
    async def test_load_uses_meta_data_version_on_fresh_cache(
        self, tmp_path, minimal_tariffs
    ):
        from datetime import UTC, datetime

        coord = self._make_coordinator(tmp_path)
        # Pre-create cache + meta as if a recent refresh succeeded.
        coord._cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(coord._cache_path, "w", encoding="utf-8") as f:
            json.dump(minimal_tariffs, f)
        with open(coord._meta_path, "w", encoding="utf-8") as f:
            json.dump({
                "fetched_at": datetime.now(UTC).isoformat(),
                "data_version": "0.0.2",
                "last_updated": "2026-05-01",
            }, f)

        await coord.async_load()
        # Cache fresh → no aiohttp call needed; state populated from meta.
        assert coord.last_data_version == "0.0.2"
        assert coord.last_data_updated == "2026-05-01"

    @pytest.mark.asyncio
    async def test_remote_schema_fail_then_data_invalid_under_bundled_returns_false(
        self, tmp_path
    ):
        """Schema fetch fails AND tariffs.json violates the bundled schema
        → async_refresh returns False, falls back to bundled tariffs."""
        # Tariffs payload that violates v1.5.0: rate is missing the
        # required `power_tiers` array (schema 1.5.0 dropped `cap_mode`,
        # but `power_tiers` remains required).
        bad_tariffs = {
            "schema_version": "1.5.0",
            "federal_minimum": [{
                "valid_from": "2026-01-01", "valid_to": None,
                "rules": [{"kw_min": 0, "kw_max": 30,
                           "self_consumption": None, "min_rp_kwh": 6.0}],
            }],
            "utilities": {"test": {
                "name_de": "Test",
                "homepage": "https://example.com",
                "rates": [{
                    # Missing required `power_tiers`.
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                }],
            }},
        }

        coord = self._make_coordinator(tmp_path)
        with aioresponses() as m:
            m.get(REMOTE_SCHEMA_URL, status=500)
            m.get(REMOTE_URL, payload=bad_tariffs)
            ok = await coord.async_refresh()

        assert ok is False
        assert coord.last_error is not None
        assert coord.last_schema_source == "bundled"
        assert get_source() == "bundled"
