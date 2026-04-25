"""Tests for Phase 6: data_coordinator.py.

Covers the bits that don't need a running aiohttp loop — the validation
fallback path, override path resolution in tariffs_db, and that
``set_override_path`` round-trips correctly.

Network fetch is exercised in the live HA-side integration test (see
plan verification scenarios 9 & 10).
"""

from __future__ import annotations

import json

import pytest

from custom_components.bfe_rueckliefertarif.data_coordinator import TariffsDataCoordinator
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
                    "cap_mode": False,
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

    def test_rejects_missing_schema_version(self):
        with pytest.raises(ValueError, match="schema_version"):
            TariffsDataCoordinator._loose_validate({
                "federal_minimum": [{}],
                "utilities": {"x": {}},
            })

    def test_rejects_empty_utilities(self):
        with pytest.raises(ValueError, match="utilities"):
            TariffsDataCoordinator._loose_validate({
                "schema_version": "1.0.0",
                "federal_minimum": [{}],
                "utilities": {},
            })

    def test_rejects_empty_federal_minimum(self):
        with pytest.raises(ValueError, match="federal_minimum"):
            TariffsDataCoordinator._loose_validate({
                "schema_version": "1.0.0",
                "federal_minimum": [],
                "utilities": {"x": {}},
            })


class TestBundledStillWorks:
    """Sanity: bundled tariffs.json must always be loadable."""

    def test_bundled_path_exists(self):
        assert _BUNDLED_DATA_PATH.is_file()

    def test_bundled_loads_with_no_override(self):
        loaded = load_tariffs()
        assert "ekz" in loaded["utilities"]
