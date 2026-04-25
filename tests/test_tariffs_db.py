"""Tests for tariffs_db.py — JSON validates against schema, lookups behave."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import jsonschema
import pytest

from custom_components.bfe_rueckliefertarif.tariffs_db import (
    _BUNDLED_DATA_PATH,
    ResolvedTariff,
    evaluate_federal_floor,
    find_active,
    find_rule,
    find_tier,
    floor_label,
    list_utility_keys,
    load_tariffs,
    resolve_tariff_at,
)

_SCHEMA_PATH = (
    Path(__file__).parent.parent
    / "custom_components"
    / "bfe_rueckliefertarif"
    / "schemas"
    / "tariffs-v1.schema.json"
)


@pytest.fixture(scope="module")
def schema():
    with open(_SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def db():
    return load_tariffs()


class TestSchemaConformance:
    def test_bundled_json_validates(self, db, schema):
        jsonschema.Draft202012Validator(schema).validate(db)

    def test_schema_itself_is_valid_jsonschema(self, schema):
        jsonschema.Draft202012Validator.check_schema(schema)

    def test_bundled_json_path_exists(self):
        assert _BUNDLED_DATA_PATH.is_file()

    def test_all_utilities_listed(self, db):
        keys = list_utility_keys(db)
        # Sanity: 13 entries today (12 utilities; AEW splits into _fixpreis + _rmp).
        assert len(keys) >= 13
        assert "ekz" in keys
        assert "aew_fixpreis" in keys
        assert "aew_rmp" in keys
        assert "aew" not in keys  # the unsplit key must be gone

    def test_cap_mode_true_implies_cap_rules(self, db):
        for ukey, u in db["utilities"].items():
            for rate in u["rates"]:
                if rate["cap_mode"]:
                    assert rate["cap_rules"], (
                        f"{ukey}@{rate['valid_from']}: cap_mode=True but cap_rules empty"
                    )


class TestFindActive:
    def _records(self):
        return [
            {"valid_from": "2024-01-01", "valid_to": "2025-01-01", "name": "old"},
            {"valid_from": "2025-01-01", "valid_to": "2026-01-01", "name": "mid"},
            {"valid_from": "2026-01-01", "valid_to": None,         "name": "now"},
        ]

    def test_inside_window(self):
        assert find_active(self._records(), date(2025, 6, 15))["name"] == "mid"

    def test_boundary_belongs_to_new_record(self):
        # 2026-01-01 == valid_from of "now" — half-open [from, to) means
        # the boundary day belongs to the *new* record.
        assert find_active(self._records(), date(2026, 1, 1))["name"] == "now"

    def test_open_ended_current(self):
        assert find_active(self._records(), date(2030, 1, 1))["name"] == "now"

    def test_before_first_returns_none(self):
        assert find_active(self._records(), date(2023, 1, 1)) is None

    def test_empty_list(self):
        assert find_active([], date(2026, 1, 1)) is None


class TestFindRule:
    def _rules(self):
        return [
            {"kw_min": 0,   "kw_max": 30,   "self_consumption": None},
            {"kw_min": 30,  "kw_max": 150,  "self_consumption": True},
            {"kw_min": 30,  "kw_max": 150,  "self_consumption": False},
            {"kw_min": 150, "kw_max": None, "self_consumption": None},
        ]

    def test_small_matches_regardless_of_ev(self):
        assert find_rule(self._rules(), 10, True)["kw_min"] == 0
        assert find_rule(self._rules(), 10, False)["kw_min"] == 0

    def test_mid_with_ev(self):
        r = find_rule(self._rules(), 50, True)
        assert r["kw_min"] == 30 and r["self_consumption"] is True

    def test_mid_without_ev(self):
        r = find_rule(self._rules(), 50, False)
        assert r["kw_min"] == 30 and r["self_consumption"] is False

    def test_xl_matches_open_ended(self):
        assert find_rule(self._rules(), 5000, True)["kw_min"] == 150
        assert find_rule(self._rules(), 5000, False)["kw_min"] == 150

    def test_kw_max_is_exclusive(self):
        # 30 kW belongs to the 30–<150 band, not the <30 band.
        assert find_rule(self._rules(), 30, True)["kw_min"] == 30


class TestFindTier:
    def test_basic(self):
        tiers = [
            {"kw_min": 0,  "kw_max": 30,   "base_model": "fixed_flat"},
            {"kw_min": 30, "kw_max": None, "base_model": "rmp_quartal"},
        ]
        assert find_tier(tiers, 10)["base_model"] == "fixed_flat"
        assert find_tier(tiers, 100)["base_model"] == "rmp_quartal"
        # Boundary (30) belongs to the upper tier (kw_max exclusive).
        assert find_tier(tiers, 30)["base_model"] == "rmp_quartal"


class TestFederalFloor:
    def test_flat_rule(self):
        rule = {"kw_min": 0, "kw_max": 30, "self_consumption": None, "min_rp_kwh": 6.0}
        assert evaluate_federal_floor(rule, 10) == 6.0

    def test_degressive_formula(self):
        rule = {
            "kw_min": 30, "kw_max": 150, "self_consumption": True,
            "formula": "180/kw",
            "min_rp_kwh_at_kw_min": 6.0, "min_rp_kwh_at_kw_max": 1.2,
        }
        assert evaluate_federal_floor(rule, 30) == 6.0
        assert evaluate_federal_floor(rule, 150) == 1.2
        assert evaluate_federal_floor(rule, 35) == round(180.0 / 35.0, 4)

    def test_no_floor_returns_none(self):
        rule = {"kw_min": 150, "kw_max": None, "self_consumption": None, "min_rp_kwh": None}
        assert evaluate_federal_floor(rule, 500) is None

    def test_degressive_zero_kw_raises(self):
        rule = {
            "kw_min": 30, "kw_max": 150, "self_consumption": True,
            "formula": "180/kw",
        }
        with pytest.raises(ValueError):
            evaluate_federal_floor(rule, 0)


class TestFloorLabel:
    def test_small(self):
        assert floor_label({"kw_min": 0, "kw_max": 30, "self_consumption": None}) == "<30 kW"

    def test_mid_mit_ev(self):
        assert floor_label(
            {"kw_min": 30, "kw_max": 150, "self_consumption": True}
        ) == "30–<150 kW mit Eigenverbrauch"

    def test_mid_ohne_ev(self):
        assert floor_label(
            {"kw_min": 30, "kw_max": 150, "self_consumption": False}
        ) == "30–<150 kW ohne Eigenverbrauch"

    def test_xl(self):
        assert floor_label({"kw_min": 150, "kw_max": None, "self_consumption": None}) == "≥150 kW"


class TestResolveTariffAt:
    def test_ekz_small_with_ev(self, db):
        rt = resolve_tariff_at(
            "ekz", date(2026, 4, 1), kw=25.0, eigenverbrauch=True, data=db
        )
        assert isinstance(rt, ResolvedTariff)
        assert rt.utility_key == "ekz"
        assert rt.base_model == "rmp_quartal"
        assert rt.hkn_rp_kwh == 3.00
        assert rt.cap_mode is True
        assert rt.cap_rp_kwh == 10.96
        assert rt.federal_floor_rp_kwh == 6.00
        assert rt.federal_floor_label == "<30 kW"
        assert rt.tariffs_json_source == "bundled"

    def test_ekz_mid_with_ev_uses_degressive_formula(self, db):
        rt = resolve_tariff_at(
            "ekz", date(2026, 4, 1), kw=35.0, eigenverbrauch=True, data=db
        )
        assert rt.federal_floor_rp_kwh == round(180.0 / 35.0, 4)
        assert rt.federal_floor_label == "30–<150 kW mit Eigenverbrauch"
        # cap stays at the small-band 10.96 (cap_rules use kw≤100 = 10.96 with EV).
        assert rt.cap_rp_kwh == 10.96

    def test_ekz_large_without_ev(self, db):
        rt = resolve_tariff_at(
            "ekz", date(2026, 4, 1), kw=200.0, eigenverbrauch=False, data=db
        )
        # ≥150 kW: no federal floor.
        assert rt.federal_floor_rp_kwh is None
        assert rt.federal_floor_label == "≥150 kW"
        # cap_rules pin ≥100 kW + ohne EV → 5.40.
        assert rt.cap_rp_kwh == 5.40

    def test_aew_fixpreis(self, db):
        rt = resolve_tariff_at(
            "aew_fixpreis", date(2026, 4, 1), kw=10.0, eigenverbrauch=True, data=db
        )
        assert rt.base_model == "fixed_flat"
        assert rt.fixed_rp_kwh == 8.20
        assert rt.hkn_structure == "bundled"
        assert rt.requires_naturemade_star is True
        assert rt.cap_mode is False
        assert rt.cap_rp_kwh is None

    def test_aew_rmp_at_30kw_in_lower_tier(self, db):
        # AEW Fixpreis caps at kw_max=30 (exclusive); aew_rmp covers any size.
        rt = resolve_tariff_at(
            "aew_rmp", date(2026, 4, 1), kw=50.0, eigenverbrauch=True, data=db
        )
        assert rt.base_model == "rmp_quartal"
        assert rt.hkn_structure == "none"

    def test_aew_fixpreis_above_30kw_has_no_tier(self, db):
        # aew_fixpreis only covers 0–<30 kW; 30 kW or more → LookupError.
        with pytest.raises(LookupError):
            resolve_tariff_at(
                "aew_fixpreis", date(2026, 4, 1), kw=30.0, eigenverbrauch=True, data=db
            )

    def test_iwb_bundled_hkn(self, db):
        rt = resolve_tariff_at(
            "iwb", date(2026, 4, 1), kw=10.0, eigenverbrauch=True, data=db
        )
        assert rt.fixed_rp_kwh == 12.95
        # IWB pays 12.95 — above the 10.96 small-band ceiling — empirical proof
        # that the cap is a *cost-recovery ceiling*, not a payment cap on producers.
        assert rt.fixed_rp_kwh > 10.96
        assert rt.cap_mode is False

    def test_unknown_utility_raises(self, db):
        with pytest.raises(KeyError):
            resolve_tariff_at(
                "does_not_exist", date(2026, 4, 1), kw=10.0,
                eigenverbrauch=True, data=db,
            )

    def test_no_active_rate_raises_lookup_error(self, db):
        # Bundled rates start 2026-01-01; pre-2026 → LookupError.
        with pytest.raises(LookupError):
            resolve_tariff_at(
                "ekz", date(2025, 6, 1), kw=25.0,
                eigenverbrauch=True, data=db,
            )
