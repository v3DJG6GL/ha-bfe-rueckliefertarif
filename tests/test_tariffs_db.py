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


class TestSeasonalConsistency:
    """Cross-validate the seasonal block against each tier's base_model.

    JSON Schema can't express "if the rate has a seasonal block AND a
    tier inside it has base_model=fixed_ht_nt then the seasonal block
    must contain summer_ht/summer_nt/winter_ht/winter_nt" without hairy
    path traversal — so we enforce it here in Python instead. Today this
    loops over zero records (all utilities have ``seasonal: null``), but
    it locks the contract for the moment a VESE import populates one.
    """

    ALL_MONTHS = set(range(1, 13))

    def test_summer_winter_months_disjoint_and_complete(self, db):
        for ukey, u in db["utilities"].items():
            for rate in u["rates"]:
                seasonal = rate.get("seasonal")
                if seasonal is None:
                    continue
                summer = set(seasonal["summer_months"])
                winter = set(seasonal["winter_months"])
                location = f"{ukey}@{rate['valid_from']}"
                assert summer & winter == set(), (
                    f"{location}: summer_months ∩ winter_months must be empty "
                    f"(overlap: {sorted(summer & winter)})"
                )
                assert summer | winter == self.ALL_MONTHS, (
                    f"{location}: summer_months ∪ winter_months must cover all "
                    f"12 months (missing: {sorted(self.ALL_MONTHS - (summer | winter))})"
                )

    def test_seasonal_required_keys_match_tier_base_model(self, db):
        for ukey, u in db["utilities"].items():
            for rate in u["rates"]:
                seasonal = rate.get("seasonal")
                if seasonal is None:
                    continue
                location = f"{ukey}@{rate['valid_from']}"
                for tier in rate["power_tiers"]:
                    bm = tier["base_model"]
                    if bm == "fixed_flat":
                        for k in ("summer_rp_kwh", "winter_rp_kwh"):
                            assert k in seasonal, (
                                f"{location} tier kw_min={tier['kw_min']}: "
                                f"fixed_flat × seasonal requires {k}"
                            )
                    elif bm == "fixed_ht_nt":
                        for k in (
                            "summer_ht_rp_kwh",
                            "summer_nt_rp_kwh",
                            "winter_ht_rp_kwh",
                            "winter_nt_rp_kwh",
                        ):
                            assert k in seasonal, (
                                f"{location} tier kw_min={tier['kw_min']}: "
                                f"fixed_ht_nt × seasonal requires {k}"
                            )
                    elif bm.startswith("rmp_"):
                        # rmp_* × seasonal is unsupported; resolve_tariff_at
                        # raises, but the schema shouldn't even ship it.
                        raise AssertionError(
                            f"{location} tier kw_min={tier['kw_min']}: "
                            f"rmp_* × seasonal is not supported"
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
        # v0.9.9 — requires_naturemade_star dropped; replaced by a notes[]
        # entry warning users that the fixed-price tariff is conditional on
        # naturemade-star certification.
        assert rt.notes is not None and len(rt.notes) >= 1
        assert rt.notes[0]["severity"] == "warning"
        assert "naturemade" in rt.notes[0]["text"]["de"].lower()
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


def _synthetic_db(utility_key: str, rates: list[dict]) -> dict:
    return {
        "schema_version": "1.0.0",
        "last_updated": "2026-01-01",
        "federal_minimum": [
            {
                "valid_from": "2026-01-01",
                "valid_to": None,
                "rules": [
                    {"kw_min": 0, "kw_max": 30, "self_consumption": None, "min_rp_kwh": 6.0},
                    {"kw_min": 30, "kw_max": 150, "self_consumption": True,
                     "formula": "180/kw",
                     "min_rp_kwh_at_kw_min": 6.0, "min_rp_kwh_at_kw_max": 1.2},
                    {"kw_min": 30, "kw_max": 150, "self_consumption": False, "min_rp_kwh": 6.2},
                    {"kw_min": 150, "kw_max": None, "self_consumption": None, "min_rp_kwh": None},
                ],
            }
        ],
        "utilities": {utility_key: {"name_de": utility_key, "rates": rates}},
    }


class TestResolveSettlementPeriodStunde:
    """#6b Phase 1 — refuse hourly Day-Ahead until Vernehmlassung 2025/59 lands."""

    def test_resolve_raises_on_settlement_period_stunde(self):
        db = _synthetic_db(
            "futurix",
            [
                {
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "stunde",
                    "power_tiers": [
                        {"kw_min": 0, "kw_max": None, "base_model": "rmp_quartal",
                         "hkn_rp_kwh": 2.0, "hkn_structure": "additive_optin"}
                    ],
                    "cap_mode": False, "cap_rules": None,
                }
            ],
        )
        with pytest.raises(NotImplementedError) as exc:
            resolve_tariff_at(
                "futurix", date(2026, 4, 1), kw=10.0,
                eigenverbrauch=True, data=db,
            )
        msg = str(exc.value)
        assert "stunde" in msg
        assert "futurix" in msg


class TestPerTierBaseModelVariation:
    """#14 — endigo case: kW threshold drives base_model selection."""

    @pytest.fixture
    def endigo_db(self):
        return _synthetic_db(
            "endigo_2026",
            [
                {
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "power_tiers": [
                        {"kw_min": 0, "kw_max": 150, "base_model": "fixed_flat",
                         "fixed_rp_kwh": 9.5, "hkn_rp_kwh": 2.0,
                         "hkn_structure": "additive_optin"},
                        {"kw_min": 150, "kw_max": None, "base_model": "rmp_quartal",
                         "hkn_rp_kwh": 2.0, "hkn_structure": "additive_optin"},
                    ],
                    "cap_mode": False, "cap_rules": None,
                }
            ],
        )

    def test_small_plant_uses_fixed_flat(self, endigo_db):
        rt = resolve_tariff_at(
            "endigo_2026", date(2026, 6, 1), kw=50.0,
            eigenverbrauch=True, data=endigo_db,
        )
        assert rt.base_model == "fixed_flat"
        assert rt.fixed_rp_kwh == 9.5

    def test_large_plant_uses_rmp_quartal(self, endigo_db):
        rt = resolve_tariff_at(
            "endigo_2026", date(2026, 6, 1), kw=200.0,
            eigenverbrauch=True, data=endigo_db,
        )
        assert rt.base_model == "rmp_quartal"
        assert rt.fixed_rp_kwh is None

    def test_threshold_belongs_to_upper_tier(self, endigo_db):
        # 150 kW boundary → upper tier (kw_max exclusive on lower).
        rt = resolve_tariff_at(
            "endigo_2026", date(2026, 6, 1), kw=150.0,
            eigenverbrauch=True, data=endigo_db,
        )
        assert rt.base_model == "rmp_quartal"


class TestRateWindowNotes:
    """v0.9.9 #5 — rate-window notes loaded into ResolvedTariff with date filtering."""

    def test_naturemade_field_dropped_from_resolved(self, db):
        rt = resolve_tariff_at(
            "ekz", date(2026, 4, 1), kw=10.0, eigenverbrauch=True, data=db
        )
        assert not hasattr(rt, "requires_naturemade_star")

    def test_notes_loaded_with_locale_text_dict(self):
        db = _synthetic_db(
            "noterix",
            [
                {
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "power_tiers": [
                        {"kw_min": 0, "kw_max": None, "base_model": "fixed_flat",
                         "fixed_rp_kwh": 9.0, "hkn_rp_kwh": 2.0,
                         "hkn_structure": "additive_optin"}
                    ],
                    "cap_mode": False, "cap_rules": None,
                    "notes": [
                        {
                            "severity": "warning",
                            "text": {
                                "de": "Nur mit Zertifikat.",
                                "en": "Certificate required.",
                                "fr": "Certificat requis.",
                            },
                        }
                    ],
                }
            ],
        )
        rt = resolve_tariff_at(
            "noterix", date(2026, 4, 1), kw=10.0, eigenverbrauch=True, data=db
        )
        assert rt.notes is not None
        assert len(rt.notes) == 1
        n = rt.notes[0]
        assert n["severity"] == "warning"
        assert n["text"]["de"].startswith("Nur mit Zertifikat")
        assert n["text"]["en"].startswith("Certificate required")
        assert n["text"]["fr"].startswith("Certificat requis")

    def test_notes_filtered_by_at_date(self):
        db = _synthetic_db(
            "noterix",
            [
                {
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "power_tiers": [
                        {"kw_min": 0, "kw_max": None, "base_model": "fixed_flat",
                         "fixed_rp_kwh": 9.0, "hkn_rp_kwh": 0.0,
                         "hkn_structure": "none"}
                    ],
                    "cap_mode": False, "cap_rules": None,
                    "notes": [
                        {
                            "valid_from": "2026-01-01", "valid_to": "2026-04-01",
                            "severity": "info",
                            "text": {"de": "Q1-Hinweis"},
                        },
                        {
                            "valid_from": "2026-04-01", "valid_to": "2026-07-01",
                            "severity": "info",
                            "text": {"de": "Q2-Hinweis"},
                        },
                    ],
                }
            ],
        )
        # March → first note only.
        rt_q1 = resolve_tariff_at(
            "noterix", date(2026, 3, 15), kw=5.0,
            eigenverbrauch=True, data=db,
        )
        assert rt_q1.notes is not None and len(rt_q1.notes) == 1
        assert rt_q1.notes[0]["text"]["de"] == "Q1-Hinweis"
        # April → second note only.
        rt_q2 = resolve_tariff_at(
            "noterix", date(2026, 4, 15), kw=5.0,
            eigenverbrauch=True, data=db,
        )
        assert rt_q2.notes is not None and len(rt_q2.notes) == 1
        assert rt_q2.notes[0]["text"]["de"] == "Q2-Hinweis"
        # August → outside both windows.
        rt_q3 = resolve_tariff_at(
            "noterix", date(2026, 8, 1), kw=5.0,
            eigenverbrauch=True, data=db,
        )
        assert rt_q3.notes is not None and len(rt_q3.notes) == 0

    def test_no_notes_field_yields_none(self):
        db = _synthetic_db(
            "noterix",
            [
                {
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "power_tiers": [
                        {"kw_min": 0, "kw_max": None, "base_model": "fixed_flat",
                         "fixed_rp_kwh": 9.0, "hkn_rp_kwh": 0.0,
                         "hkn_structure": "none"}
                    ],
                    "cap_mode": False, "cap_rules": None,
                }
            ],
        )
        rt = resolve_tariff_at(
            "noterix", date(2026, 4, 1), kw=5.0,
            eigenverbrauch=True, data=db,
        )
        assert rt.notes is None


class TestRateWindowBonuses:
    """v0.10.0 #3 Phase 1 (Batch C) — rate-window bonuses loaded into
    ``ResolvedTariff`` for display-only rendering. No conditional
    evaluation yet (that's Batch D)."""

    @staticmethod
    def _bonus_rate(rate_rp_kwh: float, applies_when: str = "always", **extra) -> dict:
        b = {
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [
                {"kw_min": 0, "kw_max": None, "base_model": "fixed_flat",
                 "fixed_rp_kwh": 9.0, "hkn_rp_kwh": 2.0,
                 "hkn_structure": "additive_optin"}
            ],
            "cap_mode": False, "cap_rules": None,
            "bonuses": [
                {"name": "Eco", "rate_rp_kwh": rate_rp_kwh,
                 "applies_when": applies_when, **extra}
            ],
        }
        return b

    def test_bonuses_loaded_when_present(self):
        db = _synthetic_db("bonusy", [self._bonus_rate(1.5)])
        rt = resolve_tariff_at(
            "bonusy", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db
        )
        assert rt.bonuses is not None
        assert len(rt.bonuses) == 1
        b = rt.bonuses[0]
        assert b["name"] == "Eco"
        assert b["rate_rp_kwh"] == 1.5
        assert b["applies_when"] == "always"

    def test_bonuses_none_when_key_absent(self):
        rate = self._bonus_rate(1.5)
        del rate["bonuses"]
        db = _synthetic_db("bonusy", [rate])
        rt = resolve_tariff_at(
            "bonusy", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db
        )
        assert rt.bonuses is None

    def test_bonuses_none_when_empty_list(self):
        rate = self._bonus_rate(1.5)
        rate["bonuses"] = []
        db = _synthetic_db("bonusy", [rate])
        rt = resolve_tariff_at(
            "bonusy", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db
        )
        assert rt.bonuses is None

    def test_bonuses_pass_through_unknown_keys(self):
        # Phase-2 forward-compat: schema declares additionalProperties=true
        # so future keys (kind, when) must round-trip through the loader
        # untouched.
        rate = self._bonus_rate(1.5)
        rate["bonuses"][0]["kind"] = "additive_rp_kwh"
        rate["bonuses"][0]["when"] = {"season": "winter"}
        db = _synthetic_db("bonusy", [rate])
        rt = resolve_tariff_at(
            "bonusy", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db
        )
        assert rt.bonuses is not None
        assert rt.bonuses[0]["kind"] == "additive_rp_kwh"
        assert rt.bonuses[0]["when"] == {"season": "winter"}
