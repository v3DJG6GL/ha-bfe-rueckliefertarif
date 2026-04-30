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
    evaluate_when,
    find_active,
    find_active_rate_window,
    find_rule,
    find_tier,
    find_tier_for,
    floor_label,
    list_utility_keys,
    load_tariffs,
    match_applies_when,
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
        # v0.11.0 (Batch D) — 13+ entries; AEW unified into one key with
        # ``user_inputs.tariff_model`` enum (was: aew_fixpreis + aew_rmp).
        assert len(keys) >= 13
        assert "ekz" in keys
        assert "aew" in keys
        assert "aew_fixpreis" not in keys
        assert "aew_rmp" not in keys

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
                        # v0.11.0 (Batch D) — rmp_* with seasonal is allowed
                        # IF the seasonal block carries only summer_months /
                        # winter_months (classification for hkn_cases.when /
                        # bonuses.when). Rate-key overrides remain unsupported.
                        rate_keys = (
                            "summer_rp_kwh",
                            "winter_rp_kwh",
                            "summer_ht_rp_kwh",
                            "summer_nt_rp_kwh",
                            "winter_ht_rp_kwh",
                            "winter_nt_rp_kwh",
                        )
                        offending = [k for k in rate_keys if k in seasonal]
                        assert not offending, (
                            f"{location} tier kw_min={tier['kw_min']}: "
                            f"rmp_* × seasonal-with-rate-overrides is not "
                            f"supported (offending keys: {offending})"
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

    def test_aew_fixpreis_via_user_inputs(self, db):
        # AEW unified user_input gates fixed_flat vs rmp_quartal tiers.
        # v1.2.0 bundled data: key "aew_fixpreis_rmp" with prose values.
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=10.0, eigenverbrauch=True,
            user_inputs={"aew_fixpreis_rmp": "AEW Fixpreis"}, data=db,
        )
        assert rt.base_model == "fixed_flat"
        assert rt.fixed_rp_kwh == 8.20
        assert rt.hkn_structure == "bundled"
        assert rt.notes is not None and len(rt.notes) >= 1
        assert rt.notes[0]["severity"] == "info"
        assert "fix" in rt.notes[0]["text"]["de"].lower()
        assert rt.cap_mode is False
        assert rt.cap_rp_kwh is None
        # Tier's applies_when is captured for downstream introspection.
        assert rt.tier_applies_when == {"aew_fixpreis_rmp": "AEW Fixpreis"}

    def test_aew_rmp_via_user_inputs_at_50kw(self, db):
        # "Referenzmarktpreis" picks the RMP-quartal tier (kw_max=3000).
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=50.0, eigenverbrauch=True,
            user_inputs={"aew_fixpreis_rmp": "Referenzmarktpreis"}, data=db,
        )
        assert rt.base_model == "rmp_quartal"
        assert rt.hkn_structure == "none"

    def test_aew_at_30kw_requires_rmp_choice(self, db):
        # v1.2.0 bundled data: at 30 kW only the rmp_quartal tier covers
        # (kw_min=30..3000) and it's gated on "Referenzmarktpreis".
        # Without that user_input the resolver finds no covering tier.
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=30.0, eigenverbrauch=True,
            user_inputs={"aew_fixpreis_rmp": "Referenzmarktpreis"}, data=db,
        )
        assert rt.base_model == "rmp_quartal"
        assert rt.tier_applies_when == {"aew_fixpreis_rmp": "Referenzmarktpreis"}

    def test_aew_default_user_input_falls_back_to_declaration_default(self, db):
        # No user_inputs supplied → resolver defaults from decl.default
        # ("AEW Fixpreis"). Same outcome as test_aew_fixpreis_via_user_inputs.
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=10.0, eigenverbrauch=True, data=db,
        )
        assert rt.base_model == "fixed_flat"
        assert rt.tier_applies_when == {"aew_fixpreis_rmp": "AEW Fixpreis"}

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


class TestFindTierFor:
    """v0.11.0 (Batch D) — kW-band lookup with applies_when filter."""

    def _tiers(self):
        return [
            {
                "kw_min": 0, "kw_max": 30, "base_model": "fixed_flat",
                "applies_when": {"tariff_model": "fixpreis"},
                "fixed_rp_kwh": 8.0, "hkn_rp_kwh": 0.0, "hkn_structure": "bundled",
            },
            {
                "kw_min": 0, "kw_max": 30, "base_model": "rmp_quartal",
                "applies_when": {"tariff_model": "rmp"},
                "hkn_rp_kwh": 0.0, "hkn_structure": "none",
            },
            {
                "kw_min": 30, "kw_max": None, "base_model": "rmp_quartal",
                "hkn_rp_kwh": 0.0, "hkn_structure": "none",
            },
        ]

    def test_picks_applies_when_match_over_unconditional(self):
        tier = find_tier_for(self._tiers(), 10.0, {"tariff_model": "fixpreis"})
        assert tier is not None
        assert tier["base_model"] == "fixed_flat"

    def test_picks_other_applies_when_match(self):
        tier = find_tier_for(self._tiers(), 10.0, {"tariff_model": "rmp"})
        assert tier is not None
        assert tier["base_model"] == "rmp_quartal"
        assert tier["kw_max"] == 30

    def test_falls_back_to_unconditional_above_kw_band(self):
        # 50 kW: only the unconditional ≥30 tier covers; tariff_model
        # doesn't gate that band.
        tier = find_tier_for(self._tiers(), 50.0, {"tariff_model": "fixpreis"})
        assert tier is not None
        assert tier["kw_min"] == 30
        assert tier.get("applies_when") is None

    def test_unmatched_user_input_returns_none_when_no_fallback(self):
        # Tiers in 0–<30 band all have applies_when. With unknown
        # tariff_model AND kw inside the conditional band, no tier matches.
        tier = find_tier_for(self._tiers(), 10.0, {"tariff_model": "unknown"})
        assert tier is None


class TestEvaluateWhen:
    """v0.11.0 (Batch D) — strict when_clause vocabulary (season + user_inputs)."""

    def test_season_only_match(self):
        assert evaluate_when(
            {"season": "winter"}, season="winter", user_inputs={}
        ) is True

    def test_season_only_no_match(self):
        assert evaluate_when(
            {"season": "winter"}, season="summer", user_inputs={}
        ) is False

    def test_season_none_runtime_no_match(self):
        # Hour without seasonal classification cannot match a season clause.
        assert evaluate_when(
            {"season": "winter"}, season=None, user_inputs={}
        ) is False

    def test_user_inputs_only_match(self):
        assert evaluate_when(
            {"user_inputs": {"supply_product": True}},
            season=None, user_inputs={"supply_product": True},
        ) is True

    def test_user_inputs_only_no_match(self):
        assert evaluate_when(
            {"user_inputs": {"supply_product": True}},
            season=None, user_inputs={"supply_product": False},
        ) is False

    def test_user_inputs_missing_key_no_match(self):
        # Missing key in runtime ≠ expected value → no match.
        assert evaluate_when(
            {"user_inputs": {"supply_product": True}},
            season=None, user_inputs={},
        ) is False

    def test_combined_and_match(self):
        assert evaluate_when(
            {"season": "winter", "user_inputs": {"supply_product": True}},
            season="winter", user_inputs={"supply_product": True},
        ) is True

    def test_combined_and_partial_no_match(self):
        # Season matches but user_input doesn't.
        assert evaluate_when(
            {"season": "winter", "user_inputs": {"supply_product": True}},
            season="winter", user_inputs={"supply_product": False},
        ) is False

    def test_unknown_clause_key_raises(self):
        with pytest.raises(ValueError, match="unknown when_clause key"):
            evaluate_when(
                {"kwh_le": 1000}, season=None, user_inputs={}
            )


class TestResolveTariffAtBatchD:
    """v0.11.0 (Batch D) — user_inputs_decl + hkn_cases + tier_applies_when
    loaded into ResolvedTariff."""

    def _rate(self, *, with_decl=True, with_hkn_cases=True):
        rate = {
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "cap_mode": False,
            "power_tiers": [
                {
                    "kw_min": 0, "kw_max": None, "base_model": "fixed_flat",
                    "fixed_rp_kwh": 5.0,
                    "hkn_rp_kwh": 1.0, "hkn_structure": "additive_optin",
                }
            ],
        }
        if with_decl:
            rate["user_inputs"] = [
                {
                    "key": "supply_product", "type": "boolean",
                    "default": False, "label_de": "Ökostrom-Produkt",
                }
            ]
        if with_hkn_cases:
            rate["power_tiers"][0]["hkn_cases"] = [
                {"when": {"user_inputs": {"supply_product": True}}, "rp_kwh": 4.0},
            ]
        return rate

    def test_user_inputs_decl_loaded(self):
        db = _synthetic_db("syn", [self._rate()])
        rt = resolve_tariff_at(
            "syn", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db,
        )
        assert rt.user_inputs_decl is not None
        assert rt.user_inputs_decl[0]["key"] == "supply_product"
        assert rt.user_inputs_decl[0]["type"] == "boolean"

    def test_user_inputs_decl_none_when_absent(self):
        db = _synthetic_db("syn", [self._rate(with_decl=False)])
        rt = resolve_tariff_at(
            "syn", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db,
        )
        assert rt.user_inputs_decl is None

    def test_hkn_cases_loaded(self):
        db = _synthetic_db("syn", [self._rate()])
        rt = resolve_tariff_at(
            "syn", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db,
        )
        assert rt.hkn_cases is not None
        assert rt.hkn_cases[0]["rp_kwh"] == 4.0

    def test_hkn_cases_none_when_absent(self):
        db = _synthetic_db("syn", [self._rate(with_hkn_cases=False)])
        rt = resolve_tariff_at(
            "syn", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db,
        )
        assert rt.hkn_cases is None

    def test_default_user_input_applied_when_missing(self):
        # No user_inputs supplied at call time → resolver fills in
        # decl.default before tier filtering. Here the only tier has no
        # applies_when, so the choice doesn't gate anything — but the
        # resolver shouldn't crash.
        db = _synthetic_db("syn", [self._rate()])
        rt = resolve_tariff_at(
            "syn", date(2026, 4, 1), kw=5.0, eigenverbrauch=True, data=db,
        )
        assert rt is not None  # didn't raise
        assert rt.tier_applies_when is None  # tier has no clause


class TestMatchAppliesWhen:
    """v0.12.0 (schema v1.2.0) — clause matcher reused for tarif_urls."""

    def test_none_clause_matches(self):
        assert match_applies_when(None, {"x": 1}) is True

    def test_empty_clause_matches(self):
        assert match_applies_when({}, {"x": 1}) is True

    def test_none_user_inputs_treated_as_empty(self):
        assert match_applies_when({"x": 1}, None) is False

    def test_exact_scalar_match(self):
        assert match_applies_when({"x": "a"}, {"x": "a", "y": 2}) is True

    def test_missing_key_fails(self):
        assert match_applies_when({"x": 1}, {"y": 2}) is False

    def test_value_mismatch_fails(self):
        assert match_applies_when({"x": "a"}, {"x": "b"}) is False

    def test_multi_key_all_must_match(self):
        ui = {"a": 1, "b": 2, "c": 3}
        assert match_applies_when({"a": 1, "b": 2}, ui) is True
        assert match_applies_when({"a": 1, "b": 99}, ui) is False


class TestFindActiveRateWindow:
    """Bundled-data round-trip on the convenience accessor."""

    def test_known_utility_returns_record(self):
        rec = find_active_rate_window("ekz", date(2026, 6, 1))
        assert rec is not None
        assert rec["valid_from"] <= "2026-06-01"

    def test_unknown_utility_returns_none(self):
        assert find_active_rate_window("does_not_exist", date(2026, 6, 1)) is None

    def test_date_before_any_window_returns_none(self):
        # The bundled data starts in 2017 for some utilities, but pre-2010 is
        # outside every utility's coverage.
        assert find_active_rate_window("ekz", date(1999, 1, 1)) is None
