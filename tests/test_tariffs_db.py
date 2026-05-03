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
    compute_user_inputs_periods,
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
    user_inputs_decl_signature,
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

    def test_cap_rules_truthy_or_absent(self, db):
        """v0.22.0 — schema 1.5.0 dropped cap_mode. Cap activation is now
        signaled solely by a non-empty ``cap_rules`` array. Verify every
        rate either has a non-empty cap_rules array or omits the field
        entirely; ``cap_rules: []`` (empty) is also valid (= no cap)."""
        for ukey, u in db["utilities"].items():
            for rate in u["rates"]:
                cap_rules = rate.get("cap_rules")
                if cap_rules:
                    assert isinstance(cap_rules, list), (
                        f"{ukey}@{rate['valid_from']}: cap_rules must be a list"
                    )
                    for rule in cap_rules:
                        assert "cap_rp_kwh" in rule, (
                            f"{ukey}@{rate['valid_from']}: cap_rules entry missing cap_rp_kwh"
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
        # v0.22.0 — cap activation = `cap_rp_kwh` set (resolver derived
        # from a non-empty `cap_rules` array).
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
        # AEW unified user_input gates fixed_flat vs rmp_quartal vs
        # fixed_seasonal tiers. v1.6.0 bundled data: key "fixpreis_rmp"
        # with four enum values; the fixed_flat tier covers 2..30 kW
        # and carries an additive HKN bonus.
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=10.0, eigenverbrauch=True,
            user_inputs={"fixpreis_rmp": "fixpreis"}, data=db,
        )
        assert rt.base_model == "fixed_flat"
        assert rt.fixed_rp_kwh == 8.20
        assert rt.hkn_structure == "additive_optin"
        assert rt.notes is not None and len(rt.notes) >= 1
        assert rt.notes[0]["severity"] == "info"
        assert "fix" in rt.notes[0]["text"]["de"].lower()
        assert rt.cap_rp_kwh is None
        # Tier's applies_when is captured for downstream introspection.
        assert rt.tier_applies_when == {"fixpreis_rmp": "fixpreis"}

    def test_aew_rmp_via_user_inputs_at_50kw(self, db):
        # "rmp" picks the RMP-quartal tier (kw_max=3000).
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=50.0, eigenverbrauch=True,
            user_inputs={"fixpreis_rmp": "rmp"}, data=db,
        )
        assert rt.base_model == "rmp_quartal"
        assert rt.hkn_structure == "none"

    def test_aew_at_30kw_requires_rmp_choice(self, db):
        # v1.5.0 bundled data: at exactly 30 kW the fixed_flat tier
        # (kw_min=2, kw_max=30, half-open) no longer covers; only the
        # rmp_quartal tier (kw_min=2..3000) does. So the user must have
        # picked "rmp" to get a covering tier.
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=30.0, eigenverbrauch=True,
            user_inputs={"fixpreis_rmp": "rmp"}, data=db,
        )
        assert rt.base_model == "rmp_quartal"
        assert rt.tier_applies_when == {"fixpreis_rmp": "rmp"}

    def test_aew_default_user_input_falls_back_to_declaration_default(self, db):
        # No user_inputs supplied → resolver defaults from decl.default
        # ("fixpreis"). Same outcome as test_aew_fixpreis_via_user_inputs.
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=10.0, eigenverbrauch=True, data=db,
        )
        assert rt.base_model == "fixed_flat"
        assert rt.tier_applies_when == {"fixpreis_rmp": "fixpreis"}

    def test_iwb_bundled_hkn(self, db):
        rt = resolve_tariff_at(
            "iwb", date(2026, 4, 1), kw=10.0, eigenverbrauch=True, data=db
        )
        assert rt.fixed_rp_kwh == 12.95
        # IWB pays 12.95 — above the 10.96 small-band ceiling — empirical proof
        # that the cap is a *cost-recovery ceiling*, not a payment cap on producers.
        assert rt.fixed_rp_kwh > 10.96
        assert rt.cap_rp_kwh is None

    def test_unknown_utility_raises(self, db):
        with pytest.raises(KeyError):
            resolve_tariff_at(
                "does_not_exist", date(2026, 4, 1), kw=10.0,
                eigenverbrauch=True, data=db,
            )

    def test_no_active_rate_raises_lookup_error(self, db):
        # Many utilities have only 2026+ rate windows; pre-2026 → LookupError.
        # (EKZ + AEW now carry historical 2017/2025 windows; pick a utility
        # with no historical data for a clean miss.)
        with pytest.raises(LookupError):
            resolve_tariff_at(
                "bkw", date(2025, 6, 1), kw=25.0,
                eigenverbrauch=True, data=db,
            )

    def test_resolve_with_hkn_rp_kwh_null_returns_zero(self):
        # Schema permits hkn_rp_kwh: null for hkn_structure in {none, bundled}.
        # Lint case_22 in the data repo enforces null specifically. The runtime
        # must coerce null to 0.0 at the boundary (the dataclass field is float).
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                "hkn_rp_kwh": None,
                "hkn_structure": "bundled",
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.hkn_rp_kwh == 0.0
        assert rt.hkn_structure == "bundled"

    def test_resolve_with_hkn_rp_kwh_missing_returns_zero(self):
        # Schema doesn't list hkn_rp_kwh as required for power_tier; for
        # hkn_structure in {none, bundled} the field can be omitted entirely.
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "rmp_quartal",
                "hkn_structure": "none",
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.hkn_rp_kwh == 0.0
        assert rt.hkn_structure == "none"


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
                    "cap_rules": None,
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
                    "cap_rules": None,
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
                    "cap_rules": None,
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
                    "cap_rules": None,
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
                    "cap_rules": None,
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
            "cap_rules": None,
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


class TestUserInputsDeclSignature:
    """v0.13.0 — Phase 2 hashable signature for grouping rate windows
    that share the same user_inputs[] declaration shape."""

    def test_empty_returns_empty_tuple(self):
        assert user_inputs_decl_signature({}) == ()
        assert user_inputs_decl_signature({"user_inputs": []}) == ()

    def test_none_returns_empty_tuple(self):
        assert user_inputs_decl_signature(None) == ()

    def test_same_decls_produce_equal_signatures(self):
        a = {"user_inputs": [
            {"key": "model", "type": "enum", "default": "x",
             "values": ["x", "y"], "label_de": "A"},
        ]}
        b = {"user_inputs": [
            # Different label_de, same key/type/default/values: same sig.
            {"key": "model", "type": "enum", "default": "x",
             "values": ["x", "y"], "label_de": "B-different"},
        ]}
        assert user_inputs_decl_signature(a) == user_inputs_decl_signature(b)

    def test_different_keys_produce_different_signatures(self):
        a = {"user_inputs": [{"key": "alpha", "type": "boolean",
                              "default": True, "label_de": "A"}]}
        b = {"user_inputs": [{"key": "beta", "type": "boolean",
                              "default": True, "label_de": "A"}]}
        assert user_inputs_decl_signature(a) != user_inputs_decl_signature(b)

    def test_different_values_produce_different_signatures(self):
        a = {"user_inputs": [{"key": "k", "type": "enum", "default": "x",
                              "values": ["x", "y"], "label_de": "A"}]}
        b = {"user_inputs": [{"key": "k", "type": "enum", "default": "x",
                              "values": ["x", "z"], "label_de": "A"}]}
        assert user_inputs_decl_signature(a) != user_inputs_decl_signature(b)

    def test_different_defaults_produce_different_signatures(self):
        a = {"user_inputs": [{"key": "k", "type": "enum", "default": "x",
                              "values": ["x", "y"], "label_de": "A"}]}
        b = {"user_inputs": [{"key": "k", "type": "enum", "default": "y",
                              "values": ["x", "y"], "label_de": "A"}]}
        assert user_inputs_decl_signature(a) != user_inputs_decl_signature(b)

    def test_array_order_does_not_matter(self):
        a = {"user_inputs": [
            {"key": "alpha", "type": "boolean", "default": True, "label_de": "A"},
            {"key": "beta", "type": "boolean", "default": False, "label_de": "B"},
        ]}
        b = {"user_inputs": [
            {"key": "beta", "type": "boolean", "default": False, "label_de": "B"},
            {"key": "alpha", "type": "boolean", "default": True, "label_de": "A"},
        ]}
        assert user_inputs_decl_signature(a) == user_inputs_decl_signature(b)

    def test_value_order_does_not_matter(self):
        # Curator reordering values list shouldn't trigger a split.
        a = {"user_inputs": [{"key": "k", "type": "enum", "default": "x",
                              "values": ["x", "y", "z"], "label_de": "A"}]}
        b = {"user_inputs": [{"key": "k", "type": "enum", "default": "x",
                              "values": ["z", "x", "y"], "label_de": "A"}]}
        assert user_inputs_decl_signature(a) == user_inputs_decl_signature(b)


class TestComputeUserInputsPeriods:
    """v0.13.0 — Phase 2 grouping of overlapping rate windows by
    user_inputs declaration signature."""

    def _patch_db(self, monkeypatch, utility_rates):
        """Install a synthetic db with one utility ('syn') and the given
        rate-window list."""
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "homepage": "https://example.test",
                    "rates": utility_rates,
                }
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)

    def test_unknown_utility_returns_empty(self, monkeypatch):
        self._patch_db(monkeypatch, [])
        assert compute_user_inputs_periods(
            "does_not_exist", date(2026, 1, 1), None
        ) == []

    def test_no_overlap_returns_empty(self, monkeypatch):
        self._patch_db(monkeypatch, [{
            "valid_from": "2026-01-01",
            "valid_to": "2027-01-01",
            "settlement_period": "quartal",
            "power_tiers": [],
        }])
        # Span is entirely BEFORE the only rate window.
        assert compute_user_inputs_periods(
            "syn", date(2024, 1, 1), date(2025, 1, 1)
        ) == []

    def test_single_window_returns_one_period(self, monkeypatch):
        rate = {
            "valid_from": "2026-01-01",
            "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [
                {"key": "k", "type": "boolean", "default": True,
                 "label_de": "K"},
            ],
        }
        self._patch_db(monkeypatch, [rate])
        periods = compute_user_inputs_periods("syn", date(2026, 4, 1), None)
        assert len(periods) == 1
        period_from, period_to, rep = periods[0]
        assert period_from == date(2026, 4, 1)
        assert period_to is None
        assert rep is rate

    def test_two_windows_same_decls_collapse_to_one_period(self, monkeypatch):
        common = [
            {"key": "k", "type": "boolean", "default": True,
             "label_de": "K"},
        ]
        rate_a = {
            "valid_from": "2026-01-01",
            "valid_to": "2027-01-01",
            "settlement_period": "quartal",
            "power_tiers": [], "user_inputs": common,
        }
        rate_b = {
            "valid_from": "2027-01-01",
            "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [], "user_inputs": common,
        }
        self._patch_db(monkeypatch, [rate_a, rate_b])
        periods = compute_user_inputs_periods(
            "syn", date(2026, 1, 1), None
        )
        assert len(periods) == 1
        period_from, period_to, rep = periods[0]
        assert period_from == date(2026, 1, 1)
        assert period_to is None
        assert rep is rate_a  # representative is first window

    def test_two_windows_different_decls_split_into_two_periods(
        self, monkeypatch
    ):
        rate_a = {
            "valid_from": "2026-01-01",
            "valid_to": "2027-01-01",
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [
                {"key": "old_key", "type": "enum", "default": "fix",
                 "values": ["fix", "rmp"], "label_de": "K"},
            ],
        }
        rate_b = {
            "valid_from": "2027-01-01",
            "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [
                {"key": "new_key", "type": "enum", "default": "fix",
                 "values": ["fix", "rmp"], "label_de": "K"},
            ],
        }
        self._patch_db(monkeypatch, [rate_a, rate_b])
        periods = compute_user_inputs_periods(
            "syn", date(2026, 1, 1), None
        )
        assert len(periods) == 2
        # Period 0: 2026 window
        assert periods[0][0] == date(2026, 1, 1)
        assert periods[0][1] == date(2027, 1, 1)
        assert periods[0][2] is rate_a
        # Period 1: 2027 window (open)
        assert periods[1][0] == date(2027, 1, 1)
        assert periods[1][1] is None
        assert periods[1][2] is rate_b

    def test_span_clamps_period_endpoints(self, monkeypatch):
        rate = {
            "valid_from": "2025-01-01",
            "valid_to": "2028-01-01",
            "settlement_period": "quartal",
            "power_tiers": [],
            "user_inputs": [{"key": "k", "type": "boolean",
                             "default": True, "label_de": "K"}],
        }
        self._patch_db(monkeypatch, [rate])
        # Span is a sub-interval of the rate window.
        periods = compute_user_inputs_periods(
            "syn", date(2026, 6, 1), date(2027, 3, 1)
        )
        assert len(periods) == 1
        assert periods[0][0] == date(2026, 6, 1)
        assert periods[0][1] == date(2027, 3, 1)

    def test_pure_rate_change_does_not_split(self, monkeypatch):
        # Same user_inputs decl across two windows, only fixed_rp_kwh
        # differs → one period (no split).
        decl = [{"key": "k", "type": "boolean", "default": True,
                 "label_de": "K"}]
        rate_a = {
            "valid_from": "2026-01-01", "valid_to": "2027-01-01",
            "settlement_period": "quartal",
            "power_tiers": [{"kw_min": 0, "kw_max": None,
                             "base_model": "fixed_flat",
                             "fixed_rp_kwh": 8.0,
                             "hkn_rp_kwh": 0.0, "hkn_structure": "none"}],
            "user_inputs": decl,
        }
        rate_b = {
            "valid_from": "2027-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{"kw_min": 0, "kw_max": None,
                             "base_model": "fixed_flat",
                             "fixed_rp_kwh": 9.5,  # different rate
                             "hkn_rp_kwh": 0.0, "hkn_structure": "none"}],
            "user_inputs": decl,
        }
        self._patch_db(monkeypatch, [rate_a, rate_b])
        periods = compute_user_inputs_periods(
            "syn", date(2026, 1, 1), None
        )
        assert len(periods) == 1


class TestSelfConsumptionRelevantPublicExport:
    """v0.16.0 — Issue 3: helper moved from config_flow to tariffs_db so
    services can import it without a circular dependency. Smoke-test the
    public re-export with one relevant + one irrelevant case.
    """

    def test_relevant_case_returns_true(self, monkeypatch):
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2026-01-01", "valid_to": None,
                "rules": [
                    {"kw_min": 0, "kw_max": 30,
                     "self_consumption": True, "min_rp_kwh": 6.0},
                    {"kw_min": 0, "kw_max": 30,
                     "self_consumption": False, "min_rp_kwh": 4.0},
                ],
            }],
            "utilities": {"syn": {"name_de": "Syn", "rates": []}},
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        assert tdb.self_consumption_relevant("syn", "2026-04-01", 10.0) is True

    def test_irrelevant_case_returns_false(self, monkeypatch):
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2026-01-01", "valid_to": None,
                "rules": [{"kw_min": 0, "kw_max": None,
                           "self_consumption": None, "min_rp_kwh": 4.0}],
            }],
            "utilities": {
                "syn": {"name_de": "Syn", "rates": [{
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "power_tiers": [{"kw_min": 0, "kw_max": None,
                                     "base_model": "fixed_flat",
                                     "fixed_rp_kwh": 8.0,
                                     "hkn_rp_kwh": 0.0,
                                     "hkn_structure": "none"}],
                }]},
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        assert tdb.self_consumption_relevant("syn", "2026-04-01", 10.0) is False


class TestUserInputLabelHelpers:
    """v0.16.1 — helpers moved from config_flow to tariffs_db so the
    recompute renderer can label-translate user_inputs without a
    circular import.
    """

    def test_pick_localised_label_picks_lang_then_de_then_en(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            pick_localised_label,
        )
        d = {"label_de": "Wahltarif", "label_en": "Choice tariff"}
        assert pick_localised_label(d, "label", "de", "—") == "Wahltarif"
        assert pick_localised_label(d, "label", "fr", "—") == "Wahltarif"  # de fallback
        assert pick_localised_label({}, "label", "de", "fb") == "fb"

    def test_user_input_label_falls_back_to_key(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            user_input_label,
        )
        assert (
            user_input_label({"key": "regio_top40_opted_in"}, "de")
            == "regio_top40_opted_in"
        )
        assert (
            user_input_label({"key": "k", "label_de": "L"}, "de")
            == "L"
        )

    def test_pick_value_label_falls_back_to_value(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            pick_value_label,
        )
        decl = {"value_labels_de": {"a": "Alpha"}}
        assert pick_value_label(decl, "a", "de") == "Alpha"
        assert pick_value_label(decl, "z", "de") == "z"
        assert pick_value_label({}, "v", "de") == "v"

    def test_resolve_user_inputs_decl_returns_active_window_decls(
        self, monkeypatch
    ):
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [],
            "utilities": {
                "syn": {"name_de": "Syn", "rates": [{
                    "valid_from": "2026-01-01", "valid_to": None,
                    "settlement_period": "quartal",
                    "user_inputs": [
                        {"key": "k1", "type": "boolean", "default": False,
                         "label_de": "Eins"}
                    ],
                    "power_tiers": [{
                        "kw_min": 0, "kw_max": None,
                        "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                        "hkn_rp_kwh": 0.0, "hkn_structure": "none",
                    }],
                }]},
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        decls = tdb.resolve_user_inputs_decl("syn", "2026-04-01")
        assert len(decls) == 1
        assert decls[0]["key"] == "k1"

    def test_resolve_user_inputs_decl_returns_empty_when_no_window(
        self, monkeypatch
    ):
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [],
            "utilities": {"syn": {"name_de": "Syn", "rates": []}},
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        assert tdb.resolve_user_inputs_decl("syn", "2026-04-01") == ()


# ----- v0.17.0 — tariff_model_label / settlement_period_label ---------------


class TestTariffModelLabel:
    """v0.17.0 — localised display labels for tariff-model enums."""

    def test_de_fixed_flat_plain(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            tariff_model_label,
        )
        assert tariff_model_label("fixed_flat", None, "de") == "Fixpreis"

    def test_de_fixed_flat_seasonal(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            tariff_model_label,
        )
        assert (
            tariff_model_label("fixed_flat", {"summer_rp_kwh": 6}, "de")
            == "Fixpreis (saisonal)"
        )

    def test_de_fixed_ht_nt_seasonal(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            tariff_model_label,
        )
        assert (
            tariff_model_label("fixed_ht_nt", {"summer_ht_rp_kwh": 1}, "de")
            == "Fixpreis (HT/NT, saisonal)"
        )

    def test_en_rmp_quartal(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            tariff_model_label,
        )
        assert (
            tariff_model_label("rmp_quartal", None, "en")
            == "Reference market price (quarterly)"
        )

    def test_unknown_model_falls_back_to_raw(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            tariff_model_label,
        )
        assert tariff_model_label("foo_bar_xyz", None, "de") == "foo_bar_xyz"

    def test_missing_model_returns_dash(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            tariff_model_label,
        )
        assert tariff_model_label(None, None, "de") == "—"
        assert tariff_model_label("", None, "de") == "—"

    def test_unknown_lang_falls_back_to_english(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            tariff_model_label,
        )
        # fr falls back to en table (no French entries yet).
        assert (
            tariff_model_label("fixed_flat", None, "fr")
            == "Fixed flat rate"
        )


class TestSettlementPeriodLabel:
    """v0.17.0 — localised display labels for settlement_period enums."""

    def test_de_quartal_monat_stunde(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            settlement_period_label,
        )
        assert settlement_period_label("quartal", "de") == "Quartal"
        assert settlement_period_label("monat", "de") == "Monat"
        assert settlement_period_label("stunde", "de") == "Stunde"

    def test_en_quartal_monat_stunde(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            settlement_period_label,
        )
        assert settlement_period_label("quartal", "en") == "Quarterly"
        assert settlement_period_label("monat", "en") == "Monthly"
        assert settlement_period_label("stunde", "en") == "Hourly"

    def test_unknown_period_falls_back_to_raw(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            settlement_period_label,
        )
        assert settlement_period_label("woche", "de") == "woche"

    def test_missing_period_returns_dash(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            settlement_period_label,
        )
        assert settlement_period_label(None, "de") == "—"
        assert settlement_period_label("", "de") == "—"


# ----- v0.17.0 — diff_tariffs_data ------------------------------------------


def _utility_dict(name: str, rate_windows: list[dict]) -> dict:
    return {"name_de": name, "rates": rate_windows}


def _rate(valid_from: str, **extra) -> dict:
    return {"valid_from": valid_from, "settlement_period": "quartal", **extra}


class TestDiffTariffsData:
    """v0.17.0 — refresh-prices notification needs to know what changed
    between the cached tariffs.json and the freshly-fetched copy.
    """

    def test_no_changes_returns_no_changes_true(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        old = {
            "data_version": "1.0.0",
            "utilities": {
                "ekz": _utility_dict("EKZ", [_rate("2026-01-01")]),
            },
        }
        new = {
            "data_version": "1.0.0",
            "utilities": {
                "ekz": _utility_dict("EKZ", [_rate("2026-01-01")]),
            },
        }
        diff = diff_tariffs_data(old, new)
        assert diff["no_changes"] is True
        assert diff["added_utilities"] == []
        assert diff["modified_rate_windows"] == []

    def test_added_utility_listed(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        old = {"utilities": {"ekz": _utility_dict("EKZ", [_rate("2026-01-01")])}}
        new = {
            "utilities": {
                "ekz": _utility_dict("EKZ", [_rate("2026-01-01")]),
                "ewz": _utility_dict("ewz", [_rate("2026-01-01")]),
            },
        }
        diff = diff_tariffs_data(old, new)
        assert diff["no_changes"] is False
        assert len(diff["added_utilities"]) == 1
        assert diff["added_utilities"][0]["key"] == "ewz"
        assert diff["added_utilities"][0]["name"] == "ewz"
        assert diff["added_utilities"][0]["rate_window_dates"] == ["2026-01-01"]

    def test_removed_utility_listed(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        old = {
            "utilities": {
                "ekz": _utility_dict("EKZ", [_rate("2026-01-01")]),
                "ewz": _utility_dict("ewz", [_rate("2026-01-01")]),
            },
        }
        new = {"utilities": {"ekz": _utility_dict("EKZ", [_rate("2026-01-01")])}}
        diff = diff_tariffs_data(old, new)
        assert len(diff["removed_utilities"]) == 1
        assert diff["removed_utilities"][0]["key"] == "ewz"

    def test_added_rate_window_for_existing_utility(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        old = {"utilities": {"ekz": _utility_dict("EKZ", [_rate("2024-01-01")])}}
        new = {
            "utilities": {
                "ekz": _utility_dict(
                    "EKZ", [_rate("2024-01-01"), _rate("2026-01-01")]
                ),
            },
        }
        diff = diff_tariffs_data(old, new)
        assert diff["added_utilities"] == []
        assert len(diff["added_rate_windows"]) == 1
        assert diff["added_rate_windows"][0]["key"] == "ekz"
        assert diff["added_rate_windows"][0]["rate_window_dates"] == [
            "2026-01-01"
        ]

    def test_modified_rate_window_detected_by_deep_equality(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        old = {
            "utilities": {
                "ekz": _utility_dict(
                    "EKZ", [_rate("2026-01-01", price_floor_rp_kwh=6.0)]
                ),
            },
        }
        new = {
            "utilities": {
                "ekz": _utility_dict(
                    "EKZ", [_rate("2026-01-01", price_floor_rp_kwh=6.5)]
                ),
            },
        }
        diff = diff_tariffs_data(old, new)
        assert len(diff["modified_rate_windows"]) == 1
        assert diff["modified_rate_windows"][0]["key"] == "ekz"
        assert (
            diff["modified_rate_windows"][0]["rate_window_dates"]
            == ["2026-01-01"]
        )

    def test_unchanged_rate_window_not_listed(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        old = {
            "utilities": {
                "ekz": _utility_dict(
                    "EKZ", [_rate("2026-01-01", price_floor_rp_kwh=6.0)]
                ),
            },
        }
        new = {
            "utilities": {
                "ekz": _utility_dict(
                    "EKZ", [_rate("2026-01-01", price_floor_rp_kwh=6.0)]
                ),
            },
        }
        diff = diff_tariffs_data(old, new)
        assert diff["modified_rate_windows"] == []
        assert diff["no_changes"] is True

    def test_data_version_change_recorded(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        old = {"data_version": "1.0.0", "utilities": {}}
        new = {"data_version": "1.1.0", "utilities": {}}
        diff = diff_tariffs_data(old, new)
        assert diff["data_version_changed"] == ("1.0.0", "1.1.0")
        assert diff["no_changes"] is False

    def test_handles_none_inputs_safely(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            diff_tariffs_data,
        )
        diff = diff_tariffs_data(None, None)
        assert diff["no_changes"] is True
        assert diff["added_utilities"] == []


# ----- v0.22.0 — schema 1.5.0 resolver tests -------------------------------


class TestSchema150CapRulesActivation:
    """v0.22.0 — schema 1.5.0 dropped ``cap_mode``. Cap activation is
    signaled by a non-empty ``cap_rules`` array; missing key or empty
    array both behave as "no cap"."""

    def test_cap_rules_empty_list_means_no_cap(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "cap_rules": [],
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                "hkn_structure": "none",
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.cap_rp_kwh is None

    def test_cap_rules_missing_means_no_cap(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            # No cap_rules key at all.
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                "hkn_structure": "none",
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.cap_rp_kwh is None

    def test_cap_rules_present_picks_matching_rule(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "cap_rules": [
                {"kw_min": 0, "kw_max": 100,
                 "self_consumption": True, "cap_rp_kwh": 10.96},
                {"kw_min": 0, "kw_max": 100,
                 "self_consumption": False, "cap_rp_kwh": 8.20},
            ],
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "rmp_quartal",
                "hkn_structure": "additive_optin", "hkn_rp_kwh": 3.0,
            }],
        }])
        rt_with_ev = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt_with_ev.cap_rp_kwh == 10.96
        rt_no_ev = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=False, data=synthetic,
        )
        assert rt_no_ev.cap_rp_kwh == 8.20

    def test_legacy_cap_mode_key_tolerated_and_ignored(self):
        # Old data files with stray ``cap_mode`` keys still validate
        # (additionalProperties:true on tariff_rate_window) and the
        # resolver simply ignores the field.
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "cap_mode": True,           # stray legacy key
            "cap_rules": [
                {"kw_min": 0, "kw_max": None,
                 "self_consumption": None, "cap_rp_kwh": 7.20},
            ],
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "rmp_quartal", "hkn_structure": "none",
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.cap_rp_kwh == 7.20  # picked from cap_rules, regardless of stray cap_mode


class TestSchema150HknDefaultsInheritance:
    """v0.22.0 — schema 1.5.0 rate-level ``hkn_structure_default`` and
    ``hkn_rp_kwh_default``. When a tier omits its own ``hkn_structure``
    / ``hkn_rp_kwh``, the resolver inherits the rate-level default."""

    def test_inherits_hkn_structure_default(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "hkn_structure_default": "additive_optin",
            "hkn_rp_kwh_default": 3.0,
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "rmp_quartal",
                # tier omits hkn_structure + hkn_rp_kwh → inherit defaults
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.hkn_structure == "additive_optin"
        assert rt.hkn_rp_kwh == 3.0
        assert rt.hkn_structure_default == "additive_optin"
        assert rt.hkn_rp_kwh_default == 3.0

    def test_tier_explicit_value_wins_over_default(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "hkn_structure_default": "additive_optin",
            "hkn_rp_kwh_default": 3.0,
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "rmp_quartal",
                "hkn_structure": "none",  # explicit override
                "hkn_rp_kwh": None,
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        # Explicit "none" wins; rate-level default still exposed for inspection.
        assert rt.hkn_structure == "none"
        assert rt.hkn_structure_default == "additive_optin"

    def test_no_defaults_no_tier_field_falls_back_to_legacy_none(self):
        # Old shape (no rate-level default, tier omits hkn_structure):
        # resolver coerces to "none" (existing schema-pre-1.5.0 behaviour).
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.hkn_structure == "none"
        assert rt.hkn_rp_kwh == 0.0


class TestSchema160TierLevelBonuses:
    """v0.23.0 — schema 1.6.0 optional tier-level ``bonuses`` overlay.
    Concatenated after rate-level bonuses at evaluation time. (The v0.22.0
    ``tier_seasonal`` companion was dropped — see TestFixedSeasonalResolver
    for the new dispatch model.)"""

    def test_resolve_loads_tier_bonuses_as_tuple(self):
        tier_bonus = {
            "kind": "additive_rp_kwh", "name": "Tier-Bonus",
            "rate_rp_kwh": 0.5, "applies_when": "always",
        }
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                "hkn_structure": "none",
                "bonuses": [tier_bonus],
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.tier_bonuses == (tier_bonus,)
        # Rate-level bonuses untouched.
        assert rt.bonuses is None

    def test_tier_bonuses_default_to_none_when_absent(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                "hkn_structure": "none",
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.tier_bonuses is None

    def test_tier_bonuses_empty_list_collapses_to_none(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                "hkn_structure": "none",
                "bonuses": [],   # explicitly empty
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.tier_bonuses is None


class TestFixedSeasonalResolver:
    """v0.23.0 — schema 1.6.0 ``base_model: "fixed_seasonal"`` makes the
    tier-level seasonal block authoritative for both prices AND the
    summer/winter calendar (Q1 decision). Rate-level seasonal is
    irrelevant for these tiers."""

    _SEASONAL = {
        "summer_months": [4, 5, 6, 7, 8, 9],
        "winter_months": [10, 11, 12, 1, 2, 3],
        "summer_rp_kwh": 20.0,
        "winter_rp_kwh": 30.0,
    }

    def test_fixed_seasonal_writes_tier_seasonal_into_rt_seasonal(self):
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_seasonal",
                "hkn_structure": "none",
                "seasonal": self._SEASONAL,
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.base_model == "fixed_seasonal"
        assert rt.seasonal == self._SEASONAL

    def test_fixed_seasonal_ignores_rate_level_seasonal(self):
        # Even when the rate carries its own seasonal block, fixed_seasonal
        # tier overrides it for this resolution. (Rate-level seasonal would
        # mean different summer/winter month splits — irrelevant here.)
        rate_seasonal = {"summer_months": [7], "winter_months": [1]}
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "seasonal": rate_seasonal,
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_seasonal",
                "hkn_structure": "none",
                "seasonal": self._SEASONAL,
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.seasonal == self._SEASONAL
        assert rt.seasonal != rate_seasonal

    def test_non_fixed_seasonal_uses_rate_level_seasonal(self):
        # Regression guard: tier-level seasonal on non-fixed_seasonal tiers
        # MUST be ignored (the v0.22.0 overlay convention is gone).
        rate_seasonal = {
            "summer_months": [4, 5, 6, 7, 8, 9],
            "winter_months": [10, 11, 12, 1, 2, 3],
        }
        synthetic = _synthetic_db("synth", [{
            "valid_from": "2026-01-01", "valid_to": None,
            "settlement_period": "quartal",
            "seasonal": rate_seasonal,
            "power_tiers": [{
                "kw_min": 0, "kw_max": None,
                "base_model": "fixed_flat", "fixed_rp_kwh": 8.0,
                "hkn_structure": "none",
            }],
        }])
        rt = resolve_tariff_at(
            "synth", date(2026, 4, 1), kw=10.0,
            eigenverbrauch=True, data=synthetic,
        )
        assert rt.base_model == "fixed_flat"
        assert rt.seasonal == rate_seasonal

    def test_aew_2026_spezial_resolves_to_fixed_seasonal(self, db):
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=15.0, eigenverbrauch=True,
            user_inputs={"fixpreis_rmp": "spezial"}, data=db,
        )
        assert rt.base_model == "fixed_seasonal"
        assert rt.seasonal["summer_rp_kwh"] == 20.0
        assert rt.seasonal["winter_rp_kwh"] == 30.0
        assert rt.hkn_rp_kwh == 15.0
        assert rt.hkn_structure == "additive_optin"

    def test_aew_2026_spezialmitbonus_carries_winter_bonus(self, db):
        rt = resolve_tariff_at(
            "aew", date(2026, 4, 1), kw=15.0, eigenverbrauch=True,
            user_inputs={"fixpreis_rmp": "spezialmitbonus"}, data=db,
        )
        assert rt.base_model == "fixed_seasonal"
        assert rt.tier_bonuses is not None
        assert len(rt.tier_bonuses) == 1
        bonus = rt.tier_bonuses[0]
        assert bonus["kind"] == "additive_rp_kwh"
        assert bonus["rate_rp_kwh"] == 15.0
        assert bonus["when"]["season"] == "winter"
