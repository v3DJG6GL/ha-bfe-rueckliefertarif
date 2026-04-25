"""Pure-function tests for the v2 config flow.

Tests that don't need the HA test harness — focused on the migration maps,
the preset → v2-value translation, the tariff validator, and the presence
of all required keys in the localisation files.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from custom_components.bfe_rueckliefertarif.config_flow import (
    _PRESET_LEGACY_TO_NEW,
    _validate_tariff,
)
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    BASISVERGUETUNG_FIXPREIS,
    BASISVERGUETUNG_REFERENZMARKTPREIS,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_ANLAGENKATEGORIE,
    CONF_BASISVERGUETUNG,
    CONF_ENERGIEVERSORGER,
    CONF_FIXPREIS_RP_KWH,
    CONF_HKN_VERGUETUNG_RP_KWH,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_NAMENSPRAEFIX,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    CONF_VERGUETUNGS_OBERGRENZE,
    _V1_TO_V2_KEY_MAP,
    _V1_TO_V2_VALUE_MAP,
)
from custom_components.bfe_rueckliefertarif.presets import PRESETS, list_preset_keys
from custom_components.bfe_rueckliefertarif.tariff import Segment


_COMPONENT_DIR = Path(__file__).resolve().parents[1] / "custom_components" / "bfe_rueckliefertarif"


class TestPresetLegacyToNew:
    def test_rmp_passthrough_maps_to_referenzmarktpreis(self):
        assert _PRESET_LEGACY_TO_NEW["rmp_passthrough"] == BASISVERGUETUNG_REFERENZMARKTPREIS

    def test_fixed_rate_maps_to_fixpreis(self):
        assert _PRESET_LEGACY_TO_NEW["fixed_rate"] == BASISVERGUETUNG_FIXPREIS

    def test_covers_all_preset_base_modes(self):
        from custom_components.bfe_rueckliefertarif.presets import PRESETS

        used = {p.base_mode for p in PRESETS.values()}
        assert used <= set(_PRESET_LEGACY_TO_NEW.keys())


class TestValidateTariff:
    def _base(self, **overrides):
        data = {
            CONF_ANLAGENKATEGORIE: Segment.SMALL_MIT_EV.value,
            CONF_INSTALLIERTE_LEISTUNG_KW: 0.0,
            CONF_BASISVERGUETUNG: BASISVERGUETUNG_REFERENZMARKTPREIS,
            CONF_HKN_VERGUETUNG_RP_KWH: 0.0,
            CONF_FIXPREIS_RP_KWH: 0.0,
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }
        data.update(overrides)
        return data

    def test_small_segment_no_kw_required(self):
        assert _validate_tariff(self._base()) == {}

    def test_mid_mit_ev_requires_kw(self):
        errors = _validate_tariff(
            self._base(anlagenkategorie=Segment.MID_MIT_EV.value)
        )
        assert errors == {CONF_INSTALLIERTE_LEISTUNG_KW: "kw_required_for_degressive"}

    def test_large_mit_ev_requires_kw(self):
        errors = _validate_tariff(
            self._base(anlagenkategorie=Segment.LARGE_MIT_EV.value)
        )
        assert errors == {CONF_INSTALLIERTE_LEISTUNG_KW: "kw_required_for_degressive"}

    def test_mid_mit_ev_with_kw_ok(self):
        data = self._base(
            anlagenkategorie=Segment.MID_MIT_EV.value,
            installierte_leistung_kw=60.0,
        )
        assert _validate_tariff(data) == {}

    def test_fixpreis_requires_positive_rate(self):
        errors = _validate_tariff(
            self._base(basisverguetung=BASISVERGUETUNG_FIXPREIS)
        )
        assert errors == {CONF_FIXPREIS_RP_KWH: "fixpreis_required"}

    def test_fixpreis_with_rate_ok(self):
        data = self._base(
            basisverguetung=BASISVERGUETUNG_FIXPREIS,
            fixpreis_rp_kwh=12.91,
        )
        assert _validate_tariff(data) == {}

    def test_xl_mit_ev_no_kw_required(self):
        # XL segments don't use the degressive formula
        assert _validate_tariff(
            self._base(anlagenkategorie=Segment.XL_MIT_EV.value)
        ) == {}


class TestMigrationMap:
    def test_all_v1_keys_mapped(self):
        v1_keys = {
            "preset",
            "segment",
            "kw",
            "base_mode",
            "hkn_bonus_rp_kwh",
            "fixed_rate_rp_kwh",
            "billing_mode",
            "export_entity",
            "compensation_entity",
            "entity_prefix",
        }
        assert set(_V1_TO_V2_KEY_MAP.keys()) == v1_keys

    def test_v2_key_targets_match_const_module(self):
        targets = {
            "preset": CONF_ENERGIEVERSORGER,
            "segment": CONF_ANLAGENKATEGORIE,
            "kw": CONF_INSTALLIERTE_LEISTUNG_KW,
            "base_mode": CONF_BASISVERGUETUNG,
            "hkn_bonus_rp_kwh": CONF_HKN_VERGUETUNG_RP_KWH,
            "fixed_rate_rp_kwh": CONF_FIXPREIS_RP_KWH,
            "billing_mode": CONF_ABRECHNUNGS_RHYTHMUS,
            "export_entity": CONF_STROMNETZEINSPEISUNG_KWH,
            "compensation_entity": CONF_RUECKLIEFERVERGUETUNG_CHF,
            "entity_prefix": CONF_NAMENSPRAEFIX,
        }
        assert _V1_TO_V2_KEY_MAP == targets

    def test_value_map_basisverguetung(self):
        m = _V1_TO_V2_VALUE_MAP[CONF_BASISVERGUETUNG]
        assert m["rmp_passthrough"] == BASISVERGUETUNG_REFERENZMARKTPREIS
        assert m["fixed_rate"] == BASISVERGUETUNG_FIXPREIS

    def test_value_map_abrechnungs_rhythmus(self):
        m = _V1_TO_V2_VALUE_MAP[CONF_ABRECHNUNGS_RHYTHMUS]
        assert m["quarterly"] == ABRECHNUNGS_RHYTHMUS_QUARTAL
        assert m["monthly"] == ABRECHNUNGS_RHYTHMUS_MONAT

    def test_apply_migration_logic(self):
        """Replicate the logic from async_migrate_entry on a fixture."""
        v1_data = {
            "preset": "ekz",
            "segment": "small_mit_ev",
            "kw": 10.0,
            "base_mode": "rmp_passthrough",
            "hkn_bonus_rp_kwh": 3.0,
            "fixed_rate_rp_kwh": 0.0,
            "billing_mode": "quarterly",
            "export_entity": "sensor.power_meter_exported",
            "compensation_entity": "sensor.power_meter_exported_compensation",
            "entity_prefix": "ekz_rueckliefertarif",
        }
        migrated = {_V1_TO_V2_KEY_MAP.get(k, k): v for k, v in v1_data.items()}
        for field, mapping in _V1_TO_V2_VALUE_MAP.items():
            if field in migrated and migrated[field] in mapping:
                migrated[field] = mapping[migrated[field]]

        assert migrated[CONF_ENERGIEVERSORGER] == "ekz"
        assert migrated[CONF_ANLAGENKATEGORIE] == "small_mit_ev"
        assert migrated[CONF_INSTALLIERTE_LEISTUNG_KW] == 10.0
        assert migrated[CONF_BASISVERGUETUNG] == BASISVERGUETUNG_REFERENZMARKTPREIS
        assert migrated[CONF_HKN_VERGUETUNG_RP_KWH] == 3.0
        assert migrated[CONF_ABRECHNUNGS_RHYTHMUS] == ABRECHNUNGS_RHYTHMUS_QUARTAL
        assert migrated[CONF_STROMNETZEINSPEISUNG_KWH] == "sensor.power_meter_exported"
        assert (
            migrated[CONF_RUECKLIEFERVERGUETUNG_CHF]
            == "sensor.power_meter_exported_compensation"
        )
        assert migrated[CONF_NAMENSPRAEFIX] == "ekz_rueckliefertarif"

    def test_apply_migration_idempotent_on_v2_data(self):
        v2_data = {
            CONF_ENERGIEVERSORGER: "ekz",
            CONF_BASISVERGUETUNG: BASISVERGUETUNG_REFERENZMARKTPREIS,
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }
        migrated = {_V1_TO_V2_KEY_MAP.get(k, k): v for k, v in v2_data.items()}
        for field, mapping in _V1_TO_V2_VALUE_MAP.items():
            if field in migrated and migrated[field] in mapping:
                migrated[field] = mapping[migrated[field]]
        assert migrated == v2_data


class TestPresetVerguetungsObergrenze:
    """Each preset must declare verguetungs_obergrenze; only EKZ/Groupe E/Primeo on."""

    def test_every_preset_has_verguetungs_obergrenze(self):
        for key, preset in PRESETS.items():
            assert hasattr(preset, "verguetungs_obergrenze"), (
                f"preset {key} missing verguetungs_obergrenze field"
            )
            assert isinstance(preset.verguetungs_obergrenze, bool)

    def test_only_ekz_groupe_e_primeo_apply_cap(self):
        with_cap = {k for k, p in PRESETS.items() if p.verguetungs_obergrenze}
        assert with_cap == {"ekz", "groupe_e", "primeo"}, (
            f"Unexpected utilities applying Vergütungs-Obergrenze: {with_cap}"
        )

    def test_custom_default_is_no_cap(self):
        assert PRESETS["custom"].verguetungs_obergrenze is False


class TestV2ToV3Migration:
    """v2→v3 seeds verguetungs_obergrenze from the entry's energieversorger preset."""

    def _migrate_v2_to_v3(self, entry_data: dict) -> dict:
        """Replicate the logic in async_migrate_entry's v2→v3 branch."""
        data = dict(entry_data)
        energieversorger = data.get(CONF_ENERGIEVERSORGER, "custom")
        preset = PRESETS.get(energieversorger)
        data[CONF_VERGUETUNGS_OBERGRENZE] = (
            preset.verguetungs_obergrenze if preset is not None else False
        )
        return data

    def test_ekz_entry_gets_cap_on(self):
        v2 = {CONF_ENERGIEVERSORGER: "ekz"}
        v3 = self._migrate_v2_to_v3(v2)
        assert v3[CONF_VERGUETUNGS_OBERGRENZE] is True

    def test_groupe_e_entry_gets_cap_on(self):
        v2 = {CONF_ENERGIEVERSORGER: "groupe_e"}
        v3 = self._migrate_v2_to_v3(v2)
        assert v3[CONF_VERGUETUNGS_OBERGRENZE] is True

    def test_bkw_entry_gets_cap_off(self):
        v2 = {CONF_ENERGIEVERSORGER: "bkw"}
        v3 = self._migrate_v2_to_v3(v2)
        assert v3[CONF_VERGUETUNGS_OBERGRENZE] is False

    def test_iwb_entry_gets_cap_off(self):
        v2 = {CONF_ENERGIEVERSORGER: "iwb"}
        v3 = self._migrate_v2_to_v3(v2)
        assert v3[CONF_VERGUETUNGS_OBERGRENZE] is False

    def test_custom_entry_gets_cap_off(self):
        v2 = {CONF_ENERGIEVERSORGER: "custom"}
        v3 = self._migrate_v2_to_v3(v2)
        assert v3[CONF_VERGUETUNGS_OBERGRENZE] is False

    def test_unknown_energieversorger_defaults_off(self):
        v2 = {CONF_ENERGIEVERSORGER: "future_unknown_utility"}
        v3 = self._migrate_v2_to_v3(v2)
        assert v3[CONF_VERGUETUNGS_OBERGRENZE] is False

    def test_v2_other_keys_preserved(self):
        v2 = {
            CONF_ENERGIEVERSORGER: "ekz",
            CONF_HKN_VERGUETUNG_RP_KWH: 3.0,
            CONF_BASISVERGUETUNG: BASISVERGUETUNG_REFERENZMARKTPREIS,
        }
        v3 = self._migrate_v2_to_v3(v2)
        assert v3[CONF_HKN_VERGUETUNG_RP_KWH] == 3.0
        assert v3[CONF_BASISVERGUETUNG] == BASISVERGUETUNG_REFERENZMARKTPREIS


class TestStringsAndTranslations:
    """JSON-shape sanity for strings.json + translations/*.json."""

    @pytest.fixture
    def en_strings(self):
        return json.loads((_COMPONENT_DIR / "strings.json").read_text())

    @pytest.fixture
    def de_translations(self):
        return json.loads(
            (_COMPONENT_DIR / "translations" / "de.json").read_text()
        )

    @pytest.fixture
    def fr_translations(self):
        return json.loads(
            (_COMPONENT_DIR / "translations" / "fr.json").read_text()
        )

    @pytest.mark.parametrize(
        "step",
        ["user", "tariff", "entities"],
    )
    def test_config_steps_present(self, en_strings, de_translations, step):
        for d in (en_strings, de_translations):
            assert step in d["config"]["step"], f"missing config.step.{step}"

    def test_user_step_has_menu_options(self, en_strings, de_translations):
        for d in (en_strings, de_translations):
            menu = d["config"]["step"]["user"]["menu_options"]
            for key in list_preset_keys():
                assert f"preset_{key}" in menu, f"missing menu_options.preset_{key}"

    @pytest.mark.parametrize(
        "field",
        [
            CONF_ANLAGENKATEGORIE,
            CONF_INSTALLIERTE_LEISTUNG_KW,
            CONF_BASISVERGUETUNG,
            CONF_HKN_VERGUETUNG_RP_KWH,
            CONF_FIXPREIS_RP_KWH,
            CONF_VERGUETUNGS_OBERGRENZE,
            CONF_ABRECHNUNGS_RHYTHMUS,
        ],
    )
    def test_tariff_field_label_and_help(self, en_strings, de_translations, field):
        for d in (en_strings, de_translations):
            assert field in d["config"]["step"]["tariff"]["data"]
            assert field in d["config"]["step"]["tariff"]["data_description"]

    @pytest.mark.parametrize(
        "field",
        [
            CONF_STROMNETZEINSPEISUNG_KWH,
            CONF_RUECKLIEFERVERGUETUNG_CHF,
            CONF_NAMENSPRAEFIX,
        ],
    )
    def test_entities_field_label_and_help(self, en_strings, de_translations, field):
        for d in (en_strings, de_translations):
            assert field in d["config"]["step"]["entities"]["data"]
            assert field in d["config"]["step"]["entities"]["data_description"]

    def test_selector_options_anlagenkategorie(self, en_strings, de_translations):
        for d in (en_strings, de_translations):
            opts = d["selector"]["anlagenkategorie"]["options"]
            for seg in Segment:
                assert seg.value in opts, f"missing selector option {seg.value}"

    def test_selector_options_basisverguetung(self, en_strings, de_translations):
        for d in (en_strings, de_translations):
            opts = d["selector"]["basisverguetung"]["options"]
            assert BASISVERGUETUNG_REFERENZMARKTPREIS in opts
            assert BASISVERGUETUNG_FIXPREIS in opts

    def test_selector_options_abrechnungs_rhythmus(
        self, en_strings, de_translations
    ):
        for d in (en_strings, de_translations):
            opts = d["selector"]["abrechnungs_rhythmus"]["options"]
            assert ABRECHNUNGS_RHYTHMUS_QUARTAL in opts
            assert ABRECHNUNGS_RHYTHMUS_MONAT in opts

    @pytest.mark.parametrize(
        "key",
        [
            "basisverguetung",
            "hkn_verguetung",
            "naechste_referenzmarktpreis_publikation",
            "referenzmarktpreis_q",
            "referenzmarktpreis_m",
        ],
    )
    def test_sensor_translations_present(self, en_strings, de_translations, key):
        for d in (en_strings, de_translations):
            assert key in d["entity"]["sensor"]
            assert "name" in d["entity"]["sensor"][key]

    @pytest.mark.parametrize(
        "key",
        [
            "reload_referenzmarktpreise",
            "recompute_letztes_publiziertes_quartal",
            "recompute_historie",
        ],
    )
    def test_button_translations_present(self, en_strings, de_translations, key):
        for d in (en_strings, de_translations):
            assert key in d["entity"]["button"]
            assert "name" in d["entity"]["button"][key]

    def test_options_step_init_is_menu(self, en_strings, de_translations):
        # Options init is a menu with three sub-steps.
        for d in (en_strings, de_translations):
            assert "init" in d["options"]["step"]
            assert "menu_options" in d["options"]["step"]["init"]
            menu = d["options"]["step"]["init"]["menu_options"]
            assert "tariff" in menu
            assert "reimport_quarter" in menu
            assert "entities" in menu

    @pytest.mark.parametrize("sub_step", ["tariff", "reimport_quarter", "entities"])
    def test_options_substeps_present(self, en_strings, de_translations, sub_step):
        for d in (en_strings, de_translations):
            assert sub_step in d["options"]["step"]
            assert "data" in d["options"]["step"][sub_step]

    def test_fr_translations_minimum_keys(self, fr_translations):
        # French has the essentials but may skip detailed help text.
        assert "user" in fr_translations["config"]["step"]
        assert "tariff" in fr_translations["config"]["step"]
        assert "entities" in fr_translations["config"]["step"]
        assert (
            "small_mit_ev"
            in fr_translations["selector"]["anlagenkategorie"]["options"]
        )
