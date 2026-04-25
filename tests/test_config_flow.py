"""Pure-function tests for the v0.5 config flow.

Tests that don't need the HA test harness — focused on the tariff validator
and the presence of all required keys in the localisation files.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from custom_components.bfe_rueckliefertarif.config_flow import _validate_tariff
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_NAMENSPRAEFIX,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
)
from custom_components.bfe_rueckliefertarif.tariffs_db import list_utility_keys


_COMPONENT_DIR = Path(__file__).resolve().parents[1] / "custom_components" / "bfe_rueckliefertarif"


class TestValidateTariff:
    """v0.5 validator: only the kw>0 check survives. Plant category is gone
    (federal floor / cap derive from kW + EV); fixpreis is JSON-resolved."""

    def _base(self, **overrides):
        data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }
        data.update(overrides)
        return data

    def test_positive_kw_passes(self):
        assert _validate_tariff(self._base()) == {}

    def test_zero_kw_rejected(self):
        errors = _validate_tariff(self._base(installierte_leistung_kw=0.0))
        assert errors == {CONF_INSTALLIERTE_LEISTUNG_KW: "kw_required"}

    def test_negative_kw_rejected(self):
        errors = _validate_tariff(self._base(installierte_leistung_kw=-1.0))
        assert errors == {CONF_INSTALLIERTE_LEISTUNG_KW: "kw_required"}

    def test_eigenverbrauch_false_still_passes(self):
        # Pure ohne-Eigenverbrauch (Volleinspeisung) plants are valid.
        assert _validate_tariff(self._base(eigenverbrauch_aktiviert=False)) == {}

    def test_hkn_aktiviert_doesnt_affect_validation(self):
        assert _validate_tariff(self._base(hkn_aktiviert=True)) == {}


class TestUtilityKeys:
    """tariffs.json shape sanity for the menu wiring."""

    def test_aew_split_present(self):
        keys = set(list_utility_keys())
        assert "aew_fixpreis" in keys
        assert "aew_rmp" in keys

    def test_old_aew_key_gone(self):
        assert "aew" not in list_utility_keys()

    def test_minimum_thirteen_utilities(self):
        # 11 utilities + AEW split → 13. Adding more is fine; fewer breaks v0.5.
        assert len(list_utility_keys()) >= 13


class TestStringsAndTranslations:
    """JSON-shape sanity for strings.json + translations/*.json.

    Phase 3 dropped CONF_ANLAGENKATEGORIE, CONF_BASISVERGUETUNG,
    CONF_FIXPREIS_RP_KWH, CONF_VERGUETUNGS_OBERGRENZE from the tariff step.
    """

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
            for key in list_utility_keys():
                assert f"preset_{key}" in menu, f"missing menu_options.preset_{key}"

    @pytest.mark.parametrize(
        "field",
        [
            CONF_INSTALLIERTE_LEISTUNG_KW,
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_HKN_AKTIVIERT,
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

    def test_legacy_anlagenkategorie_selector_gone(self, en_strings):
        # Hassfest tolerates extra selector keys, but we want a clean break:
        # nothing should still be referencing the deleted dropdown.
        assert "anlagenkategorie" not in en_strings.get("selector", {})

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
            "aktuelle_verguetung_chf_kwh",
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
            "recompute_aktuelles_quartal_estimate",
            "recompute_historie",
        ],
    )
    def test_button_translations_present(self, en_strings, de_translations, key):
        for d in (en_strings, de_translations):
            assert key in d["entity"]["button"]
            assert "name" in d["entity"]["button"][key]

    def test_options_step_init_is_menu(self, en_strings, de_translations):
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
