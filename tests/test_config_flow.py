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
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_NAMENSPRAEFIX,
    CONF_PLANT_NAME,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    CONF_VALID_FROM,
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

    def test_unified_aew_present(self):
        # v0.11.0 (Batch D) — AEW collapsed back to one key with a
        # ``user_inputs.tariff_model`` enum picking fixpreis vs rmp.
        keys = set(list_utility_keys())
        assert "aew" in keys

    def test_old_aew_split_keys_gone(self):
        # v0.11.0 (Batch D) — `aew_fixpreis` / `aew_rmp` retired.
        keys = set(list_utility_keys())
        assert "aew_fixpreis" not in keys
        assert "aew_rmp" not in keys

    def test_minimum_thirteen_utilities(self):
        # v0.11.0: 13+ unified utilities (was 11 + AEW-split = 13 before).
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

    def test_every_utility_has_display_name(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _utility_display_name,
        )

        for key in list_utility_keys():
            name = _utility_display_name(key)
            assert name and name != key, (
                f"{key} missing name_de/name_fr in tariffs.json"
            )

    @pytest.mark.parametrize(
        "field",
        [
            CONF_VALID_FROM,
            CONF_INSTALLIERTE_LEISTUNG_KW,
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_HKN_AKTIVIERT,
        ],
    )
    def test_tariff_field_label_and_help(self, en_strings, de_translations, field):
        for d in (en_strings, de_translations):
            assert field in d["config"]["step"]["tariff"]["data"]
            assert field in d["config"]["step"]["tariff"]["data_description"]

    def test_abrechnungs_rhythmus_form_field_dropped(
        self, en_strings, de_translations
    ):
        """v0.9.8 — billing toggle is gone from every form (#9). Translations
        must not still ship the field labels or selector options."""
        for d in (en_strings, de_translations):
            for step in ("tariff",):
                assert CONF_ABRECHNUNGS_RHYTHMUS not in d["config"]["step"][step]["data"]
            for step in ("apply_change", "add_new_row", "edit_row"):
                assert CONF_ABRECHNUNGS_RHYTHMUS not in d["options"]["step"][step]["data"]
            assert CONF_ABRECHNUNGS_RHYTHMUS not in d.get("selector", {})

    @pytest.mark.parametrize(
        "field",
        [
            CONF_STROMNETZEINSPEISUNG_KWH,
            CONF_RUECKLIEFERVERGUETUNG_CHF,
            CONF_PLANT_NAME,
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

    def test_selector_options_block_is_empty(
        self, en_strings, de_translations
    ):
        # v0.9.8: abrechnungs_rhythmus selector dropped (#9). The "selector"
        # block is currently empty until the next batch reintroduces a
        # selector-based field.
        for d in (en_strings, de_translations):
            assert d.get("selector", {}) == {}

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

    def test_button_platform_strings_removed(self, en_strings, de_translations):
        # v0.9.0: button.py is gone entirely. There must be no entity.button
        # block left over in any locale.
        for d in (en_strings, de_translations):
            assert "button" not in d.get("entity", {}), (
                "entity.button block must be removed after v0.9.0"
            )

    def test_options_step_init_is_menu(self, en_strings, de_translations):
        for d in (en_strings, de_translations):
            assert "init" in d["options"]["step"]
            assert "menu_options" in d["options"]["step"]["init"]
            menu = d["options"]["step"]["init"]["menu_options"]
            # v0.9.6: refresh_prices renamed → refresh_data (combined refresh).
            assert set(menu.keys()) == {
                "apply_change",
                "manage_history",
                "recompute_history",
                "refresh_data",
                "entities",
            }

    @pytest.mark.parametrize(
        "sub_step",
        ["apply_change", "recompute_history", "refresh_data", "entities"],
    )
    def test_options_substeps_present(self, en_strings, de_translations, sub_step):
        for d in (en_strings, de_translations):
            assert sub_step in d["options"]["step"]
            assert "data" in d["options"]["step"][sub_step]

    def test_old_options_steps_removed(self, en_strings, de_translations):
        # v0.9.0: tariff and reimport_quarter step blocks must be gone.
        for d in (en_strings, de_translations):
            assert "tariff" not in d["options"]["step"]
            assert "reimport_quarter" not in d["options"]["step"]

    def test_fr_translations_minimum_keys(self, fr_translations):
        # French has the essentials but may skip detailed help text.
        assert "user" in fr_translations["config"]["step"]
        assert "tariff" in fr_translations["config"]["step"]
        assert "entities" in fr_translations["config"]["step"]


class TestNotesBlockHelper:
    """v0.9.9 — `_notes_block` returns a localized markdown note for
    rate-window-level notes; falls back gracefully when nothing applies."""

    def test_bkw_has_naturemade_warning_in_de(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _notes_block

        class _Hass:
            class config:
                language = "de"

        out = _notes_block("bkw", "2026-04-01", _Hass())
        assert "naturemade" in out.lower()
        assert "⚠" in out  # warning emoji prefix

    def test_bkw_falls_back_when_unknown_locale(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _notes_block

        class _Hass:
            class config:
                language = "xx"  # unknown locale → falls back to de

        out = _notes_block("bkw", "2026-04-01", _Hass())
        assert "naturemade" in out.lower()

    def test_utility_without_notes_returns_empty(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _notes_block

        class _Hass:
            class config:
                language = "en"

        out = _notes_block("ekz", "2026-04-01", _Hass())
        assert out == ""

    def test_unknown_utility_returns_empty(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _notes_block

        out = _notes_block("does_not_exist", "2026-04-01", None)
        assert out == ""

    def test_invalid_date_returns_empty(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _notes_block

        out = _notes_block("bkw", "not-a-date", None)
        assert out == ""

    def test_pick_note_text_locale_priority(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _pick_note_text

        text = {"de": "Hallo", "en": "Hello", "fr": "Bonjour"}
        assert _pick_note_text(text, "fr") == "Bonjour"
        # User locale missing → fallback to de.
        assert _pick_note_text(text, "es") == "Hallo"
        # Empty / missing → None.
        assert _pick_note_text(None, "en") is None
        assert _pick_note_text({}, "en") is None


class TestPickValueLabel:
    """v0.12.0 — value_labels_<lang> lookup for enum dropdowns."""

    def test_returns_locale_label(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _pick_value_label

        decl = {
            "key": "model",
            "type": "enum",
            "values": ["fixpreis", "rmp"],
            "value_labels_de": {"fixpreis": "AEW Fixpreis", "rmp": "RMP"},
            "value_labels_en": {"fixpreis": "AEW Fixed", "rmp": "RMP"},
        }
        assert _pick_value_label(decl, "fixpreis", "de") == "AEW Fixpreis"
        assert _pick_value_label(decl, "fixpreis", "en") == "AEW Fixed"

    def test_falls_back_to_de_then_en(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _pick_value_label

        decl_de_only = {
            "values": ["a"],
            "value_labels_de": {"a": "Eins"},
        }
        # Unknown locale → de fallback.
        assert _pick_value_label(decl_de_only, "a", "fr") == "Eins"

        decl_en_only = {
            "values": ["a"],
            "value_labels_en": {"a": "One"},
        }
        # No de → en fallback.
        assert _pick_value_label(decl_en_only, "a", "fr") == "One"

    def test_falls_back_to_raw_value_when_no_labels(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _pick_value_label

        decl = {"values": ["fixpreis"]}
        assert _pick_value_label(decl, "fixpreis", "de") == "fixpreis"

    def test_unknown_value_returns_value(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _pick_value_label

        decl = {"value_labels_de": {"a": "Eins"}}
        assert _pick_value_label(decl, "z", "de") == "z"


class TestFormatTarifUrlsBlock:
    """v0.12.0 — markdown rendering of rate.tarif_urls (schema v1.2.0)."""

    def test_empty_returns_empty_string(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_tarif_urls_block,
        )

        assert _format_tarif_urls_block([], "de") == ""

    def test_renders_locale_heading_and_label(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_tarif_urls_block,
        )

        urls = [
            {
                "url": "https://example.test/de.pdf",
                "label_de": "Tarifblatt",
                "label_en": "Tariff sheet",
            }
        ]
        de_out = _format_tarif_urls_block(urls, "de")
        assert "Tarifinformationen" in de_out
        assert "[Tarifblatt](https://example.test/de.pdf)" in de_out

        en_out = _format_tarif_urls_block(urls, "en")
        assert "documentation" in en_out.lower()
        assert "[Tariff sheet](https://example.test/de.pdf)" in en_out

    def test_falls_back_to_url_when_no_label(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_tarif_urls_block,
        )

        urls = [{"url": "https://example.test/raw.pdf"}]
        out = _format_tarif_urls_block(urls, "de")
        assert "[https://example.test/raw.pdf](https://example.test/raw.pdf)" in out

    def test_skips_entries_without_url(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_tarif_urls_block,
        )

        urls = [{"label_de": "no url"}]
        # Heading would appear alone → caller-friendly: collapse to "".
        assert _format_tarif_urls_block(urls, "de") == ""


class TestResolveTarifUrls:
    """v0.12.0 — resolver pulls active rate-window's tarif_urls and
    filters by applies_when. Uses synthetic data via load_tariffs override
    is heavy; instead exercise the contract on inputs the resolver short-
    circuits, plus a bundled smoke test."""

    def test_missing_inputs_returns_empty(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _resolve_tarif_urls,
        )

        assert _resolve_tarif_urls(None, "2026-04-01", None) == []
        assert _resolve_tarif_urls("ekz", None, None) == []
        assert _resolve_tarif_urls("ekz", "not-a-date", None) == []

    def test_unknown_utility_returns_empty(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _resolve_tarif_urls,
        )

        assert _resolve_tarif_urls("does_not_exist", "2026-04-01", None) == []

    def test_bundled_ekz_has_at_least_one_url(self):
        # Smoke test on bundled v1.2.0 data: every 2026 rate window has a
        # tarif_urls entry per the upstream migration.
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _resolve_tarif_urls,
        )

        urls = _resolve_tarif_urls("ekz", "2026-06-01", None)
        assert len(urls) >= 1
        assert all(entry.get("url") for entry in urls)

    def test_applies_when_filter_drops_unmatched(self, monkeypatch):
        # Synthetic: patch load_tariffs to return a 1-utility db with a
        # rate window carrying 2 tarif_urls — one gated, one unconditional.
        from custom_components.bfe_rueckliefertarif import config_flow as cf
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb

        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "rates": [
                        {
                            "valid_from": "2026-01-01",
                            "valid_to": None,
                            "settlement_period": "quartal",
                            "power_tiers": [],
                            "tarif_urls": [
                                {"url": "https://example.test/always.pdf"},
                                {
                                    "url": "https://example.test/fixpreis.pdf",
                                    "applies_when": {"model": "fixpreis"},
                                },
                            ],
                        }
                    ],
                }
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)

        # No user_inputs → only the unconditional URL passes.
        out = cf._resolve_tarif_urls("syn", "2026-06-01", None)
        assert [e["url"] for e in out] == ["https://example.test/always.pdf"]

        # With matching user_inputs → both URLs pass.
        out = cf._resolve_tarif_urls("syn", "2026-06-01", {"model": "fixpreis"})
        urls = [e["url"] for e in out]
        assert "https://example.test/always.pdf" in urls
        assert "https://example.test/fixpreis.pdf" in urls

        # With non-matching user_inputs → only unconditional.
        out = cf._resolve_tarif_urls("syn", "2026-06-01", {"model": "rmp"})
        assert [e["url"] for e in out] == ["https://example.test/always.pdf"]
