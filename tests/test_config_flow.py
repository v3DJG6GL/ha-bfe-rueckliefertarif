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
    CONF_INSTALLIERTE_LEISTUNG_KWP,
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
            CONF_INSTALLIERTE_LEISTUNG_KWP: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
            CONF_ABRECHNUNGS_RHYTHMUS: ABRECHNUNGS_RHYTHMUS_QUARTAL,
        }
        data.update(overrides)
        return data

    def test_positive_kw_passes(self):
        assert _validate_tariff(self._base()) == {}

    def test_zero_kw_rejected(self):
        errors = _validate_tariff(self._base(installierte_leistung_kwp=0.0))
        assert errors == {CONF_INSTALLIERTE_LEISTUNG_KWP: "kw_required"}

    def test_negative_kw_rejected(self):
        errors = _validate_tariff(self._base(installierte_leistung_kwp=-1.0))
        assert errors == {CONF_INSTALLIERTE_LEISTUNG_KWP: "kw_required"}

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
        ["user", "tariff_pick", "tariff_details", "entities"],
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
        "field,step",
        [
            (CONF_VALID_FROM, "tariff_pick"),
            (CONF_INSTALLIERTE_LEISTUNG_KWP, "tariff_pick"),
            (CONF_EIGENVERBRAUCH_AKTIVIERT, "tariff_details"),
            (CONF_HKN_AKTIVIERT, "tariff_details"),
        ],
    )
    def test_tariff_field_label_and_help(self, en_strings, de_translations, field, step):
        for d in (en_strings, de_translations):
            assert field in d["config"]["step"][step]["data"]
            assert field in d["config"]["step"][step]["data_description"]

    def test_abrechnungs_rhythmus_form_field_dropped(
        self, en_strings, de_translations
    ):
        """v0.9.8 — billing toggle is gone from every form (#9). Translations
        must not still ship the field labels or selector options."""
        for d in (en_strings, de_translations):
            for step in ("tariff_pick", "tariff_details"):
                assert CONF_ABRECHNUNGS_RHYTHMUS not in d["config"]["step"][step]["data"]
            for step in ("add_new_row", "edit_row"):
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
                "manage_history",
                "recompute_history",
                "refresh_data",
                "entities",
            }

    @pytest.mark.parametrize(
        "sub_step",
        ["recompute_history", "refresh_data", "entities"],
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
        assert "tariff_pick" in fr_translations["config"]["step"]
        assert "tariff_details" in fr_translations["config"]["step"]
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

    def test_renders_blockquote_with_severity_label_de(self):
        # v0.12.1 — note rendered as markdown blockquote with locale severity.
        from custom_components.bfe_rueckliefertarif.config_flow import _notes_block

        class _Hass:
            class config:
                language = "de"

        out = _notes_block("bkw", "2026-04-01", _Hass())
        # Blockquote prefix + emoji + bold severity label.
        assert "> ⚠️ **Warnung:**" in out

    def test_renders_blockquote_with_severity_label_en(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _notes_block

        class _Hass:
            class config:
                language = "en"

        out = _notes_block("bkw", "2026-04-01", _Hass())
        assert "> ⚠️ **Warning:**" in out


class TestChangeAdvisory:
    """v0.12.1 — gate-change advisory banner shown on first re-submit
    after the user changes valid_from in the ConfigFlow tariff step."""

    def test_empty_when_not_shown(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_change_advisory,
        )

        assert _format_change_advisory(False, "de") == ""

    def test_locale_picks(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_change_advisory,
        )

        de = _format_change_advisory(True, "de")
        en = _format_change_advisory(True, "en")
        fr = _format_change_advisory(True, "fr")
        assert "Datum geändert" in de
        assert "date changed" in en.lower()
        assert "modifiée" in fr
        # Each starts with the ℹ️ emoji prefix for visual distinction.
        for s in (de, en, fr):
            assert s.startswith("ℹ️")

    def test_unknown_locale_falls_back_to_en(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_change_advisory,
        )

        out = _format_change_advisory(True, "xx")
        assert "date changed" in out.lower()


class TestEditRowWizard:
    """v0.12.1 — manage-history wizard: Step 1 picks utility + valid_from,
    Step 2 captures kW / EV / HKN / user_inputs and saves."""

    def _make_flow(self, options, data=None):
        from types import MappingProxyType, SimpleNamespace
        from unittest.mock import AsyncMock, MagicMock

        from custom_components.bfe_rueckliefertarif.config_flow import (
            BfeRuecklieferTarifOptionsFlow,
        )

        flow = BfeRuecklieferTarifOptionsFlow.__new__(BfeRuecklieferTarifOptionsFlow)
        flow.hass = MagicMock()
        flow.hass.async_add_executor_job = AsyncMock(return_value=None)
        flow_entry = SimpleNamespace(
            entry_id="t",
            data=data or {"stromnetzeinspeisung_kwh": "sensor.foo"},
            options=MappingProxyType(options),
        )
        flow.handler = "t"
        flow.hass.config_entries.async_get_entry.return_value = flow_entry
        flow.hass.config_entries.async_get_known_entry.return_value = flow_entry
        return flow

    @pytest.mark.asyncio
    async def test_add_pick_then_save(self):
        from custom_components.bfe_rueckliefertarif.const import (
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_ENERGIEVERSORGER,
            CONF_HKN_AKTIVIERT,
            CONF_INSTALLIERTE_LEISTUNG_KWP,
            OPT_CONFIG_HISTORY,
        )

        existing = [
            {"valid_from": "2026-02-01", "valid_to": None,
             "config": {
                 CONF_ENERGIEVERSORGER: "ekz",
                 CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
                 CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                 CONF_HKN_AKTIVIERT: True,
                 "abrechnungs_rhythmus": "QUARTAL",
             }},
        ]
        flow = self._make_flow({OPT_CONFIG_HISTORY: existing})

        # Step 1 (add picker)
        step1 = await flow.async_step_add_pick_row()
        assert step1["type"].name in ("FORM", "form")
        assert step1["step_id"] == "add_pick_row"

        # v0.13.0 — kW lives on Step 1 alongside utility + valid_from.
        step1_submit = await flow.async_step_add_pick_row(
            {
                "valid_from": "2026-04-01",
                CONF_ENERGIEVERSORGER: "ekz",
                CONF_INSTALLIERTE_LEISTUNG_KWP: 12.0,
            }
        )
        # Should auto-route to Step 2.
        assert step1_submit["step_id"] == "add_new_row"

        # Step 2 (details + save) — no kW field anymore; carried from Step 1.
        step2_submit = await flow.async_step_add_new_row({
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
        })
        # Save returns to manage_history menu.
        assert step2_submit["type"].name in ("MENU", "menu")
        # Verify the new record landed in the entry options.
        flow.hass.config_entries.async_update_entry.assert_called_once()
        new_options = flow.hass.config_entries.async_update_entry.call_args.kwargs[
            "options"
        ]
        history = new_options[OPT_CONFIG_HISTORY]
        assert history[-1]["valid_from"] == "2026-04-01"
        assert history[-1]["config"][CONF_INSTALLIERTE_LEISTUNG_KWP] == 12.0

    @pytest.mark.asyncio
    async def test_edit_pick_then_save(self):
        from custom_components.bfe_rueckliefertarif.const import (
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_ENERGIEVERSORGER,
            CONF_HKN_AKTIVIERT,
            CONF_INSTALLIERTE_LEISTUNG_KWP,
            OPT_CONFIG_HISTORY,
        )

        existing = [
            {"valid_from": "2026-02-01", "valid_to": None,
             "config": {
                 CONF_ENERGIEVERSORGER: "ekz",
                 CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
                 CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                 CONF_HKN_AKTIVIERT: True,
                 "abrechnungs_rhythmus": "QUARTAL",
             }},
        ]
        flow = self._make_flow({OPT_CONFIG_HISTORY: existing})

        # Simulate the menu dispatch for "edit_pick_row_0".
        flow._editing_idx = 0
        step1 = await flow.async_step_edit_pick_row()
        assert step1["step_id"] == "edit_pick_row"

        # v0.13.0 — kW lives on Step 1; tweak it here and Step 2 inherits.
        step1_submit = await flow.async_step_edit_pick_row(
            {
                "valid_from": "2026-02-01",
                CONF_ENERGIEVERSORGER: "ekz",
                CONF_INSTALLIERTE_LEISTUNG_KWP: 15.5,
            }
        )
        assert step1_submit["step_id"] == "edit_row"

        # Step 2: no kW field anymore. Submit → save replaces history[0].
        step2_submit = await flow.async_step_edit_row({
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: True,
            "delete": False,
        })
        assert step2_submit["type"].name in ("MENU", "menu")
        flow.hass.config_entries.async_update_entry.assert_called_once()
        new_options = flow.hass.config_entries.async_update_entry.call_args.kwargs[
            "options"
        ]
        history = new_options[OPT_CONFIG_HISTORY]
        assert len(history) == 1
        assert history[0]["config"][CONF_INSTALLIERTE_LEISTUNG_KWP] == 15.5

    @pytest.mark.asyncio
    async def test_edit_row_save_triggers_reload(self):
        # v0.16.1 — Issue 1: editing the active rate window via
        # manage_history must trigger an entry reload so the coordinator
        # re-runs auto-import and the recompute notification fires.
        # async_update_entry alone does not reload; we add an explicit
        # async_reload via async_create_task.
        from custom_components.bfe_rueckliefertarif.const import (
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_ENERGIEVERSORGER,
            CONF_HKN_AKTIVIERT,
            CONF_INSTALLIERTE_LEISTUNG_KWP,
            OPT_CONFIG_HISTORY,
        )

        existing = [
            {"valid_from": "2026-02-01", "valid_to": None,
             "config": {
                 CONF_ENERGIEVERSORGER: "ekz",
                 CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
                 CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                 CONF_HKN_AKTIVIERT: True,
                 "abrechnungs_rhythmus": "QUARTAL",
             }},
        ]
        flow = self._make_flow({OPT_CONFIG_HISTORY: existing})
        flow._editing_idx = 0
        await flow.async_step_edit_pick_row({
            "valid_from": "2026-02-01",
            CONF_ENERGIEVERSORGER: "ekz",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 15.5,
        })
        await flow.async_step_edit_row({
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: True,
            "delete": False,
        })
        # async_update_entry was called for the persistence...
        flow.hass.config_entries.async_update_entry.assert_called_once()
        # ...and async_create_task was called for the reload trigger.
        flow.hass.async_create_task.assert_called_once()
        # The arg to async_create_task is the reload coroutine; we don't
        # need to await it (Mock returns a sentinel) — just assert the
        # reload was scheduled against this entry's id.
        flow.hass.config_entries.async_reload.assert_called_with("t")

    @pytest.mark.asyncio
    async def test_edit_row_delete_triggers_reload(self):
        # Same reload contract on the delete branch.
        from custom_components.bfe_rueckliefertarif.const import (
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_ENERGIEVERSORGER,
            CONF_HKN_AKTIVIERT,
            CONF_INSTALLIERTE_LEISTUNG_KWP,
            OPT_CONFIG_HISTORY,
        )

        existing = [
            {"valid_from": "2026-01-01", "valid_to": "2026-04-01",
             "config": {
                 CONF_ENERGIEVERSORGER: "ekz",
                 CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
                 CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                 CONF_HKN_AKTIVIERT: False,
                 "abrechnungs_rhythmus": "QUARTAL",
             }},
            {"valid_from": "2026-04-01", "valid_to": None,
             "config": {
                 CONF_ENERGIEVERSORGER: "ekz",
                 CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
                 CONF_EIGENVERBRAUCH_AKTIVIERT: True,
                 CONF_HKN_AKTIVIERT: True,
                 "abrechnungs_rhythmus": "QUARTAL",
             }},
        ]
        flow = self._make_flow({OPT_CONFIG_HISTORY: existing})
        flow._editing_idx = 0
        await flow.async_step_edit_pick_row({
            "valid_from": "2026-01-01",
            CONF_ENERGIEVERSORGER: "ekz",
            CONF_INSTALLIERTE_LEISTUNG_KWP: 8.0,
        })
        await flow.async_step_edit_row({"delete": True})
        flow.hass.config_entries.async_update_entry.assert_called_once()
        flow.hass.async_create_task.assert_called_once()
        flow.hass.config_entries.async_reload.assert_called_with("t")


class TestPickValueLabel:
    """v0.12.0 — value_labels_<lang> lookup for enum dropdowns."""

    def test_returns_locale_label(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import pick_value_label

        decl = {
            "key": "model",
            "type": "enum",
            "values": ["fixpreis", "rmp"],
            "value_labels_de": {"fixpreis": "AEW Fixpreis", "rmp": "RMP"},
            "value_labels_en": {"fixpreis": "AEW Fixed", "rmp": "RMP"},
        }
        assert pick_value_label(decl, "fixpreis", "de") == "AEW Fixpreis"
        assert pick_value_label(decl, "fixpreis", "en") == "AEW Fixed"

    def test_falls_back_to_de_then_en(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import pick_value_label

        decl_de_only = {
            "values": ["a"],
            "value_labels_de": {"a": "Eins"},
        }
        # Unknown locale → de fallback.
        assert pick_value_label(decl_de_only, "a", "fr") == "Eins"

        decl_en_only = {
            "values": ["a"],
            "value_labels_en": {"a": "One"},
        }
        # No de → en fallback.
        assert pick_value_label(decl_en_only, "a", "fr") == "One"

    def test_falls_back_to_raw_value_when_no_labels(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import pick_value_label

        decl = {"values": ["fixpreis"]}
        assert pick_value_label(decl, "fixpreis", "de") == "fixpreis"

    def test_unknown_value_returns_value(self):
        from custom_components.bfe_rueckliefertarif.tariffs_db import pick_value_label

        decl = {"value_labels_de": {"a": "Eins"}}
        assert pick_value_label(decl, "z", "de") == "z"


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

    def test_falls_back_to_url_derived_label_for_pdf(self):
        # v0.12.1 — when no curator label, build "📄 PDF · domain".
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_tarif_urls_block,
        )

        urls = [{"url": "https://www.example.test/raw.pdf"}]
        out = _format_tarif_urls_block(urls, "de")
        assert "📄 PDF · example.test" in out
        # Link still points at the URL.
        assert "(https://www.example.test/raw.pdf)" in out

    def test_falls_back_to_url_derived_label_for_html(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_tarif_urls_block,
        )

        urls = [{"url": "https://aew.ch/foo"}]
        out_de = _format_tarif_urls_block(urls, "de")
        assert "🌐 Webseite · aew.ch" in out_de
        out_en = _format_tarif_urls_block(urls, "en")
        assert "🌐 Webpage · aew.ch" in out_en

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
        # config_flow.py imports load_tariffs at module load, so we must
        # patch both names. Same for find_active (used inside the helper).
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        monkeypatch.setattr(cf, "load_tariffs", lambda: synthetic)

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


class TestSelfConsumptionRelevant:
    """v0.13.0 — A2.1 data-gate for the self-consumption form field.

    True iff at least one rule the resolver would consume actually
    distinguishes on ``self_consumption`` for this (utility, kW, date).
    """

    def test_relevant_when_federal_rule_distinguishes(self, monkeypatch):
        """30-150 kW band of bundled federal_minimum has a 'mit/ohne EV' split."""
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            self_consumption_relevant,
        )

        # Bundled federal_minimum has self_consumption=true/false for
        # the 30–<150 kW band. So at kW=50, EV bool changes which rule
        # is selected.
        assert self_consumption_relevant("ekz", "2026-04-01", 50.0) is True

    def test_irrelevant_when_federal_rule_uses_null(self, monkeypatch):
        """≥150 kW band uses self_consumption=null → field is inert."""
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            self_consumption_relevant,
        )

        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2026-01-01",
                "valid_to": None,
                "rules": [
                    {"kw_min": 0, "kw_max": None, "self_consumption": None,
                     "min_rp_kwh": 4.0},
                ],
            }],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "homepage": "https://example.test",
                    "rates": [],
                }
            },
        }
        # Helper lives in tariffs_db; patch its load_tariffs.
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        assert self_consumption_relevant("syn", "2026-04-01", 200.0) is False

    def test_irrelevant_when_no_federal_record(self, monkeypatch):
        """valid_from before any federal_minimum → no rule path. No
        cap_rules either → False (inert)."""
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            self_consumption_relevant,
        )

        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2026-01-01",
                "valid_to": None,
                "rules": [
                    {"kw_min": 0, "kw_max": None, "self_consumption": True,
                     "min_rp_kwh": 4.0},
                ],
            }],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "homepage": "https://example.test",
                    "rates": [
                        {
                            "valid_from": "2025-01-01",
                            "valid_to": "2026-01-01",
                            "settlement_period": "quartal",
                            "cap_mode": False,
                            "power_tiers": [
                                {"kw_min": 0, "kw_max": None,
                                 "base_model": "fixed_flat",
                                 "fixed_rp_kwh": 8.0,
                                 "hkn_rp_kwh": 0.0,
                                 "hkn_structure": "none"},
                            ],
                        }
                    ],
                }
            },
        }
        # Helper lives in tariffs_db; patch its load_tariffs.
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        # 2025-04-01 is before the only federal_minimum record (2026-01-01).
        assert self_consumption_relevant("syn", "2025-04-01", 10.0) is False

    def test_relevant_via_cap_rules(self, monkeypatch):
        """Federal rule has self_consumption=null but cap_rules distinguish
        → field IS relevant via the cap-rule path."""
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb
        from custom_components.bfe_rueckliefertarif.tariffs_db import (
            self_consumption_relevant,
        )

        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [{
                "valid_from": "2026-01-01",
                "valid_to": None,
                "rules": [
                    {"kw_min": 0, "kw_max": None, "self_consumption": None,
                     "min_rp_kwh": 4.0},
                ],
            }],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "homepage": "https://example.test",
                    "rates": [
                        {
                            "valid_from": "2026-01-01",
                            "valid_to": None,
                            "settlement_period": "quartal",
                            "cap_mode": True,
                            "cap_rules": [
                                {"kw_min": 0, "kw_max": None,
                                 "self_consumption": True, "cap_rp_kwh": 12.0},
                                {"kw_min": 0, "kw_max": None,
                                 "self_consumption": False, "cap_rp_kwh": 8.0},
                            ],
                            "power_tiers": [
                                {"kw_min": 0, "kw_max": None,
                                 "base_model": "fixed_flat",
                                 "fixed_rp_kwh": 8.0,
                                 "hkn_rp_kwh": 0.0,
                                 "hkn_structure": "none"},
                            ],
                        }
                    ],
                }
            },
        }
        # Helper lives in tariffs_db; patch its load_tariffs.
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        assert self_consumption_relevant("syn", "2026-04-01", 10.0) is True


class TestFindTierDryRun:
    """v0.13.0 — A2.3 / O4 defensive check at form submit. AEW kW=10 +
    'Referenzmarktpreis' is the canonical hole: no covering tier."""

    def test_resolvable_combination_passes(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _find_tier_dry_run,
        )
        # AEW kW=10 + AEW Fixpreis matches tier 0 (kw 0-30, fixpreis).
        assert _find_tier_dry_run(
            "aew", "2026-04-01", 10.0,
            {"aew_fixpreis_rmp": "AEW Fixpreis"},
        ) is True

    def test_aew_kw10_rmp_combination_rejected(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _find_tier_dry_run,
        )
        # The canonical AEW hole: kW=10 + Referenzmarktpreis covers
        # neither tier 0 (clause mismatch) nor tier 1 (kW out of range).
        assert _find_tier_dry_run(
            "aew", "2026-04-01", 10.0,
            {"aew_fixpreis_rmp": "Referenzmarktpreis"},
        ) is False

    def test_no_rate_window_returns_true_permissive(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _find_tier_dry_run,
        )
        # When no rate window covers the date, the no_active_rate
        # error path handles it; the dry-run is permissive (True) so
        # we don't double-error on the same condition.
        assert _find_tier_dry_run(
            "aew", "1999-04-01", 10.0, None,
        ) is True


class TestHknPositiveWhitelist:
    """v0.13.0 — A2.2 the HKN toggle renders only when the active rate
    window's hkn_structure is exactly ``additive_optin``. Bundled, none,
    or lookup-failure (None) all hide the field."""

    @pytest.mark.parametrize(
        "structure,expected_visible",
        [
            ("additive_optin", True),
            ("bundled", False),
            ("none", False),
            (None, False),
            ("unknown_future_value", False),
        ],
    )
    def test_gate(self, structure, expected_visible):
        from custom_components.bfe_rueckliefertarif.config_flow import _tariff_schema

        schema = _tariff_schema({}, hkn_structure=structure)
        rendered_keys = {str(k) for k in schema.schema}
        assert (CONF_HKN_AKTIVIERT in rendered_keys) is expected_visible


class TestKwAwareUserInputFiltering:
    """v0.14.0 — _add_user_input_fields_namespaced filters enum/boolean
    candidate values via constraint-aware probing of _find_tier_dry_run.
    Impossible (kW × user_input) combos never reach the form."""

    def _enum_options(self, schema_dict: dict, key: str) -> list[str]:
        """Pull the rendered enum option values for ``key`` out of the
        schema dict the helper builds."""
        from homeassistant.helpers import selector as ha_selector

        for k, v in schema_dict.items():
            if str(k) == key:
                assert isinstance(v, ha_selector.SelectSelector)
                return [opt["value"] for opt in v.config["options"]]
        raise KeyError(f"key {key!r} not in schema_dict")

    def _aew_2026_decl(self) -> dict:
        return {
            "key": "aew_fixpreis_rmp",
            "type": "enum",
            "default": "AEW Fixpreis",
            "values": ["AEW Fixpreis", "Referenzmarktpreis"],
        }

    def test_aew_kw31_filters_to_rmp_only(self):
        # AEW 2026: tier 0 covers [0, 30) with "AEW Fixpreis"; tier 1
        # covers [30, 3000) with "Referenzmarktpreis". At kW=31 only tier 1
        # resolves, so "AEW Fixpreis" must be filtered out.
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _add_user_input_fields_namespaced,
        )

        schema_dict: dict = {}
        _add_user_input_fields_namespaced(
            schema_dict,
            (self._aew_2026_decl(),),
            {"aew_fixpreis_rmp": "AEW Fixpreis"},
            "de",
            gate_utility="aew",
            gate_valid_from="2026-04-01",
            gate_kw=31.0,
        )
        assert self._enum_options(schema_dict, "aew_fixpreis_rmp") == [
            "Referenzmarktpreis"
        ]

    def test_aew_kw15_filters_to_fixpreis_only(self):
        # At kW=15 only tier 0 resolves (kw_max=30 excludes tier 1).
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _add_user_input_fields_namespaced,
        )

        schema_dict: dict = {}
        _add_user_input_fields_namespaced(
            schema_dict,
            (self._aew_2026_decl(),),
            {"aew_fixpreis_rmp": "AEW Fixpreis"},
            "de",
            gate_utility="aew",
            gate_valid_from="2026-04-01",
            gate_kw=15.0,
        )
        assert self._enum_options(schema_dict, "aew_fixpreis_rmp") == [
            "AEW Fixpreis"
        ]

    def test_no_gate_args_no_filtering(self):
        # Backwards compat: caller without gate args sees all candidates.
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _add_user_input_fields_namespaced,
        )

        schema_dict: dict = {}
        _add_user_input_fields_namespaced(
            schema_dict,
            (self._aew_2026_decl(),),
            {"aew_fixpreis_rmp": "AEW Fixpreis"},
            "de",
        )
        assert self._enum_options(schema_dict, "aew_fixpreis_rmp") == [
            "AEW Fixpreis",
            "Referenzmarktpreis",
        ]

    def test_filter_empty_omits_field(self, monkeypatch):
        # Synthetic: a utility whose only tier is gated on a value not in
        # the decl's `values` list. Filter result is empty → field is
        # omitted entirely (page-level no_matching_tier surfaces it).
        from custom_components.bfe_rueckliefertarif import config_flow as cf
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb

        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "rates": [{
                        "valid_from": "2026-01-01",
                        "valid_to": None,
                        "settlement_period": "quartal",
                        "power_tiers": [{
                            "kw_min": 0.0,
                            "kw_max": 100.0,
                            "base_model": "fixed_flat",
                            "fixed_rp_kwh": 8.0,
                            "applies_when": {"flavour": "unreachable"},
                        }],
                        "user_inputs": [{
                            "key": "flavour",
                            "type": "enum",
                            "default": "a",
                            "values": ["a", "b"],
                        }],
                    }],
                }
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        monkeypatch.setattr(cf, "load_tariffs", lambda: synthetic)

        decl = synthetic["utilities"]["syn"]["rates"][0]["user_inputs"][0]
        schema_dict: dict = {}
        cf._add_user_input_fields_namespaced(
            schema_dict, (decl,), {"flavour": "a"}, "de",
            gate_utility="syn",
            gate_valid_from="2026-04-01",
            gate_kw=10.0,
        )
        rendered_keys = {str(k) for k in schema_dict}
        assert "flavour" not in rendered_keys

    def test_constraint_aware_2field_keeps_both_when_each_reachable(self, monkeypatch):
        # Two user_input fields: A (enum: x, y), B (boolean). Tiers:
        #   tier 0 needs A=x AND B=true
        #   tier 1 needs A=y AND B=false
        # At a kW where both tiers cover, both candidate values for A
        # remain (x reachable via B=True; y reachable via B=False), and
        # both bool values for B remain.
        from custom_components.bfe_rueckliefertarif import config_flow as cf
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb

        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "rates": [{
                        "valid_from": "2026-01-01",
                        "valid_to": None,
                        "settlement_period": "quartal",
                        "power_tiers": [
                            {
                                "kw_min": 0.0, "kw_max": 100.0,
                                "base_model": "fixed_flat",
                                "fixed_rp_kwh": 8.0,
                                "applies_when": {"a_choice": "x", "b_flag": True},
                            },
                            {
                                "kw_min": 0.0, "kw_max": 100.0,
                                "base_model": "fixed_flat",
                                "fixed_rp_kwh": 9.0,
                                "applies_when": {"a_choice": "y", "b_flag": False},
                            },
                        ],
                        "user_inputs": [
                            {"key": "a_choice", "type": "enum",
                             "default": "x", "values": ["x", "y"]},
                            {"key": "b_flag", "type": "boolean", "default": True},
                        ],
                    }],
                }
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        monkeypatch.setattr(cf, "load_tariffs", lambda: synthetic)

        decls = tuple(
            synthetic["utilities"]["syn"]["rates"][0]["user_inputs"]
        )
        schema_dict: dict = {}
        cf._add_user_input_fields_namespaced(
            schema_dict, decls,
            {"a_choice": "x", "b_flag": True}, "de",
            gate_utility="syn", gate_valid_from="2026-04-01", gate_kw=10.0,
        )
        # Both A values reachable.
        assert self._enum_options(schema_dict, "a_choice") == ["x", "y"]
        # B rendered (bool selector, both values reachable across siblings).
        rendered_keys = {str(k) for k in schema_dict}
        assert "b_flag" in rendered_keys

    def test_constraint_aware_2field_drops_unreachable(self, monkeypatch):
        # Same fixture as above but with tier 1 removed: only tier 0
        # remains, so A=y is unreachable for ANY combination of B.
        from custom_components.bfe_rueckliefertarif import config_flow as cf
        from custom_components.bfe_rueckliefertarif import tariffs_db as tdb

        synthetic = {
            "schema_version": "1.2.0",
            "last_updated": "2026-01-01",
            "federal_minimum": [],
            "utilities": {
                "syn": {
                    "name_de": "Syn",
                    "rates": [{
                        "valid_from": "2026-01-01",
                        "valid_to": None,
                        "settlement_period": "quartal",
                        "power_tiers": [{
                            "kw_min": 0.0, "kw_max": 100.0,
                            "base_model": "fixed_flat",
                            "fixed_rp_kwh": 8.0,
                            "applies_when": {"a_choice": "x", "b_flag": True},
                        }],
                        "user_inputs": [
                            {"key": "a_choice", "type": "enum",
                             "default": "x", "values": ["x", "y"]},
                            {"key": "b_flag", "type": "boolean", "default": True},
                        ],
                    }],
                }
            },
        }
        monkeypatch.setattr(tdb, "load_tariffs", lambda: synthetic)
        monkeypatch.setattr(cf, "load_tariffs", lambda: synthetic)

        decls = tuple(
            synthetic["utilities"]["syn"]["rates"][0]["user_inputs"]
        )
        schema_dict: dict = {}
        cf._add_user_input_fields_namespaced(
            schema_dict, decls,
            {"a_choice": "x", "b_flag": True}, "de",
            gate_utility="syn", gate_valid_from="2026-04-01", gate_kw=10.0,
        )
        # Only A=x reachable (A=y has no tier under any B combination).
        assert self._enum_options(schema_dict, "a_choice") == ["x"]

    def test_text_field_passes_through_unfiltered(self):
        # Non-gate-affecting types (text/number) are not filtered: they
        # pass through with the unfiltered candidates list (i.e. the
        # helper renders them via the bool/enum branches it has, or skips
        # if neither). Smoke test that gates don't crash on a synthetic
        # text decl (the helper currently renders only enum + boolean,
        # so a text decl is silently skipped — assert no exception, no
        # field added).
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _add_user_input_fields_namespaced,
        )

        schema_dict: dict = {}
        _add_user_input_fields_namespaced(
            schema_dict,
            ({"key": "freeform", "type": "text", "default": ""},),
            {"freeform": ""}, "de",
            gate_utility="aew",
            gate_valid_from="2026-04-01",
            gate_kw=10.0,
        )
        assert "freeform" not in {str(k) for k in schema_dict}


# ----- v0.17.0 — Issue 6.3: user_inputs help block --------------------------


class TestUserInputsHelpBlock:
    """v0.17.0 — surface ``description_de/fr/en`` from rate-window
    user_input declarations as Markdown help text in config flow forms.
    """

    def test_returns_empty_when_no_decls(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _user_inputs_help_block,
        )
        assert _user_inputs_help_block(None, "de") == ""
        assert _user_inputs_help_block([], "de") == ""

    def test_returns_empty_when_no_decl_has_description(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _user_inputs_help_block,
        )
        decls = [{"key": "k", "type": "boolean", "label_de": "Wahl"}]
        assert _user_inputs_help_block(decls, "de") == ""

    def test_renders_label_and_description_de(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _user_inputs_help_block,
        )
        decls = [
            {
                "key": "regio_top40_opted_in",
                "type": "boolean",
                "label_de": "Wahltarif TOP-40 abonniert",
                "description_de": "Voraussetzungen: dauerhafte Begrenzung.",
            },
        ]
        block = _user_inputs_help_block(decls, "de")
        assert "Versorger-spezifische Optionen" in block
        assert (
            "**Wahltarif TOP-40 abonniert:** Voraussetzungen: dauerhafte "
            "Begrenzung."
        ) in block

    def test_skips_decls_without_description_field(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _user_inputs_help_block,
        )
        decls = [
            {
                "key": "with_desc",
                "label_de": "Mit",
                "description_de": "Beschreibung.",
            },
            {"key": "without_desc", "label_de": "Ohne"},
        ]
        block = _user_inputs_help_block(decls, "de")
        assert "Mit" in block
        assert "Ohne" not in block

    def test_falls_back_label_when_no_label_de(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _user_inputs_help_block,
        )
        decls = [
            {
                "key": "raw_key",
                "description_de": "Nur Beschreibung.",
            },
        ]
        block = _user_inputs_help_block(decls, "de")
        assert "**raw_key:** Nur Beschreibung." in block


# ----- v0.17.0 — Issue 7: refresh-prices notification redesign --------------


def _refresh_result(**overrides):
    base = {
        "available": [],
        "newly_imported": [],
        "tariffs_refreshed": True,
        "tariffs_data_version": "1.4.1",
        "tariffs_diff": None,
        "tariffs_error": None,
    }
    base.update(overrides)
    return base


class TestRefreshNotificationRendering:
    """v0.17.0 — Issue 7: per-utility added/modified/added-rate-window
    sections in the refresh-prices notification body. Title is bilingual
    static; sections are English to match the recompute notification.
    """

    def test_bfe_section_always_present(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _render_refresh_notification,
        )
        body = _render_refresh_notification(
            _refresh_result(
                available=[1, 2, 3],
                tariffs_diff={
                    "added_utilities": [], "removed_utilities": [],
                    "added_rate_windows": [], "modified_rate_windows": [],
                    "data_version_changed": None, "no_changes": True,
                },
            )
        )
        assert "## Reference market prices (SFOE)" in body

    def test_no_changes_message_when_diff_empty(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _render_refresh_notification,
        )
        body = _render_refresh_notification(
            _refresh_result(
                tariffs_diff={
                    "added_utilities": [], "removed_utilities": [],
                    "added_rate_windows": [], "modified_rate_windows": [],
                    "data_version_changed": None, "no_changes": True,
                },
            )
        )
        assert "## Tariff data" in body
        assert "No changes since last refresh" in body
        assert "### Newly added utilities" not in body

    def test_added_utilities_section(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _render_refresh_notification,
        )
        body = _render_refresh_notification(
            _refresh_result(
                tariffs_diff={
                    "added_utilities": [{
                        "key": "ekz",
                        "name": "EKZ",
                        "rate_window_dates": ["2026-01-01", "2025-01-01"],
                    }],
                    "removed_utilities": [],
                    "added_rate_windows": [],
                    "modified_rate_windows": [],
                    "data_version_changed": None,
                    "no_changes": False,
                },
            )
        )
        assert "### Newly added utilities" in body
        assert "- EKZ" in body
        assert "- 2026" in body
        assert "- 2025" in body

    def test_modified_rate_windows_section(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _render_refresh_notification,
        )
        body = _render_refresh_notification(
            _refresh_result(
                tariffs_diff={
                    "added_utilities": [],
                    "removed_utilities": [],
                    "added_rate_windows": [],
                    "modified_rate_windows": [{
                        "key": "regio_energie_solothurn",
                        "name": "Regio Energie Solothurn",
                        "rate_window_dates": ["2024-01-01"],
                    }],
                    "data_version_changed": None,
                    "no_changes": False,
                },
            )
        )
        assert "### Modified rate windows" in body
        assert "- Regio Energie Solothurn" in body
        assert "- 2024" in body

    def test_year_grouping_collapses_single_window(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_rate_window_dates,
        )
        # Single window per year → just year
        assert _format_rate_window_dates(["2026-01-01"]) == ["2026"]
        assert _format_rate_window_dates(
            ["2026-01-01", "2025-01-01"]
        ) == ["2026", "2025"]

    def test_year_expands_when_multiple_windows_in_year(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _format_rate_window_dates,
        )
        # Two windows in 2026 → both dates
        assert _format_rate_window_dates(
            ["2026-01-01", "2026-07-01", "2025-01-01"]
        ) == ["2026-01-01", "2026-07-01", "2025"]

    def test_fetch_failure_shows_error(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _render_refresh_notification,
        )
        body = _render_refresh_notification(
            _refresh_result(
                tariffs_refreshed=False,
                tariffs_error="HTTP 503",
                tariffs_diff=None,
            )
        )
        assert "Refresh failed: HTTP 503" in body
        assert "Using cached tariffs." in body
