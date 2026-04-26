"""Config flow for BFE Rückliefertarif (v0.5).

Three-step flow:
1. ``user`` — clickable menu of utilities (one click advances). Utility list
   comes from ``data/tariffs.json`` so adding a utility is a JSON-only change.
2. ``tariff`` — 4 personal-input fields (kW, Eigenverbrauch, HKN opt-in,
   Abrechnungs-Rhythmus). Utility-published values (HKN rate, cap_mode,
   fixed price) come from JSON and are NOT user-editable.
3. ``entities`` — 3 entity-wiring fields.

Plus an Options Flow that re-exposes the tariff step.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector

from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_NAMENSPRAEFIX,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    DOMAIN,
    OPT_HKN_OPTIN_HISTORY,
    OPT_PLANT_HISTORY,
)
from .tariffs_db import list_utility_keys, load_tariffs


async def _async_warm_cache(hass) -> None:
    """Pre-load tariffs.json via executor so the in-event-loop callers below
    hit the lru_cache instead of triggering HA's blocking-I/O detector.

    Cheap to call repeatedly — after the first hit the cache is populated
    and the executor job is a no-op dict return.
    """
    await hass.async_add_executor_job(load_tariffs)

if TYPE_CHECKING:
    from homeassistant.data_entry_flow import FlowResult


# Locale-aware data-source URLs surfaced via description_placeholders.
# Hassfest forbids URLs in translation strings — must be runtime-injected.
_AGENCY_URLS: dict[str, str] = {
    "de": "https://www.bfe.admin.ch/bfe/de/home/foerderung/erneuerbare-energien/einspeiseverguetung.html",
    "en": "https://www.bfe.admin.ch/bfe/en/home/promotion/renewable-energy/feed-in-remuneration-at-cost.html",
}
_OPENDATA_URLS: dict[str, str] = {
    "de": "https://opendata.swiss/de/dataset/referenz-marktpreise-gemass-art-15-enfv",
    "en": "https://opendata.swiss/en/dataset/referenz-marktpreise-gemass-art-15-enfv",
    "fr": "https://opendata.swiss/fr/dataset/referenz-marktpreise-gemass-art-15-enfv",
}
# Fedlex (Swiss federal law portal) — deep-linked to the relevant article:
# - EnV SR 730.01 (ELI 2017/763), Art. 12 Abs. 1bis: Mindestvergütung floors.
# - StromVV SR 734.71 (ELI 2008/226), Art. 4 Abs. 3 Bst. e: cap formula.
# Both in force 1.1.2026 via AS 2025 138 / AS 2025 139.
_FEDLEX_ENV_URLS: dict[str, str] = {
    "de": "https://www.fedlex.admin.ch/eli/cc/2017/763/de#art_12",
    "en": "https://www.fedlex.admin.ch/eli/cc/2017/763/de#art_12",
    "fr": "https://www.fedlex.admin.ch/eli/cc/2017/763/fr#art_12",
}
_FEDLEX_STROMVV_URLS: dict[str, str] = {
    "de": "https://www.fedlex.admin.ch/eli/cc/2008/226/de#art_4",
    "en": "https://www.fedlex.admin.ch/eli/cc/2008/226/de#art_4",
    "fr": "https://www.fedlex.admin.ch/eli/cc/2008/226/fr#art_4",
}


def _source_links(hass) -> dict[str, str]:
    """Return locale-correct data-source URLs for description_placeholders."""
    lang = (getattr(hass.config, "language", None) or "en").split("-")[0].lower()
    return {
        "agency_url": _AGENCY_URLS.get(lang, _AGENCY_URLS["en"]),
        "opendata_url": _OPENDATA_URLS.get(lang, _OPENDATA_URLS["en"]),
        "env_url": _FEDLEX_ENV_URLS.get(lang, _FEDLEX_ENV_URLS["en"]),
        "stromvv_url": _FEDLEX_STROMVV_URLS.get(lang, _FEDLEX_STROMVV_URLS["en"]),
    }


def _utility_display_name(key: str, db: dict | None = None) -> str:
    """Human-readable label from tariffs.json (`name_de`, falling back to key)."""
    db = db if db is not None else load_tariffs()
    u = db["utilities"].get(key)
    if u is None:
        return key
    return u.get("name_de") or u.get("name_fr") or key


def _tariff_schema(defaults: dict[str, Any] | None = None) -> vol.Schema:
    """Build the v0.5 tariff-step schema with optional pre-filled defaults.

    Only personal inputs: installed kW, Eigenverbrauch yes/no, HKN opt-in
    yes/no, Abrechnungs-Rhythmus. All utility-published values come from
    ``data/tariffs.json`` looked up by the utility chosen in step 1.
    """
    d = defaults or {}
    return vol.Schema(
        {
            vol.Required(
                CONF_INSTALLIERTE_LEISTUNG_KW,
                default=d.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0,
                    max=10000,
                    step=0.1,
                    mode=selector.NumberSelectorMode.BOX,
                    unit_of_measurement="kW",
                )
            ),
            vol.Required(
                CONF_EIGENVERBRAUCH_AKTIVIERT,
                default=d.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True),
            ): selector.BooleanSelector(),
            vol.Required(
                CONF_HKN_AKTIVIERT,
                default=d.get(CONF_HKN_AKTIVIERT, False),
            ): selector.BooleanSelector(),
            vol.Required(
                CONF_ABRECHNUNGS_RHYTHMUS,
                default=d.get(CONF_ABRECHNUNGS_RHYTHMUS, ABRECHNUNGS_RHYTHMUS_QUARTAL),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        ABRECHNUNGS_RHYTHMUS_QUARTAL,
                        ABRECHNUNGS_RHYTHMUS_MONAT,
                    ],
                    translation_key=CONF_ABRECHNUNGS_RHYTHMUS,
                    mode=selector.SelectSelectorMode.LIST,
                )
            ),
        }
    )


def _validate_tariff(user_input: dict[str, Any]) -> dict[str, str]:
    """Per-field error keys for the tariff step. Empty dict = valid.

    v0.5: only need to validate that kW is positive (the federal degressive
    formula and most utility cap-rules need a non-zero kW). The 8-segment
    plant-category dropdown is gone, so there's no "kw_required_for_degressive"
    case anymore — kW is required for everyone.
    """
    errors: dict[str, str] = {}
    if float(user_input.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0)) <= 0:
        errors[CONF_INSTALLIERTE_LEISTUNG_KW] = "kw_required"
    return errors


def _record_history_changes(
    *, old_data: dict, new_data: dict, old_options: dict
) -> dict:
    """Auto-append to plant_history / hkn_optin_history when the user changes
    kW / Eigenverbrauch / HKN-opt-in in the options flow.

    The prior open-ended record's ``valid_to`` is closed to "today"; a new
    open-ended record is appended carrying the new values. If the history
    list was empty (first ever options edit), a single record is appended
    with ``valid_from = today``. Manual backfill (editing past records) is
    a v0.6 polish — for now any mid-year change the user remembers can be
    fixed by editing entry.options directly via developer tools.
    """
    from datetime import date

    today = date.today().isoformat()
    new_options = {**old_options}

    plant_changed = (
        old_data.get(CONF_INSTALLIERTE_LEISTUNG_KW)
        != new_data.get(CONF_INSTALLIERTE_LEISTUNG_KW)
        or old_data.get(CONF_EIGENVERBRAUCH_AKTIVIERT)
        != new_data.get(CONF_EIGENVERBRAUCH_AKTIVIERT)
    )
    if plant_changed:
        history = list(new_options.get(OPT_PLANT_HISTORY) or [])
        if history and history[-1].get("valid_to") is None:
            history[-1] = {**history[-1], "valid_to": today}
        history.append(
            {
                "valid_from": today,
                "valid_to": None,
                "installierte_leistung_kw": float(
                    new_data[CONF_INSTALLIERTE_LEISTUNG_KW]
                ),
                "eigenverbrauch_aktiviert": bool(
                    new_data[CONF_EIGENVERBRAUCH_AKTIVIERT]
                ),
            }
        )
        new_options[OPT_PLANT_HISTORY] = history

    hkn_changed = old_data.get(CONF_HKN_AKTIVIERT) != new_data.get(CONF_HKN_AKTIVIERT)
    if hkn_changed:
        history = list(new_options.get(OPT_HKN_OPTIN_HISTORY) or [])
        if history and history[-1].get("valid_to") is None:
            history[-1] = {**history[-1], "valid_to": today}
        history.append(
            {
                "valid_from": today,
                "valid_to": None,
                "opted_in": bool(new_data[CONF_HKN_AKTIVIERT]),
            }
        )
        new_options[OPT_HKN_OPTIN_HISTORY] = history

    return new_options


class BfeRuecklieferTarifFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Menu-first 3-step config flow."""

    VERSION = 4

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> "BfeRuecklieferTarifOptionsFlow":
        return BfeRuecklieferTarifOptionsFlow()

    # ----- Step 1: utility menu --------------------------------------------------

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        await _async_warm_cache(self.hass)
        return self.async_show_menu(
            step_id="user",
            menu_options=[f"preset_{k}" for k in list_utility_keys()],
            description_placeholders=_source_links(self.hass),
        )

    async def _apply_preset(self, key: str) -> "FlowResult":
        self._data[CONF_ENERGIEVERSORGER] = key
        return await self.async_step_tariff()

    # 13 menu-option wrappers — one per utility entry in tariffs.json.
    # AEW splits into aew_fixpreis + aew_rmp; future multi-product utilities
    # (e.g. Primeo SolarAktiv) follow the same pattern.
    async def async_step_preset_ekz(self, user_input=None):
        return await self._apply_preset("ekz")

    async def async_step_preset_bkw(self, user_input=None):
        return await self._apply_preset("bkw")

    async def async_step_preset_ckw(self, user_input=None):
        return await self._apply_preset("ckw")

    async def async_step_preset_groupe_e(self, user_input=None):
        return await self._apply_preset("groupe_e")

    async def async_step_preset_primeo(self, user_input=None):
        return await self._apply_preset("primeo")

    async def async_step_preset_romande_energie(self, user_input=None):
        return await self._apply_preset("romande_energie")

    async def async_step_preset_sak(self, user_input=None):
        return await self._apply_preset("sak")

    async def async_step_preset_sgsw(self, user_input=None):
        return await self._apply_preset("sgsw")

    async def async_step_preset_ewz(self, user_input=None):
        return await self._apply_preset("ewz")

    async def async_step_preset_iwb(self, user_input=None):
        return await self._apply_preset("iwb")

    async def async_step_preset_sig(self, user_input=None):
        return await self._apply_preset("sig")

    async def async_step_preset_aew_fixpreis(self, user_input=None):
        return await self._apply_preset("aew_fixpreis")

    async def async_step_preset_aew_rmp(self, user_input=None):
        return await self._apply_preset("aew_rmp")

    # ----- Step 2: tariff configuration -----------------------------------------

    async def async_step_tariff(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        errors: dict[str, str] = {}

        if user_input is not None:
            errors = _validate_tariff(user_input)
            if not errors:
                self._data.update(user_input)
                return await self.async_step_entities()

        defaults = user_input if user_input is not None else self._data
        return self.async_show_form(
            step_id="tariff",
            data_schema=_tariff_schema(defaults),
            errors=errors,
            description_placeholders={
                "utility_name": _utility_display_name(self._data[CONF_ENERGIEVERSORGER]),
                **_source_links(self.hass),
            },
        )

    # ----- Step 3: HA entities --------------------------------------------------

    async def async_step_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        if user_input is not None:
            self._data.update(user_input)
            return self.async_create_entry(
                title=_utility_display_name(self._data[CONF_ENERGIEVERSORGER]),
                data=self._data,
            )

        default_prefix = self._data.get(CONF_ENERGIEVERSORGER, "bfe")
        schema = vol.Schema(
            {
                vol.Required(CONF_STROMNETZEINSPEISUNG_KWH): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        domain="sensor", device_class="energy"
                    )
                ),
                vol.Required(CONF_RUECKLIEFERVERGUETUNG_CHF): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="sensor")
                ),
                vol.Optional(
                    CONF_NAMENSPRAEFIX,
                    default=f"{default_prefix}_rueckliefertarif",
                ): str,
            }
        )
        return self.async_show_form(step_id="entities", data_schema=schema)


class BfeRuecklieferTarifOptionsFlow(config_entries.OptionsFlow):
    """Options flow: menu with tariff edit, specific-quarter re-import, and entity wiring.

    HA 2024.12+ exposes ``config_entry`` as a read-only property on
    OptionsFlow, sourced from ``self.handler`` (the entry_id). Don't override
    ``__init__`` or assign ``self.config_entry`` — that raises AttributeError
    on current HA.
    """

    # ----- Menu --------------------------------------------------------------

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        return self.async_show_menu(
            step_id="init",
            menu_options=["tariff", "reimport_quarter", "entities"],
        )

    # ----- Sub-step: edit tariff settings ------------------------------------

    async def async_step_tariff(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = _validate_tariff(user_input)
            if not errors:
                new_data = {**self.config_entry.data, **user_input}
                new_options = _record_history_changes(
                    old_data=dict(self.config_entry.data),
                    new_data=new_data,
                    old_options=dict(self.config_entry.options or {}),
                )
                self.hass.config_entries.async_update_entry(
                    self.config_entry, data=new_data, options=new_options
                )
                self.hass.async_create_task(
                    self.hass.config_entries.async_reload(self.config_entry.entry_id)
                )
                return self.async_create_entry(title="", data={})

        defaults = user_input if user_input is not None else dict(self.config_entry.data)
        return self.async_show_form(
            step_id="tariff",
            data_schema=_tariff_schema(defaults),
            errors=errors,
            description_placeholders=_source_links(self.hass),
        )

    # ----- Sub-step: re-import a specific past quarter -----------------------

    async def async_step_reimport_quarter(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        from .bfe import PriceNotYetPublished
        from .quarters import Quarter
        from .services import (
            _build_recompute_report,
            _notify_recompute,
            _reimport_quarter,
        )

        errors: dict[str, str] = {}
        if user_input is not None:
            quarter_str = (user_input.get("quarter") or "").strip()
            try:
                q = Quarter.parse(quarter_str)
            except ValueError:
                errors["quarter"] = "invalid_quarter"
                q = None
            if q is not None:
                try:
                    await _reimport_quarter(self.hass, q)
                except PriceNotYetPublished:
                    errors["quarter"] = "price_not_yet_published"
                except Exception:  # noqa: BLE001
                    errors["base"] = "reimport_failed"
                else:
                    report = _build_recompute_report(self.hass, [q])
                    _notify_recompute(self.hass, self.config_entry.entry_id, report)
                    return self.async_create_entry(title="", data={})

        default = (
            user_input.get("quarter", "")
            if user_input
            else ""
        )
        schema = vol.Schema({vol.Required("quarter", default=default): str})
        return self.async_show_form(
            step_id="reimport_quarter",
            data_schema=schema,
            errors=errors,
        )

    # ----- Sub-step: re-wire HA entities -------------------------------------

    async def async_step_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        if user_input is not None:
            new_data = {**self.config_entry.data, **user_input}
            self.hass.config_entries.async_update_entry(
                self.config_entry, data=new_data
            )
            self.hass.async_create_task(
                self.hass.config_entries.async_reload(self.config_entry.entry_id)
            )
            return self.async_create_entry(title="", data={})

        current = dict(self.config_entry.data)
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_STROMNETZEINSPEISUNG_KWH,
                    default=current.get(CONF_STROMNETZEINSPEISUNG_KWH),
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        domain="sensor", device_class="energy"
                    )
                ),
                vol.Required(
                    CONF_RUECKLIEFERVERGUETUNG_CHF,
                    default=current.get(CONF_RUECKLIEFERVERGUETUNG_CHF),
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="sensor")
                ),
                vol.Optional(
                    CONF_NAMENSPRAEFIX,
                    default=current.get(CONF_NAMENSPRAEFIX, "bfe_rueckliefertarif"),
                ): str,
            }
        )
        return self.async_show_form(step_id="entities", data_schema=schema)
