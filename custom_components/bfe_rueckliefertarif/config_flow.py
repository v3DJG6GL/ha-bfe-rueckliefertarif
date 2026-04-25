"""Config flow for BFE Rückliefertarif.

Three-step flow:
1. ``user`` — clickable menu of utilities (one click advances).
2. ``tariff`` — 6 tariff fields, pre-filled from the chosen utility's preset.
3. ``entities`` — 3 entity-wiring fields.

Plus an Options Flow that re-exposes the tariff step (post-setup edits).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector

from .const import (
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
    DOMAIN,
)
from .presets import PRESETS, get_preset, list_preset_keys
from .tariff import Segment

if TYPE_CHECKING:
    from homeassistant.data_entry_flow import FlowResult


_DEGRESSIVE_KATEGORIEN = {Segment.MID_MIT_EV.value, Segment.LARGE_MIT_EV.value}

# Preset attributes use the v1 vocabulary (base_mode = "rmp_passthrough"|"fixed_rate")
# because presets.py is unchanged. Translate to the v2 stored values when seeding.
_PRESET_LEGACY_TO_NEW: dict[str, str] = {
    "rmp_passthrough": BASISVERGUETUNG_REFERENZMARKTPREIS,
    "fixed_rate": BASISVERGUETUNG_FIXPREIS,
}

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
# Fedlex (Swiss federal law portal) — EnV SR 730.01 carries Art. 12 Abs. 1bis
# (Mindestvergütung floors); StromVV SR 734.71 carries Art. 4 Abs. 3 Bst. e
# (cap formula). Both in force 1.1.2026 via AS 2025 138 / AS 2025 139.
# ELI numbers verified via redirect from legacy admin.ch/opc URLs.
_FEDLEX_ENV_URLS: dict[str, str] = {
    "de": "https://www.fedlex.admin.ch/eli/cc/2017/766/de",
    "en": "https://www.fedlex.admin.ch/eli/cc/2017/766/en",
    "fr": "https://www.fedlex.admin.ch/eli/cc/2017/766/fr",
}
_FEDLEX_STROMVV_URLS: dict[str, str] = {
    "de": "https://www.fedlex.admin.ch/eli/cc/2008/226/de",
    "en": "https://www.fedlex.admin.ch/eli/cc/2008/226/en",
    "fr": "https://www.fedlex.admin.ch/eli/cc/2008/226/fr",
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


def _tariff_schema(defaults: dict[str, Any] | None = None) -> vol.Schema:
    """Build the tariff-step schema with optional pre-filled defaults.

    Field order is intentional: plant category → installed power →
    base price (radio) → fixed price (number, conditional on base price) →
    HKN payment → billing period. Fixed price sits next to the radio that
    makes it conditional; HKN follows because it's an additive bonus.
    """
    d = defaults or {}
    return vol.Schema(
        {
            vol.Required(
                CONF_ANLAGENKATEGORIE,
                default=d.get(CONF_ANLAGENKATEGORIE, Segment.SMALL_MIT_EV.value),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[s.value for s in Segment],
                    translation_key=CONF_ANLAGENKATEGORIE,
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Optional(
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
                CONF_BASISVERGUETUNG,
                default=d.get(CONF_BASISVERGUETUNG, BASISVERGUETUNG_REFERENZMARKTPREIS),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        BASISVERGUETUNG_REFERENZMARKTPREIS,
                        BASISVERGUETUNG_FIXPREIS,
                    ],
                    translation_key=CONF_BASISVERGUETUNG,
                    mode=selector.SelectSelectorMode.LIST,
                )
            ),
            vol.Optional(
                CONF_FIXPREIS_RP_KWH,
                default=d.get(CONF_FIXPREIS_RP_KWH, 0.0),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0,
                    max=30,
                    step=0.01,
                    mode=selector.NumberSelectorMode.BOX,
                    unit_of_measurement="Rp/kWh",
                )
            ),
            vol.Required(
                CONF_HKN_VERGUETUNG_RP_KWH,
                default=d.get(CONF_HKN_VERGUETUNG_RP_KWH, 0.0),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0,
                    max=10,
                    step=0.01,
                    mode=selector.NumberSelectorMode.BOX,
                    unit_of_measurement="Rp/kWh",
                )
            ),
            vol.Required(
                CONF_VERGUETUNGS_OBERGRENZE,
                default=d.get(CONF_VERGUETUNGS_OBERGRENZE, False),
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
    """Return per-field error keys for the tariff step. Empty dict = valid."""
    errors: dict[str, str] = {}
    if (
        user_input[CONF_ANLAGENKATEGORIE] in _DEGRESSIVE_KATEGORIEN
        and float(user_input.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0)) <= 0
    ):
        errors[CONF_INSTALLIERTE_LEISTUNG_KW] = "kw_required_for_degressive"
    if user_input[CONF_BASISVERGUETUNG] == BASISVERGUETUNG_FIXPREIS:
        if float(user_input.get(CONF_FIXPREIS_RP_KWH, 0)) <= 0:
            errors[CONF_FIXPREIS_RP_KWH] = "fixpreis_required"
    return errors


class BfeRuecklieferTarifFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Menu-first 3-step config flow."""

    VERSION = 3

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
        return self.async_show_menu(
            step_id="user",
            menu_options=[f"preset_{k}" for k in list_preset_keys()],
            description_placeholders=_source_links(self.hass),
        )

    async def _apply_preset(self, key: str) -> "FlowResult":
        self._data[CONF_ENERGIEVERSORGER] = key
        preset = get_preset(key)
        self._data[CONF_BASISVERGUETUNG] = _PRESET_LEGACY_TO_NEW[preset.base_mode]
        self._data[CONF_HKN_VERGUETUNG_RP_KWH] = preset.hkn_bonus_rp_kwh
        self._data[CONF_VERGUETUNGS_OBERGRENZE] = preset.verguetungs_obergrenze
        if preset.fixed_rate_rp_kwh is not None:
            self._data[CONF_FIXPREIS_RP_KWH] = preset.fixed_rate_rp_kwh
        return await self.async_step_tariff()

    # 12 menu-option wrappers — each routes to _apply_preset.
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

    async def async_step_preset_aew(self, user_input=None):
        return await self._apply_preset("aew")

    async def async_step_preset_custom(self, user_input=None):
        return await self._apply_preset("custom")

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
        preset = get_preset(self._data.get(CONF_ENERGIEVERSORGER, "custom"))
        return self.async_show_form(
            step_id="tariff",
            data_schema=_tariff_schema(defaults),
            errors=errors,
            description_placeholders={
                "utility_name": preset.display_name,
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
                title=get_preset(self._data[CONF_ENERGIEVERSORGER]).display_name,
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
                self.hass.config_entries.async_update_entry(
                    self.config_entry, data=new_data
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
        from homeassistant.components.persistent_notification import (
            async_create as _notify,
        )

        from .bfe import PriceNotYetPublished
        from .quarters import Quarter
        from .services import _reimport_quarter

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
                    _notify(
                        self.hass,
                        f"Feed-in remuneration for {q} recomputed.",
                        title="BFE Rückliefertarif",
                        notification_id=f"{DOMAIN}_{self.config_entry.entry_id}_reimport_specific",
                    )
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
