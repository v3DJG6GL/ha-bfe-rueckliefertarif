"""Config flow for BFE Rückliefertarif."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector

from .const import (
    BASE_MODE_FIXED,
    BASE_MODE_RMP,
    BILLING_MODE_MONTHLY,
    BILLING_MODE_QUARTERLY,
    CONF_BASE_MODE,
    CONF_BILLING_MODE,
    CONF_COMPENSATION_ENTITY,
    CONF_ENTITY_PREFIX,
    CONF_EXPORT_ENTITY,
    CONF_FIXED_RATE,
    CONF_HKN_BONUS,
    CONF_KW,
    CONF_PRESET,
    CONF_SEGMENT,
    DOMAIN,
)
from .presets import PRESETS, get_preset, list_preset_keys
from .tariff import Segment

if TYPE_CHECKING:
    from homeassistant.data_entry_flow import FlowResult


_DEGRESSIVE_SEGMENTS = {Segment.MID_MIT_EV.value, Segment.LARGE_MIT_EV.value}


class BfeRuecklieferTarifFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Config flow: (1) pick preset → (2) system details → (3) HA entities."""

    VERSION = 1

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> "FlowResult":
        if user_input is not None:
            self._data.update(user_input)
            preset = get_preset(user_input[CONF_PRESET])
            self._data[CONF_BASE_MODE] = preset.base_mode
            self._data[CONF_HKN_BONUS] = preset.hkn_bonus_rp_kwh
            if preset.fixed_rate_rp_kwh is not None:
                self._data[CONF_FIXED_RATE] = preset.fixed_rate_rp_kwh
            return await self.async_step_system()

        schema = vol.Schema(
            {
                vol.Required(CONF_PRESET, default="ekz"): vol.In(
                    {k: PRESETS[k].display_name for k in list_preset_keys()}
                ),
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema)

    async def async_step_system(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        errors: dict[str, str] = {}
        preset_key = self._data.get(CONF_PRESET, "custom")
        preset = get_preset(preset_key)

        if user_input is not None:
            seg = user_input[CONF_SEGMENT]
            if seg in _DEGRESSIVE_SEGMENTS and user_input.get(CONF_KW, 0) <= 0:
                errors[CONF_KW] = "kw_required_for_degressive"
            if user_input[CONF_BASE_MODE] == BASE_MODE_FIXED:
                if user_input.get(CONF_FIXED_RATE, 0) <= 0:
                    errors[CONF_FIXED_RATE] = "fixed_rate_required"
            if not errors:
                self._data.update(user_input)
                return await self.async_step_entities()

        schema_dict: dict[Any, Any] = {
            vol.Required(
                CONF_SEGMENT,
                default=user_input.get(CONF_SEGMENT) if user_input else Segment.SMALL_MIT_EV.value,
            ): vol.In({s.value: s.value for s in Segment}),
            vol.Optional(
                CONF_KW, default=user_input.get(CONF_KW, 0.0) if user_input else 0.0
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=10000, step=0.1, mode="box")
            ),
            vol.Required(
                CONF_BASE_MODE,
                default=user_input.get(CONF_BASE_MODE) if user_input else preset.base_mode,
            ): vol.In([BASE_MODE_RMP, BASE_MODE_FIXED]),
            vol.Required(
                CONF_HKN_BONUS,
                default=user_input.get(CONF_HKN_BONUS)
                if user_input
                else preset.hkn_bonus_rp_kwh,
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=10, step=0.01, mode="box")
            ),
            vol.Optional(
                CONF_FIXED_RATE,
                default=user_input.get(CONF_FIXED_RATE)
                if user_input
                else (preset.fixed_rate_rp_kwh or 0.0),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=30, step=0.01, mode="box")
            ),
            vol.Required(
                CONF_BILLING_MODE,
                default=user_input.get(CONF_BILLING_MODE) if user_input else BILLING_MODE_QUARTERLY,
            ): vol.In([BILLING_MODE_QUARTERLY, BILLING_MODE_MONTHLY]),
        }

        return self.async_show_form(
            step_id="system",
            data_schema=vol.Schema(schema_dict),
            errors=errors,
            description_placeholders={"preset": preset.display_name},
        )

    async def async_step_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        errors: dict[str, str] = {}
        if user_input is not None:
            self._data.update(user_input)
            return self.async_create_entry(
                title=get_preset(self._data[CONF_PRESET]).display_name,
                data=self._data,
            )

        default_prefix = self._data.get(CONF_PRESET, "bfe")
        schema = vol.Schema(
            {
                vol.Required(CONF_EXPORT_ENTITY): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="sensor", device_class="energy")
                ),
                vol.Required(CONF_COMPENSATION_ENTITY): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="sensor")
                ),
                vol.Optional(
                    CONF_ENTITY_PREFIX, default=f"{default_prefix}_rueckliefertarif"
                ): str,
            }
        )
        return self.async_show_form(step_id="entities", data_schema=schema, errors=errors)
