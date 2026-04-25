"""BFE Rückliefertarif integration for Home Assistant."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .const import (
    _V1_TO_V2_KEY_MAP,
    _V1_TO_V2_VALUE_MAP,
    CONF_ENERGIEVERSORGER,
    CONF_VERGUETUNGS_OBERGRENZE,
    DOMAIN,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor", "button"]


async def async_setup_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
    """Set up a config entry."""
    from .services import async_register_services

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {"config": dict(entry.data)}
    await async_register_services(hass)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok


async def async_migrate_entry(
    hass: "HomeAssistant", entry: "ConfigEntry"
) -> bool:
    """Migrate v1 → v2 (key/value renames) and v2 → v3 (Vergütungs-Obergrenze)."""
    if entry.version == 1:
        data = {_V1_TO_V2_KEY_MAP.get(k, k): v for k, v in entry.data.items()}
        for field, mapping in _V1_TO_V2_VALUE_MAP.items():
            if field in data and data[field] in mapping:
                data[field] = mapping[data[field]]
        hass.config_entries.async_update_entry(entry, data=data, version=2)
        _LOGGER.info("Migrated %s entry %s from v1 to v2", DOMAIN, entry.entry_id)

    if entry.version == 2:
        from .presets import PRESETS

        data = dict(entry.data)
        energieversorger = data.get(CONF_ENERGIEVERSORGER, "custom")
        preset = PRESETS.get(energieversorger)
        data[CONF_VERGUETUNGS_OBERGRENZE] = (
            preset.verguetungs_obergrenze if preset is not None else False
        )
        hass.config_entries.async_update_entry(entry, data=data, version=3)
        _LOGGER.info(
            "Migrated %s entry %s from v2 to v3 (Vergütungs-Obergrenze=%s)",
            DOMAIN,
            entry.entry_id,
            data[CONF_VERGUETUNGS_OBERGRENZE],
        )
    return True
