"""BFE Rückliefertarif integration for Home Assistant."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .const import DOMAIN

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor", "button"]


async def async_setup_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
    """Set up a config entry."""
    from .data_coordinator import TariffsDataCoordinator
    from .services import async_register_services

    hass.data.setdefault(DOMAIN, {})

    # Phase 6: a single TariffsDataCoordinator is shared across config entries
    # (only ever one in v0.5; the dict-keyed shape is forward-compat).
    if "_tariffs_data" not in hass.data[DOMAIN]:
        tdc = TariffsDataCoordinator(hass)
        await tdc.async_load()
        hass.data[DOMAIN]["_tariffs_data"] = tdc

    hass.data[DOMAIN][entry.entry_id] = {
        "config": dict(entry.data),
        "options": dict(entry.options or {}),
    }
    await async_register_services(hass)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
