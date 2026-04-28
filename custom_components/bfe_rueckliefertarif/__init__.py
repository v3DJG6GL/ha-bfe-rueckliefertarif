"""BFE Rückliefertarif integration for Home Assistant."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .const import CONF_VALID_FROM, DOMAIN, OPT_CONFIG_HISTORY, build_history_config

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor"]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
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

    # v0.8.2: split first-setup vs pathological-empty-history. The earlier
    # falsy guard re-seeded the sentinel from entry.data even when
    # OPT_CONFIG_HISTORY went empty mid-life — and entry.data may already
    # carry the *latest* utility (mutated by _sync_entry_data_from_history
    # in the options flow). After the OptionsFlow wipe race was fixed, an
    # empty history can no longer happen via normal flows; if it does, log
    # loudly and refuse to silently encode the wrong utility.
    options = dict(entry.options or {})
    options.pop("plant_history", None)
    options.pop("hkn_optin_history", None)

    history = options.get(OPT_CONFIG_HISTORY)
    if history is None:
        # First setup — synthesize sentinel from the just-collected entry.data.
        # v0.9.2: anchor at user-supplied CONF_VALID_FROM (plant install date)
        # instead of 1970-01-01. The 1970 fallback covers any pre-v0.9.2 entry
        # that somehow reaches this code path without the new field.
        options[OPT_CONFIG_HISTORY] = [
            {
                "valid_from": entry.data.get(CONF_VALID_FROM, "1970-01-01"),
                "valid_to": None,
                "config": build_history_config(entry.data),
            }
        ]
        hass.config_entries.async_update_entry(entry, options=options)
    elif not history:
        _LOGGER.error(
            "OPT_CONFIG_HISTORY is empty for entry %s — refusing to silently "
            "re-seed (would likely encode the wrong utility from mutated "
            "entry.data). Open Settings → Manage configuration history and "
            "add a transition, or restore a snapshot.",
            entry.entry_id,
        )

    hass.data[DOMAIN][entry.entry_id] = {
        "config": dict(entry.data),
        "options": dict(entry.options or {}),
    }
    await async_register_services(hass)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
