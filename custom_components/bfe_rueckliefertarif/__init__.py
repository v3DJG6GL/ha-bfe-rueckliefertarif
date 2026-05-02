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
    await _async_register_card(hass)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def _async_register_card(hass: HomeAssistant) -> None:
    """v0.19.0 — auto-register the BFE tariff analysis Lovelace card.

    Ships ``www/bfe-tariff-analysis-card.js`` as a Lovelace resource so
    users don't need to manually wire it up. Idempotent — the
    ``_card_registered`` sentinel guards against double-registration
    when multiple config entries are set up. The ``?v=`` cache-bust
    query string forces browsers to re-fetch on integration updates.

    Failure to register (e.g. test environments without a real http
    component, or a frontend that hasn't loaded yet) logs a warning and
    falls through silently — the integration's core function (LTS
    writes via the recorder) is unaffected.
    """
    if hass.data[DOMAIN].get("_card_registered"):
        return
    from pathlib import Path

    www_dir = Path(__file__).parent / "www"
    card_path = str(www_dir / "bfe-tariff-analysis-card.js")
    card_url = "/api/bfe_rueckliefertarif/static/bfe-tariff-analysis-card.js"
    apex_path = str(www_dir / "apexcharts.min.js")
    apex_url = "/api/bfe_rueckliefertarif/static/apexcharts.min.js"

    # v0.21.5 — both stat-the-apex-file and read-the-manifest were running
    # synchronously on the event loop, triggering HA's blocking-call warning.
    # Wrap in a single executor call.
    def _io():
        return Path(apex_path).is_file(), _read_manifest_version()

    try:
        apex_present, version = await hass.async_add_executor_job(_io)
    except Exception:
        apex_present = False
        version = "dev"

    try:
        from homeassistant.components.frontend import add_extra_js_url
        from homeassistant.components.http import StaticPathConfig

        # v0.20.2: ship ApexCharts as a fetchable static asset, but DO NOT
        # add it via add_extra_js_url — that would inject a <script> tag
        # at HA boot, polluting window.ApexCharts and breaking other HACS
        # cards (notably RomRider/apexcharts-card) that bundle their own
        # internal ApexCharts copy. The card fetches + scope-isolates the
        # bundle on demand instead (see `_loadApex` in the card JS).
        static_paths = [StaticPathConfig(card_url, card_path, cache_headers=False)]
        if apex_present:
            static_paths.append(
                StaticPathConfig(apex_url, apex_path, cache_headers=True)
            )
        await hass.http.async_register_static_paths(static_paths)
        add_extra_js_url(hass, f"{card_url}?v={version}")
        hass.data[DOMAIN]["_card_registered"] = True
    except Exception as exc:
        _LOGGER.debug(
            "Lovelace card auto-registration skipped (%s) — manual resource "
            "wiring still works",
            exc,
        )


def _read_manifest_version() -> str:
    """Read ``manifest.json`` version. Used as cache-bust query string."""
    import json
    from pathlib import Path

    manifest = Path(__file__).parent / "manifest.json"
    return json.loads(manifest.read_text())["version"]


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
