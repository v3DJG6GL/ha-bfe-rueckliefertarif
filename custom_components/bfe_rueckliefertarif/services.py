"""Service handlers: reimport_quarter, reimport_range, reimport_all_history, refresh."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import voluptuous as vol

from .bfe import PriceNotYetPublished, fetch_monthly, fetch_quarterly
from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_ANLAGENKATEGORIE,
    CONF_BASISVERGUETUNG,
    CONF_FIXPREIS_RP_KWH,
    CONF_HKN_VERGUETUNG_RP_KWH,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    DOMAIN,
)
from .ha_recorder import (
    build_compensation_stats,
    build_metadata_compensation,
    import_statistics,
    read_compensation_anchor,
    read_hourly_export,
    read_post_quarter_sums,
)
from .importer import TariffConfig, compute_quarter_plan, cumulative_sums
from .quarters import Quarter, quarter_bounds_utc
from .tariff import Segment

if TYPE_CHECKING:
    from datetime import timedelta

    from homeassistant.core import HomeAssistant, ServiceCall

_LOGGER = logging.getLogger(__name__)


async def async_register_services(hass: "HomeAssistant") -> None:
    """Register services on first integration setup."""
    if hass.services.has_service(DOMAIN, "reimport_quarter"):
        return

    hass.services.async_register(
        DOMAIN,
        "reimport_quarter",
        _handle_reimport_quarter,
        schema=vol.Schema({vol.Required("quarter"): str}),
    )
    hass.services.async_register(
        DOMAIN,
        "reimport_range",
        _handle_reimport_range,
        schema=vol.Schema(
            {
                vol.Required("start_quarter"): str,
                vol.Required("end_quarter"): str,
            }
        ),
    )
    hass.services.async_register(
        DOMAIN, "reimport_all_history", _handle_reimport_all_history
    )
    hass.services.async_register(DOMAIN, "refresh", _handle_refresh)


def _cfg_for_entry(hass: "HomeAssistant") -> tuple[dict, TariffConfig]:
    """Pull the first config entry's config (v1 supports a single entry)."""
    entries = hass.data.get(DOMAIN, {})
    if not entries:
        raise RuntimeError("BFE Rückliefertarif not configured")
    # Use first entry
    entry_data = next(iter(entries.values()))
    cfg = entry_data["config"]
    tariff_cfg = TariffConfig(
        anlagenkategorie=Segment(cfg[CONF_ANLAGENKATEGORIE]),
        installierte_leistung_kw=float(cfg.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0) or 0.0),
        basisverguetung=cfg[CONF_BASISVERGUETUNG],
        hkn_verguetung_rp_kwh=float(cfg.get(CONF_HKN_VERGUETUNG_RP_KWH, 0.0)),
        fixpreis_rp_kwh=(
            float(cfg[CONF_FIXPREIS_RP_KWH]) if cfg.get(CONF_FIXPREIS_RP_KWH) else None
        ),
    )
    return cfg, tariff_cfg


async def _reimport_quarter(hass: "HomeAssistant", q: Quarter) -> None:
    """Core re-import routine for a single quarter."""
    import aiohttp

    cfg, tariff_cfg = _cfg_for_entry(hass)
    export_id = cfg[CONF_STROMNETZEINSPEISUNG_KWH]
    comp_id = cfg[CONF_RUECKLIEFERVERGUETUNG_CHF]
    abrechnungs_rhythmus = cfg[CONF_ABRECHNUNGS_RHYTHMUS]

    q_start, q_end = quarter_bounds_utc(q)

    async with aiohttp.ClientSession() as session:
        quarterly = await fetch_quarterly(session)
        monthly = (
            await fetch_monthly(session)
            if abrechnungs_rhythmus == ABRECHNUNGS_RHYTHMUS_MONAT
            else None
        )

    if q not in quarterly:
        raise PriceNotYetPublished(f"BFE has not published 2026Q{q.q}/{q.year} yet")
    q_price = quarterly[q]

    hourly_kwh = await read_hourly_export(hass, export_id, q_start, q_end)
    anchor = await read_compensation_anchor(
        hass, comp_id, q_start - _one_hour()
    )
    post_sums = await read_post_quarter_sums(
        hass, comp_id, q_end, q_end + _one_hour() * 24 * 365
    )
    old_first_post = post_sums[0][1] if post_sums else None

    plan = compute_quarter_plan(
        q=q,
        hourly_kwh=hourly_kwh,
        quarterly_price=q_price,
        monthly_prices=monthly,
        cfg=tariff_cfg,
        billing_mode=abrechnungs_rhythmus,
        anchor_sum_chf=anchor,
        old_post_quarter_first_sum_chf=old_first_post,
    )

    # Write the quarter's compensation chain.
    sums = cumulative_sums(plan)
    records = [(r.start, s) for r, s in zip(plan.records, sums, strict=True)]
    await import_statistics(
        hass,
        build_metadata_compensation(comp_id),
        build_compensation_stats(records),
    )

    # Transition-spike fix: shift all post-quarter records by delta.
    if post_sums and plan.post_quarter_delta_chf != 0.0:
        shifted = [(s, total + plan.post_quarter_delta_chf) for s, total in post_sums]
        await import_statistics(
            hass,
            build_metadata_compensation(comp_id),
            build_compensation_stats(shifted),
        )

    _LOGGER.info(
        "Imported %s: final_sum=%.4f CHF, post_delta=%.4f CHF, hours=%d",
        q,
        plan.final_sum_chf,
        plan.post_quarter_delta_chf,
        len(plan.records),
    )


def _one_hour() -> "timedelta":
    from datetime import timedelta

    return timedelta(hours=1)


async def _handle_reimport_quarter(call: "ServiceCall") -> None:
    q = Quarter.parse(call.data["quarter"])
    await _reimport_quarter(call.hass, q)


async def _handle_reimport_range(call: "ServiceCall") -> None:
    start = Quarter.parse(call.data["start_quarter"])
    end = Quarter.parse(call.data["end_quarter"])
    q = start
    while q <= end:
        try:
            await _reimport_quarter(call.hass, q)
        except PriceNotYetPublished as exc:
            _LOGGER.warning("Skipping %s: %s", q, exc)
        q = q.next()


async def _handle_reimport_all_history(call: "ServiceCall") -> None:
    """Re-import every quarter where both BFE data and export LTS are available.

    Strategy: fetch quarterly CSV → iterate known quarters → attempt each import.
    The export-data coverage check is implicit (hours outside coverage → kWh 0).
    """
    import aiohttp

    async with aiohttp.ClientSession() as session:
        quarterly = await fetch_quarterly(session)
    for q in sorted(quarterly.keys()):
        try:
            await _reimport_quarter(call.hass, q)
        except PriceNotYetPublished as exc:
            _LOGGER.warning("Skipping %s: %s", q, exc)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error("Failed importing %s: %s", q, exc)


async def _handle_refresh(call: "ServiceCall") -> None:
    """Force-poll BFE now and re-import any newly-published quarter not yet covered.

    Phase 4 wires this into the coordinator; for Phase 3 it behaves like reimport_all_history
    but only imports quarters that haven't been imported at the currently-published price.
    """
    # v1: simplest implementation — same as reimport_all_history. Coordinator in Phase 4
    # will gate this with a state-file check.
    await _handle_reimport_all_history(call)
