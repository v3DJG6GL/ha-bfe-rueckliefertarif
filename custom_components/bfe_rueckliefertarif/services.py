"""Service handlers: reimport_quarter, reimport_range, reimport_all_history, refresh."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import voluptuous as vol

from .bfe import PriceNotYetPublished, fetch_monthly, fetch_quarterly
from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    DOMAIN,
    OPT_HKN_OPTIN_HISTORY,
    OPT_PLANT_HISTORY,
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
from .quarters import Quarter, hours_in_range, quarter_bounds_utc, quarter_of
from .tariffs_db import find_active, resolve_tariff_at

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
    hass.services.async_register(
        DOMAIN, "refresh_tariffs", _handle_refresh_tariffs
    )


def _cfg_for_entry(
    hass: "HomeAssistant", *, for_quarter: Quarter | None = None
) -> tuple[dict, TariffConfig]:
    """Build the TariffConfig for a config entry, optionally as-of a quarter.

    When `for_quarter` is given, plant_history and hkn_optin_history (in
    `entry.options`) are consulted at the quarter's start date, so past
    quarters get the kW / EV / HKN-opt-in values that were active back
    then. When omitted, today's `entry.data` is used.
    """
    from datetime import date

    entries = hass.data.get(DOMAIN, {})
    if not entries:
        raise RuntimeError("BFE Rückliefertarif not configured")
    entry_data = next(iter(entries.values()))
    cfg = entry_data["config"]
    options = entry_data.get("options") or {}

    utility_key = cfg.get(CONF_ENERGIEVERSORGER)

    if for_quarter is not None:
        at_date = date(for_quarter.year, ((for_quarter.q - 1) * 3) + 1, 1)
    else:
        at_date = date.today()

    kw, eigenverbrauch = _resolve_plant(cfg, options, at_date)
    hkn_aktiviert = _resolve_hkn_optin(cfg, options, at_date)

    resolved = resolve_tariff_at(
        utility_key, at_date, kw=kw, eigenverbrauch=eigenverbrauch
    )
    hkn_resolved = resolved.hkn_rp_kwh if hkn_aktiviert else 0.0

    tariff_cfg = TariffConfig(
        eigenverbrauch_aktiviert=eigenverbrauch,
        installierte_leistung_kw=kw,
        hkn_aktiviert=hkn_aktiviert,
        hkn_rp_kwh_resolved=hkn_resolved,
        resolved=resolved,
    )
    return cfg, tariff_cfg


def _resolve_plant(cfg: dict, options: dict, at_date) -> tuple[float, bool]:
    """Pick (kW, eigenverbrauch) active at `at_date` from plant_history or fall
    back to current entry.data."""
    history = options.get(OPT_PLANT_HISTORY) or []
    rec = find_active(history, at_date) if history else None
    if rec is not None:
        return float(rec["installierte_leistung_kw"]), bool(rec["eigenverbrauch_aktiviert"])
    return (
        float(cfg.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0) or 0.0),
        bool(cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True)),
    )


def _resolve_hkn_optin(cfg: dict, options: dict, at_date) -> bool:
    """Pick HKN opt-in state active at `at_date` from hkn_optin_history."""
    history = options.get(OPT_HKN_OPTIN_HISTORY) or []
    rec = find_active(history, at_date) if history else None
    if rec is not None:
        return bool(rec["opted_in"])
    return bool(cfg.get(CONF_HKN_AKTIVIERT, False))


async def _reimport_quarter(
    hass: "HomeAssistant", q: Quarter, *, force_fresh: bool = False
) -> None:
    """Core re-import routine for a single quarter.

    Per-customer history (`plant_history` / `hkn_optin_history` in
    `entry.options`) is consulted via `_cfg_for_entry(for_quarter=q)`, so
    past quarters get the kW / EV / HKN-opt-in values that were active at
    the start of that quarter — not "today's" config. After the LTS write,
    a snapshot of the resolved values is recorded in
    `coordinator._imported[<quarter>]["snapshot"]`.

    `force_fresh` is reserved for v0.6: it will signal that the snapshot
    should be ignored and the rate fully recomputed (used after correcting
    a wrong tariff entry). For v0.5 the path is identical either way —
    history-driven recompute is the default.
    """
    import aiohttp

    cfg, tariff_cfg = _cfg_for_entry(hass, for_quarter=q)
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

    # Snapshot: freeze the resolved tariff state into _imported[q] so future
    # tariffs.json edits don't retroactively change what we wrote to LTS.
    _record_snapshot(hass, q, q_price.chf_per_mwh, plan, tariff_cfg)


def _record_snapshot(
    hass: "HomeAssistant",
    q: Quarter,
    q_price_chf_mwh: float,
    plan,
    tariff_cfg: TariffConfig,
) -> None:
    """Write the per-quarter import snapshot to the coordinator's storage.

    The snapshot makes past LTS values introspectable: a future user (or a
    sanity-check SQL) can compare implied per-quarter rate vs. "what was
    the rate when we imported?". v0.6 will additionally use this to
    short-circuit recompute when force_fresh=False.
    """
    entries = hass.data.get(DOMAIN, {})
    if not entries:
        return
    entry_data = next(iter(entries.values()))
    coordinator = entry_data.get("coordinator")
    if coordinator is None:
        return

    rt = tariff_cfg.resolved
    # Flat per-quarter rate: the rate every hour got, in Rp/kWh. For
    # ABRECHNUNGS_RHYTHMUS_QUARTAL this is exact; for monthly, it's
    # Q_total_CHF / Q_kWh, which equals the quarterly effective rate.
    total_kwh = sum(r.kwh for r in plan.records)
    if total_kwh > 0:
        rate_rp_kwh = (plan.final_sum_chf - plan.anchor_sum_chf) * 100.0 / total_kwh
    else:
        # No export this quarter — nothing imported, but record the rate
        # the importer would have applied (first record's rate).
        rate_rp_kwh = plan.records[0].rate_rp_kwh if plan.records else 0.0

    cap_applied = (
        rt.cap_mode
        and rt.cap_rp_kwh is not None
        and rate_rp_kwh >= rt.cap_rp_kwh - 1e-6
    )

    snapshot = {
        "rate_rp_kwh": round(rate_rp_kwh, 4),
        "kw": tariff_cfg.installierte_leistung_kw,
        "eigenverbrauch_aktiviert": tariff_cfg.eigenverbrauch_aktiviert,
        "hkn_rp_kwh": tariff_cfg.hkn_rp_kwh_resolved,
        "hkn_optin": tariff_cfg.hkn_aktiviert,
        "cap_mode": rt.cap_mode,
        "cap_applied": bool(cap_applied),
        "tariffs_json_version": rt.tariffs_json_version,
        "tariffs_json_source": rt.tariffs_json_source,
    }

    from datetime import datetime, timezone

    key = str(q)
    coordinator._imported[key] = {
        "q_price_chf_mwh": q_price_chf_mwh,
        "imported_at": datetime.now(timezone.utc).isoformat(),
        "snapshot": snapshot,
    }
    hass.async_create_task(coordinator._async_save_state())


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


async def _reimport_all_history(hass: "HomeAssistant") -> dict:
    """Re-import every quarter BFE has published. Returns a result summary.

    Result dict keys:
    - ``available``: sorted list of all Quarters BFE has published
    - ``imported``: list of Quarters successfully recomputed
    - ``skipped``: list of Quarters skipped because BFE has not yet published them
    - ``failed``: list of Quarters that errored unexpectedly
    """
    import aiohttp

    async with aiohttp.ClientSession() as session:
        quarterly = await fetch_quarterly(session)

    imported: list = []
    skipped: list = []
    failed: list = []
    for q in sorted(quarterly.keys()):
        try:
            await _reimport_quarter(hass, q)
            imported.append(q)
        except PriceNotYetPublished as exc:
            _LOGGER.warning("Skipping %s: %s", q, exc)
            skipped.append(q)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error("Failed importing %s: %s", q, exc)
            failed.append(q)

    return {
        "available": sorted(quarterly.keys()),
        "imported": imported,
        "skipped": skipped,
        "failed": failed,
    }


async def _import_running_quarter_estimate(hass: "HomeAssistant") -> dict:
    """Write LTS for the running quarter using the current effective-rate estimate.

    Used while BFE has not yet published the running quarter so the user
    can see realistic CHF values in the Energy Dashboard immediately
    instead of whatever stale price source was wired before. Iterates
    hours from quarter_start up to the last completed hour and writes
    ``kWh × effective_rate`` to the compensation LTS.

    The rate comes from ``coordinator.data['tariff_breakdown']['effective_rp_kwh']``
    — for ``basisverguetung = fixpreis`` it's exact; for
    ``basisverguetung = referenz_marktpreis`` and a quarter BFE has not
    yet published, it's the most-recently-published BFE quarter's rate
    (``is_estimate=True`` in the breakdown). Once BFE publishes the
    running quarter, the regular import path overwrites these LTS values
    with exact BFE-based numbers.

    Returns a result dict for the caller to surface in a notification.
    """
    from datetime import datetime, timezone

    cfg, _tariff_cfg = _cfg_for_entry(hass)
    export_id = cfg[CONF_STROMNETZEINSPEISUNG_KWH]
    comp_id = cfg[CONF_RUECKLIEFERVERGUETUNG_CHF]

    entries = hass.data.get(DOMAIN, {})
    entry_data = next(iter(entries.values()))
    coordinator = entry_data.get("coordinator")
    if coordinator is None or not coordinator.data:
        raise RuntimeError(
            "Coordinator not ready — run 'Reload reference market prices' first"
        )
    breakdown = coordinator.data.get("tariff_breakdown")
    if not breakdown:
        raise RuntimeError("Tariff breakdown unavailable")

    rate_rp_kwh = float(breakdown["effective_rp_kwh"])
    is_estimate = bool(breakdown.get("is_estimate", False))
    estimate_basis = breakdown.get("estimate_basis")

    now = datetime.now(timezone.utc)
    q = quarter_of(now)
    q_start_utc, _q_end_utc = quarter_bounds_utc(q)
    last_full_hour = now.replace(minute=0, second=0, microsecond=0)

    if last_full_hour <= q_start_utc:
        # Quarter has just started — nothing to write yet.
        return {
            "quarter": str(q),
            "rate_rp_kwh": rate_rp_kwh,
            "hours_imported": 0,
            "chf_total": 0.0,
            "is_estimate": is_estimate,
            "estimate_basis": estimate_basis,
        }

    hourly_kwh = await read_hourly_export(hass, export_id, q_start_utc, last_full_hour)
    anchor = await read_compensation_anchor(
        hass, comp_id, q_start_utc - _one_hour()
    )

    rate_chf_kwh = rate_rp_kwh / 100.0
    running_sum = anchor
    records: list[tuple] = []
    for h in hours_in_range(q_start_utc, last_full_hour):
        kwh = hourly_kwh.get(h, 0.0)
        running_sum += kwh * rate_chf_kwh
        records.append((h, running_sum))

    if records:
        await import_statistics(
            hass,
            build_metadata_compensation(comp_id),
            build_compensation_stats(records),
        )

    chf_total = running_sum - anchor
    _LOGGER.info(
        "Imported running %s estimate: rate=%.4f Rp/kWh, hours=%d, total=%.4f CHF (estimate=%s, basis=%s)",
        q, rate_rp_kwh, len(records), chf_total, is_estimate, estimate_basis,
    )

    return {
        "quarter": str(q),
        "rate_rp_kwh": rate_rp_kwh,
        "hours_imported": len(records),
        "chf_total": chf_total,
        "is_estimate": is_estimate,
        "estimate_basis": estimate_basis,
    }


async def _refresh_coordinator(hass: "HomeAssistant") -> dict:
    """Trigger a fresh BFE poll via the coordinator. Returns ``{available, newly_imported}``.

    The coordinator's ``_async_update_data`` fetches BFE prices and auto-imports
    quarters whose price changed since the last successful import. We snapshot
    its private ``_imported`` map before/after to report what was new this tick.
    """
    entries = hass.data.get(DOMAIN, {})
    if not entries:
        raise RuntimeError("BFE Rückliefertarif not configured")
    entry_data = next(iter(entries.values()))
    coordinator = entry_data.get("coordinator")
    if coordinator is None:
        raise RuntimeError("Coordinator not yet ready")

    before = set(coordinator._imported.keys())
    await coordinator.async_refresh()
    after = set(coordinator._imported.keys())

    return {
        "available": sorted(coordinator.quarterly.keys()),
        "newly_imported": sorted(after - before),
    }


async def _handle_reimport_all_history(call: "ServiceCall") -> None:
    await _reimport_all_history(call.hass)


async def _handle_refresh(call: "ServiceCall") -> None:
    """Force the coordinator to poll BFE now (auto-imports newly-published quarters)."""
    await _refresh_coordinator(call.hass)


async def _handle_refresh_tariffs(call: "ServiceCall") -> None:
    """Force a fresh fetch of the companion repo's tariffs.json.

    Falls back to the bundled file silently on any error (network /
    schema). Useful after the user knows a yearly update has landed in the
    companion repo and doesn't want to wait for the next daily refresh.
    """
    tdc = call.hass.data.get(DOMAIN, {}).get("_tariffs_data")
    if tdc is None:
        raise RuntimeError("BFE Rückliefertarif data coordinator not initialized")
    await tdc.async_refresh()
