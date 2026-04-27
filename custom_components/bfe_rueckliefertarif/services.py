"""Service handlers: reimport_all_history, refresh, refresh_tariffs."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .bfe import PriceNotYetPublished, fetch_monthly, fetch_quarterly
from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    CONFIG_HISTORY_FIELDS,
    DOMAIN,
    OPT_CONFIG_HISTORY,
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
    if hass.services.has_service(DOMAIN, "reimport_all_history"):
        return

    hass.services.async_register(
        DOMAIN, "reimport_all_history", _handle_reimport_all_history
    )
    hass.services.async_register(DOMAIN, "refresh", _handle_refresh)
    hass.services.async_register(
        DOMAIN, "refresh_tariffs", _handle_refresh_tariffs
    )


def _first_entry_data(hass: "HomeAssistant") -> dict:
    """Return the first config entry's storage dict, with live config/options.

    ``hass.data[DOMAIN]`` carries one slot per config entry (keyed by
    ``entry_id``) plus a shared ``_tariffs_data`` slot (the
    TariffsDataCoordinator from Phase 6). Skip underscore-prefixed keys
    so we always get an actual config entry.

    v0.8.5: ``config`` and ``options`` keys are re-pulled from the live
    ``ConfigEntry`` on every call. Earlier versions cached them at
    ``async_setup_entry`` time and never refreshed, so any change made via
    the OptionsFlow stayed invisible to recompute paths until the entry
    was reloaded — and OptionsFlowWithReload silently skips the auto-
    reload when ``_edit_row`` has pre-written options inline (its diff
    check sees no change). Live reads make this class of bug structural.
    """
    entries = hass.data.get(DOMAIN, {})
    for key, value in entries.items():
        if not key.startswith("_") and isinstance(value, dict):
            entry = hass.config_entries.async_get_entry(key)
            if entry is not None:
                value["config"] = dict(entry.data)
                value["options"] = dict(entry.options or {})
            return value
    raise RuntimeError("BFE Rückliefertarif not configured")


def _cfg_for_entry(
    hass: "HomeAssistant", *, for_quarter: Quarter | None = None
) -> tuple[dict, TariffConfig]:
    """Build the TariffConfig for a config entry, optionally as-of a quarter.

    The returned ``cfg`` dict merges entity-wiring fields from ``entry.data``
    (export sensor, compensation sensor, name prefix) with the historically-
    resolved versioned fields from ``OPT_CONFIG_HISTORY`` at ``for_quarter``'s
    start date (or today). Versioned fields in ``entry.data`` are intentionally
    overridden by the resolved record — post-A+ the history is the source of
    truth and ``entry.data`` versioned fields, if present, are stale.
    """
    from datetime import date

    entry_data = _first_entry_data(hass)
    raw_data = entry_data["config"]
    options = entry_data.get("options") or {}

    if for_quarter is not None:
        at_date = date(for_quarter.year, ((for_quarter.q - 1) * 3) + 1, 1)
    else:
        at_date = date.today()

    resolved_cfg = _resolve_config_at(options, at_date, {})
    cfg = {**raw_data, **resolved_cfg}

    utility_key = resolved_cfg.get(CONF_ENERGIEVERSORGER)
    kw = float(resolved_cfg.get(CONF_INSTALLIERTE_LEISTUNG_KW) or 0.0)
    eigenverbrauch = bool(resolved_cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT))
    hkn_aktiviert = bool(resolved_cfg.get(CONF_HKN_AKTIVIERT))

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


def _resolve_config_at(options: dict, at_date, fallback_cfg: dict) -> dict:
    """Pick the full config dict active at ``at_date``.

    Reads ``OPT_CONFIG_HISTORY``; falls back to ``fallback_cfg`` (typically
    ``{}``) when the history list is empty or when ``at_date`` predates every
    record's ``valid_from``. Both branches are unreachable in normal operation
    — ``async_setup_entry`` synthesizes a 1970 sentinel that always matches
    — but the warning makes the degenerate state observable if it ever happens.
    """
    history = options.get(OPT_CONFIG_HISTORY) or []
    if not history:
        return {k: fallback_cfg.get(k) for k in CONFIG_HISTORY_FIELDS}
    rec = find_active(history, at_date)
    if rec is not None:
        return rec["config"]
    _LOGGER.warning(
        "config-history: %s predates earliest record (%s); using fallback. "
        "This usually means the 1970 sentinel is missing — check "
        "OPT_CONFIG_HISTORY in the config entry options.",
        at_date, history[0]["valid_from"],
    )
    return {k: fallback_cfg.get(k) for k in CONFIG_HISTORY_FIELDS}


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


def _aggregate_by_period(
    records, rhythm: str | None, intended_hkn_rp_kwh: float | None = None
) -> list[dict]:
    """Bucket per-hour records by Zurich-local period. Pure function.

    ``rhythm == "quartal"`` → bucket key ``YYYYQN`` (e.g. ``2026Q1``).
    Anything else (including ``"monat"`` and ``None``) → bucket key
    ``YYYY-MM``. Avg rates are kWh-weighted; ``None`` for zero-export
    periods. The Base/HKN columns decompose the total (``base + hkn = total``
    within rounding).

    ``intended_hkn_rp_kwh`` (when given) is stored per-period so the renderer
    can detect cap-forfeiture by comparing applied (kWh-weighted avg) against
    the intended utility-published rate that was active when the records were
    computed.
    """
    from zoneinfo import ZoneInfo

    z = ZoneInfo("Europe/Zurich")
    quarterly = rhythm == ABRECHNUNGS_RHYTHMUS_QUARTAL
    buckets: dict[str, dict[str, float]] = defaultdict(
        lambda: {"kwh": 0.0, "chf": 0.0, "base_chf": 0.0, "hkn_chf": 0.0}
    )
    for r in records:
        local = r.start.astimezone(z)
        if quarterly:
            key = f"{local.year}Q{(local.month - 1) // 3 + 1}"
        else:
            key = local.strftime("%Y-%m")
        b = buckets[key]
        b["kwh"] += r.kwh
        b["chf"] += r.compensation_chf
        # Component CHF accumulators (only present when HourRecord carries
        # the breakdown; pre-v0.7.5 records would lack these attrs).
        base_rp = getattr(r, "base_rp_kwh", None)
        hkn_rp = getattr(r, "hkn_rp_kwh", None)
        if base_rp is not None and hkn_rp is not None:
            b["base_chf"] += r.kwh * base_rp / 100.0
            b["hkn_chf"] += r.kwh * hkn_rp / 100.0

    out: list[dict] = []
    for key in sorted(buckets):
        b = buckets[key]
        kwh = b["kwh"]
        avg_rate = (b["chf"] * 100.0 / kwh) if kwh > 0 else None
        avg_base = (b["base_chf"] * 100.0 / kwh) if kwh > 0 else None
        avg_hkn = (b["hkn_chf"] * 100.0 / kwh) if kwh > 0 else None
        out.append(
            {
                "period": key,
                "kwh": round(kwh, 3),
                "chf": round(b["chf"], 4),
                "rate_rp_kwh_avg": round(avg_rate, 4) if avg_rate is not None else None,
                "base_rp_kwh_avg": round(avg_base, 4) if avg_base is not None else None,
                "hkn_rp_kwh_avg": round(avg_hkn, 4) if avg_hkn is not None else None,
                "intended_hkn_rp_kwh": (
                    round(intended_hkn_rp_kwh, 4)
                    if intended_hkn_rp_kwh is not None
                    else None
                ),
            }
        )
    return out


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
    try:
        entry_data = _first_entry_data(hass)
    except RuntimeError:
        return
    coordinator = entry_data.get("coordinator")
    if coordinator is None:
        return

    rt = tariff_cfg.resolved
    cfg = entry_data.get("config") or {}
    billing = cfg.get(CONF_ABRECHNUNGS_RHYTHMUS)

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

    total_chf = plan.final_sum_chf - plan.anchor_sum_chf

    snapshot = {
        "rate_rp_kwh": round(rate_rp_kwh, 4),
        "kw": tariff_cfg.installierte_leistung_kw,
        "eigenverbrauch_aktiviert": tariff_cfg.eigenverbrauch_aktiviert,
        "hkn_rp_kwh": tariff_cfg.hkn_rp_kwh_resolved,
        "hkn_optin": tariff_cfg.hkn_aktiviert,
        "cap_mode": rt.cap_mode,
        "cap_rp_kwh": rt.cap_rp_kwh,
        "cap_applied": bool(cap_applied),
        "total_kwh": round(total_kwh, 3),
        "total_chf": round(total_chf, 4),
        "periods": _aggregate_by_period(
            plan.records, billing,
            intended_hkn_rp_kwh=tariff_cfg.hkn_rp_kwh_resolved,
        ),
        "utility_key": rt.utility_key,
        "base_model": rt.base_model,
        "billing": billing,
        "floor_label": rt.federal_floor_label,
        "floor_rp_kwh": rt.federal_floor_rp_kwh,
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


async def _reimport_all_history(hass: "HomeAssistant") -> dict:
    """Re-import every quarter BFE has published, then estimate the running quarter.

    v0.9.2 changes:
    - **Clear-then-rewrite**: wipes the compensation LTS chain and the
      in-memory ``coordinator._imported`` snapshot map at the top, so the
      first run after a fresh install is idempotent (no leftover rows from
      HA's energy-component auto-compensation polluting the cumulative sum).
    - **Pre-valid_from skip**: quarters whose Zurich-local start date predates
      the earliest ``OPT_CONFIG_HISTORY`` record's ``valid_from`` are silently
      skipped — there's no config to compute a tariff for them, and the user's
      plant didn't exist yet anyway.

    Result dict keys:
    - ``available``: sorted list of all Quarters BFE has published
    - ``imported``: list of Quarters successfully recomputed from BFE prices
    - ``skipped``: list of Quarters skipped because BFE has not yet published them
    - ``failed``: list of Quarters that errored unexpectedly
    - ``estimated``: list of Quarters written via the running-quarter estimate
      (typically the current in-progress quarter; empty if BFE already published it
      or the estimate failed)
    - ``before_active``: list of Quarters skipped because they predate
      ``OPT_CONFIG_HISTORY[0].valid_from`` (the plant install date)
    """
    import aiohttp
    from datetime import date, datetime, timezone

    from homeassistant.components.recorder import get_instance
    from homeassistant.components.recorder.statistics import clear_statistics

    # Clear LTS + snapshot map first so the rewrite below is the sole source
    # of truth. Source kWh data (export sensor LTS) is untouched.
    # Read comp_id from entry.data directly (NOT _cfg_for_entry) — the
    # latter requires a resolvable utility, which isn't guaranteed at this
    # point if the user has a degenerate history.
    entry_data = _first_entry_data(hass)
    comp_id = entry_data["config"][CONF_RUECKLIEFERVERGUETUNG_CHF]
    instance = get_instance(hass)
    await hass.async_add_executor_job(clear_statistics, instance, [comp_id])

    coordinator = entry_data.get("coordinator")
    if coordinator is not None:
        coordinator._imported.clear()
        await coordinator._async_save_state()

    # Earliest valid_from = plant install date (sentinel anchor). Quarters
    # before this date are skipped — no config covers them.
    options = entry_data.get("options") or {}
    history = options.get(OPT_CONFIG_HISTORY) or []
    earliest_date: date | None = None
    if history:
        try:
            earliest_date = date.fromisoformat(history[0]["valid_from"])
        except (KeyError, ValueError, TypeError):
            earliest_date = None

    async with aiohttp.ClientSession() as session:
        quarterly = await fetch_quarterly(session)

    imported: list = []
    skipped: list = []
    failed: list = []
    before_active: list = []
    for q in sorted(quarterly.keys()):
        if earliest_date is not None:
            q_start_local = date(q.year, ((q.q - 1) * 3) + 1, 1)
            if q_start_local < earliest_date:
                before_active.append(q)
                continue
        try:
            await _reimport_quarter(hass, q)
            imported.append(q)
        except PriceNotYetPublished as exc:
            _LOGGER.warning("Skipping %s: %s", q, exc)
            skipped.append(q)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error("Failed importing %s: %s", q, exc)
            failed.append(q)

    # Append the running quarter as a conservative estimate when BFE hasn't
    # published it yet — uses the active utility's tariff settings (fixed
    # price, HKN, federal floor, or previous-quarter reference) so the user
    # sees realistic CHF in the Energy Dashboard immediately.
    estimated: list = []
    running_q = quarter_of(datetime.now(timezone.utc))
    if running_q not in imported:
        try:
            await _import_running_quarter_estimate(hass)
            estimated.append(running_q)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning("Running-quarter estimate failed: %s", exc)

    return {
        "available": sorted(quarterly.keys()),
        "imported": imported,
        "skipped": skipped,
        "failed": failed,
        "estimated": estimated,
        "before_active": before_active,
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

    cfg, tariff_cfg = _cfg_for_entry(hass)
    export_id = cfg[CONF_STROMNETZEINSPEISUNG_KWH]
    comp_id = cfg[CONF_RUECKLIEFERVERGUETUNG_CHF]
    billing = cfg.get(CONF_ABRECHNUNGS_RHYTHMUS)

    entry_data = _first_entry_data(hass)
    coordinator = entry_data.get("coordinator")
    if coordinator is None or not coordinator.data:
        raise RuntimeError(
            "Coordinator not ready — run 'Refresh prices from BFE' first"
        )
    breakdown = coordinator.data.get("tariff_breakdown")
    if not breakdown:
        raise RuntimeError("Tariff breakdown unavailable")

    rate_rp_kwh = float(breakdown["effective_rp_kwh"])
    is_estimate = bool(breakdown.get("is_estimate", False))

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
        }

    hourly_kwh = await read_hourly_export(hass, export_id, q_start_utc, last_full_hour)
    anchor = await read_compensation_anchor(
        hass, comp_id, q_start_utc - _one_hour()
    )

    rate_chf_kwh = rate_rp_kwh / 100.0
    running_sum = anchor
    records: list[tuple] = []
    total_kwh = 0.0
    for h in hours_in_range(q_start_utc, last_full_hour):
        kwh = hourly_kwh.get(h, 0.0)
        total_kwh += kwh
        running_sum += kwh * rate_chf_kwh
        records.append((h, running_sum))

    if records:
        await import_statistics(
            hass,
            build_metadata_compensation(comp_id),
            build_compensation_stats(records),
        )

    chf_total = running_sum - anchor

    # Snapshot so _build_recompute_report can render this quarter as an
    # "(estimate)" row alongside the published-quarter rows. Rate/HKN
    # decomposition isn't available from the breakdown — store rate as the
    # total and leave Base/HKN cells empty in the renderer.
    rt = tariff_cfg.resolved
    snapshot = {
        "rate_rp_kwh": round(rate_rp_kwh, 4),
        "kw": tariff_cfg.installierte_leistung_kw,
        "eigenverbrauch_aktiviert": tariff_cfg.eigenverbrauch_aktiviert,
        "hkn_rp_kwh": tariff_cfg.hkn_rp_kwh_resolved,
        "hkn_optin": tariff_cfg.hkn_aktiviert,
        "cap_mode": rt.cap_mode,
        "cap_rp_kwh": rt.cap_rp_kwh,
        "cap_applied": False,
        "total_kwh": round(total_kwh, 3),
        "total_chf": round(chf_total, 4),
        "periods": [
            {
                "period": str(q),
                "kwh": round(total_kwh, 3),
                "chf": round(chf_total, 4),
                "rate_rp_kwh_avg": round(rate_rp_kwh, 4),
                "base_rp_kwh_avg": None,
                "hkn_rp_kwh_avg": None,
                "intended_hkn_rp_kwh": None,
            }
        ],
        "utility_key": rt.utility_key,
        "base_model": rt.base_model,
        "billing": billing,
        "floor_label": rt.federal_floor_label,
        "floor_rp_kwh": rt.federal_floor_rp_kwh,
        "tariffs_json_version": rt.tariffs_json_version,
        "tariffs_json_source": rt.tariffs_json_source,
        "is_current_estimate": True,
    }
    coordinator._imported[str(q)] = {
        "q_price_chf_mwh": None,
        "imported_at": datetime.now(timezone.utc).isoformat(),
        "snapshot": snapshot,
    }
    hass.async_create_task(coordinator._async_save_state())

    _LOGGER.info(
        "Imported running %s estimate: rate=%.4f Rp/kWh, hours=%d, total=%.4f CHF (estimate=%s)",
        q, rate_rp_kwh, len(records), chf_total, is_estimate,
    )

    return {
        "quarter": str(q),
        "rate_rp_kwh": rate_rp_kwh,
        "hours_imported": len(records),
        "chf_total": chf_total,
        "is_estimate": is_estimate,
    }


async def _refresh_coordinator(hass: "HomeAssistant") -> dict:
    """Trigger a fresh BFE poll via the coordinator. Returns ``{available, newly_imported}``.

    The coordinator's ``_async_update_data`` fetches BFE prices and auto-imports
    quarters whose price changed since the last successful import. We snapshot
    its private ``_imported`` map before/after to report what was new this tick.
    """
    entry_data = _first_entry_data(hass)
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
    """Service handler for `bfe_rueckliefertarif.reimport_all_history`.

    v0.9.2: also emits the same recompute summary notification the OptionsFlow
    `recompute_history` step does, so calling the service via the YAML/Python
    API gives the same user-visible output.
    """
    hass = call.hass
    result = await _reimport_all_history(hass)
    quarters_for_report = list(result.get("imported", [])) + list(
        result.get("estimated", [])
    )
    if not quarters_for_report and not result.get("before_active"):
        return
    before_active = result.get("before_active") or []
    earliest: str | None = None
    try:
        entry_data = _first_entry_data(hass)
        history = (entry_data.get("options") or {}).get(OPT_CONFIG_HISTORY) or []
        if history:
            earliest = history[0].get("valid_from")
    except RuntimeError:
        pass
    report = _build_recompute_report(
        hass,
        quarters_for_report,
        before_active_count=len(before_active),
        before_active_earliest=earliest,
    )
    # Use the first non-underscore key in hass.data[DOMAIN] as the entry id.
    entry_id: str | None = None
    for key in (hass.data.get(DOMAIN) or {}):
        if not key.startswith("_"):
            entry_id = key
            break
    if entry_id:
        _notify_recompute(hass, entry_id, report)


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


# ----- Recompute report + notification (v0.7.0) ----------------------------


@dataclass(frozen=True)
class _RecomputeReportRow:
    """One row per billing period in the recompute notification table.

    ``period`` is ``YYYY-MM`` for monthly billing, ``YYYYQN`` for quarterly.
    Rate columns are kWh-weighted averages over the period; ``base + hkn``
    sums to ``rate_rp_kwh_avg`` within rounding (HKN may be 0 if the user
    didn't opt in or the cap forfeited it).

    v0.8.6: per-period config metadata (utility, kw, EV, HKN, billing, cap,
    floor, tariffs version) is carried alongside so multi-utility recompute
    notifications can group rows by config and render per-group headings.
    Fields are nullable to keep legacy snapshots renderable (group falls
    back to "(unknown)" for those rows).
    """

    period: str
    rate_rp_kwh_avg: float | None
    base_rp_kwh_avg: float | None
    hkn_rp_kwh_avg: float | None
    intended_hkn_rp_kwh: float | None
    total_kwh: float | None
    total_chf: float | None
    utility_key_at_period: str | None = None
    utility_name_at_period: str | None = None
    kw_at_period: float | None = None
    eigenverbrauch_at_period: bool | None = None
    hkn_optin_at_period: bool | None = None
    billing_at_period: str | None = None
    base_model_at_period: str | None = None
    cap_mode_at_period: bool | None = None
    cap_rp_kwh_at_period: float | None = None
    floor_label_at_period: str | None = None
    floor_rp_kwh_at_period: float | None = None
    tariffs_version_at_period: str | None = None
    tariffs_source_at_period: str | None = None
    # v0.9.0: marks rows from the running-quarter estimate (BFE not yet
    # published; rate is the active utility's effective floor).
    # v0.9.2: ``estimate_basis`` dropped — the renderer just appends a "*"
    # to the period cell + a single footnote line. The basis label is still
    # exposed on the live BasisVerguetungSensor's attributes.
    is_current_estimate: bool = False


@dataclass(frozen=True)
class _RecomputeReport:
    rows: list[_RecomputeReportRow]   # newest-first
    quarters_recomputed: int
    config: dict
    # v0.9.2: how many BFE-published quarters were skipped because they
    # predate OPT_CONFIG_HISTORY[0].valid_from (the plant install date).
    # When non-zero, the renderer emits a single footer line so the user
    # knows older quarters exist but were intentionally not imported.
    before_active_count: int = 0
    before_active_earliest: str | None = None


def _build_recompute_report(
    hass: "HomeAssistant",
    quarters: list[Quarter],
    *,
    before_active_count: int = 0,
    before_active_earliest: str | None = None,
) -> _RecomputeReport:
    """Pull current config + per-quarter snapshots into a render-ready report.

    Each quarter's snapshot carries a 3-element ``monthly`` list (added in
    v0.7); we flatten across quarters and sort newest-first. Old snapshots
    that pre-date the ``monthly`` field render as empty rows ("—" cells).
    """
    from .tariffs_db import load_tariffs

    cfg, tariff_cfg = _cfg_for_entry(hass)
    coordinator = _first_entry_data(hass).get("coordinator")
    rt = tariff_cfg.resolved

    db = load_tariffs()
    utility_meta = db["utilities"].get(rt.utility_key, {})
    header = {
        "utility_key": rt.utility_key,
        "utility_name": utility_meta.get("name_de", rt.utility_key),
        "base_model": rt.base_model,
        "settlement_period": rt.settlement_period,
        "kw": tariff_cfg.installierte_leistung_kw,
        "eigenverbrauch": tariff_cfg.eigenverbrauch_aktiviert,
        "hkn_optin": tariff_cfg.hkn_aktiviert,
        "hkn_rp_kwh": rt.hkn_rp_kwh,
        "billing": cfg.get(CONF_ABRECHNUNGS_RHYTHMUS),
        "floor_label": rt.federal_floor_label,
        "floor_rp_kwh": rt.federal_floor_rp_kwh,
        "cap_mode": rt.cap_mode,
        "cap_rp_kwh": rt.cap_rp_kwh,
        "tariffs_version": rt.tariffs_json_version,
        "tariffs_source": rt.tariffs_json_source,
    }

    rows: list[_RecomputeReportRow] = []
    if coordinator is not None:
        for q in sorted(quarters):
            snap = (coordinator._imported.get(str(q)) or {}).get("snapshot") or {}
            # Per-period config metadata — pulled from the snapshot so each
            # row carries the utility/cap/floor that was active at import
            # time. Defaults are None for legacy snapshots predating v0.8.6.
            snap_utility_key = snap.get("utility_key")
            snap_utility_name = (
                db["utilities"].get(snap_utility_key, {}).get("name_de")
                if snap_utility_key else None
            ) or snap_utility_key
            row_meta = {
                "utility_key_at_period": snap_utility_key,
                "utility_name_at_period": snap_utility_name,
                "kw_at_period": snap.get("kw"),
                "eigenverbrauch_at_period": snap.get("eigenverbrauch_aktiviert"),
                "hkn_optin_at_period": snap.get("hkn_optin"),
                "billing_at_period": snap.get("billing"),
                "base_model_at_period": snap.get("base_model"),
                "cap_mode_at_period": snap.get("cap_mode"),
                "cap_rp_kwh_at_period": snap.get("cap_rp_kwh"),
                "floor_label_at_period": snap.get("floor_label"),
                "floor_rp_kwh_at_period": snap.get("floor_rp_kwh"),
                "tariffs_version_at_period": snap.get("tariffs_json_version"),
                "tariffs_source_at_period": snap.get("tariffs_json_source"),
                "is_current_estimate": bool(snap.get("is_current_estimate", False)),
            }
            # Prefer v0.7.5+ "periods" key; fall back to legacy "monthly" so
            # snapshots from older imports still render (Base/HKN cells = —).
            periods = snap.get("periods") or [
                {**m, "period": m.get("month")} for m in (snap.get("monthly") or [])
            ]
            for p in periods:
                rows.append(
                    _RecomputeReportRow(
                        period=p.get("period") or p.get("month"),
                        rate_rp_kwh_avg=p.get("rate_rp_kwh_avg"),
                        base_rp_kwh_avg=p.get("base_rp_kwh_avg"),
                        hkn_rp_kwh_avg=p.get("hkn_rp_kwh_avg"),
                        intended_hkn_rp_kwh=p.get("intended_hkn_rp_kwh"),
                        total_kwh=p.get("kwh"),
                        total_chf=p.get("chf"),
                        **row_meta,
                    )
                )
    rows.sort(key=lambda r: r.period, reverse=True)
    return _RecomputeReport(
        rows=rows,
        quarters_recomputed=len(quarters),
        config=header,
        before_active_count=before_active_count,
        before_active_earliest=before_active_earliest,
    )


def _row_config_fingerprint(r: _RecomputeReportRow) -> tuple:
    """Group key — rows sharing this fingerprint render under one heading.

    v0.8.6: covers the rate-affecting fields (utility, kw, EV, HKN opt-in,
    billing). Cap mode / floor / tariffs version are presentation-only and
    don't change the group identity.
    """
    return (
        r.utility_key_at_period,
        r.kw_at_period,
        r.eigenverbrauch_at_period,
        r.hkn_optin_at_period,
        r.billing_at_period,
    )


def _group_rows_by_config(
    rows: list[_RecomputeReportRow],
) -> list[tuple[tuple, list[_RecomputeReportRow]]]:
    """Bucket rows by config fingerprint, preserving the input order.

    Group order = order in which each fingerprint first appeared in the
    input list. Since ``rows`` is newest-first, the group containing the
    most-recent row comes first.
    """
    buckets: dict[tuple, list[_RecomputeReportRow]] = {}
    order: list[tuple] = []
    for r in rows:
        key = _row_config_fingerprint(r)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(r)
    return [(key, buckets[key]) for key in order]


def _render_config_block(c: dict, *, is_today: bool = False) -> list[str]:
    """Shared bullet-list renderer used by both the active-today block and
    each per-group "Configuration in effect" block (v0.9.2).

    Expected dict keys (any may be missing — None-guarded throughout):
    - ``utility_key`` (str), ``utility_name`` (str)
    - ``base_model`` (str), ``settlement_period`` (str)
    - ``kw`` (float), ``eigenverbrauch`` (bool), ``hkn_optin`` (bool)
    - ``hkn_rp_kwh`` (float, only used when ``hkn_optin`` is True)
    - ``billing`` (str)
    - ``floor_label`` (str), ``floor_rp_kwh`` (float)
    - ``cap_mode`` (bool), ``cap_rp_kwh`` (float)
    - ``tariffs_version`` (str), ``tariffs_source`` (str)

    ``is_today=True`` causes the cap line to read "Active — current cap …"
    (today's value); otherwise it reads "Active — cap …" (the snapshot's
    value at import time).
    """
    lines: list[str] = []
    utility_key = c.get("utility_key") or "(unknown)"
    utility_name = c.get("utility_name") or utility_key
    lines.append(f"- **Utility:** {utility_key} — {utility_name}")

    base_model = c.get("base_model")
    settlement = c.get("settlement_period")
    if base_model and settlement:
        lines.append(f"- **Tariff model:** {base_model} (settlement: {settlement})")
    elif base_model:
        lines.append(f"- **Tariff model:** {base_model}")

    kw = c.get("kw")
    if kw is not None:
        lines.append(f"- **Installed power:** {kw:.1f} kW")
    else:
        lines.append("- **Installed power:** —")

    ev = c.get("eigenverbrauch")
    ev_str = "Yes" if ev else ("No" if ev is False else "—")
    lines.append(f"- **Eigenverbrauch (self-consumption):** {ev_str}")

    hkn_optin = c.get("hkn_optin")
    if hkn_optin:
        hkn_rp = c.get("hkn_rp_kwh")
        if hkn_rp is not None:
            lines.append(f"- **HKN opt-in:** Yes ({hkn_rp:.2f} Rp/kWh additive)")
        else:
            lines.append("- **HKN opt-in:** Yes")
    elif hkn_optin is False:
        lines.append("- **HKN opt-in:** No")
    else:
        lines.append("- **HKN opt-in:** —")

    billing = c.get("billing")
    lines.append(f"- **Billing period:** {billing or '—'}")

    floor_label = c.get("floor_label")
    if floor_label:
        floor_v = c.get("floor_rp_kwh")
        suffix = f" ({floor_v:.2f} Rp/kWh)" if floor_v is not None else " (none)"
        lines.append(
            f"- **Federal floor (Mindestvergütung):** {floor_label}{suffix}"
        )

    cap_mode = c.get("cap_mode")
    if cap_mode:
        cap_v = c.get("cap_rp_kwh")
        cap_str = f"{cap_v:.2f} Rp/kWh" if cap_v is not None else "n/a"
        kw_str = f"{kw:.1f} kW" if kw is not None else "—"
        cap_ev_str = "Yes" if ev else ("No" if ev is False else "—")
        cap_label = "current cap" if is_today else "cap"
        lines.append(
            f"- **Cap mode (Anrechenbarkeitsgrenze):** Active — {cap_label} "
            f"{cap_str} ({kw_str}, EV={cap_ev_str})"
        )
    elif cap_mode is False:
        lines.append("- **Cap mode (Anrechenbarkeitsgrenze):** Off")

    tv = c.get("tariffs_version")
    ts = c.get("tariffs_source")
    if tv:
        src = f" ({ts})" if ts else ""
        lines.append(f"- **Tariff data:** v{tv}{src}")
    return lines


def _render_active_today_block(c: dict) -> list[str]:
    """The 'Active configuration (today)' header block — one per report."""
    return [
        "## Active configuration (today)",
        *_render_config_block(c, is_today=True),
    ]


def _period_bounds(period: str) -> tuple[str, str] | None:
    """Parse ``YYYYQN`` or ``YYYY-MM`` → ``(start_iso, end_iso)`` (end inclusive
    last day). Returns ``None`` if unparseable. Used by the date-bounded
    "Configuration in effect: X → Y" group heading."""
    from datetime import date, timedelta

    s = period.strip()
    if "Q" in s:
        try:
            year_str, q_str = s.split("Q", 1)
            year = int(year_str)
            qn = int(q_str)
            if qn not in (1, 2, 3, 4):
                return None
            start_month = (qn - 1) * 3 + 1
            start = date(year, start_month, 1)
            if qn == 4:
                next_q = date(year + 1, 1, 1)
            else:
                next_q = date(year, start_month + 3, 1)
            end = next_q - timedelta(days=1)
            return start.isoformat(), end.isoformat()
        except (ValueError, TypeError):
            return None
    if "-" in s and len(s) == 7:
        try:
            year_str, month_str = s.split("-", 1)
            year = int(year_str)
            month = int(month_str)
            start = date(year, month, 1)
            if month == 12:
                next_m = date(year + 1, 1, 1)
            else:
                next_m = date(year, month + 1, 1)
            end = next_m - timedelta(days=1)
            return start.isoformat(), end.isoformat()
        except (ValueError, TypeError):
            return None
    return None


def _render_group_heading(
    fingerprint: tuple,
    sample_row: _RecomputeReportRow,
    group_rows: list[_RecomputeReportRow],
) -> list[str]:
    """v0.9.2: date-bounded heading + the same bullet-list as the active-today
    block. Replaces the prior horizontal one-liner.

    Date range is derived from the rows' period strings (the renderer is
    hass-free by design). When the group contains an ``is_current_estimate``
    row, the end-of-range is rendered as ``now`` instead of the period's
    last calendar day.
    """
    utility_key, kw, ev, hkn_optin, billing = fingerprint

    # Earliest period start, latest period end (or "now" for open groups).
    starts: list[str] = []
    ends: list[str] = []
    has_estimate = False
    for r in group_rows:
        if r.is_current_estimate:
            has_estimate = True
        bounds = _period_bounds(r.period or "")
        if bounds is None:
            continue
        starts.append(bounds[0])
        ends.append(bounds[1])

    if starts:
        start_str = min(starts)
        end_str = "now" if has_estimate else max(ends)
        heading = f"## Configuration in effect: {start_str} → {end_str}"
    else:
        # Defensive — shouldn't happen with v0.7.5+ snapshots.
        heading = "## Configuration in effect: —"

    config_dict = {
        "utility_key": utility_key,
        "utility_name": sample_row.utility_name_at_period,
        "base_model": sample_row.base_model_at_period,
        # settlement_period not carried per-row; omit (renderer handles None).
        "settlement_period": None,
        "kw": kw,
        "eigenverbrauch": ev,
        "hkn_optin": hkn_optin,
        # The intended HKN rate from the snapshot is the "published" value;
        # display it next to "Yes" when opted-in.
        "hkn_rp_kwh": sample_row.intended_hkn_rp_kwh,
        "billing": billing,
        "floor_label": sample_row.floor_label_at_period,
        "floor_rp_kwh": sample_row.floor_rp_kwh_at_period,
        "cap_mode": sample_row.cap_mode_at_period,
        "cap_rp_kwh": sample_row.cap_rp_kwh_at_period,
        "tariffs_version": sample_row.tariffs_version_at_period,
        "tariffs_source": sample_row.tariffs_source_at_period,
    }
    return [heading, *_render_config_block(config_dict)]


def _render_period_table(
    rows: list[_RecomputeReportRow],
) -> tuple[list[str], list[_RecomputeReportRow]]:
    """The per-period markdown table for one group. Returns (lines, forfeit_rows).

    v0.9.2: estimate rows render as ``YYYYQN *`` (compact asterisk anchor)
    instead of the prior wide ``*(estimate · …)*`` decoration. When any row
    in the table is ``is_current_estimate``, a single footnote line is
    appended below the table explaining what the asterisk means.
    """
    lines = [
        "_Rates in Rp/kWh; energy in kWh; CHF totals._",
        "",
        "| Period | Base | HKN | Total | kWh | CHF |",
        "|---|---|---|---|---|---|",
    ]
    forfeit_rows: list[_RecomputeReportRow] = []
    has_estimate = False
    for r in rows:
        base = f"{r.base_rp_kwh_avg:.3f}" if r.base_rp_kwh_avg is not None else "—"
        intended = r.intended_hkn_rp_kwh
        applied = r.hkn_rp_kwh_avg
        is_forfeit = (
            intended is not None
            and intended > 0
            and applied is not None
            and applied < intended - 1e-4
        )
        if applied is None:
            hkn = "—"
        elif is_forfeit:
            hkn = f"{applied:.3f} / {intended:.2f}"
            forfeit_rows.append(r)
        else:
            hkn = f"{applied:.3f}"
        rate = f"{r.rate_rp_kwh_avg:.3f}" if r.rate_rp_kwh_avg is not None else "—"
        kwh = f"{r.total_kwh:,.2f}" if r.total_kwh is not None else "—"
        chf = f"{r.total_chf:,.2f}" if r.total_chf is not None else "—"
        if r.is_current_estimate:
            has_estimate = True
            period_cell = f"{r.period} *"
        else:
            period_cell = r.period
        lines.append(f"| {period_cell} | {base} | {hkn} | {rate} | {kwh} | {chf} |")
    if has_estimate:
        lines.append("")
        lines.append(
            "_* Estimated from today's kWh production (running quarter). "
            "Rates may vary._"
        )
    return lines, forfeit_rows


def _format_recompute_notification(report: _RecomputeReport) -> tuple[str, str]:
    """Pure function: report → ``(title, markdown_body)``. Easy to unit-test.

    v0.8.6: groups rows by config fingerprint and renders one section per
    distinct config used. The "Active configuration (today)" block always
    appears at the top; per-config sections follow in newest-first order.
    When the recompute spans only one config and that config matches
    today's, the per-config heading is suppressed so single-utility output
    looks identical to v0.8.5.
    """
    n_periods = len(report.rows)
    n_q = report.quarters_recomputed

    def _plural(n: int, singular: str, plural: str) -> str:
        return singular if n == 1 else plural

    if n_q == 1 and n_periods == 1:
        title = "Tariff recomputed — 1 period"
    elif n_q == 1:
        title = f"Tariff recomputed — 1 quarter ({n_periods} periods)"
    else:
        title = (
            f"Tariff history recomputed — {n_periods} "
            f"{_plural(n_periods, 'period', 'periods')} across {n_q} "
            f"{_plural(n_q, 'quarter', 'quarters')}"
        )

    c = report.config
    lines = _render_active_today_block(c)

    LIMIT = 24
    if n_periods > LIMIT:
        shown_rows = report.rows[:LIMIT]
        elided = n_periods - LIMIT
    else:
        shown_rows = report.rows
        elided = 0

    groups = _group_rows_by_config(shown_rows)

    # Suppress the redundant per-group heading when there's exactly one
    # group and it matches today's active config — the active-today block
    # alone already says what's going on.
    today_fingerprint = (
        c.get("utility_key"),
        c.get("kw"),
        c.get("eigenverbrauch"),
        c.get("hkn_optin"),
        c.get("billing"),
    )
    suppress_per_group_heading = (
        len(groups) == 1 and groups[0][0] == today_fingerprint
    )

    all_forfeits: list[tuple[_RecomputeReportRow, _RecomputeReportRow]] = []

    if suppress_per_group_heading:
        lines.append("")
        lines.append("## Per-period results")
        lines.append("")
        table_lines, forfeit_rows = _render_period_table(shown_rows)
        lines.extend(table_lines)
        if forfeit_rows:
            lines.append("")
            published = c.get("hkn_rp_kwh")
            published_str = (
                f"{published:.2f} Rp/kWh" if published is not None else "n/a"
            )
            n_f = len(forfeit_rows)
            lines.append(
                f"_ℹ️ HKN was reduced or forfeited in {n_f} "
                f"{_plural(n_f, 'period', 'periods')} (cell shows "
                "`applied / published`) because base + HKN exceeded the "
                f"Anrechenbarkeitsgrenze. Published HKN: {published_str}._"
            )
    else:
        for fingerprint, group_rows in groups:
            sample = group_rows[0]
            lines.append("")
            lines.extend(_render_group_heading(fingerprint, sample, group_rows))
            lines.append("")
            table_lines, forfeit_rows = _render_period_table(group_rows)
            lines.extend(table_lines)
            if forfeit_rows:
                lines.append("")
                # Per-group published HKN (resolved at import time, captured
                # in the snapshot's intended_hkn_rp_kwh).
                published = sample.intended_hkn_rp_kwh
                published_str = (
                    f"{published:.2f} Rp/kWh" if published is not None else "n/a"
                )
                n_f = len(forfeit_rows)
                lines.append(
                    f"_ℹ️ HKN was reduced or forfeited in {n_f} "
                    f"{_plural(n_f, 'period', 'periods')} (cell shows "
                    "`applied / published`) because base + HKN exceeded the "
                    f"Anrechenbarkeitsgrenze. Published HKN: {published_str}._"
                )

    if elided:
        lines.append("")
        lines.append(f"_{elided} older period(s) not shown — see logs._")
    if n_q > 1:
        total_kwh = sum(r.total_kwh or 0 for r in report.rows)
        total_chf = sum(r.total_chf or 0 for r in report.rows)
        lines.append("")
        lines.append(
            f"**Totals:** {n_periods} {_plural(n_periods, 'period', 'periods')} · "
            f"{total_kwh:,.2f} kWh · {total_chf:,.2f} CHF."
        )
    if report.before_active_count > 0:
        lines.append("")
        anchor = report.before_active_earliest or "plant install date"
        lines.append(
            f"_{report.before_active_count} quarter(s) before plant install "
            f"({anchor}) — not imported._"
        )
    lines.append("")
    lines.append("_For per-hour inspection, see `tools/verify_quarters.sql`._")
    return title, "\n".join(lines)


def _notify_recompute(
    hass: "HomeAssistant", entry_id: str, report: _RecomputeReport
) -> None:
    """Emit (or replace) the recompute summary notification for an entry."""
    from homeassistant.components.persistent_notification import async_create

    if not report.rows and report.quarters_recomputed == 0:
        return
    title, body = _format_recompute_notification(report)
    async_create(
        hass,
        body,
        title=title,
        notification_id=f"{DOMAIN}_{entry_id}_recompute_summary",
    )
