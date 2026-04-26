"""Service handlers: reimport_quarter, reimport_range, reimport_all_history, refresh."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

import voluptuous as vol

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


def _first_entry_data(hass: "HomeAssistant") -> dict:
    """Return the first config entry's storage dict.

    ``hass.data[DOMAIN]`` carries one slot per config entry (keyed by
    ``entry_id``) plus a shared ``_tariffs_data`` slot (the
    TariffsDataCoordinator from Phase 6). Skip underscore-prefixed keys
    so we always get an actual config entry.
    """
    entries = hass.data.get(DOMAIN, {})
    for key, value in entries.items():
        if not key.startswith("_") and isinstance(value, dict):
            return value
    raise RuntimeError("BFE Rückliefertarif not configured")


def _cfg_for_entry(
    hass: "HomeAssistant", *, for_quarter: Quarter | None = None
) -> tuple[dict, TariffConfig]:
    """Build the TariffConfig for a config entry, optionally as-of a quarter.

    When ``for_quarter`` is given, the per-entry config history (in
    ``entry.options[OPT_CONFIG_HISTORY]``) is consulted at the quarter's
    start date, so past quarters get the utility / kW / EV / HKN /
    billing-rhythm values that were active back then. When omitted,
    today's date is used.
    """
    from datetime import date

    entry_data = _first_entry_data(hass)
    cfg = entry_data["config"]
    options = entry_data.get("options") or {}

    if for_quarter is not None:
        at_date = date(for_quarter.year, ((for_quarter.q - 1) * 3) + 1, 1)
    else:
        at_date = date.today()

    resolved_cfg = _resolve_config_at(options, at_date, cfg)
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

    Reads ``OPT_CONFIG_HISTORY``; falls back to ``fallback_cfg`` (entry.data)
    only if the history list is empty (shouldn't happen post-setup since
    ``async_setup_entry`` synthesizes an initial sentinel record). When
    ``at_date`` predates the first record, returns the first record's config
    (best guess: that's what was used before any recorded change).
    """
    history = options.get(OPT_CONFIG_HISTORY) or []
    if not history:
        return {k: fallback_cfg.get(k) for k in CONFIG_HISTORY_FIELDS}
    rec = find_active(history, at_date)
    if rec is not None:
        return rec["config"]
    return history[0]["config"]


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


def _aggregate_by_period(records, rhythm: str | None) -> list[dict]:
    """Bucket per-hour records by Zurich-local period. Pure function.

    ``rhythm == "quartal"`` → bucket key ``YYYYQN`` (e.g. ``2026Q1``).
    Anything else (including ``"monat"`` and ``None``) → bucket key
    ``YYYY-MM``. Avg rates are kWh-weighted; ``None`` for zero-export
    periods. The Base/HKN columns decompose the total (``base + hkn = total``
    within rounding) so the user can see what the HKN opt-in contributed.
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
        "cap_applied": bool(cap_applied),
        "total_kwh": round(total_kwh, 3),
        "total_chf": round(total_chf, 4),
        "periods": _aggregate_by_period(plan.records, billing),
        "utility_key": rt.utility_key,
        "base_model": rt.base_model,
        "billing": billing,
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

    entry_data = _first_entry_data(hass)
    coordinator = entry_data.get("coordinator")
    if coordinator is None or not coordinator.data:
        raise RuntimeError(
            "Coordinator not ready — run 'Reload data' first"
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


# ----- Recompute report + notification (v0.7.0) ----------------------------


@dataclass(frozen=True)
class _RecomputeReportRow:
    """One row per billing period in the recompute notification table.

    ``period`` is ``YYYY-MM`` for monthly billing, ``YYYYQN`` for quarterly.
    Rate columns are kWh-weighted averages over the period; ``base + hkn``
    sums to ``rate_rp_kwh_avg`` within rounding (HKN may be 0 if the user
    didn't opt in or the cap forfeited it).
    """

    period: str
    rate_rp_kwh_avg: float | None
    base_rp_kwh_avg: float | None
    hkn_rp_kwh_avg: float | None
    total_kwh: float | None
    total_chf: float | None


@dataclass(frozen=True)
class _RecomputeReport:
    rows: list[_RecomputeReportRow]   # newest-first
    quarters_recomputed: int
    config: dict


def _build_recompute_report(
    hass: "HomeAssistant", quarters: list[Quarter]
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
        "tariffs_version": rt.tariffs_json_version,
        "tariffs_source": rt.tariffs_json_source,
    }

    rows: list[_RecomputeReportRow] = []
    if coordinator is not None:
        for q in sorted(quarters):
            snap = (coordinator._imported.get(str(q)) or {}).get("snapshot") or {}
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
                        total_kwh=p.get("kwh"),
                        total_chf=p.get("chf"),
                    )
                )
    rows.sort(key=lambda r: r.period, reverse=True)
    return _RecomputeReport(
        rows=rows,
        quarters_recomputed=len(quarters),
        config=header,
    )


def _format_recompute_notification(report: _RecomputeReport) -> tuple[str, str]:
    """Pure function: report → ``(title, markdown_body)``. Easy to unit-test."""
    n_periods = len(report.rows)
    n_q = report.quarters_recomputed
    if n_q == 1:
        title = f"Tariff recomputed — 1 quarter ({n_periods} periods)"
    else:
        title = (
            f"Tariff history recomputed — {n_periods} periods across {n_q} quarters"
        )

    c = report.config
    lines = [
        "## Current configuration",
        f"- **Utility:** {c['utility_key']} — {c['utility_name']}",
        f"- **Tariff model:** {c['base_model']} (settlement: {c['settlement_period']})",
        f"- **Installed power:** {c['kw']:.1f} kW",
        f"- **Eigenverbrauch (self-consumption):** "
        + ("Yes" if c["eigenverbrauch"] else "No"),
    ]
    if c["hkn_optin"]:
        lines.append(
            f"- **HKN opt-in:** Yes ({c['hkn_rp_kwh']:.2f} Rp/kWh additive)"
        )
    else:
        lines.append("- **HKN opt-in:** No")
    lines.append(f"- **Billing period:** {c['billing']}")

    if c["floor_label"]:
        floor = c["floor_rp_kwh"]
        suffix = f" ({floor:.2f} Rp/kWh)" if floor is not None else " (none)"
        lines.append(
            f"- **Federal floor (Mindestvergütung):** {c['floor_label']}{suffix}"
        )
    lines.append(
        "- **Cap mode (Anrechenbarkeitsgrenze):** "
        + ("Active" if c["cap_mode"] else "Off")
    )
    lines.append(
        f"- **Tariff data:** v{c['tariffs_version']} ({c['tariffs_source']})"
    )

    lines.append("")
    lines.append("## Per-period results")
    lines.append("")
    LIMIT = 24
    if n_periods > LIMIT:
        shown = report.rows[:LIMIT]
        elided = n_periods - LIMIT
    else:
        shown = report.rows
        elided = 0
    lines.append(
        "| Period | Base (Rp/kWh) | HKN (Rp/kWh) | Total (Rp/kWh) | "
        "kWh exported | CHF |"
    )
    lines.append("|---|---|---|---|---|---|")
    for r in shown:
        base = f"{r.base_rp_kwh_avg:.4f}" if r.base_rp_kwh_avg is not None else "—"
        hkn = f"{r.hkn_rp_kwh_avg:.4f}" if r.hkn_rp_kwh_avg is not None else "—"
        rate = f"{r.rate_rp_kwh_avg:.4f}" if r.rate_rp_kwh_avg is not None else "—"
        kwh = f"{r.total_kwh:,.2f}" if r.total_kwh is not None else "—"
        chf = f"{r.total_chf:,.2f}" if r.total_chf is not None else "—"
        lines.append(f"| {r.period} | {base} | {hkn} | {rate} | {kwh} | {chf} |")
    if elided:
        lines.append("")
        lines.append(f"_{elided} older period(s) not shown — see logs._")
    if n_q > 1:
        total_kwh = sum(r.total_kwh or 0 for r in report.rows)
        total_chf = sum(r.total_chf or 0 for r in report.rows)
        lines.append("")
        lines.append(
            f"**Totals:** {n_periods} periods · {total_kwh:,.2f} kWh · "
            f"{total_chf:,.2f} CHF."
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
