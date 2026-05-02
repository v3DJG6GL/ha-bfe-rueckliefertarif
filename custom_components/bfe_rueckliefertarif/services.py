"""Service handlers: reimport_all_history, refresh_data, refresh_tariffs."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date
from typing import TYPE_CHECKING

from .bfe import PriceNotYetPublishedError, fetch_monthly, fetch_quarterly
from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KWP,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    DOMAIN,
    OPT_CONFIG_HISTORY,
    build_history_config,
)
from .ha_recorder import (
    build_compensation_stats,
    build_metadata_compensation,
    import_statistics,
    read_compensation_anchor,
    read_hourly_export,
    read_post_quarter_sums,
)
from .importer import (
    QuarterSegment,
    TariffConfig,
    compute_quarter_plan_segmented,
    cumulative_sums,
)
from .quarters import Quarter, hours_in_range, quarter_bounds_utc, quarter_of
from .tariffs_db import (
    find_active,
    load_tariffs,
    pick_value_label,
    resolve_tariff_at,
    resolve_user_inputs_decl,
    settlement_period_label,
    tariff_model_label,
    user_input_label,
)

if TYPE_CHECKING:
    from datetime import timedelta

    from homeassistant.core import HomeAssistant, ServiceCall

_LOGGER = logging.getLogger(__name__)


async def async_register_services(hass: HomeAssistant) -> None:
    """Register services on first integration setup."""
    from homeassistant.core import SupportsResponse

    if hass.services.has_service(DOMAIN, "reimport_all_history"):
        # v0.9.6: drop the legacy ``refresh`` service if it survived from a
        # pre-v0.9.6 install of the integration in the same HA process. The
        # idempotency guard above means we're returning early on this code
        # path, so we still want to clean up — only register what's current.
        if hass.services.has_service(DOMAIN, "refresh"):
            hass.services.async_remove(DOMAIN, "refresh")
        return

    hass.services.async_register(
        DOMAIN, "reimport_all_history", _handle_reimport_all_history
    )
    hass.services.async_register(DOMAIN, "refresh_data", _handle_refresh_data)
    hass.services.async_register(
        DOMAIN, "refresh_tariffs", _handle_refresh_tariffs
    )
    # v0.19.0 — Tier 2 analytics service: returns the recompute report as a
    # JSON dict for consumption by the BFE tariff analysis Lovelace card.
    hass.services.async_register(
        DOMAIN,
        "get_breakdown",
        _handle_get_breakdown,
        supports_response=SupportsResponse.ONLY,
    )
    # v0.19.0 — Tier 1 diagnostic dump: same markdown report as
    # `recompute_history` but without any LTS rewrites.
    hass.services.async_register(
        DOMAIN, "show_report", _handle_show_report
    )


def _first_entry_data(hass: HomeAssistant) -> dict:
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
    hass: HomeAssistant, *, for_quarter: Quarter | None = None
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

    if for_quarter is not None:
        at_date = date(for_quarter.year, ((for_quarter.q - 1) * 3) + 1, 1)
    else:
        at_date = date.today()
    return _cfg_for_entry_at_date(hass, at_date)


def _cfg_for_entry_at_date(
    hass: HomeAssistant, at_date
) -> tuple[dict, TariffConfig]:
    """v0.9.9 — like ``_cfg_for_entry`` but takes an arbitrary calendar date,
    not just quarter starts. Used by ``_resolve_quarter_segments`` to build a
    per-segment ``TariffConfig`` mid-quarter.
    """
    entry_data = _first_entry_data(hass)
    raw_data = entry_data["config"]
    options = entry_data.get("options") or {}

    resolved_cfg = _resolve_config_at(options, at_date, {})
    cfg = {**raw_data, **resolved_cfg}

    utility_key = resolved_cfg.get(CONF_ENERGIEVERSORGER)
    kw = float(resolved_cfg.get(CONF_INSTALLIERTE_LEISTUNG_KWP) or 0.0)
    eigenverbrauch = bool(resolved_cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT))
    hkn_aktiviert = bool(resolved_cfg.get(CONF_HKN_AKTIVIERT))
    # v0.11.0 (Batch D) — declared user_inputs from the active history
    # record. Empty dict when the rate window declares nothing or the
    # record predates Batch D.
    user_inputs = dict(resolved_cfg.get("user_inputs") or {})

    resolved = resolve_tariff_at(
        utility_key,
        at_date,
        kw=kw,
        eigenverbrauch=eigenverbrauch,
        user_inputs=user_inputs,
    )
    hkn_resolved = resolved.hkn_rp_kwh if hkn_aktiviert else 0.0

    tariff_cfg = TariffConfig(
        eigenverbrauch_aktiviert=eigenverbrauch,
        installierte_leistung_kwp=kw,
        hkn_aktiviert=hkn_aktiviert,
        hkn_rp_kwh_resolved=hkn_resolved,
        resolved=resolved,
        user_inputs=user_inputs,
    )
    return cfg, tariff_cfg


def _resolve_quarter_segments(
    hass: HomeAssistant, q: Quarter
) -> list[QuarterSegment]:
    """Split ``q`` into contiguous (config × season) segments.

    A segment is a half-open ``[start_utc, end_utc)`` UTC range carrying a
    fully-resolved ``TariffConfig``. Boundaries come from two sources:

    1. ``OPT_CONFIG_HISTORY`` records whose ``valid_from`` falls strictly
       inside the quarter — each emits a new segment.
    2. Seasonal-month boundaries declared by the active rate window's
       ``seasonal.summer_months/winter_months`` — each summer↔winter
       transition inside the quarter emits a new segment.

    Single-segment quarters (typical case: one config record covering
    everything, no seasonal block) round-trip bytewise to the legacy
    ``compute_quarter_plan`` path. Multi-segment quarters drive sub-row
    rendering downstream.
    """
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    _zrh = ZoneInfo("Europe/Zurich")

    def _zurich_midnight_utc(d: date) -> datetime:
        """Zurich-local 00:00 of ``d`` expressed in UTC. Mirrors the convention
        used by ``quarter_bounds_utc`` / ``month_bounds_utc`` so segment
        boundaries align with the importer's hour iteration.
        """
        local = datetime(d.year, d.month, d.day, tzinfo=_zrh)
        return local.astimezone(ZoneInfo("UTC"))

    q_start_utc, q_end_utc = quarter_bounds_utc(q)
    q_start_date = date(q.year, ((q.q - 1) * 3) + 1, 1)
    # Calendar first-day-of-next-quarter (half-open upper bound).
    if q.q == 4:
        q_end_date = date(q.year + 1, 1, 1)
    else:
        q_end_date = date(q.year, q.q * 3 + 1, 1)

    options = _first_entry_data(hass).get("options") or {}
    history = options.get(OPT_CONFIG_HISTORY) or []

    # Boundary dates (calendar) where the config record changes inside the
    # quarter. Always include q_start as the first boundary.
    boundary_dates: list[date] = [q_start_date]
    for rec in history:
        f = date.fromisoformat(rec["valid_from"])
        if q_start_date < f < q_end_date:
            boundary_dates.append(f)
    boundary_dates = sorted(set(boundary_dates))

    # Now overlay seasonal boundaries. Resolve the cfg at each config-segment
    # start; if its rate window has a `seasonal` block, expand boundaries on
    # summer↔winter month transitions inside the segment.
    expanded: list[date] = []
    for i, seg_start in enumerate(boundary_dates):
        seg_end = (
            boundary_dates[i + 1] if i + 1 < len(boundary_dates) else q_end_date
        )
        expanded.append(seg_start)
        try:
            _, tariff_cfg = _cfg_for_entry_at_date(hass, seg_start)
        except (KeyError, LookupError, ValueError):
            continue
        seasonal = tariff_cfg.resolved.seasonal
        if not seasonal:
            continue
        summer_months = set(seasonal.get("summer_months") or [])
        winter_months = set(seasonal.get("winter_months") or [])
        if not summer_months or not winter_months:
            continue
        # Walk month-by-month inside [seg_start, seg_end), inserting
        # boundaries where the season label flips.
        cursor = seg_start
        prev_label: str | None = None
        while cursor < seg_end:
            month = cursor.month
            if month in summer_months:
                lbl = "summer"
            elif month in winter_months:
                lbl = "winter"
            else:
                lbl = None
            if prev_label is not None and lbl != prev_label and cursor != seg_start:
                expanded.append(cursor)
            prev_label = lbl
            # Advance to the first of the next month.
            if cursor.month == 12:
                cursor = date(cursor.year + 1, 1, 1)
            else:
                cursor = date(cursor.year, cursor.month + 1, 1)
    expanded = sorted(set(expanded))

    # Materialize segments. Each segment carries a stable seg_id like
    # "2026-01-01" (its start date) so downstream aggregation buckets are
    # human-readable.
    segments: list[QuarterSegment] = []
    for i, seg_start in enumerate(expanded):
        seg_end = expanded[i + 1] if i + 1 < len(expanded) else q_end_date
        try:
            _, tariff_cfg = _cfg_for_entry_at_date(hass, seg_start)
        except (KeyError, LookupError, ValueError):
            continue
        start_utc = _zurich_midnight_utc(seg_start)
        end_utc = _zurich_midnight_utc(seg_end)
        # Clip to quarter bounds (defensive — q_end_date math should already
        # match q_end_utc).
        if start_utc < q_start_utc:
            start_utc = q_start_utc
        if end_utc > q_end_utc:
            end_utc = q_end_utc
        if start_utc >= end_utc:
            continue
        segments.append(
            QuarterSegment(
                seg_id=seg_start.isoformat(),
                start_utc=start_utc,
                end_utc=end_utc,
                cfg=tariff_cfg,
            )
        )
    if not segments:
        # Fallback: if every per-record resolution failed, synthesize a
        # single segment from the quarter-start cfg so the caller still
        # gets a usable plan.
        _, tariff_cfg = _cfg_for_entry(hass, for_quarter=q)
        segments.append(
            QuarterSegment(
                seg_id="single",
                start_utc=q_start_utc,
                end_utc=q_end_utc,
                cfg=tariff_cfg,
            )
        )
    return segments


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
        return build_history_config(fallback_cfg)
    rec = find_active(history, at_date)
    if rec is not None:
        return rec["config"]
    _LOGGER.warning(
        "config-history: %s predates earliest record (%s); using fallback. "
        "This usually means the 1970 sentinel is missing — check "
        "OPT_CONFIG_HISTORY in the config entry options.",
        at_date, history[0]["valid_from"],
    )
    return build_history_config(fallback_cfg)


async def _reimport_quarter(
    hass: HomeAssistant,
    q: Quarter,
    *,
    anchor_override: float | None = None,
    force_fresh: bool = False,
) -> float:
    """Core re-import routine for a single quarter.

    Per-customer history (`plant_history` / `hkn_optin_history` in
    `entry.options`) is consulted via `_cfg_for_entry(for_quarter=q)`, so
    past quarters get the kW / EV / HKN-opt-in values that were active at
    the start of that quarter — not "today's" config. After the LTS write,
    a snapshot of the resolved values is recorded in
    `coordinator._imported[<quarter>]["snapshot"]`.

    `anchor_override` (v0.9.11): when set, skip the LTS anchor read and use
    the provided value as the plan's ``anchor_sum_chf``. Used by
    ``_reimport_all_history`` to thread the cumulative chain sum through
    memory, sidestepping the recorder commit-timer race that otherwise
    caused the first quarter after an inter-quarter HTTP cache hit to
    observe a stale (pre-commit) anchor of 0. ``None`` preserves the
    original behavior (read anchor from LTS), which is correct for the
    coordinator's stale-detect path where the chain is quiescent.

    `force_fresh` is reserved for v0.6: it will signal that the snapshot
    should be ignored and the rate fully recomputed (used after correcting
    a wrong tariff entry). For v0.5 the path is identical either way —
    history-driven recompute is the default.

    Returns ``plan.final_sum_chf`` so callers chaining multiple quarters
    can pass it as the next quarter's ``anchor_override``. Returns the
    override unchanged when no records were written (e.g. empty quarter).
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
        raise PriceNotYetPublishedError(f"BFE has not published 2026Q{q.q}/{q.year} yet")
    q_price = quarterly[q]

    hourly_kwh = await read_hourly_export(hass, export_id, q_start, q_end)
    if anchor_override is not None:
        # Cleared-chain path: the chain is empty (or being rewritten);
        # ``post_sums`` would always be empty too, so skip both reads.
        anchor = anchor_override
        post_sums: list = []
        old_first_post = None
    else:
        anchor = await read_compensation_anchor(
            hass, comp_id, q_start - _one_hour()
        )
        post_sums = await read_post_quarter_sums(
            hass, comp_id, q_end, q_end + _one_hour() * 24 * 365
        )
        old_first_post = post_sums[0][1] if post_sums else None

    segments = _resolve_quarter_segments(hass, q)
    plan = compute_quarter_plan_segmented(
        q=q,
        hourly_kwh=hourly_kwh,
        quarterly_price=q_price,
        monthly_prices=monthly,
        segments=segments,
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
    _record_snapshot(hass, q, q_price.chf_per_mwh, plan, tariff_cfg, segments=segments)
    return plan.final_sum_chf


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

    v0.9.9 — when records carry distinct ``seg_id`` values, each period bucket
    additionally accumulates per-segment kwh/CHF/base/hkn so downstream
    rendering can emit sub-rows. The top-level period totals always reflect
    the whole period (sum across all segments).
    """
    from zoneinfo import ZoneInfo

    z = ZoneInfo("Europe/Zurich")
    quarterly = rhythm == ABRECHNUNGS_RHYTHMUS_QUARTAL
    buckets: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "kwh": 0.0,
            "chf": 0.0,
            "base_chf": 0.0,
            "hkn_chf": 0.0,
            "bonus_chf": 0.0,
        }
    )
    # Sub-bucket per (period, seg_id) for sub-row rendering. Keyed by seg_id
    # (insertion-order preserved since Python 3.7), so the rendered sub-rows
    # appear in chronological-segment order.
    sub_buckets: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for r in records:
        local = r.start.astimezone(z)
        if quarterly:
            key = f"{local.year}Q{(local.month - 1) // 3 + 1}"
        else:
            key = local.strftime("%Y-%m")
        b = buckets[key]
        b["kwh"] += r.kwh
        b["chf"] += r.compensation_chf
        base_rp = getattr(r, "base_rp_kwh", None)
        hkn_rp = getattr(r, "hkn_rp_kwh", None)
        bonus_rp = getattr(r, "bonus_rp_kwh", 0.0) or 0.0
        base_chf_inc = r.kwh * base_rp / 100.0 if base_rp is not None else 0.0
        hkn_chf_inc = r.kwh * hkn_rp / 100.0 if hkn_rp is not None else 0.0
        bonus_chf_inc = r.kwh * bonus_rp / 100.0
        if base_rp is not None and hkn_rp is not None:
            b["base_chf"] += base_chf_inc
            b["hkn_chf"] += hkn_chf_inc
        b["bonus_chf"] += bonus_chf_inc

        seg_id = getattr(r, "seg_id", None)
        if seg_id is not None:
            sb = sub_buckets[key].setdefault(
                seg_id,
                {
                    "kwh": 0.0,
                    "chf": 0.0,
                    "base_chf": 0.0,
                    "hkn_chf": 0.0,
                    "bonus_chf": 0.0,
                },
            )
            sb["kwh"] += r.kwh
            sb["chf"] += r.compensation_chf
            if base_rp is not None and hkn_rp is not None:
                sb["base_chf"] += base_chf_inc
                sb["hkn_chf"] += hkn_chf_inc
            sb["bonus_chf"] += bonus_chf_inc

    out: list[dict] = []
    for key in sorted(buckets):
        b = buckets[key]
        kwh = b["kwh"]
        avg_rate = (b["chf"] * 100.0 / kwh) if kwh > 0 else None
        avg_base = (b["base_chf"] * 100.0 / kwh) if kwh > 0 else None
        avg_hkn = (b["hkn_chf"] * 100.0 / kwh) if kwh > 0 else None
        avg_bonus = (b["bonus_chf"] * 100.0 / kwh) if kwh > 0 else None
        period_dict: dict = {
            "period": key,
            "kwh": round(kwh, 3),
            "chf": round(b["chf"], 4),
            "rate_rp_kwh_avg": round(avg_rate, 4) if avg_rate is not None else None,
            "base_rp_kwh_avg": round(avg_base, 4) if avg_base is not None else None,
            "hkn_rp_kwh_avg": round(avg_hkn, 4) if avg_hkn is not None else None,
            "bonus_rp_kwh_avg": round(avg_bonus, 4) if avg_bonus is not None else None,
            "intended_hkn_rp_kwh": (
                round(intended_hkn_rp_kwh, 4)
                if intended_hkn_rp_kwh is not None
                else None
            ),
        }
        # Sub-rows: only emit when the period actually spans >1 segment.
        # Single-segment periods stay rendered as a single row (legacy shape).
        seg_map = sub_buckets.get(key) or {}
        if len(seg_map) > 1:
            sub_rows: list[dict] = []
            # Keep insertion order — segments are added in record order which
            # is chronological by hours_in_range.
            for seg_id, sb in seg_map.items():
                s_kwh = sb["kwh"]
                s_chf = sb["chf"]
                s_avg_rate = (s_chf * 100.0 / s_kwh) if s_kwh > 0 else None
                s_avg_base = (sb["base_chf"] * 100.0 / s_kwh) if s_kwh > 0 else None
                s_avg_hkn = (sb["hkn_chf"] * 100.0 / s_kwh) if s_kwh > 0 else None
                s_avg_bonus = (sb["bonus_chf"] * 100.0 / s_kwh) if s_kwh > 0 else None
                sub_rows.append(
                    {
                        "seg_id": seg_id,
                        "kwh": round(s_kwh, 3),
                        "chf": round(s_chf, 4),
                        "rate_rp_kwh_avg": round(s_avg_rate, 4) if s_avg_rate is not None else None,
                        "base_rp_kwh_avg": round(s_avg_base, 4) if s_avg_base is not None else None,
                        "hkn_rp_kwh_avg": round(s_avg_hkn, 4) if s_avg_hkn is not None else None,
                        "bonus_rp_kwh_avg": round(s_avg_bonus, 4) if s_avg_bonus is not None else None,
                    }
                )
            period_dict["sub_rows"] = sub_rows
        out.append(period_dict)
    return out


def _floor_source(rt) -> str:
    """``"utility"`` if utility floor binds higher than federal, else
    ``"federal"`` (or ``"federal"`` when both are absent — federal is the
    default narrative even when the value is None).
    """
    fed = rt.federal_floor_rp_kwh or 0.0
    utl = rt.price_floor_rp_kwh or 0.0
    return "utility" if utl > fed else "federal"


def _pick_note_text(text_dict: dict | None, lang: str) -> str | None:
    """Pick the best language match from a note's ``text`` dict.

    Order: user locale → ``de`` (Swiss default) → first available key.
    Returns ``None`` when ``text_dict`` is empty / missing.
    """
    if not text_dict:
        return None
    if lang in text_dict:
        return text_dict[lang]
    if "de" in text_dict:
        return text_dict["de"]
    return next(iter(text_dict.values()), None)


def _render_notes_lines(notes, lang: str) -> list[str]:
    """Render a list of active rate-window notes as recompute-block bullets.

    Returns ``[]`` when ``notes`` is empty / None. Otherwise emits a parent
    ``- **Notes:**`` line followed by one indented bullet per note formatted
    as ``  - *(severity)* text``. The severity is italicized so it doesn't
    compete with the surrounding bold field labels.
    """
    if not notes:
        return []
    lines: list[str] = []
    rendered_any = False
    for n in notes:
        text = _pick_note_text(n.get("text") if isinstance(n, dict) else None, lang)
        if not text:
            continue
        sev = n.get("severity", "info") if isinstance(n, dict) else "info"
        if not rendered_any:
            lines.append("- **Notes:**")
            rendered_any = True
        lines.append(f"  - *({sev})* {text}")
    return lines


_YES_NO = {
    "de": ("Ja", "Nein"),
    "fr": ("Oui", "Non"),
    "it": ("Sì", "No"),
}


def _yes_no(value: bool, lang: str) -> str:
    """Localized Yes/No for boolean rendering. Defaults to English."""
    pair = _YES_NO.get(lang, ("Yes", "No"))
    return pair[0] if value else pair[1]


def _format_user_input_value(decl: dict, value, lang: str) -> str:
    """Format a user-input value for display: booleans as Yes/No (localized);
    enums via ``value_labels_<lang>`` lookup; everything else stringified."""
    dtype = decl.get("type") if isinstance(decl, dict) else None
    if dtype == "boolean" and isinstance(value, bool):
        return _yes_no(value, lang)
    if dtype == "enum" and decl:
        return pick_value_label(decl, str(value), lang)
    return str(value)


def _render_when_summary(
    clause: dict | None,
    decls: list | tuple | None = None,
    lang: str = "en",
) -> str:
    """Compact one-line summary of a ``when_clause`` for display in the
    recompute config block. Returns ``""`` when the clause is empty/None.

    Schema vocabulary (v1.1.0): ``season`` and/or ``user_inputs``. v0.16.1:
    when ``decls`` is provided, ``user_inputs`` keys/values are
    label-translated (e.g. ``regio_top40_opted_in=True`` →
    ``Wahltarif TOP-40 abonniert=Ja``).
    """
    if not clause:
        return ""
    parts: list[str] = []
    season = clause.get("season")
    if season:
        parts.append(f"season={season}")
    sub = clause.get("user_inputs") or {}
    decl_by_key = (
        {d.get("key"): d for d in decls if isinstance(d, dict)}
        if decls else {}
    )
    for k, v in sub.items():
        decl = decl_by_key.get(k, {})
        label = user_input_label(decl, lang) if decl else k
        value_str = _format_user_input_value(decl, v, lang)
        parts.append(f"{label}={value_str}")
    return ", ".join(parts)


def _render_bonuses_lines(
    bonuses,
    decls: list | tuple | None = None,
    lang: str = "en",
) -> list[str]:
    """Render rate-window-level bonuses as recompute-block bullets.

    v0.10.0 / Batch C — list of declared bonuses (display-only).
    v0.11.0 / Batch D — adds the bonus ``kind`` and a ``when={…}`` summary.
    v0.16.1 — multiplier_pct rendered as ``+8.00%`` / ``−15.00%``; ``note``
    suffix dropped.
    v0.17.0 — when-clause dropped. The condition under which a bonus applies
    duplicated the ``Active user inputs:`` line above and read as current
    state (e.g. ``when ...=Ja`` while the user actually has Nein), which
    confused users. The ``(opt-in)`` / ``(always)`` ``applies_when``
    annotation stays — compact and unambiguous.

    Returns ``[]`` when ``bonuses`` is empty / None.
    """
    if not bonuses:
        return []
    lines: list[str] = ["- **Bonuses:**"]
    for b in bonuses:
        if not isinstance(b, dict):
            continue
        name = b.get("name") or "—"
        kind = b.get("kind", "additive_rp_kwh")
        if kind == "multiplier_pct":
            mp = b.get("multiplier_pct")
            if isinstance(mp, (int, float)) and not isinstance(mp, bool):
                delta = float(mp) - 100.0
                sign = "+" if delta >= 0 else "−"
                value_str = f"{sign}{abs(delta):.2f}%"
            else:
                value_str = "—"
        else:
            rate = b.get("rate_rp_kwh")
            value_str = (
                f"{rate:.2f} Rp/kWh"
                if isinstance(rate, (int, float)) and not isinstance(rate, bool)
                else "—"
            )
        applies = b.get("applies_when")
        if applies == "always":
            applies_part = " (always)"
        elif applies == "opt_in":
            applies_part = " (opt-in)"
        else:
            applies_part = ""
        lines.append(f"  - {name}: {value_str}{applies_part}")
    return lines


_MONTH_ABBR_EN = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _format_segment_label(seg_id: str | None, meta: dict | None) -> str:
    """Build the period-cell label for one sub-row.

    Falls back through several shapes:
    - With full ``meta`` (utility name + date range) → ``"Jan 1 – Feb 14
      (Utility A)"``.
    - With only ``seg_id`` (start date) → ``"from 2026-02-15"``.
    - Neither → ``"segment"``.
    """
    if not meta:
        if seg_id:
            return f"from {seg_id}"
        return "segment"
    f_iso = meta.get("valid_from") or seg_id or ""
    t_iso = meta.get("valid_to") or ""
    utility_name = meta.get("utility_name") or meta.get("utility_key") or ""
    f_disp = _short_date(f_iso) if f_iso else "?"
    # Sub-row range is half-open at month/quarter boundary; render the inclusive
    # last day so the user reads it naturally.
    if t_iso:
        from datetime import date, timedelta

        try:
            t_excl = date.fromisoformat(t_iso)
            t_incl = t_excl - timedelta(days=1)
            t_disp = _short_date(t_incl.isoformat())
        except ValueError:
            t_disp = _short_date(t_iso)
    else:
        t_disp = "?"
    range_str = f"{f_disp} – {t_disp}"
    return f"{range_str} ({utility_name})" if utility_name else range_str


def _short_date(iso: str) -> str:
    """Compact date label: ``2026-02-15`` → ``Feb 15``."""
    from datetime import date

    try:
        d = date.fromisoformat(iso)
    except ValueError:
        return iso
    return f"{_MONTH_ABBR_EN[d.month]} {d.day}"


def _record_snapshot(
    hass: HomeAssistant,
    q: Quarter,
    q_price_chf_mwh: float,
    plan,
    tariff_cfg: TariffConfig,
    segments: list | None = None,
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
        "kwp": tariff_cfg.installierte_leistung_kwp,
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
        "utility_floor_rp_kwh": rt.price_floor_rp_kwh,
        "floor_source": _floor_source(rt),
        "tariffs_json_version": rt.tariffs_json_version,
        "tariffs_json_source": rt.tariffs_json_source,
        # v0.9.9 — captured at import time so the recompute notification
        # can render rate-window context (seasonal label, contextual notes)
        # for past quarters even after the rate window has rolled forward.
        "seasonal": rt.seasonal,
        "notes_active": list(rt.notes) if rt.notes else None,
        # v0.16.0 — fields needed to render Tariff-model rate values,
        # EV-relevance annotation, and Active-user-inputs/Bonuses lines
        # in the recompute notification per-period block.
        "settlement_period": rt.settlement_period,
        "valid_from": rt.valid_from,
        "fixed_rp_kwh": rt.fixed_rp_kwh,
        "fixed_ht_rp_kwh": rt.fixed_ht_rp_kwh,
        "fixed_nt_rp_kwh": rt.fixed_nt_rp_kwh,
        "user_inputs": dict(tariff_cfg.user_inputs or {}),
        "bonuses_active": list(rt.bonuses) if rt.bonuses else None,
        # v0.16.1 — gating for HKN-line annotation and label-translation
        # of user_inputs in the recompute notification's per-period block.
        "hkn_structure": rt.hkn_structure,
        "user_inputs_decl": list(
            resolve_user_inputs_decl(rt.utility_key, rt.valid_from)
        ),
    }

    # v0.9.9 — segment metadata for sub-row rendering. Only persisted when
    # the quarter actually spans >1 segment (single-segment quarters render
    # as today's flat shape).
    if segments and len(segments) > 1:
        from zoneinfo import ZoneInfo

        from .quarters import month_bounds_utc as _mbu  # noqa: F401

        z = ZoneInfo("Europe/Zurich")
        segments_meta: dict[str, dict] = {}
        db = load_tariffs()
        for seg in segments:
            srt = seg.cfg.resolved
            utility_meta = db["utilities"].get(srt.utility_key, {})
            seg_local_start = seg.start_utc.astimezone(z)
            seg_local_end = seg.end_utc.astimezone(z)
            segments_meta[seg.seg_id] = {
                "valid_from": seg_local_start.date().isoformat(),
                "valid_to": seg_local_end.date().isoformat(),
                "utility_key": srt.utility_key,
                "utility_name": utility_meta.get("name_de", srt.utility_key),
                "kwp": seg.cfg.installierte_leistung_kwp,
                "eigenverbrauch": seg.cfg.eigenverbrauch_aktiviert,
                "hkn_optin": seg.cfg.hkn_aktiviert,
                "base_model": srt.base_model,
            }
        snapshot["segments_meta"] = segments_meta

    from datetime import datetime

    key = str(q)
    coordinator._imported[key] = {
        "q_price_chf_mwh": q_price_chf_mwh,
        "imported_at": datetime.now(UTC).isoformat(),
        "snapshot": snapshot,
    }
    hass.async_create_task(coordinator._async_save_state())


def _one_hour() -> timedelta:
    from datetime import timedelta

    return timedelta(hours=1)


async def _reimport_all_history(hass: HomeAssistant) -> dict:
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
    from datetime import date, datetime

    import aiohttp
    from homeassistant.components.recorder import get_instance

    # Clear LTS + snapshot map first so the rewrite below is the sole source
    # of truth. Source kWh data (export sensor LTS) is untouched.
    # Read comp_id from entry.data directly (NOT _cfg_for_entry) — the
    # latter requires a resolvable utility, which isn't guaranteed at this
    # point if the user has a degenerate history.
    entry_data = _first_entry_data(hass)
    comp_id = entry_data["config"][CONF_RUECKLIEFERVERGUETUNG_CHF]
    instance = get_instance(hass)
    # `clear_statistics` mutates `statistics_meta` and is gated on the
    # recorder's main thread (`table_managers/statistics_meta.py:399`).
    # `Recorder.async_clear_statistics` is the @callback API that queues a
    # ClearStatisticsTask onto that thread (same mechanism
    # `async_import_statistics` uses for our writes — which is why imports
    # always worked while the v0.9.2/v0.9.3 clear path didn't).
    #
    # `async_block_till_done` then drains the recorder's task queue, so the
    # clear is fully committed before the per-quarter loop starts. Without
    # this, anchor reads in `_reimport_quarter` (via the DbWorker executor
    # pool, parallel to the recorder thread) could observe stale pre-clear
    # LTS data and seed the rewrite's cumulative sum chain at the wrong
    # baseline.
    instance.async_clear_statistics([comp_id])
    await instance.async_block_till_done()

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
    # v0.9.11: thread the cumulative LTS sum through memory rather than
    # re-reading it between quarters. The recorder's commit timer is
    # asynchronous (default 1 s), so back-to-back anchor reads can observe
    # pre-commit state and write the next quarter from anchor=0 — yielding
    # a "Q1 dashboard = total - prior_quarters" off-by-one in the Energy
    # Dashboard. Threading in memory eliminates the read-after-write race.
    cumulative_sum_chf = 0.0
    for q in sorted(quarterly.keys()):
        if earliest_date is not None:
            q_start_local = date(q.year, ((q.q - 1) * 3) + 1, 1)
            if q_start_local < earliest_date:
                before_active.append(q)
                continue
        try:
            cumulative_sum_chf = await _reimport_quarter(
                hass, q, anchor_override=cumulative_sum_chf
            )
            imported.append(q)
        except PriceNotYetPublishedError as exc:
            _LOGGER.warning("Skipping %s: %s", q, exc)
            skipped.append(q)
        except Exception as exc:
            _LOGGER.error("Failed importing %s: %s", q, exc)
            failed.append(q)

    # Append the running quarter as a conservative estimate when BFE hasn't
    # published it yet — uses the active utility's tariff settings (fixed
    # price, HKN, federal floor, or previous-quarter reference) so the user
    # sees realistic CHF in the Energy Dashboard immediately.
    estimated: list = []
    running_q = quarter_of(datetime.now(UTC))
    if running_q not in imported:
        try:
            await _import_running_quarter_estimate(
                hass, anchor_override=cumulative_sum_chf
            )
            estimated.append(running_q)
        except Exception as exc:
            _LOGGER.warning("Running-quarter estimate failed: %s", exc)

    # Final flush: ensure all queued imports are committed to disk before
    # the user sees the recompute notification and navigates to the
    # Energy Dashboard. Without this, `import_statistics`'s per-call
    # drain leaves the *last* write subject to the recorder's commit
    # timer, so a fresh dashboard query immediately after recompute may
    # still see stale data for ~1 second.
    await instance.async_block_till_done()

    return {
        "available": sorted(quarterly.keys()),
        "imported": imported,
        "skipped": skipped,
        "failed": failed,
        "estimated": estimated,
        "before_active": before_active,
    }


async def _import_running_quarter_estimate(
    hass: HomeAssistant, *, anchor_override: float | None = None
) -> dict:
    """Write LTS for the running quarter using a per-hour effective-rate estimate.

    Used while BFE has not yet published the running quarter so the user
    can see realistic CHF values in the Energy Dashboard immediately
    instead of whatever stale price source was wired before. Iterates
    hours from quarter_start up to the last completed hour and writes
    ``kWh × effective_rate(hour)`` to the compensation LTS.

    v0.9.5: per-hour rate resolution (was: one flat ``effective_rp_kwh``
    for every hour). For ``fixed_ht_nt`` utilities this means the right
    rate is applied to each hour based on its Zurich-local time / day —
    HT during workday daytime, NT overnight/weekends. For ``fixed_flat``
    the per-hour rate is constant; for RMP utilities lacking BFE data we
    fall back to the federal floor as the reference price (mirrors
    ``coordinator._tariff_breakdown``'s ``is_estimate`` branch).

    The snapshot's ``periods`` entry is built via the same
    ``_aggregate_by_period`` helper closed quarters use, so Base / HKN /
    intended_hkn columns now light up in the recompute notification with
    kWh-weighted averages over the actual export profile.

    Returns a result dict for the caller to surface in a notification.
    """
    from datetime import datetime

    from .importer import HourRecord, _effective_rate_breakdown_at_hour

    cfg, tariff_cfg = _cfg_for_entry(hass)
    export_id = cfg[CONF_STROMNETZEINSPEISUNG_KWH]
    comp_id = cfg[CONF_RUECKLIEFERVERGUETUNG_CHF]
    billing = cfg.get(CONF_ABRECHNUNGS_RHYTHMUS)

    entry_data = _first_entry_data(hass)
    coordinator = entry_data.get("coordinator")
    if coordinator is None:
        raise RuntimeError(
            "Coordinator not registered — integration not loaded"
        )

    rt = tariff_cfg.resolved
    # RMP-based utilities (rmp_quartal / rmp_monat) reaching this code path
    # don't have a published BFE price for the running quarter (otherwise
    # _reimport_all_history would have imported it via _reimport_quarter
    # and we'd never be here). Fall back to the federal floor as the
    # reference — `_effective_rate_breakdown_at_hour` will then compose
    # floor + HKN + cap correctly. For fixed_flat / fixed_ht_nt utilities
    # the reference is ignored.
    fallback_ref_rp_kwh = float(rt.federal_floor_rp_kwh or 0.0)
    is_estimate = rt.base_model in ("rmp_quartal", "rmp_monat")

    now = datetime.now(UTC)
    q = quarter_of(now)
    q_start_utc, _q_end_utc = quarter_bounds_utc(q)
    last_full_hour = now.replace(minute=0, second=0, microsecond=0)

    if last_full_hour <= q_start_utc:
        # Quarter has just started — nothing to write yet.
        return {
            "quarter": str(q),
            "rate_rp_kwh": 0.0,
            "hours_imported": 0,
            "chf_total": 0.0,
            "is_estimate": is_estimate,
        }

    hourly_kwh = await read_hourly_export(hass, export_id, q_start_utc, last_full_hour)
    if anchor_override is not None:
        # v0.9.11: skip the LTS read; caller passed the prior quarter's
        # final cumulative sum directly. Avoids the recorder commit-timer
        # race that otherwise lets a stale 0 anchor through when the
        # estimate runs back-to-back with the prior quarter's import.
        anchor = anchor_override
    else:
        anchor = await read_compensation_anchor(
            hass, comp_id, q_start_utc - _one_hour()
        )

    hour_records: list[HourRecord] = []
    running_sum = anchor
    total_kwh = 0.0
    for h in hours_in_range(q_start_utc, last_full_hour):
        kwh = hourly_kwh.get(h, 0.0)
        rate_rp, base_rp, hkn_rp, bonus_rp = _effective_rate_breakdown_at_hour(
            tariff_cfg, fallback_ref_rp_kwh, h
        )
        chf = kwh * rate_rp / 100.0
        running_sum += chf
        total_kwh += kwh
        hour_records.append(
            HourRecord(
                start=h,
                kwh=kwh,
                rate_rp_kwh=rate_rp,
                compensation_chf=chf,
                base_rp_kwh=base_rp,
                hkn_rp_kwh=hkn_rp,
                bonus_rp_kwh=bonus_rp,
            )
        )

    # Build LTS records (cumulative running sum at each hour).
    lts_records: list[tuple] = []
    acc = anchor
    for r in hour_records:
        acc += r.compensation_chf
        lts_records.append((r.start, acc))

    if lts_records:
        await import_statistics(
            hass,
            build_metadata_compensation(comp_id),
            build_compensation_stats(lts_records),
        )

    chf_total = running_sum - anchor
    # kWh-weighted average effective rate over the hours imported so far.
    avg_rate_rp_kwh = (chf_total * 100.0 / total_kwh) if total_kwh > 0 else 0.0

    # Aggregate per period using the same helper closed quarters use, so
    # Base / HKN / intended_hkn columns are populated with kWh-weighted
    # averages.
    intended_hkn = (
        tariff_cfg.resolved.hkn_rp_kwh if tariff_cfg.hkn_aktiviert else None
    )
    periods = _aggregate_by_period(
        hour_records, billing, intended_hkn_rp_kwh=intended_hkn
    )

    snapshot = {
        "rate_rp_kwh": round(avg_rate_rp_kwh, 4),
        "kwp": tariff_cfg.installierte_leistung_kwp,
        "eigenverbrauch_aktiviert": tariff_cfg.eigenverbrauch_aktiviert,
        "hkn_rp_kwh": tariff_cfg.hkn_rp_kwh_resolved,
        "hkn_optin": tariff_cfg.hkn_aktiviert,
        "cap_mode": rt.cap_mode,
        "cap_rp_kwh": rt.cap_rp_kwh,
        "cap_applied": False,
        "total_kwh": round(total_kwh, 3),
        "total_chf": round(chf_total, 4),
        "periods": periods,
        "utility_key": rt.utility_key,
        "base_model": rt.base_model,
        "billing": billing,
        "floor_label": rt.federal_floor_label,
        "floor_rp_kwh": rt.federal_floor_rp_kwh,
        "utility_floor_rp_kwh": rt.price_floor_rp_kwh,
        "floor_source": _floor_source(rt),
        "tariffs_json_version": rt.tariffs_json_version,
        "tariffs_json_source": rt.tariffs_json_source,
        "is_current_estimate": True,
        # v0.9.9 — same rate-window context fields as the published-quarter
        # snapshot so the running-quarter row in the recompute notification
        # gets the same Seasonal/Notes treatment.
        "seasonal": rt.seasonal,
        "notes_active": list(rt.notes) if rt.notes else None,
        # v0.16.0 — same set of new fields as published-quarter snapshot
        # (see _reimport_quarter for context). Keeps per-period rendering
        # uniform between running-quarter and closed-quarter rows.
        "settlement_period": rt.settlement_period,
        "valid_from": rt.valid_from,
        "fixed_rp_kwh": rt.fixed_rp_kwh,
        "fixed_ht_rp_kwh": rt.fixed_ht_rp_kwh,
        "fixed_nt_rp_kwh": rt.fixed_nt_rp_kwh,
        "user_inputs": dict(tariff_cfg.user_inputs or {}),
        "bonuses_active": list(rt.bonuses) if rt.bonuses else None,
        # v0.16.1 — same as published-quarter snapshot.
        "hkn_structure": rt.hkn_structure,
        "user_inputs_decl": list(
            resolve_user_inputs_decl(rt.utility_key, rt.valid_from)
        ),
    }
    coordinator._imported[str(q)] = {
        "q_price_chf_mwh": None,
        "imported_at": datetime.now(UTC).isoformat(),
        "snapshot": snapshot,
    }
    hass.async_create_task(coordinator._async_save_state())

    _LOGGER.info(
        "Imported running %s estimate: avg_rate=%.4f Rp/kWh, hours=%d, "
        "total=%.4f CHF (estimate=%s, base_model=%s)",
        q, avg_rate_rp_kwh, len(lts_records), chf_total, is_estimate,
        rt.base_model,
    )

    return {
        "quarter": str(q),
        "rate_rp_kwh": avg_rate_rp_kwh,
        "hours_imported": len(lts_records),
        "chf_total": chf_total,
        "is_estimate": is_estimate,
    }


async def _refresh_upstream_data(hass: HomeAssistant) -> dict:
    """Trigger a fresh fetch from BOTH upstream sources (v0.9.6).

    Two independent network operations:
    1. **BFE poll** — ``BfeCoordinator.async_refresh()`` fetches the BFE
       quarterly/monthly RMP CSVs and auto-imports any newly-published
       quarter. Snapshot ``_imported`` keys before/after for the diff.
    2. **Tariffs.json fetch** — ``TariffsDataCoordinator.async_refresh()``
       pulls the companion repo's tariff database and updates the local
       cache + the ``_OVERRIDE_PATH`` for ``tariffs_db.load_tariffs``.

    Both operations run unconditionally; the second's success/failure is
    surfaced in the result dict but does not gate the first. The user-
    visible flow is the OptionsFlow "Refresh prices & tariff data" step,
    which renders both statuses in a single notification.

    Returns:
        ``{
            "available": sorted Quarters BFE has,
            "newly_imported": Quarters newly imported this tick,
            "tariffs_refreshed": bool — True iff async_refresh returned True,
            "tariffs_version": str | None — tariffs.json schema version after refresh,
            "tariffs_error": str | None — last_error message on refresh failure,
        }``
    """
    entry_data = _first_entry_data(hass)
    coordinator = entry_data.get("coordinator")
    if coordinator is None:
        raise RuntimeError("Coordinator not yet ready")

    before = set(coordinator._imported.keys())
    await coordinator.async_refresh()
    after = set(coordinator._imported.keys())

    # Tariffs.json refresh — separate fetch; failure is non-fatal (the
    # coordinator falls back to the cached / bundled tariffs).
    tdc = hass.data.get(DOMAIN, {}).get("_tariffs_data")
    tariffs_refreshed = False
    tariffs_version: str | None = None
    tariffs_error: str | None = None

    # v0.17.0 — snapshot the parsed tariffs.json before & after the refresh
    # so we can diff added / modified rate windows per utility for the
    # notification body. ``load_tariffs()`` is mtime-cached; the post-refresh
    # call returns the freshly-rewritten file. Wrapped defensively so a
    # failure here never blocks the refresh.
    from .tariffs_db import diff_tariffs_data, load_tariffs

    old_tariffs: dict | None = None
    try:
        old_tariffs = await hass.async_add_executor_job(load_tariffs)
    except Exception:
        old_tariffs = None

    if tdc is not None:
        tariffs_refreshed = await tdc.async_refresh()
        tariffs_error = tdc.last_error
        if tariffs_refreshed:
            try:
                data = await hass.async_add_executor_job(load_tariffs)
                tariffs_version = data.get("schema_version")
            except Exception:
                tariffs_version = None

    new_tariffs: dict | None = None
    try:
        new_tariffs = await hass.async_add_executor_job(load_tariffs)
    except Exception:
        new_tariffs = None

    tariffs_diff: dict | None = None
    if old_tariffs is not None and new_tariffs is not None:
        try:
            tariffs_diff = diff_tariffs_data(old_tariffs, new_tariffs)
        except Exception:
            tariffs_diff = None

    return {
        "available": sorted(coordinator.quarterly.keys()),
        "newly_imported": sorted(after - before),
        "tariffs_refreshed": tariffs_refreshed,
        "tariffs_version": tariffs_version,
        "tariffs_error": tariffs_error,
        "tariffs_data_version": tdc.last_data_version if tdc else None,
        "tariffs_data_last_updated": tdc.last_data_updated if tdc else None,
        "tariffs_schema_source": tdc.last_schema_source if tdc else None,
        "tariffs_schema_error": tdc.last_schema_error if tdc else None,
        "tariffs_diff": tariffs_diff,
    }


async def _handle_reimport_all_history(call: ServiceCall) -> None:
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


async def _handle_refresh_data(call: ServiceCall) -> None:
    """Refresh both BFE prices AND companion-repo tariffs.json.

    v0.9.6: combined service (was: ``refresh`` doing BFE only). The
    tariffs-only ``refresh_tariffs`` service stays for power users.
    """
    await _refresh_upstream_data(call.hass)


def _resolve_quarters(data: dict | None) -> list[Quarter]:
    """Map (year, quarter) call args to a list of Quarters.

    - both year + quarter → ``[Quarter(year, quarter)]``
    - year only          → all 4 quarters of that year
    - quarter only       → that quarter of the current year
    - neither            → ``[current_quarter]``
    """
    from datetime import datetime as _dt

    data = data or {}
    year = data.get("year")
    quarter = data.get("quarter")
    today = date.today()
    if year is not None and quarter is not None:
        return [Quarter(int(year), int(quarter))]
    if year is not None:
        return [Quarter(int(year), q) for q in (1, 2, 3, 4)]
    if quarter is not None:
        return [Quarter(today.year, int(quarter))]
    return [quarter_of(_dt.now(UTC))]


def _report_to_dict(report) -> dict:
    """JSON-serializable dict of a ``_RecomputeReport`` for service responses.

    The report dataclass has no datetime fields — all date-likes are
    pre-formatted strings (``period``: ``"YYYYQN"``, ``valid_from``:
    ``"YYYY-MM-DD"``). Plain ``dataclasses.asdict`` handles nested
    dataclasses + tuple-to-list conversion.
    """
    from dataclasses import asdict

    return asdict(report)


async def _handle_get_breakdown(call: ServiceCall):
    """v0.19.0 — Tier 2 analytics service. Returns the recompute report as
    a JSON dict for the BFE tariff analysis Lovelace card.

    Read-only: no LTS rewrites, no auto-import. Missing quarters surface
    as empty rows in the response — the card displays a "Run Recompute
    history first" hint to the user.
    """
    quarters = _resolve_quarters(dict(call.data) if call.data else None)
    report = _build_recompute_report(call.hass, quarters)
    return _report_to_dict(report)


async def _handle_show_report(call: ServiceCall) -> None:
    """v0.19.0 — Tier 1 diagnostic dump. Builds the same markdown report
    as ``recompute_history`` and emits it as a persistent notification —
    no LTS rewrites.
    """
    quarters = _resolve_quarters(dict(call.data) if call.data else None)
    report = _build_recompute_report(call.hass, quarters)
    # Find the first config entry id (skips the singleton "_tariffs_data" key).
    entries = call.hass.data.get(DOMAIN, {})
    entry_id = next(
        (key for key in entries if not key.startswith("_")), None
    )
    if entry_id is not None:
        _notify_recompute(call.hass, entry_id, report)


async def _handle_refresh_tariffs(call: ServiceCall) -> None:
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
class _PeriodSubRow:
    """One sub-row beneath a main period row (v0.9.9).

    Emitted when a period spans more than one (config × season) segment.
    ``label`` is a human-readable segment description like
    ``"Jan 1 – Feb 14 (utility A)"`` or ``"Apr 1 – Jun 30 (summer)"``.
    """

    label: str
    base_rp_kwh_avg: float | None
    hkn_rp_kwh_avg: float | None
    rate_rp_kwh_avg: float | None
    total_kwh: float | None
    total_chf: float | None
    # v0.11.0 (Batch D) — applied bonus per kWh (kWh-weighted) within this
    # segment. ``None`` for legacy snapshots predating the field.
    bonus_rp_kwh_avg: float | None = None


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
    utility_floor_rp_kwh_at_period: float | None = None
    floor_source_at_period: str | None = None
    tariffs_version_at_period: str | None = None
    tariffs_source_at_period: str | None = None
    # v0.9.0: marks rows from the running-quarter estimate (BFE not yet
    # published; rate is the active utility's effective floor).
    # v0.9.2: ``estimate_basis`` dropped — the renderer just appends a "*"
    # to the period cell + a single footnote line. The basis label is still
    # exposed on the live BasisVerguetungSensor's attributes.
    is_current_estimate: bool = False
    # v0.9.9 — rate-window-derived metadata captured per period so per-group
    # config blocks can render seasonal labels + contextual notes.
    seasonal_at_period: dict | None = None
    notes_active_at_period: list | None = None
    # v0.9.9 — per-period sub-rows for seasonal / mid-period config splits.
    # ``None`` keeps legacy single-row rendering; populated when a period
    # spans more than one (config × season) segment.
    sub_rows: tuple = ()
    # v0.16.0 — extra rate-window context threaded through so per-group
    # blocks can render Tariff-model rate values, EV-relevance annotation,
    # Active-user-inputs/Bonuses lines, and a settlement-period suffix.
    settlement_period_at_period: str | None = None
    valid_from_at_period: str | None = None
    fixed_rp_kwh_at_period: float | None = None
    fixed_ht_rp_kwh_at_period: float | None = None
    fixed_nt_rp_kwh_at_period: float | None = None
    user_inputs_at_period: dict | None = None
    bonuses_active_at_period: list | None = None
    # v0.16.1 — HKN-structure gating + user_inputs declaration list (used
    # for label-translation of user-input keys/values in per-group blocks).
    hkn_structure_at_period: str | None = None
    user_inputs_decl_at_period: list | None = None
    # v0.11.0 (Batch D) — applied bonus per kWh (kWh-weighted) for the
    # period. ``None`` for legacy snapshots predating the field.
    bonus_rp_kwh_avg: float | None = None


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
    hass: HomeAssistant,
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
    user_lang = (getattr(hass.config, "language", None) or "en").split("-")[0].lower()
    header = {
        "utility_key": rt.utility_key,
        "utility_name": utility_meta.get("name_de", rt.utility_key),
        "base_model": rt.base_model,
        "settlement_period": rt.settlement_period,
        "kwp": tariff_cfg.installierte_leistung_kwp,
        "eigenverbrauch": tariff_cfg.eigenverbrauch_aktiviert,
        "hkn_optin": tariff_cfg.hkn_aktiviert,
        "hkn_rp_kwh": rt.hkn_rp_kwh,
        "billing": cfg.get(CONF_ABRECHNUNGS_RHYTHMUS),
        "floor_label": rt.federal_floor_label,
        "floor_rp_kwh": rt.federal_floor_rp_kwh,
        "utility_floor_rp_kwh": rt.price_floor_rp_kwh,
        "floor_source": _floor_source(rt),
        "cap_mode": rt.cap_mode,
        "cap_rp_kwh": rt.cap_rp_kwh,
        "tariffs_version": rt.tariffs_json_version,
        "tariffs_source": rt.tariffs_json_source,
        # v0.9.9 — seasonal applied + rate-window notes.
        "seasonal": rt.seasonal,
        "notes_active": list(rt.notes) if rt.notes else None,
        "notes_lang": user_lang,
        # v0.10.0 — Batch C / Phase 1: display-only bonuses for the
        # active-today block. v0.16.0 also threads them per-period via
        # the snapshot so per-group blocks can render the same.
        "bonuses_active": list(rt.bonuses) if rt.bonuses else None,
        # v0.16.0 — fields used by Tariff-model rate rendering, EV-line
        # relevance gating, and Active-user-inputs line.
        "valid_from": rt.valid_from,
        "fixed_rp_kwh": rt.fixed_rp_kwh,
        "fixed_ht_rp_kwh": rt.fixed_ht_rp_kwh,
        "fixed_nt_rp_kwh": rt.fixed_nt_rp_kwh,
        "user_inputs": dict(tariff_cfg.user_inputs or {}),
        # v0.16.1 — HKN-line gating (additive_optin / bundled / none) and
        # label-translation of user_inputs (label_de / value_labels_de).
        "hkn_structure": rt.hkn_structure,
        "user_inputs_decl": list(
            resolve_user_inputs_decl(rt.utility_key, rt.valid_from)
        ),
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
                "kw_at_period": snap.get("kwp"),
                "eigenverbrauch_at_period": snap.get("eigenverbrauch_aktiviert"),
                "hkn_optin_at_period": snap.get("hkn_optin"),
                "billing_at_period": snap.get("billing"),
                "base_model_at_period": snap.get("base_model"),
                "cap_mode_at_period": snap.get("cap_mode"),
                "cap_rp_kwh_at_period": snap.get("cap_rp_kwh"),
                "floor_label_at_period": snap.get("floor_label"),
                "floor_rp_kwh_at_period": snap.get("floor_rp_kwh"),
                "utility_floor_rp_kwh_at_period": snap.get("utility_floor_rp_kwh"),
                "floor_source_at_period": snap.get("floor_source"),
                "tariffs_version_at_period": snap.get("tariffs_json_version"),
                "tariffs_source_at_period": snap.get("tariffs_json_source"),
                "is_current_estimate": bool(snap.get("is_current_estimate", False)),
                "seasonal_at_period": snap.get("seasonal"),
                "notes_active_at_period": snap.get("notes_active"),
                # v0.16.0 — per-period rate-window context for richer
                # per-group rendering. .get() defaults to None for
                # legacy snapshots predating these fields.
                "settlement_period_at_period": snap.get("settlement_period"),
                "valid_from_at_period": snap.get("valid_from"),
                "fixed_rp_kwh_at_period": snap.get("fixed_rp_kwh"),
                "fixed_ht_rp_kwh_at_period": snap.get("fixed_ht_rp_kwh"),
                "fixed_nt_rp_kwh_at_period": snap.get("fixed_nt_rp_kwh"),
                "user_inputs_at_period": snap.get("user_inputs"),
                "bonuses_active_at_period": snap.get("bonuses_active"),
                # v0.16.1 — HKN-structure gating + user_inputs declaration
                # list for label translation. Default None on legacy.
                "hkn_structure_at_period": snap.get("hkn_structure"),
                "user_inputs_decl_at_period": snap.get("user_inputs_decl"),
            }
            # Prefer v0.7.5+ "periods" key; fall back to legacy "monthly" so
            # snapshots from older imports still render (Base/HKN cells = —).
            periods = snap.get("periods") or [
                {**m, "period": m.get("month")} for m in (snap.get("monthly") or [])
            ]
            segments_meta = snap.get("segments_meta") or {}
            for p in periods:
                sub_rows_payload = p.get("sub_rows") or []
                materialized_sub_rows: tuple[_PeriodSubRow, ...] = ()
                if sub_rows_payload:
                    sub_list: list[_PeriodSubRow] = []
                    for sr in sub_rows_payload:
                        sub_list.append(
                            _PeriodSubRow(
                                label=_format_segment_label(
                                    sr.get("seg_id"),
                                    segments_meta.get(sr.get("seg_id")),
                                ),
                                base_rp_kwh_avg=sr.get("base_rp_kwh_avg"),
                                hkn_rp_kwh_avg=sr.get("hkn_rp_kwh_avg"),
                                rate_rp_kwh_avg=sr.get("rate_rp_kwh_avg"),
                                total_kwh=sr.get("kwh"),
                                total_chf=sr.get("chf"),
                                bonus_rp_kwh_avg=sr.get("bonus_rp_kwh_avg"),
                            )
                        )
                    materialized_sub_rows = tuple(sub_list)
                rows.append(
                    _RecomputeReportRow(
                        period=p.get("period") or p.get("month"),
                        rate_rp_kwh_avg=p.get("rate_rp_kwh_avg"),
                        base_rp_kwh_avg=p.get("base_rp_kwh_avg"),
                        hkn_rp_kwh_avg=p.get("hkn_rp_kwh_avg"),
                        intended_hkn_rp_kwh=p.get("intended_hkn_rp_kwh"),
                        total_kwh=p.get("kwh"),
                        total_chf=p.get("chf"),
                        sub_rows=materialized_sub_rows,
                        bonus_rp_kwh_avg=p.get("bonus_rp_kwh_avg"),
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


def _canon_fingerprint(
    utility_key, kw, ev, hkn_optin, billing, user_inputs=None
) -> tuple:
    """Canonicalize a config fingerprint so int-vs-float / bool-vs-int /
    None-vs-missing variations don't break equality between the
    active-today block and per-period rows. v0.16.0.

    v0.17.0 — adds user_inputs (sorted tuple of (key,value) pairs) so that
    rate-window-specific toggles like ``regio_top40_opted_in`` participate
    in row grouping and active-today equality. Pre-v0.16.0 snapshots that
    lack user_inputs canonicalize to ``()`` and remain equal to today's
    config when today also has no user_inputs declared.
    """
    if isinstance(user_inputs, dict) and user_inputs:
        ui_canon: tuple = tuple(
            sorted((str(k), str(v)) for k, v in user_inputs.items())
        )
    else:
        ui_canon = ()
    return (
        str(utility_key) if utility_key is not None else None,
        float(kw) if isinstance(kw, (int, float)) and not isinstance(kw, bool) else None,
        bool(ev) if ev is not None else None,
        bool(hkn_optin) if hkn_optin is not None else None,
        str(billing) if billing else None,
        ui_canon,
    )


def _row_config_fingerprint(r: _RecomputeReportRow) -> tuple:
    """Group key — rows sharing this fingerprint render under one heading.

    v0.8.6: covers the rate-affecting fields (utility, kw, EV, HKN opt-in,
    billing). Cap mode / floor / tariffs version are presentation-only and
    don't change the group identity.

    v0.16.0: routed through ``_canon_fingerprint`` so type drifts (e.g.
    snapshot ``kw=8`` int vs live config ``kw=8.0`` float) collapse to
    the same key.
    """
    return _canon_fingerprint(
        r.utility_key_at_period,
        r.kw_at_period,
        r.eigenverbrauch_at_period,
        r.hkn_optin_at_period,
        r.billing_at_period,
        r.user_inputs_at_period,
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


# v0.17.0 — sub-bullet labels for the tariff-model section. French falls
# back to English (no French rate-tariff vocabulary in the recompute body
# yet; consistent with existing notification chrome).
_RATE_SUBLABELS: dict[str, dict[str, str]] = {
    "de": {
        "rate": "Tarif",
        "summer": "Sommer",
        "winter": "Winter",
        "ht": "HT",
        "nt": "NT",
        "ht_summer": "HT Sommer",
        "ht_winter": "HT Winter",
        "nt_summer": "NT Sommer",
        "nt_winter": "NT Winter",
        "settlement": "Abrechnungsperiode",
    },
    "en": {
        "rate": "Rate",
        "summer": "Summer",
        "winter": "Winter",
        "ht": "HT",
        "nt": "NT",
        "ht_summer": "HT summer",
        "ht_winter": "HT winter",
        "nt_summer": "NT summer",
        "nt_winter": "NT winter",
        "settlement": "Settlement period",
    },
}


def _render_tariff_model_lines(c: dict) -> list[str]:
    """Render the Tariff-model bullet plus localised sub-bullets for rates
    and settlement period. Returns ``[]`` when ``base_model`` is missing.

    v0.17.0 — replaces the old ``_format_tariff_model`` one-liner. Localised
    model labels (DE: Fixpreis / Fixpreis (HT/NT) / Referenzmarktpreis;
    EN: Fixed flat rate / Fixed HT/NT rate / Reference market price);
    sub-bullets break out summer/winter and HT/NT rates, plus settlement.

    fixed_flat (plain):
        - **Tariff model:** Fixpreis
            - Tarif: 6.20 Rp/kWh
            - Abrechnungsperiode: Quartal

    fixed_flat (seasonal):
        - **Tariff model:** Fixpreis (saisonal)
            - Sommer: 6.20 Rp/kWh
            - Winter: 9.00 Rp/kWh
            - Abrechnungsperiode: Quartal

    fixed_ht_nt (plain): HT + NT sub-bullets + settlement
    fixed_ht_nt (seasonal): HT/NT × summer/winter (4 sub-bullets) + settlement
    rmp_*: just the model line (settlement is encoded in the model name)
    """
    base_model = c.get("base_model")
    if not base_model:
        return []
    notes_lang = c.get("notes_lang") or "en"
    seasonal = c.get("seasonal") or {}
    sub = _RATE_SUBLABELS.get(notes_lang) or _RATE_SUBLABELS["en"]

    model = tariff_model_label(base_model, seasonal, notes_lang)
    lines: list[str] = [f"- **Tariff model:** {model}"]

    if base_model == "fixed_flat":
        s_summer = seasonal.get("summer_rp_kwh") if seasonal else None
        s_winter = seasonal.get("winter_rp_kwh") if seasonal else None
        if s_summer is not None and s_winter is not None:
            lines.append(f"    - {sub['summer']}: {s_summer:.2f} Rp/kWh")
            lines.append(f"    - {sub['winter']}: {s_winter:.2f} Rp/kWh")
        else:
            fr = c.get("fixed_rp_kwh")
            if fr is not None:
                lines.append(f"    - {sub['rate']}: {fr:.2f} Rp/kWh")
    elif base_model == "fixed_ht_nt":
        s_ht_summer = seasonal.get("summer_ht_rp_kwh") if seasonal else None
        s_ht_winter = seasonal.get("winter_ht_rp_kwh") if seasonal else None
        s_nt_summer = seasonal.get("summer_nt_rp_kwh") if seasonal else None
        s_nt_winter = seasonal.get("winter_nt_rp_kwh") if seasonal else None
        if all(v is not None for v in (s_ht_summer, s_ht_winter, s_nt_summer, s_nt_winter)):
            lines.append(f"    - {sub['ht_summer']}: {s_ht_summer:.2f} Rp/kWh")
            lines.append(f"    - {sub['ht_winter']}: {s_ht_winter:.2f} Rp/kWh")
            lines.append(f"    - {sub['nt_summer']}: {s_nt_summer:.2f} Rp/kWh")
            lines.append(f"    - {sub['nt_winter']}: {s_nt_winter:.2f} Rp/kWh")
        else:
            ht = c.get("fixed_ht_rp_kwh")
            nt = c.get("fixed_nt_rp_kwh")
            if ht is not None:
                lines.append(f"    - {sub['ht']}: {ht:.2f} Rp/kWh")
            if nt is not None:
                lines.append(f"    - {sub['nt']}: {nt:.2f} Rp/kWh")

    # Settlement sub-bullet for fixed_* models only — for rmp_*, the
    # settlement period is encoded in the model name itself
    # ("Referenzmarktpreis (Quartal)" vs "(Monat)"), so a separate
    # sub-bullet would be redundant.
    settlement = c.get("settlement_period")
    if settlement and base_model.startswith("fixed_"):
        lines.append(
            f"    - {sub['settlement']}: {settlement_period_label(settlement, notes_lang)}"
        )

    return lines


def _render_config_block(c: dict, *, is_today: bool = False) -> list[str]:
    """Shared bullet-list renderer used by both the active-today block and
    each per-group "Configuration in effect" block (v0.9.2).

    Expected dict keys (any may be missing — None-guarded throughout):
    - ``utility_key`` (str), ``utility_name`` (str)
    - ``base_model`` (str), ``settlement_period`` (str)
    - ``kwp`` (float), ``eigenverbrauch`` (bool), ``hkn_optin`` (bool)
    - ``hkn_rp_kwh`` (float, only used when ``hkn_optin`` is True)
    - ``billing`` (str)
    - ``floor_label`` (str), ``floor_rp_kwh`` (float)
    - ``cap_mode`` (bool), ``cap_rp_kwh`` (float)
    - ``tariffs_version`` (str), ``tariffs_source`` (str)
    - ``bonuses_active`` (list[dict] | None) — v0.10.0 display-only
    - v0.16.0: ``valid_from`` (str), ``fixed_rp_kwh`` / ``fixed_ht_rp_kwh``
      / ``fixed_nt_rp_kwh`` (float | None), ``user_inputs`` (dict | None)

    ``is_today=True`` causes the cap line to read "Active — current cap …"
    (today's value); otherwise it reads "Active — cap …" (the snapshot's
    value at import time).
    """
    notes_lang = c.get("notes_lang") or "en"
    lines: list[str] = []

    # v0.17.1 — Pass 1: user-defined Configuration sub-bullets at the top.
    # Groups Installed power + Self-consumption + HKN opt-in + each
    # user_input under a single Configuration parent so user-controllable
    # values aren't interleaved with utility-defined descriptors.
    config_subs: list[str] = []

    kwp = c.get("kwp")
    if kwp is not None:
        config_subs.append(f"    - **Installed power:** {kwp:.1f} kWp")
    else:
        config_subs.append("    - **Installed power:** —")

    # v0.17.1 — Issue 8.1: suppress Self-consumption line entirely when
    # the user's choice has no effect on rates for this (utility, valid_from,
    # kWp). Pre-v0.16.0 snapshots lack `valid_from` → relevance can't be
    # determined → permissive default keeps the line (no regression).
    ev = c.get("eigenverbrauch")
    if ev is not None:
        utility_key = c.get("utility_key")
        valid_from = c.get("valid_from")
        relevant = True
        if utility_key and valid_from and kwp is not None:
            try:
                from .tariffs_db import self_consumption_relevant
                relevant = self_consumption_relevant(
                    utility_key, valid_from, float(kwp)
                )
            except Exception:
                # Permissive: keep showing on any lookup failure.
                pass
        if relevant:
            config_subs.append(
                f"    - **Self-consumption:** {'Yes' if ev else 'No'}"
            )

    # v0.16.1/v0.17.1 — gate HKN line on hkn_structure. Same 3-way split
    # as v0.16.1, just reparented under Configuration.
    hkn_structure = c.get("hkn_structure")
    hkn_optin = c.get("hkn_optin")
    if hkn_structure == "additive_optin":
        if hkn_optin:
            hkn_rp = c.get("hkn_rp_kwh")
            if hkn_rp is not None:
                config_subs.append(
                    f"    - **HKN opt-in:** Yes ({hkn_rp:.2f} Rp/kWh additive)"
                )
            else:
                config_subs.append("    - **HKN opt-in:** Yes")
        elif hkn_optin is False:
            config_subs.append("    - **HKN opt-in:** No")
        else:
            config_subs.append("    - **HKN opt-in:** —")
    elif hkn_structure == "bundled":
        config_subs.append(
            "    - **HKN:** bundled in base rate (no opt-in available)"
        )
    elif hkn_structure == "none":
        config_subs.append("    - **HKN:** not paid by utility")
    else:
        # Legacy snapshot pre-v0.16.1.
        if hkn_optin:
            hkn_rp = c.get("hkn_rp_kwh")
            if hkn_rp is not None:
                config_subs.append(
                    f"    - **HKN opt-in:** Yes ({hkn_rp:.2f} Rp/kWh additive)"
                )
            else:
                config_subs.append("    - **HKN opt-in:** Yes")
        elif hkn_optin is False:
            config_subs.append("    - **HKN opt-in:** No")
        else:
            config_subs.append("    - **HKN opt-in:** —")

    # v0.17.1 — each user_input becomes its own sub-bullet under
    # Configuration (replaces the pre-0.17.1 collated "Active user inputs:"
    # one-liner). Localised label via user_input_label; value via
    # _format_user_input_value.
    ui = c.get("user_inputs")
    if isinstance(ui, dict) and ui:
        decls = c.get("user_inputs_decl") or []
        decl_by_key = {d.get("key"): d for d in decls if isinstance(d, dict)}
        for k, v in sorted(ui.items()):
            decl = decl_by_key.get(k, {})
            label = user_input_label(decl, notes_lang) if decl else k
            value_str = _format_user_input_value(decl, v, notes_lang)
            config_subs.append(f"    - **{label}:** {value_str}")

    if config_subs:
        lines.append("- **Configuration:**")
        lines.extend(config_subs)

    # Pass 2: utility/tariff descriptors below.
    utility_key = c.get("utility_key") or "(unknown)"
    utility_name = c.get("utility_name") or utility_key
    # v0.17.0 — drop the slug; only show the human-readable name.
    lines.append(f"- **Utility:** {utility_name}")

    lines.extend(_render_tariff_model_lines(c))

    floor_source = c.get("floor_source")
    floor_label = c.get("floor_label")
    fed_floor = c.get("floor_rp_kwh")
    utl_floor = c.get("utility_floor_rp_kwh")
    if floor_source == "utility":
        fed_str = (
            f"federal {fed_floor:.2f} Rp/kWh"
            if fed_floor is not None
            else "no federal floor"
        )
        lines.append(
            f"- **Utility floor:** {utl_floor:.2f} Rp/kWh "
            f"(dominant over {fed_str})"
        )
    elif floor_label:
        suffix = f" ({fed_floor:.2f} Rp/kWh)" if fed_floor is not None else " (none)"
        lines.append(
            f"- **Federal floor (Mindestvergütung):** {floor_label}{suffix}"
        )

    # v0.17.1 — Issue 8.4: drop the "Off" branch. Cap mode is only emitted
    # when actually active. Mirrors HKN: don't echo state with no impact.
    cap_mode = c.get("cap_mode")
    if cap_mode:
        cap_v = c.get("cap_rp_kwh")
        cap_str = f"{cap_v:.2f} Rp/kWh" if cap_v is not None else "n/a"
        kwp_str = f"{kwp:.1f} kWp" if kwp is not None else "—"
        cap_ev_str = "Yes" if ev else ("No" if ev is False else "—")
        cap_label = "current cap" if is_today else "cap"
        lines.append(
            f"- **Cap mode (Anrechenbarkeitsgrenze):** Active — {cap_label} "
            f"{cap_str} ({kwp_str}, EV={cap_ev_str})"
        )

    tv = c.get("tariffs_version")
    ts = c.get("tariffs_source")
    if tv:
        src = f" ({ts})" if ts else ""
        lines.append(f"- **Tariff data:** v{tv}{src}")

    # v0.17.1 — Issues 8.2 + 8.3: dropped "Billing period:" + "Seasonal rates:"
    # main bullets. Period is encoded in the tariff-model sub-bullet
    # (Abrechnungsperiode for fixed_*; in the model name itself for rmp_*);
    # seasonal info is encoded in the model name ("(saisonal)") + Sommer/Winter
    # rate sub-bullets. Both main bullets duplicated this info.

    lines.extend(
        _render_bonuses_lines(
            c.get("bonuses_active"),
            c.get("user_inputs_decl"),
            notes_lang,
        )
    )

    return lines


def _render_active_today_block(c: dict) -> list[str]:
    """The 'Active configuration (today)' header block — one per report."""
    return [
        "## Active configuration (today)",
        *_render_config_block(c, is_today=True),
    ]


def _should_emit_today_block(report: _RecomputeReport) -> bool:
    """Emit the 'Active configuration (today)' block only when today's date
    falls within at least one recomputed period.

    v0.18.0 (Issue 8.6): editing a past transition recomputes that quarter
    only; today's config is irrelevant noise in the notification body.
    Editing the running quarter still surfaces the today block (today's
    date is inside the running quarter's bounds).
    """
    if not report.rows:
        return False
    today = date.today()
    for row in report.rows:
        bounds = _period_bounds(row.period or "")
        if bounds is None:
            continue
        try:
            start = date.fromisoformat(bounds[0])
            end = date.fromisoformat(bounds[1])
        except ValueError:
            continue
        if start <= today <= end:
            return True
    return False


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
    *,
    notes_lang: str = "en",
) -> list[str]:
    """v0.9.2: date-bounded heading + the same bullet-list as the active-today
    block. Replaces the prior horizontal one-liner.

    Date range is derived from the rows' period strings (the renderer is
    hass-free by design). When the group contains an ``is_current_estimate``
    row, the end-of-range is rendered as ``now`` instead of the period's
    last calendar day.
    """
    # v0.17.0 — fingerprint extended with user_inputs (canonicalised tuple);
    # not used here (sample_row carries the raw dict for rendering).
    utility_key, kwp, ev, hkn_optin, billing, _user_inputs_canon = fingerprint

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
        # v0.16.0 — settlement_period now carried per-row.
        "settlement_period": sample_row.settlement_period_at_period,
        "kwp": kwp,
        "eigenverbrauch": ev,
        "hkn_optin": hkn_optin,
        # The intended HKN rate from the snapshot is the "published" value;
        # display it next to "Yes" when opted-in.
        "hkn_rp_kwh": sample_row.intended_hkn_rp_kwh,
        "billing": billing,
        "floor_label": sample_row.floor_label_at_period,
        "floor_rp_kwh": sample_row.floor_rp_kwh_at_period,
        "utility_floor_rp_kwh": sample_row.utility_floor_rp_kwh_at_period,
        "floor_source": sample_row.floor_source_at_period,
        "cap_mode": sample_row.cap_mode_at_period,
        "cap_rp_kwh": sample_row.cap_rp_kwh_at_period,
        "tariffs_version": sample_row.tariffs_version_at_period,
        "tariffs_source": sample_row.tariffs_source_at_period,
        "seasonal": sample_row.seasonal_at_period,
        "notes_active": sample_row.notes_active_at_period,
        "notes_lang": notes_lang,
        # v0.16.0 — fields used by Tariff-model rate rendering, EV-line
        # relevance gating, and Active-user-inputs/Bonuses lines.
        "valid_from": sample_row.valid_from_at_period,
        "fixed_rp_kwh": sample_row.fixed_rp_kwh_at_period,
        "fixed_ht_rp_kwh": sample_row.fixed_ht_rp_kwh_at_period,
        "fixed_nt_rp_kwh": sample_row.fixed_nt_rp_kwh_at_period,
        "user_inputs": sample_row.user_inputs_at_period,
        "bonuses_active": sample_row.bonuses_active_at_period,
        # v0.16.1 — HKN-structure gating + user_inputs declaration list.
        "hkn_structure": sample_row.hkn_structure_at_period,
        "user_inputs_decl": sample_row.user_inputs_decl_at_period,
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

    v0.11.0 (Batch D): an optional Bonus column appears between HKN and
    Total when any row in the table has a non-zero applied bonus. When
    every row has zero/None bonus, the legacy 6-column layout is preserved
    so notifications for utilities without bonus declarations look
    bytewise-identical to v0.10.0.
    """
    # Pre-scan: emit the Bonus column only when at least one row or sub-row
    # in this group has a non-zero bonus. Threshold mirrors the cap-forfeit
    # epsilon so rounding noise doesn't inflate the column width.
    def _nonzero(v: float | None) -> bool:
        return v is not None and abs(v) > 1e-4

    show_bonus = any(
        _nonzero(r.bonus_rp_kwh_avg)
        or any(_nonzero(s.bonus_rp_kwh_avg) for s in (r.sub_rows or ()))
        for r in rows
    )

    if show_bonus:
        lines = [
            "_Rates in Rp/kWh; energy in kWh; CHF totals._",
            "",
            "| Period | Base | HKN | Bonus | Total | kWh | CHF |",
            "|---|---|---|---|---|---|---|",
        ]
    else:
        lines = [
            "_Rates in Rp/kWh; energy in kWh; CHF totals._",
            "",
            "| Period | Base | HKN | Total | kWh | CHF |",
            "|---|---|---|---|---|---|",
        ]
    forfeit_rows: list[_RecomputeReportRow] = []
    has_estimate = False

    def _format_cell_block(
        period_cell: str,
        base_v: float | None,
        applied_v: float | None,
        intended_v: float | None,
        rate_v: float | None,
        kwh_v: float | None,
        chf_v: float | None,
        bonus_v: float | None,
    ) -> tuple[str, bool]:
        """Render one row's markdown cells. Returns (markdown, is_forfeit).
        Six columns by default, seven when ``show_bonus`` is True (closure
        binding via outer scope). Shared between main rows and sub-rows so
        both honour the same dash / forfeit conventions.
        """
        base_s = f"{base_v:.3f}" if base_v is not None else "—"
        forfeit = (
            intended_v is not None
            and intended_v > 0
            and applied_v is not None
            and applied_v < intended_v - 1e-4
        )
        if applied_v is None:
            hkn_s = "—"
        elif forfeit:
            hkn_s = f"{applied_v:.3f} / {intended_v:.2f}"
        else:
            hkn_s = f"{applied_v:.3f}"
        rate_s = f"{rate_v:.3f}" if rate_v is not None else "—"
        kwh_s = f"{kwh_v:,.2f}" if kwh_v is not None else "—"
        chf_s = f"{chf_v:,.2f}" if chf_v is not None else "—"
        if show_bonus:
            bonus_s = f"{bonus_v:.3f}" if _nonzero(bonus_v) else ""
            return (
                f"| {period_cell} | {base_s} | {hkn_s} | {bonus_s} | "
                f"{rate_s} | {kwh_s} | {chf_s} |",
                forfeit,
            )
        return (
            f"| {period_cell} | {base_s} | {hkn_s} | {rate_s} | {kwh_s} | {chf_s} |",
            forfeit,
        )

    for r in rows:
        if r.is_current_estimate:
            has_estimate = True
            period_cell = f"{r.period} *"
        else:
            period_cell = r.period
        markdown, is_forfeit = _format_cell_block(
            period_cell,
            r.base_rp_kwh_avg,
            r.hkn_rp_kwh_avg,
            r.intended_hkn_rp_kwh,
            r.rate_rp_kwh_avg,
            r.total_kwh,
            r.total_chf,
            r.bonus_rp_kwh_avg,
        )
        lines.append(markdown)
        if is_forfeit:
            forfeit_rows.append(r)
        # v0.9.9 — sub-rows under the main row when this period spans
        # >1 (config × season) segment.
        for sub in r.sub_rows or ():
            sub_markdown, _ = _format_cell_block(
                f"  ↳ {sub.label}",
                sub.base_rp_kwh_avg,
                sub.hkn_rp_kwh_avg,
                # Sub-row forfeit detection deliberately suppressed: the
                # main row's slash already conveys the cap-forfeit story for
                # the period.
                None,
                sub.rate_rp_kwh_avg,
                sub.total_kwh,
                sub.total_chf,
                sub.bonus_rp_kwh_avg,
            )
            lines.append(sub_markdown)
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
    emit_today = _should_emit_today_block(report)
    lines: list[str] = list(_render_active_today_block(c)) if emit_today else []

    max_rows = 24
    if n_periods > max_rows:
        shown_rows = report.rows[:max_rows]
        elided = n_periods - max_rows
    else:
        shown_rows = report.rows
        elided = 0

    groups = _group_rows_by_config(shown_rows)

    # Suppress the redundant per-group heading when there's exactly one
    # group and it matches today's active config — the active-today block
    # alone already says what's going on. v0.16.0: route through the same
    # canonicalizer the row-fingerprint uses so int-vs-float / bool-vs-int
    # drift between live config and snapshot doesn't break equality.
    today_fingerprint = _canon_fingerprint(
        c.get("utility_key"),
        c.get("kwp"),
        c.get("eigenverbrauch"),
        c.get("hkn_optin"),
        c.get("billing"),
        c.get("user_inputs"),
    )
    notes_lang = report.config.get("notes_lang", "en")

    # v0.16.1 — per-group suppression. The active-today block already
    # rendered today's config; for the group whose fingerprint matches, we
    # render only its data table (with a "Per-period results" delimiter so
    # the table is attributable). Other groups still get a full heading +
    # config block + table. This unifies the four cases (single match,
    # single non-match, multi with one match, multi with no match) so the
    # active-today config never duplicates as a per-group block.
    multi_group = len(groups) > 1
    for fingerprint, group_rows in groups:
        sample = group_rows[0]
        lines.append("")
        # When today block was suppressed (today outside recomputed range),
        # always emit the full group heading — there's no implicit "today"
        # context to lean on. Only collapse to "Per-period results" when the
        # today block ran first AND this group matches today's fingerprint.
        if emit_today and fingerprint == today_fingerprint:
            heading = (
                "## Per-period results (active config)"
                if multi_group else "## Per-period results"
            )
            lines.append(heading)
        else:
            lines.extend(
                _render_group_heading(
                    fingerprint, sample, group_rows, notes_lang=notes_lang
                )
            )
        lines.append("")
        table_lines, forfeit_rows = _render_period_table(group_rows)
        lines.extend(table_lines)
        if forfeit_rows:
            lines.append("")
            # For the today-group, the published HKN comes from today's
            # active config (header `c`); for other groups, from the
            # group's snapshot (sample.intended_hkn_rp_kwh).
            published = (
                c.get("hkn_rp_kwh")
                if fingerprint == today_fingerprint
                else sample.intended_hkn_rp_kwh
            )
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
    hass: HomeAssistant, entry_id: str, report: _RecomputeReport
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
