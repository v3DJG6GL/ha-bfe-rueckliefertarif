"""DataUpdateCoordinator — polls BFE every 6h, auto-imports newly published quarters.

The coordinator owns the "what's the current state of the world" view:
- Most recently published quarterly and monthly BFE prices.
- Which quarters have been imported (persisted via helpers.storage.Store).
- Derived current effective tariff (for the basis sensor).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .bfe import BfePrice, fetch_monthly, fetch_quarterly
from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    CONF_ABRECHNUNGS_RHYTHMUS,
    DOMAIN,
)
from .quarters import Month, Quarter, quarter_of
from .tariff import chf_per_mwh_to_rp_per_kwh

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)
_UPDATE_INTERVAL = timedelta(hours=6)
_STORAGE_VERSION = 1
_STORAGE_KEY_FMT = "bfe_rueckliefertarif.{entry_id}"


class BfeCoordinator(DataUpdateCoordinator):
    """Polls BFE CSVs, caches prices, tracks which quarters are imported."""

    def __init__(self, hass: "HomeAssistant", entry: "ConfigEntry") -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{entry.entry_id}",
            update_interval=_UPDATE_INTERVAL,
        )
        self.entry = entry
        self._store = self._make_store()
        self._imported: dict[str, dict[str, Any]] = {}
        self.quarterly: dict[Quarter, BfePrice] = {}
        self.monthly: dict[Month, BfePrice] = {}
        # Cached earliest hour the user's grid-export sensor has any LTS data
        # for. Used to filter the "skipped quarter(s)" notification so we
        # don't claim HA has records when it doesn't. Lazily computed; reset
        # on coordinator restart only.
        self._earliest_export_hour: datetime | None = None
        # v0.9.14 — single-flight guard around `_auto_import_newly_published`.
        # The first-refresh path schedules it as a background task while
        # `_async_update_data` returns early; a 6h tick that fires before the
        # background task finishes must wait, not interleave.
        self._auto_import_lock = asyncio.Lock()

    @property
    def _config(self) -> dict:
        """Live merge of entity-wiring (entry.data) + today's resolved versioned fields.

        v0.9.0 (Option A+): versioned fields (utility, kW, EV, HKN, billing)
        live exclusively in ``entry.options[OPT_CONFIG_HISTORY]`` — the open
        record IS today's config. Entity-wiring fields (export sensor,
        compensation sensor, name prefix) stay in ``entry.data``. This merge
        gives consumers a single dict identical in shape to pre-A+ but with
        history as the source of truth for everything that varies over time.
        """
        from datetime import date

        from .services import _resolve_config_at

        data = dict(self.entry.data)
        versioned = _resolve_config_at(self.entry.options or {}, date.today(), {})
        return {**data, **versioned}

    def _make_store(self):
        from homeassistant.helpers.storage import Store

        return Store(self.hass, _STORAGE_VERSION, _STORAGE_KEY_FMT.format(entry_id=self.entry.entry_id))

    async def async_load_state(self) -> None:
        data = await self._store.async_load()
        if data:
            self._imported = data.get("imported", {})

    async def _async_save_state(self) -> None:
        await self._store.async_save({"imported": self._imported})

    async def _async_update_data(self) -> dict[str, Any]:
        import aiohttp

        abrechnungs_rhythmus = self._config.get(CONF_ABRECHNUNGS_RHYTHMUS)
        async with aiohttp.ClientSession() as session:
            self.quarterly = await fetch_quarterly(session)
            if abrechnungs_rhythmus == ABRECHNUNGS_RHYTHMUS_MONAT:
                self.monthly = await fetch_monthly(session)

        # v0.9.14 — defer auto-import on the first refresh so sensor
        # platform setup doesn't block on the recorder drain
        # (`async_block_till_done` inside `import_statistics` waits for
        # the entire recorder queue, not just our writes — multi-second
        # on Postgres-backed recorders during cold HA startup).
        # Subsequent ticks (6h tick, refresh_data service) run inline.
        # v0.9.15 — `is_user_reload` distinguishes cold startup /
        # apply_change reload (gate the running-quarter estimate on
        # config-staleness) from 6h tick / refresh_data (keep
        # unconditional kWh roll-forward).
        if self.data is None:
            self.hass.async_create_background_task(
                self._auto_import_newly_published(is_user_reload=True),
                name=f"{DOMAIN}_initial_auto_import_{self.entry.entry_id}",
            )
        else:
            await self._auto_import_newly_published(is_user_reload=False)
        breakdown = self._tariff_breakdown()
        return {
            "quarterly": self.quarterly,
            "monthly": self.monthly,
            "current_tariff_rp_kwh": breakdown["effective_rp_kwh"] if breakdown else None,
            "current_tariff_chf_kwh": breakdown["effective_chf_kwh"] if breakdown else None,
            "tariff_breakdown": breakdown,
            "next_publication": _next_publication_estimate(datetime.now(timezone.utc)),
        }

    def _tariff_breakdown(self) -> dict[str, Any] | None:
        """Return a dict explaining how the effective Rückliefervergütung is computed.

        Used by the BasisVerguetungSensor / AktuelleVerguetungChfKwhSensor
        extra_state_attributes so the user can verify what their tariff
        settings actually resolve to. v0.5 fields:

        - ``utility``, ``tariff_source``, ``floor_label`` — from tariffs.json
        - ``eigenverbrauch_aktiviert``, ``hkn_aktiviert`` — user choice
        - ``base_input_rp_kwh``, ``base_source`` — how the base is sourced
          (fixed_flat / rmp_quartal price / fallback floor)
        - ``minimalverguetung_rp_kwh`` — federal floor for (kW, EV)
        - ``anrechenbarkeitsgrenze_rp_kwh`` — utility cap (None when cap_mode is off)
        - ``obergrenze_aktiv`` — whether the cap binds the producer's payment
        - ``effective_rp_kwh`` / ``effective_chf_kwh`` — final per-kWh rate
        - ``is_estimate``, ``estimate_basis`` — set when BFE hasn't published
          the running quarter yet
        """
        from datetime import date

        from .const import (
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_ENERGIEVERSORGER,
            CONF_HKN_AKTIVIERT,
            CONF_INSTALLIERTE_LEISTUNG_KW,
        )
        from .tariff import effective_rp_kwh
        from .tariffs_db import resolve_tariff_at

        utility_key = self._config.get(CONF_ENERGIEVERSORGER)
        if not utility_key:
            return None
        kw = float(self._config.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0) or 0.0)
        eigenverbrauch = bool(self._config.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True))
        hkn_aktiviert = bool(self._config.get(CONF_HKN_AKTIVIERT, False))

        try:
            rt = resolve_tariff_at(
                utility_key, date.today(), kw=kw, eigenverbrauch=eigenverbrauch
            )
        except (KeyError, LookupError):
            return None

        hkn = rt.hkn_rp_kwh if hkn_aktiviert else 0.0
        floor = rt.federal_floor_rp_kwh
        floor_value = floor if floor is not None else 0.0
        cap = rt.cap_rp_kwh if rt.cap_mode else None

        now = datetime.now(timezone.utc)
        q = quarter_of(now)
        is_estimate = False
        estimate_basis: str | None = None
        if rt.base_model == "fixed_flat":
            base_input = rt.fixed_rp_kwh or 0.0
            base_label = "fixed_flat"
        elif rt.base_model == "fixed_ht_nt":
            base_input = rt.fixed_ht_rp_kwh or 0.0
            base_label = "fixed_ht_nt"
        elif q in self.quarterly:
            base_input = chf_per_mwh_to_rp_per_kwh(self.quarterly[q].chf_per_mwh)
            base_label = f"referenz_marktpreis_{q}"
        else:
            # BFE has not yet published the running quarter (or no BFE data
            # at all) — fall back to the federal floor. Never leak historical
            # BFE prices: the estimate must derive only from configured values.
            # Once BFE publishes, the normal import path overwrites LTS exactly.
            base_input = floor_value
            base_label = "fallback_mindestverguetung"
            is_estimate = True
            estimate_basis = "mindestverguetung_floor"

        base_after_floor = max(base_input, floor_value)
        theoretical_total = base_after_floor + hkn
        effective = effective_rp_kwh(
            base_input,
            hkn,
            federal_floor_rp_kwh=floor,
            cap_rp_kwh=cap,
            cap_mode=rt.cap_mode,
        )
        if rt.cap_mode and cap is not None:
            obergrenze_aktiv = theoretical_total > cap
            hkn_gekuerzt_auf = (
                max(0.0, cap - base_after_floor)
                if obergrenze_aktiv and base_after_floor < cap
                else None
            )
        else:
            obergrenze_aktiv = False
            hkn_gekuerzt_auf = None

        tariff_source = (
            f"tariffs.json v{rt.tariffs_json_version} {rt.utility_key} "
            f"@ {rt.valid_from} ({rt.tariffs_json_source})"
        )
        return {
            "utility": rt.utility_key,
            "tariff_source": tariff_source,
            "floor_label": rt.federal_floor_label,
            "eigenverbrauch_aktiviert": eigenverbrauch,
            "hkn_aktiviert": hkn_aktiviert,
            "base_model": rt.base_model,
            "base_input_rp_kwh": round(base_input, 4),
            "base_source": base_label,
            "minimalverguetung_rp_kwh": round(floor_value, 4),
            "base_after_floor_rp_kwh": round(base_after_floor, 4),
            "hkn_verguetung_rp_kwh": round(hkn, 4),
            "theoretical_total_rp_kwh": round(theoretical_total, 4),
            "anrechenbarkeitsgrenze_rp_kwh": round(cap, 4) if cap is not None else None,
            "effective_rp_kwh": round(effective, 4),
            "effective_chf_kwh": round(effective / 100.0, 6),
            "obergrenze_aktiv": obergrenze_aktiv,
            "hkn_gekuerzt_auf": round(hkn_gekuerzt_auf, 4) if hkn_gekuerzt_auf is not None else None,
            "is_estimate": is_estimate,
            "estimate_basis": estimate_basis,
        }

    async def _auto_import_newly_published(
        self, *, is_user_reload: bool = False
    ) -> None:
        """Detect quarters needing reimport — BFE-price changes OR config drift.

        v0.7: extended from "BFE-price-only" to also reimport when the
        snapshot's resolved config (utility, kW, EV, HKN opt-in,
        tariffs.json version) differs from what the integration would
        resolve today. So a kW change in the options flow now triggers
        retroactive recompute on the next coordinator refresh.

        ``_reimport_quarter`` updates ``self._imported[key]`` itself with the
        full snapshot, so this just drives the loop and skips quarters whose
        snapshot already matches the current resolved config.

        Quarters BFE has published but the bundled tariff database doesn't
        cover (e.g. pre-2026 dates while v0.5 ships only 2026 utility data)
        produce a single persistent notification listing the gap, instead
        of one warning per skipped quarter. Populating older years is a
        community-PR effort against the bfe-tariffs-data companion repo.
        Genuine errors still surface as warnings.

        After the loop, any quarters that were actually reimported get
        summarized in a single rich notification card (utility, model,
        kW/EV/HKN/billing, plus a per-month results table). v0.9.14: the
        running quarter rides along in that notification when the
        estimate ran successfully (so apply_change recomputes show the
        running quarter alongside the stale-published ones).
        """
        from datetime import date

        from .const import OPT_CONFIG_HISTORY
        from .services import (
            _build_recompute_report,
            _import_running_quarter_estimate,
            _notify_recompute,
            _reimport_quarter,
        )

        # v0.9.14 — single-flight: a 6h tick that fires while the
        # first-refresh-deferred background task is still running waits
        # here, instead of double-mutating `_imported`.
        async with self._auto_import_lock:
            # v0.9.3: skip quarters that predate the earliest config-history
            # record (the plant install date). Without this guard, every
            # 6-hourly coordinator refresh logs a "predates earliest record"
            # WARNING for each pre-install quarter BFE has published — same
            # root cause `_reimport_all_history`'s pre-active filter addresses
            # for the explicit Recompute button.
            history = (self.entry.options or {}).get(OPT_CONFIG_HISTORY) or []
            earliest_date: date | None = None
            if history:
                try:
                    earliest_date = date.fromisoformat(history[0]["valid_from"])
                except (KeyError, ValueError, TypeError):
                    earliest_date = None

            no_data_skipped: list[str] = []
            reimported: list[Quarter] = []
            # v0.9.12 — track the most recent quarter reimported in this tick so
            # a contiguous running-quarter estimate can chain its anchor through
            # memory (avoiding the recorder commit-timer race that v0.9.11 fixed
            # in `_reimport_all_history`).
            last_reimported_q: Quarter | None = None
            last_reimported_final: float = 0.0

            for q, price in sorted(self.quarterly.items()):
                if earliest_date is not None:
                    q_start_local = date(q.year, ((q.q - 1) * 3) + 1, 1)
                    if q_start_local < earliest_date:
                        continue
                key = str(q)
                prior = self._imported.get(key)
                if prior and not self._snapshot_is_stale(prior, q, price):
                    continue
                try:
                    final_sum = await _reimport_quarter(self.hass, q)
                    reimported.append(q)
                    last_reimported_q = q
                    last_reimported_final = final_sum
                except LookupError as exc:
                    # No tariff data covering this date — expected for pre-2026
                    # quarters. Surface via a persistent notification below.
                    _LOGGER.debug("Auto-import skipped %s: %s", q, exc)
                    no_data_skipped.append(str(q))
                except Exception as exc:  # noqa: BLE001
                    _LOGGER.warning("Auto-import skipped %s: %s", q, exc)

            # v0.9.12 — refresh the running-quarter estimate under the same
            # triggers as published quarters (6h tick, apply_change reload via
            # `async_config_entry_first_refresh`, `refresh_data` service). Skip
            # only when BFE has just published the running quarter — in that
            # case the loop above already imported it via `_reimport_quarter`.
            # v0.9.15 — gate the estimate on user-reload triggers (cold
            # startup, apply_change reload) so an edit to a historical
            # record that doesn't touch the running quarter's resolved
            # config doesn't list the running quarter in the notification.
            # 6h tick / refresh_data keep unconditional behavior to
            # preserve the kWh roll-forward feature.
            running_q = quarter_of(datetime.now(timezone.utc))
            running_q_estimated = False
            prior_running_snapshot = (
                self._imported.get(str(running_q)) or {}
            ).get("snapshot") or {}
            running_q_config_changed = self._running_q_config_changed(
                prior_running_snapshot, running_q
            )
            if running_q not in self.quarterly:
                should_run = (not is_user_reload) or running_q_config_changed
                if should_run:
                    try:
                        if (
                            last_reimported_q is not None
                            and last_reimported_q.next() == running_q
                        ):
                            await _import_running_quarter_estimate(
                                self.hass, anchor_override=last_reimported_final
                            )
                        else:
                            await _import_running_quarter_estimate(self.hass)
                        running_q_estimated = True
                    except Exception as exc:  # noqa: BLE001
                        _LOGGER.warning(
                            "Running-quarter estimate failed during auto-import: %s",
                            exc,
                        )

            await self._notify_skipped_quarters(no_data_skipped)
            if reimported:
                # v0.9.15 — only list the running quarter when its
                # resolved config actually changed (NOT just because
                # the estimate ran on a kWh-roll-forward tick). Mirrors
                # `_snapshot_is_stale`'s rule for published quarters:
                # the recompute notification reflects what changed
                # because of the user's edit, not all maintenance work.
                notify_quarters = list(reimported)
                if running_q_estimated and running_q_config_changed:
                    notify_quarters.append(running_q)
                report = _build_recompute_report(self.hass, notify_quarters)
                _notify_recompute(self.hass, self.entry.entry_id, report)

    def _snapshot_is_stale(
        self, prior: dict, q: Quarter, price: BfePrice
    ) -> bool:
        """True when ``prior`` (a stored ``_imported[key]``) doesn't match the
        current resolved config for quarter ``q`` or the current BFE price.

        Honors per-customer history: ``_cfg_for_entry(for_quarter=q)`` resolves
        the kW / EV / HKN-opt-in active at the start of ``q`` per
        ``plant_history`` and ``hkn_optin_history``, NOT today's
        ``entry.data``. So a 2026 kW change does not mark Q1 2025 stale.

        ``tariffs_json_source`` (bundled vs remote) is intentionally ignored
        — only ``tariffs_json_version`` bumps trigger a rewrite. Bundled→
        remote on the same version is a transparent transition.
        """
        if prior.get("q_price_chf_mwh") != price.chf_per_mwh:
            return True
        snap = prior.get("snapshot") or {}
        if not snap:
            return True

        from .services import _cfg_for_entry

        try:
            cfg, tariff_cfg = _cfg_for_entry(self.hass, for_quarter=q)
        except (RuntimeError, LookupError):
            # Config not available yet — let the normal error path handle it.
            return False
        rt = tariff_cfg.resolved
        if snap.get("utility_key") != rt.utility_key:
            return True
        if snap.get("kw") != tariff_cfg.installierte_leistung_kw:
            return True
        if snap.get("eigenverbrauch_aktiviert") != tariff_cfg.eigenverbrauch_aktiviert:
            return True
        if snap.get("hkn_optin") != tariff_cfg.hkn_aktiviert:
            return True
        if snap.get("billing") != cfg.get(CONF_ABRECHNUNGS_RHYTHMUS):
            return True
        if snap.get("tariffs_json_version") != rt.tariffs_json_version:
            return True
        return False

    def _running_q_config_changed(
        self, prior_snapshot: dict, running_q: Quarter
    ) -> bool:
        """v0.9.15 — running-quarter analog of `_snapshot_is_stale`, minus the
        BFE-price field (running quarter has no published price yet).

        Returns True when today's resolved config for ``running_q`` differs
        from the prior snapshot — or when there is no prior snapshot
        (first-ever estimate for this quarter, e.g. fresh install or a
        quarter-boundary crossing).

        Used to gate whether the apply_change-reload / cold-startup path
        runs the running-quarter estimate at all, AND whether the running
        quarter is listed in the recompute notification.
        """
        if not prior_snapshot:
            return True

        from .services import _cfg_for_entry

        try:
            cfg, tariff_cfg = _cfg_for_entry(self.hass, for_quarter=running_q)
        except (RuntimeError, LookupError):
            return False
        rt = tariff_cfg.resolved
        if prior_snapshot.get("utility_key") != rt.utility_key:
            return True
        if prior_snapshot.get("kw") != tariff_cfg.installierte_leistung_kw:
            return True
        if prior_snapshot.get("eigenverbrauch_aktiviert") != tariff_cfg.eigenverbrauch_aktiviert:
            return True
        if prior_snapshot.get("hkn_optin") != tariff_cfg.hkn_aktiviert:
            return True
        if prior_snapshot.get("billing") != cfg.get(CONF_ABRECHNUNGS_RHYTHMUS):
            return True
        if prior_snapshot.get("tariffs_json_version") != rt.tariffs_json_version:
            return True
        return False

    async def _notify_skipped_quarters(self, skipped: list[str]) -> None:
        """Summarize skipped quarters in a single persistent UI notification.

        Filters the input list to quarters where the user's grid-export
        sensor actually has recorded data — without that gate the message
        used to falsely claim HA had records for quarters back to 2017 just
        because BFE published prices for them.

        Mechanism-agnostic wording: the message is about *what we don't have
        in tariffs.json yet for this user's utility*, NOT about whether
        BFE-RMP was relevant pre-2026 (it was for ~17% of CH utilities by
        2025, not for most). ``notification_id`` is stable per entry, so
        re-running auto-import updates the card in place (no stacking).
        """
        from homeassistant.components.persistent_notification import (
            async_create as _notify,
            async_dismiss as _dismiss,
        )

        from .const import CONF_ENERGIEVERSORGER, DOMAIN
        from .tariffs_db import load_tariffs

        nid = f"{DOMAIN}_{self.entry.entry_id}_skipped_quarters"
        if skipped:
            skipped = await self._filter_skipped_to_quarters_with_export(skipped)
        if not skipped:
            # Nothing to report — clear any prior card.
            _dismiss(self.hass, nid)
            return

        utility_key = self._config.get(CONF_ENERGIEVERSORGER) or "(unknown)"
        try:
            db = load_tariffs()
            rates = db["utilities"].get(utility_key, {}).get("rates") or []
            earliest_window = min((r["valid_from"] for r in rates), default=None)
        except Exception:  # noqa: BLE001
            earliest_window = None
        window_text = (
            f"starts at **{earliest_window}**"
            if earliest_window
            else "is empty"
        )

        skipped_text = ", ".join(skipped)
        msg = (
            f"The bundled tariff database for utility **`{utility_key}`** "
            f"{window_text}, but **{len(skipped)} earlier quarter(s) where "
            f"you have export data couldn't be imported** "
            f"(no remuneration was written for them):\n\n"
            f"{skipped_text}\n\n"
            f"To make older quarters importable, the `rates[]` list for "
            f"**`{utility_key}`** in the "
            f"[bfe-tariffs-data](https://github.com/v3DJG6GL/bfe-tariffs-data) "
            f"companion repo needs to be extended back in time. Historical "
            f"per-utility rates are available at "
            f"[VESE pvtarif](https://opendata.vese.ch/pvtarif/) — open an "
            f"issue or PR upstream and the next daily refresh will pick the "
            f"new data up automatically."
        )

        _notify(
            self.hass,
            msg,
            title="Older quarters skipped — your utility's tariff records start later than your export data",
            notification_id=nid,
        )

    async def _filter_skipped_to_quarters_with_export(
        self, skipped: list[str]
    ) -> list[str]:
        """Drop quarters that end before the user's first hour of export data.

        Lazily resolves and caches ``self._earliest_export_hour``: the
        earliest hour where the user's grid-export sensor has a non-zero
        LTS row. Surfaces only quarters whose end is at or after that hour.
        On any recorder error, returns ``skipped`` unchanged (don't suppress
        a real notification just because LTS lookup hiccuped).
        """
        from .const import CONF_STROMNETZEINSPEISUNG_KWH
        from .ha_recorder import read_hourly_export

        statistic_id = self._config.get(CONF_STROMNETZEINSPEISUNG_KWH)
        if not statistic_id:
            return skipped

        if self._earliest_export_hour is None:
            try:
                rows = await read_hourly_export(
                    self.hass,
                    statistic_id,
                    datetime(1970, 1, 1, tzinfo=timezone.utc),
                    datetime.now(timezone.utc),
                )
            except Exception as exc:  # noqa: BLE001
                _LOGGER.debug("read_hourly_export failed: %s", exc)
                return skipped
            non_zero_hours = [h for h, v in rows.items() if v > 0]
            if not non_zero_hours:
                # User's sensor has no recorded export at all — suppress
                # the entire notification.
                return []
            self._earliest_export_hour = min(non_zero_hours)

        from .quarters import Quarter, quarter_end_zurich

        threshold = self._earliest_export_hour
        kept: list[str] = []
        for s in skipped:
            try:
                q = Quarter.parse(s)
            except ValueError:
                kept.append(s)
                continue
            q_end_utc = quarter_end_zurich(q).astimezone(timezone.utc)
            if q_end_utc >= threshold:
                kept.append(s)
        return kept

def _next_publication_estimate(now: datetime) -> datetime:
    """Rough estimate: 2 weeks after each quarter end. For the diagnostic sensor."""
    current_q = quarter_of(now)
    # Estimate publication ~15 days after quarter ends
    next_q = current_q.next()
    from .quarters import quarter_start_zurich

    q_end_of_current = quarter_start_zurich(next_q)
    pub = q_end_of_current + timedelta(days=15)
    return pub.astimezone(timezone.utc)
