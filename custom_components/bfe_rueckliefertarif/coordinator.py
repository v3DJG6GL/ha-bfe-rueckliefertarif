"""DataUpdateCoordinator — polls BFE every 6h, auto-imports newly published quarters.

The coordinator owns the "what's the current state of the world" view:
- Most recently published quarterly and monthly BFE prices.
- Which quarters have been imported (persisted via helpers.storage.Store).
- Derived current effective tariff (for the basis sensor).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
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
# 15 min for live sensor accuracy across HT/NT and seasonal boundaries.
# Per-tick work is cheap.
_UPDATE_INTERVAL = timedelta(minutes=15)
_STORAGE_VERSION = 1
_STORAGE_KEY_FMT = "bfe_rueckliefertarif.{entry_id}"


class BfeCoordinator(DataUpdateCoordinator):
    """Polls BFE CSVs, caches prices, tracks which quarters are imported."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
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
        # Single-flight guard around `_auto_import_newly_published`. The
        # first-refresh path schedules it as a background task while
        # `_async_update_data` returns early; a 6h tick that fires before the
        # background task finishes must wait, not interleave.
        self._auto_import_lock = asyncio.Lock()

    @property
    def _config(self) -> dict:
        """Live merge of entity-wiring (entry.data) + today's resolved versioned fields.

        Versioned fields (utility, kW, EV, HKN, billing) live exclusively in
        ``entry.options[OPT_CONFIG_HISTORY]`` — the open record IS today's
        config. Entity-wiring fields (export sensor, compensation sensor,
        name prefix) stay in ``entry.data``. This merge gives consumers a
        single dict with history as the source of truth for everything that
        varies over time.
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
            if abrechnungs_rhythmus == ABRECHNUNGS_RHYTHMUS_MONAT:
                self.quarterly, self.monthly = await asyncio.gather(
                    fetch_quarterly(session), fetch_monthly(session)
                )
            else:
                self.quarterly = await fetch_quarterly(session)

        # Defer auto-import on the first refresh so sensor platform setup
        # doesn't block on the recorder drain (`async_block_till_done`
        # inside `import_statistics` waits for the entire recorder queue,
        # not just our writes — multi-second on Postgres-backed recorders
        # during cold HA startup). Subsequent ticks (6h tick, refresh_data
        # service) run inline. `is_user_reload` distinguishes cold startup
        # / apply_change reload (gate the running-quarter estimate on
        # config-staleness) from 6h tick / refresh_data (keep unconditional
        # kWh roll-forward).
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
            "current_tariff_chf_kwh": breakdown["effective_chf_kwh"] if breakdown else None,
            "tariff_breakdown": breakdown,
        }

    def _tariff_breakdown(self) -> dict[str, Any] | None:
        """Return the live tariff breakdown dict for the
        ``grid_export_tariff_current`` sensor's state + attributes.

        Thin wrapper around ``importer.compute_breakdown_at(hour=now)``.
        Adds coordinator-state-dependent fields (``is_estimate``,
        ``estimate_basis``, ``tariffs_data_*``, ``current_rmp_*``) on top of
        the pure-tariff math returned by the importer helper. The seasonal
        / HT-NT / bonus dispatch logic is fully owned by the importer, so
        the live sensor and the importer's per-period writes can never
        diverge.
        """
        from .const import CONF_ENERGIEVERSORGER
        from .importer import compute_breakdown_at
        from .services import _cfg_for_entry_at_date

        utility_key = self._config.get(CONF_ENERGIEVERSORGER)
        if not utility_key:
            return None

        from datetime import date

        try:
            _cfg, tariff_cfg = _cfg_for_entry_at_date(self.hass, date.today())
        except (RuntimeError, KeyError, LookupError):
            return None

        now = datetime.now(UTC)
        q = quarter_of(now)
        rt = tariff_cfg.resolved

        # Reference price for RMP-based base models. None for fixed models.
        if rt.base_model in ("rmp_quartal", "rmp_monat"):
            if q in self.quarterly:
                reference = chf_per_mwh_to_rp_per_kwh(
                    self.quarterly[q].chf_per_mwh
                )
                is_estimate = False
                estimate_basis: str | None = None
            else:
                # BFE hasn't published the running quarter yet — fall back
                # to the federal floor. Never leak historical BFE prices.
                reference = rt.federal_floor_rp_kwh or 0.0
                is_estimate = True
                estimate_basis = "mindestverguetung_floor"
        else:
            reference = 0.0  # unused by fixed_flat / fixed_ht_nt
            is_estimate = False
            estimate_basis = None

        try:
            breakdown = compute_breakdown_at(tariff_cfg, reference, now)
        except (ValueError, KeyError):
            return None

        # When RMP fallback kicked in, override base_source so the
        # "estimate" intent is visible in the attribute.
        if is_estimate:
            breakdown["base_source"] = "fallback_mindestverguetung"
        elif rt.base_model in ("rmp_quartal", "rmp_monat"):
            breakdown["base_source"] = f"referenz_marktpreis_{q}"

        # Coordinator-only fields — depend on coordinator state, not pure
        # tariff math.
        breakdown["is_estimate"] = is_estimate
        breakdown["estimate_basis"] = estimate_basis

        # Tariffs DB sync info — moved from the dropped tariffs_last_remote_update sensor.
        tdc = self.hass.data.get(DOMAIN, {}).get("_tariffs_data")
        if tdc is not None:
            breakdown["tariffs_data_version"] = getattr(tdc, "last_data_version", None)
            breakdown["tariffs_data_last_synced"] = getattr(tdc, "last_remote_update", None)
        else:
            breakdown["tariffs_data_version"] = None
            breakdown["tariffs_data_last_synced"] = None

        # Reference-market-price diagnostics — moved from the dropped
        # referenzmarktpreis_q / _m sensors.
        if q in self.quarterly:
            breakdown["current_rmp_chf_mwh_q"] = self.quarterly[q].chf_per_mwh
        else:
            breakdown["current_rmp_chf_mwh_q"] = None
        if self._config.get(CONF_ABRECHNUNGS_RHYTHMUS) == ABRECHNUNGS_RHYTHMUS_MONAT:
            m = Month(now.year, now.month)
            if m in self.monthly:
                breakdown["current_rmp_chf_mwh_m"] = self.monthly[m].chf_per_mwh
            else:
                breakdown["current_rmp_chf_mwh_m"] = None

        return breakdown

    async def _auto_import_newly_published(
        self, *, is_user_reload: bool = False
    ) -> None:
        """Detect quarters needing reimport — BFE-price changes OR config drift.

        Reimports when the snapshot's resolved config (utility, kW, EV,
        HKN opt-in, tariffs.json version) differs from what the integration
        would resolve today. So a kW change in the options flow triggers
        retroactive recompute on the next coordinator refresh.

        ``_reimport_quarter`` updates ``self._imported[key]`` itself with the
        full snapshot, so this just drives the loop and skips quarters whose
        snapshot already matches the current resolved config.

        Quarters BFE has published but the bundled tariff database doesn't
        cover produce a single persistent notification listing the gap,
        instead of one warning per skipped quarter. Populating older years
        is a community-PR effort against the bfe-tariffs-data companion
        repo. Genuine errors still surface as warnings.

        After the loop, any quarters that were actually reimported get
        summarized in a single rich notification card (utility, model,
        kW/EV/HKN/billing, plus a per-month results table). The running
        quarter rides along in that notification when the estimate ran
        successfully (so apply_change recomputes show the running quarter
        alongside the stale-published ones).
        """
        from datetime import date

        from .const import OPT_CONFIG_HISTORY
        from .services import (
            _build_recompute_report,
            _import_running_quarter_estimate,
            _notify_recompute,
            _reimport_quarter,
        )

        # Single-flight: a 6h tick that fires while the first-refresh-deferred
        # background task is still running waits here, instead of
        # double-mutating `_imported`.
        async with self._auto_import_lock:
            # Skip quarters that predate the earliest config-history record
            # (the plant install date). Without this guard, every 6-hourly
            # coordinator refresh logs a "predates earliest record" WARNING
            # for each pre-install quarter BFE has published — same root
            # cause `_reimport_all_history`'s pre-active filter addresses
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
            # Track the most recent quarter reimported in this tick so a
            # contiguous running-quarter estimate can chain its anchor through
            # memory (avoiding the recorder commit-timer race that
            # `_reimport_all_history` handles separately).
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
                except Exception as exc:
                    _LOGGER.warning("Auto-import skipped %s: %s", q, exc)

            # Refresh the running-quarter estimate under the same triggers as
            # published quarters (6h tick, apply_change reload via
            # `async_config_entry_first_refresh`, `refresh_data` service). Skip
            # only when BFE has just published the running quarter — in that
            # case the loop above already imported it via `_reimport_quarter`.
            # Gate the estimate on user-reload triggers (cold startup,
            # apply_change reload) so an edit to a historical record that
            # doesn't touch the running quarter's resolved config doesn't
            # list the running quarter in the notification. 6h tick /
            # refresh_data keep unconditional behavior to preserve the kWh
            # roll-forward feature.
            running_q = quarter_of(datetime.now(UTC))
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
                    except Exception as exc:
                        _LOGGER.warning(
                            "Running-quarter estimate failed during auto-import: %s",
                            exc,
                        )

            await self._notify_skipped_quarters(no_data_skipped)
            # Only list the running quarter when its resolved config actually
            # changed (NOT just because the estimate ran on a kWh-roll-forward
            # tick). Also fire when ONLY the running quarter changed
            # (active-tariff edit case), so editing the open record produces a
            # notification too.
            if reimported or (running_q_estimated and running_q_config_changed):
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
        if snap.get("kwp") != tariff_cfg.installierte_leistung_kwp:
            return True
        if snap.get("eigenverbrauch_aktiviert") != tariff_cfg.eigenverbrauch_aktiviert:
            return True
        if snap.get("hkn_optin") != tariff_cfg.hkn_aktiviert:
            return True
        if snap.get("billing") != cfg.get(CONF_ABRECHNUNGS_RHYTHMUS):
            return True
        if snap.get("tariffs_json_version") != rt.tariffs_json_version:
            return True
        if (snap.get("user_inputs") or {}) != (tariff_cfg.user_inputs or {}):
            return True
        return False

    def _running_q_config_changed(
        self, prior_snapshot: dict, running_q: Quarter
    ) -> bool:
        """Running-quarter analog of `_snapshot_is_stale`, minus the
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
        if prior_snapshot.get("kwp") != tariff_cfg.installierte_leistung_kwp:
            return True
        if prior_snapshot.get("eigenverbrauch_aktiviert") != tariff_cfg.eigenverbrauch_aktiviert:
            return True
        if prior_snapshot.get("hkn_optin") != tariff_cfg.hkn_aktiviert:
            return True
        if prior_snapshot.get("billing") != cfg.get(CONF_ABRECHNUNGS_RHYTHMUS):
            return True
        if prior_snapshot.get("tariffs_json_version") != rt.tariffs_json_version:
            return True
        if (prior_snapshot.get("user_inputs") or {}) != (tariff_cfg.user_inputs or {}):
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
        )
        from homeassistant.components.persistent_notification import (
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
        except Exception:
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
                    datetime(1970, 1, 1, tzinfo=UTC),
                    datetime.now(UTC),
                )
            except Exception as exc:
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
            q_end_utc = quarter_end_zurich(q).astimezone(UTC)
            if q_end_utc >= threshold:
                kept.append(s)
        return kept

