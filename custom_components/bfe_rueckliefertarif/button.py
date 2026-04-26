"""Admin buttons for BFE Rückliefertarif.

One-shot actions exposed on the integration's device page:
- Reload data: refetch tariffs.json from the companion repo *and* re-poll BFE
  reference market prices (auto-imports newly published / staleness-flagged
  quarters in one go).
- Recompute the most recently *published* quarter (BFE publishes ~10 working
  days after each quarter end, so the running quarter usually has no price
  yet — picking the latest published quarter is what users actually want).
- Recompute entire history.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from homeassistant.components.button import ButtonEntity
from homeassistant.components.persistent_notification import async_create as notify
from homeassistant.const import EntityCategory

from .const import CONF_NAMENSPRAEFIX, DOMAIN

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: "HomeAssistant",
    entry: "ConfigEntry",
    async_add_entities: "AddEntitiesCallback",
) -> None:
    """Register admin buttons for this config entry."""
    cfg = hass.data[DOMAIN][entry.entry_id]["config"]
    prefix = cfg.get(CONF_NAMENSPRAEFIX, "bfe_rueckliefertarif")

    async_add_entities(
        [
            ReloadDatenButton(entry, prefix),
            RecomputeLetztesPubliziertesQuartalButton(entry, prefix),
            RecomputeAktuellesQuartalEstimateButton(entry, prefix),
            RecomputeHistorieButton(entry, prefix),
        ]
    )


class _BaseButton(ButtonEntity):
    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        entry: "ConfigEntry",
        prefix: str,
        suffix: str,
        translation_key: str,
    ) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_{suffix}"
        self.entity_id = f"button.{prefix}_{suffix}"
        self._attr_translation_key = translation_key


def _format_quarters(qs) -> str:
    return ", ".join(str(q) for q in qs)


class ReloadDatenButton(_BaseButton):
    """Refetch tariffs.json from the companion repo and re-poll BFE prices.

    Order: tariffs first so when ``_refresh_coordinator`` triggers
    ``_auto_import_newly_published`` next, it resolves rates against the
    freshly fetched tariff file. A tariff-fetch failure is non-fatal and
    must not block the BFE refresh.
    """

    def __init__(self, entry: "ConfigEntry", prefix: str) -> None:
        super().__init__(
            entry,
            prefix,
            "reload_daten",
            "reload_daten",
        )

    async def async_press(self) -> None:
        from .services import _refresh_coordinator

        tariff_line = await self._refresh_tariffs()

        try:
            result = await _refresh_coordinator(self.hass)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.exception("BFE refresh failed")
            rmp_line = f"Referenz-Marktpreise: Fehler — {exc}"
        else:
            avail = result["available"]
            new = result["newly_imported"]
            line = f"Referenz-Marktpreise: {len(avail)} Quartale verfügbar"
            if avail:
                line += f" (neuestes: {max(avail)})"
            if new:
                line += f"; neu importiert: {_format_quarters(new)}"
            else:
                line += "; keine neuen Quartale seit letztem Import."
            rmp_line = line

        notify(
            self.hass,
            f"{tariff_line}\n{rmp_line}",
            title="BFE Rückliefertarif",
            notification_id=f"{DOMAIN}_{self._entry.entry_id}_reload",
        )

    async def _refresh_tariffs(self) -> str:
        tdc = self.hass.data.get(DOMAIN, {}).get("_tariffs_data")
        if tdc is None:
            return "Tarifdaten: Coordinator nicht initialisiert — übersprungen."
        try:
            ok = await tdc.async_refresh()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.exception("Tariffs refresh raised")
            return f"Tarifdaten: Fehler — {exc}; bundled Fallback aktiv."
        if not ok:
            err = tdc.last_error or "unbekannt"
            return f"Tarifdaten: Fehler — {err}; bundled Fallback aktiv."
        ts = tdc.last_remote_update.strftime("%Y-%m-%d %H:%M UTC") if tdc.last_remote_update else "unbekannt"
        return f"Tarifdaten: aktualisiert ({ts})."


class RecomputeLetztesPubliziertesQuartalButton(_BaseButton):
    """Recompute the most recently *published* quarter.

    Uses ``max(coordinator.quarterly.keys())`` instead of ``quarter_of(now())``.
    BFE publishes ~10 working days after each quarter end, so the running
    quarter (what ``quarter_of(now())`` returns) almost never has a price
    yet — picking the latest published quarter is what users want.
    """

    def __init__(self, entry: "ConfigEntry", prefix: str) -> None:
        super().__init__(
            entry,
            prefix,
            "recompute_letztes_publiziertes_quartal",
            "recompute_letztes_publiziertes_quartal",
        )

    async def async_press(self) -> None:
        from .services import (
            _build_recompute_report,
            _notify_recompute,
            _reimport_quarter,
        )

        coordinator = self.hass.data[DOMAIN][self._entry.entry_id].get("coordinator")
        if coordinator is None or not coordinator.quarterly:
            notify(
                self.hass,
                "No reference market prices available — run 'Reload data' first.",
                title="BFE Rückliefertarif",
                notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_quarter",
            )
            return

        q = max(coordinator.quarterly.keys())
        try:
            await _reimport_quarter(self.hass, q)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.exception("Recompute %s failed", q)
            notify(
                self.hass,
                f"Recompute of {q} failed: {exc}",
                title="BFE Rückliefertarif",
                notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_quarter",
            )
            return

        report = _build_recompute_report(self.hass, [q])
        _notify_recompute(self.hass, self._entry.entry_id, report)


class RecomputeAktuellesQuartalEstimateButton(_BaseButton):
    """Re-import LTS for the running quarter using the current effective-rate estimate.

    Useful when BFE has not yet published the running quarter and the user
    wants the Energy Dashboard to show realistic CHF values immediately
    instead of whatever stale price source was wired before. Once BFE
    publishes, the regular import path overwrites these values with exact
    BFE-based numbers.
    """

    def __init__(self, entry: "ConfigEntry", prefix: str) -> None:
        super().__init__(
            entry,
            prefix,
            "recompute_aktuelles_quartal_estimate",
            "recompute_aktuelles_quartal_estimate",
        )

    async def async_press(self) -> None:
        from .services import _import_running_quarter_estimate

        try:
            result = await _import_running_quarter_estimate(self.hass)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.exception("Running-quarter estimate import failed")
            notify(
                self.hass,
                f"Estimate import failed: {exc}",
                title="BFE Rückliefertarif",
                notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_estimate",
            )
            return

        lines = [
            f"Quarter {result['quarter']} estimated at "
            f"{result['rate_rp_kwh']:.4f} Rp/kWh — "
            f"{result['hours_imported']} hours imported, "
            f"total {result['chf_total']:.2f} CHF",
        ]
        if result["is_estimate"]:
            lines.append(
                f"Based on {result['estimate_basis']} — "
                "will be overwritten with exact values when BFE publishes."
            )
        notify(
            self.hass,
            "\n".join(lines),
            title="BFE Rückliefertarif",
            notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_estimate",
        )


class RecomputeHistorieButton(_BaseButton):
    def __init__(self, entry: "ConfigEntry", prefix: str) -> None:
        super().__init__(
            entry,
            prefix,
            "recompute_historie",
            "recompute_historie",
        )

    async def async_press(self) -> None:
        from .services import (
            _build_recompute_report,
            _notify_recompute,
            _reimport_all_history,
        )

        try:
            result = await _reimport_all_history(self.hass)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.exception("Reimport history failed")
            notify(
                self.hass,
                f"Recompute failed: {exc}",
                title="BFE Rückliefertarif",
                notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_history",
            )
            return

        imported = result["imported"]
        if imported:
            report = _build_recompute_report(self.hass, imported)
            _notify_recompute(self.hass, self._entry.entry_id, report)
        else:
            # Nothing reimported (everything was skipped) — short status card.
            skipped = result.get("skipped") or []
            failed = result.get("failed") or []
            lines = ["0 quarters recomputed."]
            if skipped:
                lines.append(
                    f"{len(skipped)} skipped (not yet published by BFE): "
                    f"{_format_quarters(skipped)}"
                )
            if failed:
                lines.append(
                    f"{len(failed)} errors — see logs: {_format_quarters(failed)}"
                )
            notify(
                self.hass,
                "\n".join(lines),
                title="BFE Rückliefertarif",
                notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_history",
            )
