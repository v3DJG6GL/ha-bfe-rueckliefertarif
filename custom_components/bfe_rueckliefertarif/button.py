"""Admin buttons for BFE Rückliefertarif.

Three one-shot actions exposed on the integration's device page:
- Reload Referenz-Marktpreise (calls the ``refresh`` service).
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
            ReloadReferenzmarktpreiseButton(entry, prefix),
            RecomputeLetztesPubliziertesQuartalButton(entry, prefix),
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


class ReloadReferenzmarktpreiseButton(_BaseButton):
    def __init__(self, entry: "ConfigEntry", prefix: str) -> None:
        super().__init__(
            entry,
            prefix,
            "reload_referenzmarktpreise",
            "reload_referenzmarktpreise",
        )

    async def async_press(self) -> None:
        await self.hass.services.async_call(DOMAIN, "refresh", blocking=True)
        notify(
            self.hass,
            "Referenz-Marktpreise aktualisiert.",
            title="BFE Rückliefertarif",
            notification_id=f"{DOMAIN}_{self._entry.entry_id}_refresh",
        )


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
        from .services import _reimport_quarter

        coordinator = self.hass.data[DOMAIN][self._entry.entry_id].get("coordinator")
        if coordinator is None or not coordinator.quarterly:
            notify(
                self.hass,
                "Keine Referenz-Marktpreise verfügbar — zuerst 'Referenz-Marktpreise jetzt laden' ausführen.",
                title="BFE Rückliefertarif",
                notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_quarter",
            )
            return

        q = max(coordinator.quarterly.keys())
        try:
            await _reimport_quarter(self.hass, q)
            msg = f"Rückliefervergütung für {q} neu berechnet."
        except Exception as exc:  # noqa: BLE001
            _LOGGER.exception("Recompute %s failed", q)
            msg = f"Fehler beim Neuberechnen von {q}: {exc}"
        notify(
            self.hass,
            msg,
            title="BFE Rückliefertarif",
            notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_quarter",
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
        await self.hass.services.async_call(
            DOMAIN, "reimport_all_history", blocking=True
        )
        notify(
            self.hass,
            "Rückliefervergütung für die gesamte Historie neu berechnet.",
            title="BFE Rückliefertarif",
            notification_id=f"{DOMAIN}_{self._entry.entry_id}_recompute_history",
        )
