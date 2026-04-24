"""Admin buttons for BFE Rückliefertarif.

Three one-shot actions exposed on the integration's device page:
- Reload Referenz-Marktpreise (calls the ``refresh`` service).
- Recompute current quarter's Rückliefervergütung.
- Recompute entire history.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from homeassistant.components.button import ButtonEntity
from homeassistant.components.persistent_notification import async_create as notify
from homeassistant.const import EntityCategory

from .const import CONF_NAMENSPRAEFIX, DOMAIN
from .quarters import quarter_of

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
            RecomputeAktuellesQuartalButton(entry, prefix),
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


class RecomputeAktuellesQuartalButton(_BaseButton):
    def __init__(self, entry: "ConfigEntry", prefix: str) -> None:
        super().__init__(
            entry,
            prefix,
            "recompute_aktuelles_quartal",
            "recompute_aktuelles_quartal",
        )

    async def async_press(self) -> None:
        from .services import _reimport_quarter

        q = quarter_of(datetime.now(UTC))
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
