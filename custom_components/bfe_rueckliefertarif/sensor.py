"""Diagnostic sensors for BFE Rückliefertarif.

The actually-important side effect of this integration is writing the Energy
Dashboard compensation LTS (sum-type). The sensors declared here are for
visibility and debugging — they expose current tariff values and publication state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.sensor import SensorEntity, SensorStateClass
from homeassistant.const import EntityCategory
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    BILLING_MODE_MONTHLY,
    CONF_BILLING_MODE,
    CONF_ENTITY_PREFIX,
    CONF_HKN_BONUS,
    DOMAIN,
)
from .coordinator import BfeCoordinator
from .quarters import quarter_of

if TYPE_CHECKING:
    from datetime import datetime

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


async def async_setup_entry(
    hass: "HomeAssistant",
    entry: "ConfigEntry",
    async_add_entities: "AddEntitiesCallback",
) -> None:
    """Register diagnostic sensors for this config entry."""
    from datetime import datetime, timezone

    cfg = hass.data[DOMAIN][entry.entry_id]["config"]
    prefix = cfg.get(CONF_ENTITY_PREFIX, "bfe_rueckliefertarif")

    coordinator = BfeCoordinator(hass, entry)
    await coordinator.async_load_state()
    await coordinator.async_config_entry_first_refresh()
    hass.data[DOMAIN][entry.entry_id]["coordinator"] = coordinator

    sensors: list[SensorEntity] = [
        BasisTarifSensor(coordinator, entry, prefix),
        HknBonusSensor(entry, prefix, cfg.get(CONF_HKN_BONUS, 0.0)),
        NextPublicationSensor(coordinator, entry, prefix),
        ReferenzQSensor(coordinator, entry, prefix),
    ]
    if cfg.get(CONF_BILLING_MODE) == BILLING_MODE_MONTHLY:
        sensors.append(ReferenzMSensor(coordinator, entry, prefix))

    async_add_entities(sensors)

    # Current time (local var) silences unused-import on release builds
    _ = datetime.now(timezone.utc)


class _BaseSensor(SensorEntity):
    _attr_has_entity_name = True

    def __init__(self, entry: "ConfigEntry", prefix: str, suffix: str, name: str) -> None:
        self._attr_unique_id = f"{entry.entry_id}_{suffix}"
        self.entity_id = f"sensor.{prefix}_{suffix}"
        self._attr_name = name


class BasisTarifSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current effective tariff in Rp/kWh (LTS-enabled for graphing)."""

    _attr_native_unit_of_measurement = "Rp/kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 2

    def __init__(
        self, coordinator: BfeCoordinator, entry: "ConfigEntry", prefix: str
    ) -> None:
        CoordinatorEntity.__init__(self, coordinator)
        _BaseSensor.__init__(self, entry, prefix, "basis", "Effektiver Rückliefertarif")

    @property
    def native_value(self) -> float | None:
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("current_tariff_rp_kwh")


class HknBonusSensor(_BaseSensor):
    """Configured HKN bonus — diagnostic so user can spot staleness."""

    _attr_native_unit_of_measurement = "Rp/kWh"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, entry: "ConfigEntry", prefix: str, value: float) -> None:
        super().__init__(entry, prefix, "hkn_bonus", "HKN-Vergütung (konfiguriert)")
        self._attr_native_value = value


class NextPublicationSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Estimated datetime of next BFE publication."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self, coordinator: BfeCoordinator, entry: "ConfigEntry", prefix: str
    ) -> None:
        CoordinatorEntity.__init__(self, coordinator)
        _BaseSensor.__init__(
            self, entry, prefix, "next_publication", "Nächste BFE-Publikation"
        )

    @property
    def native_value(self) -> "datetime | None":
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("next_publication")


class ReferenzQSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current quarterly BFE reference market price, CHF/MWh."""

    _attr_native_unit_of_measurement = "CHF/MWh"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self, coordinator: BfeCoordinator, entry: "ConfigEntry", prefix: str
    ) -> None:
        CoordinatorEntity.__init__(self, coordinator)
        _BaseSensor.__init__(
            self,
            entry,
            prefix,
            "referenzmarktpreis_q",
            "Referenz-Marktpreis aktuelles Quartal",
        )

    @property
    def native_value(self) -> float | None:
        from datetime import datetime, timezone

        prices = self.coordinator.quarterly if self.coordinator else {}
        q = quarter_of(datetime.now(timezone.utc))
        price = prices.get(q)
        return price.chf_per_mwh if price else None


class ReferenzMSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current monthly BFE reference market price, CHF/MWh (monthly billing only)."""

    _attr_native_unit_of_measurement = "CHF/MWh"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self, coordinator: BfeCoordinator, entry: "ConfigEntry", prefix: str
    ) -> None:
        CoordinatorEntity.__init__(self, coordinator)
        _BaseSensor.__init__(
            self,
            entry,
            prefix,
            "referenzmarktpreis_m",
            "Referenz-Marktpreis aktueller Monat",
        )

    @property
    def native_value(self) -> float | None:
        from datetime import datetime, timezone

        from .quarters import Month

        now = datetime.now(timezone.utc)
        m = Month(now.year, now.month)
        prices = self.coordinator.monthly if self.coordinator else {}
        price = prices.get(m)
        return price.chf_per_mwh if price else None
