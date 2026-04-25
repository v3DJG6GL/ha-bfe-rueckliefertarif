"""Diagnostic sensors for BFE Rückliefertarif.

The actually-important side effect of this integration is writing the Energy
Dashboard compensation LTS (sum-type). The sensors declared here are for
visibility and debugging — they expose current tariff values and publication state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import EntityCategory
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_HKN_VERGUETUNG_RP_KWH,
    CONF_NAMENSPRAEFIX,
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
    cfg = hass.data[DOMAIN][entry.entry_id]["config"]
    prefix = cfg.get(CONF_NAMENSPRAEFIX, "bfe_rueckliefertarif")

    coordinator = BfeCoordinator(hass, entry)
    await coordinator.async_load_state()
    await coordinator.async_config_entry_first_refresh()
    hass.data[DOMAIN][entry.entry_id]["coordinator"] = coordinator

    sensors: list[SensorEntity] = [
        BasisVerguetungSensor(coordinator, entry, prefix),
        AktuelleVerguetungChfKwhSensor(coordinator, entry, prefix),
        HknVerguetungSensor(entry, prefix, cfg.get(CONF_HKN_VERGUETUNG_RP_KWH, 0.0)),
        NaechsteReferenzmarktpreisPublikationSensor(coordinator, entry, prefix),
        ReferenzmarktpreisQSensor(coordinator, entry, prefix),
    ]
    if cfg.get(CONF_ABRECHNUNGS_RHYTHMUS) == ABRECHNUNGS_RHYTHMUS_MONAT:
        sensors.append(ReferenzmarktpreisMSensor(coordinator, entry, prefix))

    async_add_entities(sensors)


class _BaseSensor(SensorEntity):
    _attr_has_entity_name = True

    def __init__(
        self,
        entry: "ConfigEntry",
        prefix: str,
        suffix: str,
        translation_key: str,
    ) -> None:
        self._attr_unique_id = f"{entry.entry_id}_{suffix}"
        self.entity_id = f"sensor.{prefix}_{suffix}"
        self._attr_translation_key = translation_key


class BasisVerguetungSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current effective Basisvergütung in Rp/kWh.

    The sensor's state is the effective per-kWh rate the importer is applying
    right now. ``extra_state_attributes`` exposes the full computation
    breakdown (Mindestvergütung floor, Anrechenbarkeitsgrenze cap, whether
    the cap is binding) so users can verify what their tariff settings
    actually resolve to.
    """

    _attr_native_unit_of_measurement = "Rp/kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 2

    def __init__(
        self, coordinator: BfeCoordinator, entry: "ConfigEntry", prefix: str
    ) -> None:
        CoordinatorEntity.__init__(self, coordinator)
        _BaseSensor.__init__(self, entry, prefix, "basisverguetung", "basisverguetung")

    @property
    def native_value(self) -> float | None:
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("current_tariff_rp_kwh")

    @property
    def extra_state_attributes(self) -> dict | None:
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("tariff_breakdown")


class AktuelleVerguetungChfKwhSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current effective Rückliefervergütung in CHF/kWh — for Energy Dashboard wiring.

    Mirrors ``BasisVerguetungSensor`` but reports in CHF/kWh and exposes
    ``SensorDeviceClass.MONETARY`` so HA's Energy Dashboard surfaces it as a
    candidate "Return to grid → Price" entity. Wire this sensor there to get
    realistic running CHF estimates during the open quarter — the integration
    overwrites those LTS values with exact BFE-based numbers once the running
    quarter is published.

    For ``basisverguetung = referenz_marktpreis`` and a quarter BFE has not
    yet published, the value falls back to the configured Plant-category
    Mindestvergütung floor (plus any configured HKN, capped per the
    Anrechenbarkeitsgrenze if cap mode is on) — never to historical BFE
    data. The ``extra_state_attributes`` dict carries ``is_estimate`` and
    ``estimate_basis`` so the user can tell exact from estimated readings.
    """

    _attr_native_unit_of_measurement = "CHF/kWh"
    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 4

    def __init__(
        self, coordinator: BfeCoordinator, entry: "ConfigEntry", prefix: str
    ) -> None:
        CoordinatorEntity.__init__(self, coordinator)
        _BaseSensor.__init__(
            self,
            entry,
            prefix,
            "aktuelle_verguetung_chf_kwh",
            "aktuelle_verguetung_chf_kwh",
        )

    @property
    def native_value(self) -> float | None:
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("current_tariff_chf_kwh")

    @property
    def extra_state_attributes(self) -> dict | None:
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("tariff_breakdown")


class HknVerguetungSensor(_BaseSensor):
    """Configured HKN-Vergütung — diagnostic so user can spot staleness."""

    _attr_native_unit_of_measurement = "Rp/kWh"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, entry: "ConfigEntry", prefix: str, value: float) -> None:
        super().__init__(entry, prefix, "hkn_verguetung", "hkn_verguetung")
        self._attr_native_value = value


class NaechsteReferenzmarktpreisPublikationSensor(
    CoordinatorEntity[BfeCoordinator], _BaseSensor
):
    """Estimated datetime of next BFE Referenz-Marktpreis publication."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self, coordinator: BfeCoordinator, entry: "ConfigEntry", prefix: str
    ) -> None:
        CoordinatorEntity.__init__(self, coordinator)
        _BaseSensor.__init__(
            self,
            entry,
            prefix,
            "naechste_referenzmarktpreis_publikation",
            "naechste_referenzmarktpreis_publikation",
        )

    @property
    def native_value(self) -> "datetime | None":
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("next_publication")


class ReferenzmarktpreisQSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current quarterly BFE Referenz-Marktpreis, CHF/MWh."""

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
            "referenzmarktpreis_q",
        )

    @property
    def native_value(self) -> float | None:
        from datetime import datetime, timezone

        prices = self.coordinator.quarterly if self.coordinator else {}
        q = quarter_of(datetime.now(timezone.utc))
        price = prices.get(q)
        return price.chf_per_mwh if price else None


class ReferenzmarktpreisMSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current monthly BFE Referenz-Marktpreis, CHF/MWh (monatlicher Rhythmus only)."""

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
            "referenzmarktpreis_m",
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
