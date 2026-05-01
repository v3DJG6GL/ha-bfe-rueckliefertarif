"""Diagnostic sensors for BFE Rückliefertarif.

The actually-important side effect of this integration is writing the Energy
Dashboard compensation LTS (sum-type). The sensors declared here are for
visibility and debugging — they expose current tariff values and publication state.
"""

from __future__ import annotations

from datetime import UTC
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
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
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
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Register diagnostic sensors for this config entry."""
    from datetime import date

    from .tariffs_db import resolve_tariff_at

    cfg = hass.data[DOMAIN][entry.entry_id]["config"]
    prefix = cfg.get(CONF_NAMENSPRAEFIX, "bfe_rueckliefertarif")

    coordinator = BfeCoordinator(hass, entry)
    await coordinator.async_load_state()
    # Register in hass.data BEFORE first_refresh: the refresh runs
    # _auto_import_newly_published, which calls _reimport_quarter →
    # _record_snapshot. _record_snapshot looks up the coordinator via
    # _first_entry_data(hass).get("coordinator") and silently no-ops if
    # it's missing — which would lose the per-quarter snapshot writes
    # (monthly[] / total_kwh / total_chf added in v0.7) that the
    # post-refresh recompute notification reads from. Setting this slot
    # first keeps both writes and reads using the same dict.
    hass.data[DOMAIN][entry.entry_id]["coordinator"] = coordinator
    await coordinator.async_config_entry_first_refresh()

    # Resolve HKN from tariffs.json for the diagnostic sensor's static value.
    # If the user opted in, surface the utility's published HKN; otherwise 0.
    hkn_value = 0.0
    try:
        rt = resolve_tariff_at(
            cfg[CONF_ENERGIEVERSORGER],
            date.today(),
            kw=float(cfg.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0) or 0.0),
            eigenverbrauch=bool(cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True)),
        )
        if cfg.get(CONF_HKN_AKTIVIERT, False):
            hkn_value = rt.hkn_rp_kwh
    except (KeyError, LookupError):
        pass

    sensors: list[SensorEntity] = [
        BasisVerguetungSensor(coordinator, entry, prefix),
        AktuelleVerguetungChfKwhSensor(coordinator, entry, prefix),
        HknVerguetungSensor(entry, prefix, hkn_value),
        NaechsteReferenzmarktpreisPublikationSensor(coordinator, entry, prefix),
        ReferenzmarktpreisQSensor(coordinator, entry, prefix),
        TariffsDataLastUpdateSensor(entry, prefix),
    ]
    if cfg.get(CONF_ABRECHNUNGS_RHYTHMUS) == ABRECHNUNGS_RHYTHMUS_MONAT:
        sensors.append(ReferenzmarktpreisMSensor(coordinator, entry, prefix))

    async_add_entities(sensors)


class _BaseSensor(SensorEntity):
    _attr_has_entity_name = True

    def __init__(
        self,
        entry: ConfigEntry,
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
        self, coordinator: BfeCoordinator, entry: ConfigEntry, prefix: str
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
    _attr_suggested_display_precision = 4
    # HA disallows `MEASUREMENT` with `MONETARY` (monetary expects TOTAL or
    # None — and a per-kWh price is neither cumulative nor a single point
    # in time, so leave state_class unset).

    def __init__(
        self, coordinator: BfeCoordinator, entry: ConfigEntry, prefix: str
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

    def __init__(self, entry: ConfigEntry, prefix: str, value: float) -> None:
        super().__init__(entry, prefix, "hkn_verguetung", "hkn_verguetung")
        self._attr_native_value = value


class NaechsteReferenzmarktpreisPublikationSensor(
    CoordinatorEntity[BfeCoordinator], _BaseSensor
):
    """Estimated datetime of next BFE Referenz-Marktpreis publication."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self, coordinator: BfeCoordinator, entry: ConfigEntry, prefix: str
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
    def native_value(self) -> datetime | None:
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("next_publication")


class ReferenzmarktpreisQSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current quarterly BFE Referenz-Marktpreis, CHF/MWh."""

    _attr_native_unit_of_measurement = "CHF/MWh"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self, coordinator: BfeCoordinator, entry: ConfigEntry, prefix: str
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
        from datetime import datetime

        prices = self.coordinator.quarterly if self.coordinator else {}
        q = quarter_of(datetime.now(UTC))
        price = prices.get(q)
        return price.chf_per_mwh if price else None


class TariffsDataLastUpdateSensor(_BaseSensor):
    """Diagnostic: when the companion repo's tariffs.json was last fetched.

    `None` while the bundled file is in use (no successful fetch yet, or
    the user disconnected from the network). Phase 6 of the v0.5 plan.
    """

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, entry: ConfigEntry, prefix: str) -> None:
        super().__init__(
            entry, prefix,
            "tariffs_last_remote_update",
            "tariffs_last_remote_update",
        )
        self._entry = entry

    @property
    def native_value(self) -> datetime | None:
        from .const import DOMAIN as _DOMAIN  # avoid top-level cycle

        tdc = self.hass.data.get(_DOMAIN, {}).get("_tariffs_data") if self.hass else None
        return tdc.last_remote_update if tdc else None

    @property
    def extra_state_attributes(self) -> dict | None:
        from .const import DOMAIN as _DOMAIN
        from .tariffs_db import get_source

        tdc = self.hass.data.get(_DOMAIN, {}).get("_tariffs_data") if self.hass else None
        return {
            "source": get_source(),
            "last_error": tdc.last_error if tdc else None,
            "data_version": tdc.last_data_version if tdc else None,
            "data_last_updated": tdc.last_data_updated if tdc else None,
            "schema_source": tdc.last_schema_source if tdc else None,
            "last_schema_error": tdc.last_schema_error if tdc else None,
        }


class ReferenzmarktpreisMSensor(CoordinatorEntity[BfeCoordinator], _BaseSensor):
    """Current monthly BFE Referenz-Marktpreis, CHF/MWh (monatlicher Rhythmus only)."""

    _attr_native_unit_of_measurement = "CHF/MWh"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self, coordinator: BfeCoordinator, entry: ConfigEntry, prefix: str
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
        from datetime import datetime

        from .quarters import Month

        now = datetime.now(UTC)
        m = Month(now.year, now.month)
        prices = self.coordinator.monthly if self.coordinator else {}
        price = prices.get(m)
        return price.chf_per_mwh if price else None
