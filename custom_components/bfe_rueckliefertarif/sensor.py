"""Sensor platform — single live tariff sensor.

v0.19.0 slim-down: down from 7 sensors to 1. The full applied-factor
breakdown (seasonal, HT/NT, bonuses, floor, cap, HKN) lives in the
sensor's ``extra_state_attributes``. Per-period analysis lives in the
``bfe_rueckliefertarif.get_breakdown`` service + the BFE tariff analysis
Lovelace card.

Why one sensor:
- ``MONETARY`` device class makes HA's Energy Dashboard auto-discover it
  as a "Return to grid → Price" candidate. That's the load-bearing
  integration point.
- Per-component sensors (``basisverguetung``, ``hkn_verguetung``, RMP-q/m,
  publication-date, tariffs-update) added clutter without scaling — every
  utility has a different mix of bonuses / HT-NT / seasonal, so a fixed
  sensor inventory either over- or under-fits.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_NAMENSPRAEFIX, DOMAIN
from .coordinator import BfeCoordinator

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Register the live tariff sensor for this config entry."""
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

    async_add_entities([GridExportTariffCurrentSensor(coordinator, entry, prefix)])


class GridExportTariffCurrentSensor(
    CoordinatorEntity[BfeCoordinator], SensorEntity
):
    """Current effective Rückliefervergütung (CHF/kWh) — Energy Dashboard wired.

    State: ``effective_chf_kwh`` for the current hour, with all applied
    factors layered in (seasonal rate, HT/NT, HKN, bonuses, federal floor,
    utility cap). Refreshes every 15 min so HT/NT and seasonal boundaries
    are picked up promptly.

    Attributes (``extra_state_attributes``): full breakdown — see
    ``importer.compute_breakdown_at`` for the dict shape. Power users can
    write template sensors / automations against any component
    (``base_input_rp_kwh``, ``applied_bonus_rp_kwh``, ``season_now``,
    ``ht_nt_now``, ``bonuses_applied``, etc.).

    For ``base_model = referenz_marktpreis`` and a quarter BFE has not yet
    published, the value falls back to the configured Plant-category
    Mindestvergütung floor — never to historical BFE data. ``is_estimate``
    + ``estimate_basis`` attributes flag this state.
    """

    _attr_has_entity_name = True
    _attr_native_unit_of_measurement = "CHF/kWh"
    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_suggested_display_precision = 4
    # state_class deliberately unset — HA disallows MEASUREMENT with MONETARY,
    # and per-kWh price is neither cumulative nor a snapshot in time.

    def __init__(
        self, coordinator: BfeCoordinator, entry: ConfigEntry, prefix: str
    ) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{entry.entry_id}_grid_export_tariff_current"
        self.entity_id = f"sensor.{prefix}_grid_export_tariff_current"
        self._attr_translation_key = "grid_export_tariff_current"

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
