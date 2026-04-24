"""DataUpdateCoordinator — polls BFE every 6h, auto-imports newly published quarters.

The coordinator owns the "what's the current state of the world" view:
- Most recently published quarterly and monthly BFE prices.
- Which quarters have been imported (persisted via helpers.storage.Store).
- Derived current effective tariff (for the basis sensor).
"""

from __future__ import annotations

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
        self._config = dict(entry.data)
        self._store = self._make_store()
        self._imported: dict[str, dict[str, Any]] = {}
        self.quarterly: dict[Quarter, BfePrice] = {}
        self.monthly: dict[Month, BfePrice] = {}

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

        await self._auto_import_newly_published()
        return {
            "quarterly": self.quarterly,
            "monthly": self.monthly,
            "current_tariff_rp_kwh": self._current_effective_tariff_rp_kwh(),
            "next_publication": _next_publication_estimate(datetime.now(timezone.utc)),
        }

    async def _auto_import_newly_published(self) -> None:
        """Detect quarters in BFE data that aren't imported yet or were imported at a different price."""
        from .services import _reimport_quarter

        for q, price in sorted(self.quarterly.items()):
            key = str(q)
            prior = self._imported.get(key)
            price_chf = price.chf_per_mwh
            if prior and prior.get("q_price_chf_mwh") == price_chf:
                continue
            try:
                await _reimport_quarter(self.hass, q)
            except Exception as exc:  # noqa: BLE001
                _LOGGER.warning("Auto-import skipped %s: %s", q, exc)
                continue
            self._imported[key] = {
                "q_price_chf_mwh": price_chf,
                "imported_at": datetime.now(timezone.utc).isoformat(),
            }
        await self._async_save_state()

    def _current_effective_tariff_rp_kwh(self) -> float | None:
        """Basisvergütung-sensor value: effective Rp/kWh for the current quarter.

        Returns the tariff the importer would apply right now. If the current
        quarter's BFE price hasn't been published yet, falls back to the
        Mindestvergütung + HKN-Vergütung (the conservative placeholder shown in
        HA during the ~2-week publication gap).
        """
        from .const import (
            BASISVERGUETUNG_FIXPREIS,
            CONF_ANLAGENKATEGORIE,
            CONF_BASISVERGUETUNG,
            CONF_FIXPREIS_RP_KWH,
            CONF_HKN_VERGUETUNG_RP_KWH,
            CONF_INSTALLIERTE_LEISTUNG_KW,
        )
        from .tariff import (
            Segment,
            effective_rp_kwh_fixed,
            effective_rp_kwh_rmp,
            mindestverguetung_rp_kwh,
        )

        now = datetime.now(timezone.utc)
        q = quarter_of(now)
        seg = Segment(self._config[CONF_ANLAGENKATEGORIE])
        kw = float(self._config.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0) or 0.0)
        hkn = float(self._config.get(CONF_HKN_VERGUETUNG_RP_KWH, 0.0))
        basisverguetung = self._config[CONF_BASISVERGUETUNG]

        if basisverguetung == BASISVERGUETUNG_FIXPREIS:
            fixed = float(self._config.get(CONF_FIXPREIS_RP_KWH, 0.0) or 0.0)
            return effective_rp_kwh_fixed(fixed, seg, kw, hkn)

        # Referenz-Marktpreis mode: need BFE price for current quarter, else fall back
        if q in self.quarterly:
            ref = chf_per_mwh_to_rp_per_kwh(self.quarterly[q].chf_per_mwh)
            return effective_rp_kwh_rmp(ref, seg, kw, hkn)

        floor = mindestverguetung_rp_kwh(seg, kw)
        if floor is None:
            return None
        return floor + hkn


def _next_publication_estimate(now: datetime) -> datetime:
    """Rough estimate: 2 weeks after each quarter end. For the diagnostic sensor."""
    current_q = quarter_of(now)
    # Estimate publication ~15 days after quarter ends
    next_q = current_q.next()
    from .quarters import quarter_start_zurich

    q_end_of_current = quarter_start_zurich(next_q)
    pub = q_end_of_current + timedelta(days=15)
    return pub.astimezone(timezone.utc)
