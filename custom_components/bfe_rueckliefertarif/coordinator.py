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
        breakdown = self._tariff_breakdown()
        return {
            "quarterly": self.quarterly,
            "monthly": self.monthly,
            "current_tariff_rp_kwh": breakdown["effective_rp_kwh"] if breakdown else None,
            "tariff_breakdown": breakdown,
            "next_publication": _next_publication_estimate(datetime.now(timezone.utc)),
        }

    def _tariff_breakdown(self) -> dict[str, Any] | None:
        """Return a dict explaining how the effective rate is computed.

        Useful for the diagnostic sensor's attributes so the user can see
        whether the cap is binding (e.g. 30 + 10 → capped at 10.96).
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
            anrechenbarkeitsgrenze_rp_kwh,
            mindestverguetung_rp_kwh,
        )

        try:
            seg = Segment(self._config[CONF_ANLAGENKATEGORIE])
        except (KeyError, ValueError):
            return None
        kw = float(self._config.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0) or 0.0)
        hkn = float(self._config.get(CONF_HKN_VERGUETUNG_RP_KWH, 0.0))
        basisverguetung = self._config.get(CONF_BASISVERGUETUNG)

        floor = mindestverguetung_rp_kwh(seg, kw) if kw > 0 or seg.value not in (
            "mid_mit_ev", "large_mit_ev"
        ) else 0.0
        floor = floor if floor is not None else 0.0
        cap = anrechenbarkeitsgrenze_rp_kwh(seg)

        now = datetime.now(timezone.utc)
        q = quarter_of(now)
        if basisverguetung == BASISVERGUETUNG_FIXPREIS:
            base_input = float(self._config.get(CONF_FIXPREIS_RP_KWH, 0.0) or 0.0)
            base_label = "fixpreis"
        else:
            if q in self.quarterly:
                base_input = chf_per_mwh_to_rp_per_kwh(self.quarterly[q].chf_per_mwh)
                base_label = f"referenz_marktpreis_{q}"
            else:
                base_input = floor
                base_label = "fallback_mindestverguetung"

        base_after_floor = max(base_input, floor)
        theoretical_total = base_after_floor + hkn
        effective = min(theoretical_total, cap)
        cap_binds = theoretical_total > cap

        return {
            "anlagenkategorie": seg.value,
            "basisverguetung": basisverguetung,
            "base_input_rp_kwh": round(base_input, 4),
            "base_source": base_label,
            "minimalverguetung_rp_kwh": round(floor, 4),
            "base_after_floor_rp_kwh": round(base_after_floor, 4),
            "hkn_verguetung_rp_kwh": round(hkn, 4),
            "theoretical_total_rp_kwh": round(theoretical_total, 4),
            "anrechenbarkeitsgrenze_rp_kwh": round(cap, 4),
            "effective_rp_kwh": round(effective, 4),
            "cap_binds": cap_binds,
            "hkn_reduced_to": round(max(0.0, cap - base_after_floor), 4) if cap_binds else None,
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

def _next_publication_estimate(now: datetime) -> datetime:
    """Rough estimate: 2 weeks after each quarter end. For the diagnostic sensor."""
    current_q = quarter_of(now)
    # Estimate publication ~15 days after quarter ends
    next_q = current_q.next()
    from .quarters import quarter_start_zurich

    q_end_of_current = quarter_start_zurich(next_q)
    pub = q_end_of_current + timedelta(days=15)
    return pub.astimezone(timezone.utc)
