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
            "current_tariff_chf_kwh": breakdown["effective_chf_kwh"] if breakdown else None,
            "tariff_breakdown": breakdown,
            "next_publication": _next_publication_estimate(datetime.now(timezone.utc)),
        }

    def _tariff_breakdown(self) -> dict[str, Any] | None:
        """Return a dict explaining how the effective Rückliefervergütung is computed.

        Used by the BasisVerguetungSensor / AktuelleVerguetungChfKwhSensor
        extra_state_attributes so the user can verify what their tariff
        settings actually resolve to. v0.5 fields:

        - ``utility``, ``tariff_source``, ``floor_label`` — from tariffs.json
        - ``eigenverbrauch_aktiviert``, ``hkn_aktiviert`` — user choice
        - ``base_input_rp_kwh``, ``base_source`` — how the base is sourced
          (fixed_flat / rmp_quartal price / fallback floor)
        - ``minimalverguetung_rp_kwh`` — federal floor for (kW, EV)
        - ``anrechenbarkeitsgrenze_rp_kwh`` — utility cap (None when cap_mode is off)
        - ``obergrenze_aktiv`` — whether the cap binds the producer's payment
        - ``effective_rp_kwh`` / ``effective_chf_kwh`` — final per-kWh rate
        - ``is_estimate``, ``estimate_basis`` — set when BFE hasn't published
          the running quarter yet
        """
        from datetime import date

        from .const import (
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            CONF_ENERGIEVERSORGER,
            CONF_HKN_AKTIVIERT,
            CONF_INSTALLIERTE_LEISTUNG_KW,
        )
        from .tariff import effective_rp_kwh
        from .tariffs_db import resolve_tariff_at

        utility_key = self._config.get(CONF_ENERGIEVERSORGER)
        if not utility_key:
            return None
        kw = float(self._config.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0) or 0.0)
        eigenverbrauch = bool(self._config.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True))
        hkn_aktiviert = bool(self._config.get(CONF_HKN_AKTIVIERT, False))

        try:
            rt = resolve_tariff_at(
                utility_key, date.today(), kw=kw, eigenverbrauch=eigenverbrauch
            )
        except (KeyError, LookupError):
            return None

        hkn = rt.hkn_rp_kwh if hkn_aktiviert else 0.0
        floor = rt.federal_floor_rp_kwh
        floor_value = floor if floor is not None else 0.0
        cap = rt.cap_rp_kwh if rt.cap_mode else None

        now = datetime.now(timezone.utc)
        q = quarter_of(now)
        is_estimate = False
        estimate_basis: str | None = None
        if rt.base_model == "fixed_flat":
            base_input = rt.fixed_rp_kwh or 0.0
            base_label = "fixed_flat"
        elif rt.base_model == "fixed_ht_nt":
            base_input = rt.fixed_ht_rp_kwh or 0.0
            base_label = "fixed_ht_nt"
        elif q in self.quarterly:
            base_input = chf_per_mwh_to_rp_per_kwh(self.quarterly[q].chf_per_mwh)
            base_label = f"referenz_marktpreis_{q}"
        else:
            # BFE has not yet published the running quarter (or no BFE data
            # at all) — fall back to the federal floor. Never leak historical
            # BFE prices: the estimate must derive only from configured values.
            # Once BFE publishes, the normal import path overwrites LTS exactly.
            base_input = floor_value
            base_label = "fallback_mindestverguetung"
            is_estimate = True
            estimate_basis = "mindestverguetung_floor"

        base_after_floor = max(base_input, floor_value)
        theoretical_total = base_after_floor + hkn
        effective = effective_rp_kwh(
            base_input,
            hkn,
            federal_floor_rp_kwh=floor,
            cap_rp_kwh=cap,
            cap_mode=rt.cap_mode,
        )
        if rt.cap_mode and cap is not None:
            obergrenze_aktiv = theoretical_total > cap
            hkn_gekuerzt_auf = (
                max(0.0, cap - base_after_floor)
                if obergrenze_aktiv and base_after_floor < cap
                else None
            )
        else:
            obergrenze_aktiv = False
            hkn_gekuerzt_auf = None

        tariff_source = (
            f"tariffs.json v{rt.tariffs_json_version} {rt.utility_key} "
            f"@ {rt.valid_from} ({rt.tariffs_json_source})"
        )
        return {
            "utility": rt.utility_key,
            "tariff_source": tariff_source,
            "floor_label": rt.federal_floor_label,
            "eigenverbrauch_aktiviert": eigenverbrauch,
            "hkn_aktiviert": hkn_aktiviert,
            "base_model": rt.base_model,
            "base_input_rp_kwh": round(base_input, 4),
            "base_source": base_label,
            "minimalverguetung_rp_kwh": round(floor_value, 4),
            "base_after_floor_rp_kwh": round(base_after_floor, 4),
            "hkn_verguetung_rp_kwh": round(hkn, 4),
            "theoretical_total_rp_kwh": round(theoretical_total, 4),
            "anrechenbarkeitsgrenze_rp_kwh": round(cap, 4) if cap is not None else None,
            "effective_rp_kwh": round(effective, 4),
            "effective_chf_kwh": round(effective / 100.0, 6),
            "obergrenze_aktiv": obergrenze_aktiv,
            "hkn_gekuerzt_auf": round(hkn_gekuerzt_auf, 4) if hkn_gekuerzt_auf is not None else None,
            "is_estimate": is_estimate,
            "estimate_basis": estimate_basis,
        }

    async def _auto_import_newly_published(self) -> None:
        """Detect quarters in BFE data that aren't imported yet or were imported at a different price.

        ``_reimport_quarter`` updates ``self._imported[key]`` itself with the
        full snapshot (Phase 4), so this just drives the loop and skips
        quarters that are already imported at the current price.
        """
        from .services import _reimport_quarter

        for q, price in sorted(self.quarterly.items()):
            key = str(q)
            prior = self._imported.get(key)
            if prior and prior.get("q_price_chf_mwh") == price.chf_per_mwh:
                continue
            try:
                await _reimport_quarter(self.hass, q)
            except Exception as exc:  # noqa: BLE001
                _LOGGER.warning("Auto-import skipped %s: %s", q, exc)

def _next_publication_estimate(now: datetime) -> datetime:
    """Rough estimate: 2 weeks after each quarter end. For the diagnostic sensor."""
    current_q = quarter_of(now)
    # Estimate publication ~15 days after quarter ends
    next_q = current_q.next()
    from .quarters import quarter_start_zurich

    q_end_of_current = quarter_start_zurich(next_q)
    pub = q_end_of_current + timedelta(days=15)
    return pub.astimezone(timezone.utc)
