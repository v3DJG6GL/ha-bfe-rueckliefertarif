"""BFE CSV fetcher and parser for Referenz-Marktpreise (Art. 15 EnFV).

Endpoints (stable, published by BFE under OGD programme):
- Quartalspreise: https://www.bfe-ogd.ch/ogd60_rmp_quartalspreise.csv
- Monatspreise:   https://www.bfe-ogd.ch/ogd60_rmp_monatspreise.csv

Schema (both files):
    Year, Period|Month, Days, Volume_pv_MWh, Price_pv_CHF_MWh,
    Volume_wasserkraft_MWh, Price_wasserkraft_CHF_MWh,
    Volume_windenergie_MWh, Price_windenergie_CHF_MWh,
    Volume_biomasse_MWh, Price_biomasse_CHF_MWh

Only the PV columns are consumed. All prices in CHF/MWh; convert via
tariff.chf_per_mwh_to_rp_per_kwh.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .const import BFE_MONATSPREISE_URL, BFE_QUARTALSPREISE_URL
from .quarters import Month, Quarter

if TYPE_CHECKING:
    import aiohttp


class PriceNotYetPublishedError(Exception):
    """Raised when a period exists in logic but has no BFE publication yet."""


@dataclass(frozen=True)
class BfePrice:
    chf_per_mwh: float
    days: int
    volume_mwh: float


async def fetch_csv(session: aiohttp.ClientSession, url: str) -> str:
    import aiohttp

    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.text()


def _parse_pv_price(row: dict) -> BfePrice | None:
    try:
        return BfePrice(
            chf_per_mwh=float(row["Price_pv_CHF_MWh"]),
            days=int(row["Days"]),
            volume_mwh=float(row["Volume_pv_MWh"]),
        )
    except (KeyError, ValueError):
        return None


def parse_quartalspreise(csv_text: str) -> dict[Quarter, BfePrice]:
    """Parse the quarterly CSV into {Quarter: BfePrice}. PV column only."""
    out: dict[Quarter, BfePrice] = {}
    for row in csv.DictReader(io.StringIO(csv_text)):
        year_raw = row.get("Year", "").strip()
        period_raw = (row.get("Period") or row.get("Quarter") or "").strip()
        if not year_raw or not period_raw:
            continue
        try:
            year = int(year_raw)
            q_num = int(period_raw.upper().lstrip("Q"))
        except ValueError:
            continue
        if not 1 <= q_num <= 4:
            continue
        price = _parse_pv_price(row)
        if price is not None:
            out[Quarter(year, q_num)] = price
    return out


def parse_monatspreise(csv_text: str) -> dict[Month, BfePrice]:
    """Parse the monthly CSV into {Month: BfePrice}. PV column only."""
    out: dict[Month, BfePrice] = {}
    for row in csv.DictReader(io.StringIO(csv_text)):
        year_raw = row.get("Year", "").strip()
        month_raw = row.get("Month", "").strip()
        if not year_raw or not month_raw:
            continue
        try:
            year = int(year_raw)
            month = int(month_raw)
        except ValueError:
            continue
        if not 1 <= month <= 12:
            continue
        price = _parse_pv_price(row)
        if price is not None:
            out[Month(year, month)] = price
    return out


async def fetch_quarterly(session: aiohttp.ClientSession) -> dict[Quarter, BfePrice]:
    return parse_quartalspreise(await fetch_csv(session, BFE_QUARTALSPREISE_URL))


async def fetch_monthly(session: aiohttp.ClientSession) -> dict[Month, BfePrice]:
    return parse_monatspreise(await fetch_csv(session, BFE_MONATSPREISE_URL))


def get_quarter(prices: dict[Quarter, BfePrice], q: Quarter) -> BfePrice:
    if q not in prices:
        raise PriceNotYetPublishedError(f"BFE has not yet published quarterly PV price for {q}")
    return prices[q]


def get_month(prices: dict[Month, BfePrice], m: Month) -> BfePrice:
    if m not in prices:
        raise PriceNotYetPublishedError(f"BFE has not yet published monthly PV price for {m}")
    return prices[m]
