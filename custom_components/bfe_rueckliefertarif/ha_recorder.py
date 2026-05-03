"""Home Assistant recorder I/O — thin wrapper around the official statistics API.

Keeps the HA-dependent code in one module so the rest of the integration stays
unit-testable without HA installed.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    statistics_during_period,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant


async def read_hourly_export(
    hass: HomeAssistant, statistic_id: str, start: datetime, end: datetime
) -> dict[datetime, float]:
    """Read hourly export kWh from LTS. Derives per-hour from differencing `sum`.

    Reads one hour earlier than `start` to anchor the first hour's delta. Returns
    {hour_start_utc: kwh_in_that_hour}. Missing hours are omitted (caller treats as 0).
    """
    padded_start = start - timedelta(hours=1)
    data = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        padded_start,
        end,
        {statistic_id},
        "hour",
        None,
        {"sum"},
    )
    rows = data.get(statistic_id, [])
    sums_by_hour: dict[datetime, float] = {}
    for row in rows:
        s = row.get("start")
        total = row.get("sum")
        if s is None or total is None:
            continue
        ts = _to_datetime(s)
        sums_by_hour[ts] = float(total)
    # Difference consecutive sums to get per-hour kWh
    ordered = sorted(sums_by_hour)
    out: dict[datetime, float] = {}
    for i in range(1, len(ordered)):
        prev, curr = ordered[i - 1], ordered[i]
        if curr < start:
            continue
        kwh = sums_by_hour[curr] - sums_by_hour[prev]
        if kwh < 0:
            # total_increasing sensor reset; fall back to the current delta as 0
            kwh = 0.0
        out[prev] = kwh  # kWh observed during prev..curr window is attributed to the earlier hour
    return out


async def read_compensation_anchor(
    hass: HomeAssistant, statistic_id: str, at: datetime
) -> float:
    """Read the compensation LTS `sum` at a specific hour start. Returns 0 if missing."""
    data = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        at,
        at + timedelta(hours=1),
        {statistic_id},
        "hour",
        None,
        {"sum"},
    )
    rows = data.get(statistic_id, [])
    if not rows:
        return 0.0
    total = rows[0].get("sum")
    return float(total) if total is not None else 0.0


async def read_post_quarter_sums(
    hass: HomeAssistant, statistic_id: str, start: datetime, end: datetime
) -> list[tuple[datetime, float]]:
    """Read all compensation LTS `sum` values in [start, end). For transition-spike shift."""
    data = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        start,
        end,
        {statistic_id},
        "hour",
        None,
        {"sum"},
    )
    rows = data.get(statistic_id, [])
    out: list[tuple[datetime, float]] = []
    for row in rows:
        s = row.get("start")
        total = row.get("sum")
        if s is None or total is None:
            continue
        out.append((_to_datetime(s), float(total)))
    return sorted(out, key=lambda x: x[0])


def build_compensation_stats(
    records: list[tuple[datetime, float]],
) -> list[dict[str, Any]]:
    """Convert (hour_start_utc, running_sum_chf) pairs to HA LTS stat dicts."""
    return [{"start": s, "sum": total} for s, total in records]


def build_metadata_compensation(statistic_id: str, name: str | None = None) -> dict[str, Any]:
    """Metadata for compensation LTS — sum type, CHF, no unit_class."""
    return {
        "source": "recorder",
        "statistic_id": statistic_id,
        "unit_of_measurement": "CHF",
        "has_mean": False,
        "has_sum": True,
        "mean_type": 0,
        "unit_class": None,
        "name": name,
    }


def build_metadata_basis(statistic_id: str, name: str | None = None) -> dict[str, Any]:
    """Metadata for tariff basis LTS — mean type, Rp/kWh, no unit_class."""
    return {
        "source": "recorder",
        "statistic_id": statistic_id,
        "unit_of_measurement": "Rp/kWh",
        "has_mean": True,
        "has_sum": False,
        "mean_type": 1,
        "unit_class": None,
        "name": name,
    }


async def import_statistics(
    hass: HomeAssistant,
    metadata: dict[str, Any],
    stats: list[dict[str, Any]],
) -> None:
    """Dispatch to HA's async_import_statistics and wait for the recorder to drain.

    ``async_import_statistics`` is a ``@callback`` that *queues* the write onto
    the recorder's executor thread; it does not block. Without the
    ``async_block_till_done`` below, a follow-up call that reads the LTS chain
    (e.g. ``read_compensation_anchor`` for the next quarter in
    ``_reimport_all_history``) can race the queue and observe ``sum=0``,
    leading to every quarter being written from anchor 0 — and the Energy
    Dashboard then displays per-period deltas as ``current - previous``
    instead of the actual quarter total.
    """
    async_import_statistics(hass, metadata, stats)
    await get_instance(hass).async_block_till_done()


def _to_datetime(value: Any) -> datetime:
    """Recorder returns `start` as either datetime or unix timestamp (HA version-dependent)."""
    if isinstance(value, datetime):
        return value
    from datetime import UTC
    return datetime.fromtimestamp(float(value), tz=UTC)
