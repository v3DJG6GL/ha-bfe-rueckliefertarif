"""Europe/Zurich quarter/month arithmetic, DST-aware."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from .const import TIMEZONE

ZURICH = ZoneInfo(TIMEZONE)
_QUARTER_RE = re.compile(r"^(\d{4})[Qq]([1-4])$")
_MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


@dataclass(frozen=True, order=True)
class Quarter:
    year: int
    q: int  # 1..4

    @classmethod
    def parse(cls, s: str) -> Quarter:
        m = _QUARTER_RE.match(s.strip())
        if not m:
            raise ValueError(f"Invalid quarter identifier: {s!r} (expected e.g. '2026Q1')")
        return cls(int(m.group(1)), int(m.group(2)))

    def __str__(self) -> str:
        return f"{self.year}Q{self.q}"

    def start_month(self) -> int:
        return 1 + 3 * (self.q - 1)

    def months(self) -> tuple[Month, Month, Month]:
        sm = self.start_month()
        return (Month(self.year, sm), Month(self.year, sm + 1), Month(self.year, sm + 2))

    def next(self) -> Quarter:
        if self.q == 4:
            return Quarter(self.year + 1, 1)
        return Quarter(self.year, self.q + 1)

    def prev(self) -> Quarter:
        if self.q == 1:
            return Quarter(self.year - 1, 4)
        return Quarter(self.year, self.q - 1)


@dataclass(frozen=True, order=True)
class Month:
    year: int
    month: int  # 1..12

    @classmethod
    def parse(cls, s: str) -> Month:
        m = _MONTH_RE.match(s.strip())
        if not m:
            raise ValueError(f"Invalid month identifier: {s!r} (expected e.g. '2026-01')")
        return cls(int(m.group(1)), int(m.group(2)))

    def __str__(self) -> str:
        return f"{self.year}-{self.month:02d}"

    def quarter(self) -> Quarter:
        return Quarter(self.year, (self.month - 1) // 3 + 1)


def month_start_zurich(m: Month) -> datetime:
    """Returns 00:00 local time on the first day of the month."""
    return datetime(m.year, m.month, 1, tzinfo=ZURICH)


def month_end_zurich(m: Month) -> datetime:
    """Returns 00:00 local time on the first day of the following month."""
    if m.month == 12:
        return datetime(m.year + 1, 1, 1, tzinfo=ZURICH)
    return datetime(m.year, m.month + 1, 1, tzinfo=ZURICH)


def quarter_start_zurich(q: Quarter) -> datetime:
    return month_start_zurich(Month(q.year, q.start_month()))


def quarter_end_zurich(q: Quarter) -> datetime:
    return quarter_start_zurich(q.next())


def quarter_bounds_utc(q: Quarter) -> tuple[datetime, datetime]:
    """Start and end (exclusive) of a quarter in UTC, DST-safe."""
    return quarter_start_zurich(q).astimezone(UTC), quarter_end_zurich(q).astimezone(UTC)


def month_bounds_utc(m: Month) -> tuple[datetime, datetime]:
    return month_start_zurich(m).astimezone(UTC), month_end_zurich(m).astimezone(UTC)


def quarter_of(dt: datetime) -> Quarter:
    """Returns the Quarter containing dt (interpreted in Zurich local time)."""
    local = dt.astimezone(ZURICH) if dt.tzinfo else dt.replace(tzinfo=ZURICH)
    return Month(local.year, local.month).quarter()


def hours_in_range(start: datetime, end: datetime) -> list[datetime]:
    """Aware UTC hours in [start, end), advancing 1h at a time.

    Output is always in UTC (HA LTS timestamps are stored as unix epochs — no DST ambiguity).
    """
    start_utc = start.astimezone(UTC)
    end_utc = end.astimezone(UTC)
    out: list[datetime] = []
    h = start_utc
    while h < end_utc:
        out.append(h)
        h += timedelta(hours=1)
    return out
