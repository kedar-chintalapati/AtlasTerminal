"""Time/calendar utilities."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def to_date(v: str | date | datetime) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def date_range(start: date, end: date, step_days: int = 1) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=step_days)
    return days


def week_of_year(d: date) -> int:
    return d.isocalendar()[1]


def day_of_year(d: date) -> int:
    return d.timetuple().tm_yday


def season(d: date) -> str:
    """Meteorological season for northern hemisphere."""
    m = d.month
    if m in (12, 1, 2):
        return "winter"
    if m in (3, 4, 5):
        return "spring"
    if m in (6, 7, 8):
        return "summer"
    return "fall"


def trading_days_between(start: date, end: date) -> int:
    """Approximate number of US trading days between two dates."""
    days = 0
    current = start
    while current <= end:
        if current.weekday() < 5:   # Mon–Fri
            days += 1
        current += timedelta(days=1)
    return days


def window_labels(
    reference: date,
    windows: list[int] = (1, 5, 10, 21),
) -> dict[str, date]:
    """Return {label: date} for forward-window endpoints."""
    return {f"+{w}d": reference + timedelta(days=w) for w in windows}
