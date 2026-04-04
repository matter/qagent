"""Trading calendar utilities wrapping exchange_calendars."""

from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache

import exchange_calendars as xcals
import pandas as pd

from backend.config import settings
from backend.logger import get_logger

log = get_logger(__name__)


@lru_cache(maxsize=1)
def _calendar() -> xcals.ExchangeCalendar:
    """Return the cached exchange calendar instance."""
    name = settings.market.calendar  # default: "NYSE"
    log.info("calendar.load", exchange=name)
    return xcals.get_calendar(name)


def is_trading_day(dt: date) -> bool:
    """Check whether *dt* is a valid trading session."""
    cal = _calendar()
    ts = pd.Timestamp(dt)
    return cal.is_session(ts)


def get_trading_days(start: date, end: date) -> list[date]:
    """Return a list of trading days in [start, end]."""
    cal = _calendar()
    sessions = cal.sessions_in_range(
        pd.Timestamp(start), pd.Timestamp(end)
    )
    return [s.date() for s in sessions]


def offset_trading_days(dt: date, n: int) -> date:
    """Shift *dt* by *n* trading days (positive = forward, negative = back)."""
    cal = _calendar()
    ts = pd.Timestamp(dt)
    # Ensure we start from a valid session
    if not cal.is_session(ts):
        if n >= 0:
            ts = cal.date_to_session(ts, direction="next")
        else:
            ts = cal.date_to_session(ts, direction="previous")
    result = cal.session_offset(ts, n)
    return result.date()


def get_latest_trading_day() -> date:
    """Return the most recent completed trading day (<= today)."""
    cal = _calendar()
    today = pd.Timestamp(date.today())
    ts = cal.date_to_session(today, direction="previous")
    return ts.date()


def snap_to_trading_day(dt: date, direction: str = "backward") -> date:
    """Find the nearest trading day on or before/after the given date.

    Args:
        dt: The date to snap.
        direction: 'backward' snaps to the nearest trading day on or before *dt*.
                   'forward' snaps to the nearest trading day on or after *dt*.

    Returns:
        The snapped trading day as a ``date``.
    """
    cal = _calendar()
    ts = pd.Timestamp(dt)
    if cal.is_session(ts):
        return dt
    cal_dir = "previous" if direction == "backward" else "next"
    return cal.date_to_session(ts, direction=cal_dir).date()
