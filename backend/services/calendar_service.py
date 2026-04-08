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
    """Return the most recent *completed* trading day (<= today).

    If today is a trading session but the market hasn't closed yet,
    returns the previous session to prevent fetching incomplete data.
    """
    cal = _calendar()
    today = pd.Timestamp(date.today())
    ts = cal.date_to_session(today, direction="previous")

    # If resolved session is today, check if today's session has finished
    if ts.date() == date.today():
        now_utc = pd.Timestamp.now(tz="UTC")
        try:
            close_time = cal.session_close(ts)
            if now_utc < close_time:
                # Market hasn't closed yet – use previous session
                ts = cal.session_offset(ts, -1)
        except Exception:
            pass

    return ts.date()


def is_market_open() -> bool:
    """Return True if the US market session is currently open."""
    cal = _calendar()
    now_utc = pd.Timestamp.now(tz="UTC")
    try:
        return cal.is_open_on_minute(now_utc)
    except Exception:
        return False


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
