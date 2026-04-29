"""Trading calendar utilities wrapping exchange_calendars."""

from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache

import exchange_calendars as xcals
import pandas as pd

from backend.logger import get_logger
from backend.services.market_context import get_default_calendar, normalize_market

log = get_logger(__name__)


@lru_cache(maxsize=8)
def _calendar(market: str | None = None) -> xcals.ExchangeCalendar:
    """Return the cached exchange calendar instance."""
    resolved_market = normalize_market(market)
    name = get_default_calendar(resolved_market)
    log.info("calendar.load", exchange=name, market=resolved_market)
    return xcals.get_calendar(name)


def is_trading_day(dt: date | str, market: str | None = None) -> bool:
    """Check whether *dt* is a valid trading session."""
    cal = _calendar(market)
    ts = _to_timestamp(dt)
    return cal.is_session(ts)


def get_trading_days(*args, market: str | None = None) -> list[date]:
    """Return a list of trading days in [start, end]."""
    resolved_market, start, end = _resolve_range_args(args, market)
    cal = _calendar(resolved_market)
    sessions = cal.sessions_in_range(
        _to_timestamp(start), _to_timestamp(end)
    )
    return [s.date() for s in sessions]


def offset_trading_days(dt: date | str, n: int, market: str | None = None) -> date:
    """Shift *dt* by *n* trading days (positive = forward, negative = back)."""
    cal = _calendar(market)
    ts = _to_timestamp(dt)
    # Ensure we start from a valid session
    if not cal.is_session(ts):
        if n >= 0:
            ts = cal.date_to_session(ts, direction="next")
        else:
            ts = cal.date_to_session(ts, direction="previous")
    result = cal.session_offset(ts, n)
    return result.date()


def get_latest_trading_day(market: str | None = None) -> date:
    """Return the most recent *completed* trading day.

    Uses local exchange time to determine "today" so that timezone
    differences don't cause fetching of future trading days.
    If today's session hasn't closed yet, returns the previous session.
    """
    resolved_market = normalize_market(market)
    cal = _calendar(resolved_market)
    now_utc = pd.Timestamp.now(tz="UTC")
    local_tz = "Asia/Shanghai" if resolved_market == "CN" else "America/New_York"
    now_local = now_utc.tz_convert(local_tz)
    today_local = now_local.normalize().tz_localize(None)
    ts = cal.date_to_session(today_local, direction="previous")

    # If resolved session is today in exchange-local time, check if market has closed
    if ts.normalize() == today_local:
        try:
            close_time = cal.session_close(ts)
            if now_utc < close_time:
                ts = cal.session_offset(ts, -1)
        except Exception:
            pass

    return ts.date()


def is_market_open(market: str | None = None) -> bool:
    """Return True if the market session is currently open."""
    cal = _calendar(market)
    now_utc = pd.Timestamp.now(tz="UTC")
    try:
        return cal.is_open_on_minute(now_utc)
    except Exception:
        return False


def snap_to_trading_day(
    dt: date | str,
    direction: str = "backward",
    market: str | None = None,
) -> date:
    """Find the nearest trading day on or before/after the given date.

    Args:
        dt: The date to snap.
        direction: 'backward' snaps to the nearest trading day on or before *dt*.
                   'forward' snaps to the nearest trading day on or after *dt*.

    Returns:
        The snapped trading day as a ``date``.
    """
    cal = _calendar(market)
    ts = _to_timestamp(dt)
    if cal.is_session(ts):
        return ts.date()
    cal_dir = "previous" if direction == "backward" else "next"
    return cal.date_to_session(ts, direction=cal_dir).date()


def _resolve_range_args(args: tuple, market: str | None) -> tuple[str, date | str, date | str]:
    if len(args) == 2:
        start, end = args
        return normalize_market(market), start, end
    if len(args) == 3:
        market_arg, start, end = args
        return normalize_market(market_arg), start, end
    raise TypeError("get_trading_days expects (start, end) or (market, start, end)")


def _to_timestamp(value: date | str) -> pd.Timestamp:
    return pd.Timestamp(value)
