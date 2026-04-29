"""Market scope helpers shared by services, APIs, and MCP tools."""

from __future__ import annotations

import re
from typing import Literal

from backend.config import MarketEntryConfig, Settings, settings

Market = Literal["US", "CN"]

_ALIASES = {
    "US": "US",
    "USA": "US",
    "CN": "CN",
    "CHINA": "CN",
    "A": "CN",
    "A_SHARE": "CN",
    "ASHARE": "CN",
}


def normalize_market(value: str | None) -> Market:
    """Normalize user/API market input.

    Missing values default to US for backward compatibility with the
    pre-V2 API and local assets.
    """
    if value is None or str(value).strip() == "":
        return "US"

    key = str(value).strip().upper().replace("-", "_")
    market = _ALIASES.get(key)
    if market is None:
        raise ValueError(f"Unsupported market '{value}'. Expected one of: US, CN")
    return market  # type: ignore[return-value]


def normalize_ticker(ticker: str, market: str | None = None) -> str:
    """Normalize ticker formatting for storage in one market.

    US symbols are stored uppercase for yfinance compatibility. CN symbols are
    stored in BaoStock-native lowercase form, for example ``sh.600000``.
    """
    resolved = normalize_market(market)
    value = str(ticker).strip()
    if resolved == "CN":
        return value.lower()
    return value.upper()


_CN_TICKER_RE = re.compile(r"^(sh|sz|bj)\.\d{6}$", re.IGNORECASE)


def infer_ticker_market(ticker: str) -> Market | None:
    """Infer a market from ticker syntax when it is unambiguous."""
    value = str(ticker).strip()
    if _CN_TICKER_RE.match(value):
        return "CN"
    if value and "." not in value and value.replace("-", "").isalnum():
        return "US"
    return None


def get_market_config(
    market: str | None = None,
    app_settings: Settings | None = None,
) -> MarketEntryConfig:
    """Return the configured provider/calendar/benchmark defaults for a market."""
    resolved = normalize_market(market)
    cfg = (app_settings or settings).markets.get(resolved)
    if cfg is None:
        raise ValueError(f"Market '{resolved}' is not configured")
    return cfg


def get_default_provider(market: str | None = None, app_settings: Settings | None = None) -> str:
    return get_market_config(market, app_settings).provider


def get_default_calendar(market: str | None = None, app_settings: Settings | None = None) -> str:
    return get_market_config(market, app_settings).calendar


def get_default_benchmark(market: str | None = None, app_settings: Settings | None = None) -> str:
    return get_market_config(market, app_settings).benchmark


def get_default_group(market: str | None = None, app_settings: Settings | None = None) -> str:
    return get_market_config(market, app_settings).default_group
