"""Market data provider registry."""

from __future__ import annotations

from backend.providers.base import DataProvider
from backend.providers.baostock_provider import BaoStockProvider
from backend.providers.yfinance_provider import YFinanceProvider
from backend.services.market_context import get_default_provider, normalize_market


def get_provider(market: str | None = None, provider_name: str | None = None) -> DataProvider:
    resolved_market = normalize_market(market)
    name = (provider_name or get_default_provider(resolved_market)).lower()

    if resolved_market == "US" and name == "yfinance":
        return YFinanceProvider()
    if resolved_market == "CN" and name == "baostock":
        return BaoStockProvider()

    raise ValueError(f"Unknown data provider '{name}' for market '{resolved_market}'")

