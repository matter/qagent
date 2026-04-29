"""Market data provider registry."""

from __future__ import annotations

from collections.abc import Callable

from backend.providers.base import DataProvider
from backend.providers.baostock_provider import BaoStockProvider
from backend.providers.yfinance_provider import YFinanceProvider
from backend.services.market_context import get_default_provider, normalize_market

ProviderFactory = Callable[[], DataProvider]

_PROVIDERS: dict[tuple[str, str], ProviderFactory] = {}


def register_provider(market: str, provider_name: str, factory: ProviderFactory) -> None:
    resolved_market = normalize_market(market)
    name = provider_name.strip().lower()
    if not name:
        raise ValueError("provider_name is required")
    _PROVIDERS[(resolved_market, name)] = factory


def available_providers(market: str | None = None) -> list[str]:
    if market is None:
        return sorted({name for _, name in _PROVIDERS})
    resolved_market = normalize_market(market)
    return sorted(name for (registered_market, name) in _PROVIDERS if registered_market == resolved_market)


def get_provider(market: str | None = None, provider_name: str | None = None) -> DataProvider:
    resolved_market = normalize_market(market)
    name = (provider_name or get_default_provider(resolved_market)).lower()
    factory = _PROVIDERS.get((resolved_market, name))
    if factory is None:
        available = ", ".join(available_providers(resolved_market)) or "none"
        raise ValueError(
            f"Unknown data provider '{name}' for market '{resolved_market}'. "
            f"Available providers: {available}"
        )
    return factory()


register_provider("US", "yfinance", YFinanceProvider)
register_provider("CN", "baostock", BaoStockProvider)
