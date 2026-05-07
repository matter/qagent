"""Abstract base class for market data providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, List

import pandas as pd


class DataProvider(ABC):
    """Interface that every concrete data provider must implement."""

    def capabilities(self) -> dict[str, Any]:
        """Return provider capability metadata.

        Concrete providers should override this with data quality semantics.
        The base fallback is intentionally conservative.
        """
        return {
            "provider": self.__class__.__name__,
            "market": None,
            "datasets": [],
            "cost": "unknown",
            "quality_level": "exploratory",
            "pit_supported": False,
            "license_scope": "unknown",
            "notes": ["Provider has not declared data quality capabilities."],
        }

    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """Return stock universe.

        Expected columns: market, ticker, name, exchange, sector, status
        """

    @abstractmethod
    def get_daily_bars(
        self, tickers: List[str], start: date, end: date
    ) -> pd.DataFrame:
        """Return daily OHLCV bars.

        Expected columns: market, date, ticker, open, high, low, close, volume, adj_factor
        """

    @abstractmethod
    def get_index_data(
        self, symbol: str, start: date, end: date
    ) -> pd.DataFrame:
        """Return daily bars for an index / ETF.

        Expected columns: market, symbol, date, open, high, low, close, volume
        """
