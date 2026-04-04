"""Abstract base class for market data providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List

import pandas as pd


class DataProvider(ABC):
    """Interface that every concrete data provider must implement."""

    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """Return stock universe.

        Expected columns: ticker, name, exchange, sector, status
        """

    @abstractmethod
    def get_daily_bars(
        self, tickers: List[str], start: date, end: date
    ) -> pd.DataFrame:
        """Return daily OHLCV bars.

        Expected columns: date, ticker, open, high, low, close, volume, adj_factor
        """

    @abstractmethod
    def get_index_data(
        self, symbol: str, start: date, end: date
    ) -> pd.DataFrame:
        """Return daily bars for an index / ETF.

        Expected columns: date, open, high, low, close, volume
        """
