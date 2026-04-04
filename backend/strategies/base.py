"""Strategy base class -- the protocol every strategy must implement."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class StrategyContext:
    """Data bundle passed to a strategy on each evaluation date.

    Attributes:
        prices: OHLCV DataFrame with MultiIndex columns (field, ticker).
        factor_values: Mapping of factor_name -> DataFrame(dates x tickers).
        model_predictions: Mapping of model_id -> Series(ticker -> prediction).
        current_date: The date being evaluated.
    """

    prices: pd.DataFrame
    factor_values: dict[str, pd.DataFrame] = field(default_factory=dict)
    model_predictions: dict[str, pd.Series] = field(default_factory=dict)
    current_date: object = None


class StrategyBase(ABC):
    """Abstract base for all strategy implementations.

    Subclasses must define ``name`` and implement ``generate_signals``.

    Attributes:
        name: Unique human-readable identifier.
        description: Short explanation of the strategy logic.
    """

    name: str
    description: str = ""

    @abstractmethod
    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        """Generate trading signals for the current evaluation date.

        Args:
            context: A :class:`StrategyContext` with prices, factors, and
                model predictions available up to *current_date*.

        Returns:
            DataFrame with index=ticker, columns=[signal, weight, strength].

            * **signal**: 1 = buy, -1 = sell, 0 = hold.
            * **weight**: Target weight in 0--1 (before position sizing).
            * **strength**: Signal strength used for ranking / sizing.
        """
        ...

    def required_factors(self) -> list[str]:
        """Return list of factor names this strategy depends on."""
        return []

    def required_models(self) -> list[str]:
        """Return list of model IDs this strategy depends on."""
        return []
