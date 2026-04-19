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
        current_weights: Current portfolio weights per ticker (from last rebalance).
        holding_days: Number of consecutive trading days each ticker has been held.
        avg_entry_price: Volume-weighted average entry price per ticker.
        unrealized_pnl: Unrealised P&L fraction per ticker (price / entry - 1).
        diagnostics: Optional dict strategies can populate to expose per-day
            internal state (e.g. active_host, candidate_pool, gate results).
            Captured by the backtest engine into rebalance_diagnostics.
    """

    prices: pd.DataFrame
    factor_values: dict[str, pd.DataFrame] = field(default_factory=dict)
    model_predictions: dict[str, pd.Series] = field(default_factory=dict)
    current_date: object = None
    current_weights: dict[str, float] = field(default_factory=dict)
    holding_days: dict[str, int] = field(default_factory=dict)
    avg_entry_price: dict[str, float] = field(default_factory=dict)
    unrealized_pnl: dict[str, float] = field(default_factory=dict)
    diagnostics: dict = field(default_factory=dict)


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
