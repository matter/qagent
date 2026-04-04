"""Factor base class – the protocol every factor must implement."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class FactorBase(ABC):
    """Abstract base for all factor implementations.

    Subclasses must define ``name`` and implement ``compute``.

    Attributes:
        name: Unique human-readable identifier.
        description: Short explanation of what the factor captures.
        params: Default hyper-parameters (window sizes, etc.).
        category: One of momentum / volatility / volume / trend / statistical / custom.
    """

    name: str
    description: str = ""
    params: dict = {}
    category: str = "custom"  # momentum / volatility / volume / trend / statistical / custom

    @abstractmethod
    def compute(self, data: pd.DataFrame) -> pd.Series:
        """Compute the factor for a single stock.

        Args:
            data: DataFrame with columns ``open``, ``high``, ``low``,
                  ``close``, ``volume`` and a DatetimeIndex (trading days).

        Returns:
            pd.Series with the same index containing factor values.
            NaN is acceptable for warm-up periods.
        """
        ...
