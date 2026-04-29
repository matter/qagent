"""Abstract base class for all ML models."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class ModelBase(ABC):
    """Base interface that every model implementation must satisfy."""

    @abstractmethod
    def fit(self, X: pd.DataFrame, y: pd.Series, **kwargs) -> "ModelBase":
        """Fit the model on training data.

        Args:
            X: Feature matrix (rows = samples, columns = factor names).
            y: Target series aligned with X.
            **kwargs: Optional estimator-specific fit arguments such as
                sample weights, validation sets, or ranking query groups.

        Returns:
            self, for method chaining.
        """
        ...

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Generate predictions for new data.

        Args:
            X: Feature matrix with the same columns used during fit.

        Returns:
            Series of predictions, index aligned with X.
        """
        ...

    @abstractmethod
    def get_params(self) -> dict:
        """Return the model hyper-parameters as a JSON-serialisable dict."""
        ...

    def feature_importance(self) -> pd.Series:
        """Return feature importance as a sorted Series (descending).

        Not all model types support this; the default raises NotImplementedError.
        """
        raise NotImplementedError("feature_importance is not implemented for this model")
