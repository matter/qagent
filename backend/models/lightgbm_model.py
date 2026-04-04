"""LightGBM wrapper implementing the ModelBase interface."""

from __future__ import annotations

from typing import Any

import pandas as pd

from backend.models.base import ModelBase


# Default hyper-parameters shared by regressor and classifier.
_DEFAULT_PARAMS: dict[str, Any] = {
    "n_estimators": 500,
    "learning_rate": 0.05,
    "max_depth": 6,
    "num_leaves": 63,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_samples": 20,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "random_state": 42,
    "verbosity": -1,
    "n_jobs": -1,
}


class LightGBMModel(ModelBase):
    """Thin wrapper around LGBMRegressor / LGBMClassifier.

    Parameters
    ----------
    task : str
        ``"regression"`` or ``"classification"``.  Determines which
        LightGBM estimator is used.
    params : dict | None
        Override default hyper-parameters.  Keys that are not supplied
        fall back to ``_DEFAULT_PARAMS``.
    """

    def __init__(
        self,
        task: str = "regression",
        params: dict[str, Any] | None = None,
    ) -> None:
        if task not in ("regression", "classification"):
            raise ValueError(f"task must be 'regression' or 'classification', got '{task}'")
        self.task = task

        # Merge user params on top of defaults
        merged = dict(_DEFAULT_PARAMS)
        if params:
            merged.update(params)
        self._params = merged
        self._model = self._build_estimator(merged)
        self._feature_names: list[str] = []

    # ------------------------------------------------------------------
    # ModelBase interface
    # ------------------------------------------------------------------

    def fit(self, X: pd.DataFrame, y: pd.Series, **kwargs) -> "LightGBMModel":
        self._feature_names = list(X.columns)

        fit_kwargs: dict[str, Any] = {}

        # If caller provides eval_set for early stopping
        if "eval_set" in kwargs:
            fit_kwargs["eval_set"] = kwargs["eval_set"]
            fit_kwargs["callbacks"] = [
                _early_stopping_callback(50),
                _log_evaluation_callback(100),
            ]

        self._model.fit(X, y, **fit_kwargs)
        return self

    def predict(self, X: pd.DataFrame) -> pd.Series:
        preds = self._model.predict(X)
        return pd.Series(preds, index=X.index, name="prediction")

    def get_params(self) -> dict:
        return {
            "task": self.task,
            **self._params,
        }

    def feature_importance(self) -> pd.Series:
        importance = self._model.feature_importances_
        return (
            pd.Series(importance, index=self._feature_names, name="importance")
            .sort_values(ascending=False)
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_estimator(self, params: dict):
        import lightgbm as lgb

        constructor_params = dict(params)
        if self.task == "classification":
            return lgb.LGBMClassifier(**constructor_params)
        return lgb.LGBMRegressor(**constructor_params)


def _early_stopping_callback(stopping_rounds: int):
    """Return a LightGBM early-stopping callback."""
    import lightgbm as lgb
    return lgb.early_stopping(stopping_rounds=stopping_rounds, verbose=False)


def _log_evaluation_callback(period: int):
    """Return a LightGBM log-evaluation callback."""
    import lightgbm as lgb
    return lgb.log_evaluation(period=period)
