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
        ``"regression"``, ``"classification"``, or ``"ranking"``. Determines
        which LightGBM estimator is used.
    params : dict | None
        Override default hyper-parameters.  Keys that are not supplied
        fall back to ``_DEFAULT_PARAMS``.
    """

    def __init__(
        self,
        task: str = "regression",
        params: dict[str, Any] | None = None,
    ) -> None:
        if task not in ("regression", "classification", "ranking"):
            raise ValueError(
                f"task must be 'regression', 'classification', or 'ranking', got '{task}'"
            )
        self.task = task
        self._backend = "lightgbm"
        self._fallback_reason: str | None = None

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

        # If caller provides eval_set for early stopping and the backend is
        # actually LightGBM, wire through its native callbacks.  The sklearn
        # fallback does not support these arguments.
        if "eval_set" in kwargs and self._backend == "lightgbm":
            fit_kwargs["eval_set"] = kwargs["eval_set"]
            fit_kwargs["callbacks"] = [
                _early_stopping_callback(50),
                _log_evaluation_callback(100),
            ]

        # Pass sample_weight if provided
        if "sample_weight" in kwargs:
            fit_kwargs["sample_weight"] = kwargs["sample_weight"]
        if "group" in kwargs and self._backend == "lightgbm":
            fit_kwargs["group"] = kwargs["group"]
        if "eval_group" in kwargs and self._backend == "lightgbm":
            fit_kwargs["eval_group"] = kwargs["eval_group"]

        self._model.fit(X, y, **fit_kwargs)
        return self

    def predict(self, X: pd.DataFrame) -> pd.Series:
        preds = self._model.predict(X)
        return pd.Series(preds, index=X.index, name="prediction")

    def predict_proba(self, X: pd.DataFrame):
        """Return class probabilities (n_samples, n_classes) for classifiers."""
        if self.task != "classification":
            raise NotImplementedError("predict_proba only available for classification")
        return self._model.predict_proba(X)

    def predict_raw(self, X: pd.DataFrame) -> pd.Series:
        """Return raw leaf/margin values before sigmoid transform."""
        import numpy as np

        if self._backend == "lightgbm":
            raw = self._model.predict(X, raw_score=True)
            if isinstance(raw, np.ndarray) and raw.ndim == 2:
                raw = raw[:, 1] if raw.shape[1] > 1 else raw[:, 0]
            return pd.Series(raw, index=X.index, name="raw_score")

        if self.task == "classification" and hasattr(self._model, "decision_function"):
            raw = self._model.decision_function(X)
            if isinstance(raw, np.ndarray) and raw.ndim == 2:
                raw = raw[:, 1] if raw.shape[1] > 1 else raw[:, 0]
            return pd.Series(raw, index=X.index, name="raw_score")

        raw = self._model.predict(X)
        return pd.Series(raw, index=X.index, name="raw_score")

    @property
    def is_classifier(self) -> bool:
        return self.task == "classification"

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
        try:
            import lightgbm as lgb
        except Exception as exc:
            self._backend = "sklearn_fallback"
            self._fallback_reason = str(exc)
            return self._build_fallback_estimator(params)

        constructor_params = dict(params)
        try:
            if self.task == "classification":
                return lgb.LGBMClassifier(**constructor_params)
            if self.task == "ranking":
                constructor_params.setdefault("objective", "lambdarank")
                constructor_params.setdefault("metric", "ndcg")
                return lgb.LGBMRanker(**constructor_params)
            return lgb.LGBMRegressor(**constructor_params)
        except Exception as exc:
            self._backend = "sklearn_fallback"
            self._fallback_reason = str(exc)
            return self._build_fallback_estimator(params)

    def _build_fallback_estimator(self, params: dict):
        from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor

        fallback_params = self._map_fallback_params(params)
        if self.task == "classification":
            return GradientBoostingClassifier(**fallback_params)
        return GradientBoostingRegressor(**fallback_params)

    @staticmethod
    def _map_fallback_params(params: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "learning_rate",
            "n_estimators",
            "subsample",
            "max_depth",
            "min_samples_split",
            "min_samples_leaf",
            "max_features",
            "random_state",
        }
        mapped = {k: v for k, v in params.items() if k in allowed}
        # Keep behavior reasonably close to the lightgbm defaults.
        mapped.setdefault("random_state", 42)
        return mapped


def _early_stopping_callback(stopping_rounds: int):
    """Return a LightGBM early-stopping callback."""
    import lightgbm as lgb
    return lgb.early_stopping(stopping_rounds=stopping_rounds, verbose=False)


def _log_evaluation_callback(period: int):
    """Return a LightGBM log-evaluation callback."""
    import lightgbm as lgb
    return lgb.log_evaluation(period=period)
