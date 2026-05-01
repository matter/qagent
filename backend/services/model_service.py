"""Model training, persistence, and prediction service."""

from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from backend.config import settings
from backend.db import get_connection
from backend.logger import get_logger
from backend.models.base import ModelBase
from backend.models.lightgbm_model import LightGBMModel
from backend.services.calendar_service import snap_to_trading_day
from backend.services.feature_service import FeatureService
from backend.services.group_service import GroupService
from backend.services.label_service import LabelService
from backend.services.market_context import normalize_market, normalize_ticker
from backend.services.ranking_dataset import build_date_groups, compute_ranking_metrics
from backend.time_utils import utc_now_iso, utc_now_naive

log = get_logger(__name__)


def _infer_task_from_model(model_instance) -> str:
    """Infer task type from model instance.

    Returns 'classification' or 'regression'.
    """
    if hasattr(model_instance, "is_classifier") and model_instance.is_classifier:
        return "classification"

    class_name = model_instance.__class__.__name__
    if "Classifier" in class_name:
        return "classification"
    if "Regressor" in class_name:
        return "regression"

    if hasattr(model_instance, "task") and model_instance.task == "classification":
        return "classification"

    return "regression"


# Registry of supported model types
_MODEL_REGISTRY: dict[str, type] = {
    "lightgbm": LightGBMModel,
}

# Label target types that should be treated as classification
_CLASSIFICATION_TARGETS = {"binary", "top_quantile", "bottom_quantile", "large_move", "excess_binary", "path_quality", "triple_barrier"}
_RANKING_OBJECTIVES = {"ranking", "pairwise", "listwise"}

# Parameters determined by the system, not user-configurable
_RESERVED_MODEL_PARAMS = {"task", "objective", "metric", "verbosity", "n_jobs"}


class ModelService:
    """Train, persist, load and run inference with ML models."""

    def __init__(self) -> None:
        self._feature_service = FeatureService()
        self._label_service = LabelService()
        self._group_service = GroupService()

        # Instance-level caches – models are immutable after training,
        # so these are safe to keep for the lifetime of the service.
        self._model_cache: dict[str, ModelBase] = {}
        self._record_cache: dict[tuple[str, str], dict] = {}
        self._frozen_cache: dict[str, list[str] | None] = {}

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train_model(
        self,
        name: str,
        feature_set_id: str,
        label_id: str,
        model_type: str = "lightgbm",
        model_params: dict[str, Any] | None = None,
        train_config: dict[str, Any] | None = None,
        universe_group_id: str | None = None,
        sample_weight_config: dict[str, Any] | None = None,
        market: str | None = None,
        objective_type: str | None = None,
        ranking_config: dict[str, Any] | None = None,
    ) -> dict:
        """End-to-end model training pipeline.

        Steps
        -----
        1.  Resolve universe tickers from group.
        2.  Compute features via FeatureService.
        3.  Compute labels via LabelService.
        4.  Build aligned X / y DataFrames.
        5.  Split by date ranges (train / valid / test).
        6.  Fit model on train set.
        7.  Predict on valid and test sets.
        8.  Calculate evaluation metrics.
        9.  Save model file (joblib) + metadata.json.
        10. Save record to DuckDB models table.
        11. Return summary dict.
        """
        if model_type not in _MODEL_REGISTRY:
            raise ValueError(
                f"Unsupported model_type '{model_type}'. "
                f"Choose from {list(_MODEL_REGISTRY.keys())}"
            )

        train_config = train_config or {}
        model_params = model_params or {}
        resolved_market = normalize_market(market)
        objective = (objective_type or "").strip().lower() or None
        if objective and objective not in {"regression", "classification", *_RANKING_OBJECTIVES}:
            raise ValueError(
                "objective_type must be one of: regression, classification, ranking, pairwise, listwise"
            )
        ranking_config = ranking_config or {}

        # ---- Strip reserved params that are set by the system ----
        stripped = [k for k in model_params if k in _RESERVED_MODEL_PARAMS]
        if stripped:
            log.warning("model.train.reserved_params_stripped", params=stripped)
            model_params = {k: v for k, v in model_params.items() if k not in _RESERVED_MODEL_PARAMS}

        # ---- Normalize train_config from frontend nested format ----
        # Frontend sends: { train_period: {start, end}, valid_period: ..., test_period: ... }
        # Backend expects: { train_start, train_end, valid_start, valid_end, test_start, test_end }
        train_config = self._normalize_train_config(train_config, market=resolved_market)

        # ---- 1. Resolve tickers ----
        if universe_group_id:
            tickers = self._group_service.get_group_tickers(universe_group_id, market=resolved_market)
            if not tickers:
                raise ValueError(f"Universe group '{universe_group_id}' has no members")
        else:
            raise ValueError("universe_group_id is required")

        # ---- Determine overall date range ----
        train_start = train_config.get("train_start")
        test_end = train_config.get("test_end")
        if not train_start or not test_end:
            raise ValueError("train_config must include train_start and test_end")

        overall_start = train_start
        overall_end = test_end

        log.info(
            "model.train.start",
            name=name,
            market=resolved_market,
            model_type=model_type,
            tickers=len(tickers),
            date_range=f"{overall_start} ~ {overall_end}",
        )

        # ---- 2. Compute features ----
        feature_data = self._feature_service.compute_features(
            feature_set_id, tickers, overall_start, overall_end, market=resolved_market
        )
        # feature_data: dict[factor_name -> DataFrame(dates x tickers)]

        # ---- 3. Compute labels ----
        label_def = self._label_service.get_label(label_id, market=resolved_market)
        label_df = self._label_service.compute_label_values(
            label_id, tickers, overall_start, overall_end, market=resolved_market
        )
        # label_df: DataFrame with columns [ticker, date, label_value]

        if label_df.empty:
            raise ValueError("No label data computed for the given parameters")

        label_summary = {
            "label_id": label_id,
            "target_type": label_def["target_type"],
            "horizon": label_def.get("horizon"),
            "config": label_def.get("config"),
            "samples": len(label_df),
            "mean": round(float(label_df["label_value"].mean()), 6),
            "std": round(float(label_df["label_value"].std()), 6),
            "min": round(float(label_df["label_value"].min()), 6),
            "max": round(float(label_df["label_value"].max()), 6),
        }

        # ---- 4. Build X and y aligned by (date, ticker) ----
        X, y = self._build_Xy(feature_data, label_df)
        if X.empty:
            raise ValueError("No aligned (date, ticker) pairs after joining features and labels")

        log.info("model.train.data_built", X_shape=X.shape, y_size=len(y))

        # ---- 5. Split by date ranges ----
        purge_gap = int(train_config.get("purge_gap", 5))

        # Preflight: validate date ordering and coverage
        self._validate_split_config(train_config, X, y, purge_gap)

        splits = self._split_by_dates(X, y, train_config, purge_gap)

        X_train, y_train = splits["train"]
        X_valid, y_valid = splits["valid"]
        X_test, y_test = splits["test"]

        log.info(
            "model.train.splits",
            train=len(X_train),
            valid=len(X_valid),
            test=len(X_test),
        )

        if len(X_train) == 0:
            raise ValueError("Training set is empty after date split")

        # ---- 6. Determine task type and fit ----
        if objective in _RANKING_OBJECTIVES:
            task = "ranking"
        elif objective in {"regression", "classification"}:
            task = objective
        else:
            task = "classification" if label_def["target_type"] in _CLASSIFICATION_TARGETS else "regression"

        model_cls = _MODEL_REGISTRY[model_type]
        model_instance: ModelBase = model_cls(task=task, params=model_params)

        fit_kwargs: dict[str, Any] = {}
        fit_X_train, fit_y_train = X_train, y_train
        fit_X_valid, fit_y_valid = X_valid, y_valid
        fit_X_test, fit_y_test = X_test, y_test
        metric_y_train = y_train
        metric_y_valid, metric_y_test = y_valid, y_test

        if task == "ranking":
            if ranking_config.get("query_group", "date") != "date":
                raise ValueError("ranking_config.query_group must be 'date' in V2.0")
            min_group_size = int(ranking_config.get("min_group_size", 5))
            label_gain = str(ranking_config.get("label_gain", "ordinal"))
            train_rank = build_date_groups(
                X_train,
                y_train,
                min_group_size=min_group_size,
                label_gain=label_gain,
            )
            valid_rank = build_date_groups(
                X_valid,
                y_valid,
                min_group_size=min_group_size,
                label_gain=label_gain,
            )
            test_rank = build_date_groups(
                X_test,
                y_test,
                min_group_size=min_group_size,
                label_gain=label_gain,
            )
            if train_rank.X.empty:
                raise ValueError(
                    f"Ranking training set is empty after date grouping; "
                    f"min_group_size={min_group_size}"
                )
            fit_X_train, fit_y_train = train_rank.X, train_rank.y
            fit_X_valid, fit_y_valid = valid_rank.X, valid_rank.y
            fit_X_test, fit_y_test = test_rank.X, test_rank.y
            metric_y_train = train_rank.raw_y
            metric_y_valid, metric_y_test = valid_rank.raw_y, test_rank.raw_y
            fit_kwargs["group"] = train_rank.group_sizes
            if not fit_X_valid.empty:
                fit_kwargs["eval_set"] = [(fit_X_valid, fit_y_valid)]
                fit_kwargs["eval_group"] = [valid_rank.group_sizes]
            ranking_summary = {
                "query_group": "date",
                "min_group_size": min_group_size,
                "label_gain": label_gain,
                "train_groups": len(train_rank.group_sizes),
                "valid_groups": len(valid_rank.group_sizes),
                "test_groups": len(test_rank.group_sizes),
                "dropped_train_groups": train_rank.dropped_groups,
                "dropped_valid_groups": valid_rank.dropped_groups,
                "dropped_test_groups": test_rank.dropped_groups,
            }
        else:
            ranking_summary = None
            if len(X_valid) > 0:
                fit_kwargs["eval_set"] = [(X_valid, y_valid)]

        # ---- 6a. Build sample weights if configured ----
        if sample_weight_config:
            weights = self._build_sample_weights(fit_X_train, metric_y_train, sample_weight_config, feature_data)
            if weights is not None:
                fit_kwargs["sample_weight"] = weights
                log.info("model.train.sample_weights",
                         weight_mean=round(float(weights.mean()), 4),
                         weight_std=round(float(weights.std()), 4),
                         weight_min=round(float(weights.min()), 4),
                         weight_max=round(float(weights.max()), 4))

        model_instance.fit(fit_X_train, fit_y_train, **fit_kwargs)

        # ---- 7. Predict on valid and test sets ----
        preds_valid = model_instance.predict(fit_X_valid) if len(fit_X_valid) > 0 else pd.Series(dtype=float)
        preds_test = model_instance.predict(fit_X_test) if len(fit_X_test) > 0 else pd.Series(dtype=float)

        # ---- 8. Calculate eval metrics ----
        if task == "ranking":
            eval_at = ranking_config.get("eval_at", [5, 10, 20])
            eval_metrics = {
                "train_samples": len(fit_y_train),
                "valid_samples": len(fit_y_valid),
                "test_samples": len(fit_y_test),
            }
            valid_metrics = compute_ranking_metrics(metric_y_valid, preds_valid, eval_at=eval_at)
            test_metrics = compute_ranking_metrics(metric_y_test, preds_test, eval_at=eval_at)
            eval_metrics.update({f"valid_{k}": v for k, v in valid_metrics.items()})
            eval_metrics.update({f"test_{k}": v for k, v in test_metrics.items()})
            if ranking_summary is not None:
                eval_metrics["ranking_groups"] = ranking_summary
            eval_metrics["objective_type"] = objective or "ranking"
            if objective == "pairwise":
                eval_metrics["pairwise_mode"] = "lambdarank"
        else:
            eval_metrics = self._compute_eval_metrics(
                task, y_train, y_valid, y_test, preds_valid, preds_test
            )
        eval_metrics["task_type"] = task

        # Feature importance
        try:
            fi = model_instance.feature_importance()
            eval_metrics["feature_importance"] = {
                k: round(float(v), 6) for k, v in fi.head(30).items()
            }
        except NotImplementedError:
            pass

        log.info("model.train.metrics", metrics=eval_metrics)

        # ---- 9. Validate feature dimensions ----
        trained_features = list(fit_X_train.columns)
        trained_feature_count = len(trained_features)

        # Get expected features from feature set
        fs_record = self._feature_service.get_feature_set(feature_set_id, market=resolved_market)
        feature_lineage = self._build_feature_lineage(
            trained_features,
            fs_record.get("factor_refs", []),
        )
        undeclared_features = feature_lineage["undeclared"]
        missing_features = feature_lineage["missing"]

        if undeclared_features:
            log.warning(
                "model.train.feature_mismatch",
                trained=trained_feature_count,
                declared=len(feature_lineage["declared"]),
                undeclared=undeclared_features,
            )
            raise ValueError(
                f"Feature lineage mismatch: model trained on undeclared features "
                f"{undeclared_features}. Check feature_set {feature_set_id} factor_names."
            )
        if missing_features:
            log.warning(
                "model.train.feature_missing",
                trained=trained_feature_count,
                declared=len(feature_lineage["declared"]),
                missing=missing_features,
            )
            eval_metrics["missing_features"] = missing_features

        # ---- 10. Save model file + metadata ----
        model_id = uuid.uuid4().hex[:12]
        model_dir = settings.models_dir / model_id
        model_dir.mkdir(parents=True, exist_ok=True)

        joblib.dump(model_instance, str(model_dir / "model.joblib"))

        metadata = {
            "model_id": model_id,
            "market": resolved_market,
            "name": name,
            "model_type": model_type,
            "task": task,
            "objective_type": objective or task,
            "ranking_config": ranking_config if task == "ranking" else None,
            "feature_set_id": feature_set_id,
            "label_id": label_id,
            "model_params": model_instance.get_params(),
            "train_config": train_config,
            "eval_metrics": eval_metrics,
            "feature_names": trained_features,
            "feature_lineage": feature_lineage,
            "universe_group_id": universe_group_id,
            "sample_weight_config": sample_weight_config,
            "created_at": utc_now_iso(),
        }
        with open(model_dir / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        # ---- 11. Save record to DuckDB ----
        self._insert_model_record(
            model_id=model_id,
            market=resolved_market,
            name=name,
            feature_set_id=feature_set_id,
            label_id=label_id,
            model_type=model_type,
            model_params=model_instance.get_params(),
            train_config=train_config,
            eval_metrics=eval_metrics,
        )

        # ---- 12. Return summary ----
        summary = {
            "model_id": model_id,
            "market": resolved_market,
            "name": name,
            "model_type": model_type,
            "task": task,
            "objective_type": objective or task,
            "ranking_config": ranking_config if task == "ranking" else None,
            "train_samples": len(fit_X_train),
            "valid_samples": len(fit_X_valid),
            "test_samples": len(fit_X_test),
            "features": trained_feature_count,
            "feature_names": trained_features,
            "feature_lineage": feature_lineage,
            "eval_metrics": eval_metrics,
            "label_summary": label_summary,
        }
        log.info("model.train.done", model_id=model_id, market=resolved_market, features=trained_feature_count)
        return summary

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def list_models(self, market: str | None = None) -> list[dict]:
        """List all model records."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, market, name, feature_set_id, label_id, model_type,
                      model_params, train_config, eval_metrics,
                      status, created_at, updated_at
               FROM models
               WHERE market = ?
               ORDER BY created_at DESC""",
            [resolved_market],
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_model(self, model_id: str, market: str | None = None) -> dict:
        """Return a single model record including eval_metrics and feature_names."""
        resolved_market = normalize_market(market)
        cache_key = (resolved_market, model_id)
        if cache_key in self._record_cache:
            return self._record_cache[cache_key]

        row = self._fetch_row(model_id, market=resolved_market)
        if row is None:
            raise ValueError(f"Model {model_id} not found")

        # Load feature_names from metadata.json if available
        metadata_path = settings.models_dir / model_id / "metadata.json"
        if metadata_path.exists():
            with open(metadata_path) as f:
                metadata = json.load(f)
                if "feature_names" in metadata:
                    row["feature_names"] = metadata["feature_names"]
            if "market" not in metadata:
                try:
                    metadata["market"] = row["market"]
                    with open(metadata_path, "w") as f:
                        json.dump(metadata, f, indent=2, default=str)
                    log.info("model.backfill_metadata_market", model_id=model_id, market=row["market"])
                except Exception as exc:
                    log.warning("model.backfill_metadata_market_failed", model_id=model_id, error=str(exc))

        # Lazy backfill task_type for old models
        if not row.get("task_type"):
            model_path = settings.models_dir / model_id / "model.joblib"
            if model_path.exists():
                try:
                    model_instance = joblib.load(str(model_path))
                    task = _infer_task_from_model(model_instance)
                    eval_metrics = row.get("eval_metrics") or {}
                    eval_metrics["task_type"] = task
                    conn = get_connection()
                    conn.execute(
                        "UPDATE models SET eval_metrics = ? WHERE id = ? AND market = ?",
                        [json.dumps(eval_metrics, default=str), model_id, row["market"]],
                    )
                    row["task_type"] = task
                    row["eval_metrics"] = eval_metrics
                    log.info("model.backfill_task_type", model_id=model_id, task_type=task)
                except Exception:
                    pass

        self._record_cache[cache_key] = row
        return row

    def delete_model(self, model_id: str, market: str | None = None) -> None:
        """Delete a model record and its files on disk."""
        row = self._fetch_row(model_id, market)
        if row is None:
            raise ValueError(f"Model {model_id} not found")

        conn = get_connection()
        conn.execute("DELETE FROM models WHERE id = ? AND market = ?", [model_id, row["market"]])

        model_dir = settings.models_dir / model_id
        if model_dir.exists():
            shutil.rmtree(model_dir)

        # Invalidate caches
        self._model_cache.pop(model_id, None)
        self._record_cache.pop((row["market"], model_id), None)
        self._frozen_cache.pop(model_id, None)

        log.info("model.deleted", model_id=model_id, market=row["market"])

    def load_model(self, model_id: str, market: str | None = None) -> ModelBase:
        """Load a trained model from disk. Backfills task_type if missing."""
        if model_id in self._model_cache:
            self.get_model(model_id, market=market)
            return self._model_cache[model_id]

        model_path = settings.models_dir / model_id / "model.joblib"
        if not model_path.exists():
            raise ValueError(f"Model file not found for {model_id}")
        model_instance = joblib.load(str(model_path))

        # Backfill task_type for old models missing it
        try:
            record = self.get_model(model_id, market=market)
            if not record.get("task_type"):
                task = _infer_task_from_model(model_instance)
                eval_metrics = record.get("eval_metrics") or {}
                eval_metrics["task_type"] = task
                conn = get_connection()
                conn.execute(
                    "UPDATE models SET eval_metrics = ? WHERE id = ? AND market = ?",
                    [json.dumps(eval_metrics, default=str), model_id, record["market"]],
                )
                log.info("model.backfill_task_type", model_id=model_id, task_type=task)
        except Exception:
            pass

        self._model_cache[model_id] = model_instance
        return model_instance

    def predict(
        self,
        model_id: str,
        feature_set_id: str | None = None,
        tickers: list[str] | None = None,
        date: str | None = None,
        market: str | None = None,
    ) -> pd.Series:
        """Generate predictions for a given date and set of tickers.

        Args:
            model_id: ID of the trained model.
            feature_set_id: Override feature set (defaults to model's own).
            tickers: Ticker list.
            date: Target date string (YYYY-MM-DD).

        Returns:
            Series indexed by ticker with prediction values.
            For classification models, returns probability of positive class.
        """
        record = self.get_model(model_id, market=market)
        resolved_market = record["market"]
        model_instance = self.load_model(model_id, market=resolved_market)

        fs_id = feature_set_id or record["feature_set_id"]
        if not tickers:
            raise ValueError("tickers must be provided")
        if not date:
            raise ValueError("date must be provided")
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]

        # Compute features for the given date (use a small window around it)
        feature_data = self._feature_service.compute_features(
            fs_id, tickers, date, date, market=resolved_market
        )

        # Build X for the single date
        X = self._build_X_for_date(feature_data, tickers, date)
        if X.empty:
            return pd.Series(dtype=float, name="prediction")

        # Align to frozen training features
        frozen = self._load_frozen_features(model_id)
        if frozen:
            X = self._align_features_to_frozen(X, frozen, model_id)

        # Check if this is a classification model
        task = record.get("task_type") or _infer_task_from_model(model_instance)
        if task == "classification" and hasattr(model_instance, "predict_proba"):
            # Return probability of positive class for classification
            proba = model_instance.predict_proba(X)
            # proba is (n_samples, n_classes), take positive class (index 1)
            preds = pd.Series(proba[:, 1] if proba.shape[1] > 1 else proba[:, 0], index=X.index)
        else:
            preds = model_instance.predict(X)

        # Re-index by ticker
        preds.index = X.index.get_level_values("ticker") if "ticker" in X.index.names else X.index
        preds = self._break_prediction_ties(preds)
        preds.name = "prediction"
        return preds

    def predict_detailed(
        self,
        model_id: str,
        tickers: list[str],
        date: str,
        feature_set_id: str | None = None,
        market: str | None = None,
    ) -> pd.DataFrame:
        """Generate detailed predictions including prob, label, raw_score.

        For classification models returns DataFrame with columns:
            prob   – positive-class probability
            label  – hard classification (0/1)
            raw_score – raw model output (log-odds if available)

        For regression models returns DataFrame with column:
            prediction – raw prediction value
        """
        record = self.get_model(model_id, market=market)
        resolved_market = record["market"]
        model_instance = self.load_model(model_id, market=resolved_market)

        fs_id = feature_set_id or record["feature_set_id"]
        if not tickers:
            raise ValueError("tickers must be provided")
        if not date:
            raise ValueError("date must be provided")
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]

        feature_data = self._feature_service.compute_features(fs_id, tickers, date, date, market=resolved_market)
        X = self._build_X_for_date(feature_data, tickers, date)
        if X.empty:
            return pd.DataFrame()

        # Align to frozen training features
        frozen = self._load_frozen_features(model_id)
        if frozen:
            X = self._align_features_to_frozen(X, frozen, model_id)

        ticker_idx = X.index.get_level_values("ticker") if "ticker" in X.index.names else X.index
        task = record.get("task_type") or _infer_task_from_model(model_instance)

        if task == "classification" and hasattr(model_instance, "predict_proba"):
            proba = model_instance.predict_proba(X)
            prob_series = proba[:, 1] if proba.shape[1] > 1 else proba[:, 0]
            labels = model_instance.predict(X)
            label_values = labels.values if isinstance(labels, pd.Series) else labels

            raw_score = prob_series.copy()
            if hasattr(model_instance, "predict_raw"):
                try:
                    raw_series = model_instance.predict_raw(X)
                    raw_score = raw_series.values if isinstance(raw_series, pd.Series) else raw_series
                except Exception:
                    pass

            result = pd.DataFrame({
                "prob": prob_series,
                "label": label_values,
                "raw_score": raw_score,
            }, index=ticker_idx)
        else:
            preds = model_instance.predict(X)
            pred_values = preds.values if isinstance(preds, pd.Series) else preds
            result = pd.DataFrame({"prediction": pred_values}, index=ticker_idx)

        result.index.name = "ticker"
        return result

    def predict_with_features(
        self,
        model_id: str,
        feature_data: dict[str, pd.DataFrame],
        tickers: list[str],
        date: str,
        market: str | None = None,
    ) -> pd.Series:
        """Generate predictions using pre-computed feature data.

        Skips FeatureService.compute_features() entirely — the caller is
        responsible for providing the correct feature DataFrames.

        Args:
            model_id: ID of the trained model.
            feature_data: dict[factor_name -> DataFrame(dates x tickers)].
            tickers: Ticker list.
            date: Target date string (YYYY-MM-DD).

        Returns:
            Series indexed by ticker with prediction values.
            For classification models, returns probability of positive class.
        """
        record = self.get_model(model_id, market=market)
        resolved_market = record["market"]
        model_instance = self.load_model(model_id, market=resolved_market)
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]

        X = self._build_X_for_date(feature_data, tickers, date)
        if X.empty:
            return pd.Series(dtype=float, name="prediction")

        # Align to frozen training features
        frozen = self._load_frozen_features(model_id)
        if frozen:
            X = self._align_features_to_frozen(X, frozen, model_id)

        # Check if this is a classification model
        task = record.get("task_type") or _infer_task_from_model(model_instance)
        if task == "classification" and hasattr(model_instance, "predict_proba"):
            # Return probability of positive class for classification
            proba = model_instance.predict_proba(X)
            # proba is (n_samples, n_classes), take positive class (index 1)
            preds = pd.Series(proba[:, 1] if proba.shape[1] > 1 else proba[:, 0], index=X.index)
        else:
            preds = model_instance.predict(X)

        preds.index = X.index.get_level_values("ticker") if "ticker" in X.index.names else X.index
        preds = self._break_prediction_ties(preds)
        preds.name = "prediction"
        return preds

    def predict_batch(
        self,
        model_id: str,
        tickers: list[str],
        dates: list[str],
        feature_set_id: str | None = None,
        market: str | None = None,
    ) -> dict[str, dict[str, float]]:
        """Batch predictions across multiple dates in a single call.

        Loads the model and features once for the full date range, then
        builds X per date and predicts.  Much faster than calling
        predict() in a loop for each date.

        Returns:
            dict[date_str -> dict[ticker -> prediction_value]]
        """
        record = self.get_model(model_id, market=market)
        resolved_market = record["market"]
        model_instance = self.load_model(model_id, market=resolved_market)
        fs_id = feature_set_id or record["feature_set_id"]

        if not tickers:
            raise ValueError("tickers must be provided")
        if not dates:
            raise ValueError("dates must be provided")
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]

        sorted_dates = sorted(dates)
        start_date = sorted_dates[0]
        end_date = sorted_dates[-1]

        # Single bulk feature load for the entire date range
        feature_data = self._feature_service.compute_features_from_cache(
            fs_id, tickers, start_date, end_date, market=resolved_market
        )

        frozen = self._load_frozen_features(model_id)
        task = record.get("task_type") or _infer_task_from_model(model_instance)

        results: dict[str, dict[str, float]] = {}
        for date in sorted_dates:
            X = self._build_X_for_date(feature_data, tickers, date)
            if X.empty:
                results[date] = {}
                continue

            if frozen:
                try:
                    X = self._align_features_to_frozen(X, frozen, model_id)
                except ValueError:
                    results[date] = {}
                    continue

            if task == "classification" and hasattr(model_instance, "predict_proba"):
                proba = model_instance.predict_proba(X)
                preds = pd.Series(
                    proba[:, 1] if proba.shape[1] > 1 else proba[:, 0],
                    index=X.index,
                )
            else:
                preds = model_instance.predict(X)

            preds.index = (
                X.index.get_level_values("ticker")
                if "ticker" in X.index.names
                else X.index
            )
            preds = self._break_prediction_ties(preds)

            results[date] = {
                str(t): round(float(v), 6) for t, v in preds.items()
            }

        return results

    def _load_frozen_features(self, model_id: str) -> list[str] | None:
        """Load the feature name list frozen at training time from metadata.json."""
        if model_id in self._frozen_cache:
            return self._frozen_cache[model_id]

        metadata_path = settings.models_dir / model_id / "metadata.json"
        result: list[str] | None = None
        if metadata_path.exists():
            with open(metadata_path) as f:
                metadata = json.load(f)
                result = metadata.get("feature_names")
        self._frozen_cache[model_id] = result
        return result

    @staticmethod
    def _align_features_to_frozen(
        X: pd.DataFrame, frozen: list[str], model_id: str
    ) -> pd.DataFrame:
        """Reorder/filter X columns to match frozen training features.

        Raises ValueError if any frozen feature is completely missing from X.
        """
        missing = [f for f in frozen if f not in X.columns]
        if missing:
            raise ValueError(
                f"Model {model_id} was trained on {len(frozen)} features but "
                f"{len(missing)} are missing from the current feature set: "
                f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
            )
        return X[frozen]

    # ------------------------------------------------------------------
    # Data building helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_Xy(
        feature_data: dict[str, pd.DataFrame],
        label_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Build aligned X and y from feature data and label values.

        feature_data: dict[factor_name -> DataFrame(dates x tickers)]
        label_df: DataFrame[ticker, date, label_value]

        Returns X with MultiIndex (date, ticker), columns = factor names
                y Series aligned with X
        """
        # Convert feature data (dates x tickers per factor) into long format
        # then pivot into X: (date, ticker) x factor_name
        long_frames: list[pd.DataFrame] = []
        factor_names = sorted(feature_data.keys())

        for factor_name in factor_names:
            df = feature_data[factor_name]
            # df: index=dates, columns=tickers
            stacked = df.stack()
            stacked.name = factor_name
            stacked.index.names = ["date", "ticker"]
            long_frames.append(stacked)

        if not long_frames:
            return pd.DataFrame(), pd.Series(dtype=float)

        # Concatenate all factor series into a single DataFrame
        X = pd.concat(long_frames, axis=1)
        X.index.names = ["date", "ticker"]

        # Convert label_df to indexed form
        label_df = label_df.copy()
        label_df["date"] = pd.to_datetime(label_df["date"])
        label_indexed = label_df.set_index(["date", "ticker"])["label_value"]

        # Align X and y on common (date, ticker) pairs
        common_idx = X.index.intersection(label_indexed.index)
        X = X.loc[common_idx].copy()
        y = label_indexed.loc[common_idx].copy()

        # Drop rows where any feature or label is NaN
        mask = X.notna().all(axis=1) & y.notna()
        X = X.loc[mask]
        y = y.loc[mask]

        return X, y

    @staticmethod
    def _build_sample_weights(
        X_train: pd.DataFrame,
        y_train: pd.Series,
        config: dict[str, Any],
        feature_data: dict[str, pd.DataFrame],
    ) -> np.ndarray | None:
        """Build a sample weight vector from config.

        Supported config keys:
        - ``label_quantile_boost``: dict with ``quantile`` (0-1, top fraction to boost),
          ``weight`` (multiplier for those samples, default 3.0)
        - ``recency_half_life``: int, number of trading days for exponential decay.
          Recent samples get higher weight.
        - ``factor_boost``: dict with ``factor`` (factor name present in feature_data),
          ``threshold`` (value above which to boost), ``weight`` (multiplier).
          Useful for boosting breakout / near-52w-high / orderly-path samples.

        Multiple keys can coexist; weights are multiplied together.
        """
        n = len(X_train)
        if n == 0:
            return None

        weights = np.ones(n, dtype=np.float64)

        # --- label_quantile_boost: boost top-quantile label samples ---
        lqb = config.get("label_quantile_boost")
        if lqb:
            q = lqb.get("quantile", 0.2)
            w = lqb.get("weight", 3.0)
            threshold = y_train.quantile(1 - q)
            weights[y_train.values >= threshold] *= w

        # --- recency_half_life: exponential time decay ---
        half_life = config.get("recency_half_life")
        if half_life and half_life > 0:
            dates = X_train.index.get_level_values("date")
            unique_dates = np.sort(dates.unique())
            date_rank = pd.Series(np.arange(len(unique_dates)), index=unique_dates)
            ranks = dates.map(date_rank).values.astype(float)
            max_rank = ranks.max()
            # decay: weight = 2^((rank - max_rank) / half_life)
            decay = np.power(2.0, (ranks - max_rank) / half_life)
            weights *= decay

        # --- factor_boost: boost samples where a factor exceeds threshold ---
        fb = config.get("factor_boost")
        if fb:
            factor_name = fb.get("factor")
            threshold = fb.get("threshold", 0.0)
            w = fb.get("weight", 3.0)
            direction = fb.get("direction", "above")  # "above" or "below"
            if factor_name and factor_name in X_train.columns:
                vals = X_train[factor_name].values
                if direction == "above":
                    mask = vals >= threshold
                else:
                    mask = vals <= threshold
                weights[mask] *= w

        # --- factor_boosts: list of factor_boost configs ---
        fbs = config.get("factor_boosts")
        if fbs and isinstance(fbs, list):
            for fb_item in fbs:
                factor_name = fb_item.get("factor")
                threshold = fb_item.get("threshold", 0.0)
                w = fb_item.get("weight", 3.0)
                direction = fb_item.get("direction", "above")
                if factor_name and factor_name in X_train.columns:
                    vals = X_train[factor_name].values
                    if direction == "above":
                        mask = vals >= threshold
                    else:
                        mask = vals <= threshold
                    weights[mask] *= w

        # Normalize so mean weight = 1.0 for stable gradient scale
        mean_w = weights.mean()
        if mean_w > 0:
            weights /= mean_w

        return weights

    @staticmethod
    def _build_X_for_date(
        feature_data: dict[str, pd.DataFrame],
        tickers: list[str],
        date: str,
    ) -> pd.DataFrame:
        """Build X matrix for a single date from feature data.

        Returns DataFrame with MultiIndex (date, ticker), columns = factor names.
        """
        target_date = pd.Timestamp(date)
        records: dict[str, dict[str, float]] = {}

        for factor_name, df in feature_data.items():
            # df: index=dates, columns=tickers
            if df.empty:
                continue
            # Find the closest date <= target
            available = df.index[df.index <= target_date]
            if len(available) == 0:
                continue
            closest = available[-1]
            for ticker in tickers:
                if ticker in df.columns:
                    val = df.loc[closest, ticker]
                    if not pd.isna(val):
                        if ticker not in records:
                            records[ticker] = {}
                        records[ticker][factor_name] = float(val)

        if not records:
            return pd.DataFrame()

        rows = []
        index_tuples = []
        factor_names = sorted(feature_data.keys())
        for ticker, vals in records.items():
            row = {fn: vals.get(fn, np.nan) for fn in factor_names}
            rows.append(row)
            index_tuples.append((target_date, ticker))

        X = pd.DataFrame(rows, index=pd.MultiIndex.from_tuples(index_tuples, names=["date", "ticker"]))
        # Drop rows with any NaN
        X = X.dropna()
        return X

    @staticmethod
    def _break_prediction_ties(preds: pd.Series) -> pd.Series:
        """Make equal-score ordering deterministic across windows and runs.

        LightGBM predictions are often discretized, so ties are common. Many
        strategies sort scores directly, and pandas' default sort is not stable
        for equal values. We add a tiny ticker-based epsilon so ties break
        consistently without changing materially different scores.
        """
        if preds.empty:
            return preds

        adjusted = preds.astype(float).copy()
        tickers = pd.Index(adjusted.index.astype(str))
        alpha_order = pd.Series(
            np.arange(len(tickers), dtype=float),
            index=tickers.sort_values(),
        )
        # Alphabetically earlier ticker gets a slightly larger adjusted score.
        ranks = tickers.map(alpha_order).to_numpy(dtype=float)
        epsilon = 1e-12
        adjusted = adjusted + (len(tickers) - ranks) * epsilon
        adjusted.name = preds.name
        return adjusted

    @staticmethod
    def _build_feature_lineage(
        trained_features: list[str],
        factor_refs: list[dict],
    ) -> dict[str, list]:
        declared: list[dict[str, str]] = []
        by_name: dict[str, dict[str, str]] = {}
        for ref in factor_refs or []:
            factor_id = str(ref.get("factor_id", ""))
            factor_name = str(ref.get("factor_name") or factor_id)
            entry = {
                "factor_id": factor_id,
                "factor_name": factor_name,
            }
            declared.append(entry)
            by_name[factor_name] = entry

        trained: list[dict[str, str]] = []
        undeclared: list[str] = []
        for feature_name in trained_features:
            entry = by_name.get(feature_name)
            if entry is None:
                undeclared.append(feature_name)
                continue
            trained.append(entry)

        trained_names = set(trained_features)
        missing = [
            entry for entry in declared
            if entry["factor_name"] not in trained_names
        ]
        return {
            "declared": declared,
            "trained": trained,
            "missing": missing,
            "undeclared": undeclared,
        }

    @staticmethod
    def _normalize_train_config(train_config: dict, market: str | None = None) -> dict:
        """Normalize train_config from various frontend formats to flat keys.

        Handles the nested format from the frontend:
            { train_period: {start, end}, valid_period: {start, end}, test_period: {start, end} }
        And converts it to the flat format the backend expects:
            { train_start, train_end, valid_start, valid_end, test_start, test_end }
        Also snaps all date values to the nearest valid trading day.
        """
        result = dict(train_config)

        # Map nested period format to flat keys
        for period_key, (start_key, end_key) in {
            "train_period": ("train_start", "train_end"),
            "valid_period": ("valid_start", "valid_end"),
            "test_period": ("test_start", "test_end"),
        }.items():
            if period_key in result:
                period = result.pop(period_key)
                if isinstance(period, dict):
                    if "start" in period and start_key not in result:
                        result[start_key] = period["start"]
                    if "end" in period and end_key not in result:
                        result[end_key] = period["end"]

        # Snap all date values to nearest trading days
        from datetime import date as _date
        date_keys_forward = {"train_start", "valid_start", "test_start"}
        date_keys_backward = {"train_end", "valid_end", "test_end"}
        for key in date_keys_forward | date_keys_backward:
            if key in result and result[key]:
                try:
                    dt = _date.fromisoformat(str(result[key]))
                    direction = "forward" if key in date_keys_forward else "backward"
                    result[key] = str(snap_to_trading_day(dt, direction=direction, market=market))
                except (ValueError, TypeError):
                    pass  # leave as-is, will fail downstream with a clear error

        return result

    @staticmethod
    def _validate_split_config(
        train_config: dict, X: pd.DataFrame, y: pd.Series, purge_gap: int
    ) -> None:
        """Preflight validation for train/valid/test split configuration.

        Raises ValueError with a clear message if the config is invalid.
        """
        required = ["train_start", "train_end", "valid_start", "valid_end", "test_start", "test_end"]
        for key in required:
            if not train_config.get(key):
                raise ValueError(f"train_config 缺少必要字段: {key}")

        ts = {k: pd.Timestamp(train_config[k]) for k in required}

        # Date ordering
        if ts["train_start"] > ts["train_end"]:
            raise ValueError(f"训练开始日期 ({ts['train_start'].date()}) 晚于结束日期 ({ts['train_end'].date()})")
        if ts["valid_start"] > ts["valid_end"]:
            raise ValueError(f"验证开始日期 ({ts['valid_start'].date()}) 晚于结束日期 ({ts['valid_end'].date()})")
        if ts["test_start"] > ts["test_end"]:
            raise ValueError(f"测试开始日期 ({ts['test_start'].date()}) 晚于结束日期 ({ts['test_end'].date()})")
        if ts["train_end"] >= ts["valid_start"]:
            raise ValueError(
                f"训练结束日期 ({ts['train_end'].date()}) 不早于验证开始日期 ({ts['valid_start'].date()})，"
                "请确保区间不重叠"
            )
        if ts["valid_end"] >= ts["test_start"]:
            raise ValueError(
                f"验证结束日期 ({ts['valid_end'].date()}) 不早于测试开始日期 ({ts['test_start'].date()})，"
                "请确保区间不重叠"
            )

        # Check sample counts
        dates = X.index.get_level_values("date")
        train_n = ((dates >= ts["train_start"]) & (dates <= ts["train_end"])).sum()
        valid_n = ((dates >= ts["valid_start"]) & (dates <= ts["valid_end"])).sum()
        test_n = ((dates >= ts["test_start"]) & (dates <= ts["test_end"])).sum()

        if train_n == 0:
            raise ValueError(
                f"训练集在 {ts['train_start'].date()} ~ {ts['train_end'].date()} 区间内无样本。"
                "请检查特征/标签数据是否覆盖该区间。"
            )
        if valid_n == 0:
            raise ValueError(
                f"验证集在 {ts['valid_start'].date()} ~ {ts['valid_end'].date()} 区间内无样本。"
                "空验证集会导致模型无法早停和评估，请调整区间。"
            )
        if test_n == 0:
            raise ValueError(
                f"测试集在 {ts['test_start'].date()} ~ {ts['test_end'].date()} 区间内无样本。"
                "请检查数据覆盖范围。"
            )

        # Warn if purge would consume too many training samples
        train_dates = sorted(dates[(dates >= ts["train_start"]) & (dates <= ts["train_end"])].unique())
        if purge_gap > 0 and len(train_dates) <= purge_gap:
            raise ValueError(
                f"训练集仅有 {len(train_dates)} 个交易日，purge_gap={purge_gap} 天会清空全部训练数据。"
                "请扩大训练区间或减小 purge_gap。"
            )

    @staticmethod
    def _split_by_dates(
        X: pd.DataFrame,
        y: pd.Series,
        train_config: dict,
        purge_gap: int,
    ) -> dict[str, tuple[pd.DataFrame, pd.Series]]:
        """Split X and y by date ranges defined in train_config.

        Handles purge_gap by removing trading days between train_end
        and valid_start.
        """
        train_start = pd.Timestamp(train_config.get("train_start"))
        train_end = pd.Timestamp(train_config.get("train_end"))
        valid_start = pd.Timestamp(train_config.get("valid_start"))
        valid_end = pd.Timestamp(train_config.get("valid_end"))
        test_start = pd.Timestamp(train_config.get("test_start"))
        test_end = pd.Timestamp(train_config.get("test_end"))

        dates = X.index.get_level_values("date")

        # Train set
        train_mask = (dates >= train_start) & (dates <= train_end)
        X_train = X.loc[train_mask]
        y_train = y.loc[train_mask]

        # Apply purge gap: find trading dates in [train_end-purge, train_end]
        # and also in [valid_start, valid_start+purge], remove those from
        # the boundaries.  In practice we just skip purge_gap trading days
        # from the end of training data.
        if purge_gap > 0:
            unique_train_dates = sorted(X_train.index.get_level_values("date").unique())
            if len(unique_train_dates) > purge_gap:
                purge_cutoff = unique_train_dates[-purge_gap]
                purge_mask = X_train.index.get_level_values("date") < purge_cutoff
                X_train = X_train.loc[purge_mask]
                y_train = y_train.loc[purge_mask]

        # Validation set
        valid_mask = (dates >= valid_start) & (dates <= valid_end)
        X_valid = X.loc[valid_mask]
        y_valid = y.loc[valid_mask]

        # Test set
        test_mask = (dates >= test_start) & (dates <= test_end)
        X_test = X.loc[test_mask]
        y_test = y.loc[test_mask]

        return {
            "train": (X_train, y_train),
            "valid": (X_valid, y_valid),
            "test": (X_test, y_test),
        }

    @staticmethod
    def _compute_eval_metrics(
        task: str,
        y_train: pd.Series,
        y_valid: pd.Series,
        y_test: pd.Series,
        preds_valid: pd.Series,
        preds_test: pd.Series,
    ) -> dict:
        """Compute evaluation metrics for the model.

        Regression: IC (Spearman rank correlation), RMSE
        Classification: AUC, F1
        """
        from scipy.stats import spearmanr

        metrics: dict[str, Any] = {
            "train_samples": len(y_train),
            "valid_samples": len(y_valid),
            "test_samples": len(y_test),
        }

        if task == "regression":
            # Validation metrics
            if len(y_valid) > 0 and len(preds_valid) > 0:
                ic_valid, _ = spearmanr(y_valid.values, preds_valid.values)
                rmse_valid = float(np.sqrt(np.mean((y_valid.values - preds_valid.values) ** 2)))
                metrics["valid_ic"] = round(float(ic_valid), 6) if not np.isnan(ic_valid) else None
                metrics["valid_rmse"] = round(rmse_valid, 6)

                # Per-date IC (daily rank IC)
                metrics["valid_daily_ic"] = _compute_daily_ic(y_valid, preds_valid)

            # Test metrics
            if len(y_test) > 0 and len(preds_test) > 0:
                ic_test, _ = spearmanr(y_test.values, preds_test.values)
                rmse_test = float(np.sqrt(np.mean((y_test.values - preds_test.values) ** 2)))
                metrics["test_ic"] = round(float(ic_test), 6) if not np.isnan(ic_test) else None
                metrics["test_rmse"] = round(rmse_test, 6)

                metrics["test_daily_ic"] = _compute_daily_ic(y_test, preds_test)

        elif task == "classification":
            from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

            # Validation metrics
            if len(y_valid) > 0 and len(preds_valid) > 0:
                try:
                    metrics["valid_auc"] = round(float(roc_auc_score(y_valid, preds_valid)), 6)
                except ValueError:
                    metrics["valid_auc"] = None
                preds_binary = (preds_valid >= 0.5).astype(int)
                metrics["valid_f1"] = round(float(f1_score(y_valid, preds_binary, zero_division=0)), 6)
                metrics["valid_accuracy"] = round(float(accuracy_score(y_valid, preds_binary)), 6)
                metrics["valid_precision"] = round(float(precision_score(y_valid, preds_binary, zero_division=0)), 6)
                metrics["valid_recall"] = round(float(recall_score(y_valid, preds_binary, zero_division=0)), 6)

                # Daily IC on predicted probabilities (ranking quality)
                metrics["valid_daily_ic"] = _compute_daily_ic(y_valid, preds_valid)

            # Test metrics
            if len(y_test) > 0 and len(preds_test) > 0:
                try:
                    metrics["test_auc"] = round(float(roc_auc_score(y_test, preds_test)), 6)
                except ValueError:
                    metrics["test_auc"] = None
                preds_binary = (preds_test >= 0.5).astype(int)
                metrics["test_f1"] = round(float(f1_score(y_test, preds_binary, zero_division=0)), 6)
                metrics["test_accuracy"] = round(float(accuracy_score(y_test, preds_binary)), 6)
                metrics["test_precision"] = round(float(precision_score(y_test, preds_binary, zero_division=0)), 6)
                metrics["test_recall"] = round(float(recall_score(y_test, preds_binary, zero_division=0)), 6)

                metrics["test_daily_ic"] = _compute_daily_ic(y_test, preds_test)

        # ---- Promote daily IC summary to top-level for easy access ----
        for prefix in ("test", "valid"):
            daily_key = f"{prefix}_daily_ic"
            if daily_key in metrics and isinstance(metrics[daily_key], dict):
                dic = metrics[daily_key]
                if "mean_ic" in dic and "ic_mean" not in metrics:
                    metrics["ic_mean"] = dic["mean_ic"]
                if "std_ic" in dic and "ic_std" not in metrics:
                    metrics["ic_std"] = dic["std_ic"]
                if "ir" in dic and "ir" not in metrics:
                    metrics["ir"] = dic["ir"]
                break  # prefer test over valid

        # ---- Long-short portfolio metrics (Sharpe, return, drawdown) ----
        ls_src_y, ls_src_p = (y_test, preds_test) if len(y_test) > 0 else (y_valid, preds_valid)
        ls_metrics = _compute_long_short_metrics(ls_src_y, ls_src_p)
        if ls_metrics:
            metrics.update(ls_metrics)

        return metrics

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _insert_model_record(
        self,
        model_id: str,
        market: str,
        name: str,
        feature_set_id: str,
        label_id: str,
        model_type: str,
        model_params: dict,
        train_config: dict,
        eval_metrics: dict,
    ) -> None:
        conn = get_connection()
        now = utc_now_naive()
        conn.execute(
            """INSERT INTO models
               (id, market, name, feature_set_id, label_id, model_type,
                model_params, train_config, eval_metrics,
                status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'trained', ?, ?)""",
            [
                model_id,
                market,
                name,
                feature_set_id,
                label_id,
                model_type,
                json.dumps(model_params, default=str),
                json.dumps(train_config, default=str),
                json.dumps(eval_metrics, default=str),
                now,
                now,
            ],
        )
        log.info("model.record_saved", model_id=model_id, market=market)

    def _fetch_row(self, model_id: str, market: str | None = None) -> dict | None:
        resolved_market = normalize_market(market)
        conn = get_connection()
        row = conn.execute(
            """SELECT id, market, name, feature_set_id, label_id, model_type,
                      model_params, train_config, eval_metrics,
                      status, created_at, updated_at
               FROM models WHERE id = ? AND market = ?""",
            [model_id, resolved_market],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        def _parse_json(raw):
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    return {}
            return raw if raw else {}

        eval_metrics = _parse_json(row[8])
        return {
            "id": row[0],
            "market": row[1],
            "name": row[2],
            "feature_set_id": row[3],
            "label_id": row[4],
            "model_type": row[5],
            "model_params": _parse_json(row[6]),
            "train_config": _parse_json(row[7]),
            "eval_metrics": eval_metrics,
            "task_type": eval_metrics.get("task_type"),
            "status": row[9],
            "created_at": str(row[10]) if row[10] else None,
            "updated_at": str(row[11]) if row[11] else None,
        }


def _compute_daily_ic(y: pd.Series, preds: pd.Series) -> dict:
    """Compute daily (cross-sectional) IC between actual and predicted.

    Returns dict with mean_ic, std_ic, ir (information ratio).
    """
    from scipy.stats import spearmanr

    if not isinstance(y.index, pd.MultiIndex):
        return {}

    dates = y.index.get_level_values("date").unique()
    daily_ics: list[float] = []

    for dt in dates:
        try:
            y_day = y.xs(dt, level="date")
            p_day = preds.xs(dt, level="date")
            common = y_day.index.intersection(p_day.index)
            if len(common) < 5:
                continue
            ic, _ = spearmanr(y_day.loc[common].values, p_day.loc[common].values)
            if not np.isnan(ic):
                daily_ics.append(float(ic))
        except (KeyError, ValueError):
            continue

    if not daily_ics:
        return {}

    mean_ic = float(np.mean(daily_ics))
    std_ic = float(np.std(daily_ics)) if len(daily_ics) > 1 else 0.0
    ir = mean_ic / std_ic if std_ic > 0 else 0.0

    return {
        "mean_ic": round(mean_ic, 6),
        "std_ic": round(std_ic, 6),
        "ir": round(ir, 6),
        "num_days": len(daily_ics),
    }


def _compute_long_short_metrics(
    y: pd.Series, preds: pd.Series
) -> dict[str, Any]:
    """Compute long-short portfolio metrics from model predictions.

    On each date, go long the top quintile and short the bottom quintile
    (by predicted score).  The daily return is the mean return of the long
    leg minus the mean return of the short leg.

    Returns dict with sharpe, annual_return, max_drawdown, calmar.
    """
    if not isinstance(y.index, pd.MultiIndex) or len(y) == 0:
        return {}

    dates = sorted(y.index.get_level_values("date").unique())
    if len(dates) < 10:
        return {}

    daily_returns: list[float] = []
    for dt in dates:
        try:
            y_day = y.xs(dt, level="date")
            p_day = preds.xs(dt, level="date")
            common = y_day.index.intersection(p_day.index)
            if len(common) < 10:
                continue
            p_vals = p_day.loc[common]
            y_vals = y_day.loc[common]
            n = max(1, len(common) // 5)  # quintile
            top_idx = p_vals.nlargest(n).index
            bot_idx = p_vals.nsmallest(n).index
            ret = float(y_vals.loc[top_idx].mean() - y_vals.loc[bot_idx].mean())
            daily_returns.append(ret)
        except (KeyError, ValueError):
            continue

    if len(daily_returns) < 10:
        return {}

    arr = np.array(daily_returns)
    ann_factor = 252
    mean_ret = float(np.mean(arr))
    std_ret = float(np.std(arr))
    annual_return = mean_ret * ann_factor
    annual_vol = std_ret * np.sqrt(ann_factor)
    sharpe = float(annual_return / annual_vol) if annual_vol > 0 else 0.0

    # Max drawdown from cumulative returns
    cum = np.cumsum(arr)
    running_max = np.maximum.accumulate(cum)
    drawdowns = running_max - cum
    max_dd = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

    calmar = float(annual_return / max_dd) if max_dd > 0 else 0.0

    return {
        "sharpe": round(sharpe, 4),
        "annual_return": round(annual_return, 6),
        "max_drawdown": round(max_dd, 6),
        "calmar": round(calmar, 4),
    }
