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

log = get_logger(__name__)

# Registry of supported model types
_MODEL_REGISTRY: dict[str, type] = {
    "lightgbm": LightGBMModel,
}

# Label target types that should be treated as classification
_CLASSIFICATION_TARGETS = {"binary"}


class ModelService:
    """Train, persist, load and run inference with ML models."""

    def __init__(self) -> None:
        self._feature_service = FeatureService()
        self._label_service = LabelService()
        self._group_service = GroupService()

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

        # ---- Normalize train_config from frontend nested format ----
        # Frontend sends: { train_period: {start, end}, valid_period: ..., test_period: ... }
        # Backend expects: { train_start, train_end, valid_start, valid_end, test_start, test_end }
        train_config = self._normalize_train_config(train_config)

        # ---- 1. Resolve tickers ----
        if universe_group_id:
            tickers = self._group_service.get_group_tickers(universe_group_id)
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
            model_type=model_type,
            tickers=len(tickers),
            date_range=f"{overall_start} ~ {overall_end}",
        )

        # ---- 2. Compute features ----
        feature_data = self._feature_service.compute_features(
            feature_set_id, tickers, overall_start, overall_end
        )
        # feature_data: dict[factor_name -> DataFrame(dates x tickers)]

        # ---- 3. Compute labels ----
        label_def = self._label_service.get_label(label_id)
        label_df = self._label_service.compute_label_values(
            label_id, tickers, overall_start, overall_end
        )
        # label_df: DataFrame with columns [ticker, date, label_value]

        if label_df.empty:
            raise ValueError("No label data computed for the given parameters")

        # ---- 4. Build X and y aligned by (date, ticker) ----
        X, y = self._build_Xy(feature_data, label_df)
        if X.empty:
            raise ValueError("No aligned (date, ticker) pairs after joining features and labels")

        log.info("model.train.data_built", X_shape=X.shape, y_size=len(y))

        # ---- 5. Split by date ranges ----
        purge_gap = int(train_config.get("purge_gap", 5))
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
        task = "classification" if label_def["target_type"] in _CLASSIFICATION_TARGETS else "regression"
        model_cls = _MODEL_REGISTRY[model_type]
        model_instance: ModelBase = model_cls(task=task, params=model_params)

        fit_kwargs: dict[str, Any] = {}
        if len(X_valid) > 0:
            fit_kwargs["eval_set"] = [(X_valid, y_valid)]

        model_instance.fit(X_train, y_train, **fit_kwargs)

        # ---- 7. Predict on valid and test sets ----
        preds_valid = model_instance.predict(X_valid) if len(X_valid) > 0 else pd.Series(dtype=float)
        preds_test = model_instance.predict(X_test) if len(X_test) > 0 else pd.Series(dtype=float)

        # ---- 8. Calculate eval metrics ----
        eval_metrics = self._compute_eval_metrics(
            task, y_train, y_valid, y_test, preds_valid, preds_test
        )

        # Feature importance
        try:
            fi = model_instance.feature_importance()
            eval_metrics["feature_importance"] = {
                k: round(float(v), 6) for k, v in fi.head(30).items()
            }
        except NotImplementedError:
            pass

        log.info("model.train.metrics", metrics=eval_metrics)

        # ---- 9. Save model file + metadata ----
        model_id = uuid.uuid4().hex[:12]
        model_dir = settings.models_dir / model_id
        model_dir.mkdir(parents=True, exist_ok=True)

        joblib.dump(model_instance, str(model_dir / "model.joblib"))

        metadata = {
            "model_id": model_id,
            "name": name,
            "model_type": model_type,
            "task": task,
            "feature_set_id": feature_set_id,
            "label_id": label_id,
            "model_params": model_instance.get_params(),
            "train_config": train_config,
            "eval_metrics": eval_metrics,
            "feature_names": list(X.columns),
            "universe_group_id": universe_group_id,
            "created_at": datetime.utcnow().isoformat(),
        }
        with open(model_dir / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        # ---- 10. Save record to DuckDB ----
        self._insert_model_record(
            model_id=model_id,
            name=name,
            feature_set_id=feature_set_id,
            label_id=label_id,
            model_type=model_type,
            model_params=model_instance.get_params(),
            train_config=train_config,
            eval_metrics=eval_metrics,
        )

        # ---- 11. Return summary ----
        summary = {
            "model_id": model_id,
            "name": name,
            "model_type": model_type,
            "task": task,
            "train_samples": len(X_train),
            "valid_samples": len(X_valid),
            "test_samples": len(X_test),
            "features": len(X.columns),
            "eval_metrics": eval_metrics,
        }
        log.info("model.train.done", model_id=model_id)
        return summary

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def list_models(self) -> list[dict]:
        """List all model records."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, name, feature_set_id, label_id, model_type,
                      model_params, train_config, eval_metrics,
                      status, created_at, updated_at
               FROM models
               ORDER BY created_at DESC"""
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_model(self, model_id: str) -> dict:
        """Return a single model record including eval_metrics."""
        row = self._fetch_row(model_id)
        if row is None:
            raise ValueError(f"Model {model_id} not found")
        return row

    def delete_model(self, model_id: str) -> None:
        """Delete a model record and its files on disk."""
        row = self._fetch_row(model_id)
        if row is None:
            raise ValueError(f"Model {model_id} not found")

        conn = get_connection()
        conn.execute("DELETE FROM models WHERE id = ?", [model_id])

        model_dir = settings.models_dir / model_id
        if model_dir.exists():
            shutil.rmtree(model_dir)

        log.info("model.deleted", model_id=model_id)

    def load_model(self, model_id: str) -> ModelBase:
        """Load a trained model from disk."""
        model_path = settings.models_dir / model_id / "model.joblib"
        if not model_path.exists():
            raise ValueError(f"Model file not found for {model_id}")
        return joblib.load(str(model_path))

    def predict(
        self,
        model_id: str,
        feature_set_id: str | None = None,
        tickers: list[str] | None = None,
        date: str | None = None,
    ) -> pd.Series:
        """Generate predictions for a given date and set of tickers.

        Args:
            model_id: ID of the trained model.
            feature_set_id: Override feature set (defaults to model's own).
            tickers: Ticker list.
            date: Target date string (YYYY-MM-DD).

        Returns:
            Series indexed by ticker with prediction values.
        """
        record = self.get_model(model_id)
        model_instance = self.load_model(model_id)

        fs_id = feature_set_id or record["feature_set_id"]
        if not tickers:
            raise ValueError("tickers must be provided")
        if not date:
            raise ValueError("date must be provided")

        # Compute features for the given date (use a small window around it)
        feature_data = self._feature_service.compute_features(
            fs_id, tickers, date, date
        )

        # Build X for the single date
        X = self._build_X_for_date(feature_data, tickers, date)
        if X.empty:
            return pd.Series(dtype=float, name="prediction")

        preds = model_instance.predict(X)
        # Re-index by ticker
        preds.index = X.index.get_level_values("ticker") if "ticker" in X.index.names else X.index
        preds.name = "prediction"
        return preds

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
    def _normalize_train_config(train_config: dict) -> dict:
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
                    result[key] = str(snap_to_trading_day(dt, direction=direction))
                except (ValueError, TypeError):
                    pass  # leave as-is, will fail downstream with a clear error

        return result

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
            from sklearn.metrics import f1_score, roc_auc_score

            # Validation metrics
            if len(y_valid) > 0 and len(preds_valid) > 0:
                try:
                    metrics["valid_auc"] = round(float(roc_auc_score(y_valid, preds_valid)), 6)
                except ValueError:
                    metrics["valid_auc"] = None
                preds_binary = (preds_valid >= 0.5).astype(int)
                metrics["valid_f1"] = round(float(f1_score(y_valid, preds_binary, zero_division=0)), 6)

            # Test metrics
            if len(y_test) > 0 and len(preds_test) > 0:
                try:
                    metrics["test_auc"] = round(float(roc_auc_score(y_test, preds_test)), 6)
                except ValueError:
                    metrics["test_auc"] = None
                preds_binary = (preds_test >= 0.5).astype(int)
                metrics["test_f1"] = round(float(f1_score(y_test, preds_binary, zero_division=0)), 6)

        return metrics

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _insert_model_record(
        self,
        model_id: str,
        name: str,
        feature_set_id: str,
        label_id: str,
        model_type: str,
        model_params: dict,
        train_config: dict,
        eval_metrics: dict,
    ) -> None:
        conn = get_connection()
        now = datetime.utcnow()
        conn.execute(
            """INSERT INTO models
               (id, name, feature_set_id, label_id, model_type,
                model_params, train_config, eval_metrics,
                status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'trained', ?, ?)""",
            [
                model_id,
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
        log.info("model.record_saved", model_id=model_id)

    def _fetch_row(self, model_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, feature_set_id, label_id, model_type,
                      model_params, train_config, eval_metrics,
                      status, created_at, updated_at
               FROM models WHERE id = ?""",
            [model_id],
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

        return {
            "id": row[0],
            "name": row[1],
            "feature_set_id": row[2],
            "label_id": row[3],
            "model_type": row[4],
            "model_params": _parse_json(row[5]),
            "train_config": _parse_json(row[6]),
            "eval_metrics": _parse_json(row[7]),
            "status": row[8],
            "created_at": str(row[9]) if row[9] else None,
            "updated_at": str(row[10]) if row[10] else None,
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
