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

# Parameters determined by the system, not user-configurable
_RESERVED_MODEL_PARAMS = {"task", "objective", "metric", "verbosity", "n_jobs"}


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

        # ---- Strip reserved params that are set by the system ----
        stripped = [k for k in model_params if k in _RESERVED_MODEL_PARAMS]
        if stripped:
            log.warning("model.train.reserved_params_stripped", params=stripped)
            model_params = {k: v for k, v in model_params.items() if k not in _RESERVED_MODEL_PARAMS}

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
        preds = self._break_prediction_ties(preds)
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
