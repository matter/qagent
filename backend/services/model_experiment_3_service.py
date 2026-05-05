"""QAgent 3.0 model experiment and package service.

M6 makes model training depend on materialized Dataset artifacts.  The service
does not compute features or labels internally; it consumes the audited sample
panel produced by M4 and writes experiment, prediction, package, and promotion
records through the 3.0 research kernel.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from backend.db import get_connection
from backend.models.lightgbm_model import LightGBMModel
from backend.services.dataset_service import DatasetService
from backend.services.research_kernel_service import ResearchKernelService
from backend.time_utils import utc_now_naive


_MODEL_REGISTRY = {"lightgbm": LightGBMModel}
_INDEX_COLUMNS = {"date", "asset_id", "ticker", "membership_state", "available_at", "label"}


class ModelExperiment3Service:
    """Train dataset-based model experiments and promote model packages."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        dataset_service: DatasetService | None = None,
    ) -> None:
        self.kernel = kernel_service or ResearchKernelService()
        self._datasets = dataset_service or DatasetService(kernel_service=self.kernel)

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train_experiment(
        self,
        *,
        name: str,
        dataset_id: str,
        model_type: str = "lightgbm",
        objective: str = "regression",
        model_params: dict[str, Any] | None = None,
        random_seed: int = 42,
        lifecycle_stage: str = "experiment",
    ) -> dict:
        if model_type not in _MODEL_REGISTRY:
            raise ValueError(f"Unsupported model_type {model_type!r}")
        if objective not in {"regression", "classification"}:
            raise ValueError("M6 supports objective regression or classification")

        dataset = self._datasets.get_dataset(dataset_id)
        if dataset.get("status") != "materialized" or not dataset.get("dataset_artifact_id"):
            raise ValueError(f"Dataset {dataset_id} must be materialized before model training")

        panel = self._load_dataset_panel(dataset)
        feature_names = self._feature_columns(panel)
        if not feature_names:
            raise ValueError("Dataset has no feature columns")
        X, y, meta = self._build_xy(panel, feature_names)
        splits = self._split_dataset(X, y, meta, dataset.get("split_policy") or {})
        X_train, y_train = splits["train"]
        X_valid, y_valid = splits["valid"]
        X_test, y_test = splits["test"]
        if X_train.empty:
            raise ValueError("Training split is empty")

        params = dict(model_params or {})
        params.setdefault("random_state", random_seed)
        model = _MODEL_REGISTRY[model_type](task=objective, params=params)
        fit_kwargs = {}
        if not X_valid.empty:
            fit_kwargs["eval_set"] = [(X_valid, y_valid)]
        model.fit(X_train, y_train, **fit_kwargs)

        preds_valid = model.predict(X_valid) if not X_valid.empty else pd.Series(dtype=float)
        preds_test = model.predict(X_test) if not X_test.empty else pd.Series(dtype=float)
        metrics = self._compute_metrics(
            objective=objective,
            y_train=y_train,
            y_valid=y_valid,
            y_test=y_test,
            preds_valid=preds_valid,
            preds_test=preds_test,
        )
        feature_importance = self._feature_importance(model)
        feature_schema = {
            "features": feature_names,
            "label": "label",
            "index": ["date", "asset_id"],
            "dataset_artifact_id": dataset["dataset_artifact_id"],
        }
        qa = self._build_training_qa(splits=splits, metrics=metrics)

        run = self.kernel.create_run(
            run_type="model_train_experiment",
            project_id=dataset["project_id"],
            market_profile_id=dataset["market_profile_id"],
            lifecycle_stage=lifecycle_stage,
            retention_class="standard",
            created_by="model_experiment_3",
            params={
                "dataset_id": dataset_id,
                "model_type": model_type,
                "objective": objective,
                "model_params": params,
                "random_seed": random_seed,
            },
            input_refs=[{"type": "dataset", "id": dataset_id}],
        )
        experiment_id = uuid.uuid4().hex[:12]
        self._insert_experiment(
            experiment_id=experiment_id,
            run=run,
            dataset=dataset,
            name=name,
            model_type=model_type,
            objective=objective,
            random_seed=random_seed,
            params=params,
            feature_schema=feature_schema,
            metrics=metrics,
            qa=qa,
            lifecycle_stage=lifecycle_stage,
            status="running",
        )

        model_artifact = self._create_model_artifact(
            run_id=run["id"],
            model=model,
            experiment_id=experiment_id,
            feature_schema=feature_schema,
            metrics=metrics,
            lifecycle_stage=lifecycle_stage,
        )
        predictions = self._prediction_frame(
            model=model,
            X=X,
            meta=meta,
            split_policy=dataset.get("split_policy") or {},
            feature_names=feature_names,
        )
        prediction_artifact = self.kernel.create_dataframe_artifact(
            run_id=run["id"],
            artifact_type="model_predictions",
            frame=predictions,
            lifecycle_stage=lifecycle_stage,
            retention_class="standard",
            metadata={"model_experiment_id": experiment_id, "dataset_id": dataset_id},
        )
        prediction_run = self._insert_prediction_run(
            run_id=run["id"],
            project_id=dataset["project_id"],
            market_profile_id=dataset["market_profile_id"],
            dataset_id=dataset_id,
            model_experiment_id=experiment_id,
            model_package_id=None,
            prediction_artifact_id=prediction_artifact["id"],
            profile=self._profile_predictions(predictions),
            status="completed",
        )
        self.kernel.add_lineage(
            from_type="dataset",
            from_id=dataset_id,
            to_type="artifact",
            to_id=model_artifact["id"],
            relation="model_training_input",
            metadata={"experiment_id": experiment_id},
        )
        self.kernel.add_lineage(
            from_type="model_experiment",
            from_id=experiment_id,
            to_type="artifact",
            to_id=model_artifact["id"],
            relation="model_file",
        )
        self.kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary=metrics,
            qa_summary=qa,
        )
        self._complete_experiment(
            experiment_id,
            status="completed",
            model_artifact_id=model_artifact["id"],
            prediction_run_id=prediction_run["id"],
            prediction_artifact_id=prediction_artifact["id"],
            metrics=metrics,
            qa=qa,
        )
        return {
            "run": self.kernel.get_run(run["id"]),
            "experiment": self.get_experiment(experiment_id),
            "model_artifact": model_artifact,
            "prediction_run": prediction_run,
            "prediction_artifact": prediction_artifact,
            "metrics": metrics,
            "feature_schema": feature_schema,
            "feature_importance": feature_importance,
            "qa": qa,
        }

    # ------------------------------------------------------------------
    # Promote and predict
    # ------------------------------------------------------------------

    def promote_experiment(
        self,
        experiment_id: str,
        *,
        package_name: str | None = None,
        approved_by: str = "system",
        rationale: str | None = None,
        lifecycle_stage: str = "candidate",
    ) -> dict:
        experiment = self.get_experiment(experiment_id)
        if experiment["status"] != "completed":
            raise ValueError(f"Experiment {experiment_id} is not completed")
        if not experiment.get("model_artifact_id"):
            raise ValueError(f"Experiment {experiment_id} has no model artifact")
        qa = experiment.get("qa_summary") or {}
        if qa.get("blocking"):
            raise ValueError("Experiment QA is blocking; cannot promote")

        package_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO model_packages
               (id, project_id, market_profile_id, name, source_experiment_id,
                model_artifact_id, feature_schema, prediction_contract, metrics,
                qa_summary, lifecycle_stage, status, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'candidate', ?, ?, ?)""",
            [
                package_id,
                experiment["project_id"],
                experiment["market_profile_id"],
                package_name or experiment["name"],
                experiment_id,
                experiment["model_artifact_id"],
                json.dumps(experiment.get("feature_schema") or {}, default=str),
                json.dumps({"input": "dataset_panel", "output": "prediction_by_asset_date"}, default=str),
                json.dumps(experiment.get("metrics") or {}, default=str),
                json.dumps(qa, default=str),
                lifecycle_stage,
                json.dumps({"approved_by": approved_by}, default=str),
                now,
                now,
            ],
        )
        promotion_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO promotion_records
               (id, project_id, source_type, source_id, target_type, target_id,
                decision, policy_snapshot, qa_summary, approved_by, rationale,
                created_at)
               VALUES (?, ?, 'model_experiment', ?, 'model_package', ?,
                       'promoted', ?, ?, ?, ?, ?)""",
            [
                promotion_id,
                experiment["project_id"],
                experiment_id,
                package_id,
                json.dumps({"policy": "M6_basic_manual_or_nonblocking_qa"}, default=str),
                json.dumps(qa, default=str),
                approved_by,
                rationale,
                now,
            ],
        )
        self.kernel.add_lineage(
            from_type="model_experiment",
            from_id=experiment_id,
            to_type="model_package",
            to_id=package_id,
            relation="promoted",
            metadata={"promotion_record_id": promotion_id},
        )
        return {
            "package": self.get_model_package(package_id),
            "promotion_record": self.get_promotion_record(promotion_id),
        }

    def predict_panel(self, *, model_package_id: str, dataset_id: str) -> dict:
        package = self.get_model_package(model_package_id)
        dataset = self._datasets.get_dataset(dataset_id)
        if dataset.get("status") != "materialized" or not dataset.get("dataset_artifact_id"):
            raise ValueError(f"Dataset {dataset_id} must be materialized before prediction")
        panel = self._load_dataset_panel(dataset)
        feature_schema = package.get("feature_schema") or {}
        feature_names = feature_schema.get("features") or self._feature_columns(panel)
        X, _, meta = self._build_xy(panel, feature_names)
        model = self._load_model(package["model_artifact_id"])
        predictions = self._prediction_frame(
            model=model,
            X=X,
            meta=meta,
            split_policy=dataset.get("split_policy") or {},
            feature_names=feature_names,
        )
        run = self.kernel.create_run(
            run_type="model_predict_panel",
            project_id=dataset["project_id"],
            market_profile_id=dataset["market_profile_id"],
            lifecycle_stage=package["lifecycle_stage"],
            retention_class="standard",
            created_by="model_experiment_3",
            params={"model_package_id": model_package_id, "dataset_id": dataset_id},
            input_refs=[
                {"type": "model_package", "id": model_package_id},
                {"type": "dataset", "id": dataset_id},
            ],
        )
        artifact = self.kernel.create_dataframe_artifact(
            run_id=run["id"],
            artifact_type="model_predictions",
            frame=predictions,
            lifecycle_stage=package["lifecycle_stage"],
            retention_class="standard",
            metadata={"model_package_id": model_package_id, "dataset_id": dataset_id},
        )
        profile = self._profile_predictions(predictions)
        prediction_run = self._insert_prediction_run(
            run_id=run["id"],
            project_id=dataset["project_id"],
            market_profile_id=dataset["market_profile_id"],
            dataset_id=dataset_id,
            model_experiment_id=None,
            model_package_id=model_package_id,
            prediction_artifact_id=artifact["id"],
            profile=profile,
            status="completed",
        )
        self.kernel.update_run_status(run["id"], status="completed", metrics_summary=profile)
        return {
            "run": self.kernel.get_run(run["id"]),
            "prediction_run": prediction_run,
            "prediction_artifact": artifact,
            "profile": profile,
        }

    # ------------------------------------------------------------------
    # Getters
    # ------------------------------------------------------------------

    def get_experiment(self, experiment_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id, model_spec_id,
                      dataset_id, name, model_type, objective, random_seed,
                      params, split_policy, feature_schema, metrics, qa_summary,
                      model_artifact_id, prediction_run_id,
                      prediction_artifact_id, status, lifecycle_stage,
                      created_at, completed_at
               FROM model_experiments
               WHERE id = ?""",
            [experiment_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"ModelExperiment {experiment_id} not found")
        return self._experiment_row(row)

    def list_experiments(self, *, dataset_id: str | None = None) -> list[dict]:
        query = """SELECT id, run_id, project_id, market_profile_id, model_spec_id,
                          dataset_id, name, model_type, objective, random_seed,
                          params, split_policy, feature_schema, metrics, qa_summary,
                          model_artifact_id, prediction_run_id,
                          prediction_artifact_id, status, lifecycle_stage,
                          created_at, completed_at
                   FROM model_experiments"""
        params: list[Any] = []
        if dataset_id:
            query += " WHERE dataset_id = ?"
            params.append(dataset_id)
        query += " ORDER BY created_at DESC"
        return [self._experiment_row(row) for row in get_connection().execute(query, params).fetchall()]

    def get_model_package(self, package_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name,
                      source_experiment_id, model_artifact_id, feature_schema,
                      prediction_contract, metrics, qa_summary, lifecycle_stage,
                      status, metadata, created_at, updated_at
               FROM model_packages
               WHERE id = ?""",
            [package_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"ModelPackage {package_id} not found")
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "source_experiment_id": row[4],
            "model_artifact_id": row[5],
            "feature_schema": _json(row[6], {}),
            "prediction_contract": _json(row[7], {}),
            "metrics": _json(row[8], {}),
            "qa_summary": _json(row[9], {}),
            "lifecycle_stage": row[10],
            "status": row[11],
            "metadata": _json(row[12], {}),
            "created_at": str(row[13]) if row[13] else None,
            "updated_at": str(row[14]) if row[14] else None,
        }

    def get_promotion_record(self, promotion_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, source_type, source_id, target_type,
                      target_id, decision, policy_snapshot, qa_summary,
                      approved_by, rationale, created_at
               FROM promotion_records
               WHERE id = ?""",
            [promotion_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"PromotionRecord {promotion_id} not found")
        return {
            "id": row[0],
            "project_id": row[1],
            "source_type": row[2],
            "source_id": row[3],
            "target_type": row[4],
            "target_id": row[5],
            "decision": row[6],
            "policy_snapshot": _json(row[7], {}),
            "qa_summary": _json(row[8], {}),
            "approved_by": row[9],
            "rationale": row[10],
            "created_at": str(row[11]) if row[11] else None,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _load_dataset_panel(self, dataset: dict) -> pd.DataFrame:
        artifact = self.kernel.get_artifact(dataset["dataset_artifact_id"])
        path = Path(artifact["uri"])
        if not path.exists():
            raise ValueError(f"Dataset artifact file not found: {path}")
        return pd.read_parquet(path)

    def _create_model_artifact(
        self,
        *,
        run_id: str,
        model,
        experiment_id: str,
        feature_schema: dict,
        metrics: dict,
        lifecycle_stage: str,
    ) -> dict:
        run = self.kernel.get_run(run_id)
        artifact_id = uuid.uuid4().hex[:12]
        model_dir = Path(self.kernel._artifact_root) / _stage_dir(lifecycle_stage) / run_id  # noqa: SLF001
        model_dir.mkdir(parents=True, exist_ok=True)
        path = model_dir / f"{artifact_id}.joblib"
        joblib.dump(
            {
                "model": model,
                "experiment_id": experiment_id,
                "feature_schema": feature_schema,
                "metrics": metrics,
            },
            path,
        )
        data = path.read_bytes()
        import hashlib

        get_connection().execute(
            """INSERT INTO artifacts
               (id, run_id, project_id, artifact_type, uri, format,
                schema_version, byte_size, content_hash, lifecycle_stage,
                retention_class, rebuildable, metadata, created_at)
               VALUES (?, ?, ?, 'model_file', ?, 'joblib', '1', ?, ?, ?,
                       'standard', FALSE, ?, ?)""",
            [
                artifact_id,
                run_id,
                run["project_id"],
                str(path),
                len(data),
                hashlib.sha256(data).hexdigest(),
                lifecycle_stage,
                json.dumps({"experiment_id": experiment_id}, default=str),
                utc_now_naive(),
            ],
        )
        self.kernel.add_lineage(
            from_type="research_run",
            from_id=run_id,
            to_type="artifact",
            to_id=artifact_id,
            relation="produced",
        )
        return self.kernel.get_artifact(artifact_id)

    def _load_model(self, artifact_id: str):
        artifact = self.kernel.get_artifact(artifact_id)
        payload = joblib.load(artifact["uri"])
        return payload["model"] if isinstance(payload, dict) and "model" in payload else payload

    @staticmethod
    def _feature_columns(panel: pd.DataFrame) -> list[str]:
        return [col for col in panel.columns if col not in _INDEX_COLUMNS]

    @staticmethod
    def _build_xy(
        panel: pd.DataFrame,
        feature_names: list[str],
    ) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
        frame = panel.copy()
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.dropna(subset=[*feature_names, "label"])
        index = pd.MultiIndex.from_frame(frame[["date", "asset_id"]])
        X = frame[feature_names].astype(float).copy()
        X.index = index
        y = frame["label"].astype(float).copy()
        y.index = index
        meta = frame[["date", "asset_id", "ticker"]].copy()
        meta.index = index
        return X, y, meta

    @staticmethod
    def _split_dataset(
        X: pd.DataFrame,
        y: pd.Series,
        meta: pd.DataFrame,
        split_policy: dict[str, Any],
    ) -> dict[str, tuple[pd.DataFrame, pd.Series]]:
        del meta
        dates = X.index.get_level_values("date")
        result = {}
        for name in ("train", "valid", "test"):
            raw = split_policy.get(name) or {}
            if raw.get("start") and raw.get("end"):
                start = pd.Timestamp(raw["start"])
                end = pd.Timestamp(raw["end"])
                mask = (dates >= start) & (dates <= end)
                result[name] = (X.loc[mask], y.loc[mask])
            else:
                result[name] = (pd.DataFrame(columns=X.columns), pd.Series(dtype=float))
        return result

    @staticmethod
    def _compute_metrics(
        *,
        objective: str,
        y_train: pd.Series,
        y_valid: pd.Series,
        y_test: pd.Series,
        preds_valid: pd.Series,
        preds_test: pd.Series,
    ) -> dict[str, Any]:
        metrics: dict[str, Any] = {
            "objective": objective,
            "train_samples": int(len(y_train)),
            "valid_samples": int(len(y_valid)),
            "test_samples": int(len(y_test)),
        }
        if objective == "regression":
            if len(y_valid) and len(preds_valid):
                metrics["valid_rmse"] = _rmse(y_valid, preds_valid)
                metrics["valid_ic"] = _ic(y_valid, preds_valid)
            if len(y_test) and len(preds_test):
                metrics["test_rmse"] = _rmse(y_test, preds_test)
                metrics["test_ic"] = _ic(y_test, preds_test)
                metrics["test_daily_ic"] = _daily_ic(y_test, preds_test)
        else:
            metrics["classification_note"] = "M6 basic classifier metrics are deferred"
        daily = metrics.get("test_daily_ic") or {}
        if daily:
            metrics["ic_mean"] = daily.get("mean_ic")
            metrics["ir"] = daily.get("ir")
        return metrics

    @staticmethod
    def _feature_importance(model) -> dict[str, float]:
        try:
            return {key: round(float(value), 6) for key, value in model.feature_importance().items()}
        except Exception:
            return {}

    @staticmethod
    def _build_training_qa(*, splits: dict, metrics: dict) -> dict:
        issues = []
        if len(splits["train"][0]) == 0:
            issues.append({"code": "empty_train_split", "severity": "error", "message": "Training split is empty"})
        if len(splits["test"][0]) == 0:
            issues.append({"code": "empty_test_split", "severity": "warning", "message": "Test split is empty"})
        if metrics.get("test_rmse") is None:
            issues.append({"code": "missing_test_metric", "severity": "warning", "message": "No test RMSE was computed"})
        return {
            "blocking": any(issue["severity"] == "error" for issue in issues),
            "issues": issues,
            "checks": {
                "train_samples": len(splits["train"][0]),
                "valid_samples": len(splits["valid"][0]),
                "test_samples": len(splits["test"][0]),
            },
        }

    @staticmethod
    def _prediction_frame(
        *,
        model,
        X: pd.DataFrame,
        meta: pd.DataFrame,
        split_policy: dict[str, Any],
        feature_names: list[str],
    ) -> pd.DataFrame:
        predictions = model.predict(X[feature_names])
        frame = meta.copy()
        frame["prediction"] = predictions.values
        frame["split"] = _split_names_for_dates(frame["date"], split_policy)
        frame["date"] = pd.to_datetime(frame["date"]).dt.date.astype(str)
        return frame.reset_index(drop=True).sort_values(["date", "asset_id"])

    @staticmethod
    def _profile_predictions(frame: pd.DataFrame) -> dict:
        values = pd.to_numeric(frame["prediction"], errors="coerce")
        return {
            "row_count": int(len(frame)),
            "asset_count": int(frame["asset_id"].nunique()) if not frame.empty else 0,
            "date_count": int(pd.to_datetime(frame["date"]).nunique()) if not frame.empty else 0,
            "mean": round(float(values.mean()), 6) if values.notna().any() else None,
            "std": round(float(values.std()), 6) if values.notna().sum() > 1 else 0.0,
        }

    def _insert_experiment(
        self,
        *,
        experiment_id: str,
        run: dict,
        dataset: dict,
        name: str,
        model_type: str,
        objective: str,
        random_seed: int,
        params: dict,
        feature_schema: dict,
        metrics: dict,
        qa: dict,
        lifecycle_stage: str,
        status: str,
    ) -> None:
        get_connection().execute(
            """INSERT INTO model_experiments
               (id, run_id, project_id, market_profile_id, dataset_id, name,
                model_type, objective, random_seed, params, split_policy,
                feature_schema, metrics, qa_summary, status, lifecycle_stage,
                created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                experiment_id,
                run["id"],
                dataset["project_id"],
                dataset["market_profile_id"],
                dataset["id"],
                name,
                model_type,
                objective,
                random_seed,
                json.dumps(params, default=str),
                json.dumps(dataset.get("split_policy") or {}, default=str),
                json.dumps(feature_schema, default=str),
                json.dumps(metrics, default=str),
                json.dumps(qa, default=str),
                status,
                lifecycle_stage,
                utc_now_naive(),
            ],
        )

    def _complete_experiment(
        self,
        experiment_id: str,
        *,
        status: str,
        model_artifact_id: str,
        prediction_run_id: str,
        prediction_artifact_id: str,
        metrics: dict,
        qa: dict,
    ) -> None:
        get_connection().execute(
            """UPDATE model_experiments
                  SET status = ?,
                      model_artifact_id = ?,
                      prediction_run_id = ?,
                      prediction_artifact_id = ?,
                      metrics = ?,
                      qa_summary = ?,
                      completed_at = ?
                WHERE id = ?""",
            [
                status,
                model_artifact_id,
                prediction_run_id,
                prediction_artifact_id,
                json.dumps(metrics, default=str),
                json.dumps(qa, default=str),
                utc_now_naive(),
                experiment_id,
            ],
        )

    def _insert_prediction_run(
        self,
        *,
        run_id: str,
        project_id: str,
        market_profile_id: str,
        dataset_id: str,
        model_experiment_id: str | None,
        model_package_id: str | None,
        prediction_artifact_id: str,
        profile: dict,
        status: str,
    ) -> dict:
        prediction_run_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO prediction_runs
               (id, run_id, project_id, market_profile_id, model_experiment_id,
                model_package_id, dataset_id, prediction_artifact_id, profile,
                status, created_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                prediction_run_id,
                run_id,
                project_id,
                market_profile_id,
                model_experiment_id,
                model_package_id,
                dataset_id,
                prediction_artifact_id,
                json.dumps(profile, default=str),
                status,
                utc_now_naive(),
                utc_now_naive(),
            ],
        )
        row = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id,
                      model_experiment_id, model_package_id, dataset_id,
                      prediction_artifact_id, profile, status, created_at,
                      completed_at
               FROM prediction_runs
               WHERE id = ?""",
            [prediction_run_id],
        ).fetchone()
        return self._prediction_run_row(row)

    @staticmethod
    def _experiment_row(row: tuple) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "market_profile_id": row[3],
            "model_spec_id": row[4],
            "dataset_id": row[5],
            "name": row[6],
            "model_type": row[7],
            "objective": row[8],
            "random_seed": row[9],
            "params": _json(row[10], {}),
            "split_policy": _json(row[11], {}),
            "feature_schema": _json(row[12], {}),
            "metrics": _json(row[13], {}),
            "qa_summary": _json(row[14], {}),
            "model_artifact_id": row[15],
            "prediction_run_id": row[16],
            "prediction_artifact_id": row[17],
            "status": row[18],
            "lifecycle_stage": row[19],
            "created_at": str(row[20]) if row[20] else None,
            "completed_at": str(row[21]) if row[21] else None,
        }

    @staticmethod
    def _prediction_run_row(row: tuple) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "market_profile_id": row[3],
            "model_experiment_id": row[4],
            "model_package_id": row[5],
            "dataset_id": row[6],
            "prediction_artifact_id": row[7],
            "profile": _json(row[8], {}),
            "status": row[9],
            "created_at": str(row[10]) if row[10] else None,
            "completed_at": str(row[11]) if row[11] else None,
        }


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _stage_dir(lifecycle_stage: str) -> str:
    return {
        "scratch": "scratch",
        "experiment": "experiments",
        "candidate": "candidates",
        "validated": "published",
        "published": "published",
        "archived": "archived",
    }.get(lifecycle_stage, "experiments")


def _rmse(y: pd.Series, preds: pd.Series) -> float:
    common = y.index.intersection(preds.index)
    return round(float(np.sqrt(np.mean((y.loc[common].values - preds.loc[common].values) ** 2))), 6)


def _ic(y: pd.Series, preds: pd.Series) -> float | None:
    common = y.index.intersection(preds.index)
    if len(common) < 2:
        return None
    corr = spearmanr(y.loc[common].values, preds.loc[common].values).correlation
    return round(float(corr), 6) if corr is not None and not np.isnan(corr) else None


def _daily_ic(y: pd.Series, preds: pd.Series) -> dict:
    if not isinstance(y.index, pd.MultiIndex):
        return {}
    values = []
    for date_value in y.index.get_level_values("date").unique():
        y_day = y.xs(date_value, level="date")
        p_day = preds.xs(date_value, level="date")
        common = y_day.index.intersection(p_day.index)
        if len(common) < 2:
            continue
        corr = spearmanr(y_day.loc[common], p_day.loc[common]).correlation
        if corr is not None and not np.isnan(corr):
            values.append(float(corr))
    if not values:
        return {}
    mean = float(np.mean(values))
    std = float(np.std(values)) if len(values) > 1 else 0.0
    return {
        "mean_ic": round(mean, 6),
        "std_ic": round(std, 6),
        "ir": round(mean / std, 6) if std > 0 else 0.0,
        "num_days": len(values),
    }


def _split_names_for_dates(dates: pd.Series, split_policy: dict[str, Any]) -> list[str]:
    result = []
    for value in pd.to_datetime(dates):
        name = "unknown"
        for split_name in ("train", "valid", "test"):
            raw = split_policy.get(split_name) or {}
            if raw.get("start") and raw.get("end"):
                if pd.Timestamp(raw["start"]) <= value <= pd.Timestamp(raw["end"]):
                    name = split_name
                    break
        result.append(name)
    return result
