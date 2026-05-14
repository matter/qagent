"""Dataset asset service for QAgent 3.0.

Datasets are the auditable bridge between factors/features and model
experiments.  M4 keeps computation compatible with legacy FeatureService and
LabelService, but persists the resulting sample panel as a reusable artifact.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.services.feature_service import FeatureService
from backend.services.label_service import LabelService
from backend.services.market_context import normalize_market
from backend.services.research_kernel_service import ResearchKernelService
from backend.services.universe_service import UniverseService
from backend.time_utils import utc_now_naive


class DatasetService:
    """Create, materialize, profile, sample, and query 3.0 datasets."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        universe_service: UniverseService | None = None,
        feature_service: FeatureService | None = None,
        label_service: LabelService | None = None,
    ) -> None:
        self._kernel = kernel_service or ResearchKernelService()
        self._universe = universe_service or UniverseService(kernel_service=self._kernel)
        self._feature_service = feature_service or FeatureService()
        self._label_service = label_service or LabelService()

    # ------------------------------------------------------------------
    # Pipeline/spec creation
    # ------------------------------------------------------------------

    def create_feature_pipeline_from_feature_set(
        self,
        *,
        feature_set_id: str,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        market: str | None = None,
        name: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
    ) -> dict:
        project = self._kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        resolved_market = normalize_market(market or _market_for_profile(profile_id))
        feature_set = self._feature_service.get_feature_set(feature_set_id, market=resolved_market)

        pipeline_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        conn = get_connection()
        conn.execute(
            """INSERT INTO feature_pipelines
               (id, project_id, market_profile_id, name, description,
                source_type, source_ref, preprocessing, lifecycle_stage,
                status, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'legacy_feature_set', ?, ?, ?,
                       'draft', ?, ?, ?)""",
            [
                pipeline_id,
                project["id"],
                profile_id,
                name or feature_set["name"],
                description or feature_set.get("description"),
                json.dumps({"feature_set_id": feature_set_id}, default=str),
                json.dumps(feature_set.get("preprocessing") or {}, default=str),
                lifecycle_stage,
                json.dumps({"legacy_feature_set": True}, default=str),
                now,
                now,
            ],
        )
        factor_refs = feature_set.get("factor_refs") or []
        for index, ref in enumerate(factor_refs):
            conn.execute(
                """INSERT INTO feature_pipeline_nodes
                   (id, feature_pipeline_id, node_order, node_type, name,
                    input_refs, params, created_at)
                   VALUES (?, ?, ?, 'raw_factor', ?, ?, ?, ?)""",
                [
                    uuid.uuid4().hex[:12],
                    pipeline_id,
                    index,
                    ref.get("factor_name") or ref.get("factor_id"),
                    json.dumps([ref], default=str),
                    json.dumps({"preprocessing": feature_set.get("preprocessing") or {}}, default=str),
                    now,
                ],
            )
        return self.get_feature_pipeline(pipeline_id)

    def create_label_spec_from_label(
        self,
        *,
        label_id: str,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        market: str | None = None,
        lifecycle_stage: str = "experiment",
    ) -> dict:
        project = self._kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        resolved_market = normalize_market(market or _market_for_profile(profile_id))
        label = self._label_service.get_label(label_id, market=resolved_market)
        label_spec_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO label_specs
               (id, project_id, market_profile_id, name, description,
                target_type, horizon, benchmark, source_type, source_ref,
                lifecycle_stage, status, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'legacy_label', ?, ?,
                       'draft', ?, ?, ?)""",
            [
                label_spec_id,
                project["id"],
                profile_id,
                label["name"],
                label.get("description"),
                label["target_type"],
                int(label["horizon"]),
                label.get("benchmark"),
                json.dumps({"label_id": label_id}, default=str),
                lifecycle_stage,
                json.dumps(
                    {
                        "legacy_label": True,
                        "config": label.get("config"),
                        "future_window_semantics": "forward trading-day horizon",
                    },
                    default=str,
                ),
                now,
                now,
            ],
        )
        return self.get_label_spec(label_spec_id)

    def create_dataset(
        self,
        *,
        name: str,
        universe_id: str,
        feature_pipeline_id: str | None = None,
        feature_set_id: str | None = None,
        label_spec_id: str | None = None,
        label_id: str | None = None,
        start_date: str,
        end_date: str,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        split_policy: dict[str, Any] | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        retention_class: str = "standard",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        universe = self._universe.get_universe(universe_id)
        project = self._kernel.get_project(project_id or universe["project_id"])
        profile_id = market_profile_id or universe["market_profile_id"]
        market = _market_for_profile(profile_id)

        if feature_pipeline_id is None:
            if not feature_set_id:
                raise ValueError("feature_pipeline_id or feature_set_id is required")
            feature_pipeline = self.create_feature_pipeline_from_feature_set(
                feature_set_id=feature_set_id,
                project_id=project["id"],
                market_profile_id=profile_id,
                market=market,
            )
            feature_pipeline_id = feature_pipeline["id"]
        else:
            feature_pipeline = self.get_feature_pipeline(feature_pipeline_id)
            feature_set_id = _json(feature_pipeline.get("source_ref"), {}).get("feature_set_id")

        if label_spec_id is None:
            if not label_id:
                raise ValueError("label_spec_id or label_id is required")
            label_spec = self.create_label_spec_from_label(
                label_id=label_id,
                project_id=project["id"],
                market_profile_id=profile_id,
                market=market,
            )
            label_spec_id = label_spec["id"]
        else:
            label_spec = self.get_label_spec(label_spec_id)
            label_id = _json(label_spec.get("source_ref"), {}).get("label_id")

        if not feature_set_id:
            raise ValueError("Only legacy feature_set backed pipelines are supported in M4")
        if not label_id:
            raise ValueError("Only legacy label backed label specs are supported in M4")

        dataset_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO datasets
               (id, project_id, market_profile_id, name, description,
                universe_id, feature_pipeline_id, label_spec_id, legacy_label_id,
                start_date, end_date, split_policy, lifecycle_stage,
                retention_class, status, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft',
                       ?, ?, ?)""",
            [
                dataset_id,
                project["id"],
                profile_id,
                name.strip(),
                description,
                universe_id,
                feature_pipeline_id,
                label_spec_id,
                label_id,
                start_date,
                end_date,
                json.dumps(split_policy or {}, default=str),
                lifecycle_stage,
                retention_class,
                json.dumps(metadata or {}, default=str),
                now,
                now,
            ],
        )
        return self.get_dataset(dataset_id)

    # ------------------------------------------------------------------
    # Materialization/query
    # ------------------------------------------------------------------

    def materialize_dataset(self, dataset_id: str, run_id: str | None = None) -> dict:
        dataset = self.get_dataset(dataset_id)
        market = _market_for_profile(dataset["market_profile_id"])
        qa = self._validate_dataset_qa(dataset)
        if qa["blocking"]:
            if run_id:
                self._kernel.update_run_status(
                    run_id,
                    status="failed",
                    qa_summary=qa,
                    error_message="; ".join(issue["message"] for issue in qa["issues"]),
                )
            raise ValueError("; ".join(issue["message"] for issue in qa["issues"]))

        if run_id:
            run = self._kernel.update_run_status(run_id, status="running", qa_summary=qa)
        else:
            run = self._kernel.create_run(
                run_type="dataset_materialize",
                project_id=dataset["project_id"],
                market_profile_id=dataset["market_profile_id"],
                lifecycle_stage=dataset["lifecycle_stage"],
                retention_class=dataset["retention_class"],
                created_by="dataset_service",
                params={
                    "dataset_id": dataset_id,
                    "start_date": dataset["start_date"],
                    "end_date": dataset["end_date"],
                    "universe_id": dataset["universe_id"],
                    "feature_pipeline_id": dataset["feature_pipeline_id"],
                    "label_spec_id": dataset["label_spec_id"],
                },
            )

        universe_run_id = self._universe.latest_materialized_run_id(dataset["universe_id"])
        if not universe_run_id:
            self._universe.materialize_universe(
                dataset["universe_id"],
                start_date=dataset["start_date"],
                end_date=dataset["end_date"],
                lifecycle_stage=dataset["lifecycle_stage"],
            )
            universe_run_id = self._universe.latest_materialized_run_id(dataset["universe_id"])

        universe_frame = self._load_universe_memberships(dataset["universe_id"], universe_run_id)
        tickers = sorted(universe_frame["symbol"].dropna().unique().tolist())
        if not tickers:
            raise ValueError("Dataset universe has no mapped symbols")

        pipeline = self.get_feature_pipeline(dataset["feature_pipeline_id"])
        feature_set_id = _json(pipeline["source_ref"], {}).get("feature_set_id")
        label_spec = self.get_label_spec(dataset["label_spec_id"])
        label_id = _json(label_spec["source_ref"], {}).get("label_id")
        if not feature_set_id or not label_id:
            raise ValueError("M4 datasets require legacy feature_set and label sources")

        feature_data = self._feature_service.compute_features_from_cache(
            feature_set_id,
            tickers,
            dataset["start_date"],
            dataset["end_date"],
            market=market,
        )
        label_df = self._label_service.compute_label_values_cached(
            label_id,
            tickers,
            dataset["start_date"],
            dataset["end_date"],
            market=market,
        )
        panel = self._build_panel(feature_data, label_df, universe_frame)
        if panel.empty:
            raise ValueError("Dataset materialization produced no aligned rows")

        profile = self._profile_panel(
            panel,
            dataset=dataset,
            feature_names=sorted(feature_data.keys()),
            label_spec=label_spec,
            qa=qa,
        )

        artifact = self._kernel.create_dataframe_artifact(
            run_id=run["id"],
            artifact_type="dataset_panel",
            frame=panel,
            lifecycle_stage=dataset["lifecycle_stage"],
            retention_class=dataset["retention_class"],
            metadata={
                "dataset_id": dataset_id,
                "feature_pipeline_id": dataset["feature_pipeline_id"],
                "label_spec_id": dataset["label_spec_id"],
            },
        )
        profile_artifact = self._kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="dataset_profile",
            payload=profile,
            lifecycle_stage=dataset["lifecycle_stage"],
            retention_class=dataset["retention_class"],
            metadata={"dataset_id": dataset_id},
            rebuildable=True,
        )
        self._kernel.add_lineage(
            from_type="universe",
            from_id=dataset["universe_id"],
            to_type="artifact",
            to_id=artifact["id"],
            relation="dataset_input",
        )
        self._kernel.add_lineage(
            from_type="feature_pipeline",
            from_id=dataset["feature_pipeline_id"],
            to_type="artifact",
            to_id=artifact["id"],
            relation="dataset_input",
        )
        self._kernel.add_lineage(
            from_type="label_spec",
            from_id=dataset["label_spec_id"],
            to_type="artifact",
            to_id=artifact["id"],
            relation="dataset_input",
        )

        conn = get_connection()
        conn.execute("DELETE FROM dataset_columns WHERE dataset_id = ?", [dataset_id])
        for ordinal, feature_name in enumerate(sorted(feature_data.keys())):
            conn.execute(
                """INSERT INTO dataset_columns
                   (dataset_id, column_name, role, dtype, ordinal, source_ref, metadata)
                   VALUES (?, ?, 'feature', 'double', ?, ?, NULL)""",
                [
                    dataset_id,
                    feature_name,
                    ordinal,
                    json.dumps({"feature_pipeline_id": dataset["feature_pipeline_id"]}, default=str),
                ],
            )
        conn.execute(
            """INSERT INTO dataset_columns
               (dataset_id, column_name, role, dtype, ordinal, source_ref, metadata)
               VALUES (?, 'label', 'label', 'double', ?, ?, NULL)""",
            [
                dataset_id,
                len(feature_data),
                json.dumps({"label_spec_id": dataset["label_spec_id"]}, default=str),
            ],
        )
        conn.execute(
            """INSERT OR REPLACE INTO dataset_profiles
               (dataset_id, run_id, profile, created_at)
               VALUES (?, ?, ?, ?)""",
            [dataset_id, run["id"], json.dumps(profile, default=str), utc_now_naive()],
        )
        conn.execute(
            """UPDATE datasets
                  SET status = 'materialized',
                      materialized_run_id = ?,
                      dataset_artifact_id = ?,
                      profile_artifact_id = ?,
                      row_count = ?,
                      feature_count = ?,
                      label_count = 1,
                      qa_summary = ?,
                      updated_at = ?
                WHERE id = ?""",
            [
                run["id"],
                artifact["id"],
                profile_artifact["id"],
                len(panel),
                len(feature_data),
                json.dumps(qa, default=str),
                utc_now_naive(),
                dataset_id,
            ],
        )
        self._kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary={
                "dataset_id": dataset_id,
                "rows": len(panel),
                "features": len(feature_data),
                "dataset_artifact_id": artifact["id"],
                "profile_artifact_id": profile_artifact["id"],
            },
            qa_summary=qa,
        )
        return {
            "dataset": self.get_dataset(dataset_id),
            "run": self._kernel.get_run(run["id"]),
            "artifact": artifact,
            "profile_artifact": profile_artifact,
            "profile": profile,
        }

    def profile_dataset(self, dataset_id: str) -> dict:
        dataset = self.get_dataset(dataset_id)
        row = get_connection().execute(
            """SELECT profile
               FROM dataset_profiles
               WHERE dataset_id = ?
               ORDER BY created_at DESC
               LIMIT 1""",
            [dataset_id],
        ).fetchone()
        if row:
            return _json(row[0], {})
        if dataset.get("dataset_artifact_id"):
            panel = self._load_dataset_panel(dataset)
            columns = [col for col in panel.columns if col not in _INDEX_COLUMNS | {"label"}]
            label_spec = self.get_label_spec(dataset["label_spec_id"])
            return self._profile_panel(panel, dataset=dataset, feature_names=columns, label_spec=label_spec, qa=dataset.get("qa_summary") or {})
        raise ValueError(f"Dataset {dataset_id} is not materialized")

    def sample_dataset(
        self,
        dataset_id: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> dict:
        panel = self._load_dataset_panel(self.get_dataset(dataset_id))
        rows = panel.sort_values(["date", "asset_id"]).iloc[offset : offset + limit]
        return {
            "dataset_id": dataset_id,
            "limit": limit,
            "offset": offset,
            "rows": self._records(rows),
        }

    def query_dataset(
        self,
        dataset_id: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        asset_ids: list[str] | None = None,
        columns: list[str] | None = None,
        limit: int = 1000,
    ) -> dict:
        panel = self._load_dataset_panel(self.get_dataset(dataset_id))
        if start_date:
            panel = panel[pd.to_datetime(panel["date"]) >= pd.Timestamp(start_date)]
        if end_date:
            panel = panel[pd.to_datetime(panel["date"]) <= pd.Timestamp(end_date)]
        if asset_ids:
            allowed = set(asset_ids)
            panel = panel[panel["asset_id"].isin(allowed)]
        if columns:
            keep = [col for col in [*_INDEX_COLUMNS, *columns] if col in panel.columns]
            panel = panel[keep]
        panel = panel.sort_values(["date", "asset_id"]).head(limit)
        return {"dataset_id": dataset_id, "rows": self._records(panel), "count": len(panel)}

    # ------------------------------------------------------------------
    # Getters
    # ------------------------------------------------------------------

    def get_feature_pipeline(self, feature_pipeline_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      source_type, source_ref, preprocessing, lifecycle_stage,
                      status, metadata, created_at, updated_at
               FROM feature_pipelines
               WHERE id = ?""",
            [feature_pipeline_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Feature pipeline {feature_pipeline_id} not found")
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "source_type": row[5],
            "source_ref": _json(row[6], {}),
            "preprocessing": _json(row[7], {}),
            "lifecycle_stage": row[8],
            "status": row[9],
            "metadata": _json(row[10], {}),
            "created_at": str(row[11]) if row[11] else None,
            "updated_at": str(row[12]) if row[12] else None,
        }

    def get_label_spec(self, label_spec_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      target_type, horizon, benchmark, source_type, source_ref,
                      lifecycle_stage, status, metadata, created_at, updated_at
               FROM label_specs
               WHERE id = ?""",
            [label_spec_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Label spec {label_spec_id} not found")
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "target_type": row[5],
            "horizon": int(row[6]),
            "benchmark": row[7],
            "source_type": row[8],
            "source_ref": _json(row[9], {}),
            "lifecycle_stage": row[10],
            "status": row[11],
            "metadata": _json(row[12], {}),
            "created_at": str(row[13]) if row[13] else None,
            "updated_at": str(row[14]) if row[14] else None,
        }

    def get_dataset(self, dataset_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      universe_id, feature_pipeline_id, label_spec_id,
                      legacy_label_id, start_date, end_date, split_policy,
                      lifecycle_stage, retention_class, status,
                      materialized_run_id, dataset_artifact_id,
                      profile_artifact_id, row_count, feature_count,
                      label_count, qa_summary, metadata, created_at, updated_at
               FROM datasets
               WHERE id = ?""",
            [dataset_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Dataset {dataset_id} not found")
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "universe_id": row[5],
            "feature_pipeline_id": row[6],
            "label_spec_id": row[7],
            "legacy_label_id": row[8],
            "start_date": str(row[9]),
            "end_date": str(row[10]),
            "split_policy": _json(row[11], {}),
            "lifecycle_stage": row[12],
            "retention_class": row[13],
            "status": row[14],
            "materialized_run_id": row[15],
            "dataset_artifact_id": row[16],
            "profile_artifact_id": row[17],
            "row_count": int(row[18] or 0),
            "feature_count": int(row[19] or 0),
            "label_count": int(row[20] or 0),
            "qa_summary": _json(row[21], {}),
            "metadata": _json(row[22], {}),
            "created_at": str(row[23]) if row[23] else None,
            "updated_at": str(row[24]) if row[24] else None,
        }

    def list_datasets(
        self,
        *,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        universe_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, project_id, market_profile_id, name, description,
                          universe_id, feature_pipeline_id, label_spec_id,
                          legacy_label_id, start_date, end_date, split_policy,
                          lifecycle_stage, retention_class, status,
                          materialized_run_id, dataset_artifact_id,
                          profile_artifact_id, row_count, feature_count,
                          label_count, qa_summary, metadata, created_at, updated_at
                   FROM datasets
                   WHERE 1 = 1"""
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if market_profile_id:
            query += " AND market_profile_id = ?"
            params.append(market_profile_id)
        if universe_id:
            query += " AND universe_id = ?"
            params.append(universe_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self.get_dataset(row[0]) for row in rows]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _validate_dataset_qa(self, dataset: dict) -> dict:
        label_spec = self.get_label_spec(dataset["label_spec_id"])
        horizon = int(label_spec["horizon"])
        split_policy = dataset.get("split_policy") or {}
        purge_gap = int(split_policy.get("purge_gap", 0) or 0)
        issues: list[dict[str, Any]] = []
        if purge_gap < horizon:
            issues.append(
                {
                    "code": "purge_gap_lt_label_horizon",
                    "severity": "error",
                    "message": (
                        f"split_policy.purge_gap={purge_gap} is smaller than "
                        f"label horizon={horizon}; increase purge_gap to avoid leakage"
                    ),
                }
            )

        ranges = _split_ranges(split_policy)
        if ranges:
            ordered = [item for item in ("train", "valid", "test") if item in ranges]
            for left, right in zip(ordered, ordered[1:]):
                if ranges[left][1] >= ranges[right][0]:
                    issues.append(
                        {
                            "code": "split_overlap",
                            "severity": "error",
                            "message": f"{left} period overlaps {right} period",
                        }
                    )
        return {
            "blocking": any(issue["severity"] == "error" for issue in issues),
            "issues": issues,
            "checks": {
                "purge_gap": purge_gap,
                "label_horizon": horizon,
                "split_ranges": {k: [str(v[0].date()), str(v[1].date())] for k, v in ranges.items()},
            },
        }

    def _load_universe_memberships(self, universe_id: str, run_id: str | None) -> pd.DataFrame:
        query = """SELECT m.date, m.asset_id, a.symbol, m.membership_state, m.available_at
                   FROM universe_memberships m
                   JOIN assets a ON a.asset_id = m.asset_id
                   WHERE m.universe_id = ?"""
        params: list[Any] = [universe_id]
        if run_id:
            query += " AND m.run_id = ?"
            params.append(run_id)
        query += " ORDER BY m.date, m.asset_id"
        return get_connection().execute(query, params).fetchdf()

    @staticmethod
    def _build_panel(
        feature_data: dict[str, pd.DataFrame],
        label_df: pd.DataFrame,
        universe_frame: pd.DataFrame,
    ) -> pd.DataFrame:
        feature_frames: list[pd.Series] = []
        for feature_name in sorted(feature_data.keys()):
            feature = feature_data[feature_name]
            if feature.empty:
                continue
            stacked = feature.stack()
            stacked.name = feature_name
            stacked.index.names = ["date", "symbol"]
            feature_frames.append(stacked)
        if not feature_frames:
            return pd.DataFrame()

        X = pd.concat(feature_frames, axis=1)
        labels = label_df.copy()
        labels["date"] = pd.to_datetime(labels["date"])
        labels = labels.rename(columns={"ticker": "symbol", "label_value": "label"})
        y = labels.set_index(["date", "symbol"])["label"]
        common = X.index.intersection(y.index)
        X = X.loc[common]
        y = y.loc[common]
        frame = X.copy()
        frame["label"] = y
        frame = frame.dropna()
        if frame.empty:
            return pd.DataFrame()

        panel = frame.reset_index()
        universe = universe_frame.copy()
        universe["date"] = pd.to_datetime(universe["date"])
        panel = panel.merge(
            universe[["date", "symbol", "asset_id", "membership_state", "available_at"]],
            on=["date", "symbol"],
            how="inner",
        )
        panel = panel.rename(columns={"symbol": "ticker"})
        panel["date"] = pd.to_datetime(panel["date"]).dt.date.astype(str)
        panel["available_at"] = pd.to_datetime(panel["available_at"]).astype(str)
        ordered = ["date", "asset_id", "ticker", "membership_state", "available_at"]
        feature_cols = [col for col in sorted(feature_data.keys()) if col in panel.columns]
        return panel[ordered + feature_cols + ["label"]].sort_values(["date", "asset_id"]).reset_index(drop=True)

    @staticmethod
    def _profile_panel(
        panel: pd.DataFrame,
        *,
        dataset: dict,
        feature_names: list[str],
        label_spec: dict,
        qa: dict,
    ) -> dict:
        feature_missing: dict[str, float] = {}
        for name in feature_names:
            if name in panel.columns:
                feature_missing[name] = round(float(panel[name].isna().mean()), 6)
        labels = pd.to_numeric(panel["label"], errors="coerce")
        dates = pd.to_datetime(panel["date"])
        return {
            "dataset_id": dataset["id"],
            "project_id": dataset["project_id"],
            "market_profile_id": dataset["market_profile_id"],
            "coverage": {
                "row_count": int(len(panel)),
                "asset_count": int(panel["asset_id"].nunique()),
                "date_count": int(dates.nunique()),
                "date_range": {
                    "start": str(dates.min().date()) if len(dates) else None,
                    "end": str(dates.max().date()) if len(dates) else None,
                },
                "rows_per_date": {
                    "min": int(panel.groupby("date")["asset_id"].nunique().min()),
                    "max": int(panel.groupby("date")["asset_id"].nunique().max()),
                    "mean": round(float(panel.groupby("date")["asset_id"].nunique().mean()), 4),
                },
            },
            "feature_count": len(feature_names),
            "feature_missing": feature_missing,
            "label": {
                "label_spec_id": label_spec["id"],
                "target_type": label_spec["target_type"],
                "horizon": label_spec["horizon"],
                "missing_ratio": round(float(labels.isna().mean()), 6),
                "mean": round(float(labels.mean()), 6) if labels.notna().any() else None,
                "std": round(float(labels.std()), 6) if labels.notna().sum() > 1 else 0.0,
                "min": round(float(labels.min()), 6) if labels.notna().any() else None,
                "max": round(float(labels.max()), 6) if labels.notna().any() else None,
                "quantiles": {
                    "p05": round(float(labels.quantile(0.05)), 6) if labels.notna().any() else None,
                    "p50": round(float(labels.quantile(0.50)), 6) if labels.notna().any() else None,
                    "p95": round(float(labels.quantile(0.95)), 6) if labels.notna().any() else None,
                },
            },
            "qa": qa,
        }

    def _load_dataset_panel(self, dataset: dict) -> pd.DataFrame:
        artifact_id = dataset.get("dataset_artifact_id")
        if not artifact_id:
            raise ValueError(f"Dataset {dataset['id']} is not materialized")
        artifact = self._kernel.get_artifact(artifact_id)
        path = Path(artifact["uri"])
        if not path.exists():
            raise ValueError(f"Dataset artifact file not found: {path}")
        return pd.read_parquet(path)

    @staticmethod
    def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
        records = []
        for item in frame.replace({np.nan: None}).to_dict(orient="records"):
            records.append({key: _scalar(value) for key, value in item.items()})
        return records


_INDEX_COLUMNS = {"date", "asset_id", "ticker", "membership_state", "available_at"}


def _json(value: Any, default: Any = None) -> Any:
    if default is None:
        default = {}
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _scalar(value: Any) -> Any:
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return str(value)
    return value


def _market_for_profile(profile_id: str) -> str:
    if profile_id == "US_EQ":
        return "US"
    if profile_id == "CN_A":
        return "CN"
    raise ValueError(f"Unsupported market profile {profile_id}")


def _split_ranges(split_policy: dict[str, Any]) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    ranges: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for name in ("train", "valid", "test"):
        raw = split_policy.get(name)
        if isinstance(raw, dict) and raw.get("start") and raw.get("end"):
            ranges[name] = (pd.Timestamp(raw["start"]), pd.Timestamp(raw["end"]))
    return ranges
