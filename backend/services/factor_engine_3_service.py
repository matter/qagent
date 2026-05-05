"""QAgent 3.0 factor engine service.

M5 upgrades factors from legacy CRUD/cache objects into auditable
FactorSpec/FactorRun artifacts.  The first production slice supports
time-series ``FactorBase`` code, including legacy 2.0 factors via an adapter,
and persists official 3.0 factor values by ``asset_id``.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from backend.db import get_connection
from backend.factors.loader import load_factor_from_code
from backend.services.factor_engine import FactorEngine
from backend.services.factor_service import FactorService
from backend.services.label_service import LabelService
from backend.services.market_context import normalize_market
from backend.services.research_kernel_service import ResearchKernelService
from backend.services.universe_service import UniverseService
from backend.time_utils import utc_now_naive


_PROFILE_BY_MARKET = {"US": "US_EQ", "CN": "CN_A"}
_MARKET_BY_PROFILE = {"US_EQ": "US", "CN_A": "CN"}
_SUPPORTED_COMPUTE_MODES = {"time_series", "panel"}


class FactorEngine3Service:
    """Create, preview, materialize, and evaluate 3.0 factors."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        universe_service: UniverseService | None = None,
        factor_service: FactorService | None = None,
        factor_engine: FactorEngine | None = None,
        label_service: LabelService | None = None,
    ) -> None:
        self.kernel = kernel_service or ResearchKernelService()
        self._universe = universe_service or UniverseService(kernel_service=self.kernel)
        self._factor_service = factor_service or FactorService()
        self._legacy_engine = factor_engine or FactorEngine()
        self._label_service = label_service or LabelService()

    # ------------------------------------------------------------------
    # Specs
    # ------------------------------------------------------------------

    def create_spec_from_legacy_factor(
        self,
        *,
        legacy_factor_id: str,
        project_id: str | None = None,
        market: str | None = None,
        name: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        semantic_tags: list[str] | None = None,
        expected_warmup: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        resolved_market = normalize_market(market)
        legacy = self._factor_service.get_factor(legacy_factor_id, market=resolved_market)
        profile_id = _PROFILE_BY_MARKET[resolved_market]
        return self.create_python_spec(
            name=name or legacy["name"],
            source_code=legacy["source_code"],
            project_id=project_id,
            market_profile_id=profile_id,
            description=description or legacy.get("description"),
            source_type="legacy_factor",
            source_ref={
                "legacy_factor_id": legacy_factor_id,
                "legacy_market": resolved_market,
                "legacy_version": legacy["version"],
            },
            default_params=legacy.get("params") or {},
            compute_mode="time_series",
            expected_warmup=expected_warmup if expected_warmup is not None else _infer_warmup(legacy.get("params") or {}),
            semantic_tags=semantic_tags or [legacy.get("category") or "custom", "legacy_adapter"],
            lifecycle_stage=lifecycle_stage,
            metadata={**(metadata or {}), "legacy_factor": True},
        )

    def create_python_spec(
        self,
        *,
        name: str,
        source_code: str,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        source_type: str = "python_factor",
        source_ref: dict[str, Any] | None = None,
        params_schema: dict[str, Any] | None = None,
        default_params: dict[str, Any] | None = None,
        required_inputs: list[str] | None = None,
        compute_mode: str = "time_series",
        expected_warmup: int = 0,
        applicable_profiles: list[str] | None = None,
        semantic_tags: list[str] | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        if compute_mode not in _SUPPORTED_COMPUTE_MODES:
            raise ValueError(
                f"compute_mode {compute_mode!r} is not implemented in M5; "
                "cross_sectional is a planned skeleton for a later slice"
            )
        factor_instance = load_factor_from_code(source_code)
        project = self.kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        if profile_id not in _MARKET_BY_PROFILE:
            raise ValueError(f"Unsupported market profile {profile_id}")

        spec_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        default_params = default_params if default_params is not None else getattr(factor_instance, "params", {})
        description = description if description is not None else getattr(factor_instance, "description", "")
        tags = semantic_tags or [getattr(factor_instance, "category", "custom")]
        get_connection().execute(
            """INSERT INTO factor_specs
               (id, project_id, market_profile_id, name, description, version,
                source_type, source_ref, source_code, code_hash, params_schema,
                default_params, required_inputs, compute_mode, expected_warmup,
                applicable_profiles, semantic_tags, lifecycle_stage, status,
                metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                spec_id,
                project["id"],
                profile_id,
                name.strip(),
                description,
                source_type,
                json.dumps(source_ref or {}, default=str),
                source_code,
                hashlib.sha256(source_code.encode("utf-8")).hexdigest(),
                json.dumps(params_schema or {}, default=str),
                json.dumps(default_params or {}, default=str),
                json.dumps(required_inputs or ["open", "high", "low", "close", "volume"], default=str),
                compute_mode,
                int(expected_warmup or 0),
                json.dumps(applicable_profiles or [profile_id], default=str),
                json.dumps(tags, default=str),
                lifecycle_stage,
                status,
                json.dumps(metadata or {}, default=str),
                now,
                now,
            ],
        )
        return self.get_factor_spec(spec_id)

    def list_factor_specs(
        self,
        *,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        project = self.kernel.get_project(project_id) if project_id else None
        where = []
        params: list[Any] = []
        if project:
            where.append("project_id = ?")
            params.append(project["id"])
        if market_profile_id:
            where.append("market_profile_id = ?")
            params.append(market_profile_id)
        if status:
            where.append("status = ?")
            params.append(status)
        query = """SELECT id, project_id, market_profile_id, name, description,
                          version, source_type, source_ref, source_code,
                          code_hash, params_schema, default_params,
                          required_inputs, compute_mode, expected_warmup,
                          applicable_profiles, semantic_tags, lifecycle_stage,
                          status, metadata, created_at, updated_at
                   FROM factor_specs"""
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY created_at DESC, name"
        return [self._spec_row(row) for row in get_connection().execute(query, params).fetchall()]

    def get_factor_spec(self, factor_spec_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      version, source_type, source_ref, source_code,
                      code_hash, params_schema, default_params,
                      required_inputs, compute_mode, expected_warmup,
                      applicable_profiles, semantic_tags, lifecycle_stage,
                      status, metadata, created_at, updated_at
               FROM factor_specs
               WHERE id = ?""",
            [factor_spec_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"FactorSpec {factor_spec_id} not found")
        return self._spec_row(row)

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def preview_factor(
        self,
        *,
        factor_spec_id: str,
        universe_id: str,
        start_date: str,
        end_date: str,
        params: dict[str, Any] | None = None,
        limit: int = 5000,
    ) -> dict:
        return self._compute_factor(
            factor_spec_id=factor_spec_id,
            universe_id=universe_id,
            start_date=start_date,
            end_date=end_date,
            mode="preview",
            lifecycle_stage="scratch",
            retention_class="scratch",
            params=params,
            persist_values=False,
            row_limit=limit,
        )

    def materialize_factor(
        self,
        *,
        factor_spec_id: str,
        universe_id: str,
        start_date: str,
        end_date: str,
        params: dict[str, Any] | None = None,
        lifecycle_stage: str | None = None,
    ) -> dict:
        spec = self.get_factor_spec(factor_spec_id)
        return self._compute_factor(
            factor_spec_id=factor_spec_id,
            universe_id=universe_id,
            start_date=start_date,
            end_date=end_date,
            mode="materialize",
            lifecycle_stage=lifecycle_stage or spec["lifecycle_stage"],
            retention_class="rebuildable",
            params=params,
            persist_values=True,
            row_limit=None,
        )

    def evaluate_factor_run(
        self,
        *,
        factor_run_id: str,
        label_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        source_run = self.get_factor_run(factor_run_id)
        if not source_run.get("output_artifact_id"):
            raise ValueError(f"FactorRun {factor_run_id} has no output artifact")
        spec = self.get_factor_spec(source_run["factor_spec_id"])
        market = _market_for_profile(source_run["market_profile_id"])

        frame = self._load_factor_frame(source_run["output_artifact_id"])
        if start_date:
            frame = frame[pd.to_datetime(frame["date"]) >= pd.Timestamp(start_date)]
        if end_date:
            frame = frame[pd.to_datetime(frame["date"]) <= pd.Timestamp(end_date)]
        if frame.empty:
            raise ValueError("No factor values available for evaluation")

        symbol_by_asset = dict(zip(frame["asset_id"], frame["symbol"]))
        tickers = sorted(set(symbol_by_asset.values()))
        label_values = self._label_service.compute_label_values(
            label_id,
            tickers,
            start_date or source_run["start_date"],
            end_date or source_run["end_date"],
            market=market,
        )
        aligned = self._align_factor_and_label(frame, label_values)
        metrics, ic_series = self._evaluate_aligned(aligned)
        qa = self._build_eval_qa(aligned, metrics)

        run = self.kernel.create_run(
            run_type="factor_evaluate",
            project_id=source_run["project_id"],
            market_profile_id=source_run["market_profile_id"],
            lifecycle_stage=spec["lifecycle_stage"],
            retention_class="standard",
            created_by="factor_engine_3",
            params={
                "factor_run_id": factor_run_id,
                "factor_spec_id": spec["id"],
                "label_id": label_id,
                "start_date": start_date or source_run["start_date"],
                "end_date": end_date or source_run["end_date"],
            },
            input_refs=[
                {"type": "factor_run", "id": factor_run_id},
                {"type": "label", "id": label_id},
            ],
        )
        factor_eval_run = self._create_factor_run_record(
            run=run,
            spec=spec,
            universe_id=source_run["universe_id"],
            start_date=start_date or source_run["start_date"],
            end_date=end_date or source_run["end_date"],
            mode="evaluate",
            params={"source_factor_run_id": factor_run_id, "label_id": label_id},
            status="running",
        )
        payload = {
            "factor_run_id": factor_run_id,
            "factor_spec_id": spec["id"],
            "label_id": label_id,
            "metrics": metrics,
            "ic_series": ic_series,
            "qa": qa,
        }
        artifact = self.kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="factor_evaluation",
            payload=payload,
            lifecycle_stage=spec["lifecycle_stage"],
            retention_class="standard",
            metadata={"factor_run_id": factor_run_id, "label_id": label_id},
        )
        self.kernel.add_lineage(
            from_type="factor_run",
            from_id=factor_run_id,
            to_type="artifact",
            to_id=artifact["id"],
            relation="evaluated",
            metadata={"run_id": run["id"], "label_id": label_id},
        )
        self.kernel.add_lineage(
            from_type="research_run",
            from_id=run["id"],
            to_type="factor_run",
            to_id=factor_run_id,
            relation="evaluated",
            metadata={"label_id": label_id, "artifact_id": artifact["id"]},
        )
        self.kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary=metrics,
            qa_summary=qa,
        )
        self._complete_factor_run_record(
            factor_eval_run["id"],
            status="completed",
            artifact_id=artifact["id"],
            profile={"metrics": metrics, "ic_points": len(ic_series)},
            qa=qa,
        )
        return {
            "run": self.kernel.get_run(run["id"]),
            "factor_run": self.get_factor_run(factor_eval_run["id"]),
            "evaluation_artifact": artifact,
            "metrics": metrics,
            "ic_series": ic_series,
            "qa": qa,
        }

    def get_factor_run(self, factor_run_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id, factor_spec_id,
                      factor_spec_version, universe_id, start_date, end_date,
                      mode, status, params, data_snapshot_id, data_policy,
                      output_artifact_id, profile, qa_summary, created_at,
                      completed_at
               FROM factor_runs
               WHERE id = ?""",
            [factor_run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"FactorRun {factor_run_id} not found")
        return self._factor_run_row(row)

    def list_factor_runs(
        self,
        *,
        factor_spec_id: str | None = None,
        universe_id: str | None = None,
        mode: str | None = None,
    ) -> list[dict]:
        where = []
        params: list[Any] = []
        if factor_spec_id:
            where.append("factor_spec_id = ?")
            params.append(factor_spec_id)
        if universe_id:
            where.append("universe_id = ?")
            params.append(universe_id)
        if mode:
            where.append("mode = ?")
            params.append(mode)
        query = """SELECT id, run_id, project_id, market_profile_id, factor_spec_id,
                          factor_spec_version, universe_id, start_date, end_date,
                          mode, status, params, data_snapshot_id, data_policy,
                          output_artifact_id, profile, qa_summary, created_at,
                          completed_at
                   FROM factor_runs"""
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY created_at DESC"
        return [self._factor_run_row(row) for row in get_connection().execute(query, params).fetchall()]

    def sample_factor_run(self, factor_run_id: str, *, limit: int = 20, offset: int = 0) -> dict:
        run = self.get_factor_run(factor_run_id)
        if not run.get("output_artifact_id"):
            raise ValueError(f"FactorRun {factor_run_id} has no output artifact")
        frame = self._load_factor_frame(run["output_artifact_id"])
        rows = frame.sort_values(["date", "asset_id"]).iloc[offset : offset + limit]
        return {
            "factor_run_id": factor_run_id,
            "limit": limit,
            "offset": offset,
            "rows": _records(rows),
        }

    # ------------------------------------------------------------------
    # Compute internals
    # ------------------------------------------------------------------

    def _compute_factor(
        self,
        *,
        factor_spec_id: str,
        universe_id: str,
        start_date: str,
        end_date: str,
        mode: str,
        lifecycle_stage: str,
        retention_class: str,
        params: dict[str, Any] | None,
        persist_values: bool,
        row_limit: int | None,
    ) -> dict:
        spec = self.get_factor_spec(factor_spec_id)
        universe = self._universe.get_universe(universe_id)
        if universe["project_id"] != spec["project_id"]:
            raise ValueError("FactorSpec and Universe belong to different projects")
        if universe["market_profile_id"] != spec["market_profile_id"]:
            raise ValueError("FactorSpec and Universe use different market profiles")

        run = self.kernel.create_run(
            run_type=f"factor_{mode}",
            project_id=spec["project_id"],
            market_profile_id=spec["market_profile_id"],
            lifecycle_stage=lifecycle_stage,
            retention_class=retention_class,
            created_by="factor_engine_3",
            params={
                "factor_spec_id": factor_spec_id,
                "universe_id": universe_id,
                "start_date": start_date,
                "end_date": end_date,
                "params": params or {},
            },
            input_refs=[
                {"type": "factor_spec", "id": factor_spec_id},
                {"type": "universe", "id": universe_id},
            ],
        )
        factor_run = self._create_factor_run_record(
            run=run,
            spec=spec,
            universe_id=universe_id,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            params=params or {},
            status="running",
        )

        try:
            universe_run_id = self._universe.latest_materialized_run_id(universe_id)
            if not universe_run_id:
                self._universe.materialize_universe(
                    universe_id,
                    start_date=start_date,
                    end_date=end_date,
                    lifecycle_stage=lifecycle_stage,
                )
                universe_run_id = self._universe.latest_materialized_run_id(universe_id)
            universe_frame = self._load_universe_memberships(universe_id, universe_run_id)
            symbols = sorted(universe_frame["symbol"].dropna().unique().tolist())
            if not symbols:
                raise ValueError("Universe materialization has no mapped symbols")

            wide = self._compute_wide_factor(spec, symbols, start_date, end_date)
            panel = self._wide_to_asset_panel(wide, universe_frame, spec=spec, factor_run_id=factor_run["id"])
            if row_limit is not None:
                panel = panel.head(row_limit)
            if panel.empty:
                raise ValueError("Factor computation produced no aligned asset rows")

            profile = self._profile_panel(panel, spec=spec)
            artifact = self.kernel.create_dataframe_artifact(
                run_id=run["id"],
                artifact_type="factor_preview" if mode == "preview" else "factor_values",
                frame=panel,
                lifecycle_stage=lifecycle_stage,
                retention_class=retention_class,
                metadata={
                    "factor_spec_id": factor_spec_id,
                    "factor_run_id": factor_run["id"],
                    "universe_id": universe_id,
                    "mode": mode,
                },
            )
            if persist_values:
                self._persist_factor_values(factor_run_id=factor_run["id"], spec=spec, panel=panel)

            self.kernel.add_lineage(
                from_type="factor_spec",
                from_id=factor_spec_id,
                to_type="artifact",
                to_id=artifact["id"],
                relation=mode,
                metadata={"run_id": run["id"], "factor_run_id": factor_run["id"]},
            )
            self.kernel.add_lineage(
                from_type="universe",
                from_id=universe_id,
                to_type="artifact",
                to_id=artifact["id"],
                relation="factor_input",
                metadata={"run_id": run["id"], "factor_run_id": factor_run["id"]},
            )
            qa = self._build_compute_qa(panel, spec=spec)
            self._complete_factor_run_record(
                factor_run["id"],
                status="completed",
                artifact_id=artifact["id"],
                profile=profile,
                qa=qa,
            )
            self.kernel.update_run_status(
                run["id"],
                status="completed",
                metrics_summary={
                    "factor_spec_id": factor_spec_id,
                    "factor_run_id": factor_run["id"],
                    "artifact_id": artifact["id"],
                    "rows": profile["coverage"]["row_count"],
                    "asset_count": profile["coverage"]["asset_count"],
                    "date_count": profile["coverage"]["date_count"],
                },
                qa_summary=qa,
            )
            return {
                "spec": spec,
                "run": self.kernel.get_run(run["id"]),
                "factor_run": self.get_factor_run(factor_run["id"]),
                "artifact": artifact,
                "profile": profile,
                "qa": qa,
            }
        except Exception as exc:
            self._complete_factor_run_record(
                factor_run["id"],
                status="failed",
                artifact_id=None,
                profile={},
                qa={"blocking": True, "issues": [{"code": "factor_compute_failed", "message": str(exc)}]},
            )
            self.kernel.update_run_status(run["id"], status="failed", error_message=str(exc))
            raise

    def _compute_wide_factor(
        self,
        spec: dict,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        if spec["compute_mode"] not in _SUPPORTED_COMPUTE_MODES:
            raise ValueError(f"compute_mode {spec['compute_mode']} is not implemented")
        market = _market_for_profile(spec["market_profile_id"])
        source_ref = spec.get("source_ref") or {}
        legacy_factor_id = source_ref.get("legacy_factor_id")
        if spec["source_type"] == "legacy_factor" and legacy_factor_id:
            return self._legacy_engine.compute_factor(
                legacy_factor_id,
                symbols,
                start_date,
                end_date,
                market=market,
            )

        factor_instance = load_factor_from_code(spec["source_code"])
        from backend.services.factor_engine import FactorEngine as _FactorEngine

        return _FactorEngine()._compute_batch(  # noqa: SLF001 - M5 adapter reuse
            factor_instance,
            symbols,
            start_date,
            end_date,
            market=market,
        )

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
    def _wide_to_asset_panel(
        wide: pd.DataFrame,
        universe_frame: pd.DataFrame,
        *,
        spec: dict,
        factor_run_id: str,
    ) -> pd.DataFrame:
        if wide.empty:
            return pd.DataFrame()
        stacked = wide.stack().reset_index()
        stacked.columns = ["date", "symbol", "value"]
        stacked["date"] = pd.to_datetime(stacked["date"])
        universe = universe_frame.copy()
        universe["date"] = pd.to_datetime(universe["date"])
        panel = stacked.merge(
            universe[["date", "symbol", "asset_id", "membership_state", "available_at"]],
            on=["date", "symbol"],
            how="inner",
        )
        panel = panel.dropna(subset=["value"]).copy()
        if panel.empty:
            return pd.DataFrame()
        panel["factor_run_id"] = factor_run_id
        panel["factor_spec_id"] = spec["id"]
        panel["market_profile_id"] = spec["market_profile_id"]
        panel["value"] = panel["value"].astype(float)
        panel["date"] = pd.to_datetime(panel["date"]).dt.date.astype(str)
        panel["available_at"] = pd.to_datetime(panel["available_at"]).astype(str)
        ordered = [
            "date",
            "asset_id",
            "symbol",
            "value",
            "factor_spec_id",
            "factor_run_id",
            "market_profile_id",
            "membership_state",
            "available_at",
        ]
        return panel[ordered].sort_values(["date", "asset_id"]).reset_index(drop=True)

    @staticmethod
    def _profile_panel(panel: pd.DataFrame, *, spec: dict) -> dict:
        values = pd.to_numeric(panel["value"], errors="coerce")
        dates = pd.to_datetime(panel["date"])
        per_date = panel.groupby("date")["asset_id"].nunique()
        return {
            "factor_spec_id": spec["id"],
            "market_profile_id": spec["market_profile_id"],
            "coverage": {
                "row_count": int(len(panel)),
                "asset_count": int(panel["asset_id"].nunique()),
                "date_count": int(dates.nunique()),
                "date_range": {
                    "start": str(dates.min().date()) if len(dates) else None,
                    "end": str(dates.max().date()) if len(dates) else None,
                },
                "avg_assets_per_date": round(float(per_date.mean()), 4) if not per_date.empty else 0.0,
            },
            "value_distribution": {
                "missing_ratio": round(float(values.isna().mean()), 6),
                "mean": round(float(values.mean()), 6) if values.notna().any() else None,
                "std": round(float(values.std()), 6) if values.notna().sum() > 1 else 0.0,
                "min": round(float(values.min()), 6) if values.notna().any() else None,
                "max": round(float(values.max()), 6) if values.notna().any() else None,
            },
        }

    @staticmethod
    def _build_compute_qa(panel: pd.DataFrame, *, spec: dict) -> dict:
        row_count = len(panel)
        nan_ratio = float(pd.to_numeric(panel["value"], errors="coerce").isna().mean()) if row_count else 1.0
        issues = []
        if row_count == 0:
            issues.append({"code": "empty_factor_output", "severity": "error", "message": "Factor produced no rows"})
        if nan_ratio > 0.5:
            issues.append({"code": "high_missing_ratio", "severity": "warning", "message": "More than half of factor values are missing"})
        return {
            "blocking": any(issue["severity"] == "error" for issue in issues),
            "issues": issues,
            "checks": {
                "compute_mode": spec["compute_mode"],
                "expected_warmup": spec["expected_warmup"],
                "nan_ratio": round(nan_ratio, 6),
            },
        }

    @staticmethod
    def _build_eval_qa(aligned: pd.DataFrame, metrics: dict) -> dict:
        issues = []
        if aligned.empty:
            issues.append({"code": "empty_eval_alignment", "severity": "error", "message": "No aligned factor/label rows"})
        if metrics.get("ic_observations", 0) == 0:
            issues.append({"code": "no_ic_observations", "severity": "warning", "message": "No daily IC observations were computed"})
        return {
            "blocking": any(issue["severity"] == "error" for issue in issues),
            "issues": issues,
            "checks": {
                "aligned_rows": int(len(aligned)),
                "ic_observations": int(metrics.get("ic_observations", 0)),
            },
        }

    def _persist_factor_values(self, *, factor_run_id: str, spec: dict, panel: pd.DataFrame) -> None:
        values = panel[
            [
                "factor_run_id",
                "factor_spec_id",
                "market_profile_id",
                "date",
                "asset_id",
                "value",
                "available_at",
            ]
        ].copy()
        values["metadata"] = None
        conn = get_connection()
        conn.execute("DELETE FROM factor_values WHERE factor_run_id = ?", [factor_run_id])
        conn.register("_factor_values_3", values)
        try:
            conn.execute(
                """INSERT INTO factor_values
                   (factor_run_id, factor_spec_id, market_profile_id, date,
                    asset_id, value, available_at, metadata)
                   SELECT factor_run_id, factor_spec_id, market_profile_id,
                          date, asset_id, value, available_at, metadata
                   FROM _factor_values_3"""
            )
        finally:
            try:
                conn.unregister("_factor_values_3")
            except Exception:
                pass

    def _create_factor_run_record(
        self,
        *,
        run: dict,
        spec: dict,
        universe_id: str,
        start_date: str,
        end_date: str,
        mode: str,
        params: dict[str, Any],
        status: str,
    ) -> dict:
        factor_run_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO factor_runs
               (id, run_id, project_id, market_profile_id, factor_spec_id,
                factor_spec_version, universe_id, start_date, end_date,
                mode, status, params, data_policy, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                factor_run_id,
                run["id"],
                run["project_id"],
                run["market_profile_id"],
                spec["id"],
                spec["version"],
                universe_id,
                start_date,
                end_date,
                mode,
                status,
                json.dumps(params or {}, default=str),
                json.dumps({"market_profile_id": spec["market_profile_id"]}, default=str),
                utc_now_naive(),
            ],
        )
        return self.get_factor_run(factor_run_id)

    def _complete_factor_run_record(
        self,
        factor_run_id: str,
        *,
        status: str,
        artifact_id: str | None,
        profile: dict,
        qa: dict,
    ) -> None:
        get_connection().execute(
            """UPDATE factor_runs
                  SET status = ?,
                      output_artifact_id = COALESCE(?, output_artifact_id),
                      profile = ?,
                      qa_summary = ?,
                      completed_at = ?
                WHERE id = ?""",
            [
                status,
                artifact_id,
                json.dumps(profile or {}, default=str),
                json.dumps(qa or {}, default=str),
                utc_now_naive(),
                factor_run_id,
            ],
        )

    def _load_factor_frame(self, artifact_id: str) -> pd.DataFrame:
        artifact = self.kernel.get_artifact(artifact_id)
        path = Path(artifact["uri"])
        if not path.exists():
            raise ValueError(f"Factor artifact file not found: {path}")
        return pd.read_parquet(path)

    @staticmethod
    def _align_factor_and_label(factor_frame: pd.DataFrame, label_frame: pd.DataFrame) -> pd.DataFrame:
        if label_frame.empty:
            return pd.DataFrame(columns=["date", "asset_id", "symbol", "value", "label"])
        factors = factor_frame[["date", "asset_id", "symbol", "value"]].copy()
        factors["date"] = pd.to_datetime(factors["date"])
        labels = label_frame.rename(columns={"ticker": "symbol", "label_value": "label"}).copy()
        labels["date"] = pd.to_datetime(labels["date"])
        aligned = factors.merge(labels[["date", "symbol", "label"]], on=["date", "symbol"], how="inner")
        aligned = aligned.dropna(subset=["value", "label"])
        return aligned.sort_values(["date", "asset_id"]).reset_index(drop=True)

    @staticmethod
    def _evaluate_aligned(aligned: pd.DataFrame) -> tuple[dict, list[dict]]:
        ic_series: list[dict] = []
        if aligned.empty:
            return {
                "coverage": 0.0,
                "aligned_rows": 0,
                "ic_observations": 0,
                "ic_mean": 0.0,
                "ic_std": 0.0,
                "ir": 0.0,
                "rank_ic_win_rate": 0.0,
            }, ic_series

        total_rows = int(len(aligned))
        for date_value, group in aligned.groupby("date"):
            if len(group) < 2:
                continue
            ic = spearmanr(group["value"], group["label"], nan_policy="omit").correlation
            if ic is None or np.isnan(ic):
                continue
            ic_series.append({"date": str(pd.Timestamp(date_value).date()), "ic": round(float(ic), 6), "n": int(len(group))})

        ic_values = [item["ic"] for item in ic_series]
        if ic_values:
            ic_mean = float(np.mean(ic_values))
            ic_std = float(np.std(ic_values, ddof=1)) if len(ic_values) > 1 else 0.0
            ir = ic_mean / ic_std if ic_std > 0 else 0.0
            win_rate = float(np.mean([1.0 if value > 0 else 0.0 for value in ic_values]))
        else:
            ic_mean = ic_std = ir = win_rate = 0.0

        return {
            "coverage": 1.0,
            "aligned_rows": total_rows,
            "ic_observations": len(ic_series),
            "ic_mean": round(ic_mean, 6),
            "ic_std": round(ic_std, 6),
            "ir": round(ir, 6),
            "rank_ic_win_rate": round(win_rate, 6),
        }, ic_series

    # ------------------------------------------------------------------
    # Row mapping
    # ------------------------------------------------------------------

    @staticmethod
    def _spec_row(row: tuple) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "version": int(row[5]),
            "source_type": row[6],
            "source_ref": _json(row[7], {}),
            "source_code": row[8],
            "code_hash": row[9],
            "params_schema": _json(row[10], {}),
            "default_params": _json(row[11], {}),
            "required_inputs": _json(row[12], []),
            "compute_mode": row[13],
            "expected_warmup": int(row[14] or 0),
            "applicable_profiles": _json(row[15], []),
            "semantic_tags": _json(row[16], []),
            "lifecycle_stage": row[17],
            "status": row[18],
            "metadata": _json(row[19], {}),
            "created_at": str(row[20]) if row[20] else None,
            "updated_at": str(row[21]) if row[21] else None,
        }

    @staticmethod
    def _factor_run_row(row: tuple) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "market_profile_id": row[3],
            "factor_spec_id": row[4],
            "factor_spec_version": int(row[5]),
            "universe_id": row[6],
            "start_date": str(row[7]) if row[7] else None,
            "end_date": str(row[8]) if row[8] else None,
            "mode": row[9],
            "status": row[10],
            "params": _json(row[11], {}),
            "data_snapshot_id": row[12],
            "data_policy": _json(row[13], {}),
            "output_artifact_id": row[14],
            "profile": _json(row[15], {}),
            "qa_summary": _json(row[16], {}),
            "created_at": str(row[17]) if row[17] else None,
            "completed_at": str(row[18]) if row[18] else None,
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


def _market_for_profile(profile_id: str) -> str:
    market = _MARKET_BY_PROFILE.get(profile_id)
    if market is None:
        raise ValueError(f"Unsupported market profile {profile_id}")
    return market


def _infer_warmup(params: dict[str, Any]) -> int:
    candidates = [value for value in params.values() if isinstance(value, int)]
    return max(candidates) if candidates else 0


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records = []
    for item in frame.replace({np.nan: None}).to_dict(orient="records"):
        records.append({key: _scalar(value) for key, value in item.items()})
    return records


def _scalar(value: Any) -> Any:
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return str(value)
    return value
