"""Research Kernel service for QAgent 3.0.

The kernel owns the side-by-side 3.0 facts: projects, runs, artifacts, and
lineage. It intentionally stays small in M1 so domain services can adopt it
incrementally without coupling to strategy/model/data internals.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from shutil import move
from pathlib import Path
from typing import Any

from backend.config import settings
from backend.db import get_connection
from backend.time_utils import utc_now_naive


_LIFECYCLE_DIRS = {
    "scratch": "scratch",
    "experiment": "experiments",
    "candidate": "candidates",
    "validated": "published",
    "published": "published",
    "archived": "archived",
}


class ResearchKernelService:
    """Manage ResearchProject, ResearchRun, Artifact, and Lineage records."""

    def __init__(self, artifact_root: Path | None = None) -> None:
        self._artifact_root = artifact_root or (settings.project_root / "data" / "artifacts")

    # ------------------------------------------------------------------
    # Projects
    # ------------------------------------------------------------------

    def get_bootstrap_project(self) -> dict:
        """Return the bootstrap US project created during DB initialization."""
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, market_profile_id, default_universe_id,
                      data_policy_id, trading_rule_set_id, cost_model_id,
                      benchmark_policy_id, artifact_policy_id, metadata,
                      created_at, updated_at
               FROM research_projects
               WHERE id = 'bootstrap_us'"""
        ).fetchone()
        if row is None:
            conn.execute(
                """INSERT INTO research_projects
                   (id, name, market_profile_id, metadata, created_at, updated_at)
                   VALUES ('bootstrap_us', 'US Research', 'US_EQ', ?, ?, ?)""",
                [
                    json.dumps({"bootstrap": True, "phase": "M1"}),
                    utc_now_naive(),
                    utc_now_naive(),
                ],
            )
            row = conn.execute(
                """SELECT id, name, market_profile_id, default_universe_id,
                          data_policy_id, trading_rule_set_id, cost_model_id,
                          benchmark_policy_id, artifact_policy_id, metadata,
                          created_at, updated_at
                   FROM research_projects
                   WHERE id = 'bootstrap_us'"""
            ).fetchone()
        return self._project_row(row)

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def create_run(
        self,
        *,
        run_type: str,
        params: dict[str, Any] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        lifecycle_stage: str = "experiment",
        retention_class: str = "standard",
        created_by: str = "system",
        status: str = "queued",
        input_refs: list[dict[str, Any]] | None = None,
    ) -> dict:
        project = (
            self.get_project(project_id)
            if project_id
            else self.get_bootstrap_project()
        )
        run_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        conn = get_connection()
        conn.execute(
            """INSERT INTO research_runs
               (id, project_id, market_profile_id, run_type, status,
                lifecycle_stage, retention_class, params, input_refs,
                created_by, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                run_id,
                project["id"],
                market_profile_id or project["market_profile_id"],
                run_type,
                status,
                lifecycle_stage,
                retention_class,
                json.dumps(params or {}, default=str),
                json.dumps(input_refs or [], default=str),
                created_by,
                now,
                now,
            ],
        )
        return self.get_run(run_id)

    def get_project(self, project_id: str | None) -> dict:
        if not project_id:
            return self.get_bootstrap_project()
        row = get_connection().execute(
            """SELECT id, name, market_profile_id, default_universe_id,
                      data_policy_id, trading_rule_set_id, cost_model_id,
                      benchmark_policy_id, artifact_policy_id, metadata,
                      created_at, updated_at
               FROM research_projects
               WHERE id = ?""",
            [project_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Research project {project_id} not found")
        return self._project_row(row)

    def get_run(self, run_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, run_type, status,
                      lifecycle_stage, retention_class, params, input_refs,
                      output_refs, metrics_summary, qa_summary, warnings,
                      error_message, created_by, created_at, started_at,
                      completed_at, updated_at
               FROM research_runs
               WHERE id = ?""",
            [run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Research run {run_id} not found")
        return self._run_row(row)

    def list_runs(
        self,
        *,
        project_id: str | None = None,
        run_type: str | None = None,
        status: str | None = None,
        lifecycle_stage: str | None = None,
        created_by: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, project_id, market_profile_id, run_type, status,
                          lifecycle_stage, retention_class, params, input_refs,
                          output_refs, metrics_summary, qa_summary, warnings,
                          error_message, created_by, created_at, started_at,
                          completed_at, updated_at
                   FROM research_runs
                   WHERE 1 = 1"""
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if run_type:
            query += " AND run_type = ?"
            params.append(run_type)
        if status:
            query += " AND status = ?"
            params.append(status)
        if lifecycle_stage:
            query += " AND lifecycle_stage = ?"
            params.append(lifecycle_stage)
        if created_by:
            query += " AND created_by = ?"
            params.append(created_by)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._run_row(row) for row in rows]

    def update_run_status(
        self,
        run_id: str,
        *,
        status: str,
        metrics_summary: dict[str, Any] | None = None,
        qa_summary: dict[str, Any] | None = None,
        warnings: list[dict[str, Any]] | None = None,
        error_message: str | None = None,
    ) -> dict:
        completed_at = utc_now_naive() if status in {"completed", "failed", "timeout"} else None
        get_connection().execute(
            """UPDATE research_runs
                  SET status = ?,
                      metrics_summary = COALESCE(?, metrics_summary),
                      qa_summary = COALESCE(?, qa_summary),
                      warnings = COALESCE(?, warnings),
                      error_message = COALESCE(?, error_message),
                      completed_at = COALESCE(?, completed_at),
                      updated_at = ?
                WHERE id = ?""",
            [
                status,
                json.dumps(metrics_summary, default=str) if metrics_summary is not None else None,
                json.dumps(qa_summary, default=str) if qa_summary is not None else None,
                json.dumps(warnings, default=str) if warnings is not None else None,
                error_message,
                completed_at,
                utc_now_naive(),
                run_id,
            ],
        )
        return self.get_run(run_id)

    # ------------------------------------------------------------------
    # Artifacts and lineage
    # ------------------------------------------------------------------

    def create_json_artifact(
        self,
        *,
        run_id: str,
        artifact_type: str,
        payload: dict[str, Any],
        lifecycle_stage: str | None = None,
        retention_class: str = "standard",
        metadata: dict[str, Any] | None = None,
        rebuildable: bool = True,
    ) -> dict:
        run = self.get_run(run_id)
        stage = lifecycle_stage or run["lifecycle_stage"]
        artifact_id = uuid.uuid4().hex[:12]
        directory = self._artifact_dir(stage, run_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{artifact_id}.json"
        data = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        path.write_bytes(data)
        content_hash = hashlib.sha256(data).hexdigest()
        now = utc_now_naive()

        conn = get_connection()
        conn.execute(
            """INSERT INTO artifacts
               (id, run_id, project_id, artifact_type, uri, format,
                schema_version, byte_size, content_hash, lifecycle_stage,
                retention_class, rebuildable, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, 'json', '1', ?, ?, ?, ?, ?, ?, ?)""",
            [
                artifact_id,
                run_id,
                run["project_id"],
                artifact_type,
                str(path),
                len(data),
                content_hash,
                stage,
                retention_class,
                rebuildable,
                json.dumps(metadata or {}, default=str),
                now,
            ],
        )
        self.add_lineage(
            from_type="research_run",
            from_id=run_id,
            to_type="artifact",
            to_id=artifact_id,
            relation="produced",
        )
        self._append_run_output(run_id, {"type": "artifact", "id": artifact_id})
        return self.get_artifact(artifact_id)

    def create_dataframe_artifact(
        self,
        *,
        run_id: str,
        artifact_type: str,
        frame: Any,
        lifecycle_stage: str | None = None,
        retention_class: str = "standard",
        metadata: dict[str, Any] | None = None,
        rebuildable: bool = True,
        format: str = "parquet",
    ) -> dict:
        """Persist a tabular artifact under the 3.0 artifact store."""
        if format != "parquet":
            raise ValueError("Only parquet dataframe artifacts are supported")

        run = self.get_run(run_id)
        stage = lifecycle_stage or run["lifecycle_stage"]
        artifact_id = uuid.uuid4().hex[:12]
        directory = self._artifact_dir(stage, run_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{artifact_id}.parquet"
        frame.to_parquet(path, index=False)
        data = path.read_bytes()
        content_hash = hashlib.sha256(data).hexdigest()
        now = utc_now_naive()

        conn = get_connection()
        conn.execute(
            """INSERT INTO artifacts
               (id, run_id, project_id, artifact_type, uri, format,
                schema_version, byte_size, content_hash, lifecycle_stage,
                retention_class, rebuildable, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, '1', ?, ?, ?, ?, ?, ?, ?)""",
            [
                artifact_id,
                run_id,
                run["project_id"],
                artifact_type,
                str(path),
                format,
                len(data),
                content_hash,
                stage,
                retention_class,
                rebuildable,
                json.dumps(metadata or {}, default=str),
                now,
            ],
        )
        self.add_lineage(
            from_type="research_run",
            from_id=run_id,
            to_type="artifact",
            to_id=artifact_id,
            relation="produced",
        )
        self._append_run_output(run_id, {"type": "artifact", "id": artifact_id})
        return self.get_artifact(artifact_id)

    def get_artifact(self, artifact_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, run_id, project_id, artifact_type, uri, format,
                      schema_version, byte_size, content_hash, lifecycle_stage,
                      retention_class, cleanup_after, rebuildable, metadata,
                      created_at
               FROM artifacts
               WHERE id = ?""",
            [artifact_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Artifact {artifact_id} not found")
        return self._artifact_row(row)

    def list_artifacts(
        self,
        *,
        project_id: str | None = None,
        run_id: str | None = None,
        artifact_type: str | None = None,
        lifecycle_stage: str | None = None,
        retention_class: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, run_id, project_id, artifact_type, uri, format,
                          schema_version, byte_size, content_hash, lifecycle_stage,
                          retention_class, cleanup_after, rebuildable, metadata,
                          created_at
                   FROM artifacts
                   WHERE 1 = 1"""
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if run_id:
            query += " AND run_id = ?"
            params.append(run_id)
        if artifact_type:
            query += " AND artifact_type = ?"
            params.append(artifact_type)
        if lifecycle_stage:
            query += " AND lifecycle_stage = ?"
            params.append(lifecycle_stage)
        if retention_class:
            query += " AND retention_class = ?"
            params.append(retention_class)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._artifact_row(row) for row in rows]

    def archive_artifact(
        self,
        artifact_id: str,
        *,
        retention_class: str = "archived",
        archive_reason: str | None = None,
    ) -> dict:
        """Move an artifact into the archived lifecycle and update metadata."""
        artifact = self.get_artifact(artifact_id)
        if artifact["lifecycle_stage"] == "archived" and artifact["retention_class"] == retention_class:
            return artifact

        source_path = Path(artifact["uri"])
        if not source_path.exists():
            raise ValueError(f"Artifact file not found: {source_path}")

        archived_dir = self._artifact_dir("archived", artifact["run_id"])
        archived_dir.mkdir(parents=True, exist_ok=True)
        target_path = archived_dir / source_path.name
        if source_path.resolve() != target_path.resolve():
            if target_path.exists():
                target_path.unlink()
            move(str(source_path), str(target_path))

        metadata = dict(artifact.get("metadata") or {})
        metadata["archived_at"] = str(utc_now_naive())
        metadata["archive_previous_uri"] = artifact["uri"]
        if archive_reason:
            metadata["archive_reason"] = archive_reason

        get_connection().execute(
            """UPDATE artifacts
                  SET uri = ?,
                      lifecycle_stage = 'archived',
                      retention_class = ?,
                      metadata = ?
                WHERE id = ?""",
            [
                str(target_path),
                retention_class,
                json.dumps(metadata, default=str),
                artifact_id,
            ],
        )
        return self.get_artifact(artifact_id)

    def preview_artifact_cleanup(
        self,
        *,
        project_id: str | None = None,
        run_id: str | None = None,
        artifact_ids: list[str] | None = None,
        lifecycle_stage: str | None = None,
        retention_class: str | None = None,
        artifact_type: str | None = None,
        include_published: bool = False,
        limit: int = 500,
    ) -> dict:
        """Return cleanup impact without deleting local files or metadata."""
        ids = [item for item in (artifact_ids or []) if str(item).strip()]
        query = """SELECT id, run_id, project_id, artifact_type, uri, format,
                          schema_version, byte_size, content_hash, lifecycle_stage,
                          retention_class, cleanup_after, rebuildable, metadata,
                          created_at
                   FROM artifacts
                   WHERE 1 = 1"""
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if run_id:
            query += " AND run_id = ?"
            params.append(run_id)
        if ids:
            query += f" AND id IN ({', '.join(['?'] * len(ids))})"
            params.extend(ids)
        if lifecycle_stage:
            query += " AND lifecycle_stage = ?"
            params.append(lifecycle_stage)
        if retention_class:
            query += " AND retention_class = ?"
            params.append(retention_class)
        if artifact_type:
            query += " AND artifact_type = ?"
            params.append(artifact_type)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        artifacts = [self._artifact_row(row) for row in rows]
        candidates: list[dict] = []
        protected: list[dict] = []
        for artifact in artifacts:
            reasons = self._cleanup_protection_reasons(
                artifact,
                include_published=include_published,
            )
            if reasons:
                protected.append({"artifact": artifact, "reasons": reasons})
            else:
                candidates.append(artifact)

        candidate_bytes = sum(item["byte_size"] for item in candidates)
        protected_bytes = sum(item["artifact"]["byte_size"] for item in protected)
        return {
            "mode": "preview_only",
            "filters": {
                "project_id": project_id,
                "run_id": run_id,
                "artifact_ids": ids,
                "lifecycle_stage": lifecycle_stage,
                "retention_class": retention_class,
                "artifact_type": artifact_type,
                "include_published": include_published,
                "limit": int(limit),
            },
            "summary": {
                "matched_count": len(artifacts),
                "candidate_count": len(candidates),
                "protected_count": len(protected),
                "candidate_bytes": candidate_bytes,
                "protected_bytes": protected_bytes,
            },
            "candidates": candidates,
            "protected": protected,
            "warnings": [
                "Preview only; no files or metadata were deleted.",
                "Published, validated, standard-retention, and non-rebuildable artifacts are protected by default.",
            ],
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
        return self._promotion_record_row(row)

    def list_promotion_records(
        self,
        *,
        project_id: str | None = None,
        source_type: str | None = None,
        source_id: str | None = None,
        target_type: str | None = None,
        target_id: str | None = None,
        decision: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, project_id, source_type, source_id, target_type,
                          target_id, decision, policy_snapshot, qa_summary,
                          approved_by, rationale, created_at
                   FROM promotion_records
                   WHERE 1 = 1"""
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if source_type:
            query += " AND source_type = ?"
            params.append(source_type)
        if source_id:
            query += " AND source_id = ?"
            params.append(source_id)
        if target_type:
            query += " AND target_type = ?"
            params.append(target_type)
        if target_id:
            query += " AND target_id = ?"
            params.append(target_id)
        if decision:
            query += " AND decision = ?"
            params.append(decision)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._promotion_record_row(row) for row in rows]

    def add_lineage(
        self,
        *,
        from_type: str,
        from_id: str,
        to_type: str,
        to_id: str,
        relation: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        edge_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO lineage_edges
               (id, from_type, from_id, to_type, to_id, relation, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                edge_id,
                from_type,
                from_id,
                to_type,
                to_id,
                relation,
                json.dumps(metadata or {}, default=str),
                utc_now_naive(),
            ],
        )
        return {
            "id": edge_id,
            "from_type": from_type,
            "from_id": from_id,
            "to_type": to_type,
            "to_id": to_id,
            "relation": relation,
            "metadata": metadata or {},
        }

    def get_lineage(self, run_id: str) -> dict:
        rows = get_connection().execute(
            """SELECT id, from_type, from_id, to_type, to_id, relation, metadata, created_at
               FROM lineage_edges
               WHERE from_id = ? OR to_id = ?
               ORDER BY created_at""",
            [run_id, run_id],
        ).fetchall()
        return {"run_id": run_id, "edges": [self._lineage_row(row) for row in rows]}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _artifact_dir(self, lifecycle_stage: str, run_id: str) -> Path:
        stage_dir = _LIFECYCLE_DIRS.get(lifecycle_stage, "experiments")
        return self._artifact_root / stage_dir / run_id

    def _append_run_output(self, run_id: str, ref: dict[str, Any]) -> None:
        run = self.get_run(run_id)
        output_refs = list(run.get("output_refs") or [])
        output_refs.append(ref)
        get_connection().execute(
            """UPDATE research_runs
                  SET output_refs = ?, updated_at = ?
                WHERE id = ?""",
            [json.dumps(output_refs, default=str), utc_now_naive(), run_id],
        )

    @staticmethod
    def _cleanup_protection_reasons(artifact: dict, *, include_published: bool) -> list[str]:
        reasons: list[str] = []
        if not artifact["rebuildable"]:
            reasons.append("non_rebuildable")
        if artifact["retention_class"] == "standard":
            reasons.append("standard_retention")
        if artifact["lifecycle_stage"] in {"validated", "published"} and not include_published:
            reasons.append("published_or_validated")
        return reasons

    @staticmethod
    def _json(value: Any, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return default
        return value

    @classmethod
    def _project_row(cls, row: tuple) -> dict:
        return {
            "id": row[0],
            "name": row[1],
            "market_profile_id": row[2],
            "default_universe_id": row[3],
            "data_policy_id": row[4],
            "trading_rule_set_id": row[5],
            "cost_model_id": row[6],
            "benchmark_policy_id": row[7],
            "artifact_policy_id": row[8],
            "metadata": cls._json(row[9], {}),
            "created_at": str(row[10]) if row[10] else None,
            "updated_at": str(row[11]) if row[11] else None,
        }

    @classmethod
    def _run_row(cls, row: tuple) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "run_type": row[3],
            "status": row[4],
            "lifecycle_stage": row[5],
            "retention_class": row[6],
            "params": cls._json(row[7], {}),
            "input_refs": cls._json(row[8], []),
            "output_refs": cls._json(row[9], []),
            "metrics_summary": cls._json(row[10], {}),
            "qa_summary": cls._json(row[11], {}),
            "warnings": cls._json(row[12], []),
            "error_message": row[13],
            "created_by": row[14],
            "created_at": str(row[15]) if row[15] else None,
            "started_at": str(row[16]) if row[16] else None,
            "completed_at": str(row[17]) if row[17] else None,
            "updated_at": str(row[18]) if row[18] else None,
        }

    @classmethod
    def _artifact_row(cls, row: tuple) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "artifact_type": row[3],
            "uri": row[4],
            "format": row[5],
            "schema_version": row[6],
            "byte_size": int(row[7]),
            "content_hash": row[8],
            "lifecycle_stage": row[9],
            "retention_class": row[10],
            "cleanup_after": str(row[11]) if row[11] else None,
            "rebuildable": bool(row[12]),
            "metadata": cls._json(row[13], {}),
            "created_at": str(row[14]) if row[14] else None,
        }

    @classmethod
    def _promotion_record_row(cls, row: tuple) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "source_type": row[2],
            "source_id": row[3],
            "target_type": row[4],
            "target_id": row[5],
            "decision": row[6],
            "policy_snapshot": cls._json(row[7], {}),
            "qa_summary": cls._json(row[8], {}),
            "approved_by": row[9],
            "rationale": row[10],
            "created_at": str(row[11]) if row[11] else None,
        }

    @classmethod
    def _lineage_row(cls, row: tuple) -> dict:
        return {
            "id": row[0],
            "from_type": row[1],
            "from_id": row[2],
            "to_type": row[3],
            "to_id": row[4],
            "relation": row[5],
            "metadata": cls._json(row[6], {}),
            "created_at": str(row[7]) if row[7] else None,
        }
