"""Production signal, paper session, and reproducibility services for QAgent 3.0."""

from __future__ import annotations

import json
import uuid
from typing import Any

from backend.db import get_connection
from backend.services.research_kernel_service import ResearchKernelService
from backend.services.strategy_graph_3_service import StrategyGraph3Service
from backend.time_utils import utc_now_naive


_PUBLISHABLE_STAGES = {"validated", "published"}


class ProductionSignal3Service:
    """Generate official StrategyGraph signals and lightweight paper sessions."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        graph_service: StrategyGraph3Service | None = None,
    ) -> None:
        self.kernel = kernel_service or ResearchKernelService()
        self.graph_service = graph_service or StrategyGraph3Service(
            kernel_service=self.kernel
        )

    # ------------------------------------------------------------------
    # Production signal
    # ------------------------------------------------------------------

    def generate_production_signal(
        self,
        *,
        strategy_graph_id: str,
        decision_date: str,
        alpha_frame: list[dict[str, Any]] | None = None,
        legacy_signal_frame: list[dict[str, Any]] | None = None,
        current_weights: dict[str, float] | None = None,
        portfolio_value: float = 1_000_000,
        qa_report_id: str | None = None,
        approved_by: str = "system",
    ) -> dict:
        graph = self._require_publishable_graph(strategy_graph_id)
        run = self.kernel.create_run(
            run_type="production_signal_generate",
            project_id=graph["project_id"],
            market_profile_id=graph["market_profile_id"],
            lifecycle_stage="published",
            retention_class="standard",
            created_by="production_signal_3",
            params={
                "strategy_graph_id": strategy_graph_id,
                "decision_date": decision_date,
                "portfolio_value": portfolio_value,
                "qa_report_id": qa_report_id,
            },
            input_refs=[{"type": "strategy_graph", "id": strategy_graph_id}],
        )
        runtime = self.graph_service.simulate_day(
            strategy_graph_id,
            decision_date=decision_date,
            alpha_frame=alpha_frame,
            legacy_signal_frame=legacy_signal_frame,
            current_weights=current_weights,
            portfolio_value=portfolio_value,
            lifecycle_stage="published",
        )
        stage_explain = self.graph_service.explain_day(runtime["strategy_signal"]["id"])
        profile = {
            **(runtime.get("profile") or {}),
            "qa_report_id": qa_report_id,
            "approved_by": approved_by,
            "target_count": len(runtime.get("targets") or []),
            "order_intent_count": len(runtime.get("order_intents") or []),
        }
        signal = self._insert_production_signal_run(
            run_id=run["id"],
            graph=graph,
            strategy_signal_id=runtime["strategy_signal"]["id"],
            decision_date=decision_date,
            portfolio_run_id=runtime["portfolio_run"]["id"],
            target_artifact_id=runtime["portfolio_run"]["target_artifact_id"],
            order_intent_artifact_id=runtime["portfolio_run"]["order_intent_artifact_id"],
            qa_report_id=qa_report_id,
            approved_by=approved_by,
            profile=profile,
        )
        self.kernel.add_lineage(
            from_type="strategy_graph",
            from_id=strategy_graph_id,
            to_type="production_signal_run",
            to_id=signal["id"],
            relation="generated_production_signal",
            metadata={"decision_date": decision_date},
        )
        self.kernel.add_lineage(
            from_type="strategy_signal",
            from_id=runtime["strategy_signal"]["id"],
            to_type="production_signal_run",
            to_id=signal["id"],
            relation="promoted_to_production_signal",
        )
        self.kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary=profile,
            qa_summary={"blocking": False, "qa_report_id": qa_report_id},
        )
        return {
            "run": self.kernel.get_run(run["id"]),
            "strategy_graph": graph,
            "strategy_signal": runtime["strategy_signal"],
            "production_signal_run": signal,
            "stage_explain": stage_explain,
            "target_portfolio": runtime["targets"],
            "constraint_trace": runtime["constraint_trace"],
            "order_intents": runtime["order_intents"],
            "profile": profile,
        }

    def get_production_signal_run(self, signal_run_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id,
                      strategy_graph_id, strategy_signal_id, decision_date,
                      portfolio_run_id, target_artifact_id,
                      order_intent_artifact_id, qa_report_id, status,
                      lifecycle_stage, approved_by, profile, created_at,
                      completed_at
               FROM production_signal_runs
               WHERE id = ?""",
            [signal_run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Production signal run {signal_run_id} not found")
        return self._production_signal_row(row)

    def list_production_signal_runs(
        self,
        *,
        strategy_graph_id: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, run_id, project_id, market_profile_id,
                          strategy_graph_id, strategy_signal_id, decision_date,
                          portfolio_run_id, target_artifact_id,
                          order_intent_artifact_id, qa_report_id, status,
                          lifecycle_stage, approved_by, profile, created_at,
                          completed_at
                   FROM production_signal_runs
                   WHERE 1 = 1"""
        params: list[Any] = []
        if strategy_graph_id:
            query += " AND strategy_graph_id = ?"
            params.append(strategy_graph_id)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._production_signal_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Paper sessions
    # ------------------------------------------------------------------

    def create_paper_session(
        self,
        *,
        strategy_graph_id: str,
        start_date: str,
        name: str | None = None,
        initial_capital: float = 1_000_000,
        config: dict[str, Any] | None = None,
    ) -> dict:
        graph = self._require_publishable_graph(strategy_graph_id)
        session_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO paper_sessions
               (id, project_id, market_profile_id, strategy_graph_id, name,
                status, start_date, current_date, initial_capital, current_nav,
                current_weights, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?, NULL, ?, ?, ?, ?, ?, ?)""",
            [
                session_id,
                graph["project_id"],
                graph["market_profile_id"],
                graph["id"],
                name or f"{graph['name']} paper {start_date}",
                start_date,
                float(initial_capital),
                float(initial_capital),
                json.dumps({}, default=str),
                json.dumps(config or {}, default=str),
                now,
                now,
            ],
        )
        return self.get_paper_session(session_id)

    def get_paper_session(self, session_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, strategy_graph_id,
                      name, status, start_date, current_date, initial_capital,
                      current_nav, current_weights, config, created_at,
                      updated_at
               FROM paper_sessions
               WHERE id = ?""",
            [session_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Paper session {session_id} not found")
        return self._paper_session_row(row)

    def list_paper_sessions(self, *, status: str | None = None, limit: int = 50) -> list[dict]:
        query = """SELECT id, project_id, market_profile_id, strategy_graph_id,
                          name, status, start_date, current_date, initial_capital,
                          current_nav, current_weights, config, created_at,
                          updated_at
                   FROM paper_sessions
                   WHERE 1 = 1"""
        params: list[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._paper_session_row(row) for row in rows]

    def advance_paper_session(
        self,
        session_id: str,
        *,
        decision_date: str,
        alpha_frame: list[dict[str, Any]] | None = None,
        legacy_signal_frame: list[dict[str, Any]] | None = None,
    ) -> dict:
        session = self.get_paper_session(session_id)
        if session["status"] != "active":
            raise ValueError(f"Paper session is {session['status']}, not active")
        result = self.generate_production_signal(
            strategy_graph_id=session["strategy_graph_id"],
            decision_date=decision_date,
            alpha_frame=alpha_frame,
            legacy_signal_frame=legacy_signal_frame,
            current_weights=session.get("current_weights") or {},
            portfolio_value=session["current_nav"],
            approved_by="paper_session",
        )
        target_weights = {
            row["asset_id"]: float(row["target_weight"])
            for row in result["target_portfolio"]
            if float(row["target_weight"]) > 1e-10
        }
        turnover = sum(abs(float(order["delta_weight"])) for order in result["order_intents"])
        nav = float(session["current_nav"])
        diagnostics = {
            "execution_model": "target weights applied at next-open intent level",
            "turnover_estimate": round(turnover, 12),
            "order_intent_count": len(result["order_intents"]),
            "strategy_signal_id": result["strategy_signal"]["id"],
        }
        now = utc_now_naive()
        get_connection().execute(
            """INSERT OR REPLACE INTO paper_daily
               (session_id, date, nav, cash, current_weights,
                production_signal_run_id, diagnostics, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                session_id,
                decision_date,
                nav,
                0.0,
                json.dumps(target_weights, default=str),
                result["production_signal_run"]["id"],
                json.dumps(diagnostics, default=str),
                now,
            ],
        )
        get_connection().execute(
            """UPDATE paper_sessions
                  SET current_date = ?,
                      current_nav = ?,
                      current_weights = ?,
                      updated_at = ?
                WHERE id = ?""",
            [decision_date, nav, json.dumps(target_weights, default=str), now, session_id],
        )
        paper_daily = self.get_paper_daily(session_id, decision_date)
        return {
            "session": self.get_paper_session(session_id),
            "paper_daily": paper_daily,
            "production_signal_run": result["production_signal_run"],
            "strategy_signal": result["strategy_signal"],
            "target_portfolio": result["target_portfolio"],
            "order_intents": result["order_intents"],
            "days_processed": 1,
        }

    def get_paper_daily(self, session_id: str, date: str) -> dict:
        row = get_connection().execute(
            """SELECT session_id, date, nav, cash, current_weights,
                      production_signal_run_id, diagnostics, created_at
               FROM paper_daily
               WHERE session_id = ? AND date = ?""",
            [session_id, date],
        ).fetchone()
        if row is None:
            raise ValueError(f"Paper daily {session_id} {date} not found")
        return self._paper_daily_row(row)

    # ------------------------------------------------------------------
    # Reproducibility bundle
    # ------------------------------------------------------------------

    def export_reproducibility_bundle(
        self,
        *,
        source_type: str,
        source_id: str,
        name: str | None = None,
    ) -> dict:
        if source_type != "strategy_graph":
            raise ValueError("Only strategy_graph reproducibility bundles are supported")
        graph = self._require_publishable_graph(source_id)
        payload = {
            "source_type": source_type,
            "source_id": source_id,
            "strategy_graph": graph,
            "nodes": graph.get("nodes") or [],
            "dependency_refs": graph.get("dependency_refs") or [],
            "production_signal_runs": self.list_production_signal_runs(
                strategy_graph_id=source_id,
                limit=20,
            ),
        }
        run = self.kernel.create_run(
            run_type="reproducibility_bundle_export",
            project_id=graph["project_id"],
            market_profile_id=graph["market_profile_id"],
            lifecycle_stage="published",
            retention_class="standard",
            created_by="production_signal_3",
            params={"source_type": source_type, "source_id": source_id},
            input_refs=[{"type": source_type, "id": source_id}],
        )
        artifact = self.kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="reproducibility_bundle",
            payload=payload,
            lifecycle_stage="published",
            retention_class="standard",
            metadata={"source_type": source_type, "source_id": source_id},
            rebuildable=False,
        )
        bundle_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO reproducibility_bundles
               (id, project_id, market_profile_id, source_type, source_id,
                name, bundle_artifact_id, bundle_payload, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'created', ?)""",
            [
                bundle_id,
                graph["project_id"],
                graph["market_profile_id"],
                source_type,
                source_id,
                name or f"{graph['name']} reproducibility bundle",
                artifact["id"],
                json.dumps(payload, default=str),
                utc_now_naive(),
            ],
        )
        self.kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary={"bundle_artifact_id": artifact["id"]},
            qa_summary={"blocking": False},
        )
        self.kernel.add_lineage(
            from_type=source_type,
            from_id=source_id,
            to_type="reproducibility_bundle",
            to_id=bundle_id,
            relation="exported_bundle",
        )
        return self.get_reproducibility_bundle(bundle_id)

    def get_reproducibility_bundle(self, bundle_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, source_type, source_id,
                      name, bundle_artifact_id, bundle_payload, status, created_at
               FROM reproducibility_bundles
               WHERE id = ?""",
            [bundle_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Reproducibility bundle {bundle_id} not found")
        return self._bundle_row(row)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _require_publishable_graph(self, strategy_graph_id: str) -> dict:
        graph = self.graph_service.get_graph(strategy_graph_id)
        if graph["lifecycle_stage"] not in _PUBLISHABLE_STAGES:
            raise ValueError("Production signal requires a validated or published StrategyGraph")
        if graph["status"] not in {"active", "published", "validated"}:
            raise ValueError("Production signal requires an active StrategyGraph")
        return graph

    def _insert_production_signal_run(
        self,
        *,
        run_id: str,
        graph: dict,
        strategy_signal_id: str,
        decision_date: str,
        portfolio_run_id: str,
        target_artifact_id: str,
        order_intent_artifact_id: str,
        qa_report_id: str | None,
        approved_by: str,
        profile: dict,
    ) -> dict:
        signal_run_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO production_signal_runs
               (id, run_id, project_id, market_profile_id, strategy_graph_id,
                strategy_signal_id, decision_date, portfolio_run_id,
                target_artifact_id, order_intent_artifact_id, qa_report_id,
                status, lifecycle_stage, approved_by, profile, created_at,
                completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed',
                       'published', ?, ?, ?, ?)""",
            [
                signal_run_id,
                run_id,
                graph["project_id"],
                graph["market_profile_id"],
                graph["id"],
                strategy_signal_id,
                decision_date,
                portfolio_run_id,
                target_artifact_id,
                order_intent_artifact_id,
                qa_report_id,
                approved_by,
                json.dumps(profile, default=str),
                now,
                now,
            ],
        )
        return self.get_production_signal_run(signal_run_id)

    @staticmethod
    def _production_signal_row(row) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "market_profile_id": row[3],
            "strategy_graph_id": row[4],
            "strategy_signal_id": row[5],
            "decision_date": str(row[6]) if row[6] is not None else None,
            "portfolio_run_id": row[7],
            "target_artifact_id": row[8],
            "order_intent_artifact_id": row[9],
            "qa_report_id": row[10],
            "status": row[11],
            "lifecycle_stage": row[12],
            "approved_by": row[13],
            "profile": _json(row[14], {}),
            "created_at": str(row[15]) if row[15] is not None else None,
            "completed_at": str(row[16]) if row[16] is not None else None,
        }

    @staticmethod
    def _paper_session_row(row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "strategy_graph_id": row[3],
            "name": row[4],
            "status": row[5],
            "start_date": str(row[6]) if row[6] is not None else None,
            "current_date": str(row[7]) if row[7] is not None else None,
            "initial_capital": float(row[8]),
            "current_nav": float(row[9]),
            "current_weights": _json(row[10], {}),
            "config": _json(row[11], {}),
            "created_at": str(row[12]) if row[12] is not None else None,
            "updated_at": str(row[13]) if row[13] is not None else None,
        }

    @staticmethod
    def _paper_daily_row(row) -> dict:
        return {
            "session_id": row[0],
            "date": str(row[1]) if row[1] is not None else None,
            "nav": float(row[2]),
            "cash": float(row[3]),
            "current_weights": _json(row[4], {}),
            "production_signal_run_id": row[5],
            "diagnostics": _json(row[6], {}),
            "created_at": str(row[7]) if row[7] is not None else None,
        }

    @staticmethod
    def _bundle_row(row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "source_type": row[3],
            "source_id": row[4],
            "name": row[5],
            "bundle_artifact_id": row[6],
            "bundle_payload": _json(row[7], {}),
            "status": row[8],
            "created_at": str(row[9]) if row[9] is not None else None,
        }


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default
