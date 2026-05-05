"""StrategyGraph runtime service for QAgent 3.0.

M8 introduces graph-defined strategy assets and a single-day runtime that
produces alpha, selection, portfolio target, constraint trace, and order intent
from one service path.  Backtest/signal/paper can migrate onto this runtime
without copying portfolio or execution logic.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from backend.db import get_connection
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
from backend.services.research_kernel_service import ResearchKernelService
from backend.time_utils import utc_now_naive


class StrategyGraph3Service:
    """Create StrategyGraph assets and run single-day explanations."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        portfolio_service: PortfolioAssets3Service | None = None,
    ) -> None:
        self.kernel = kernel_service or ResearchKernelService()
        self.portfolio_service = portfolio_service or PortfolioAssets3Service(
            kernel_service=self.kernel
        )

    # ------------------------------------------------------------------
    # Graph creation
    # ------------------------------------------------------------------

    def create_builtin_alpha_graph(
        self,
        *,
        name: str,
        selection_policy: dict[str, Any] | None,
        portfolio_construction_spec_id: str,
        risk_control_spec_id: str | None = None,
        rebalance_policy_spec_id: str | None = None,
        execution_policy_spec_id: str | None = None,
        state_policy_spec_id: str | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        portfolio_spec = self.portfolio_service.get_portfolio_construction_spec(
            portfolio_construction_spec_id
        )
        project = self.kernel.get_project(project_id or portfolio_spec["project_id"])
        profile_id = market_profile_id or portfolio_spec["market_profile_id"]
        graph_config = {
            "selection_policy": selection_policy or {"top_n": 50, "score_column": "score"},
            "portfolio_construction_spec_id": portfolio_construction_spec_id,
            "risk_control_spec_id": risk_control_spec_id,
            "rebalance_policy_spec_id": rebalance_policy_spec_id,
            "execution_policy_spec_id": execution_policy_spec_id,
            "state_policy_spec_id": state_policy_spec_id,
        }
        dependency_refs = self._dependency_refs(graph_config)
        graph = self._insert_graph(
            name=name,
            graph_type="builtin_alpha_graph",
            project_id=project["id"],
            market_profile_id=profile_id,
            description=description,
            graph_config=graph_config,
            dependency_refs=dependency_refs,
            lifecycle_stage=lifecycle_stage,
            status=status,
            metadata=metadata,
        )
        self._insert_standard_nodes(graph["id"], graph_config, legacy=False)
        return self.get_graph(graph["id"])

    def create_legacy_strategy_adapter_graph(
        self,
        *,
        name: str,
        legacy_strategy_id: str,
        portfolio_construction_spec_id: str,
        risk_control_spec_id: str | None = None,
        rebalance_policy_spec_id: str | None = None,
        execution_policy_spec_id: str | None = None,
        state_policy_spec_id: str | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        portfolio_spec = self.portfolio_service.get_portfolio_construction_spec(
            portfolio_construction_spec_id
        )
        project = self.kernel.get_project(project_id or portfolio_spec["project_id"])
        profile_id = market_profile_id or portfolio_spec["market_profile_id"]
        graph_config = {
            "legacy_strategy_id": legacy_strategy_id,
            "selection_policy": {"top_n": 10, "score_column": "score"},
            "portfolio_construction_spec_id": portfolio_construction_spec_id,
            "risk_control_spec_id": risk_control_spec_id,
            "rebalance_policy_spec_id": rebalance_policy_spec_id,
            "execution_policy_spec_id": execution_policy_spec_id,
            "state_policy_spec_id": state_policy_spec_id,
        }
        dependency_refs = [
            {"type": "legacy_strategy", "id": legacy_strategy_id},
            *self._dependency_refs(graph_config),
        ]
        graph = self._insert_graph(
            name=name,
            graph_type="legacy_strategy_adapter",
            project_id=project["id"],
            market_profile_id=profile_id,
            description=description,
            graph_config=graph_config,
            dependency_refs=dependency_refs,
            lifecycle_stage=lifecycle_stage,
            status=status,
            metadata={**(metadata or {}), "legacy_adapter": True},
        )
        self._insert_standard_nodes(graph["id"], graph_config, legacy=True)
        return self.get_graph(graph["id"])

    # ------------------------------------------------------------------
    # Runtime
    # ------------------------------------------------------------------

    def simulate_day(
        self,
        strategy_graph_id: str,
        *,
        decision_date: str,
        alpha_frame: list[dict[str, Any]] | None = None,
        legacy_signal_frame: list[dict[str, Any]] | None = None,
        current_weights: dict[str, float] | None = None,
        portfolio_value: float = 1_000_000,
        lifecycle_stage: str = "experiment",
    ) -> dict:
        graph = self.get_graph(strategy_graph_id)
        config = graph.get("graph_config") or {}
        run = self.kernel.create_run(
            run_type="strategy_graph_simulate_day",
            project_id=graph["project_id"],
            market_profile_id=graph["market_profile_id"],
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            created_by="strategy_graph_3",
            params={
                "strategy_graph_id": strategy_graph_id,
                "decision_date": decision_date,
                "portfolio_value": portfolio_value,
            },
            input_refs=[{"type": "strategy_graph", "id": strategy_graph_id}],
        )

        alpha_rows = self._resolve_alpha_frame(
            graph,
            alpha_frame=alpha_frame,
            legacy_signal_frame=legacy_signal_frame,
            decision_date=decision_date,
        )
        selection_rows = self._select_assets(
            alpha_rows,
            selection_policy=config.get("selection_policy") or {},
            decision_date=decision_date,
        )
        selected_alpha = [row for row in selection_rows if row["action"] == "selected"]
        portfolio_result = self.portfolio_service.construct_portfolio(
            decision_date=decision_date,
            alpha_frame=selected_alpha,
            portfolio_spec_id=config["portfolio_construction_spec_id"],
            risk_control_spec_id=config.get("risk_control_spec_id"),
            rebalance_policy_spec_id=config.get("rebalance_policy_spec_id"),
            execution_policy_spec_id=config.get("execution_policy_spec_id"),
            state_policy_spec_id=config.get("state_policy_spec_id"),
            current_weights=current_weights,
            portfolio_value=portfolio_value,
            lifecycle_stage=lifecycle_stage,
        )
        stages = {
            "alpha": {
                "row_count": len(alpha_rows),
                "rows": alpha_rows,
            },
            "selection": {
                "selected_count": len(selected_alpha),
                "rows": selection_rows,
            },
            "portfolio": {
                "portfolio_run_id": portfolio_result["portfolio_run"]["id"],
                "target_artifact_id": portfolio_result["target_artifact"]["id"],
                "targets": portfolio_result["targets"],
                "constraint_trace": portfolio_result["constraint_trace"],
            },
            "execution": {
                "order_intent_artifact_id": portfolio_result["order_intent_artifact"]["id"],
                "order_intents": portfolio_result["order_intents"],
            },
        }
        profile = {
            **portfolio_result["profile"],
            "alpha_count": len(alpha_rows),
            "selected_count": len(selected_alpha),
        }
        explain_payload = {
            "strategy_graph": graph,
            "decision_date": decision_date,
            "stages": stages,
            "profile": profile,
            "artifact_refs": {
                "portfolio_run_id": portfolio_result["portfolio_run"]["id"],
                "portfolio_targets": portfolio_result["target_artifact"]["id"],
                "constraint_trace": portfolio_result["trace_artifact"]["id"],
                "order_intents": portfolio_result["order_intent_artifact"]["id"],
            },
        }
        stage_artifact = self.kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="strategy_graph_explain",
            payload=explain_payload,
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            metadata={"strategy_graph_id": strategy_graph_id, "decision_date": decision_date},
        )
        signal = self._insert_strategy_signal(
            run_id=run["id"],
            graph=graph,
            decision_date=decision_date,
            portfolio_run_id=portfolio_result["portfolio_run"]["id"],
            explain_artifact_id=stage_artifact["id"],
            profile=profile,
            lifecycle_stage=lifecycle_stage,
        )
        self.kernel.add_lineage(
            from_type="strategy_graph",
            from_id=strategy_graph_id,
            to_type="strategy_signal",
            to_id=signal["id"],
            relation="simulated_day",
            metadata={"decision_date": decision_date},
        )
        self.kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary=profile,
            qa_summary={"blocking": False},
        )
        return {
            "run": self.kernel.get_run(run["id"]),
            "strategy_graph": graph,
            "strategy_signal": signal,
            "stage_artifact": stage_artifact,
            "alpha_frame": alpha_rows,
            "selection_frame": selection_rows,
            "portfolio_run": portfolio_result["portfolio_run"],
            "targets": portfolio_result["targets"],
            "constraint_trace": portfolio_result["constraint_trace"],
            "order_intents": portfolio_result["order_intents"],
            "profile": profile,
            "stages": stages,
        }

    def explain_day(self, strategy_signal_id: str) -> dict:
        signal = self.get_strategy_signal(strategy_signal_id)
        artifact = self.kernel.get_artifact(signal["explain_artifact_id"])
        with open(artifact["uri"], encoding="utf-8") as fh:
            payload = json.load(fh)
        return {
            "strategy_signal": signal,
            "artifact": artifact,
            "strategy_graph": payload["strategy_graph"],
            "decision_date": payload["decision_date"],
            "stages": payload["stages"],
            "profile": payload["profile"],
            "artifact_refs": payload["artifact_refs"],
        }

    # ------------------------------------------------------------------
    # Read APIs
    # ------------------------------------------------------------------

    def get_graph(self, strategy_graph_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      graph_type, version, graph_config, dependency_refs,
                      lifecycle_stage, status, metadata, created_at, updated_at
               FROM strategy_graphs
               WHERE id = ?""",
            [strategy_graph_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"StrategyGraph {strategy_graph_id} not found")
        graph = self._graph_row(row)
        graph["nodes"] = self.list_nodes(strategy_graph_id)
        return graph

    def list_graphs(
        self,
        *,
        project_id: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        clauses = []
        params: list[Any] = []
        if project_id:
            clauses.append("project_id = ?")
            params.append(project_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = get_connection().execute(
            f"""SELECT id, project_id, market_profile_id, name, description,
                       graph_type, version, graph_config, dependency_refs,
                       lifecycle_stage, status, metadata, created_at, updated_at
                FROM strategy_graphs
                {where}
                ORDER BY created_at DESC""",
            params,
        ).fetchall()
        return [self._graph_row(row) for row in rows]

    def list_nodes(self, strategy_graph_id: str) -> list[dict]:
        rows = get_connection().execute(
            """SELECT id, strategy_graph_id, node_order, node_key, node_type,
                      name, input_schema, output_schema, data_requirements,
                      params, code_snapshot, explain_schema, created_at
               FROM strategy_nodes
               WHERE strategy_graph_id = ?
               ORDER BY node_order""",
            [strategy_graph_id],
        ).fetchall()
        return [self._node_row(row) for row in rows]

    def get_strategy_signal(self, strategy_signal_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id,
                      strategy_graph_id, decision_date, portfolio_run_id,
                      explain_artifact_id, status, lifecycle_stage, profile,
                      created_at, completed_at
               FROM strategy_signals
               WHERE id = ?""",
            [strategy_signal_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Strategy signal {strategy_signal_id} not found")
        return self._signal_row(row)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _insert_graph(
        self,
        *,
        name: str,
        graph_type: str,
        project_id: str,
        market_profile_id: str,
        description: str | None,
        graph_config: dict[str, Any],
        dependency_refs: list[dict[str, Any]],
        lifecycle_stage: str,
        status: str,
        metadata: dict[str, Any] | None,
    ) -> dict:
        graph_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO strategy_graphs
               (id, project_id, market_profile_id, name, description,
                graph_type, version, graph_config, dependency_refs,
                lifecycle_stage, status, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)""",
            [
                graph_id,
                project_id,
                market_profile_id,
                name,
                description,
                graph_type,
                json.dumps(graph_config, default=str),
                json.dumps(dependency_refs, default=str),
                lifecycle_stage,
                status,
                json.dumps(metadata or {}, default=str),
                now,
                now,
            ],
        )
        return self.get_graph(graph_id)

    def _insert_standard_nodes(
        self,
        strategy_graph_id: str,
        graph_config: dict[str, Any],
        *,
        legacy: bool,
    ) -> None:
        alpha_node = "Legacy Strategy Adapter" if legacy else "Alpha Frame Input"
        nodes = [
            ("universe", "UniverseFilter", "Universe Filter", {}, {"universe": "date, asset_id"}),
            (
                "alpha",
                "LegacyStrategyAdapter" if legacy else "AlphaModel",
                alpha_node,
                {},
                {"alpha": "date, asset_id, score, confidence, reason"},
            ),
            (
                "selection",
                "SelectionPolicy",
                "Top-N Selection",
                graph_config.get("selection_policy") or {},
                {"selection": "date, asset_id, action, reason, score_ref"},
            ),
            (
                "portfolio",
                "PortfolioConstruction",
                "Portfolio Construction",
                {"spec_id": graph_config.get("portfolio_construction_spec_id")},
                {"target": "date, asset_id, target_weight"},
            ),
            (
                "risk_execution",
                "RiskExecution",
                "Risk Control and Execution",
                {
                    "risk_control_spec_id": graph_config.get("risk_control_spec_id"),
                    "execution_policy_spec_id": graph_config.get("execution_policy_spec_id"),
                },
                {"orders": "decision_date, execution_date, asset_id, side, target_weight"},
            ),
            (
                "state",
                "StatePolicy",
                "State Policy",
                {"state_policy_spec_id": graph_config.get("state_policy_spec_id")},
                {"state": "stateless"},
            ),
        ]
        now = utc_now_naive()
        for order, (key, node_type, name, params, output_schema) in enumerate(nodes):
            get_connection().execute(
                """INSERT INTO strategy_nodes
                   (id, strategy_graph_id, node_order, node_key, node_type,
                    name, input_schema, output_schema, data_requirements,
                    params, code_snapshot, explain_schema, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    uuid.uuid4().hex[:12],
                    strategy_graph_id,
                    order,
                    key,
                    node_type,
                    name,
                    json.dumps({"input": "previous_stage"}, default=str),
                    json.dumps(output_schema, default=str),
                    json.dumps([], default=str),
                    json.dumps(params or {}, default=str),
                    None,
                    json.dumps({"payload": "stage rows and artifact refs"}, default=str),
                    now,
                ],
            )

    def _resolve_alpha_frame(
        self,
        graph: dict,
        *,
        alpha_frame: list[dict[str, Any]] | None,
        legacy_signal_frame: list[dict[str, Any]] | None,
        decision_date: str,
    ) -> list[dict[str, Any]]:
        if graph["graph_type"] == "legacy_strategy_adapter":
            if not legacy_signal_frame:
                raise ValueError("legacy_signal_frame is required for legacy adapter graphs")
            return self._legacy_signal_to_alpha(legacy_signal_frame, decision_date=decision_date)
        if not alpha_frame:
            raise ValueError("alpha_frame is required for builtin alpha graphs")
        rows = []
        for index, row in enumerate(alpha_frame):
            asset_id = str(row["asset_id"])
            score = float(row["score"])
            rows.append(
                {
                    "date": decision_date,
                    "asset_id": asset_id,
                    "score": score,
                    "rank": index + 1,
                    "confidence": float(row.get("confidence", 1.0)),
                    "reason": row.get("reason") or "provided alpha score",
                }
            )
        return sorted(rows, key=lambda item: item["score"], reverse=True)

    def _legacy_signal_to_alpha(
        self,
        rows: list[dict[str, Any]],
        *,
        decision_date: str,
    ) -> list[dict[str, Any]]:
        alpha = []
        for row in rows:
            signal = int(row.get("signal", 0))
            if signal <= 0:
                continue
            ticker = str(row.get("ticker") or row.get("asset_id")).upper()
            asset_id = row.get("asset_id") or f"US_EQ:{ticker}"
            score = float(row.get("strength", row.get("weight", 0.0)))
            alpha.append(
                {
                    "date": decision_date,
                    "asset_id": asset_id,
                    "score": score,
                    "rank": 0,
                    "confidence": 1.0,
                    "reason": "legacy signal adapter",
                }
            )
        if not alpha:
            raise ValueError("legacy_signal_frame produced no buy alpha rows")
        alpha = sorted(alpha, key=lambda item: item["score"], reverse=True)
        for index, row in enumerate(alpha, start=1):
            row["rank"] = index
        return alpha

    def _select_assets(
        self,
        alpha_rows: list[dict[str, Any]],
        *,
        selection_policy: dict[str, Any],
        decision_date: str,
    ) -> list[dict[str, Any]]:
        score_column = selection_policy.get("score_column", "score")
        top_n = int(selection_policy.get("top_n") or len(alpha_rows))
        ranked = sorted(alpha_rows, key=lambda item: float(item.get(score_column, 0.0)), reverse=True)
        selected = {row["asset_id"] for row in ranked[:top_n]}
        rows = []
        for rank, row in enumerate(ranked, start=1):
            action = "selected" if row["asset_id"] in selected else "rejected"
            rows.append(
                {
                    **row,
                    "date": decision_date,
                    "rank": rank,
                    "action": action,
                    "score_ref": score_column,
                    "reason": (
                        f"top {top_n} by {score_column}"
                        if action == "selected"
                        else f"rank {rank} outside top {top_n}"
                    ),
                }
            )
        return rows

    def _insert_strategy_signal(
        self,
        *,
        run_id: str,
        graph: dict,
        decision_date: str,
        portfolio_run_id: str,
        explain_artifact_id: str,
        profile: dict[str, Any],
        lifecycle_stage: str,
    ) -> dict:
        signal_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO strategy_signals
               (id, run_id, project_id, market_profile_id, strategy_graph_id,
                decision_date, portfolio_run_id, explain_artifact_id, status,
                lifecycle_stage, profile, created_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?)""",
            [
                signal_id,
                run_id,
                graph["project_id"],
                graph["market_profile_id"],
                graph["id"],
                decision_date,
                portfolio_run_id,
                explain_artifact_id,
                lifecycle_stage,
                json.dumps(profile, default=str),
                now,
                now,
            ],
        )
        return self.get_strategy_signal(signal_id)

    @staticmethod
    def _dependency_refs(config: dict[str, Any]) -> list[dict[str, Any]]:
        refs = []
        mapping = {
            "portfolio_construction_spec_id": "portfolio_construction_spec",
            "risk_control_spec_id": "risk_control_spec",
            "rebalance_policy_spec_id": "rebalance_policy_spec",
            "execution_policy_spec_id": "execution_policy_spec",
            "state_policy_spec_id": "state_policy_spec",
        }
        for key, ref_type in mapping.items():
            if config.get(key):
                refs.append({"type": ref_type, "id": config[key]})
        return refs

    @staticmethod
    def _graph_row(row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "graph_type": row[5],
            "version": row[6],
            "graph_config": _json(row[7], {}),
            "dependency_refs": _json(row[8], []),
            "lifecycle_stage": row[9],
            "status": row[10],
            "metadata": _json(row[11], {}),
            "created_at": str(row[12]) if row[12] is not None else None,
            "updated_at": str(row[13]) if row[13] is not None else None,
        }

    @staticmethod
    def _node_row(row) -> dict:
        return {
            "id": row[0],
            "strategy_graph_id": row[1],
            "node_order": row[2],
            "node_key": row[3],
            "node_type": row[4],
            "name": row[5],
            "input_schema": _json(row[6], {}),
            "output_schema": _json(row[7], {}),
            "data_requirements": _json(row[8], []),
            "params": _json(row[9], {}),
            "code_snapshot": row[10],
            "explain_schema": _json(row[11], {}),
            "created_at": str(row[12]) if row[12] is not None else None,
        }

    @staticmethod
    def _signal_row(row) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "market_profile_id": row[3],
            "strategy_graph_id": row[4],
            "decision_date": str(row[5]) if row[5] is not None else None,
            "portfolio_run_id": row[6],
            "explain_artifact_id": row[7],
            "status": row[8],
            "lifecycle_stage": row[9],
            "profile": _json(row[10], {}),
            "created_at": str(row[11]) if row[11] is not None else None,
            "completed_at": str(row[12]) if row[12] is not None else None,
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
