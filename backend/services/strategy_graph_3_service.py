"""StrategyGraph runtime service for QAgent 3.0.

M8 introduces graph-defined strategy assets and a single-day runtime that
produces alpha, selection, portfolio target, constraint trace, and order intent
from one service path.  Backtest/signal/paper can migrate onto this runtime
without copying portfolio or execution logic.
"""

from __future__ import annotations

import json
import math
import uuid
from typing import Any

from backend.db import get_connection
from backend.services.calendar_service import get_trading_days
from backend.services.execution_model_service import (
    evaluate_planned_price_fill,
    normalize_planned_price_buffer_bps,
    normalize_planned_price_fallback,
)
from backend.services.market_context import normalize_market
from backend.services.market_data_foundation_service import MarketDataFoundationService
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
from backend.services.portfolio_valuation_service import PortfolioValuationService
from backend.services.research_kernel_service import ResearchKernelService
from backend.time_utils import utc_now_naive


def _positive_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0:
        return None
    return numeric


class StrategyGraph3Service:
    """Create StrategyGraph assets and run single-day explanations."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        portfolio_service: PortfolioAssets3Service | None = None,
        valuation_service: PortfolioValuationService | None = None,
        market_data_service: MarketDataFoundationService | None = None,
    ) -> None:
        self.kernel = kernel_service or ResearchKernelService()
        self.portfolio_service = portfolio_service or PortfolioAssets3Service(
            kernel_service=self.kernel
        )
        self.valuation_service = valuation_service or PortfolioValuationService()
        self.market_data = market_data_service or MarketDataFoundationService()

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
        raise ValueError(
            "Legacy strategy adapters are disabled in V3.2 runtime. "
            "Re-enter or reimplement the strategy as a 3.0 StrategyGraph."
        )

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
        if legacy_signal_frame is not None:
            raise ValueError(
                "legacy_signal_frame is disabled in V3.2 runtime. "
                "Provide a 3.0 alpha_frame instead."
            )
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

    def backtest_graph(
        self,
        strategy_graph_id: str,
        *,
        start_date: str,
        end_date: str,
        alpha_frames_by_date: dict[str, list[dict[str, Any]]] | None = None,
        legacy_signal_frames_by_date: dict[str, list[dict[str, Any]]] | None = None,
        initial_capital: float = 1_000_000,
        lifecycle_stage: str = "experiment",
        price_field: str = "close",
    ) -> dict:
        if legacy_signal_frames_by_date is not None:
            raise ValueError(
                "legacy_signal_frames_by_date is disabled in V3.2 runtime. "
                "Provide alpha_frames_by_date instead."
            )
        graph = self.get_graph(strategy_graph_id)
        market = "CN" if graph["market_profile_id"] == "CN_A" else "US"
        dates = [str(day) for day in get_trading_days(start_date, end_date, market=market)]
        run = self.kernel.create_run(
            run_type="strategy_graph_backtest",
            project_id=graph["project_id"],
            market_profile_id=graph["market_profile_id"],
            lifecycle_stage=lifecycle_stage,
            retention_class="standard",
            created_by="strategy_graph_3",
            params={
                "strategy_graph_id": strategy_graph_id,
                "start_date": start_date,
                "end_date": end_date,
                "initial_capital": initial_capital,
                "price_field": price_field,
            },
            input_refs=[{"type": "strategy_graph", "id": strategy_graph_id}],
        )
        backtest_run_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO backtest_runs
               (id, run_id, project_id, market_profile_id, strategy_graph_id,
                start_date, end_date, config, summary, status,
                lifecycle_stage, created_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, NULL)""",
            [
                backtest_run_id,
                run["id"],
                graph["project_id"],
                graph["market_profile_id"],
                graph["id"],
                start_date,
                end_date,
                json.dumps(
                    {
                        "price_field": price_field,
                        "alpha_dates": sorted((alpha_frames_by_date or {}).keys()),
                    },
                    default=str,
                ),
                json.dumps({}, default=str),
                lifecycle_stage,
                now,
            ],
        )

        nav = float(initial_capital)
        current_weights: dict[str, float] = {}
        previous_date: str | None = None
        daily: list[dict[str, Any]] = []
        total_turnover = 0.0
        total_cost = 0.0
        total_filled_orders = 0
        total_blocked_orders = 0
        warnings: list[dict[str, Any]] = []
        execution_models_seen: set[str] = set()

        for decision_date in dates:
            valuation = self.valuation_service.revalue_weights(
                market_profile_id=graph["market_profile_id"],
                from_date=previous_date,
                to_date=decision_date,
                nav=nav,
                weights=current_weights,
                price_field=price_field,
            )
            nav = float(valuation["nav"])
            drifted_weights = {
                asset_id: float(weight)
                for asset_id, weight in (valuation.get("weights") or {}).items()
            }
            alpha_frame = (alpha_frames_by_date or {}).get(decision_date)
            runtime = self.simulate_day(
                strategy_graph_id,
                decision_date=decision_date,
                alpha_frame=alpha_frame,
                current_weights=drifted_weights,
                portfolio_value=nav,
                lifecycle_stage=lifecycle_stage,
            )
            target_weights = {
                row["asset_id"]: float(row["target_weight"])
                for row in runtime["targets"]
                if float(row["target_weight"]) > 1e-10
            }
            turnover = sum(abs(float(order["delta_weight"])) for order in runtime["order_intents"])
            total_turnover += turnover
            fills = self._execute_backtest_orders(
                backtest_run_id=backtest_run_id,
                order_intents=runtime["order_intents"],
                market_profile_id=graph["market_profile_id"],
                nav=nav,
            )
            if fills["diagnostics"].get("execution_model") == "planned_price":
                executed_weights = self._executed_weights_after_orders(
                    starting_weights=drifted_weights,
                    order_intents=runtime["order_intents"],
                    fill_diagnostics=fills["diagnostics"],
                )
            else:
                executed_weights = target_weights
            nav -= fills["total_cost"]
            total_cost += fills["total_cost"]
            total_filled_orders += fills["filled_order_count"]
            total_blocked_orders += fills["blocked_order_count"]
            execution_models_seen.add(str(fills["diagnostics"].get("execution_model") or "next_open"))
            diagnostics = {
                "strategy_signal_id": runtime["strategy_signal"]["id"],
                "portfolio_run_id": runtime["portfolio_run"]["id"],
                "valuation": valuation["diagnostics"],
                "turnover_estimate": round(turnover, 12),
                "execution": fills["diagnostics"],
                "target_count": len(runtime["targets"]),
            }
            gross = sum(abs(weight) for weight in executed_weights.values())
            net = sum(executed_weights.values())
            get_connection().execute(
                """INSERT OR REPLACE INTO backtest_daily
                   (backtest_run_id, date, nav, cash, gross_exposure,
                    net_exposure, diagnostics)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [
                    backtest_run_id,
                    decision_date,
                    nav,
                    0.0,
                    gross,
                    net,
                    json.dumps(diagnostics, default=str),
                ],
            )
            daily.append(
                {
                    "backtest_run_id": backtest_run_id,
                    "date": decision_date,
                    "nav": nav,
                    "cash": 0.0,
                    "gross_exposure": gross,
                    "net_exposure": net,
                    "diagnostics": diagnostics,
                }
            )
            if valuation["diagnostics"]["status"] != "valued":
                warnings.append({"date": decision_date, "valuation": valuation["diagnostics"]})
            if fills["blocked_order_count"]:
                warnings.append({"date": decision_date, "execution": fills["diagnostics"]})
            current_weights = executed_weights
            previous_date = decision_date

        summary = {
            "initial_capital": float(initial_capital),
            "final_nav": nav,
            "total_return": (nav / float(initial_capital) - 1.0) if initial_capital else 0.0,
            "days_processed": len(daily),
            "total_turnover_estimate": round(total_turnover, 12),
            "total_cost": round(total_cost, 6),
            "fill_diagnostics": {
                "filled_order_count": total_filled_orders,
                "blocked_order_count": total_blocked_orders,
                "execution_model": (
                    "mixed"
                    if len(execution_models_seen) > 1
                    else next(iter(execution_models_seen), "next_open")
                ),
            },
            "valuation_warnings": warnings,
        }
        get_connection().execute(
            """UPDATE backtest_runs
                  SET summary = ?,
                      status = 'completed',
                      completed_at = ?
                WHERE id = ?""",
            [json.dumps(summary, default=str), utc_now_naive(), backtest_run_id],
        )
        self.kernel.add_lineage(
            from_type="strategy_graph",
            from_id=strategy_graph_id,
            to_type="backtest_run",
            to_id=backtest_run_id,
            relation="backtested",
            metadata={"start_date": start_date, "end_date": end_date},
        )
        self.kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary=summary,
            qa_summary={"blocking": False, "valuation_warning_count": len(warnings)},
            warnings=warnings,
        )
        return {
            "run": self.kernel.get_run(run["id"]),
            "backtest_run": self.get_backtest_run(backtest_run_id),
            "daily": daily,
            "summary": summary,
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

    def get_backtest_run(self, backtest_run_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id,
                      strategy_graph_id, start_date, end_date, config,
                      summary, status, lifecycle_stage, created_at,
                      completed_at
               FROM backtest_runs
               WHERE id = ?""",
            [backtest_run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Backtest run {backtest_run_id} not found")
        return self._backtest_run_row(row)

    def list_backtest_runs(
        self,
        *,
        strategy_graph_id: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, run_id, project_id, market_profile_id,
                          strategy_graph_id, start_date, end_date, config,
                          summary, status, lifecycle_stage, created_at,
                          completed_at
                   FROM backtest_runs
                   WHERE 1 = 1"""
        params: list[Any] = []
        if strategy_graph_id:
            query += " AND strategy_graph_id = ?"
            params.append(strategy_graph_id)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._backtest_run_row(row) for row in rows]

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
            raise ValueError("Legacy strategy adapters are disabled in V3.2 runtime")
        if legacy_signal_frame is not None:
            raise ValueError(
                "legacy_signal_frame is disabled in V3.2 runtime. "
                "Provide a 3.0 alpha_frame instead."
            )
        if not alpha_frame:
            raise ValueError("alpha_frame is required for builtin alpha graphs")
        rows = []
        for index, row in enumerate(alpha_frame):
            asset_id = str(row["asset_id"])
            score = float(row["score"])
            alpha_row = {
                "date": decision_date,
                "asset_id": asset_id,
                "score": score,
                "rank": index + 1,
                "confidence": float(row.get("confidence", 1.0)),
                "reason": row.get("reason") or "provided alpha score",
            }
            planned_price = _positive_float(row.get("planned_price"))
            if planned_price is not None:
                alpha_row["planned_price"] = planned_price
            rows.append(alpha_row)
        return sorted(rows, key=lambda item: item["score"], reverse=True)

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

    def _execute_backtest_orders(
        self,
        *,
        backtest_run_id: str,
        order_intents: list[dict[str, Any]],
        market_profile_id: str,
        nav: float,
    ) -> dict[str, Any]:
        now = utc_now_naive()
        rows = []
        diagnostics = {
            "status": "no_orders" if not order_intents else "evaluated",
            "filled": [],
            "blocked": [],
            "missing_price": [],
            "cost_model": {},
            "trading_rules": {},
        }
        if not order_intents:
            return {
                "filled_order_count": 0,
                "blocked_order_count": 0,
                "total_cost": 0.0,
                "diagnostics": diagnostics,
            }

        profile = self.market_data.get_market_profile(market_profile_id)
        market = normalize_market(profile["market_code"])
        cost_model = profile.get("cost_model") or {}
        trading_rules = profile.get("trading_rule_set") or {}
        diagnostics["cost_model"] = {
            "commission_rate": float(cost_model.get("commission_rate") or 0.0),
            "slippage_rate": float(cost_model.get("slippage_rate") or 0.0),
            "stamp_tax_rate": float(cost_model.get("stamp_tax_rate") or 0.0),
            "min_commission": float(cost_model.get("min_commission") or 0.0),
        }
        diagnostics["trading_rules"] = {
            "lot_size": int(trading_rules.get("lot_size") or 1),
            "limit_up_down": bool(trading_rules.get("limit_up_down")),
        }

        asset_ids = sorted({str(order["asset_id"]) for order in order_intents})
        execution_dates = sorted(
            {
                str(order.get("execution_date"))
                for order in order_intents
                if order.get("execution_date")
            }
            | {
                str(order.get("decision_date"))
                for order in order_intents
                if str(order.get("execution_model") or "next_open") == "planned_price"
                and order.get("decision_date")
            }
        )
        price_field_by_order = {
            (str(order["asset_id"]), str(order.get("execution_date"))): str(order.get("price_field") or "open")
            for order in order_intents
        }
        requested_price_fields = set(price_field_by_order.values())
        if any(str(order.get("execution_model") or "next_open") == "planned_price" for order in order_intents):
            requested_price_fields.update({"high", "low", "close"})
        prices = self._load_execution_prices(
            market_profile_id=market_profile_id,
            asset_ids=asset_ids,
            execution_dates=execution_dates,
            price_fields=sorted(requested_price_fields),
        )
        statuses = self._load_trade_status(
            market_profile_id=market_profile_id,
            asset_ids=asset_ids,
            execution_dates=execution_dates,
        )

        filled_count = 0
        blocked_count = 0
        total_cost = 0.0
        for order in order_intents:
            execution_date = str(order.get("execution_date"))
            asset_id = str(order["asset_id"])
            side = str(order["side"])
            execution_model = str(order.get("execution_model") or "next_open")
            price_field = str(order.get("price_field") or "open")
            estimated_value = abs(float(order.get("estimated_value") or 0.0))
            if execution_model == "planned_price":
                planned_price = _positive_float(order.get("planned_price"))
                planned_price_source = str(order.get("planned_price_source") or "strategy_output")
                if planned_price is None and planned_price_source == "decision_close":
                    planned_price = prices.get((asset_id, str(order.get("decision_date")), "close"))
                high_price = prices.get((asset_id, execution_date, "high"))
                low_price = prices.get((asset_id, execution_date, "low"))
                close_price = prices.get((asset_id, execution_date, "close"))
                fill_fallback = normalize_planned_price_fallback(order.get("fill_fallback"))
                fill_decision = evaluate_planned_price_fill(
                    planned_price=planned_price,
                    high=high_price,
                    low=low_price,
                    buffer_bps=order.get("planned_price_buffer_bps"),
                )
                price = fill_decision.fill_price
                planned_block_reason = fill_decision.reason
                fill_type = "planned_price" if fill_decision.filled else "blocked"
                if (
                    not fill_decision.filled
                    and fill_fallback == "next_close"
                    and planned_block_reason == "planned_price_outside_buffered_range"
                    and close_price is not None
                    and close_price > 0
                ):
                    price = float(close_price)
                    planned_block_reason = None
                    fill_type = "fallback_close"
            else:
                planned_price = None
                planned_price_source = None
                high_price = None
                low_price = None
                close_price = None
                fill_fallback = None
                fill_type = None
                fill_decision = None
                planned_block_reason = None
                price = prices.get((asset_id, execution_date, price_field))
            block_reason = self._execution_block_reason(
                order=order,
                status=statuses.get((asset_id, execution_date)),
                price=price,
                market=market,
                trading_rules=trading_rules,
            )
            quantity = None
            value = estimated_value
            cost = None
            metadata = dict(order)
            metadata["execution_model"] = execution_model
            metadata["price_field"] = price_field
            if execution_model == "planned_price":
                metadata["planned_price"] = planned_price
                metadata["planned_price_source"] = planned_price_source
                metadata["planned_price_buffer_bps"] = normalize_planned_price_buffer_bps(
                    order.get("planned_price_buffer_bps")
                )
                metadata["fill_fallback"] = fill_fallback
                metadata["fill_type"] = fill_type
                metadata["execution_high"] = high_price
                metadata["execution_low"] = low_price
                metadata["execution_close"] = close_price
            if fill_decision is not None:
                metadata["planned_price_bounds"] = {
                    "lower": fill_decision.lower_bound,
                    "upper": fill_decision.upper_bound,
                }
            if block_reason is None and planned_block_reason is not None:
                if planned_block_reason == "missing_high_low":
                    block_reason = "missing_execution_price"
                else:
                    block_reason = planned_block_reason
            if block_reason:
                blocked_count += 1
                metadata["fill_status"] = "blocked"
                metadata["block_reason"] = block_reason
                diagnostics["blocked"].append(
                    {
                        "asset_id": asset_id,
                        "execution_date": execution_date,
                        "side": side,
                        "reason": block_reason,
                        **(
                            {
                                "planned_price": planned_price,
                                "planned_price_source": planned_price_source,
                                "fill_fallback": fill_fallback,
                                "fill_type": "blocked",
                                "high": high_price,
                                "low": low_price,
                                "close": close_price,
                            }
                            if execution_model == "planned_price"
                            else {}
                        ),
                    }
                )
                if block_reason == "missing_execution_price":
                    diagnostics["missing_price"].append(
                        {"asset_id": asset_id, "execution_date": execution_date, "price_field": price_field}
                    )
            else:
                quantity = self._order_quantity(
                    side=side,
                    estimated_value=estimated_value,
                    price=float(price),
                    trading_rules=trading_rules,
                )
                if quantity <= 0:
                    blocked_count += 1
                    metadata["fill_status"] = "blocked"
                    metadata["block_reason"] = "quantity_rounds_to_zero"
                    diagnostics["blocked"].append(
                        {
                            "asset_id": asset_id,
                            "execution_date": execution_date,
                            "side": side,
                            "reason": "quantity_rounds_to_zero",
                        }
                    )
                    quantity = None
                else:
                    value = abs(float(quantity) * float(price))
                    cost = self._execution_cost(side=side, trade_value=value, cost_model=cost_model)
                    total_cost += cost
                    filled_count += 1
                    metadata["fill_status"] = "filled"
                    metadata["execution_price"] = float(price)
                    metadata["quantity"] = quantity
                    metadata["cost"] = cost
                    diagnostics["filled"].append(
                        {
                            "asset_id": asset_id,
                            "execution_date": execution_date,
                            "side": side,
                            "target_weight": float(order.get("target_weight") or 0.0),
                            "quantity": quantity,
                            "price": float(price),
                            "value": round(value, 6),
                            "cost": round(cost, 6),
                            **(
                                {
                                    "planned_price": planned_price,
                                    "planned_price_source": planned_price_source,
                                    "fill_fallback": fill_fallback,
                                    "fill_type": fill_type,
                                }
                                if execution_model == "planned_price"
                                else {}
                            ),
                        }
                    )
            rows.append(
                [
                    uuid.uuid4().hex[:12],
                    backtest_run_id,
                    order.get("decision_date"),
                    execution_date,
                    asset_id,
                    side,
                    quantity,
                    price if quantity is not None else None,
                    value,
                    cost,
                    json.dumps(metadata, default=str),
                    now,
                ]
            )
        get_connection().executemany(
            """INSERT INTO backtest_trades
               (id, backtest_run_id, decision_date, execution_date, asset_id,
                side, quantity, price, value, cost, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        diagnostics["status"] = "filled" if blocked_count == 0 else ("partial" if filled_count else "blocked")
        diagnostics["filled_order_count"] = filled_count
        diagnostics["blocked_order_count"] = blocked_count
        diagnostics["total_cost"] = round(total_cost, 6)
        diagnostics["nav_after_cost"] = round(float(nav) - total_cost, 6)
        diagnostics["execution_model"] = (
            "planned_price"
            if any(str(order.get("execution_model") or "next_open") == "planned_price" for order in order_intents)
            else "next_open"
        )
        return {
            "filled_order_count": filled_count,
            "blocked_order_count": blocked_count,
            "total_cost": total_cost,
            "diagnostics": diagnostics,
        }

    def _load_execution_prices(
        self,
        *,
        market_profile_id: str,
        asset_ids: list[str],
        execution_dates: list[str],
        price_fields: list[str],
    ) -> dict[tuple[str, str, str], float]:
        if not asset_ids or not execution_dates:
            return {}
        allowed_fields = {"open", "high", "low", "close"}
        fields = [field for field in price_fields if field in allowed_fields] or ["open"]
        bars = self.market_data.query_bars(
            market_profile_id=market_profile_id,
            asset_ids=asset_ids,
            start=min(execution_dates),
            end=max(execution_dates),
            limit=max(len(asset_ids) * len(execution_dates) * 2, 100),
        )["bars"]
        result: dict[tuple[str, str, str], float] = {}
        execution_date_set = set(execution_dates)
        for row in bars:
            row_date = str(row["date"])
            if row_date not in execution_date_set:
                continue
            for field in fields:
                value = row.get(field)
                if value is None:
                    continue
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    continue
                if numeric > 0:
                    result[(str(row["asset_id"]), row_date, field)] = numeric
        return result

    @staticmethod
    def _executed_weights_after_orders(
        *,
        starting_weights: dict[str, float],
        order_intents: list[dict[str, Any]],
        fill_diagnostics: dict[str, Any],
    ) -> dict[str, float]:
        weights = {
            str(asset_id): float(weight)
            for asset_id, weight in starting_weights.items()
            if abs(float(weight)) > 1e-10
        }
        filled = {
            (str(item.get("asset_id")), str(item.get("execution_date")))
            for item in fill_diagnostics.get("filled", [])
            if item.get("asset_id") and item.get("execution_date")
        }
        for order in order_intents:
            key = (str(order.get("asset_id")), str(order.get("execution_date")))
            if key not in filled:
                continue
            target_weight = float(order.get("target_weight") or 0.0)
            asset_id = str(order["asset_id"])
            if abs(target_weight) <= 1e-10:
                weights.pop(asset_id, None)
            else:
                weights[asset_id] = target_weight
        return weights

    @staticmethod
    def _load_trade_status(
        *,
        market_profile_id: str,
        asset_ids: list[str],
        execution_dates: list[str],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        if not asset_ids or not execution_dates:
            return {}
        asset_placeholders = ",".join("?" for _ in asset_ids)
        date_placeholders = ",".join("?" for _ in execution_dates)
        rows = get_connection().execute(
            f"""SELECT asset_id, date, is_trading, is_suspended, is_st,
                       limit_up, limit_down, metadata
                FROM trade_status
                WHERE market_profile_id = ?
                  AND asset_id IN ({asset_placeholders})
                  AND date IN ({date_placeholders})""",
            [market_profile_id, *asset_ids, *execution_dates],
        ).fetchall()
        return {
            (str(row[0]), str(row[1])): {
                "is_trading": bool(row[2]),
                "is_suspended": bool(row[3]),
                "is_st": bool(row[4]),
                "limit_up": row[5],
                "limit_down": row[6],
                "metadata": _json(row[7], {}),
            }
            for row in rows
        }

    @staticmethod
    def _execution_block_reason(
        *,
        order: dict[str, Any],
        status: dict[str, Any] | None,
        price: float | None,
        market: str,
        trading_rules: dict[str, Any],
    ) -> str | None:
        if str(order.get("execution_model") or "next_open") != "planned_price" and (
            price is None or float(price) <= 0
        ):
            return "missing_execution_price"
        if not status:
            return None
        if status.get("is_suspended") or status.get("is_trading") is False:
            return "suspended"
        if market == "CN" and status.get("is_st") and order.get("side") == "buy":
            return "st_buy_blocked"
        if bool(trading_rules.get("limit_up_down")):
            side = str(order.get("side"))
            limit_up = status.get("limit_up")
            limit_down = status.get("limit_down")
            if side == "buy" and limit_up is not None and math.isclose(float(price), float(limit_up), rel_tol=0, abs_tol=1e-9):
                return "limit_up_buy_blocked"
            if side == "sell" and limit_down is not None and math.isclose(float(price), float(limit_down), rel_tol=0, abs_tol=1e-9):
                return "limit_down_sell_blocked"
        return None

    @staticmethod
    def _order_quantity(
        *,
        side: str,
        estimated_value: float,
        price: float,
        trading_rules: dict[str, Any],
    ) -> float:
        if price <= 0:
            return 0.0
        raw = estimated_value / price
        lot_size = int(trading_rules.get("lot_size") or 1)
        if side == "buy" and lot_size > 1:
            return float(math.floor(raw / lot_size) * lot_size)
        return float(raw)

    @staticmethod
    def _execution_cost(*, side: str, trade_value: float, cost_model: dict[str, Any]) -> float:
        commission_rate = float(cost_model.get("commission_rate") or 0.0)
        slippage_rate = float(cost_model.get("slippage_rate") or 0.0)
        stamp_tax_rate = float(cost_model.get("stamp_tax_rate") or 0.0)
        min_commission = float(cost_model.get("min_commission") or 0.0)
        commission = max(trade_value * commission_rate, min_commission) if trade_value > 0 else 0.0
        slippage = trade_value * slippage_rate
        stamp_tax = trade_value * stamp_tax_rate if side == "sell" else 0.0
        return round(commission + slippage + stamp_tax, 6)

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

    @staticmethod
    def _backtest_run_row(row) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "market_profile_id": row[3],
            "strategy_graph_id": row[4],
            "start_date": str(row[5]) if row[5] is not None else None,
            "end_date": str(row[6]) if row[6] is not None else None,
            "config": _json(row[7], {}),
            "summary": _json(row[8], {}),
            "status": row[9],
            "lifecycle_stage": row[10],
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
