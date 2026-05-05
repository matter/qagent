"""Portfolio, risk, rebalance, and execution assets for QAgent 3.0.

M7 makes position sizing and constraints reusable research assets.  The service
accepts an alpha frame and returns portfolio targets, constraint trace, and
order intents without calling legacy StrategyBase or BacktestService.
"""

from __future__ import annotations

import json
import math
import uuid
from typing import Any

import pandas as pd

from backend.db import get_connection
from backend.services.calendar_service import offset_trading_days
from backend.services.market_context import normalize_market
from backend.services.research_kernel_service import ResearchKernelService
from backend.time_utils import utc_now_naive


_PROFILE_BY_MARKET = {"US": "US_EQ", "CN": "CN_A"}
_MARKET_BY_PROFILE = {"US_EQ": "US", "CN_A": "CN"}
_SUPPORTED_BUILDERS = {"equal_weight", "score_proportional", "inverse_vol"}
_SUPPORTED_REBALANCE = {"none", "band"}
_SUPPORTED_EXECUTION = {"next_open"}
_SUPPORTED_STATE = {"stateless"}


class PortfolioAssets3Service:
    """Create M7 specs and run alpha-to-portfolio construction."""

    def __init__(self, *, kernel_service: ResearchKernelService | None = None) -> None:
        self.kernel = kernel_service or ResearchKernelService()
        self.ensure_default_state_policy()

    # ------------------------------------------------------------------
    # Spec creation
    # ------------------------------------------------------------------

    def create_portfolio_construction_spec(
        self,
        *,
        name: str,
        method: str,
        params: dict[str, Any] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        if method not in _SUPPORTED_BUILDERS:
            raise ValueError(f"Unsupported portfolio construction method {method!r}")
        return self._insert_spec(
            table="portfolio_construction_specs",
            name=name,
            project_id=project_id,
            market_profile_id=market_profile_id,
            description=description,
            lifecycle_stage=lifecycle_stage,
            status=status,
            metadata=metadata,
            extra_columns={"method": method, "params": params or {}},
        )

    def create_risk_control_spec(
        self,
        *,
        name: str,
        rules: list[dict[str, Any]] | None = None,
        params: dict[str, Any] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        return self._insert_spec(
            table="risk_control_specs",
            name=name,
            project_id=project_id,
            market_profile_id=market_profile_id,
            description=description,
            lifecycle_stage=lifecycle_stage,
            status=status,
            metadata=metadata,
            extra_columns={"rules": rules or [], "params": params or {}},
        )

    def create_rebalance_policy_spec(
        self,
        *,
        name: str,
        policy_type: str = "none",
        params: dict[str, Any] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        if policy_type not in _SUPPORTED_REBALANCE:
            raise ValueError(f"Unsupported rebalance policy {policy_type!r}")
        return self._insert_spec(
            table="rebalance_policy_specs",
            name=name,
            project_id=project_id,
            market_profile_id=market_profile_id,
            description=description,
            lifecycle_stage=lifecycle_stage,
            status=status,
            metadata=metadata,
            extra_columns={"policy_type": policy_type, "params": params or {}},
        )

    def create_execution_policy_spec(
        self,
        *,
        name: str,
        policy_type: str = "next_open",
        params: dict[str, Any] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        if policy_type not in _SUPPORTED_EXECUTION:
            raise ValueError(f"Unsupported execution policy {policy_type!r}")
        return self._insert_spec(
            table="execution_policy_specs",
            name=name,
            project_id=project_id,
            market_profile_id=market_profile_id,
            description=description,
            lifecycle_stage=lifecycle_stage,
            status=status,
            metadata=metadata,
            extra_columns={"policy_type": policy_type, "params": params or {}},
        )

    def create_state_policy_spec(
        self,
        *,
        name: str,
        policy_type: str = "stateless",
        params: dict[str, Any] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        status: str = "draft",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        if policy_type not in _SUPPORTED_STATE:
            raise ValueError(f"Unsupported state policy {policy_type!r}")
        return self._insert_spec(
            table="state_policy_specs",
            name=name,
            project_id=project_id,
            market_profile_id=market_profile_id,
            description=description,
            lifecycle_stage=lifecycle_stage,
            status=status,
            metadata=metadata,
            extra_columns={"policy_type": policy_type, "params": params or {}},
        )

    def ensure_default_state_policy(
        self,
        *,
        project_id: str | None = None,
        market_profile_id: str | None = None,
    ) -> dict:
        project = self.kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      policy_type, params, lifecycle_stage, status, metadata,
                      created_at, updated_at
               FROM state_policy_specs
               WHERE project_id = ?
                 AND market_profile_id = ?
                 AND name = 'Default Stateless State Policy'
               ORDER BY created_at ASC
               LIMIT 1""",
            [project["id"], profile_id],
        ).fetchone()
        if row:
            return self._policy_spec_row(row, kind="state_policy")
        return self.create_state_policy_spec(
            name="Default Stateless State Policy",
            project_id=project["id"],
            market_profile_id=profile_id,
            lifecycle_stage="experiment",
            status="active",
            metadata={"built_in": True},
        )

    # ------------------------------------------------------------------
    # Runtime
    # ------------------------------------------------------------------

    def construct_portfolio(
        self,
        *,
        decision_date: str,
        alpha_frame: list[dict[str, Any]],
        portfolio_spec_id: str,
        risk_control_spec_id: str | None = None,
        rebalance_policy_spec_id: str | None = None,
        execution_policy_spec_id: str | None = None,
        state_policy_spec_id: str | None = None,
        current_weights: dict[str, float] | None = None,
        portfolio_value: float = 1_000_000,
        lifecycle_stage: str = "experiment",
    ) -> dict:
        portfolio_spec = self.get_portfolio_construction_spec(portfolio_spec_id)
        risk_spec = self.get_risk_control_spec(risk_control_spec_id) if risk_control_spec_id else None
        rebalance_spec = (
            self.get_rebalance_policy_spec(rebalance_policy_spec_id)
            if rebalance_policy_spec_id
            else None
        )
        execution_spec = (
            self.get_execution_policy_spec(execution_policy_spec_id)
            if execution_policy_spec_id
            else self._ensure_default_execution_policy(
                project_id=portfolio_spec["project_id"],
                market_profile_id=portfolio_spec["market_profile_id"],
            )
        )
        state_spec = (
            self.get_state_policy_spec(state_policy_spec_id)
            if state_policy_spec_id
            else self.ensure_default_state_policy(
                project_id=portfolio_spec["project_id"],
                market_profile_id=portfolio_spec["market_profile_id"],
            )
        )
        self._assert_same_scope([portfolio_spec, risk_spec, rebalance_spec, execution_spec, state_spec])

        frame = self._alpha_frame(alpha_frame, decision_date=decision_date)
        constructed = self._construct_weights(frame, portfolio_spec)
        constrained, trace = self._apply_risk_controls(
            constructed,
            risk_spec,
            decision_date=decision_date,
        )
        rebalanced, rebalance_trace = self._apply_rebalance_policy(
            constrained,
            rebalance_spec,
            current_weights=current_weights or {},
            decision_date=decision_date,
        )
        trace.extend(rebalance_trace)
        targets = self._target_rows(
            rebalanced,
            frame,
            decision_date=decision_date,
            portfolio_spec=portfolio_spec,
        )
        orders = self._order_intents(
            targets=targets,
            current_weights=current_weights or {},
            execution_spec=execution_spec,
            decision_date=decision_date,
            market_profile_id=portfolio_spec["market_profile_id"],
            portfolio_value=portfolio_value,
        )

        run = self.kernel.create_run(
            run_type="portfolio_construct",
            project_id=portfolio_spec["project_id"],
            market_profile_id=portfolio_spec["market_profile_id"],
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            created_by="portfolio_assets_3",
            params={
                "decision_date": decision_date,
                "portfolio_spec_id": portfolio_spec_id,
                "risk_control_spec_id": risk_control_spec_id,
                "rebalance_policy_spec_id": rebalance_policy_spec_id,
                "execution_policy_spec_id": execution_policy_spec_id,
                "state_policy_spec_id": state_spec["id"],
                "portfolio_value": portfolio_value,
            },
            input_refs=[
                {"type": "portfolio_construction_spec", "id": portfolio_spec_id},
                *(
                    [{"type": "risk_control_spec", "id": risk_control_spec_id}]
                    if risk_control_spec_id
                    else []
                ),
            ],
        )
        alpha_artifact = self.kernel.create_dataframe_artifact(
            run_id=run["id"],
            artifact_type="alpha_frame",
            frame=frame,
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            metadata={"decision_date": decision_date},
        )
        target_artifact = self.kernel.create_dataframe_artifact(
            run_id=run["id"],
            artifact_type="portfolio_targets",
            frame=pd.DataFrame(targets),
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            metadata={"decision_date": decision_date, "portfolio_spec_id": portfolio_spec_id},
        )
        trace_artifact = self.kernel.create_json_artifact(
            run_id=run["id"],
            artifact_type="constraint_trace",
            payload={"decision_date": decision_date, "trace": trace},
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            metadata={"decision_date": decision_date},
        )
        order_artifact = self.kernel.create_dataframe_artifact(
            run_id=run["id"],
            artifact_type="order_intents",
            frame=pd.DataFrame(orders),
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            metadata={"decision_date": decision_date, "execution_policy_id": execution_spec["id"]},
        )
        profile = self._profile_run(targets=targets, trace=trace, orders=orders)
        portfolio_run = self._insert_portfolio_run(
            run_id=run["id"],
            project_id=portfolio_spec["project_id"],
            market_profile_id=portfolio_spec["market_profile_id"],
            decision_date=decision_date,
            portfolio_spec_id=portfolio_spec_id,
            risk_control_spec_id=risk_control_spec_id,
            rebalance_policy_spec_id=rebalance_policy_spec_id,
            execution_policy_spec_id=execution_spec["id"],
            state_policy_spec_id=state_spec["id"],
            input_artifact_id=alpha_artifact["id"],
            target_artifact_id=target_artifact["id"],
            trace_artifact_id=trace_artifact["id"],
            order_intent_artifact_id=order_artifact["id"],
            profile=profile,
            lifecycle_stage=lifecycle_stage,
        )
        self.kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary=profile,
            qa_summary={"blocking": False, "rules_applied": len(trace)},
        )
        return {
            "run": self.kernel.get_run(run["id"]),
            "portfolio_run": portfolio_run,
            "alpha_artifact": alpha_artifact,
            "target_artifact": target_artifact,
            "trace_artifact": trace_artifact,
            "order_intent_artifact": order_artifact,
            "targets": targets,
            "constraint_trace": trace,
            "order_intents": orders,
            "profile": profile,
            "specs": {
                "portfolio": portfolio_spec,
                "risk": risk_spec,
                "rebalance": rebalance_spec,
                "execution": execution_spec,
                "state": state_spec,
            },
        }

    def compare_builders(
        self,
        *,
        decision_date: str,
        alpha_frame: list[dict[str, Any]],
        portfolio_spec_ids: list[str],
        risk_control_spec_id: str | None = None,
        current_weights: dict[str, float] | None = None,
    ) -> dict:
        runs = []
        for spec_id in portfolio_spec_ids:
            runs.append(
                self.construct_portfolio(
                    decision_date=decision_date,
                    alpha_frame=alpha_frame,
                    portfolio_spec_id=spec_id,
                    risk_control_spec_id=risk_control_spec_id,
                    current_weights=current_weights,
                )
            )
        return {
            "decision_date": decision_date,
            "comparisons": [
                {
                    "portfolio_spec_id": run["specs"]["portfolio"]["id"],
                    "portfolio_spec_name": run["specs"]["portfolio"]["name"],
                    "method": run["specs"]["portfolio"]["method"],
                    "portfolio_run_id": run["portfolio_run"]["id"],
                    "profile": run["profile"],
                    "top_targets": [
                        row for row in run["targets"] if row["target_weight"] > 1e-8
                    ][:10],
                }
                for run in runs
            ],
        }

    # ------------------------------------------------------------------
    # Read APIs
    # ------------------------------------------------------------------

    def get_portfolio_construction_spec(self, spec_id: str) -> dict:
        return self._get_spec(
            "portfolio_construction_specs",
            spec_id,
            select="""id, project_id, market_profile_id, name, description,
                      method, params, lifecycle_stage, status, metadata,
                      created_at, updated_at""",
            parser=lambda row: self._portfolio_spec_row(row),
        )

    def get_risk_control_spec(self, spec_id: str) -> dict:
        return self._get_spec(
            "risk_control_specs",
            spec_id,
            select="""id, project_id, market_profile_id, name, description,
                      rules, params, lifecycle_stage, status, metadata,
                      created_at, updated_at""",
            parser=lambda row: self._risk_spec_row(row),
        )

    def get_rebalance_policy_spec(self, spec_id: str) -> dict:
        return self._get_spec(
            "rebalance_policy_specs",
            spec_id,
            select="""id, project_id, market_profile_id, name, description,
                      policy_type, params, lifecycle_stage, status, metadata,
                      created_at, updated_at""",
            parser=lambda row: self._policy_spec_row(row, kind="rebalance_policy"),
        )

    def get_execution_policy_spec(self, spec_id: str) -> dict:
        return self._get_spec(
            "execution_policy_specs",
            spec_id,
            select="""id, project_id, market_profile_id, name, description,
                      policy_type, params, lifecycle_stage, status, metadata,
                      created_at, updated_at""",
            parser=lambda row: self._policy_spec_row(row, kind="execution_policy"),
        )

    def get_state_policy_spec(self, spec_id: str) -> dict:
        return self._get_spec(
            "state_policy_specs",
            spec_id,
            select="""id, project_id, market_profile_id, name, description,
                      policy_type, params, lifecycle_stage, status, metadata,
                      created_at, updated_at""",
            parser=lambda row: self._policy_spec_row(row, kind="state_policy"),
        )

    def get_portfolio_run(self, portfolio_run_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id, decision_date,
                      portfolio_construction_spec_id, risk_control_spec_id,
                      rebalance_policy_spec_id, execution_policy_spec_id,
                      state_policy_spec_id, input_artifact_id, target_artifact_id,
                      trace_artifact_id, order_intent_artifact_id, profile,
                      status, lifecycle_stage, created_at, completed_at
               FROM portfolio_runs
               WHERE id = ?""",
            [portfolio_run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Portfolio run {portfolio_run_id} not found")
        return self._portfolio_run_row(row)

    def list_portfolio_runs(self, *, limit: int = 50) -> list[dict]:
        rows = get_connection().execute(
            """SELECT id, run_id, project_id, market_profile_id, decision_date,
                      portfolio_construction_spec_id, risk_control_spec_id,
                      rebalance_policy_spec_id, execution_policy_spec_id,
                      state_policy_spec_id, input_artifact_id, target_artifact_id,
                      trace_artifact_id, order_intent_artifact_id, profile,
                      status, lifecycle_stage, created_at, completed_at
               FROM portfolio_runs
               ORDER BY created_at DESC
               LIMIT ?""",
            [limit],
        ).fetchall()
        return [self._portfolio_run_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _insert_spec(
        self,
        *,
        table: str,
        name: str,
        extra_columns: dict[str, Any],
        project_id: str | None,
        market_profile_id: str | None,
        description: str | None,
        lifecycle_stage: str,
        status: str,
        metadata: dict[str, Any] | None,
    ) -> dict:
        project = self.kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        spec_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        base = {
            "id": spec_id,
            "project_id": project["id"],
            "market_profile_id": profile_id,
            "name": name.strip(),
            "description": description,
            "lifecycle_stage": lifecycle_stage,
            "status": status,
            "metadata": json.dumps(metadata or {}, default=str),
            "created_at": now,
            "updated_at": now,
        }
        columns = [*base.keys(), *extra_columns.keys()]
        values = [
            *base.values(),
            *[
                json.dumps(value, default=str)
                if isinstance(value, (dict, list))
                else value
                for value in extra_columns.values()
            ],
        ]
        placeholders = ", ".join(["?"] * len(columns))
        get_connection().execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            values,
        )
        if table == "portfolio_construction_specs":
            return self.get_portfolio_construction_spec(spec_id)
        if table == "risk_control_specs":
            return self.get_risk_control_spec(spec_id)
        if table == "rebalance_policy_specs":
            return self.get_rebalance_policy_spec(spec_id)
        if table == "execution_policy_specs":
            return self.get_execution_policy_spec(spec_id)
        if table == "state_policy_specs":
            return self.get_state_policy_spec(spec_id)
        raise ValueError(f"Unknown spec table {table!r}")

    def _ensure_default_execution_policy(
        self,
        *,
        project_id: str,
        market_profile_id: str,
    ) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      policy_type, params, lifecycle_stage, status, metadata,
                      created_at, updated_at
               FROM execution_policy_specs
               WHERE project_id = ?
                 AND market_profile_id = ?
                 AND name = 'Default Next Open Execution'
               ORDER BY created_at ASC
               LIMIT 1""",
            [project_id, market_profile_id],
        ).fetchone()
        if row:
            return self._policy_spec_row(row, kind="execution_policy")
        return self.create_execution_policy_spec(
            name="Default Next Open Execution",
            project_id=project_id,
            market_profile_id=market_profile_id,
            policy_type="next_open",
            params={"price_field": "open"},
            status="active",
            metadata={"built_in": True},
        )

    def _alpha_frame(self, rows: list[dict[str, Any]], *, decision_date: str) -> pd.DataFrame:
        if not rows:
            raise ValueError("alpha_frame must contain at least one asset row")
        frame = pd.DataFrame(rows).copy()
        if "asset_id" not in frame.columns:
            raise ValueError("alpha_frame rows must include asset_id")
        if "score" not in frame.columns:
            raise ValueError("alpha_frame rows must include score")
        frame["asset_id"] = frame["asset_id"].astype(str)
        frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
        frame = frame.dropna(subset=["asset_id", "score"])
        if frame.empty:
            raise ValueError("alpha_frame has no numeric scores")
        frame["date"] = decision_date
        if "confidence" not in frame.columns:
            frame["confidence"] = 1.0
        return frame

    def _construct_weights(self, frame: pd.DataFrame, spec: dict) -> dict[str, float]:
        method = spec["method"]
        params = spec.get("params") or {}
        score_column = params.get("score_column", "score")
        top_n = int(params.get("top_n") or len(frame))
        selected = frame.sort_values(score_column, ascending=False).head(top_n).copy()
        if selected.empty:
            return {}

        if method == "equal_weight":
            weight = 1.0 / len(selected)
            return {row.asset_id: weight for row in selected.itertuples()}

        if method == "score_proportional":
            raw = selected[score_column].clip(lower=0).astype(float)
            if float(raw.sum()) <= 0:
                raw = selected[score_column].rank(method="first").astype(float)
            total = float(raw.sum())
            return {
                str(asset_id): float(value / total)
                for asset_id, value in zip(selected["asset_id"], raw, strict=False)
            }

        if method == "inverse_vol":
            vol_col = params.get("volatility_column", "volatility")
            if vol_col not in selected.columns:
                raise ValueError("inverse_vol requires a volatility column in alpha_frame")
            vol = pd.to_numeric(selected[vol_col], errors="coerce").replace(0, math.nan)
            inv = (1.0 / vol).replace([math.inf, -math.inf], math.nan).fillna(0.0)
            if float(inv.sum()) <= 0:
                raise ValueError("inverse_vol requires positive volatility values")
            total = float(inv.sum())
            return {
                str(asset_id): float(value / total)
                for asset_id, value in zip(selected["asset_id"], inv, strict=False)
            }

        raise ValueError(f"Unsupported portfolio construction method {method!r}")

    def _apply_risk_controls(
        self,
        weights: dict[str, float],
        risk_spec: dict | None,
        *,
        decision_date: str,
    ) -> tuple[dict[str, float], list[dict]]:
        if not risk_spec:
            return self._normalize(weights), []
        current = self._normalize(weights)
        trace: list[dict] = []
        for index, rule in enumerate(risk_spec.get("rules") or []):
            rule_name = rule.get("rule")
            if rule_name == "max_positions":
                limit = int(rule.get("max_positions") or rule.get("limit") or len(current))
                if limit < len([w for w in current.values() if w > 1e-8]):
                    before = dict(current)
                    keep = sorted(current.items(), key=lambda item: item[1], reverse=True)[:limit]
                    kept_assets = {asset_id for asset_id, _ in keep}
                    current = {asset_id: weight for asset_id, weight in current.items() if asset_id in kept_assets}
                    current = self._normalize(current)
                    trace.extend(
                        self._trace_changes(
                            before,
                            current,
                            decision_date=decision_date,
                            rule_id="max_positions",
                            reason=f"limited positions to {limit}",
                            rule_order=index,
                        )
                    )
            elif rule_name == "max_single_weight":
                max_weight = float(rule.get("max_weight") or rule.get("max_single_weight") or 1.0)
                if max_weight <= 0:
                    raise ValueError("max_single_weight must be positive")
                before = dict(current)
                current = self._cap_weights(current, max_weight)
                trace.extend(
                    self._trace_changes(
                        before,
                        current,
                        decision_date=decision_date,
                        rule_id="max_single_weight",
                        reason=f"capped each asset at {max_weight:.4f}",
                        rule_order=index,
                    )
                )
            elif rule_name == "turnover_budget":
                continue
            else:
                raise ValueError(f"Unsupported risk rule {rule_name!r}")
        return self._normalize(current), trace

    def _apply_rebalance_policy(
        self,
        weights: dict[str, float],
        rebalance_spec: dict | None,
        *,
        current_weights: dict[str, float],
        decision_date: str,
    ) -> tuple[dict[str, float], list[dict]]:
        if not rebalance_spec or rebalance_spec["policy_type"] == "none":
            return weights, []
        if rebalance_spec["policy_type"] != "band":
            raise ValueError(f"Unsupported rebalance policy {rebalance_spec['policy_type']!r}")

        band = float((rebalance_spec.get("params") or {}).get("band", 0.0))
        before = dict(weights)
        adjusted = dict(weights)
        for asset_id, target in list(weights.items()):
            current = float(current_weights.get(asset_id, 0.0))
            if abs(target - current) < band:
                adjusted[asset_id] = current
        adjusted = self._normalize(adjusted)
        trace = self._trace_changes(
            before,
            adjusted,
            decision_date=decision_date,
            rule_id="rebalance_band",
            reason=f"kept existing weights when change was below {band:.4f}",
            rule_order=10_000,
        )
        return adjusted, trace

    def _target_rows(
        self,
        weights: dict[str, float],
        frame: pd.DataFrame,
        *,
        decision_date: str,
        portfolio_spec: dict,
    ) -> list[dict]:
        score_by_asset = dict(zip(frame["asset_id"], frame["score"], strict=False))
        confidence_by_asset = dict(zip(frame["asset_id"], frame["confidence"], strict=False))
        rows = []
        for rank, (asset_id, weight) in enumerate(
            sorted(weights.items(), key=lambda item: item[1], reverse=True),
            start=1,
        ):
            rows.append(
                {
                    "date": decision_date,
                    "asset_id": asset_id,
                    "target_weight": round(float(weight), 12),
                    "cash_weight": 0.0,
                    "score": float(score_by_asset.get(asset_id, 0.0)),
                    "rank": rank,
                    "confidence": float(confidence_by_asset.get(asset_id, 1.0)),
                    "construction_reason": (
                        f"{portfolio_spec['method']} from {portfolio_spec['name']}"
                    ),
                }
            )
        return rows

    def _order_intents(
        self,
        *,
        targets: list[dict],
        current_weights: dict[str, float],
        execution_spec: dict,
        decision_date: str,
        market_profile_id: str,
        portfolio_value: float,
    ) -> list[dict]:
        if execution_spec["policy_type"] != "next_open":
            raise ValueError(f"Unsupported execution policy {execution_spec['policy_type']!r}")
        market = _MARKET_BY_PROFILE.get(market_profile_id, normalize_market(None))
        execution_date = offset_trading_days(decision_date, 1, market=market)
        target_by_asset = {row["asset_id"]: float(row["target_weight"]) for row in targets}
        assets = sorted(set(current_weights) | set(target_by_asset))
        orders = []
        for asset_id in assets:
            before = float(current_weights.get(asset_id, 0.0))
            after = float(target_by_asset.get(asset_id, 0.0))
            delta = after - before
            if abs(delta) <= 1e-8:
                continue
            orders.append(
                {
                    "decision_date": decision_date,
                    "execution_date": str(execution_date),
                    "asset_id": asset_id,
                    "side": "buy" if delta > 0 else "sell",
                    "current_weight": round(before, 12),
                    "target_weight": round(after, 12),
                    "delta_weight": round(delta, 12),
                    "estimated_value": round(float(portfolio_value) * abs(delta), 2),
                    "execution_policy_id": execution_spec["id"],
                    "price_field": (execution_spec.get("params") or {}).get("price_field", "open"),
                    "reason": "move current portfolio toward target at next session open",
                }
            )
        return orders

    def _insert_portfolio_run(
        self,
        *,
        run_id: str,
        project_id: str,
        market_profile_id: str,
        decision_date: str,
        portfolio_spec_id: str,
        risk_control_spec_id: str | None,
        rebalance_policy_spec_id: str | None,
        execution_policy_spec_id: str,
        state_policy_spec_id: str,
        input_artifact_id: str,
        target_artifact_id: str,
        trace_artifact_id: str,
        order_intent_artifact_id: str,
        profile: dict,
        lifecycle_stage: str,
    ) -> dict:
        portfolio_run_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO portfolio_runs
               (id, run_id, project_id, market_profile_id, decision_date,
                portfolio_construction_spec_id, risk_control_spec_id,
                rebalance_policy_spec_id, execution_policy_spec_id,
                state_policy_spec_id, input_artifact_id, target_artifact_id,
                trace_artifact_id, order_intent_artifact_id, profile, status,
                lifecycle_stage, created_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'completed', ?, ?, ?)""",
            [
                portfolio_run_id,
                run_id,
                project_id,
                market_profile_id,
                decision_date,
                portfolio_spec_id,
                risk_control_spec_id,
                rebalance_policy_spec_id,
                execution_policy_spec_id,
                state_policy_spec_id,
                input_artifact_id,
                target_artifact_id,
                trace_artifact_id,
                order_intent_artifact_id,
                json.dumps(profile, default=str),
                lifecycle_stage,
                utc_now_naive(),
                utc_now_naive(),
            ],
        )
        return self.get_portfolio_run(portfolio_run_id)

    @staticmethod
    def _normalize(weights: dict[str, float]) -> dict[str, float]:
        cleaned = {asset: max(float(weight), 0.0) for asset, weight in weights.items()}
        total = sum(cleaned.values())
        if total <= 0:
            return {}
        return {asset: weight / total for asset, weight in cleaned.items() if weight > 1e-12}

    def _cap_weights(self, weights: dict[str, float], cap: float) -> dict[str, float]:
        current = self._normalize(weights)
        if not current:
            return {}
        capped: dict[str, float] = {}
        remaining_assets = set(current)
        remaining_weight = 1.0
        while remaining_assets:
            uncapped_total = sum(current[a] for a in remaining_assets)
            if uncapped_total <= 0:
                break
            changed = False
            for asset in list(remaining_assets):
                proposed = current[asset] / uncapped_total * remaining_weight
                if proposed >= cap:
                    capped[asset] = cap
                    remaining_weight -= cap
                    remaining_assets.remove(asset)
                    changed = True
            if not changed:
                for asset in remaining_assets:
                    capped[asset] = current[asset] / uncapped_total * remaining_weight
                break
        return self._normalize(capped)

    def _trace_changes(
        self,
        before: dict[str, float],
        after: dict[str, float],
        *,
        decision_date: str,
        rule_id: str,
        reason: str,
        rule_order: int,
    ) -> list[dict]:
        rows = []
        for asset_id in sorted(set(before) | set(after)):
            before_weight = float(before.get(asset_id, 0.0))
            after_weight = float(after.get(asset_id, 0.0))
            if abs(before_weight - after_weight) <= 1e-10:
                continue
            rows.append(
                {
                    "date": decision_date,
                    "asset_id": asset_id,
                    "before_weight": round(before_weight, 12),
                    "after_weight": round(after_weight, 12),
                    "rule_id": rule_id,
                    "rule_order": rule_order,
                    "reason": reason,
                }
            )
        return rows

    def _profile_run(
        self,
        *,
        targets: list[dict],
        trace: list[dict],
        orders: list[dict],
    ) -> dict:
        live = [row for row in targets if float(row["target_weight"]) > 1e-8]
        weights = [float(row["target_weight"]) for row in live]
        return {
            "target_count": len(targets),
            "active_positions": len(live),
            "gross_exposure": round(sum(abs(weight) for weight in weights), 12),
            "max_weight": round(max(weights), 12) if weights else 0.0,
            "min_weight": round(min(weights), 12) if weights else 0.0,
            "constraint_trace_count": len(trace),
            "order_intent_count": len(orders),
            "turnover_estimate": round(sum(abs(float(o["delta_weight"])) for o in orders), 12),
        }

    def _assert_same_scope(self, specs: list[dict | None]) -> None:
        present = [spec for spec in specs if spec is not None]
        projects = {spec["project_id"] for spec in present}
        profiles = {spec["market_profile_id"] for spec in present}
        if len(projects) > 1 or len(profiles) > 1:
            raise ValueError("Portfolio, risk, rebalance, execution, and state specs must share scope")

    def _get_spec(self, table: str, spec_id: str, *, select: str, parser) -> dict:
        row = get_connection().execute(
            f"SELECT {select} FROM {table} WHERE id = ?",
            [spec_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"{table} spec {spec_id} not found")
        return parser(row)

    def _portfolio_spec_row(self, row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "method": row[5],
            "params": _json(row[6], {}),
            "lifecycle_stage": row[7],
            "status": row[8],
            "metadata": _json(row[9], {}),
            "created_at": str(row[10]) if row[10] is not None else None,
            "updated_at": str(row[11]) if row[11] is not None else None,
        }

    def _risk_spec_row(self, row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "rules": _json(row[5], []),
            "params": _json(row[6], {}),
            "lifecycle_stage": row[7],
            "status": row[8],
            "metadata": _json(row[9], {}),
            "created_at": str(row[10]) if row[10] is not None else None,
            "updated_at": str(row[11]) if row[11] is not None else None,
        }

    def _policy_spec_row(self, row, *, kind: str) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "policy_type": row[5],
            "params": _json(row[6], {}),
            "lifecycle_stage": row[7],
            "status": row[8],
            "metadata": _json(row[9], {}),
            "created_at": str(row[10]) if row[10] is not None else None,
            "updated_at": str(row[11]) if row[11] is not None else None,
            "asset_type": kind,
        }

    def _portfolio_run_row(self, row) -> dict:
        return {
            "id": row[0],
            "run_id": row[1],
            "project_id": row[2],
            "market_profile_id": row[3],
            "decision_date": str(row[4]) if row[4] is not None else None,
            "portfolio_construction_spec_id": row[5],
            "risk_control_spec_id": row[6],
            "rebalance_policy_spec_id": row[7],
            "execution_policy_spec_id": row[8],
            "state_policy_spec_id": row[9],
            "input_artifact_id": row[10],
            "target_artifact_id": row[11],
            "trace_artifact_id": row[12],
            "order_intent_artifact_id": row[13],
            "profile": _json(row[14], {}),
            "status": row[15],
            "lifecycle_stage": row[16],
            "created_at": str(row[17]) if row[17] is not None else None,
            "completed_at": str(row[18]) if row[18] is not None else None,
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
