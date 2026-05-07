"""Agent research planning, QA gate, and playbooks for QAgent 3.0."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from backend.db import get_connection
from backend.services.research_kernel_service import ResearchKernelService
from backend.time_utils import utc_now_naive


_DEFAULT_THRESHOLDS = {
    "min_coverage": 0.95,
    "min_sharpe": 0.5,
    "max_drawdown_floor": -0.25,
    "max_annual_turnover": 15.0,
}


class AgentResearch3Service:
    """Own agent research plans, QA gate checks, promotion policy, playbooks."""

    def __init__(self, *, kernel_service: ResearchKernelService | None = None) -> None:
        self.kernel = kernel_service or ResearchKernelService()

    # ------------------------------------------------------------------
    # Playbooks
    # ------------------------------------------------------------------

    def ensure_builtin_playbooks(self) -> list[dict]:
        now = utc_now_naive()
        conn = get_connection()
        for item in _BUILTIN_PLAYBOOKS:
            existing = conn.execute(
                "SELECT id FROM research_playbooks WHERE id = ?",
                [item["id"]],
            ).fetchone()
            if existing:
                continue
            conn.execute(
                """INSERT INTO research_playbooks
                   (id, name, category, description, steps, optimization_targets,
                    required_assets, status, metadata, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)""",
                [
                    item["id"],
                    item["name"],
                    item["category"],
                    item["description"],
                    json.dumps(item["steps"], default=str),
                    json.dumps(item["optimization_targets"], default=str),
                    json.dumps(item["required_assets"], default=str),
                    json.dumps({"built_in": True}, default=str),
                    now,
                    now,
                ],
            )
        return self.list_playbooks()

    def list_playbooks(self) -> list[dict]:
        rows = get_connection().execute(
            """SELECT id, name, category, description, steps,
                      optimization_targets, required_assets, status, metadata,
                      created_at, updated_at
               FROM research_playbooks
               ORDER BY id"""
        ).fetchall()
        return [self._playbook_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Plans and trials
    # ------------------------------------------------------------------

    def create_research_plan(
        self,
        *,
        hypothesis: str,
        playbook_id: str | None = None,
        search_space: dict[str, Any] | None = None,
        budget: dict[str, Any] | None = None,
        stop_conditions: dict[str, Any] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        created_by: str = "agent",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        project = self.kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        if playbook_id:
            self.get_playbook(playbook_id)
        plan_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO agent_research_plans
               (id, project_id, market_profile_id, hypothesis, playbook_id,
                search_space, budget, stop_conditions, status, created_by,
                metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)""",
            [
                plan_id,
                project["id"],
                profile_id,
                hypothesis,
                playbook_id,
                json.dumps(search_space or {}, default=str),
                json.dumps(self._normalize_budget(budget), default=str),
                json.dumps(stop_conditions or {}, default=str),
                created_by,
                json.dumps(metadata or {}, default=str),
                now,
                now,
            ],
        )
        return self.get_plan(plan_id)

    def get_plan(self, plan_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, hypothesis,
                      playbook_id, search_space, budget, stop_conditions,
                      status, created_by, metadata, created_at, updated_at
               FROM agent_research_plans
               WHERE id = ?""",
            [plan_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Agent research plan {plan_id} not found")
        plan = self._plan_row(row)
        plan["budget_state"] = self.check_budget(plan_id)
        return plan

    def list_plans(
        self,
        *,
        project_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, project_id, market_profile_id, hypothesis,
                          playbook_id, search_space, budget, stop_conditions,
                          status, created_by, metadata, created_at, updated_at
                   FROM agent_research_plans
                   WHERE 1 = 1"""
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        plans = [self._plan_row(row) for row in rows]
        budget_states = self._budget_states_for_plan_ids([plan["id"] for plan in plans])
        for plan in plans:
            plan["budget_state"] = budget_states.get(plan["id"]) or self._budget_state(
                plan["id"],
                plan["budget"],
                used_trials=0,
            )
        return plans

    def record_trial(
        self,
        plan_id: str,
        *,
        trial_type: str,
        params: dict[str, Any] | None = None,
        result_refs: list[dict[str, Any]] | None = None,
        metrics: dict[str, Any] | None = None,
        qa_report_id: str | None = None,
        status: str = "completed",
    ) -> dict:
        budget_state = self.check_budget(plan_id)
        if not budget_state["can_run_more"]:
            raise ValueError("Plan budget exhausted; cannot record more trials")
        trial_id = uuid.uuid4().hex[:12]
        trial_index = self._next_trial_index(plan_id)
        get_connection().execute(
            """INSERT INTO agent_research_trials
               (id, plan_id, trial_index, trial_type, params, result_refs,
                metrics, qa_report_id, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                trial_id,
                plan_id,
                trial_index,
                trial_type,
                json.dumps(params or {}, default=str),
                json.dumps(result_refs or [], default=str),
                json.dumps(metrics or {}, default=str),
                qa_report_id,
                status,
                utc_now_naive(),
            ],
        )
        return self.get_trial(trial_id)

    def record_trials(
        self,
        plan_id: str,
        *,
        trials: list[dict[str, Any]],
        dedupe_by_params: bool = True,
    ) -> dict:
        """Record many trials with one budget check and one batch insert.

        Agent strategy search often evaluates a grid or candidate population in
        a batch.  Recording those results one-by-one creates unnecessary API and
        DB round trips, so this method writes the metadata in a compact path
        while preserving the same budget and trial-index contracts.
        """
        if not trials:
            return {
                "plan_id": plan_id,
                "inserted_count": 0,
                "skipped_count": 0,
                "inserted_trials": [],
                "skipped_trials": [],
                "budget_state": self.check_budget(plan_id),
            }

        plan = self.get_plan(plan_id)
        budget_state = plan["budget_state"]
        remaining = int(budget_state["remaining_trials"])
        if remaining <= 0:
            raise ValueError("Plan budget exhausted; cannot record more trials")

        existing_hashes = (
            self._existing_trial_param_hashes(plan_id)
            if dedupe_by_params
            else set()
        )
        seen_hashes: set[str] = set()
        accepted: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for item in trials:
            trial_type = str(item.get("trial_type") or "").strip()
            if not trial_type:
                raise ValueError("Each trial must include trial_type")
            params = item.get("params") or {}
            params_hash = self._stable_hash({"trial_type": trial_type, "params": params})
            if dedupe_by_params and (params_hash in existing_hashes or params_hash in seen_hashes):
                skipped.append(
                    {
                        "trial_type": trial_type,
                        "params": params,
                        "reason": "duplicate_params",
                        "params_hash": params_hash,
                    }
                )
                continue
            if len(accepted) >= remaining:
                skipped.append(
                    {
                        "trial_type": trial_type,
                        "params": params,
                        "reason": "budget_exhausted",
                        "params_hash": params_hash,
                    }
                )
                continue
            accepted.append({**item, "trial_type": trial_type, "params_hash": params_hash})
            seen_hashes.add(params_hash)

        if not accepted:
            return {
                "plan_id": plan_id,
                "inserted_count": 0,
                "skipped_count": len(skipped),
                "inserted_trials": [],
                "skipped_trials": skipped,
                "budget_state": self.check_budget(plan_id),
            }

        start_index = self._next_trial_index(plan_id)
        now = utc_now_naive()
        rows: list[list[Any]] = []
        inserted_ids: list[str] = []
        for offset, item in enumerate(accepted):
            trial_id = uuid.uuid4().hex[:12]
            inserted_ids.append(trial_id)
            params_payload = {
                **(item.get("params") or {}),
                "_trial_params_hash": item["params_hash"],
            }
            rows.append(
                [
                    trial_id,
                    plan_id,
                    start_index + offset,
                    item["trial_type"],
                    json.dumps(params_payload, default=str),
                    json.dumps(item.get("result_refs") or [], default=str),
                    json.dumps(item.get("metrics") or {}, default=str),
                    item.get("qa_report_id"),
                    item.get("status") or "completed",
                    now,
                ]
            )

        conn = get_connection()
        conn.executemany(
            """INSERT INTO agent_research_trials
               (id, plan_id, trial_index, trial_type, params, result_refs,
                metrics, qa_report_id, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        inserted_trials = self.get_trials(inserted_ids)
        return {
            "plan_id": plan_id,
            "inserted_count": len(inserted_trials),
            "skipped_count": len(skipped),
            "inserted_trials": inserted_trials,
            "skipped_trials": skipped,
            "budget_state": self.check_budget(plan_id),
        }

    def get_trial(self, trial_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, plan_id, trial_index, trial_type, params,
                      result_refs, metrics, qa_report_id, status, created_at
               FROM agent_research_trials
               WHERE id = ?""",
            [trial_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Agent research trial {trial_id} not found")
        return self._trial_row(row)

    def get_trials(self, trial_ids: list[str]) -> list[dict]:
        if not trial_ids:
            return []
        placeholders = ", ".join(["?"] * len(trial_ids))
        rows = get_connection().execute(
            f"""SELECT id, plan_id, trial_index, trial_type, params,
                       result_refs, metrics, qa_report_id, status, created_at
                FROM agent_research_trials
                WHERE id IN ({placeholders})""",
            trial_ids,
        ).fetchall()
        by_id = {row[0]: self._trial_row(row) for row in rows}
        return [by_id[trial_id] for trial_id in trial_ids if trial_id in by_id]

    def list_trials(self, plan_id: str, *, limit: int = 100) -> list[dict]:
        self.get_plan(plan_id)
        rows = get_connection().execute(
            """SELECT id, plan_id, trial_index, trial_type, params,
                      result_refs, metrics, qa_report_id, status, created_at
               FROM agent_research_trials
               WHERE plan_id = ?
               ORDER BY trial_index
               LIMIT ?""",
            [plan_id, int(limit)],
        ).fetchall()
        return [self._trial_row(row) for row in rows]

    def check_budget(self, plan_id: str) -> dict:
        row = get_connection().execute(
            "SELECT budget FROM agent_research_plans WHERE id = ?",
            [plan_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Agent research plan {plan_id} not found")
        budget = self._normalize_budget(_json(row[0], {}))
        used = get_connection().execute(
            "SELECT COUNT(*) FROM agent_research_trials WHERE plan_id = ?",
            [plan_id],
        ).fetchone()[0]
        max_trials = int(budget.get("max_trials", 1))
        return {
            "plan_id": plan_id,
            "max_trials": max_trials,
            "used_trials": int(used),
            "remaining_trials": max(max_trials - int(used), 0),
            "can_run_more": int(used) < max_trials,
            "budget": budget,
        }

    def get_plan_performance(
        self,
        plan_id: str,
        *,
        primary_metric: str = "sharpe",
        top_n: int = 10,
    ) -> dict:
        """Return compact trial ranking and metric ranges for agent search."""
        plan = self.get_plan(plan_id)
        rows = get_connection().execute(
            """SELECT id, plan_id, trial_index, trial_type, params,
                      result_refs, metrics, qa_report_id, status, created_at
               FROM agent_research_trials
               WHERE plan_id = ?
               ORDER BY trial_index""",
            [plan_id],
        ).fetchall()
        trials = [self._trial_row(row) for row in rows]
        metric_ranges = self._metric_ranges(trials)
        ranked = sorted(
            [
                trial
                for trial in trials
                if self._metric_number(trial["metrics"], primary_metric) is not None
            ],
            key=lambda trial: self._metric_number(trial["metrics"], primary_metric) or float("-inf"),
            reverse=True,
        )
        return {
            "plan": plan,
            "primary_metric": primary_metric,
            "trial_count": len(trials),
            "completed_count": sum(1 for trial in trials if trial["status"] == "completed"),
            "failed_count": sum(1 for trial in trials if trial["status"] == "failed"),
            "metric_ranges": metric_ranges,
            "best_trial": ranked[0] if ranked else None,
            "top_trials": ranked[: max(int(top_n), 0)],
            "budget_state": plan["budget_state"],
        }

    # ------------------------------------------------------------------
    # QA gate and promotion
    # ------------------------------------------------------------------

    def evaluate_qa(
        self,
        *,
        source_type: str,
        source_id: str,
        metrics: dict[str, Any] | None = None,
        artifact_refs: list[dict[str, Any]] | None = None,
        project_id: str | None = None,
        market_profile_id: str | None = None,
    ) -> dict:
        project = self.kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        metric_data = metrics or {}
        findings = self._qa_findings(
            metric_data,
            artifact_refs or [],
            source_type=source_type,
            project_id=project["id"],
            market_profile_id=profile_id,
        )
        blocking = any(item["blocking"] for item in findings)
        has_warning = any(item["severity"] == "warning" for item in findings)
        status = "fail" if blocking else ("warning" if has_warning else "pass")
        qa_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO qa_gate_results
               (id, project_id, market_profile_id, source_type, source_id,
                status, blocking, findings, metrics, artifact_refs, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                qa_id,
                project["id"],
                profile_id,
                source_type,
                source_id,
                status,
                blocking,
                json.dumps(findings, default=str),
                json.dumps(metric_data, default=str),
                json.dumps(artifact_refs or [], default=str),
                utc_now_naive(),
            ],
        )
        return self.get_qa_report(qa_id)

    def get_qa_report(self, qa_report_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, source_type, source_id,
                      status, blocking, findings, metrics, artifact_refs,
                      created_at
               FROM qa_gate_results
               WHERE id = ?""",
            [qa_report_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"QA report {qa_report_id} not found")
        return self._qa_row(row)

    def list_qa_reports(
        self,
        *,
        source_type: str | None = None,
        source_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, project_id, market_profile_id, source_type, source_id,
                          status, blocking, findings, metrics, artifact_refs,
                          created_at
                   FROM qa_gate_results
                   WHERE 1 = 1"""
        params: list[Any] = []
        if source_type:
            query += " AND source_type = ?"
            params.append(source_type)
        if source_id:
            query += " AND source_id = ?"
            params.append(source_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._qa_row(row) for row in rows]

    def ensure_default_promotion_policy(
        self,
        *,
        project_id: str | None = None,
        market_profile_id: str | None = None,
    ) -> dict:
        project = self.kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, policy_type,
                      thresholds, status, metadata, created_at, updated_at
               FROM promotion_policies
               WHERE project_id = ? AND market_profile_id = ?
                 AND name = 'Default Quant Promotion Policy'
               LIMIT 1""",
            [project["id"], profile_id],
        ).fetchone()
        if row:
            return self._policy_row(row)
        policy_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO promotion_policies
               (id, project_id, market_profile_id, name, policy_type,
                thresholds, status, metadata, created_at, updated_at)
               VALUES (?, ?, ?, 'Default Quant Promotion Policy',
                       'default_quant', ?, 'active', ?, ?, ?)""",
            [
                policy_id,
                project["id"],
                profile_id,
                json.dumps(_DEFAULT_THRESHOLDS, default=str),
                json.dumps({"built_in": True}, default=str),
                now,
                now,
            ],
        )
        return self.ensure_default_promotion_policy(
            project_id=project["id"],
            market_profile_id=profile_id,
        )

    def evaluate_promotion(
        self,
        *,
        source_type: str,
        source_id: str,
        qa_report_id: str,
        metrics: dict[str, Any] | None = None,
        policy_id: str | None = None,
        approved_by: str = "agent",
        rationale: str | None = None,
    ) -> dict:
        qa = self.get_qa_report(qa_report_id)
        if qa["blocking"]:
            raise ValueError("QA report is blocking; cannot promote")
        policy = (
            self.get_promotion_policy(policy_id)
            if policy_id
            else self.ensure_default_promotion_policy(
                project_id=qa["project_id"],
                market_profile_id=qa["market_profile_id"],
            )
        )
        failures = self._promotion_failures(metrics or qa.get("metrics") or {}, policy["thresholds"])
        decision = "promoted" if not failures and qa["status"] in {"pass", "warning"} else "rejected"
        record_id = uuid.uuid4().hex[:12]
        get_connection().execute(
            """INSERT INTO promotion_records
               (id, project_id, source_type, source_id, target_type, target_id,
                decision, policy_snapshot, qa_summary, approved_by, rationale,
                created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                record_id,
                qa["project_id"],
                source_type,
                source_id,
                source_type,
                source_id,
                decision,
                json.dumps({"policy": policy, "failures": failures}, default=str),
                json.dumps(qa, default=str),
                approved_by,
                rationale,
                utc_now_naive(),
            ],
        )
        return {
            "id": record_id,
            "project_id": qa["project_id"],
            "source_type": source_type,
            "source_id": source_id,
            "target_type": source_type,
            "target_id": source_id,
            "decision": decision,
            "policy_snapshot": {"policy": policy, "failures": failures},
            "qa_summary": qa,
            "failures": failures,
            "approved_by": approved_by,
            "rationale": rationale,
            "created_at": str(utc_now_naive()),
            "policy": policy,
            "qa_report": qa,
        }

    def get_promotion_policy(self, policy_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, policy_type,
                      thresholds, status, metadata, created_at, updated_at
               FROM promotion_policies
               WHERE id = ?""",
            [policy_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Promotion policy {policy_id} not found")
        return self._policy_row(row)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def get_playbook(self, playbook_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, name, category, description, steps,
                      optimization_targets, required_assets, status, metadata,
                      created_at, updated_at
               FROM research_playbooks
               WHERE id = ?""",
            [playbook_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Research playbook {playbook_id} not found")
        return self._playbook_row(row)

    @staticmethod
    def _normalize_budget(budget: dict[str, Any] | None) -> dict[str, Any]:
        result = dict(budget or {})
        result.setdefault("max_trials", 10)
        result.setdefault("max_wall_minutes", 120)
        result.setdefault("max_parallel_trials", 1)
        return result

    @staticmethod
    def _budget_state(
        plan_id: str,
        budget: dict[str, Any],
        *,
        used_trials: int,
    ) -> dict:
        normalized = AgentResearch3Service._normalize_budget(budget)
        max_trials = int(normalized.get("max_trials", 1))
        used = int(used_trials)
        return {
            "plan_id": plan_id,
            "max_trials": max_trials,
            "used_trials": used,
            "remaining_trials": max(max_trials - used, 0),
            "can_run_more": used < max_trials,
            "budget": normalized,
        }

    def _budget_states_for_plan_ids(self, plan_ids: list[str]) -> dict[str, dict]:
        if not plan_ids:
            return {}
        placeholders = ", ".join(["?"] * len(plan_ids))
        budget_rows = get_connection().execute(
            f"""SELECT id, budget
                FROM agent_research_plans
                WHERE id IN ({placeholders})""",
            plan_ids,
        ).fetchall()
        count_rows = get_connection().execute(
            f"""SELECT plan_id, COUNT(*) AS used_trials
                FROM agent_research_trials
                WHERE plan_id IN ({placeholders})
                GROUP BY plan_id""",
            plan_ids,
        ).fetchall()
        used_by_plan = {row[0]: int(row[1] or 0) for row in count_rows}
        return {
            row[0]: self._budget_state(
                row[0],
                _json(row[1], {}),
                used_trials=used_by_plan.get(row[0], 0),
            )
            for row in budget_rows
        }

    @staticmethod
    def _stable_hash(payload: dict[str, Any]) -> str:
        data = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(data.encode("utf-8")).hexdigest()[:16]

    def _existing_trial_param_hashes(self, plan_id: str) -> set[str]:
        rows = get_connection().execute(
            """SELECT params
               FROM agent_research_trials
               WHERE plan_id = ?""",
            [plan_id],
        ).fetchall()
        hashes: set[str] = set()
        for row in rows:
            params = _json(row[0], {})
            params_hash = params.get("_trial_params_hash")
            if isinstance(params_hash, str) and params_hash:
                hashes.add(params_hash)
        return hashes

    @staticmethod
    def _next_trial_index(plan_id: str) -> int:
        row = get_connection().execute(
            "SELECT COALESCE(MAX(trial_index), 0) + 1 FROM agent_research_trials WHERE plan_id = ?",
            [plan_id],
        ).fetchone()
        return int(row[0])

    @staticmethod
    def _metric_number(metrics: dict[str, Any], key: str) -> float | None:
        value = metrics.get(key)
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _metric_ranges(self, trials: list[dict]) -> dict[str, dict[str, float | int]]:
        values_by_metric: dict[str, list[float]] = {}
        for trial in trials:
            for key, value in (trial.get("metrics") or {}).items():
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                values_by_metric.setdefault(key, []).append(number)
        return {
            key: {
                "min": min(values),
                "max": max(values),
                "mean": sum(values) / len(values),
                "count": len(values),
            }
            for key, values in sorted(values_by_metric.items())
            if values
        }

    def _qa_findings(
        self,
        metrics: dict[str, Any],
        artifact_refs: list[dict[str, Any]],
        *,
        source_type: str,
        project_id: str,
        market_profile_id: str,
    ) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        coverage = metrics.get("coverage")
        if coverage is not None and float(coverage) < _DEFAULT_THRESHOLDS["min_coverage"]:
            findings.append(
                self._finding(
                    "coverage",
                    "warning",
                    f"Coverage {float(coverage):.2%} below threshold",
                    blocking=False,
                )
            )
        purge_gap = metrics.get("purge_gap")
        label_horizon = metrics.get("label_horizon")
        if purge_gap is not None and label_horizon is not None and int(purge_gap) < int(label_horizon):
            findings.append(
                self._finding(
                    "leakage",
                    "fail",
                    "purge_gap is smaller than label_horizon",
                    blocking=True,
                )
            )
        max_drawdown = metrics.get("max_drawdown")
        if max_drawdown is not None and float(max_drawdown) < _DEFAULT_THRESHOLDS["max_drawdown_floor"]:
            findings.append(
                self._finding(
                    "max_drawdown",
                    "fail",
                    f"Max drawdown {float(max_drawdown):.2%} is below policy floor",
                    blocking=True,
                )
            )
        turnover = metrics.get("annual_turnover")
        if turnover is not None and float(turnover) > _DEFAULT_THRESHOLDS["max_annual_turnover"]:
            findings.append(
                self._finding(
                    "turnover",
                    "warning",
                    f"Annual turnover {float(turnover):.2f} exceeds threshold",
                    blocking=False,
                )
            )
        if not artifact_refs:
            findings.append(
                self._finding(
                    "lineage",
                    "warning",
                    "No artifact refs supplied for QA evaluation",
                    blocking=False,
                )
            )
        findings.extend(
            self._artifact_findings(
                artifact_refs,
                source_type=source_type,
                project_id=project_id,
                market_profile_id=market_profile_id,
            )
        )
        if not findings:
            findings.append(
                self._finding("baseline", "pass", "QA checks passed", blocking=False)
            )
        return findings

    def _artifact_findings(
        self,
        artifact_refs: list[dict[str, Any]],
        *,
        source_type: str,
        project_id: str,
        market_profile_id: str,
    ) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        promotion_like = source_type in {
            "strategy_graph",
            "backtest_run",
            "model_package",
            "model_experiment",
            "production_signal_run",
        }
        artifact_ids = []
        for ref in artifact_refs:
            ref_type = str(ref.get("type") or ref.get("ref_type") or "")
            artifact_id = ref.get("artifact_id") or (ref.get("id") if ref_type == "artifact" else None)
            if artifact_id:
                artifact_ids.append(str(artifact_id))
        for artifact_id in artifact_ids:
            try:
                artifact = self.kernel.get_artifact(artifact_id)
            except ValueError:
                findings.append(
                    self._finding(
                        "artifact_missing",
                        "fail",
                        f"Artifact {artifact_id} referenced by QA does not exist",
                        blocking=promotion_like,
                    )
                )
                continue
            if artifact["project_id"] != project_id:
                findings.append(
                    self._finding(
                        "artifact_project_scope",
                        "fail",
                        f"Artifact {artifact_id} belongs to project {artifact['project_id']}, not {project_id}",
                        blocking=promotion_like,
                    )
                )
            run = self.kernel.get_run(artifact["run_id"])
            if run["market_profile_id"] != market_profile_id:
                findings.append(
                    self._finding(
                        "artifact_market_scope",
                        "fail",
                        (
                            f"Artifact {artifact_id} belongs to market_profile "
                            f"{run['market_profile_id']}, not {market_profile_id}"
                        ),
                        blocking=promotion_like,
                    )
                )
            if promotion_like and artifact["lifecycle_stage"] == "scratch":
                findings.append(
                    self._finding(
                        "artifact_lifecycle",
                        "fail",
                        f"Artifact {artifact_id} is scratch and cannot support promotion QA",
                        blocking=True,
                    )
                )
        return findings

    @staticmethod
    def _promotion_failures(metrics: dict[str, Any], thresholds: dict[str, Any]) -> list[dict[str, Any]]:
        failures = []
        sharpe = metrics.get("sharpe")
        if sharpe is not None and float(sharpe) < float(thresholds.get("min_sharpe", 0.0)):
            failures.append({"check": "sharpe", "reason": "sharpe below threshold"})
        max_drawdown = metrics.get("max_drawdown")
        if max_drawdown is not None and float(max_drawdown) < float(thresholds.get("max_drawdown_floor", -1.0)):
            failures.append({"check": "max_drawdown", "reason": "drawdown below floor"})
        turnover = metrics.get("annual_turnover")
        if turnover is not None and float(turnover) > float(thresholds.get("max_annual_turnover", 999.0)):
            failures.append({"check": "turnover", "reason": "turnover exceeds threshold"})
        return failures

    @staticmethod
    def _finding(check: str, severity: str, message: str, *, blocking: bool) -> dict[str, Any]:
        return {
            "check": check,
            "severity": severity,
            "message": message,
            "blocking": blocking,
        }

    @staticmethod
    def _playbook_row(row) -> dict:
        return {
            "id": row[0],
            "name": row[1],
            "category": row[2],
            "description": row[3],
            "steps": _json(row[4], []),
            "optimization_targets": _json(row[5], []),
            "required_assets": _json(row[6], []),
            "status": row[7],
            "metadata": _json(row[8], {}),
            "created_at": str(row[9]) if row[9] is not None else None,
            "updated_at": str(row[10]) if row[10] is not None else None,
        }

    @staticmethod
    def _plan_row(row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "hypothesis": row[3],
            "playbook_id": row[4],
            "search_space": _json(row[5], {}),
            "budget": _json(row[6], {}),
            "stop_conditions": _json(row[7], {}),
            "status": row[8],
            "created_by": row[9],
            "metadata": _json(row[10], {}),
            "created_at": str(row[11]) if row[11] is not None else None,
            "updated_at": str(row[12]) if row[12] is not None else None,
        }

    @staticmethod
    def _trial_row(row) -> dict:
        return {
            "id": row[0],
            "plan_id": row[1],
            "trial_index": row[2],
            "trial_type": row[3],
            "params": _json(row[4], {}),
            "result_refs": _json(row[5], []),
            "metrics": _json(row[6], {}),
            "qa_report_id": row[7],
            "status": row[8],
            "created_at": str(row[9]) if row[9] is not None else None,
        }

    @staticmethod
    def _qa_row(row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "source_type": row[3],
            "source_id": row[4],
            "status": row[5],
            "blocking": bool(row[6]),
            "findings": _json(row[7], []),
            "metrics": _json(row[8], {}),
            "artifact_refs": _json(row[9], []),
            "created_at": str(row[10]) if row[10] is not None else None,
        }

    @staticmethod
    def _policy_row(row) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "policy_type": row[4],
            "thresholds": _json(row[5], {}),
            "status": row[6],
            "metadata": _json(row[7], {}),
            "created_at": str(row[8]) if row[8] is not None else None,
            "updated_at": str(row[9]) if row[9] is not None else None,
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


_BUILTIN_PLAYBOOKS = [
    {
        "id": "single_factor_to_backtest",
        "name": "Single Factor Hypothesis To Backtest",
        "category": "factor",
        "description": "Evaluate one factor and convert it into a StrategyGraph backtest candidate.",
        "steps": [
            "data/universe profile",
            "factor preview",
            "factor materialize",
            "factor evaluate",
            "factor signal",
            "top N strategy graph",
            "portfolio variant backtest",
            "QA gate",
        ],
        "optimization_targets": ["IC stability", "Sharpe", "max drawdown", "turnover"],
        "required_assets": ["universe", "factor_spec", "portfolio_construction_spec"],
    },
    {
        "id": "multifactor_dataset_model_backtest",
        "name": "Multi-Factor Dataset To Ranking Model Backtest",
        "category": "model",
        "description": "Build a Dataset, train ranking/regression models, and compare model-driven strategies.",
        "steps": [
            "feature pipeline",
            "dataset materialize/profile",
            "model experiment",
            "prediction profile",
            "model package candidate",
            "model-driven strategy graph",
            "backtest compare",
        ],
        "optimization_targets": ["out-of-time RankIC", "spread", "Sharpe", "max drawdown"],
        "required_assets": ["dataset", "model_experiment", "model_package", "strategy_graph"],
    },
    {
        "id": "portfolio_construction_optimization",
        "name": "Portfolio Construction And Position Rule Optimization",
        "category": "portfolio",
        "description": "Hold alpha constant and compare construction, constraints, and execution variants.",
        "steps": [
            "fixed alpha",
            "create portfolio variants",
            "batch construct target",
            "backtest compare",
            "constraint drag attribution",
        ],
        "optimization_targets": ["risk-adjusted return", "turnover", "concentration"],
        "required_assets": ["alpha_frame", "portfolio_construction_spec", "risk_control_spec"],
    },
    {
        "id": "walk_forward_robustness",
        "name": "Walk-Forward Robustness Research",
        "category": "robustness",
        "description": "Run rolling or expanding windows and optimize for worst-window stability.",
        "steps": [
            "rolling/expanding split",
            "multi-window training",
            "multi-window backtest",
            "worst-window analysis",
            "stability report",
        ],
        "optimization_targets": ["worst-window Sharpe", "stability", "drawdown"],
        "required_assets": ["dataset", "model_experiment", "backtest_runs"],
    },
    {
        "id": "regime_universe_conditioning",
        "name": "Regime And Universe Conditioning Research",
        "category": "regime",
        "description": "Evaluate factor/model behavior by regime and universe condition.",
        "steps": [
            "regime factor",
            "multiple universes",
            "regime factor evaluate",
            "strategy regime gate",
            "backtest attribution",
        ],
        "optimization_targets": ["drawdown reduction", "conditional alpha", "regime fit"],
        "required_assets": ["factor_spec", "universe", "strategy_graph"],
    },
    {
        "id": "backtest_failure_diagnosis",
        "name": "Backtest Failure Diagnosis And Low-Freedom Repair",
        "category": "diagnosis",
        "description": "Inspect stage artifacts and fix only the failing strategy layer.",
        "steps": [
            "inspect rebalance/trades",
            "query stage artifacts",
            "locate failing layer",
            "small node-level repair",
            "compare before/after",
        ],
        "optimization_targets": ["execution failures", "turnover", "constraint drag"],
        "required_assets": ["strategy_graph", "portfolio_run", "backtest_run"],
    },
]
