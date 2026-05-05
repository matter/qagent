#!/usr/bin/env python3
"""HTTP smoke test for QAgent 3.0 agent research APIs."""

from __future__ import annotations

import requests


BASE = "http://127.0.0.1:8000"


def api(method: str, path: str, **kwargs):
    response = getattr(requests, method)(f"{BASE}{path}", timeout=30, **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> int:
    health = api("get", "/api/health")
    assert health["status"] == "ok"

    playbooks = api("get", "/api/research/agent/playbooks")
    assert len(playbooks) == 6

    plan = api(
        "post",
        "/api/research/agent/plans",
        json={
            "hypothesis": "M9 API smoke: rank quality plus momentum as StrategyGraph alpha",
            "playbook_id": "single_factor_to_backtest",
            "search_space": {"top_n": [10, 20], "max_single_weight": [0.05, 0.10]},
            "budget": {"max_trials": 2, "max_wall_minutes": 30},
            "stop_conditions": {"min_sharpe": 1.0},
            "created_by": "api-smoke",
        },
    )
    trial = api(
        "post",
        f"/api/research/agent/plans/{plan['id']}/trials",
        json={
            "trial_type": "strategy_graph_backtest",
            "params": {"top_n": 10},
            "result_refs": [{"type": "backtest_run", "id": "m9-api-smoke-bt"}],
            "metrics": {"sharpe": 1.3, "max_drawdown": -0.06, "annual_turnover": 4.0},
        },
    )
    batch = api(
        "post",
        f"/api/research/agent/plans/{plan['id']}/trials/batch",
        json={
            "trials": [
                {
                    "trial_type": "strategy_graph_backtest",
                    "params": {"top_n": 20},
                    "result_refs": [{"type": "backtest_run", "id": "m9-api-smoke-bt-2"}],
                    "metrics": {"sharpe": 0.9, "max_drawdown": -0.09, "annual_turnover": 4.5},
                },
                {
                    "trial_type": "strategy_graph_backtest",
                    "params": {"top_n": 20},
                    "metrics": {"sharpe": 0.9},
                },
            ],
        },
    )
    qa = api(
        "post",
        "/api/research/agent/qa",
        json={
            "source_type": "backtest_run",
            "source_id": "m9-api-smoke-bt",
            "metrics": {
                "coverage": 0.99,
                "sharpe": 1.3,
                "max_drawdown": -0.06,
                "annual_turnover": 4.0,
                "purge_gap": 5,
                "label_horizon": 5,
            },
            "artifact_refs": [{"type": "agent_research_trial", "id": trial["id"]}],
        },
    )
    promotion = api(
        "post",
        "/api/research/agent/promotion",
        json={
            "source_type": "strategy_graph",
            "source_id": "m9-api-smoke-graph",
            "qa_report_id": qa["id"],
            "metrics": {"sharpe": 1.3, "max_drawdown": -0.06, "annual_turnover": 4.0},
            "approved_by": "api-smoke",
            "rationale": "M9 API smoke",
        },
    )
    budget = api("get", f"/api/research/agent/plans/{plan['id']}/budget")
    performance = api(
        "get",
        f"/api/research/agent/plans/{plan['id']}/performance",
        params={"primary_metric": "sharpe", "top_n": 1},
    )
    fetched_trials = api("get", f"/api/research/agent/plans/{plan['id']}/trials")
    fetched_qa = api("get", f"/api/research/agent/qa/{qa['id']}")

    assert trial["trial_index"] == 1
    assert batch["inserted_count"] == 1
    assert batch["skipped_count"] == 1
    assert qa["status"] == "pass"
    assert fetched_qa["id"] == qa["id"]
    assert promotion["decision"] == "promoted"
    assert budget["remaining_trials"] == 0
    assert performance["best_trial"]["metrics"]["sharpe"] == 1.3
    assert fetched_trials and fetched_trials[0]["id"] == trial["id"]
    print(
        {
            "playbooks": len(playbooks),
            "plan_id": plan["id"],
            "trial_id": trial["id"],
            "batch_inserted": batch["inserted_count"],
            "qa_report_id": qa["id"],
            "promotion_decision": promotion["decision"],
            "remaining_trials": budget["remaining_trials"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
