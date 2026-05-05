#!/usr/bin/env python3
"""Service smoke test for QAgent 3.0 agent research playbooks and QA gate."""

from __future__ import annotations

from backend.db import init_db
from backend.services.agent_research_3_service import AgentResearch3Service


def main() -> int:
    init_db()
    service = AgentResearch3Service()
    playbooks = service.ensure_builtin_playbooks()
    plan = service.create_research_plan(
        hypothesis="M9 smoke: momentum quality blend improves US equity ranking",
        playbook_id="single_factor_to_backtest",
        search_space={"top_n": [10, 20], "max_single_weight": [0.05, 0.10]},
        budget={"max_trials": 2, "max_wall_minutes": 30},
        stop_conditions={"min_sharpe": 1.0, "max_drawdown_floor": -0.15},
        created_by="service-smoke",
    )
    trial = service.record_trial(
        plan["id"],
        trial_type="strategy_graph_backtest",
        params={"top_n": 10, "max_single_weight": 0.05},
        result_refs=[{"type": "backtest_run", "id": "m9-smoke-bt"}],
        metrics={"sharpe": 1.2, "max_drawdown": -0.07, "annual_turnover": 3.2},
    )
    qa = service.evaluate_qa(
        source_type="backtest_run",
        source_id="m9-smoke-bt",
        metrics={
            "coverage": 0.99,
            "sharpe": 1.2,
            "max_drawdown": -0.07,
            "annual_turnover": 3.2,
            "purge_gap": 5,
            "label_horizon": 5,
        },
        artifact_refs=[{"type": "agent_research_trial", "id": trial["id"]}],
    )
    promotion = service.evaluate_promotion(
        source_type="strategy_graph",
        source_id="m9-smoke-graph",
        qa_report_id=qa["id"],
        metrics={"sharpe": 1.2, "max_drawdown": -0.07, "annual_turnover": 3.2},
        approved_by="service-smoke",
        rationale="M9 service smoke",
    )
    batch = service.record_trials(
        plan["id"],
        trials=[
            {
                "trial_type": "strategy_graph_backtest",
                "params": {"top_n": 20, "max_single_weight": 0.10},
                "result_refs": [{"type": "backtest_run", "id": "m9-smoke-bt-2"}],
                "metrics": {"sharpe": 0.9, "max_drawdown": -0.09, "annual_turnover": 4.5},
            },
            {
                "trial_type": "strategy_graph_backtest",
                "params": {"top_n": 20, "max_single_weight": 0.10},
                "metrics": {"sharpe": 0.9},
            },
        ],
    )
    budget = service.check_budget(plan["id"])
    performance = service.get_plan_performance(plan["id"], primary_metric="sharpe", top_n=1)

    assert len(playbooks) == 6
    assert trial["trial_index"] == 1
    assert batch["inserted_count"] == 1
    assert batch["skipped_count"] == 1
    assert qa["status"] == "pass"
    assert promotion["decision"] == "promoted"
    assert budget["remaining_trials"] == 0
    assert performance["best_trial"]["metrics"]["sharpe"] == 1.2
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
