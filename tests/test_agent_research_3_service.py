import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.agent_research_3_service import AgentResearch3Service


class AgentResearch3ServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "agent_research3.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()

    def test_plan_budget_trial_and_qa_gate(self):
        service = AgentResearch3Service()
        playbooks = service.ensure_builtin_playbooks()
        plan = service.create_research_plan(
            hypothesis="Momentum quality blend improves risk-adjusted return",
            playbook_id=playbooks[0]["id"],
            search_space={"top_n": [10, 20], "max_weight": [0.05, 0.10]},
            budget={"max_trials": 2, "max_wall_minutes": 30},
            stop_conditions={"min_sharpe": 1.0},
        )
        first = service.record_trial(
            plan["id"],
            trial_type="strategy_graph_backtest",
            params={"top_n": 10},
            result_refs=[{"type": "backtest_run", "id": "bt-1"}],
            metrics={"sharpe": 1.4, "max_drawdown": -0.08, "annual_turnover": 3.0},
        )
        qa = service.evaluate_qa(
            source_type="backtest_run",
            source_id="bt-1",
            metrics={
                "coverage": 0.98,
                "sharpe": 1.4,
                "max_drawdown": -0.08,
                "annual_turnover": 3.0,
                "purge_gap": 5,
                "label_horizon": 5,
            },
            artifact_refs=[{"type": "backtest_run", "id": "bt-1"}],
        )
        promoted = service.evaluate_promotion(
            source_type="strategy_graph",
            source_id="graph-1",
            qa_report_id=qa["id"],
            metrics={"sharpe": 1.4, "max_drawdown": -0.08, "annual_turnover": 3.0},
            approved_by="unit-test",
        )
        second = service.record_trial(
            plan["id"],
            trial_type="strategy_graph_backtest",
            params={"top_n": 20},
            metrics={"sharpe": 0.8},
        )

        self.assertEqual(len(playbooks), 6)
        self.assertEqual(first["trial_index"], 1)
        self.assertEqual(second["trial_index"], 2)
        self.assertEqual(qa["status"], "pass")
        self.assertFalse(qa["blocking"])
        self.assertEqual(promoted["decision"], "promoted")
        self.assertFalse(service.check_budget(plan["id"])["can_run_more"])

        counts = get_connection().execute(
            """SELECT
                    (SELECT COUNT(*) FROM agent_research_plans),
                    (SELECT COUNT(*) FROM agent_research_trials),
                    (SELECT COUNT(*) FROM qa_gate_results),
                    (SELECT COUNT(*) FROM promotion_policies),
                    (SELECT COUNT(*) FROM research_playbooks)
            """
        ).fetchone()
        self.assertEqual(counts, (1, 2, 1, 1, 6))

    def test_qa_gate_blocks_leakage_and_drawdown_failures(self):
        service = AgentResearch3Service()
        qa = service.evaluate_qa(
            source_type="model_experiment",
            source_id="model-1",
            metrics={
                "coverage": 0.90,
                "purge_gap": 2,
                "label_horizon": 5,
                "max_drawdown": -0.35,
                "annual_turnover": 25.0,
            },
        )

        self.assertEqual(qa["status"], "fail")
        self.assertTrue(qa["blocking"])
        self.assertTrue(any(item["check"] == "leakage" for item in qa["findings"]))
        with self.assertRaisesRegex(ValueError, "blocking"):
            service.evaluate_promotion(
                source_type="model_package",
                source_id="model-1",
                qa_report_id=qa["id"],
                metrics={},
            )

    def test_batch_trial_recording_dedupes_and_returns_plan_performance(self):
        service = AgentResearch3Service()
        plan = service.create_research_plan(
            hypothesis="Batch search should record compact trial metadata",
            budget={"max_trials": 3},
        )

        batch = service.record_trials(
            plan["id"],
            trials=[
                {
                    "trial_type": "strategy_graph_backtest",
                    "params": {"top_n": 10, "max_weight": 0.10},
                    "metrics": {"sharpe": 0.8, "max_drawdown": -0.12},
                },
                {
                    "trial_type": "strategy_graph_backtest",
                    "params": {"top_n": 20, "max_weight": 0.08},
                    "metrics": {"sharpe": 1.3, "max_drawdown": -0.09},
                },
                {
                    "trial_type": "strategy_graph_backtest",
                    "params": {"top_n": 20, "max_weight": 0.08},
                    "metrics": {"sharpe": 1.3, "max_drawdown": -0.09},
                },
                {
                    "trial_type": "strategy_graph_backtest",
                    "params": {"top_n": 30, "max_weight": 0.06},
                    "metrics": {"sharpe": 1.0, "max_drawdown": -0.10},
                },
                {
                    "trial_type": "strategy_graph_backtest",
                    "params": {"top_n": 40, "max_weight": 0.05},
                    "metrics": {"sharpe": 1.1, "max_drawdown": -0.11},
                },
            ],
        )
        plans = service.list_plans(project_id="bootstrap_us")
        performance = service.get_plan_performance(
            plan["id"],
            primary_metric="sharpe",
            top_n=2,
        )

        self.assertEqual(batch["inserted_count"], 3)
        self.assertEqual(batch["skipped_count"], 2)
        self.assertEqual(
            [trial["trial_index"] for trial in batch["inserted_trials"]],
            [1, 2, 3],
        )
        self.assertFalse(batch["budget_state"]["can_run_more"])
        self.assertEqual(plans[0]["budget_state"]["used_trials"], 3)
        self.assertEqual(performance["best_trial"]["metrics"]["sharpe"], 1.3)
        self.assertEqual(len(performance["top_trials"]), 2)
        self.assertEqual(performance["metric_ranges"]["sharpe"]["count"], 3)


if __name__ == "__main__":
    unittest.main()
