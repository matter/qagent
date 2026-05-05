import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
from backend.services.strategy_graph_3_service import StrategyGraph3Service


class StrategyGraph3ServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "strategy_graph3.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        self.portfolio_service = PortfolioAssets3Service()
        self.portfolio = self.portfolio_service.create_portfolio_construction_spec(
            name="M8 graph equal weight",
            method="equal_weight",
            params={"top_n": 5},
        )
        self.risk = self.portfolio_service.create_risk_control_spec(
            name="M8 graph risk",
            rules=[
                {"rule": "max_positions", "max_positions": 3},
                {"rule": "max_single_weight", "max_weight": 0.40},
            ],
        )
        self.execution = self.portfolio_service.create_execution_policy_spec(
            name="M8 graph next open",
            policy_type="next_open",
            params={"price_field": "open"},
        )

    def test_simulate_day_explains_alpha_selection_portfolio_and_orders(self):
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 smoke graph",
            selection_policy={"top_n": 4, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=self.risk["id"],
            execution_policy_spec_id=self.execution["id"],
        )
        result = service.simulate_day(
            graph["id"],
            decision_date="2024-01-02",
            alpha_frame=[
                {"asset_id": "US_EQ:AAA", "score": 0.90, "confidence": 0.9},
                {"asset_id": "US_EQ:BBB", "score": 0.80, "confidence": 0.8},
                {"asset_id": "US_EQ:CCC", "score": 0.70, "confidence": 0.7},
                {"asset_id": "US_EQ:DDD", "score": 0.60, "confidence": 0.6},
                {"asset_id": "US_EQ:EEE", "score": 0.50, "confidence": 0.5},
            ],
            current_weights={"US_EQ:AAA": 0.10},
        )

        explain = service.explain_day(result["strategy_signal"]["id"])
        self.assertEqual(graph["graph_type"], "builtin_alpha_graph")
        self.assertEqual(result["strategy_signal"]["status"], "completed")
        self.assertEqual(result["stage_artifact"]["artifact_type"], "strategy_graph_explain")
        self.assertEqual(len(result["alpha_frame"]), 5)
        self.assertEqual(len(result["selection_frame"]), 5)
        self.assertLessEqual(result["profile"]["active_positions"], 3)
        self.assertTrue(result["constraint_trace"])
        self.assertTrue(result["order_intents"])
        self.assertIn("alpha", explain["stages"])
        self.assertIn("selection", explain["stages"])
        self.assertIn("portfolio", explain["stages"])
        self.assertIn("execution", explain["stages"])

        counts = get_connection().execute(
            """SELECT
                    (SELECT COUNT(*) FROM strategy_graphs),
                    (SELECT COUNT(*) FROM strategy_nodes),
                    (SELECT COUNT(*) FROM strategy_signals),
                    (SELECT COUNT(*) FROM portfolio_runs)
            """
        ).fetchone()
        self.assertEqual(counts, (1, 6, 1, 1))

    def test_legacy_signal_adapter_graph_can_run_from_legacy_signal_frame(self):
        service = StrategyGraph3Service()
        graph = service.create_legacy_strategy_adapter_graph(
            name="M8 legacy adapter graph",
            legacy_strategy_id="legacy-strategy-id",
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=self.risk["id"],
            execution_policy_spec_id=self.execution["id"],
        )
        result = service.simulate_day(
            graph["id"],
            decision_date="2024-01-02",
            legacy_signal_frame=[
                {"ticker": "AAA", "signal": 1, "weight": 0.5, "strength": 5.0},
                {"ticker": "BBB", "signal": 1, "weight": 0.3, "strength": 3.0},
                {"ticker": "CCC", "signal": 0, "weight": 0.0, "strength": 1.0},
            ],
        )

        self.assertEqual(graph["graph_type"], "legacy_strategy_adapter")
        self.assertEqual(result["alpha_frame"][0]["asset_id"], "US_EQ:AAA")
        self.assertEqual(result["strategy_signal"]["status"], "completed")
        self.assertTrue(result["order_intents"])


if __name__ == "__main__":
    unittest.main()
