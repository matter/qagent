import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service


class PortfolioAssets3ServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "portfolio3.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()

    def test_construct_targets_with_constraint_trace_and_next_open_orders(self):
        service = PortfolioAssets3Service()
        portfolio = service.create_portfolio_construction_spec(
            name="M7 equal weight top 5",
            method="equal_weight",
            params={"top_n": 5, "score_column": "score"},
        )
        risk = service.create_risk_control_spec(
            name="M7 basic long only risk",
            rules=[
                {"rule": "max_positions", "max_positions": 3},
                {"rule": "max_single_weight", "max_weight": 0.40},
            ],
        )
        rebalance = service.create_rebalance_policy_spec(
            name="M7 monthly threshold",
            policy_type="band",
            params={"band": 0.02},
        )
        execution = service.create_execution_policy_spec(
            name="M7 next open",
            policy_type="next_open",
            params={"price_field": "open"},
        )

        result = service.construct_portfolio(
            decision_date="2024-01-02",
            alpha_frame=[
                {"asset_id": "US_EQ:AAA", "score": 0.90, "confidence": 0.9},
                {"asset_id": "US_EQ:BBB", "score": 0.80, "confidence": 0.8},
                {"asset_id": "US_EQ:CCC", "score": 0.70, "confidence": 0.7},
                {"asset_id": "US_EQ:DDD", "score": 0.60, "confidence": 0.6},
                {"asset_id": "US_EQ:EEE", "score": 0.50, "confidence": 0.5},
            ],
            portfolio_spec_id=portfolio["id"],
            risk_control_spec_id=risk["id"],
            rebalance_policy_spec_id=rebalance["id"],
            execution_policy_spec_id=execution["id"],
            current_weights={"US_EQ:AAA": 0.10},
            portfolio_value=1_000_000,
        )

        targets = result["targets"]
        live_targets = [row for row in targets if row["target_weight"] > 1e-8]
        self.assertEqual(result["portfolio_run"]["status"], "completed")
        self.assertEqual(result["target_artifact"]["artifact_type"], "portfolio_targets")
        self.assertEqual(result["trace_artifact"]["artifact_type"], "constraint_trace")
        self.assertEqual(result["order_intent_artifact"]["artifact_type"], "order_intents")
        self.assertLessEqual(len(live_targets), 3)
        self.assertLessEqual(max(row["target_weight"] for row in live_targets), 0.40)
        self.assertAlmostEqual(sum(row["target_weight"] for row in live_targets), 1.0, places=6)
        self.assertTrue(
            any(item["rule_id"] == "max_positions" for item in result["constraint_trace"])
        )
        self.assertTrue(
            any(item["execution_date"] == "2024-01-03" for item in result["order_intents"])
        )
        self.assertTrue(
            all(item["side"] in {"buy", "sell"} for item in result["order_intents"])
        )

        counts = get_connection().execute(
            """SELECT
                    (SELECT COUNT(*) FROM portfolio_construction_specs),
                    (SELECT COUNT(*) FROM risk_control_specs),
                    (SELECT COUNT(*) FROM rebalance_policy_specs),
                    (SELECT COUNT(*) FROM execution_policy_specs),
                    (SELECT COUNT(*) FROM state_policy_specs),
                    (SELECT COUNT(*) FROM portfolio_runs)
            """
        ).fetchone()
        self.assertEqual(counts, (1, 1, 1, 1, 1, 1))

    def test_can_create_planned_price_execution_policy(self):
        service = PortfolioAssets3Service()

        execution = service.create_execution_policy_spec(
            name="M7 planned price",
            policy_type="planned_price",
            params={"planned_price_buffer_bps": 50, "fallback": "decision_close"},
        )

        self.assertEqual(execution["policy_type"], "planned_price")
        self.assertEqual(execution["params"]["planned_price_buffer_bps"], 50)

    def test_score_proportional_and_inverse_vol_are_independent_builders(self):
        service = PortfolioAssets3Service()
        score_spec = service.create_portfolio_construction_spec(
            name="M7 score proportional",
            method="score_proportional",
            params={"top_n": 3, "score_column": "score"},
        )
        inverse_vol_spec = service.create_portfolio_construction_spec(
            name="M7 inverse vol",
            method="inverse_vol",
            params={"top_n": 3, "volatility_column": "volatility"},
        )
        alpha = [
            {"asset_id": "US_EQ:AAA", "score": 3.0, "volatility": 0.30},
            {"asset_id": "US_EQ:BBB", "score": 2.0, "volatility": 0.10},
            {"asset_id": "US_EQ:CCC", "score": 1.0, "volatility": 0.20},
        ]

        by_score = service.construct_portfolio(
            decision_date="2024-01-02",
            alpha_frame=alpha,
            portfolio_spec_id=score_spec["id"],
        )
        by_vol = service.construct_portfolio(
            decision_date="2024-01-02",
            alpha_frame=alpha,
            portfolio_spec_id=inverse_vol_spec["id"],
        )

        score_weights = {
            row["asset_id"]: row["target_weight"]
            for row in by_score["targets"]
            if row["target_weight"] > 0
        }
        vol_weights = {
            row["asset_id"]: row["target_weight"]
            for row in by_vol["targets"]
            if row["target_weight"] > 0
        }
        self.assertGreater(score_weights["US_EQ:AAA"], score_weights["US_EQ:BBB"])
        self.assertGreater(vol_weights["US_EQ:BBB"], vol_weights["US_EQ:CCC"])
        self.assertGreater(vol_weights["US_EQ:CCC"], vol_weights["US_EQ:AAA"])

    def test_position_controller_skips_drift_and_allows_forced_exit(self):
        service = PortfolioAssets3Service()
        portfolio = service.create_portfolio_construction_spec(
            name="M6 position controlled equal",
            method="equal_weight",
            params={"top_n": 3},
        )
        controller = service.create_position_controller_spec(
            name="M6 no micro rebalance",
            controller_type="threshold",
            params={
                "rebalance_band": 0.02,
                "min_weight_delta": 0.02,
                "min_trade_value": 20_000,
            },
        )

        result = service.construct_portfolio(
            decision_date="2024-01-02",
            alpha_frame=[
                {"asset_id": "US_EQ:AAA", "score": 1.0},
                {"asset_id": "US_EQ:BBB", "score": 0.9},
                {
                    "asset_id": "US_EQ:CCC",
                    "score": 0.8,
                    "order_reason": "forced_exit:risk_violation",
                    "force_trade": True,
                },
            ],
            portfolio_spec_id=portfolio["id"],
            position_controller_spec_id=controller["id"],
            current_weights={"US_EQ:AAA": 0.32, "US_EQ:BBB": 0.0, "US_EQ:CCC": 0.08},
            portfolio_value=1_000_000,
        )

        order_assets = {row["asset_id"] for row in result["order_intents"]}
        diagnostics = result["profile"]["position_controller"]
        self.assertNotIn("US_EQ:AAA", order_assets)
        self.assertIn("US_EQ:BBB", order_assets)
        self.assertIn("US_EQ:CCC", order_assets)
        self.assertEqual(diagnostics["skipped_rebalance_count"], 1)
        self.assertGreater(diagnostics["turnover_saved"], 0)
        self.assertEqual(diagnostics["forced_exit_count"], 1)
        self.assertEqual(result["portfolio_run"]["position_controller_spec_id"], controller["id"])
        self.assertTrue(
            any(item["rule_id"] == "position_controller_drift" for item in result["constraint_trace"])
        )


if __name__ == "__main__":
    unittest.main()
