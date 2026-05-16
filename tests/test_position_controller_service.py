import unittest

from backend.services.position_controller_service import PositionControllerService


class PositionControllerServiceTests(unittest.TestCase):
    def test_skips_micro_rebalances_but_allows_forced_exit(self):
        service = PositionControllerService()
        orders = [
            {
                "asset_id": "US_EQ:AAA",
                "side": "buy",
                "current_weight": 0.10,
                "target_weight": 0.115,
                "delta_weight": 0.015,
                "estimated_value": 15000,
                "priority": 20,
            },
            {
                "asset_id": "US_EQ:BBB",
                "side": "buy",
                "current_weight": 0.00,
                "target_weight": 0.05,
                "delta_weight": 0.05,
                "estimated_value": 50000,
                "priority": 50,
            },
            {
                "asset_id": "US_EQ:CCC",
                "side": "sell",
                "current_weight": 0.08,
                "target_weight": 0.0,
                "delta_weight": -0.08,
                "estimated_value": 80000,
                "priority": 100,
                "force_trade": True,
                "order_reason": "forced_exit:risk_violation",
            },
        ]

        result = service.apply(
            orders=orders,
            params={
                "rebalance_band": 0.02,
                "min_weight_delta": 0.02,
                "min_trade_value": 20000,
            },
            portfolio_value=1_000_000,
        )

        kept_assets = {order["asset_id"] for order in result["orders"]}
        self.assertEqual(kept_assets, {"US_EQ:BBB", "US_EQ:CCC"})
        self.assertEqual(result["diagnostics"]["skipped_rebalance_count"], 1)
        self.assertAlmostEqual(result["diagnostics"]["turnover_saved"], 0.015)
        self.assertEqual(result["diagnostics"]["forced_exit_count"], 1)
        self.assertEqual(result["diagnostics"]["skipped_rebalance"][0]["asset_id"], "US_EQ:AAA")
        self.assertIn("below_rebalance_band", result["diagnostics"]["skipped_rebalance"][0]["reasons"])

    def test_turnover_budget_prioritizes_orders_and_records_drift(self):
        service = PositionControllerService()
        orders = [
            {"asset_id": "US_EQ:AAA", "side": "buy", "delta_weight": 0.06, "estimated_value": 60000, "priority": 10},
            {"asset_id": "US_EQ:BBB", "side": "buy", "delta_weight": 0.04, "estimated_value": 40000, "priority": 100},
            {"asset_id": "US_EQ:CCC", "side": "sell", "delta_weight": -0.03, "estimated_value": 30000, "priority": 50},
        ]

        result = service.apply(
            orders=orders,
            params={"turnover_budget": 0.07},
            portfolio_value=1_000_000,
        )

        kept_assets = [order["asset_id"] for order in result["orders"]]
        self.assertEqual(kept_assets, ["US_EQ:BBB", "US_EQ:CCC"])
        self.assertEqual(result["diagnostics"]["skipped_rebalance_count"], 1)
        self.assertEqual(result["diagnostics"]["skipped_rebalance"][0]["asset_id"], "US_EQ:AAA")
        self.assertIn("turnover_budget_exceeded", result["diagnostics"]["skipped_rebalance"][0]["reasons"])
        self.assertAlmostEqual(result["diagnostics"]["turnover_before"], 0.13)
        self.assertAlmostEqual(result["diagnostics"]["turnover_after"], 0.07)
        self.assertAlmostEqual(result["diagnostics"]["turnover_saved"], 0.06)


if __name__ == "__main__":
    unittest.main()
