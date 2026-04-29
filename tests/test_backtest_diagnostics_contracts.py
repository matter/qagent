import unittest

from backend.services.backtest_service import BacktestService


class BacktestDiagnosticsContractTests(unittest.TestCase):
    def test_rebalance_delta_reports_position_changes_and_turnover(self):
        diag = BacktestService._build_rebalance_diagnostics(
            date_key="2026-04-10",
            positions_before={"AAPL": 0.5, "MSFT": 0.5},
            positions_after={"MSFT": 0.4, "NVDA": 0.6},
            strategy_diagnostics={"candidate_pool": ["MSFT", "NVDA"]},
        )

        self.assertEqual(diag["date"], "2026-04-10")
        self.assertEqual(diag["positions_before"], {"AAPL": 0.5, "MSFT": 0.5})
        self.assertEqual(diag["positions_after"], {"MSFT": 0.4, "NVDA": 0.6})
        self.assertEqual(diag["added"], ["NVDA"])
        self.assertEqual(diag["removed"], ["AAPL"])
        self.assertEqual(diag["increased"], [])
        self.assertEqual(diag["decreased"], ["MSFT"])
        self.assertAlmostEqual(diag["turnover"], 1.2)
        self.assertEqual(diag["candidate_pool"], ["MSFT", "NVDA"])

    def test_list_summary_strips_heavy_diagnostics(self):
        summary = {
            "total_return": 0.12,
            "sharpe_ratio": 2.3,
            "rebalance_diagnostics": [{"date": "2026-04-10"}],
            "leakage_warnings": [{"model_id": "m1"}],
            "trade_diagnostics": {"by_reason": {}},
        }

        lightweight = BacktestService._list_summary(summary)

        self.assertEqual(lightweight["total_return"], 0.12)
        self.assertEqual(lightweight["sharpe_ratio"], 2.3)
        self.assertNotIn("rebalance_diagnostics", lightweight)
        self.assertNotIn("leakage_warnings", lightweight)
        self.assertIn("has_rebalance_diagnostics", lightweight)
        self.assertTrue(lightweight["has_rebalance_diagnostics"])


if __name__ == "__main__":
    unittest.main()
