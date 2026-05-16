import unittest

from fastapi import HTTPException

from backend import mcp_server
from backend.api import paper_trading, signals, strategies


class V32LegacyRuntimeDisabledTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_strategy_backtest_api_returns_gone_without_submitting_task(self):
        with self.assertRaises(HTTPException) as ctx:
            await strategies.run_backtest(
                "legacy-strategy",
                strategies.RunBacktestRequest(
                    universe_group_id="legacy-group",
                    config={"start_date": "2024-01-02", "end_date": "2024-01-03"},
                ),
            )

        self.assertEqual(ctx.exception.status_code, 410)
        self.assertIn("/api/research-assets/strategy-graphs", str(ctx.exception.detail))

    async def test_legacy_signal_generation_api_returns_gone(self):
        with self.assertRaises(HTTPException) as ctx:
            await signals.generate_signals(
                signals.GenerateSignalsRequest(
                    strategy_id="legacy-strategy",
                    target_date="2024-01-03",
                    universe_group_id="legacy-group",
                ),
            )

        self.assertEqual(ctx.exception.status_code, 410)
        self.assertIn("/api/research-assets/production-signals/generate", str(ctx.exception.detail))

    async def test_legacy_paper_create_and_advance_api_return_gone(self):
        with self.assertRaises(HTTPException) as create_ctx:
            await paper_trading.create_session(
                paper_trading.CreateSessionRequest(
                    strategy_id="legacy-strategy",
                    universe_group_id="legacy-group",
                    start_date="2024-01-02",
                ),
            )
        with self.assertRaises(HTTPException) as advance_ctx:
            await paper_trading.advance_session(
                "legacy-session",
                paper_trading.AdvanceRequest(target_date="2024-01-03"),
            )

        self.assertEqual(create_ctx.exception.status_code, 410)
        self.assertEqual(advance_ctx.exception.status_code, 410)
        self.assertIn("/api/research-assets/paper-sessions", str(create_ctx.exception.detail))


class V32LegacyMcpRuntimeDisabledTests(unittest.TestCase):
    def test_legacy_mcp_runtime_tools_return_disabled_payloads(self):
        strategies_result = mcp_server.list_strategies()
        create_strategy = mcp_server.create_strategy(
            "legacy-strategy",
            "class LegacyStrategy: pass",
        )
        backtest = mcp_server.run_backtest(
            "legacy-strategy",
            "{}",
            "legacy-group",
        )
        signals_result = mcp_server.generate_signals(
            "legacy-strategy",
            "2024-01-03",
            "legacy-group",
        )
        paper = mcp_server.create_paper_session(
            "legacy-strategy",
            "legacy-group",
            "2024-01-02",
        )

        self.assertEqual(strategies_result["status"], "disabled")
        self.assertEqual(create_strategy["status"], "disabled")
        self.assertEqual(backtest["status"], "disabled")
        self.assertEqual(signals_result["status"], "disabled")
        self.assertEqual(paper["status"], "disabled")
        self.assertIn("StrategyGraph", strategies_result["message"])
        self.assertIn("strategy-graphs", create_strategy["replacement"])
        self.assertIn("StrategyGraph", backtest["message"])
        self.assertIn("production-signals", signals_result["replacement"])
        self.assertIn("paper-sessions", paper["replacement"])


if __name__ == "__main__":
    unittest.main()
