import asyncio
import unittest
from unittest.mock import patch

from backend.api import strategy_graph_3 as strategy_graph_api
from backend import mcp_server


class StrategyGraph3ApiMcpTests(unittest.TestCase):
    def test_backtest_endpoint_submits_ui_task(self):
        fake_executor = _FakeExecutor()
        fake_service = _FakeStrategyGraphService()
        with (
            patch.object(strategy_graph_api, "_executor", return_value=fake_executor),
            patch.object(strategy_graph_api, "_svc", return_value=fake_service),
        ):
            result = _run_async(
                strategy_graph_api.backtest_strategy_graph(
                    "graph1",
                    strategy_graph_api.BacktestGraphRequest(
                        start_date="2024-01-02",
                        end_date="2024-01-03",
                        alpha_frames_by_date={
                            "2024-01-02": [{"asset_id": "US_EQ:AAA", "score": 1.0}],
                        },
                    ),
                )
            )

        self.assertEqual(result["task_id"], "task_strategy_graph_bt")
        self.assertEqual(fake_executor.task_type, "strategy_graph_backtest")
        self.assertEqual(fake_executor.params["strategy_graph_id"], "graph1")
        self.assertEqual(fake_executor.params["start_date"], "2024-01-02")
        self.assertIs(fake_executor.fn.__self__, fake_service)
        self.assertNotIn("legacy_signal_frames_by_date", fake_executor.params)

    def test_mcp_backtest_tool_submits_agent_task(self):
        fake_executor = _FakeExecutor()
        fake_service = _FakeStrategyGraphService()
        with (
            patch.object(mcp_server, "_task_executor", return_value=fake_executor),
            patch.object(mcp_server, "_strategy_graph_3_service", return_value=fake_service),
        ):
            result = mcp_server.backtest_strategy_graph_3_0(
                strategy_graph_id="graph1",
                start_date="2024-01-02",
                end_date="2024-01-03",
            )

        self.assertEqual(result["task_id"], "task_strategy_graph_bt")
        self.assertEqual(fake_executor.task_type, "strategy_graph_backtest")
        self.assertEqual(fake_executor.params["strategy_graph_id"], "graph1")
        self.assertEqual(fake_executor.params["price_field"], "close")
        self.assertIs(fake_executor.fn.__self__, fake_service)
        self.assertNotIn("legacy_signal_frames_by_date", fake_executor.params)

    def test_legacy_adapter_api_is_removed_from_runtime_surface(self):
        self.assertFalse(hasattr(strategy_graph_api, "LegacyAdapterGraphRequest"))
        self.assertFalse(hasattr(strategy_graph_api, "create_legacy_adapter_graph"))
        self.assertFalse(hasattr(mcp_server, "create_legacy_strategy_adapter_graph_3_0"))


class _FakeStrategyGraphService:
    def backtest_graph(self, **kwargs):
        return kwargs


class _FakeExecutor:
    def __init__(self):
        self.task_type = None
        self.fn = None
        self.params = None

    def submit(self, task_type, fn, params, timeout, source):
        self.task_type = task_type
        self.fn = fn
        self.params = params
        return "task_strategy_graph_bt"


def _run_async(coro):
    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
