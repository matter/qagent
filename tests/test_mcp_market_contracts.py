import asyncio
import unittest
from unittest.mock import patch

from backend import mcp_server


class MCPMarketContractsTests(unittest.TestCase):
    def test_market_scoped_tool_schemas_expose_market(self):
        tools = asyncio.run(mcp_server.mcp.list_tools())
        market_scoped_tools = {
            "get_stock_data",
            "search_stocks",
            "get_data_status",
            "update_data",
            "list_factors",
            "evaluate_factor",
            "create_factor",
            "list_models",
            "train_model",
            "list_strategies",
            "create_strategy",
            "run_backtest",
            "generate_signals",
            "list_groups",
            "create_group",
            "list_labels",
            "create_label",
            "list_feature_sets",
            "create_feature_set",
            "list_paper_sessions",
            "create_paper_session",
            "advance_paper_session",
        }
        schema_by_name = {tool.name: tool.inputSchema for tool in tools}

        missing = [
            name for name in sorted(market_scoped_tools)
            if "market" not in schema_by_name[name].get("properties", {})
        ]

        self.assertEqual([], missing)

    def test_invalid_market_error_is_actionable(self):
        with self.assertRaises(ValueError) as ctx:
            mcp_server.search_stocks(query="AAPL", market="HK")

        message = str(ctx.exception)
        self.assertIn("Invalid MCP request", message)
        self.assertIn("market", message)
        self.assertIn("US, CN", message)

    def test_data_tools_accept_market_and_return_agent_scope(self):
        fake_conn = _FakeConnection()
        fake_data = _FakeDataService()
        fake_executor = _FakeExecutor()

        with (
            patch("backend.mcp_server._data_service", return_value=fake_data),
            patch("backend.mcp_server._task_executor", return_value=fake_executor),
            patch("backend.mcp_server.get_connection", return_value=fake_conn),
        ):
            bars = mcp_server.get_stock_data(
                ticker="sh.600000",
                start_date="2024-01-02",
                end_date="2024-01-03",
                market="CN",
            )
            found = mcp_server.search_stocks(query="600000", limit=3, market="CN")
            status = mcp_server.get_data_status(market="CN")
            update = mcp_server.update_data(mode="incremental", market="CN")

        self.assertEqual(fake_conn.calls[0][1], ["CN", "sh.600000", "2024-01-02", "2024-01-03"])
        self.assertEqual(fake_conn.calls[1][1][0], "CN")
        self.assertEqual(bars[0]["market"], "CN")
        self.assertEqual(found[0]["market"], "CN")
        self.assertEqual(status["market"], "CN")
        self.assertEqual(update["market"], "CN")
        self.assertEqual(update["asset_scope"], {"market": "CN"})
        self.assertEqual(fake_executor.params["market"], "CN")

    def test_models_strategies_backtests_and_signals_forward_market_to_tasks(self):
        fake_model = _FakeModelService()
        fake_strategy = _FakeStrategyService()
        fake_backtest = _FakeBacktestService()
        fake_signal = _FakeSignalService()
        fake_executor = _FakeExecutor()

        with (
            patch("backend.mcp_server._model_service", return_value=fake_model),
            patch("backend.mcp_server._strategy_service", return_value=fake_strategy),
            patch("backend.mcp_server._backtest_service", return_value=fake_backtest),
            patch("backend.mcp_server._signal_service", return_value=fake_signal),
            patch("backend.mcp_server._task_executor", return_value=fake_executor),
        ):
            models = mcp_server.list_models(market="CN")
            train = mcp_server.train_model(
                name="CN ranker",
                feature_set_id="fs_cn",
                label_id="label_cn",
                model_type="lightgbm",
                model_params={},
                train_config={},
                universe_group_id="cn_all_a",
                market="CN",
                objective_type="ranking",
                ranking_config={"eval_at": [5]},
            )
            strategies = mcp_server.list_strategies(market="CN")
            created_strategy = mcp_server.create_strategy(
                name="CN strategy",
                source_code="class Placeholder: pass",
                market="CN",
            )
            backtest = mcp_server.run_backtest(
                strategy_id="strategy_cn",
                config_json='{"start_date":"2024-01-02","end_date":"2024-01-10"}',
                universe_group_id="cn_all_a",
                market="CN",
            )
            signal = mcp_server.generate_signals(
                strategy_id="strategy_cn",
                target_date="2024-01-10",
                universe_group_id="cn_all_a",
                market="CN",
            )

        self.assertEqual(models[0]["market"], "CN")
        self.assertEqual(strategies[0]["market"], "CN")
        self.assertEqual(created_strategy["market"], "CN")
        self.assertEqual(train["market"], "CN")
        self.assertEqual(train["poll_url"], "/api/tasks/task_cn")
        self.assertEqual(fake_executor.submissions[0]["params"]["market"], "CN")
        self.assertEqual(fake_executor.submissions[0]["params"]["objective_type"], "ranking")
        self.assertEqual(backtest["market"], "CN")
        self.assertEqual(fake_executor.submissions[1]["params"]["market"], "CN")
        self.assertEqual(signal["market"], "CN")
        self.assertEqual(fake_executor.submissions[2]["params"]["market"], "CN")

    def test_group_label_feature_and_paper_tools_forward_market(self):
        fake_group = _FakeGroupService()
        fake_label = _FakeLabelService()
        fake_feature = _FakeFeatureService()
        fake_paper = _FakePaperService()
        fake_executor = _FakeExecutor()

        with (
            patch("backend.mcp_server._group_service", return_value=fake_group),
            patch("backend.mcp_server._label_service", return_value=fake_label),
            patch("backend.mcp_server._feature_service", return_value=fake_feature),
            patch("backend.mcp_server._paper_service", return_value=fake_paper),
            patch("backend.mcp_server._task_executor", return_value=fake_executor),
        ):
            groups = mcp_server.list_groups(market="CN")
            group = mcp_server.create_group(
                name="CN manual",
                description="CN smoke group",
                group_type="manual",
                tickers=["sh.600000"],
                market="CN",
            )
            labels = mcp_server.list_labels(market="CN")
            feature_sets = mcp_server.list_feature_sets(market="CN")
            paper_sessions = mcp_server.list_paper_sessions(market="CN")
            paper = mcp_server.create_paper_session(
                strategy_id="strategy_cn",
                universe_group_id="cn_all_a",
                start_date="2024-01-02",
                market="CN",
            )
            advanced = mcp_server.advance_paper_session(
                session_id="paper_cn",
                steps=1,
                market="CN",
            )

        self.assertEqual(groups[0]["market"], "CN")
        self.assertEqual(group["market"], "CN")
        self.assertEqual(labels[0]["market"], "CN")
        self.assertEqual(feature_sets[0]["market"], "CN")
        self.assertEqual(paper_sessions[0]["market"], "CN")
        self.assertEqual(paper["market"], "CN")
        self.assertEqual(advanced["market"], "CN")
        self.assertEqual(fake_executor.params["market"], "CN")


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self):
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((query, params))
        if "FROM daily_bars" in query:
            return _FakeResult([("2024-01-02", 10.0, 11.0, 9.5, 10.5, 100)])
        return _FakeResult([("sh.600000", "PF Bank", "SSE", "Financials", "active")])


class _FakeExecutor:
    def __init__(self):
        self.params = None
        self.submissions = []

    def submit(self, task_type, fn, params, timeout, source=None):
        self.params = params
        self.submissions.append({"task_type": task_type, "params": params, "source": source})
        return "task_cn"


class _FakeDataService:
    def get_data_status(self, market=None):
        return {"market": market}

    def update_data(self, mode="incremental", market=None):
        return {"mode": mode, "market": market}


class _FakeModelService:
    def list_models(self, market=None):
        return [{"id": "model_cn", "market": market}]

    def train_model(self, **kwargs):
        return {"model_id": "model_cn", "market": kwargs.get("market")}


class _FakeStrategyService:
    def list_strategies(self, market=None):
        return [{"id": "strategy_cn", "market": market}]

    def create_strategy(self, **kwargs):
        return {"id": "strategy_cn", "market": kwargs.get("market")}


class _FakeBacktestService:
    def run_backtest(self, **kwargs):
        return {"backtest_id": "bt_cn", "market": kwargs.get("market")}


class _FakeSignalService:
    def generate_signals(self, **kwargs):
        return {"run_id": "signal_cn", "market": kwargs.get("market")}


class _FakeGroupService:
    def ensure_builtins(self, market=None):
        self.market = market

    def list_groups(self, market=None):
        return [{"id": "cn_all_a", "market": market}]

    def create_group(self, **kwargs):
        return {"id": "group_cn", "market": kwargs.get("market")}


class _FakeLabelService:
    def ensure_presets(self, market=None):
        self.market = market

    def list_labels(self, market=None):
        return [{"id": "label_cn", "market": market}]


class _FakeFeatureService:
    def list_feature_sets(self, market=None):
        return [{"id": "fs_cn", "market": market}]


class _FakePaperService:
    def list_sessions(self, market=None):
        return [{"id": "paper_cn", "market": market}]

    def create_session(self, **kwargs):
        return {"id": "paper_cn", "market": kwargs.get("market")}

    def advance(self, **kwargs):
        return {"session_id": kwargs.get("session_id"), "market": kwargs.get("market")}


if __name__ == "__main__":
    unittest.main()
