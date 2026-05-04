import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.api import strategies as strategy_api
from backend.services.backtest_engine import BacktestEngine, BacktestResult
from backend.services.backtest_service import BacktestService
from backend.services.strategy_service import StrategyService


_BASIC_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class BasicStrategy(StrategyBase):
    name = "Basic"
    description = "no-op"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        return pd.DataFrame(columns=["signal", "weight", "strength"])
"""


_MODEL_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class ModelStrategy(StrategyBase):
    name = "UsesModel"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        return pd.DataFrame(columns=["signal", "weight", "strength"])

    def required_models(self) -> list[str]:
        return ["model_us"]
"""


class StrategyBacktestMarketScopeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "strategy_backtest.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.addCleanup(self.conn.close)
        self._create_schema()

    def test_strategy_crud_defaults_to_us_and_filters_explicit_market(self):
        with self._patch_connections():
            us_strategy = StrategyService().create_strategy(
                "MarketScopedStrategy",
                _BASIC_STRATEGY_SOURCE,
            )
            cn_strategy = StrategyService().create_strategy(
                "MarketScopedStrategy",
                _BASIC_STRATEGY_SOURCE,
                market="CN",
            )

            us_list = StrategyService().list_strategies()
            cn_list = StrategyService().list_strategies(market="CN")

        self.assertEqual(us_strategy["market"], "US")
        self.assertEqual(cn_strategy["market"], "CN")
        self.assertEqual(us_strategy["version"], 1)
        self.assertEqual(cn_strategy["version"], 1)
        self.assertEqual([row["id"] for row in us_list], [us_strategy["id"]])
        self.assertEqual([row["id"] for row in cn_list], [cn_strategy["id"]])

    def test_strategy_rejects_model_from_other_market(self):
        self.conn.execute(
            """
            INSERT INTO models (id, market, name, feature_set_id, label_id)
            VALUES ('model_us', 'US', 'US Model', 'fs_us', 'label_us')
            """
        )

        with self._patch_connections():
            with self.assertRaisesRegex(ValueError, "market"):
                StrategyService().create_strategy(
                    "Bad CN model dependency",
                    _MODEL_STRATEGY_SOURCE,
                    market="CN",
                )

    def test_backtest_resolves_factor_names_inside_market(self):
        self.conn.execute(
            """
            INSERT INTO factors (id, market, name, version, source_code)
            VALUES
                ('factor_us_v2', 'US', 'Momentum_20', 2, 'source'),
                ('factor_cn_v1', 'CN', 'Momentum_20', 1, 'source')
            """
        )

        with patch("backend.services.backtest_service.get_connection", return_value=self.conn):
            result = BacktestService()._resolve_factor_ids(["Momentum_20"], market="CN")

        self.assertEqual(result, {"Momentum_20": "factor_cn_v1"})

    def test_backtest_engine_loads_prices_and_benchmark_by_market(self):
        self.conn.execute(
            """
            INSERT INTO daily_bars (market, ticker, date, open, high, low, close, volume)
            VALUES
                ('CN', 'sh.600000', DATE '2024-01-02', 10, 11, 9, 10.5, 100),
                ('US', 'sh.600000', DATE '2024-01-02', 100, 101, 99, 100.5, 100)
            """
        )
        self.conn.execute(
            """
            INSERT INTO index_bars (market, symbol, date, open, high, low, close, volume)
            VALUES
                ('CN', 'sh.000300', DATE '2024-01-02', 3000, 3001, 2999, 3000.5, 100),
                ('US', 'sh.000300', DATE '2024-01-02', 4000, 4001, 3999, 4000.5, 100)
            """
        )

        engine = BacktestEngine()
        with patch("backend.services.backtest_engine.get_connection", return_value=self.conn):
            close_df, open_df, *_ = engine._load_prices(
                ["sh.600000"],
                "2024-01-02",
                "2024-01-02",
                market="CN",
            )
            benchmark = engine._load_benchmark(
                "sh.000300",
                "2024-01-02",
                "2024-01-02",
                market="CN",
            )

        self.assertEqual(close_df.loc["2024-01-02", "sh.600000"], 10.5)
        self.assertEqual(open_df.loc["2024-01-02", "sh.600000"], 10)
        self.assertEqual(float(benchmark.iloc[0]), 3000.5)

    def test_raw_weight_position_sizing_preserves_strategy_cash_budget(self):
        raw_signals = pd.DataFrame(
            {
                "signal": [1, 1, 1],
                "weight": [0.45, 0.20, 0.10],
                "strength": [0.9, 0.8, 0.7],
            },
            index=["AAA", "BBB", "CCC"],
        )

        weights = BacktestService._apply_position_sizing(
            raw_signals,
            "raw_weight",
            max_positions=2,
        )

        self.assertEqual(weights, {"AAA": 0.45, "BBB": 0.20})
        self.assertLess(sum(weights.values()), 1.0)

    def test_raw_weight_backtest_config_disables_target_normalization_by_default(self):
        captured = {}
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-raw",
            "name": "Raw Weight Strategy",
            "version": 1,
            "source_code": _BASIC_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "raw_weight",
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]
        svc._model_service = unittest.mock.Mock()
        svc._feature_service = unittest.mock.Mock()
        svc._factor_service = unittest.mock.Mock()
        svc._label_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        def fake_run(weights, config):
            captured["weights"] = weights
            captured["config"] = config
            return BacktestResult(
                config=config.to_dict(),
                dates=["2026-04-06", "2026-04-07"],
                nav=[1000.0, 1000.0],
                benchmark_nav=[1000.0, 1000.0],
                drawdown=[0.0, 0.0],
                total_return=0.0,
                annual_return=0.0,
                annual_volatility=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                calmar_ratio=0.0,
                sortino_ratio=0.0,
                win_rate=0.0,
                profit_loss_ratio=0.0,
                total_trades=0,
                annual_turnover=0.0,
                total_cost=0.0,
                monthly_returns=[],
                trades=[],
                trade_diagnostics={},
            )

        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        prices = pd.DataFrame({"AAA": [10.0, 10.0]}, index=dates)
        empty_prices = pd.DataFrame({"AAA": [0.0, 0.0]}, index=dates)

        with (
            patch("backend.services.backtest_service.load_strategy_from_code") as load_strategy,
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(prices, prices, empty_prices, empty_prices, empty_prices),
            ),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result"),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
            patch.object(BacktestEngine, "run", side_effect=fake_run),
        ):
            strategy_instance = unittest.mock.Mock()
            strategy_instance.required_factors.return_value = []
            strategy_instance.generate_signals.return_value = pd.DataFrame(
                {"signal": [1], "weight": [0.5], "strength": [1.0]},
                index=["AAA"],
            )
            load_strategy.return_value = strategy_instance
            svc.run_backtest(
                "strategy-raw",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-07",
                    "benchmark": "SPY",
                    "rebalance_freq": "daily",
                },
                "sp500",
                market="US",
            )

        self.assertFalse(captured["config"].normalize_target_weights)
        self.assertEqual(float(captured["weights"].loc[pd.Timestamp("2026-04-06"), "AAA"]), 0.5)

    def test_cn_backtest_rejects_us_benchmark_before_loading_prices(self):
        with self._patch_connections():
            strategy = StrategyService().create_strategy(
                "CN strategy",
                _BASIC_STRATEGY_SOURCE,
                market="CN",
            )
            self.conn.execute(
                """
                INSERT INTO stock_groups (id, market, name, group_type)
                VALUES ('cn_group', 'CN', 'CN Group', 'custom')
                """
            )
            self.conn.execute(
                """
                INSERT INTO stock_group_members (group_id, market, ticker)
                VALUES ('cn_group', 'CN', 'sh.600000')
                """
            )

            with self.assertRaisesRegex(ValueError, "benchmark"):
                BacktestService().run_backtest(
                    strategy_id=strategy["id"],
                    config_dict={
                        "start_date": "2024-01-02",
                        "end_date": "2024-01-03",
                        "benchmark": "SPY",
                    },
                    universe_group_id="cn_group",
                    market="CN",
                )

    def test_strategy_api_forwards_market_to_backtest_task(self):
        executor = _FakeExecutor()
        strategy_service = _FakeStrategyService()
        backtest_service = _FakeBacktestService()

        with (
            patch.object(strategy_api, "_get_strategy_service", return_value=strategy_service),
            patch.object(strategy_api, "_get_backtest_service", return_value=backtest_service),
            patch.object(strategy_api, "_get_executor", return_value=executor),
        ):
            result = asyncio.run(
                strategy_api.run_backtest(
                    "strategy_cn",
                    strategy_api.RunBacktestRequest(
                        market="CN",
                        config={"benchmark": "sh.000300"},
                        universe_group_id="cn_group",
                    ),
                )
            )

        self.assertEqual(result["market"], "CN")
        self.assertEqual(executor.params["market"], "CN")

    def test_strategy_api_backtest_task_summary_includes_date_adjustment(self):
        executor = _FakeExecutor()
        strategy_service = _FakeStrategyService()
        backtest_service = _FakeBacktestService(
            result={
                "backtest_id": "bt_cn",
                "market": "CN",
                "strategy_id": "strategy_cn",
                "config": {
                    "requested_start_date": "2026-04-06",
                    "effective_start_date": "2026-04-07",
                    "requested_end_date": "2026-04-24",
                    "effective_end_date": "2026-04-24",
                    "date_adjustment": {
                        "requested_start_date": "2026-04-06",
                        "effective_start_date": "2026-04-07",
                        "reason": "calendar_or_data_trading_day_snap",
                    },
                },
            },
        )

        with (
            patch.object(strategy_api, "_get_strategy_service", return_value=strategy_service),
            patch.object(strategy_api, "_get_backtest_service", return_value=backtest_service),
            patch.object(strategy_api, "_get_executor", return_value=executor),
        ):
            asyncio.run(
                strategy_api.run_backtest(
                    "strategy_cn",
                    strategy_api.RunBacktestRequest(
                        market="CN",
                        config={"benchmark": "sh.000300"},
                        universe_group_id="cn_group",
                    ),
                )
            )

        summary = executor.fn(**executor.params)

        self.assertEqual(summary["date_adjustment"]["effective_start_date"], "2026-04-07")
        self.assertEqual(summary["requested_start_date"], "2026-04-06")
        self.assertEqual(summary["effective_start_date"], "2026-04-07")

    def test_strategy_api_rejects_flattened_backtest_config_fields(self):
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            strategy_api.RunBacktestRequest.model_validate(
                {
                    "market": "CN",
                    "universe_group_id": "cn_a_core_indices_union",
                    "start_date": "2026-01-02",
                    "end_date": "2026-04-02",
                }
            )

    def test_strategy_api_rejects_cross_market_benchmark_before_queueing(self):
        executor = _FakeExecutor()
        strategy_service = _FakeStrategyService()
        backtest_service = _FakeBacktestService()

        with (
            patch.object(strategy_api, "_get_strategy_service", return_value=strategy_service),
            patch.object(strategy_api, "_get_backtest_service", return_value=backtest_service),
            patch.object(strategy_api, "_get_executor", return_value=executor),
        ):
            with self.assertRaises(strategy_api.HTTPException) as ctx:
                asyncio.run(
                    strategy_api.run_backtest(
                        "strategy_cn",
                        strategy_api.RunBacktestRequest(
                            market="CN",
                            config={"benchmark": "SPY"},
                            universe_group_id="cn_group",
                        ),
                    )
                )

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("benchmark", str(ctx.exception.detail))
        self.assertIsNone(executor.params)

    def test_create_strategy_api_logs_unexpected_failures_with_readable_detail(self):
        strategy_service = _FailingCreateStrategyService()

        with (
            patch.object(strategy_api, "_get_strategy_service", return_value=strategy_service),
            patch.object(strategy_api.log, "error") as log_error,
        ):
            with self.assertRaises(strategy_api.HTTPException) as ctx:
                asyncio.run(
                    strategy_api.create_strategy(
                        strategy_api.CreateStrategyRequest(
                            market="CN",
                            name="CN broken strategy",
                            source_code=_BASIC_STRATEGY_SOURCE,
                        )
                    )
                )

        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("Failed to create strategy", str(ctx.exception.detail))
        self.assertIn("duckdb internal failure", str(ctx.exception.detail))
        log_error.assert_called_once()
        self.assertEqual(log_error.call_args.args[0], "api.strategy.create_failed")
        self.assertEqual(log_error.call_args.kwargs["market"], "CN")
        self.assertEqual(log_error.call_args.kwargs["name"], "CN broken strategy")

    def _patch_connections(self):
        return _MultiPatch(
            patch("backend.services.strategy_service.get_connection", return_value=self.conn),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service.get_connection", return_value=self.conn),
        )

    def _create_schema(self):
        self.conn.execute(
            """
            CREATE TABLE strategies (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                description TEXT,
                source_code TEXT NOT NULL,
                required_factors JSON,
                required_models JSON,
                position_sizing VARCHAR DEFAULT 'equal_weight',
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(market, name, version)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE factors (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                source_code TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE models (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                feature_set_id VARCHAR NOT NULL,
                label_id VARCHAR NOT NULL,
                model_type VARCHAR DEFAULT 'lightgbm',
                model_params JSON,
                train_config JSON,
                eval_metrics JSON,
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE stock_groups (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                description TEXT,
                group_type VARCHAR DEFAULT 'custom',
                filter_expr TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE stock_group_members (
                group_id VARCHAR NOT NULL,
                market VARCHAR NOT NULL DEFAULT 'US',
                ticker VARCHAR NOT NULL,
                PRIMARY KEY (group_id, market, ticker)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE daily_bars (
                market VARCHAR NOT NULL DEFAULT 'US',
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adj_factor DOUBLE DEFAULT 1.0,
                PRIMARY KEY (market, ticker, date)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE index_bars (
                market VARCHAR NOT NULL DEFAULT 'US',
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                PRIMARY KEY (market, symbol, date)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE backtest_results (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                strategy_id VARCHAR NOT NULL,
                config JSON NOT NULL,
                summary JSON NOT NULL,
                nav_series JSON,
                benchmark_nav JSON,
                drawdown_series JSON,
                monthly_returns JSON,
                trade_count INTEGER,
                trades JSON,
                result_level VARCHAR DEFAULT 'exploratory',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


class _MultiPatch:
    def __init__(self, *patches):
        self._patches = patches

    def __enter__(self):
        for item in self._patches:
            item.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        for item in reversed(self._patches):
            item.__exit__(exc_type, exc, tb)


class _FakeStore:
    def find_active_by_type_and_name(self, *args):
        return None


class _FakeExecutor:
    def __init__(self):
        self._store = _FakeStore()
        self.params = None
        self.fn = None

    def submit(self, task_type, fn, params, timeout, source):
        self.fn = fn
        self.params = params
        return "task_backtest_cn"


class _FakeStrategyService:
    def get_strategy(self, strategy_id, market=None):
        return {"id": strategy_id, "market": market}


class _FailingCreateStrategyService:
    def create_strategy(self, **kwargs):
        raise RuntimeError("duckdb internal failure")


class _FakeBacktestService:
    def __init__(self, result=None):
        self._result = result

    def run_backtest(self, strategy_id, config_dict, universe_group_id, market=None):
        if self._result is not None:
            return {
                "strategy_id": strategy_id,
                "strategy_name": "CN strategy",
                "result_level": "exploratory",
                "universe_group_id": universe_group_id,
                **self._result,
            }
        return {
            "backtest_id": "bt_cn",
            "strategy_id": strategy_id,
            "strategy_name": "CN strategy",
            "result_level": "exploratory",
            "universe_group_id": universe_group_id,
            "market": market,
        }


if __name__ == "__main__":
    unittest.main()
