import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.api import strategies as strategy_api
from backend.services.backtest_engine import BacktestConfig, BacktestEngine, BacktestResult
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


_STATEFUL_BUFFER_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class StatefulBufferStrategy(StrategyBase):
    name = "StatefulBuffer"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        date_key = str(context.current_date.date())
        context.diagnostics["context_weights"] = dict(context.current_weights)
        if date_key == "2026-04-06":
            return pd.DataFrame(
                {"signal": [1, 1], "weight": [0.50, 0.50], "strength": [1.0, 1.0]},
                index=["AAA", "BBB"],
            )
        if date_key == "2026-04-07":
            return pd.DataFrame(
                {"signal": [1, 1], "weight": [0.54, 0.46], "strength": [1.0, 1.0]},
                index=["AAA", "BBB"],
            )
        return pd.DataFrame(
            {"signal": [1, 1], "weight": [0.54, 0.46], "strength": [1.0, 1.0]},
            index=["AAA", "BBB"],
        )
"""


_STATEFUL_PLANNED_BLOCK_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class StatefulPlannedBlockStrategy(StrategyBase):
    name = "StatefulPlannedBlock"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        context.diagnostics["context_weights"] = dict(context.current_weights)
        return pd.DataFrame(
            {
                "signal": [1],
                "weight": [0.50],
                "strength": [1.0],
                "planned_price": [20.0],
            },
            index=["AAA"],
        )
"""


_EMPTY_EXIT_PLANNED_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class EmptyExitPlannedStrategy(StrategyBase):
    name = "EmptyExitPlanned"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        date_key = str(context.current_date.date())
        if date_key == "2026-04-06":
            return pd.DataFrame(
                {"signal": [1], "weight": [1.0], "strength": [1.0], "planned_price": [11.0]},
                index=["AAA"],
            )
        return pd.DataFrame(columns=["signal", "weight", "strength"])
"""


_DEFAULT_CONFIG_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class DefaultConfigStrategy(StrategyBase):
    name = "DefaultConfigStrategy"
    default_backtest_config = {
        "position_sizing": "raw_weight",
        "rebalance_freq": "daily",
        "execution_model": "planned_price",
        "planned_price_buffer_bps": 50,
        "planned_price_fallback": "next_close",
        "constraint_config": {"max_single_name_weight": 0.40},
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        return pd.DataFrame(
            {"signal": [1], "weight": [0.50], "strength": [1.0], "planned_price": [12.0]},
            index=["AAA"],
        )
"""


_MIXED_EXECUTION_INTENT_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class MixedExecutionIntentStrategy(StrategyBase):
    name = "MixedExecutionIntentStrategy"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "signal": [1, 1, 1],
                "weight": [0.30, 0.30, 0.30],
                "strength": [3.0, 2.0, 1.0],
                "execution_model": ["next_open", "planned_price", "next_close"],
                "planned_price": [None, 21.0, None],
                "planned_price_buffer_bps": [None, 50, None],
                "planned_price_fallback": [None, "cancel", None],
                "order_reason": ["liquid entry", "limit entry", "close entry"],
            },
            index=["AAA", "BBB", "CCC"],
        )
"""


_STATEFUL_ACTUAL_OPEN_CASH_BUFFER_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class StatefulActualOpenCashBufferStrategy(StrategyBase):
    name = "StatefulActualOpenCashBuffer"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        date_key = str(context.current_date.date())
        context.diagnostics["context_weights"] = dict(context.current_weights)
        if date_key == "2026-04-06":
            weight = 0.50
        else:
            weight = 0.54
        return pd.DataFrame(
            {"signal": [1], "weight": [weight], "strength": [1.0]},
            index=["AAA"],
        )
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

    def test_backtest_engine_planned_price_can_fallback_to_next_close(self):
        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        signals = pd.DataFrame({"AAA": [1.0, 1.0]}, index=dates)
        planned_prices = pd.DataFrame({"AAA": [10.81, 10.81]}, index=dates)
        close = pd.DataFrame({"AAA": [10.0, 12.0]}, index=dates)
        open_ = pd.DataFrame({"AAA": [10.0, 11.0]}, index=dates)
        high = pd.DataFrame({"AAA": [10.2, 12.0]}, index=dates)
        low = pd.DataFrame({"AAA": [9.8, 10.8]}, index=dates)
        volume = pd.DataFrame({"AAA": [100.0, 100.0]}, index=dates)
        engine = BacktestEngine()

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(close, open_, high, low, volume),
            ),
            patch.object(engine, "_load_benchmark", return_value=close["AAA"]),
        ):
            result = engine.run(
                signals,
                BacktestConfig(
                    start_date="2026-04-06",
                    end_date="2026-04-07",
                    market="US",
                    benchmark="SPY",
                    initial_capital=1200.0,
                    commission_rate=0.0,
                    slippage_rate=0.0,
                    rebalance_freq="daily",
                    execution_model="planned_price",
                    planned_price_buffer_bps=50,
                    planned_price_fallback="next_close",
                ),
                planned_prices=planned_prices,
            )

        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0]["price"], 12.0)
        planned_diag = result.trade_diagnostics["planned_price_execution"]
        self.assertEqual(planned_diag["planned_fill_count"], 0)
        self.assertEqual(planned_diag["fallback_close_count"], 1)
        self.assertEqual(planned_diag["filled"][0]["fill_type"], "fallback_close")

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

    def test_signal_weight_position_sizing_accepts_dict_strategy_output(self):
        raw_signals = {
            "AAA": {"signal": 1, "weight": 0.60, "strength": 3.0},
            "BBB": {"signal": 1, "weight": 0.40, "strength": 1.0},
        }

        normalized = BacktestService._normalize_strategy_signals(raw_signals)
        weights = BacktestService._apply_position_sizing(
            normalized,
            "signal_weight",
            max_positions=5,
        )

        self.assertEqual(weights, {"AAA": 0.75, "BBB": 0.25})

    def test_normalize_strategy_signals_preserves_planned_price_column(self):
        raw_signals = {
            "AAA": {"signal": 1, "weight": 0.60, "strength": 3.0, "planned_price": 10.5},
        }

        normalized = BacktestService._normalize_strategy_signals(raw_signals)

        self.assertIn("planned_price", normalized.columns)
        self.assertEqual(float(normalized.loc["AAA", "planned_price"]), 10.5)

    def test_create_strategy_rejects_unsupported_position_sizing(self):
        with self._patch_connections():
            with self.assertRaisesRegex(ValueError, "Unsupported position_sizing"):
                StrategyService().create_strategy(
                    name="bad sizing",
                    source_code=_BASIC_STRATEGY_SOURCE,
                    position_sizing="custom",
                    market="US",
                )

    def test_leakage_audit_uses_forward_label_horizon_effective_data_end(self):
        svc = BacktestService.__new__(BacktestService)
        svc._model_service = unittest.mock.Mock()
        svc._group_service = unittest.mock.Mock()
        svc._model_service.get_model.return_value = {
            "id": "model_forward_20d",
            "name": "Forward 20d",
            "market": "US",
            "train_config": {
                "train_start": "2025-01-02",
                "train_end": "2025-10-31",
                "valid_end": "2025-11-28",
                "test_end": "2025-12-31",
            },
            "eval_metrics": {
                "label_summary": {"horizon": 20},
            },
        }
        svc._group_service.get_group_tickers.return_value = ["AAPL"]

        warnings = svc._check_data_leakage(
            ["model_forward_20d"],
            BacktestConfig(start_date="2026-01-02", end_date="2026-02-27", market="US"),
            ["AAPL"],
            "sp500",
            market="US",
        )

        self.assertEqual(len(warnings), 1)
        self.assertTrue(warnings[0]["time_overlap"])
        self.assertEqual(warnings[0]["model_data_end"], "2026-01-30")
        self.assertEqual(warnings[0]["model_window_end"], "2025-12-31")
        self.assertEqual(warnings[0]["label_horizon"], 20)

    def test_leakage_audit_prefers_effective_label_horizon(self):
        svc = BacktestService.__new__(BacktestService)
        svc._model_service = unittest.mock.Mock()
        svc._group_service = unittest.mock.Mock()
        svc._model_service.get_model.return_value = {
            "id": "model_composite",
            "name": "Composite 10 outer 20 effective",
            "market": "US",
            "train_config": {
                "train_start": "2025-01-02",
                "train_end": "2025-10-31",
                "valid_end": "2025-11-28",
                "test_end": "2025-12-31",
            },
            "eval_metrics": {
                "label_horizon": 10,
                "effective_label_horizon": 20,
                "label_summary": {"horizon": 10, "effective_horizon": 20},
            },
        }
        svc._group_service.get_group_tickers.return_value = ["AAPL"]

        warnings = svc._check_data_leakage(
            ["model_composite"],
            BacktestConfig(start_date="2026-01-02", end_date="2026-02-27", market="US"),
            ["AAPL"],
            "sp500",
            market="US",
        )

        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["label_horizon"], 20)
        self.assertEqual(warnings[0]["model_data_end"], "2026-01-30")

    def test_leakage_audit_allows_safe_forward_label_horizon_cutoff(self):
        svc = BacktestService.__new__(BacktestService)
        svc._model_service = unittest.mock.Mock()
        svc._group_service = unittest.mock.Mock()
        svc._model_service.get_model.return_value = {
            "id": "model_forward_20d_safe",
            "name": "Forward 20d safe",
            "market": "US",
            "train_config": {
                "train_start": "2025-01-02",
                "train_end": "2025-10-31",
                "valid_end": "2025-11-14",
                "test_end": "2025-12-01",
            },
            "eval_metrics": {
                "label_summary": {"horizon": 20},
            },
        }
        svc._group_service.get_group_tickers.return_value = ["AAPL"]

        warnings = svc._check_data_leakage(
            ["model_forward_20d_safe"],
            BacktestConfig(start_date="2026-01-02", end_date="2026-02-27", market="US"),
            ["AAPL"],
            "sp500",
            market="US",
        )

        self.assertEqual(warnings, [])

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

    def test_strategy_context_tracks_engine_holdings_after_rebalance_buffer(self):
        captured_diagnostics = {}
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-stateful-buffer",
            "name": "Stateful Buffer Strategy",
            "version": 1,
            "source_code": _STATEFUL_BUFFER_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "raw_weight",
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA", "BBB"]
        svc._model_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08", "2026-04-09"])
        prices = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 10.0, 10.0],
                "BBB": [10.0, 10.0, 10.0, 10.0],
            },
            index=dates,
        )
        prices_empty = pd.DataFrame(
            {
                "AAA": [0.0, 0.0, 0.0, 0.0],
                "BBB": [0.0, 0.0, 0.0, 0.0],
            },
            index=dates,
        )

        original_merge = BacktestService._merge_engine_rebalance_diagnostics

        def capture_merge(service_diagnostics, engine_diagnostics):
            captured_diagnostics["service"] = service_diagnostics
            captured_diagnostics["engine"] = engine_diagnostics
            return original_merge(
                service_diagnostics,
                engine_diagnostics,
            )

        with (
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(prices, prices, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(svc._backtest_engine, "_load_benchmark", return_value=prices["AAA"]),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result"),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch.object(BacktestService, "_merge_engine_rebalance_diagnostics", side_effect=capture_merge),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
        ):
            svc.run_backtest(
                "strategy-stateful-buffer",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-09",
                    "benchmark": "SPY",
                    "rebalance_freq": "daily",
                    "rebalance_buffer": 0.05,
                    "rebalance_buffer_mode": "hold_overlap_only",
                    "normalize_target_weights": False,
                },
                "sp500",
                market="US",
            )

        service_diag_by_date = {
            item["date"]: item
            for item in captured_diagnostics["service"]
        }
        engine_diag_by_date = {
            item["date"]: item
            for item in captured_diagnostics["engine"]
        }

        self.assertEqual(
            engine_diag_by_date["2026-04-08"]["executed_positions_after"],
            {"AAA": 0.5, "BBB": 0.5},
        )
        self.assertEqual(
            service_diag_by_date["2026-04-08"]["context_weights"],
            {"AAA": 0.5, "BBB": 0.5},
        )

    def test_strategy_context_does_not_assume_blocked_planned_price_fill(self):
        captured_diagnostics = {}
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-planned-block",
            "name": "Stateful Planned Block Strategy",
            "version": 1,
            "source_code": _STATEFUL_PLANNED_BLOCK_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "raw_weight",
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]
        svc._model_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08"])
        close = pd.DataFrame({"AAA": [10.0, 10.0, 10.0]}, index=dates)
        open_ = pd.DataFrame({"AAA": [10.0, 10.0, 10.0]}, index=dates)
        high = pd.DataFrame({"AAA": [10.1, 10.1, 10.1]}, index=dates)
        low = pd.DataFrame({"AAA": [9.9, 9.9, 9.9]}, index=dates)
        volume = pd.DataFrame({"AAA": [100.0, 100.0, 100.0]}, index=dates)

        original_merge = BacktestService._merge_engine_rebalance_diagnostics

        def capture_merge(service_diagnostics, engine_diagnostics):
            captured_diagnostics["service"] = service_diagnostics
            captured_diagnostics["engine"] = engine_diagnostics
            return original_merge(service_diagnostics, engine_diagnostics)

        with (
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(close, open_, high, low, volume),
            ),
            patch.object(svc._backtest_engine, "_load_benchmark", return_value=close["AAA"]),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result"),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch.object(BacktestService, "_merge_engine_rebalance_diagnostics", side_effect=capture_merge),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
        ):
            svc.run_backtest(
                "strategy-planned-block",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-08",
                    "benchmark": "SPY",
                    "rebalance_freq": "daily",
                    "execution_model": "planned_price",
                    "planned_price_buffer_bps": 50,
                    "normalize_target_weights": False,
                },
                "sp500",
                market="US",
            )

        service_diag_by_date = {
            item["date"]: item
            for item in captured_diagnostics["service"]
        }
        engine_diag_by_date = {
            item["date"]: item
            for item in captured_diagnostics["engine"]
        }

        self.assertEqual(
            engine_diag_by_date["2026-04-07"]["executed_positions_after"],
            {},
        )
        self.assertEqual(
            service_diag_by_date["2026-04-07"]["context_weights"],
            {},
        )

    def test_planned_price_empty_signal_exit_uses_decision_close_fallback(self):
        captured = {}
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-empty-exit",
            "name": "Empty Exit Planned Strategy",
            "version": 1,
            "source_code": _EMPTY_EXIT_PLANNED_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "raw_weight",
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]
        svc._model_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        def fake_run(weights, config, planned_prices=None):
            captured["weights"] = weights.copy()
            captured["planned_prices"] = planned_prices.copy()
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
        close = pd.DataFrame({"AAA": [10.0, 11.0]}, index=dates)
        open_ = pd.DataFrame({"AAA": [10.0, 11.0]}, index=dates)
        high = pd.DataFrame({"AAA": [10.1, 11.1]}, index=dates)
        low = pd.DataFrame({"AAA": [9.9, 10.9]}, index=dates)
        volume = pd.DataFrame({"AAA": [100.0, 100.0]}, index=dates)

        with (
            patch("backend.services.backtest_service.load_strategy_from_code") as load_strategy,
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(close, open_, high, low, volume),
            ),
            patch.object(svc._backtest_engine, "_load_benchmark", return_value=close["AAA"]),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result"),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch.object(BacktestEngine, "run", side_effect=fake_run),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
        ):
            load_strategy.return_value = __import__(
                "backend.strategies.loader",
                fromlist=["_load_strategy_instance_unsafe"],
            )._load_strategy_instance_unsafe(_EMPTY_EXIT_PLANNED_STRATEGY_SOURCE)
            svc.run_backtest(
                "strategy-empty-exit",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-07",
                    "benchmark": "SPY",
                    "rebalance_freq": "daily",
                    "execution_model": "planned_price",
                    "planned_price_buffer_bps": 50,
                    "normalize_target_weights": False,
                },
                "sp500",
                market="US",
            )

        self.assertEqual(float(captured["weights"].loc[dates[1], "AAA"]), 0.0)
        self.assertEqual(float(captured["planned_prices"].loc[dates[1], "AAA"]), 11.0)

    def test_strategy_default_backtest_config_merges_into_effective_config(self):
        captured = {}
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-default-config",
            "name": "Default Config Strategy",
            "version": 1,
            "source_code": _DEFAULT_CONFIG_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "equal_weight",
            "constraint_config": {},
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]
        svc._model_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        def fake_run(weights, config, planned_prices=None, execution_overrides=None):
            captured["weights"] = weights.copy()
            captured["config"] = config
            captured["planned_prices"] = planned_prices.copy() if planned_prices is not None else None
            captured["execution_overrides"] = (
                execution_overrides.copy() if execution_overrides is not None else None
            )
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
        close = pd.DataFrame({"AAA": [10.0, 12.0]}, index=dates)
        open_ = pd.DataFrame({"AAA": [10.0, 11.0]}, index=dates)
        high = pd.DataFrame({"AAA": [10.1, 12.2]}, index=dates)
        low = pd.DataFrame({"AAA": [9.9, 11.8]}, index=dates)
        volume = pd.DataFrame({"AAA": [100.0, 100.0]}, index=dates)

        with (
            patch("backend.services.backtest_service.load_strategy_from_code") as load_strategy,
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(close, open_, high, low, volume),
            ),
            patch.object(svc._backtest_engine, "_load_benchmark", return_value=close["AAA"]),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result"),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch.object(BacktestEngine, "run", side_effect=fake_run),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
        ):
            load_strategy.return_value = __import__(
                "backend.strategies.loader",
                fromlist=["_load_strategy_instance_unsafe"],
            )._load_strategy_instance_unsafe(_DEFAULT_CONFIG_STRATEGY_SOURCE)
            result = svc.run_backtest(
                "strategy-default-config",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-07",
                    "benchmark": "SPY",
                    "planned_price_fallback": "cancel",
                },
                "sp500",
                market="US",
            )

        self.assertEqual(captured["config"].execution_model, "planned_price")
        self.assertEqual(captured["config"].planned_price_fallback, "cancel")
        self.assertEqual(captured["config"].rebalance_freq, "daily")
        self.assertFalse(captured["config"].normalize_target_weights)
        self.assertEqual(float(captured["weights"].loc[dates[0], "AAA"]), 0.40)
        self.assertIsNotNone(captured["planned_prices"])
        self.assertEqual(result["config"]["effective_config"]["execution_model"], "planned_price")
        self.assertEqual(
            result["config"]["config_provenance"]["planned_price_fallback"],
            "run_override",
        )
        self.assertEqual(
            result["config"]["config_provenance"]["rebalance_freq"],
            "strategy_default",
        )
        self.assertEqual(
            result["config"]["strategy_default_config"]["execution_model"],
            "planned_price",
        )

    def test_backtest_request_position_sizing_override_is_applied_and_persisted(self):
        captured = {}
        saved = {}
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-position-override",
            "name": "Position Override Strategy",
            "version": 1,
            "source_code": _BASIC_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "equal_weight",
            "constraint_config": {},
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA", "BBB"]
        svc._model_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        strategy_instance = unittest.mock.Mock()
        strategy_instance.required_factors.return_value = []
        strategy_instance.generate_signals.return_value = pd.DataFrame(
            {
                "signal": [1, 1],
                "weight": [0.5, 0.5],
                "strength": [3.0, 1.0],
            },
            index=["AAA", "BBB"],
        )

        def fake_run(weights, config, **kwargs):
            captured["weights"] = weights.copy()
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

        def capture_save_result(**kwargs):
            saved.update(kwargs)

        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        prices = pd.DataFrame({"AAA": [10.0, 10.0], "BBB": [20.0, 20.0]}, index=dates)
        empty_prices = pd.DataFrame({"AAA": [0.0, 0.0], "BBB": [0.0, 0.0]}, index=dates)

        with (
            patch("backend.services.backtest_service.load_strategy_from_code", return_value=strategy_instance),
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(prices, prices, empty_prices, empty_prices, empty_prices),
            ),
            patch.object(svc._backtest_engine, "_load_benchmark", return_value=prices["AAA"]),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result", side_effect=capture_save_result),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch.object(BacktestEngine, "run", side_effect=fake_run),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
        ):
            result = svc.run_backtest(
                "strategy-position-override",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-07",
                    "benchmark": "SPY",
                    "rebalance_freq": "daily",
                    "position_sizing": "signal_weight",
                },
                "sp500",
                market="US",
            )

        first_day_weights = captured["weights"].loc[pd.Timestamp("2026-04-06")]
        self.assertAlmostEqual(float(first_day_weights["AAA"]), 0.75)
        self.assertAlmostEqual(float(first_day_weights["BBB"]), 0.25)
        self.assertEqual(result["config"]["position_sizing"], "signal_weight")
        self.assertEqual(result["config"]["effective_config"]["position_sizing"], "signal_weight")
        self.assertEqual(
            result["config"]["config_provenance"]["position_sizing"],
            "run_override",
        )
        self.assertEqual(saved["config"]["position_sizing"], "signal_weight")

    def test_strategy_per_order_execution_intent_allows_mixed_modes(self):
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-mixed-execution",
            "name": "Mixed Execution Strategy",
            "version": 1,
            "source_code": _MIXED_EXECUTION_INTENT_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "raw_weight",
            "constraint_config": {},
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA", "BBB", "CCC"]
        svc._model_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        close = pd.DataFrame(
            {"AAA": [10.0, 12.0], "BBB": [20.0, 22.0], "CCC": [30.0, 33.0]},
            index=dates,
        )
        open_ = pd.DataFrame(
            {"AAA": [10.0, 11.0], "BBB": [20.0, 20.5], "CCC": [30.0, 31.0]},
            index=dates,
        )
        high = pd.DataFrame(
            {"AAA": [10.0, 11.5], "BBB": [20.0, 21.2], "CCC": [30.0, 33.5]},
            index=dates,
        )
        low = pd.DataFrame(
            {"AAA": [10.0, 10.8], "BBB": [20.0, 20.8], "CCC": [30.0, 30.5]},
            index=dates,
        )
        volume = pd.DataFrame(
            {"AAA": [100.0, 100.0], "BBB": [100.0, 100.0], "CCC": [100.0, 100.0]},
            index=dates,
        )

        with (
            patch("backend.services.backtest_service.load_strategy_from_code") as load_strategy,
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(close, open_, high, low, volume),
            ),
            patch.object(svc._backtest_engine, "_load_benchmark", return_value=close["AAA"]),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result"),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
        ):
            load_strategy.return_value = __import__(
                "backend.strategies.loader",
                fromlist=["_load_strategy_instance_unsafe"],
            )._load_strategy_instance_unsafe(_MIXED_EXECUTION_INTENT_STRATEGY_SOURCE)
            result = svc.run_backtest(
                "strategy-mixed-execution",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-07",
                    "benchmark": "SPY",
                    "rebalance_freq": "daily",
                    "normalize_target_weights": False,
                    "execution_model": "next_open",
                },
                "sp500",
                market="US",
            )

        by_ticker = {trade["ticker"]: trade for trade in result["trades"]}
        self.assertEqual(by_ticker["AAA"]["price"], 11.0)
        self.assertEqual(by_ticker["BBB"]["price"], 21.0)
        self.assertEqual(by_ticker["CCC"]["price"], 33.0)
        intent_diag = result["trade_diagnostics"]["order_intents"]
        self.assertEqual(intent_diag["execution_model_counts"]["next_open"], 1)
        self.assertEqual(intent_diag["execution_model_counts"]["planned_price"], 1)
        self.assertEqual(intent_diag["execution_model_counts"]["next_close"], 1)
        self.assertEqual(
            {row["ticker"]: row["execution_model"] for row in intent_diag["orders"]},
            {"AAA": "next_open", "BBB": "planned_price", "CCC": "next_close"},
        )

    def test_strategy_context_actual_open_buffer_preserves_cash_weight(self):
        captured_diagnostics = {}
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy-actual-open-cash-buffer",
            "name": "Stateful Actual Open Cash Buffer Strategy",
            "version": 1,
            "source_code": _STATEFUL_ACTUAL_OPEN_CASH_BUFFER_STRATEGY_SOURCE,
            "required_factors": [],
            "required_models": [],
            "position_sizing": "raw_weight",
        }
        svc._group_service = unittest.mock.Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]
        svc._model_service = unittest.mock.Mock()
        svc._factor_engine = unittest.mock.Mock()
        svc._backtest_engine = BacktestEngine()

        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08"])
        prices = pd.DataFrame({"AAA": [10.0, 10.0, 10.0]}, index=dates)
        prices_empty = pd.DataFrame({"AAA": [0.0, 0.0, 0.0]}, index=dates)
        original_merge = BacktestService._merge_engine_rebalance_diagnostics

        def capture_merge(service_diagnostics, engine_diagnostics):
            captured_diagnostics["service"] = service_diagnostics
            captured_diagnostics["engine"] = engine_diagnostics
            return original_merge(service_diagnostics, engine_diagnostics)

        with (
            patch.object(BacktestService, "_validate_benchmark_market"),
            patch.object(StrategyService, "_validate_dependencies"),
            patch.object(
                svc._backtest_engine,
                "_load_prices",
                return_value=(prices, prices, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(svc._backtest_engine, "_load_benchmark", return_value=prices["AAA"]),
            patch.object(BacktestService, "_resolve_factor_ids", return_value={}),
            patch.object(BacktestService, "_batch_predict_all_dates", return_value={}),
            patch.object(BacktestService, "_save_result"),
            patch.object(BacktestService, "_build_reproducibility_fingerprint", return_value={}),
            patch.object(BacktestService, "_check_data_leakage", return_value=[]),
            patch.object(BacktestService, "_merge_engine_rebalance_diagnostics", side_effect=capture_merge),
            patch("backend.services.backtest_service.get_connection", return_value=self.conn),
        ):
            svc.run_backtest(
                "strategy-actual-open-cash-buffer",
                {
                    "start_date": "2026-04-06",
                    "end_date": "2026-04-08",
                    "benchmark": "SPY",
                    "rebalance_freq": "daily",
                    "rebalance_buffer": 0.10,
                    "rebalance_buffer_reference": "actual_open",
                    "normalize_target_weights": False,
                },
                "sp500",
                market="US",
            )

        service_diag_by_date = {
            item["date"]: item
            for item in captured_diagnostics["service"]
        }
        engine_diag_by_date = {
            item["date"]: item
            for item in captured_diagnostics["engine"]
        }

        self.assertEqual(
            engine_diag_by_date["2026-04-08"]["executed_positions_after"],
            {"AAA": 0.5},
        )
        self.assertEqual(
            service_diag_by_date["2026-04-08"]["context_weights"],
            {"AAA": 0.5},
        )

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

    def test_strategy_api_backtest_task_summary_includes_debug_artifact_id(self):
        executor = _FakeExecutor()
        strategy_service = _FakeStrategyService()
        backtest_service = _FakeBacktestService(
            result={
                "backtest_id": "bt_debug",
                "market": "US",
                "strategy_id": "strategy_us",
                "debug_artifact_id": "bt_debug",
            },
        )

        with (
            patch.object(strategy_api, "_get_strategy_service", return_value=strategy_service),
            patch.object(strategy_api, "_get_backtest_service", return_value=backtest_service),
            patch.object(strategy_api, "_get_executor", return_value=executor),
        ):
            asyncio.run(
                strategy_api.run_backtest(
                    "strategy_us",
                    strategy_api.RunBacktestRequest(
                        market="US",
                        config={"debug_mode": True},
                        universe_group_id="sp500",
                    ),
                )
            )

        summary = executor.fn(**executor.params)
        self.assertEqual(summary["debug_artifact_id"], "bt_debug")

    def test_strategy_api_debug_replay_routes_to_backtest_service(self):
        svc = _FakeBacktestService(
            result={"backtest_id": "bt_debug", "market": "US"},
        )

        with patch.object(strategy_api, "_get_backtest_service", return_value=svc):
            result = asyncio.run(
                strategy_api.get_backtest_debug_replay(
                    "bt_debug",
                    market="US",
                    date="2026-04-06",
                    ticker="AAA",
                )
            )

        self.assertEqual(result["backtest_id"], "bt_debug")
        self.assertEqual(svc.debug_replay_call["date"], "2026-04-06")
        self.assertEqual(svc.debug_replay_call["ticker"], "AAA")

    def test_debug_backtest_writes_readable_replay_bundle_and_cleanup(self):
        tmp_data = Path(self._tmp.name) / "runtime"
        settings = _FakeBacktestSettings(tmp_data)
        svc = BacktestService.__new__(BacktestService)
        debug_state = {
            "debug_mode": True,
            "debug_level": "signals",
            "debug_tickers": ["AAA"],
            "debug_dates": ["2026-04-06"],
        }
        result = BacktestResult(
            config={"start_date": "2026-04-06", "end_date": "2026-04-07"},
            dates=["2026-04-06", "2026-04-07"],
            nav=[1000.0, 1010.0],
            benchmark_nav=[1000.0, 1000.0],
            drawdown=[0.0, 0.0],
            total_return=0.01,
            annual_return=1.0,
            annual_volatility=0.1,
            max_drawdown=0.0,
            sharpe_ratio=1.0,
            calmar_ratio=0.0,
            sortino_ratio=0.0,
            win_rate=1.0,
            profit_loss_ratio=0.0,
            total_trades=1,
            annual_turnover=0.5,
            total_cost=1.0,
            monthly_returns=[],
            trades=[{"date": "2026-04-06", "ticker": "AAA", "action": "buy"}],
            trade_diagnostics={"rebalance_execution_diagnostics": []},
        )
        model_predictions = {"model_1": pd.Series({"AAA": 0.9, "BBB": 0.1})}
        raw_signals = pd.DataFrame(
            {"signal": [1, 1], "weight": [0.6, 0.4], "strength": [2.0, 1.0]},
            index=["AAA", "BBB"],
        )
        target_weights = {"AAA": 0.6, "BBB": 0.4}
        adjusted_weights = {"AAA": 0.5, "BBB": 0.5}

        with patch("backend.services.backtest_service.settings", settings):
            svc._record_debug_rebalance(
                debug_state,
                date_key="2026-04-06",
                model_predictions=model_predictions,
                factor_data={},
                raw_signals=raw_signals,
                target_weights=target_weights,
                adjusted_weights=adjusted_weights,
                context_diagnostics={"gate": "passed"},
                positions_before={},
                positions_after={"AAA": 0.5},
            )
            artifact = svc._write_debug_replay_bundle(
                backtest_id="bt_debug",
                market="US",
                strategy_id="strategy_us",
                config={"debug_mode": True},
                result=result,
                rebalance_diagnostics=[],
                debug_state=debug_state,
            )
            loaded = svc.get_debug_replay(
                "bt_debug",
                market="US",
                date="2026-04-06",
                ticker="AAA",
            )
            deleted = svc.cleanup_debug_replay(ttl_hours=0)

        self.assertEqual(artifact["id"], "bt_debug")
        self.assertEqual(loaded["manifest"]["market"], "US")
        self.assertEqual(loaded["items"][0]["raw_signals"], {"AAA": {"signal": 1.0, "weight": 0.6, "strength": 2.0}})
        self.assertEqual(loaded["items"][0]["model_predictions"]["model_1"], {"AAA": 0.9})
        self.assertEqual(deleted["deleted"], 1)
        self.assertFalse(Path(artifact["path"]).exists())

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
        self.debug_replay_call = None

    def run_backtest(self, strategy_id, config_dict, universe_group_id, market=None, **_kwargs):
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

    def get_debug_replay(self, backtest_id, market=None, date=None, ticker=None):
        self.debug_replay_call = {
            "backtest_id": backtest_id,
            "market": market,
            "date": date,
            "ticker": ticker,
        }
        return {
            "backtest_id": backtest_id,
            "market": market,
            "manifest": {},
            "items": [],
        }


class _FakeBacktestSettings:
    def __init__(self, root: Path):
        self.project_root = root
        self.models_dir = root / "models"


if __name__ == "__main__":
    unittest.main()
