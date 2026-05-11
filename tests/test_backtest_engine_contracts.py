import unittest
from unittest.mock import patch

import pandas as pd

from backend.services.backtest_engine import BacktestConfig, BacktestEngine


class BacktestEngineContractTests(unittest.TestCase):
    def test_planned_price_execution_fills_at_planned_price_inside_buffer(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        prices_close = pd.DataFrame({"AAA": [10.0, 12.0]}, index=dates)
        prices_open = pd.DataFrame({"AAA": [10.0, 11.0]}, index=dates)
        prices_high = pd.DataFrame({"AAA": [10.2, 12.2]}, index=dates)
        prices_low = pd.DataFrame({"AAA": [9.8, 10.8]}, index=dates)
        prices_empty = pd.DataFrame({"AAA": [0.0, 0.0]}, index=dates)
        signals = pd.DataFrame({"AAA": [1.0, 1.0]}, index=dates)
        planned_prices = pd.DataFrame({"AAA": [11.5, 11.5]}, index=dates)
        config = BacktestConfig(
            initial_capital=1150.0,
            start_date="2026-04-06",
            end_date="2026-04-07",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
            execution_model="planned_price",
            planned_price_buffer_bps=50,
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_high, prices_low, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config, planned_prices=planned_prices)

        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0]["price"], 11.5)
        self.assertEqual(result.trades[0]["shares"], 100.0)
        planned_diag = result.trade_diagnostics["planned_price_execution"]
        self.assertEqual(planned_diag["filled_order_count"], 1)
        self.assertEqual(planned_diag["blocked_order_count"], 0)
        self.assertEqual(planned_diag["execution_model"], "planned_price")

    def test_planned_price_execution_rejects_outside_buffer_without_mutating_holdings(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        prices_close = pd.DataFrame({"AAA": [10.0, 12.0]}, index=dates)
        prices_open = pd.DataFrame({"AAA": [10.0, 11.0]}, index=dates)
        prices_high = pd.DataFrame({"AAA": [10.2, 12.0]}, index=dates)
        prices_low = pd.DataFrame({"AAA": [9.8, 10.8]}, index=dates)
        prices_empty = pd.DataFrame({"AAA": [0.0, 0.0]}, index=dates)
        signals = pd.DataFrame({"AAA": [1.0, 1.0]}, index=dates)
        planned_prices = pd.DataFrame({"AAA": [10.81, 10.81]}, index=dates)
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-07",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
            execution_model="planned_price",
            planned_price_buffer_bps=50,
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_high, prices_low, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config, planned_prices=planned_prices)

        self.assertEqual(result.trades, [])
        self.assertEqual(result.nav, [1000.0, 1000.0])
        planned_diag = result.trade_diagnostics["planned_price_execution"]
        self.assertEqual(planned_diag["filled_order_count"], 0)
        self.assertEqual(planned_diag["blocked_order_count"], 1)
        self.assertEqual(
            planned_diag["blocked"][0]["reason"],
            "planned_price_outside_buffered_range",
        )

    def test_nav_includes_cash_after_trade_costs(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        prices_close = pd.DataFrame({"AAA": [10.0, 12.0]}, index=dates)
        prices_open = pd.DataFrame({"AAA": [10.0, 10.0]}, index=dates)
        prices_empty = pd.DataFrame({"AAA": [0.0, 0.0]}, index=dates)
        signals = pd.DataFrame({"AAA": [1.0, 1.0]}, index=dates)
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-07",
            commission_rate=0.001,
            slippage_rate=0.001,
            rebalance_freq="daily",
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config)

        self.assertEqual(result.nav[0], 1000.0)
        self.assertEqual(result.nav[1], 1198.0)
        self.assertEqual(result.total_cost, 2.0)

    def test_missing_close_for_one_holding_carries_forward_last_price(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08"])
        prices_close = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 10.0],
                "BBB": [10.0, 10.0, None],
            },
            index=dates,
        )
        prices_open = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 10.0],
                "BBB": [10.0, 10.0, 10.0],
            },
            index=dates,
        )
        prices_empty = pd.DataFrame(
            {
                "AAA": [0.0, 0.0, 0.0],
                "BBB": [0.0, 0.0, 0.0],
            },
            index=dates,
        )
        signals = pd.DataFrame(
            {
                "AAA": [0.5, 0.5, 0.5],
                "BBB": [0.5, 0.5, 0.5],
            },
            index=dates,
        )
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-08",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config)

        self.assertEqual(result.nav, [1000.0, 1000.0, 1000.0])
        self.assertEqual(result.trade_diagnostics["missing_price_valuations"][0]["ticker"], "BBB")
        self.assertEqual(result.trade_diagnostics["missing_price_valuations"][0]["date"], "2026-04-08")
        self.assertEqual(result.trade_diagnostics["missing_price_valuations"][0]["valuation_method"], "last_close_carry_forward")

    def test_can_preserve_partial_target_weights_as_cash_budget(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07"])
        prices_close = pd.DataFrame({"AAA": [10.0, 10.0]}, index=dates)
        prices_open = pd.DataFrame({"AAA": [10.0, 10.0]}, index=dates)
        prices_empty = pd.DataFrame({"AAA": [0.0, 0.0]}, index=dates)
        signals = pd.DataFrame({"AAA": [0.5, 0.5]}, index=dates)
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-07",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
            normalize_target_weights=False,
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config)

        self.assertEqual(result.nav, [1000.0, 1000.0])
        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0]["shares"], 50.0)
        self.assertEqual(result.trade_diagnostics["target_weight_policy"]["normalized"], False)
        self.assertAlmostEqual(result.trade_diagnostics["target_weight_policy"]["last_cash_weight"], 0.5)

    def test_hold_overlap_buffer_skips_small_add_reduce_without_renormalizing(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08"])
        prices_close = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 10.0],
                "BBB": [10.0, 10.0, 10.0],
            },
            index=dates,
        )
        prices_open = prices_close.copy()
        prices_empty = pd.DataFrame(
            {
                "AAA": [0.0, 0.0, 0.0],
                "BBB": [0.0, 0.0, 0.0],
            },
            index=dates,
        )
        signals = pd.DataFrame(
            {
                "AAA": [0.50, 0.54, 0.54],
                "BBB": [0.50, 0.46, 0.46],
            },
            index=dates,
        )
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-08",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
            rebalance_buffer=0.05,
            rebalance_buffer_mode="hold_overlap_only",
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config)

        self.assertEqual(len(result.trades), 2)
        self.assertEqual([t["trade_reason"] for t in result.trades], ["new_entry", "new_entry"])
        self.assertNotIn("add", [t["trade_reason"] for t in result.trades])
        self.assertNotIn("reduce", [t["trade_reason"] for t in result.trades])
        diagnostics = result.trade_diagnostics["rebalance_execution_diagnostics"]
        self.assertEqual(diagnostics[1]["date"], "2026-04-08")
        self.assertEqual(diagnostics[1]["target_positions_after"], {"AAA": 0.54, "BBB": 0.46})
        self.assertEqual(diagnostics[1]["executed_positions_after"], {"AAA": 0.5, "BBB": 0.5})
        self.assertAlmostEqual(diagnostics[1]["target_turnover"], 0.08)
        self.assertAlmostEqual(diagnostics[1]["turnover"], 0.0)
        self.assertEqual(diagnostics[1]["diagnostic_layers"]["executed_positions_after"], "post_buffer")

    def test_hold_overlap_buffer_can_compare_against_actual_open_weights(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08"])
        prices_close = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 12.0],
                "BBB": [10.0, 10.0, 8.0],
            },
            index=dates,
        )
        prices_open = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 12.0],
                "BBB": [10.0, 10.0, 8.0],
            },
            index=dates,
        )
        prices_empty = pd.DataFrame(
            {
                "AAA": [0.0, 0.0, 0.0],
                "BBB": [0.0, 0.0, 0.0],
            },
            index=dates,
        )
        signals = pd.DataFrame(
            {
                "AAA": [0.50, 0.54, 0.54],
                "BBB": [0.50, 0.46, 0.46],
            },
            index=dates,
        )
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-08",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
            rebalance_buffer=0.05,
            rebalance_buffer_mode="hold_overlap_only",
            rebalance_buffer_reference="actual_open",
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config)

        self.assertEqual(len(result.trades), 4)
        self.assertEqual(
            [t["trade_reason"] for t in result.trades[:2]],
            ["new_entry", "new_entry"],
        )
        self.assertEqual(
            sorted(t["trade_reason"] for t in result.trades[2:]),
            ["add", "reduce"],
        )

    def test_actual_open_buffer_does_not_log_zero_share_trades_when_skipping(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08"])
        prices_close = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 12.0],
                "BBB": [10.0, 10.0, 8.0],
            },
            index=dates,
        )
        prices_open = prices_close.copy()
        prices_empty = pd.DataFrame(
            {
                "AAA": [0.0, 0.0, 0.0],
                "BBB": [0.0, 0.0, 0.0],
            },
            index=dates,
        )
        signals = pd.DataFrame(
            {
                "AAA": [0.50, 0.59, 0.59],
                "BBB": [0.50, 0.41, 0.41],
            },
            index=dates,
        )
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-08",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
            rebalance_buffer=0.05,
            rebalance_buffer_mode="hold_overlap_only",
            rebalance_buffer_reference="actual_open",
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config)

        self.assertEqual(len(result.trades), 2)
        self.assertEqual([t["trade_reason"] for t in result.trades], ["new_entry", "new_entry"])
        self.assertEqual(result.annual_turnover, 84.0)

    def test_hold_overlap_buffer_supports_asymmetric_add_reduce_thresholds(self):
        engine = BacktestEngine()
        dates = pd.to_datetime(["2026-04-06", "2026-04-07", "2026-04-08"])
        prices_close = pd.DataFrame(
            {
                "AAA": [10.0, 10.0, 12.0],
                "BBB": [10.0, 10.0, 8.0],
            },
            index=dates,
        )
        prices_open = prices_close.copy()
        prices_empty = pd.DataFrame(
            {
                "AAA": [0.0, 0.0, 0.0],
                "BBB": [0.0, 0.0, 0.0],
            },
            index=dates,
        )
        signals = pd.DataFrame(
            {
                "AAA": [0.50, 0.54, 0.54],
                "BBB": [0.50, 0.46, 0.46],
            },
            index=dates,
        )
        config = BacktestConfig(
            initial_capital=1000.0,
            start_date="2026-04-06",
            end_date="2026-04-08",
            commission_rate=0.0,
            slippage_rate=0.0,
            rebalance_freq="daily",
            rebalance_buffer=0.05,
            rebalance_buffer_add=1.0,
            rebalance_buffer_reduce=0.05,
            rebalance_buffer_mode="hold_overlap_only",
            rebalance_buffer_reference="actual_open",
        )

        with (
            patch.object(
                engine,
                "_load_prices",
                return_value=(prices_close, prices_open, prices_empty, prices_empty, prices_empty),
            ),
            patch.object(engine, "_load_benchmark", return_value=prices_close["AAA"]),
        ):
            result = engine.run(signals, config)

        self.assertEqual(
            [t["trade_reason"] for t in result.trades[:2]],
            ["new_entry", "new_entry"],
        )
        self.assertEqual([t["trade_reason"] for t in result.trades[2:]], ["reduce"])
        self.assertEqual(result.trades[2]["ticker"], "AAA")


if __name__ == "__main__":
    unittest.main()
