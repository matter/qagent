import unittest
from unittest.mock import patch

import pandas as pd

from backend.services.backtest_engine import BacktestConfig, BacktestEngine


class BacktestEngineContractTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
