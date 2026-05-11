import unittest

import pandas as pd

from backend.factors.loader import load_factor_from_code
from backend.strategies.base import StrategyContext
from backend.strategies.loader import load_strategy_from_code
from backend.services.custom_code_runner import UserCodeExecutionError


class CustomCodeSafetyContractTests(unittest.TestCase):
    def test_factor_loader_rejects_unbounded_loop_before_exec(self):
        source = """
from backend.factors.base import FactorBase

class BadFactor(FactorBase):
    name = "bad"

    def compute(self, data):
        while True:
            pass
"""

        with self.assertRaisesRegex(ValueError, "Unbounded while True"):
            load_factor_from_code(source)

    def test_strategy_loader_rejects_file_io_before_exec(self):
        source = """
from backend.strategies.base import StrategyBase

class BadStrategy(StrategyBase):
    name = "bad"

    def generate_signals(self, context):
        open("/tmp/qagent-leak", "w").write("x")
"""

        with self.assertRaisesRegex(ValueError, "open"):
            load_strategy_from_code(source)

    def test_factor_compute_runs_in_isolated_process(self):
        source = """
from backend.factors.base import FactorBase

class CloseFactor(FactorBase):
    name = "close_factor"

    def compute(self, data):
        return data["close"] * 2
"""

        factor = load_factor_from_code(source)
        frame = pd.DataFrame(
            {"close": [1.0, 2.0]},
            index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        )

        result = factor.compute(frame)

        self.assertEqual(factor.name, "close_factor")
        pd.testing.assert_series_equal(result, frame["close"] * 2)

    def test_factor_compute_timeout_kills_isolated_process(self):
        source = """
from backend.factors.base import FactorBase

class SlowFactor(FactorBase):
    name = "slow_factor"

    def compute(self, data):
        for _ in range(10 ** 9):
            pass
        return data["close"]
"""

        factor = load_factor_from_code(source)
        factor._execution_timeout_seconds = 0.01

        with self.assertRaisesRegex(UserCodeExecutionError, "timed out|exited with code"):
            factor.compute(pd.DataFrame({"close": [1.0]}))

    def test_strategy_generate_signals_runs_in_isolated_process(self):
        source = """
import pandas as pd
from backend.strategies.base import StrategyBase

class BuyStrategy(StrategyBase):
    name = "buy_strategy"

    def required_factors(self):
        return ["momentum_20"]

    def generate_signals(self, context):
        return pd.DataFrame(
            {"signal": [1], "weight": [0.5], "strength": [2.0]},
            index=["AAA"],
        )
"""

        strategy = load_strategy_from_code(source)
        result = strategy.generate_signals(StrategyContext(prices=pd.DataFrame()))

        self.assertEqual(strategy.name, "buy_strategy")
        self.assertEqual(strategy.required_factors(), ["momentum_20"])
        self.assertEqual(result.loc["AAA", "signal"], 1)

    def test_isolated_strategy_propagates_context_diagnostics(self):
        source = """
import pandas as pd
from backend.strategies.base import StrategyBase

class DiagnosticStrategy(StrategyBase):
    name = "diagnostic_strategy"

    def generate_signals(self, context):
        context.diagnostics["probe"] = {"selected": ["AAA"], "score": 0.7}
        return pd.DataFrame(
            {"signal": [1], "weight": [0.5], "strength": [2.0]},
            index=["AAA"],
        )
"""

        strategy = load_strategy_from_code(source)
        context = StrategyContext(prices=pd.DataFrame())

        result = strategy.generate_signals(context)

        self.assertEqual(result.loc["AAA", "signal"], 1)
        self.assertEqual(
            context.diagnostics["probe"],
            {"selected": ["AAA"], "score": 0.7},
        )

    def test_isolated_strategy_sanitizes_non_json_diagnostics(self):
        source = """
import pandas as pd
from backend.strategies.base import StrategyBase

class DiagnosticStrategy(StrategyBase):
    name = "diagnostic_strategy"

    def generate_signals(self, context):
        context.diagnostics["ticker_set"] = {"BBB", "AAA"}
        context.diagnostics["frame"] = pd.DataFrame({"score": [1.0]}, index=["AAA"])
        return pd.DataFrame(
            {"signal": [1], "weight": [0.5], "strength": [2.0]},
            index=["AAA"],
        )
"""

        strategy = load_strategy_from_code(source)
        context = StrategyContext(prices=pd.DataFrame())

        strategy.generate_signals(context)

        self.assertEqual(context.diagnostics["ticker_set"], ["AAA", "BBB"])
        self.assertIsInstance(context.diagnostics["frame"], str)

    def test_strategy_generate_signals_timeout_kills_isolated_process(self):
        source = """
from backend.strategies.base import StrategyBase

class SlowStrategy(StrategyBase):
    name = "slow_strategy"

    def generate_signals(self, context):
        for _ in range(10 ** 9):
            pass
        return None
"""

        strategy = load_strategy_from_code(source)
        strategy._execution_timeout_seconds = 0.01

        with self.assertRaisesRegex(UserCodeExecutionError, "timed out|exited with code"):
            strategy.generate_signals(StrategyContext(prices=pd.DataFrame()))


if __name__ == "__main__":
    unittest.main()
