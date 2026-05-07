import unittest

from backend.factors.loader import load_factor_from_code
from backend.strategies.loader import load_strategy_from_code


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


if __name__ == "__main__":
    unittest.main()
