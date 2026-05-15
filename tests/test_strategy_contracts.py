import unittest
from unittest.mock import patch

from backend.services.strategy_service import StrategyService


class StrategyServiceContractTests(unittest.TestCase):
    def test_list_strategy_row_is_summary_without_source_code(self):
        row = (
            "strategy_1",
            "US",
            "Fast List Strategy",
            1,
            "description",
            "from backend.strategies.base import StrategyBase\n",
            "[]",
            "[]",
            "equal_weight",
            None,
            "draft",
            "2026-05-15 10:00:00",
            "2026-05-15 10:00:00",
        )

        with patch.object(
            StrategyService,
            "extract_strategy_metadata_from_source",
            side_effect=AssertionError("list rows must not parse strategy source"),
        ):
            summary = StrategyService._row_to_summary_dict(row)

        self.assertEqual(summary["id"], "strategy_1")
        self.assertEqual(summary["name"], "Fast List Strategy")
        self.assertNotIn("source_code", summary)
        self.assertEqual(summary["default_backtest_config"], {})
        self.assertEqual(summary["default_paper_config"], {})

    def test_extracts_strategy_default_configs_from_source_metadata(self):
        source = '''
from backend.strategies.base import StrategyBase

class DefaultsStrategy(StrategyBase):
    name = "Defaults Strategy"
    default_backtest_config = {
        "position_sizing": "raw_weight",
        "rebalance_freq": "daily",
        "execution_model": "planned_price",
        "planned_price_fallback": "next_close",
    }
    default_paper_config = {"execution_model": "planned_price"}

    def generate_signals(self, context):
        return {}
'''

        metadata = StrategyService.extract_strategy_metadata_from_source(source)

        self.assertEqual(
            metadata["default_backtest_config"]["execution_model"],
            "planned_price",
        )
        self.assertEqual(
            metadata["default_backtest_config"]["planned_price_fallback"],
            "next_close",
        )
        self.assertEqual(
            metadata["default_paper_config"]["execution_model"],
            "planned_price",
        )

    def test_strategy_defaults_reject_experiment_environment_fields(self):
        source = '''
from backend.strategies.base import StrategyBase

class BadDefaultsStrategy(StrategyBase):
    name = "Bad Defaults Strategy"
    default_backtest_config = {"market": "CN", "initial_capital": 100}

    def generate_signals(self, context):
        return {}
'''

        with self.assertRaisesRegex(ValueError, "strategy default"):
            StrategyService.extract_strategy_metadata_from_source(source)

    def test_extracts_auxiliary_model_constants_used_as_prediction_keys(self):
        source = '''
from backend.strategies.base import StrategyBase

class AuxStrategy(StrategyBase):
    AUX_MODEL_ID = "95a3ca34a3f5"

    def required_models(self):
        return ["base_model"]

    def generate_signals(self, context):
        aux = context.model_predictions.get(self.AUX_MODEL_ID)
        direct = context.model_predictions["literal_model"]
        return aux
'''

        referenced = StrategyService._extract_model_references(source)

        self.assertEqual(referenced, {"95a3ca34a3f5", "literal_model"})

    def test_resolve_required_models_unions_metadata_and_source_references(self):
        strategy_def = {
            "required_models": ["base_model"],
            "source_code": '''
CONV_AUX_MODEL_ID = "95a3ca34a3f5"

class S:
    def generate_signals(self, context):
        return context.model_predictions.get(CONV_AUX_MODEL_ID)
''',
        }

        resolved = StrategyService.resolve_required_models(strategy_def)

        self.assertEqual(resolved, ["95a3ca34a3f5", "base_model"])


if __name__ == "__main__":
    unittest.main()
