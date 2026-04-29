import unittest

from backend.services.strategy_service import StrategyService


class StrategyServiceContractTests(unittest.TestCase):
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
