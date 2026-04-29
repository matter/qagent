import unittest
from unittest.mock import patch

from backend.services.signal_service import SignalService


class SignalServiceContractTests(unittest.TestCase):
    def test_diagnose_model_prediction_skips_executor_when_no_required_models(self):
        svc = SignalService.__new__(SignalService)

        with patch(
            "backend.services.signal_service.ThreadPoolExecutor",
            side_effect=AssertionError("executor should not be constructed"),
        ):
            model_predictions, model_snapshot = svc._predict_diagnose_models(
                required_models=[],
                prepare_features=lambda model_id: None,
                predict_one=lambda model_id: (model_id, None, {}),
                per_model_timeout=30,
            )

        self.assertEqual(model_predictions, {})
        self.assertEqual(model_snapshot, {})

    def test_required_model_validation_raises_for_missing_model_predictions(self):
        svc = SignalService.__new__(SignalService)

        with self.assertRaisesRegex(ValueError, "missing_model_predictions"):
            svc._raise_if_missing_model_predictions(
                required_models=["model_a", "model_b"],
                model_predictions={"model_a": object()},
                context="signal_generate",
            )

    def test_diagnose_execution_date_role_maps_to_previous_decision_day(self):
        svc = SignalService.__new__(SignalService)

        resolved = svc._resolve_diagnose_dates(
            target_date="2026-04-10",
            date_role="execution",
        )

        self.assertEqual(resolved["decision_date"], "2026-04-09")
        self.assertEqual(resolved["execution_date"], "2026-04-10")
        self.assertEqual(resolved["date_role"], "execution")
        self.assertEqual(resolved["snapshot_timing"], "pre_trade")


if __name__ == "__main__":
    unittest.main()
