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

    def test_candidate_pool_miss_returns_fixed_membership_fields(self):
        svc = SignalService.__new__(SignalService)

        detail = svc._analyze_candidate_pool_miss(
            ticker="OXY",
            strategy_diagnostics={
                "host_pool": ["AAPL"],
                "attack_pool": ["OXY"],
                "candidate_pool_pre_filter": ["AAPL", "OXY"],
                "candidate_pool": ["AAPL"],
            },
            model_predictions={},
            agg_scores={"AAPL": 0.7, "OXY": 0.8},
            candidate_set={"AAPL"},
        )

        membership = detail["pool_membership"]
        self.assertFalse(membership["in_host_pool"])
        self.assertTrue(membership["in_attack_pool"])
        self.assertFalse(membership["in_launch_pool"])
        self.assertFalse(membership["in_keep_extra"])
        self.assertTrue(membership["in_candidate_union_pre_filter"])
        self.assertFalse(membership["in_candidate_union_post_filter"])
        self.assertIn("structured_reason", detail)

    def test_selection_diagnostics_explain_current_book_blocking(self):
        svc = SignalService.__new__(SignalService)

        diagnostics = svc._build_selection_diagnostics(
            signals_list=[
                {"ticker": "AAA", "target_weight": 0.5, "strength": 1.0},
                {"ticker": "BBB", "target_weight": 0.5, "strength": 0.9},
            ],
            candidate_pool=["AAA", "BBB", "CCC"],
            portfolio_state={
                "current_weights": {"AAA": 0.5, "BBB": 0.5},
                "holding_days": {"AAA": 7, "BBB": 5},
                "avg_entry_price": {"AAA": 10.0, "BBB": 20.0},
                "unrealized_pnl": {"AAA": 0.12, "BBB": 0.08},
            },
            strategy_diagnostics={
                "replacement_trace": {
                    "selected": [
                        {"ticker": "AAA", "selected_score": 0.7, "lane": "current"},
                        {"ticker": "BBB", "selected_score": 0.6, "lane": "current"},
                    ],
                    "top_conversion": [
                        {"ticker": "CCC", "selected_score": 0.8, "lane": "conversion"},
                    ],
                }
            },
        )

        self.assertEqual(diagnostics["selected_current_count"], 2)
        self.assertEqual(diagnostics["selected_profitable_current_count"], 2)
        self.assertEqual(diagnostics["replaceable_slots_available"], 0)
        self.assertTrue(diagnostics["blocked_by_full_current_book"])
        self.assertTrue(diagnostics["blocked_by_nonreplaceable_current_holdings"])
        self.assertEqual(diagnostics["current_selected_meta"][0]["ticker"], "AAA")
        self.assertEqual(diagnostics["top_conversion_detail"][0]["ticker"], "CCC")
        self.assertTrue(diagnostics["top_conversion_detail"][0]["blocked_by_full_current_book"])


if __name__ == "__main__":
    unittest.main()
