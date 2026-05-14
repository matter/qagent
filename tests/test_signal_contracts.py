import unittest
from unittest.mock import patch

import pandas as pd

from backend.services.signal_service import SignalService


class _FakeSignalValidationConnection:
    def execute(self, query, params=None):
        return self

    def fetchone(self):
        return ("2026-04-03",)


class SignalServiceContractTests(unittest.TestCase):
    def test_dependency_validation_uses_imported_strategy_service(self):
        svc = SignalService.__new__(SignalService)
        strategy_def = {
            "name": "NoDeps",
            "status": "published",
            "required_factors": [],
            "required_models": [],
            "source_code": "class NoDeps: pass",
        }

        with patch(
            "backend.services.signal_service.get_connection",
            return_value=_FakeSignalValidationConnection(),
        ):
            validation = svc._validate_dependency_chain(
                strategy_def,
                target_date="2026-04-02",
                universe_group_id=None,
            )

        self.assertFalse(validation["blocked"])
        self.assertEqual(validation["model_statuses"], {})

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

    def test_backtest_replay_state_uses_rebalance_positions_after_when_available(self):
        svc = SignalService.__new__(SignalService)
        prices = __import__("pandas").DataFrame(
            {
                "AAA": [10.0],
                "BBB": [20.0],
            },
            index=__import__("pandas").to_datetime(["2026-01-30"]),
        )
        conn = _BacktestReplayDiagnosticsConnection()

        with patch("backend.services.signal_service.get_connection", return_value=conn):
            state = svc._reconstruct_portfolio_state(
                "bt_replay",
                "2026-01-30",
                prices,
                market="US",
            )

        self.assertEqual(state["current_weights"], {"AAA": 0.4, "BBB": 0.6})
        self.assertEqual(state["replay_positions_after"], {"AAA": 0.4, "BBB": 0.6})
        self.assertEqual(state["holding_days"], {})

    def test_replay_only_state_fields_are_filtered_before_strategy_context(self):
        context_kwargs = SignalService._strategy_context_portfolio_kwargs(
            {
                "current_weights": {"AAA": 0.4},
                "holding_days": {"AAA": 3},
                "avg_entry_price": {"AAA": 10.0},
                "unrealized_pnl": {"AAA": 0.1},
                "replay_positions_after": {"AAA": 0.4},
            }
        )

        self.assertEqual(
            context_kwargs,
            {
                "current_weights": {"AAA": 0.4},
                "holding_days": {"AAA": 3},
                "avg_entry_price": {"AAA": 10.0},
                "unrealized_pnl": {"AAA": 0.1},
            },
        )

    def test_backtest_replay_overlay_returns_saved_positions_as_canonical_signals(self):
        svc = SignalService.__new__(SignalService)

        signals, signal_tickers, replay = svc._apply_backtest_replay_overlay(
            generated_signals=[
                {"ticker": "AAA", "signal": "buy", "target_weight": 0.25, "strength": 0.7},
                {"ticker": "CCC", "signal": "buy", "target_weight": 0.75, "strength": 0.9},
            ],
            portfolio_state={
                "replay_positions_after": {"AAA": 0.4, "BBB": 0.6},
            },
            backtest_id="bt_replay",
            target_date="2026-01-30",
        )

        self.assertEqual([item["ticker"] for item in signals], ["BBB", "AAA"])
        self.assertEqual(signal_tickers, {"AAA", "BBB"})
        self.assertEqual(signals[0]["signal"], "backtest_replay")
        self.assertEqual(replay["source"], "backtest:bt_replay")
        self.assertFalse(replay["ticker_match"])
        self.assertEqual(replay["missing_from_generated"], ["BBB"])
        self.assertEqual(replay["extra_in_generated"], ["CCC"])
        self.assertEqual(replay["replay_positions_after"], {"AAA": 0.4, "BBB": 0.6})
        self.assertEqual(replay["generated_weights"], {"AAA": 0.25, "CCC": 0.75})

    def test_signal_constraints_apply_position_sizing_and_report_clipped_orders(self):
        raw = pd.DataFrame(
            {
                "signal": [1, 1],
                "weight": [0.9, 0.1],
                "strength": [0.9, 0.1],
            },
            index=["AAA", "BBB"],
        )

        adjusted, report = SignalService._apply_constraint_config_to_signals(
            raw,
            position_sizing="raw_weight",
            max_positions=5,
            max_position_pct=0.10,
            constraint_config={"max_single_name_weight": 0.15},
        )

        self.assertEqual(float(adjusted.loc["AAA", "weight"]), 0.15)
        self.assertEqual(float(adjusted.loc["BBB", "weight"]), 0.10)
        self.assertEqual(report["clipped_orders"][0]["ticker"], "AAA")
        self.assertTrue(report["constraint_pass"])

    def test_signal_service_can_stage_final_run_until_task_acceptance(self):
        svc = SignalService.__new__(SignalService)
        raw_signals = pd.DataFrame(
            {"signal": [1], "weight": [1.0], "strength": [2.0]},
            index=["AAA"],
        )
        staged = []
        saved = []

        def stage_domain_write(table, payload, commit=None):
            staged.append((table, payload, commit))

        class _PriceLookbackConnection:
            def execute(self, query, params=None):
                return self

            def fetchone(self):
                return ("2024-01-02",)

        prices = (
            pd.DataFrame({"AAA": [10.0]}, index=pd.to_datetime(["2024-01-02"])),
            pd.DataFrame({"AAA": [10.0]}, index=pd.to_datetime(["2024-01-02"])),
            pd.DataFrame({"AAA": [10.0]}, index=pd.to_datetime(["2024-01-02"])),
            pd.DataFrame({"AAA": [10.0]}, index=pd.to_datetime(["2024-01-02"])),
            pd.DataFrame({"AAA": [100]}, index=pd.to_datetime(["2024-01-02"])),
        )

        with (
            patch.object(svc, "_validate_dependency_chain", return_value={"blocked": False, "errors": [], "warnings": []}),
            patch("backend.services.signal_service.get_connection", return_value=_PriceLookbackConnection()),
            patch.object(svc, "_load_prices", return_value=prices),
            patch.object(svc, "_determine_result_level", return_value="exploratory"),
            patch.object(svc, "_build_dependency_snapshot", return_value={}),
            patch.object(svc, "_save_signal_run", side_effect=lambda **kwargs: saved.append(kwargs) or [{"ticker": "AAA"}]),
        ):
            svc._strategy_service = unittest.mock.Mock()
            strategy = unittest.mock.Mock()
            strategy.generate_signals.return_value = raw_signals
            svc._strategy_service.get_strategy.return_value = {
                "id": "strategy_stage",
                "name": "Stage Signal",
                "version": 1,
                "source_code": "source",
                "required_factors": [],
                "required_models": [],
                "position_sizing": "raw_weight",
            }
            svc._group_service = unittest.mock.Mock()
            svc._group_service.get_group_tickers.return_value = ["AAA"]
            svc._factor_engine = unittest.mock.Mock()
            svc._model_service = unittest.mock.Mock()

            with (
                patch("backend.services.signal_service.load_strategy_from_code", return_value=strategy),
                patch("backend.services.signal_service.StrategyService._validate_dependencies"),
            ):
                result = svc.generate_signals(
                    strategy_id="strategy_stage",
                    target_date="2024-01-02",
                    universe_group_id="group_stage",
                    market="US",
                    stage_domain_write=stage_domain_write,
                )
                self.assertEqual(saved, [])
                self.assertEqual(staged[0][0], "signal_runs")
                staged[0][2]()

        self.assertEqual(result["signal_count"], 1)
        self.assertEqual(saved[0]["run_id"], result["run_id"])

class _BacktestReplayDiagnosticsConnection:
    def execute(self, query, params=None):
        self.query = query
        return self

    def fetchone(self):
        import json

        return (
            json.dumps([]),
            json.dumps({"initial_capital": 1000}),
            json.dumps({
                "rebalance_diagnostics": [
                    {
                        "date": "2026-01-30",
                        "positions_after": {"AAA": 0.4, "BBB": 0.6},
                    }
                ]
            }),
        )


if __name__ == "__main__":
    unittest.main()
