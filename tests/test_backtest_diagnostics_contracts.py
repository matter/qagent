import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from unittest.mock import patch

import pandas as pd

from backend.services.backtest_engine import BacktestResult
from backend.services.backtest_service import BacktestService


class BacktestDiagnosticsContractTests(unittest.TestCase):
    def test_rebalance_delta_reports_position_changes_and_turnover(self):
        diag = BacktestService._build_rebalance_diagnostics(
            date_key="2026-04-10",
            positions_before={"AAPL": 0.5, "MSFT": 0.5},
            positions_after={"MSFT": 0.4, "NVDA": 0.6},
            strategy_diagnostics={"candidate_pool": ["MSFT", "NVDA"]},
        )

        self.assertEqual(diag["date"], "2026-04-10")
        self.assertEqual(diag["positions_before"], {"AAPL": 0.5, "MSFT": 0.5})
        self.assertEqual(diag["positions_after"], {"MSFT": 0.4, "NVDA": 0.6})
        self.assertEqual(diag["added"], ["NVDA"])
        self.assertEqual(diag["removed"], ["AAPL"])
        self.assertEqual(diag["increased"], [])
        self.assertEqual(diag["decreased"], ["MSFT"])
        self.assertAlmostEqual(diag["turnover"], 1.2)
        self.assertEqual(diag["candidate_pool"], ["MSFT", "NVDA"])

    def test_rebalance_diagnostics_distinguish_target_and_executed_weights(self):
        diag = BacktestService._build_rebalance_diagnostics(
            date_key="2026-04-10",
            positions_before={"AAPL": 0.5, "MSFT": 0.5},
            positions_after={"AAPL": 0.5, "MSFT": 0.5},
            target_positions_after={"MSFT": 0.4, "NVDA": 0.6},
            target_layer="strategy_sized",
            executed_layer="post_buffer",
        )

        self.assertEqual(diag["positions_after"], {"AAPL": 0.5, "MSFT": 0.5})
        self.assertEqual(diag["executed_positions_after"], {"AAPL": 0.5, "MSFT": 0.5})
        self.assertEqual(diag["target_positions_after"], {"MSFT": 0.4, "NVDA": 0.6})
        self.assertEqual(diag["diagnostic_layers"]["positions_after"], "post_buffer")
        self.assertEqual(diag["diagnostic_layers"]["target_positions_after"], "strategy_sized")
        self.assertEqual(diag["turnover"], 0.0)
        self.assertAlmostEqual(diag["target_turnover"], 1.2)

    def test_rebalance_diagnostics_merge_engine_post_buffer_execution_layer(self):
        diagnostics = [
            {
                "date": "2026-04-07",
                "positions_before": {},
                "positions_after": {"AAA": 0.5, "BBB": 0.5},
                "turnover": 1.0,
            },
            {
                "date": "2026-04-08",
                "positions_before": {"AAA": 0.5, "BBB": 0.5},
                "positions_after": {"AAA": 0.54, "BBB": 0.46},
                "turnover": 0.08,
            },
        ]
        engine_diagnostics = [
            {
                "date": "2026-04-08",
                "positions_before": {"AAA": 0.5, "BBB": 0.5},
                "positions_after": {"AAA": 0.5, "BBB": 0.5},
                "executed_positions_after": {"AAA": 0.5, "BBB": 0.5},
                "target_positions_after": {"AAA": 0.54, "BBB": 0.46},
                "turnover": 0.0,
                "target_turnover": 0.08,
                "diagnostic_layers": {
                    "positions_after": "post_buffer",
                    "executed_positions_after": "post_buffer",
                    "target_positions_after": "pre_buffer",
                },
            }
        ]

        merged = BacktestService._merge_engine_rebalance_diagnostics(
            diagnostics,
            engine_diagnostics,
        )

        self.assertEqual(merged[0]["positions_after"], {"AAA": 0.5, "BBB": 0.5})
        self.assertEqual(merged[1]["positions_after"], {"AAA": 0.5, "BBB": 0.5})
        self.assertEqual(merged[1]["target_positions_after"], {"AAA": 0.54, "BBB": 0.46})
        self.assertEqual(merged[1]["turnover"], 0.0)
        self.assertEqual(merged[1]["target_turnover"], 0.08)
        self.assertEqual(merged[1]["diagnostic_layers"]["positions_after"], "post_buffer")

    def test_planned_price_matrix_uses_strategy_output_and_close_fallback(self):
        dates = pd.to_datetime(["2026-04-06"])
        prices_close = pd.DataFrame(
            {"AAA": [10.0], "BBB": [20.0], "CCC": [30.0]},
            index=dates,
        )
        raw_signals = pd.DataFrame(
            {
                "signal": [1, 1, 1],
                "weight": [0.4, 0.3, 0.3],
                "strength": [3.0, 2.0, 1.0],
                "planned_price": [10.5, None, 0.0],
            },
            index=["AAA", "BBB", "CCC"],
        )
        planned_prices = pd.DataFrame(index=dates, columns=["AAA", "BBB", "CCC"], dtype=float)
        diagnostics = {"fallback_count": 0, "invalid_count": 0, "samples": []}

        BacktestService._write_planned_prices_for_date(
            planned_prices=planned_prices,
            raw_signals=raw_signals,
            selected_weights={"AAA": 0.4, "BBB": 0.3, "CCC": 0.3},
            prices_close=prices_close,
            trade_ts=dates[0],
            diagnostics=diagnostics,
        )

        self.assertEqual(float(planned_prices.loc[dates[0], "AAA"]), 10.5)
        self.assertEqual(float(planned_prices.loc[dates[0], "BBB"]), 20.0)
        self.assertEqual(float(planned_prices.loc[dates[0], "CCC"]), 30.0)
        self.assertEqual(diagnostics["fallback_count"], 2)
        self.assertEqual(diagnostics["invalid_count"], 1)
        self.assertTrue(
            any(sample["planned_price_source"] == "decision_close" for sample in diagnostics["samples"])
        )

    def test_planned_price_matrix_includes_positions_removed_from_target(self):
        dates = pd.to_datetime(["2026-04-06"])
        prices_close = pd.DataFrame(
            {"AAA": [10.0], "BBB": [20.0]},
            index=dates,
        )
        raw_signals = pd.DataFrame(
            {
                "signal": [1],
                "weight": [0.5],
                "strength": [3.0],
                "planned_price": [10.5],
            },
            index=["AAA"],
        )
        planned_prices = pd.DataFrame(index=dates, columns=["AAA", "BBB"], dtype=float)
        diagnostics = {"fallback_count": 0, "invalid_count": 0, "samples": []}

        BacktestService._write_planned_prices_for_date(
            planned_prices=planned_prices,
            raw_signals=raw_signals,
            selected_weights={"AAA": 0.5},
            current_weights={"BBB": 0.5},
            prices_close=prices_close,
            trade_ts=dates[0],
            diagnostics=diagnostics,
        )

        self.assertEqual(float(planned_prices.loc[dates[0], "AAA"]), 10.5)
        self.assertEqual(float(planned_prices.loc[dates[0], "BBB"]), 20.0)
        bbb_sample = next(sample for sample in diagnostics["samples"] if sample["ticker"] == "BBB")
        self.assertEqual(bbb_sample["planned_price_source"], "decision_close")

    def test_portfolio_compliance_metrics_flag_concentration_without_default_holding_violation(self):
        metrics = BacktestService._build_portfolio_compliance_metrics(
            rebalance_diagnostics=[
                {"date": "2026-01-05", "positions_after": {"AAPL": 1.0}},
                {"date": "2026-01-06", "positions_after": {"AAPL": 0.5, "MSFT": 0.5}},
            ],
            trades=[
                {"date": "2026-01-05", "ticker": "AAPL", "action": "buy", "holding_days": 0},
                {"date": "2026-02-20", "ticker": "AAPL", "action": "sell", "holding_days": 41},
            ],
        )

        self.assertEqual(metrics["min_position_count"], 1)
        self.assertEqual(metrics["max_trade_holding_days"], 41)
        self.assertEqual(metrics["max_target_weight"], 1.0)
        self.assertFalse(metrics["compliance_pass"])
        self.assertIn("min_position_count", metrics["violations"])
        self.assertNotIn("max_trade_holding_days", metrics["violations"])
        self.assertIn("max_trade_holding_days", metrics["heuristic_violations"])

    def test_portfolio_compliance_metrics_enforces_configured_holding_limit(self):
        metrics = BacktestService._build_portfolio_compliance_metrics(
            rebalance_diagnostics=[
                {
                    "date": "2026-01-05",
                    "positions_after": {
                        "A": 0.15,
                        "B": 0.15,
                        "C": 0.15,
                        "D": 0.15,
                        "E": 0.15,
                        "F": 0.15,
                        "G": 0.10,
                    },
                }
            ],
            trades=[
                {"date": "2026-02-20", "ticker": "A", "action": "sell", "holding_days": 41},
            ],
            config={"compliance_max_holding_days": 21},
        )

        self.assertFalse(metrics["compliance_pass"])
        self.assertIn("max_trade_holding_days", metrics["violations"])

    def test_portfolio_compliance_metrics_pass_for_diversified_short_hold_portfolio(self):
        metrics = BacktestService._build_portfolio_compliance_metrics(
            rebalance_diagnostics=[
                {
                    "date": "2026-01-05",
                    "positions_after": {
                        "A": 0.15,
                        "B": 0.15,
                        "C": 0.15,
                        "D": 0.15,
                        "E": 0.15,
                        "F": 0.15,
                        "G": 0.10,
                    },
                }
            ],
            trades=[
                {"date": "2026-01-05", "ticker": "A", "action": "buy", "holding_days": 0},
                {"date": "2026-01-24", "ticker": "A", "action": "sell", "holding_days": 19},
            ],
        )

        self.assertEqual(metrics["min_position_count"], 7)
        self.assertEqual(metrics["max_trade_holding_days"], 19)
        self.assertLessEqual(metrics["max_target_weight"], 0.15)
        self.assertTrue(metrics["compliance_pass"])

    def test_portfolio_compliance_metrics_does_not_fail_long_holds_without_hard_constraint(self):
        metrics = BacktestService._build_portfolio_compliance_metrics(
            rebalance_diagnostics=[
                {
                    "date": "2026-01-05",
                    "positions_after": {
                        "A": 0.15,
                        "B": 0.15,
                        "C": 0.15,
                        "D": 0.15,
                        "E": 0.15,
                        "F": 0.15,
                        "G": 0.10,
                    },
                }
            ],
            trades=[
                {"date": "2026-03-01", "ticker": "A", "action": "sell", "holding_days": 41},
            ],
            config={"constraint_config": {"max_single_name_weight": 0.20}},
        )

        self.assertTrue(metrics["compliance_pass"])
        self.assertEqual(metrics["max_trade_holding_days"], 41)
        self.assertIn("max_trade_holding_days", metrics["heuristic_violations"])

    def test_constraint_config_caps_weights_and_reports_weekly_failures(self):
        constraints = BacktestService._merge_constraint_config(
            {"max_single_name_weight": 0.20, "weekly_turnover_floor": 0.30},
            {"max_single_name_weight": 0.15},
        )

        weights, actions = BacktestService._apply_weight_constraints(
            {"AAPL": 0.40, "MSFT": 0.25, "NVDA": 0.10},
            constraints,
        )
        report = BacktestService._build_constraint_report(
            constraint_config=constraints,
            rebalance_diagnostics=[
                {"date": "2026-01-09", "positions_after": weights, "turnover": 0.0},
                {"date": "2026-01-16", "positions_after": weights, "turnover": 0.20},
                {"date": "2026-01-23", "positions_after": weights, "turnover": 0.42},
            ],
            trades=[],
            startup_state_report=None,
        )

        self.assertLessEqual(max(weights.values()), 0.15)
        self.assertEqual(actions["clipped"], {"AAPL": {"raw": 0.4, "clipped": 0.15}, "MSFT": {"raw": 0.25, "clipped": 0.15}})
        self.assertFalse(report["constraint_pass"])
        self.assertIn("weekly_turnover_floor", report["failed_constraints"])
        self.assertEqual(report["weekly_turnover"]["weeks"][1]["pass"], False)

    def test_top_level_backtest_constraint_config_is_preserved(self):
        constraints = BacktestService._resolve_run_constraint_config(
            strategy_config=None,
            config_dict={"max_single_name_weight": 0.20},
        )

        weights, actions = BacktestService._apply_weight_constraints(
            {"AAPL": 0.40, "MSFT": 0.10},
            constraints,
        )
        report = BacktestService._build_constraint_report(
            constraint_config=constraints,
            rebalance_diagnostics=[
                {
                    "date": "2026-01-09",
                    "positions_after": weights,
                    "turnover": 1.0,
                    "constraint_actions": actions,
                },
            ],
            trades=[],
            startup_state_report=None,
        )

        self.assertEqual(constraints["max_single_name_weight"], 0.20)
        self.assertEqual(weights["AAPL"], 0.20)
        self.assertEqual(report["max_single_name_weight"]["limit"], 0.20)
        self.assertEqual(report["max_single_name_weight"]["clipped_events"], 1)

    def test_apply_position_sizing_rejects_unsupported_mode(self):
        raw_signals = pd.DataFrame(
            {"signal": [1], "weight": [0.5], "strength": [1.0]},
            index=["AAPL"],
        )

        with self.assertRaisesRegex(ValueError, "Unsupported position_sizing"):
            BacktestService._apply_position_sizing(
                raw_signals,
                "custom",
                max_positions=10,
            )

    def test_filtered_full_debug_replay_reports_skipped_counts(self):
        debug_state = BacktestService._init_debug_replay_state(
            {
                "debug_mode": True,
                "debug_level": "full",
                "debug_tickers": ["AAPL"],
                "debug_dates": ["2026-01-09"],
            },
            market="US",
        )

        BacktestService._record_debug_rebalance(
            debug_state,
            date_key="2026-01-08",
            model_predictions={},
            factor_data={},
            raw_signals=pd.DataFrame(),
            target_weights={},
            adjusted_weights={},
            context_diagnostics=None,
            positions_before={},
            positions_after={},
        )
        BacktestService._record_debug_rebalance(
            debug_state,
            date_key="2026-01-09",
            model_predictions={},
            factor_data={},
            raw_signals=pd.DataFrame(
                {"signal": [1, 1], "weight": [0.5, 0.5], "strength": [1.0, 0.9]},
                index=["AAPL", "MSFT"],
            ),
            target_weights={"AAPL": 0.5, "MSFT": 0.5},
            adjusted_weights={"AAPL": 0.5, "MSFT": 0.5},
            context_diagnostics=None,
            positions_before={"MSFT": 0.5},
            positions_after={"AAPL": 0.5, "MSFT": 0.5},
        )

        self.assertEqual(debug_state["captured_items"], 1)
        self.assertEqual(debug_state["skipped_items"], 1)
        self.assertEqual(debug_state["skipped_by_date"], 1)
        self.assertEqual(len(debug_state["rebalance"]), 1)

    def test_constraint_report_fails_raw_target_budget_before_single_name_cap(self):
        weights, actions = BacktestService._apply_weight_constraints(
            {
                "A": 1.0,
                "B": 1.0,
                "C": 1.0,
                "D": 1.0,
                "E": 1.0,
                "F": 1.0,
                "G": 1.0,
            },
            {"max_single_name_weight": 0.20},
        )
        report = BacktestService._build_constraint_report(
            constraint_config={"max_single_name_weight": 0.20},
            rebalance_diagnostics=[
                {
                    "date": "2026-01-09",
                    "positions_after": weights,
                    "turnover": 1.0,
                    "constraint_actions": actions,
                },
            ],
            trades=[],
            startup_state_report=None,
        )

        self.assertFalse(report["constraint_pass"])
        self.assertIn("target_weight_budget", report["failed_constraints"])
        self.assertEqual(report["target_weight_budget"]["max_raw_target_sum"], 7.0)
        self.assertEqual(report["target_weight_budget"]["max_constrained_target_sum"], 1.4)

    def test_evaluation_slice_rebases_nav_and_excludes_warmup_trades(self):
        full = BacktestResult(
            config={"initial_capital": 1000.0, "start_date": "2025-12-15", "end_date": "2026-01-09"},
            dates=["2025-12-15", "2025-12-16", "2026-01-05", "2026-01-06", "2026-01-09"],
            nav=[1000.0, 1020.0, 1100.0, 1210.0, 1155.0],
            benchmark_nav=[1000.0, 1010.0, 1040.0, 1050.0, 1060.0],
            drawdown=[0.0, 0.0, 0.0, 0.0, -0.045455],
            total_return=0.155,
            annual_return=0.0,
            annual_volatility=0.0,
            max_drawdown=-0.045455,
            sharpe_ratio=0.0,
            calmar_ratio=0.0,
            sortino_ratio=0.0,
            win_rate=0.0,
            profit_loss_ratio=0.0,
            total_trades=2,
            annual_turnover=9.9,
            total_cost=4.0,
            monthly_returns=[],
            trades=[
                {"date": "2025-12-16", "ticker": "AAPL", "action": "buy", "shares": 10, "price": 10, "cost": 1.0},
                {"date": "2026-01-06", "ticker": "AAPL", "action": "sell", "shares": 5, "price": 12, "cost": 2.0},
            ],
            trade_diagnostics={},
        )

        sliced = BacktestService._slice_result_to_evaluation(
            full,
            evaluation_start_date="2026-01-05",
            evaluation_end_date="2026-01-09",
            initial_capital=1000.0,
        )

        self.assertEqual(sliced.dates, ["2026-01-05", "2026-01-06", "2026-01-09"])
        self.assertEqual(sliced.nav[0], 1000.0)
        self.assertEqual(sliced.total_trades, 1)
        self.assertEqual(sliced.trades[0]["date"], "2026-01-06")
        self.assertEqual(sliced.total_cost, 2.0)
        self.assertEqual(sliced.config["evaluation_start_date"], "2026-01-05")
        self.assertTrue(sliced.trade_diagnostics["evaluation_slice"]["warmup_trades_excluded"])

    def test_startup_state_report_flags_missing_warmup_state_for_evaluation(self):
        report = BacktestService._build_startup_state_report(
            rebalance_diagnostics=[
                {"date": "2025-12-19", "phase": "warmup", "positions_after": {"AAPL": 0.15}},
                {"date": "2026-01-09", "phase": "evaluation", "positions_before": {}, "positions_after": {}, "wait_for_anchor": True},
            ],
            warmup_start_date="2025-12-15",
            evaluation_start_date="2026-01-05",
            initial_entry_policy="require_warmup_state",
        )

        self.assertEqual(report["first_evaluation_rebalance_date"], "2026-01-09")
        self.assertEqual(report["evaluation_start_position_count"], 1)
        self.assertEqual(report["first_evaluation_positions_before_count"], 0)
        self.assertTrue(report["startup_silence_violation"])
        self.assertEqual(report["anchor_blocked_count"], 1)

    def test_list_summary_strips_heavy_diagnostics(self):
        summary = {
            "total_return": 0.12,
            "sharpe_ratio": 2.3,
            "rebalance_diagnostics": [{"date": "2026-04-10"}],
            "leakage_warnings": [{"model_id": "m1"}],
            "trade_diagnostics": {"by_reason": {}},
        }

        lightweight = BacktestService._list_summary(summary)

        self.assertEqual(lightweight["total_return"], 0.12)
        self.assertEqual(lightweight["sharpe_ratio"], 2.3)
        self.assertNotIn("rebalance_diagnostics", lightweight)
        self.assertNotIn("leakage_warnings", lightweight)
        self.assertIn("has_rebalance_diagnostics", lightweight)
        self.assertTrue(lightweight["has_rebalance_diagnostics"])

    def test_get_backtest_promotes_rebalance_diagnostics_to_top_level(self):
        svc = BacktestService()
        conn = _BacktestDetailConnection(
            summary={
                "total_return": 0.12,
                "rebalance_diagnostics": [
                    {
                        "date": "2026-04-10",
                        "lane_counts": {"core": 10},
                        "market_state": "risk_on",
                    }
                ],
            }
        )

        with patch("backend.services.backtest_service.get_connection", return_value=conn):
            detail = svc.get_backtest("bt_diag", market="CN")

        self.assertEqual(detail["rebalance_diagnostics"][0]["date"], "2026-04-10")
        self.assertEqual(detail["rebalance_diagnostics"][0]["lane_counts"], {"core": 10})

    def test_get_rebalance_diagnostics_returns_paginated_payload(self):
        svc = BacktestService()
        conn = _BacktestDiagnosticsConnection(
            diagnostics=[
                {"date": "2026-04-10", "lane_counts": {"core": 10}},
                {"date": "2026-04-13", "lane_counts": {"core": 8}},
                {"date": "2026-04-14", "lane_counts": {"core": 6}},
            ]
        )

        with patch("backend.services.backtest_service.get_connection", return_value=conn):
            payload = svc.get_rebalance_diagnostics("bt_diag", market="CN", offset=1, limit=1)

        self.assertEqual(payload["backtest_id"], "bt_diag")
        self.assertEqual(payload["market"], "CN")
        self.assertEqual(payload["total"], 3)
        self.assertEqual(payload["offset"], 1)
        self.assertEqual(payload["limit"], 1)
        self.assertEqual(payload["items"], [{"date": "2026-04-13", "lane_counts": {"core": 8}}])

    def test_batch_predict_reuses_feature_matrix_for_models_sharing_feature_set(self):
        svc = BacktestService()
        svc._model_service = _SharedFeatureModelService()

        result = svc._batch_predict_all_dates(
            ["model_a", "model_b"],
            tickers=["sh.600000", "sh.600001"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            rebalance_days=["2024-01-03"],
            market="CN",
        )

        self.assertEqual(svc._model_service.feature_compute_calls, 1)
        self.assertEqual(svc._model_service.load_model_calls, ["model_a", "model_b"])
        self.assertIn("model_a", result["2024-01-03"])
        self.assertIn("model_b", result["2024-01-03"])

    def test_batch_predict_aligns_features_to_frozen_model_schema(self):
        svc = BacktestService()
        svc._model_service = _FrozenFeatureModelService(
            feature_data={
                "close": pd.DataFrame(
                    {"AAA": [10.0, 11.0], "BBB": [20.0, 21.0]},
                    index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
                ),
                "extra_current_factor": pd.DataFrame(
                    {"AAA": [1.0, 1.0], "BBB": [1.0, 1.0]},
                    index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
                ),
            },
            frozen=["close"],
        )

        result = svc._batch_predict_all_dates(
            ["model_frozen"],
            tickers=["AAA", "BBB"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            rebalance_days=["2024-01-03"],
            market="US",
        )

        self.assertEqual(svc._model_service.loaded_model.seen_columns, [["close"]])
        self.assertIn("model_frozen", result["2024-01-03"])

    def test_batch_predict_missing_frozen_feature_raises_clear_error(self):
        svc = BacktestService()
        svc._model_service = _FrozenFeatureModelService(
            feature_data={
                "extra_current_factor": pd.DataFrame(
                    {"AAA": [1.0, 1.0], "BBB": [1.0, 1.0]},
                    index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
                ),
            },
            frozen=["close"],
        )

        with self.assertRaisesRegex(ValueError, "missing frozen feature"):
            svc._batch_predict_all_dates(
                ["model_frozen"],
                tickers=["AAA", "BBB"],
                start_date="2024-01-02",
                end_date="2024-01-03",
                rebalance_days=["2024-01-03"],
                market="US",
            )

    def test_batch_predict_serializes_shared_feature_matrix_cache_builds(self):
        fake_model_service = _ConcurrentFeatureModelService()
        svc_a = BacktestService()
        svc_b = BacktestService()
        svc_a._model_service = fake_model_service
        svc_b._model_service = fake_model_service

        first = ThreadPoolExecutor(max_workers=1)
        second = ThreadPoolExecutor(max_workers=1)
        self.addCleanup(first.shutdown, wait=True)
        self.addCleanup(second.shutdown, wait=True)

        future_a = first.submit(
            svc_a._batch_predict_all_dates,
            ["model_a"],
            ["sh.600000", "sh.600001"],
            "2024-01-02",
            "2024-01-03",
            ["2024-01-03"],
            "CN",
        )
        self.assertTrue(fake_model_service.first_compute_inside.wait(timeout=1))
        future_b = second.submit(
            svc_b._batch_predict_all_dates,
            ["model_a"],
            ["sh.600000", "sh.600001"],
            "2024-01-02",
            "2024-01-03",
            ["2024-01-03"],
            "CN",
        )
        fake_model_service.release_first_compute.set()

        result_a = future_a.result(timeout=2)
        result_b = future_b.result(timeout=2)

        self.assertIn("model_a", result_a["2024-01-03"])
        self.assertIn("model_a", result_b["2024-01-03"])
        self.assertEqual(fake_model_service.max_active_compute_calls, 1)

    def test_save_result_persists_reproducibility_fingerprint(self):
        svc = BacktestService()
        result = BacktestResult(
            config={"market": "CN"},
            dates=["2024-01-02", "2024-01-03"],
            nav=[1000.0, 1010.0],
            benchmark_nav=[1000.0, 1005.0],
            drawdown=[0.0, 0.0],
            total_return=0.01,
            annual_return=0.1,
            annual_volatility=0.2,
            max_drawdown=0.0,
            sharpe_ratio=1.2,
            calmar_ratio=0.0,
            sortino_ratio=1.3,
            win_rate=1.0,
            profit_loss_ratio=1.0,
            total_trades=1,
            annual_turnover=2.0,
            total_cost=1.0,
            monthly_returns=[],
            trades=[],
            trade_diagnostics={},
        )
        conn = _BacktestSaveConnection()

        with (
            patch("backend.services.backtest_service.get_connection", return_value=conn),
            patch.object(
                svc,
                "_build_reproducibility_fingerprint",
                return_value={
                    "hash": "fp_hash",
                    "strategy": {"source_hash": "source_hash"},
                },
            ),
        ):
            svc._save_result(
                bt_id="bt1",
                market="CN",
                strategy_id="strategy_cn",
                config={"market": "CN", "universe_group_id": "cn_group"},
                result=result,
                result_level="exploratory",
            )

        summary = conn.insert_params[4]
        self.assertEqual(summary["reproducibility_fingerprint"]["hash"], "fp_hash")
        self.assertEqual(
            BacktestService._list_summary(summary)["reproducibility_hash"],
            "fp_hash",
        )

    def test_reproducibility_fingerprint_marks_dirty_runtime_patch_hash(self):
        svc = BacktestService.__new__(BacktestService)
        svc._strategy_service = unittest.mock.Mock()
        svc._model_service = unittest.mock.Mock()
        svc._strategy_service.get_strategy.return_value = {
            "id": "strategy_dirty",
            "market": "US",
            "name": "Dirty Strategy",
            "version": 1,
            "position_sizing": "equal_weight",
            "source_code": "class S: pass",
            "required_factors": [],
            "required_models": [],
        }
        result = BacktestResult(
            config={},
            dates=["2026-01-02"],
            nav=[1_000_000.0],
            benchmark_nav=[1_000_000.0],
            drawdown=[0.0],
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

        class _Conn:
            def execute(self, sql, params=None):
                return self

            def fetchone(self):
                return (None, None, 0, 0)

        with (
            patch("backend.services.backtest_service.get_connection", return_value=_Conn()),
            patch("backend.services.backtest_service._git_commit_hash", return_value="abc123"),
            patch(
                "backend.services.backtest_service._git_runtime_state",
                return_value={
                    "dirty": True,
                    "dirty_paths": ["backend/services/backtest_service.py"],
                    "patch_hash": "patch123",
                },
            ),
        ):
            fingerprint = svc._build_reproducibility_fingerprint(
                strategy_id="strategy_dirty",
                market="US",
                config={
                    "start_date": "2026-01-02",
                    "end_date": "2026-01-02",
                    "benchmark": "SPY",
                },
                result=result,
            )

        self.assertTrue(fingerprint["runtime"]["dirty"])
        self.assertEqual(fingerprint["runtime"]["patch_hash"], "patch123")
        self.assertFalse(fingerprint["comparability"]["clean_runtime"])
        self.assertIn("dirty_worktree", fingerprint["comparability"]["warnings"])

    def test_compact_research_summary_bounds_rebalance_payload_and_reports_deltas(self):
        svc = BacktestService.__new__(BacktestService)
        svc.get_backtest = lambda bt_id, market=None: {
            "id": bt_id,
            "market": "US",
            "strategy_id": f"strategy_{bt_id}",
            "summary": {
                "total_return": 0.10 if bt_id == "baseline" else 0.14,
                "sharpe_ratio": 1.0 if bt_id == "baseline" else 1.3,
                "max_drawdown": -0.10 if bt_id == "baseline" else -0.09,
                "total_trades": 12 if bt_id == "baseline" else 14,
                "rebalance_diagnostics": [
                    {
                        "date": "2026-01-05",
                        "added": ["AAA"] if bt_id == "trial" else ["BBB"],
                        "removed": ["CCC"] if bt_id == "trial" else [],
                        "positions_after": {"AAA": 0.6, "BBB": 0.4},
                    },
                    {
                        "date": "2026-01-06",
                        "added": ["DDD"],
                        "removed": [],
                        "positions_after": {"DDD": 1.0},
                    },
                    {
                        "date": "2026-01-07",
                        "added": ["EEE"],
                        "removed": [],
                        "positions_after": {"EEE": 1.0},
                    },
                ],
            },
            "trades": [
                {"ticker": "AAA", "action": "buy", "shares": 10, "price": 10.0},
            ],
        }

        summary = svc.get_research_summary(
            baseline_backtest_id="baseline",
            trial_backtest_id="trial",
            market="US",
            changed_variable={"entry_guard": "relaxed"},
            conclusion="promote",
            reason="Sharpe improved with smaller drawdown",
            max_rebalance_items=2,
        )

        self.assertEqual(summary["baseline_id"], "baseline")
        self.assertEqual(summary["trial_id"], "trial")
        self.assertEqual(summary["changed_variable"], {"entry_guard": "relaxed"})
        self.assertEqual(summary["metric_delta"]["total_return"], 0.04)
        self.assertEqual(summary["metric_delta"]["sharpe_ratio"], 0.3)
        self.assertEqual(summary["rebalance_digest"]["shown"], 2)
        self.assertEqual(summary["rebalance_digest"]["total"], 3)
        self.assertEqual(summary["decision"]["conclusion"], "promote")

    def test_combine_portfolio_legs_builds_weighted_nav_and_leg_summary(self):
        svc = BacktestService()
        base = BacktestResult(
            config={"market": "CN"},
            dates=["2024-01-02", "2024-01-03"],
            nav=[1000.0, 1100.0],
            benchmark_nav=[1000.0, 1000.0],
            drawdown=[0.0, 0.0],
            total_return=0.1,
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
        overlay = BacktestResult(
            config={"market": "CN"},
            dates=["2024-01-02", "2024-01-03"],
            nav=[1000.0, 900.0],
            benchmark_nav=[1000.0, 1000.0],
            drawdown=[0.0, -0.1],
            total_return=-0.1,
            annual_return=0.0,
            annual_volatility=0.0,
            max_drawdown=-0.1,
            sharpe_ratio=0.0,
            calmar_ratio=0.0,
            sortino_ratio=0.0,
            win_rate=0.0,
            profit_loss_ratio=0.0,
            total_trades=2,
            annual_turnover=1.0,
            total_cost=2.0,
            monthly_returns=[],
            trades=[{"date": "2024-01-03", "ticker": "sh.600000"}],
            trade_diagnostics={"by_reason": {}},
        )

        combined = svc._combine_portfolio_legs(
            base_result=base,
            overlay_result=overlay,
            base_weight=0.65,
            overlay_weight=0.35,
            portfolio_config={"mode": "base_plus_overlay"},
        )

        self.assertEqual(combined.nav, [1000.0, 1030.0])
        self.assertEqual(combined.total_return, 0.03)
        self.assertEqual(combined.total_trades, 2)
        self.assertEqual(combined.trade_diagnostics["portfolio_legs"]["base"]["weight"], 0.65)
        self.assertEqual(
            combined.trade_diagnostics["portfolio_legs"]["overlay"]["contribution_return"],
            -0.035,
        )


class _BacktestSaveConnection:
    def __init__(self):
        self.insert_params = None

    def execute(self, sql, params=None):
        if str(sql).lstrip().upper().startswith("INSERT INTO BACKTEST_RESULTS"):
            import json

            parsed = list(params)
            parsed[3] = json.loads(parsed[3])
            parsed[4] = json.loads(parsed[4])
            self.insert_params = parsed
        return self


class _BacktestDetailConnection:
    def __init__(self, summary):
        self.summary = summary

    def execute(self, sql, params=None):
        import json

        self.row = (
            "bt_diag",
            "CN",
            "strategy_cn",
            json.dumps({"start_date": "2026-04-01", "end_date": "2026-04-30"}),
            json.dumps(self.summary),
            json.dumps({"2026-04-10": 1.0}),
            json.dumps({}),
            json.dumps({}),
            json.dumps([]),
            0,
            "exploratory",
            "2026-05-02 12:00:00",
            json.dumps([]),
        )
        return self

    def fetchone(self):
        return self.row


class _BacktestDiagnosticsConnection:
    def __init__(self, diagnostics):
        self.diagnostics = diagnostics

    def execute(self, sql, params=None):
        import json

        self.row = ("CN", json.dumps({"rebalance_diagnostics": self.diagnostics}))
        return self

    def fetchone(self):
        return self.row


class _SharedFeatureModelService:
    def __init__(self):
        self.feature_compute_calls = 0
        self.load_model_calls = []
        self._feature_service = self

    def get_model(self, model_id, market=None):
        return {"id": model_id, "feature_set_id": "shared_fs"}

    def load_model(self, model_id, market=None):
        self.load_model_calls.append(model_id)
        return _LinearPredictModel(model_id)

    def compute_features_from_cache(self, fs_id, tickers, start_date, end_date, market=None):
        self.feature_compute_calls += 1
        index = pd.to_datetime(["2024-01-02", "2024-01-03"])
        return {
            "close": pd.DataFrame(
                {
                    "sh.600000": [10.0, 11.0],
                    "sh.600001": [20.0, 21.0],
                },
                index=index,
            )
        }

    def _break_prediction_ties(self, preds):
        return preds


class _ConcurrentFeatureModelService(_SharedFeatureModelService):
    def __init__(self):
        super().__init__()
        self.first_compute_inside = Event()
        self.release_first_compute = Event()
        self._active_lock = Lock()
        self._active_compute_calls = 0
        self.max_active_compute_calls = 0

    def compute_features_from_cache(self, fs_id, tickers, start_date, end_date, market=None):
        with self._active_lock:
            self._active_compute_calls += 1
            self.max_active_compute_calls = max(
                self.max_active_compute_calls,
                self._active_compute_calls,
            )
            active_now = self._active_compute_calls
        try:
            if active_now == 1:
                self.first_compute_inside.set()
                self.release_first_compute.wait(timeout=1)
            return super().compute_features_from_cache(
                fs_id,
                tickers,
                start_date,
                end_date,
                market=market,
            )
        finally:
            with self._active_lock:
                self._active_compute_calls -= 1


class _LinearPredictModel:
    def __init__(self, model_id):
        self.model_id = model_id

    def predict(self, X):
        base = 1.0 if self.model_id == "model_a" else 2.0
        return pd.Series(base + X["close"].astype(float), index=X.index)


class _FrozenFeatureModelService(_SharedFeatureModelService):
    def __init__(self, feature_data, frozen):
        super().__init__()
        self.feature_data = feature_data
        self.frozen = frozen
        self.loaded_model = _SchemaCheckingPredictModel()

    def get_model(self, model_id, market=None):
        return {"id": model_id, "feature_set_id": "shared_fs"}

    def load_model(self, model_id, market=None):
        self.load_model_calls.append(model_id)
        return self.loaded_model

    def compute_features_from_cache(self, fs_id, tickers, start_date, end_date, market=None):
        self.feature_compute_calls += 1
        return self.feature_data

    def _load_frozen_features(self, model_id):
        return self.frozen

    def _align_features_to_frozen(self, X, frozen, model_id):
        missing = [name for name in frozen if name not in X.columns]
        if missing:
            raise ValueError(f"missing frozen feature(s) for {model_id}: {missing}")
        return X[frozen]


class _SchemaCheckingPredictModel:
    def __init__(self):
        self.seen_columns = []

    def predict(self, X):
        self.seen_columns.append(list(X.columns))
        return pd.Series(X.iloc[:, 0].astype(float), index=X.index)


if __name__ == "__main__":
    unittest.main()
