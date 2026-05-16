import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
from backend.services.strategy_graph_3_service import StrategyGraph3Service


class StrategyGraph3ServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "strategy_graph3.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        self.portfolio_service = PortfolioAssets3Service()
        self.portfolio = self.portfolio_service.create_portfolio_construction_spec(
            name="M8 graph equal weight",
            method="equal_weight",
            params={"top_n": 5},
        )
        self.risk = self.portfolio_service.create_risk_control_spec(
            name="M8 graph risk",
            rules=[
                {"rule": "max_positions", "max_positions": 3},
                {"rule": "max_single_weight", "max_weight": 0.40},
            ],
        )
        self.execution = self.portfolio_service.create_execution_policy_spec(
            name="M8 graph next open",
            policy_type="next_open",
            params={"price_field": "open"},
        )

    def test_simulate_day_explains_alpha_selection_portfolio_and_orders(self):
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 smoke graph",
            selection_policy={"top_n": 4, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=self.risk["id"],
            execution_policy_spec_id=self.execution["id"],
        )
        result = service.simulate_day(
            graph["id"],
            decision_date="2024-01-02",
            alpha_frame=[
                {"asset_id": "US_EQ:AAA", "score": 0.90, "confidence": 0.9},
                {"asset_id": "US_EQ:BBB", "score": 0.80, "confidence": 0.8},
                {"asset_id": "US_EQ:CCC", "score": 0.70, "confidence": 0.7},
                {"asset_id": "US_EQ:DDD", "score": 0.60, "confidence": 0.6},
                {"asset_id": "US_EQ:EEE", "score": 0.50, "confidence": 0.5},
            ],
            current_weights={"US_EQ:AAA": 0.10},
        )

        explain = service.explain_day(result["strategy_signal"]["id"])
        self.assertEqual(graph["graph_type"], "builtin_alpha_graph")
        self.assertEqual(result["strategy_signal"]["status"], "completed")
        self.assertEqual(result["stage_artifact"]["artifact_type"], "strategy_graph_explain")
        self.assertEqual(len(result["alpha_frame"]), 5)
        self.assertEqual(len(result["selection_frame"]), 5)
        self.assertLessEqual(result["profile"]["active_positions"], 3)
        self.assertTrue(result["constraint_trace"])
        self.assertTrue(result["order_intents"])
        self.assertIn("alpha", explain["stages"])
        self.assertIn("selection", explain["stages"])
        self.assertIn("portfolio", explain["stages"])
        self.assertIn("execution", explain["stages"])

        counts = get_connection().execute(
            """SELECT
                    (SELECT COUNT(*) FROM strategy_graphs),
                    (SELECT COUNT(*) FROM strategy_nodes),
                    (SELECT COUNT(*) FROM strategy_signals),
                    (SELECT COUNT(*) FROM portfolio_runs)
            """
        ).fetchone()
        self.assertEqual(counts, (1, 6, 1, 1))

    def test_legacy_strategy_adapter_graph_is_not_a_v3_2_runtime_path(self):
        service = StrategyGraph3Service()
        with self.assertRaisesRegex(ValueError, "Legacy strategy adapters are disabled"):
            service.create_legacy_strategy_adapter_graph(
                name="M8 legacy adapter graph",
                legacy_strategy_id="legacy-strategy-id",
                portfolio_construction_spec_id=self.portfolio["id"],
                risk_control_spec_id=self.risk["id"],
                execution_policy_spec_id=self.execution["id"],
            )

    def test_builtin_alpha_graph_rejects_legacy_signal_frames(self):
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 no legacy signal graph",
            selection_policy={"top_n": 4, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=self.risk["id"],
            execution_policy_spec_id=self.execution["id"],
        )

        with self.assertRaisesRegex(ValueError, "legacy_signal_frame is disabled"):
            service.simulate_day(
                graph["id"],
                decision_date="2024-01-02",
                alpha_frame=[
                    {"asset_id": "US_EQ:AAA", "score": 0.90, "confidence": 0.9},
                ],
                legacy_signal_frame=[
                    {"ticker": "BBB", "signal": 1, "weight": 0.3, "strength": 3.0},
                ],
            )

        with self.assertRaisesRegex(ValueError, "legacy_signal_frames_by_date is disabled"):
            service.backtest_graph(
                graph["id"],
                start_date="2024-01-02",
                end_date="2024-01-03",
                alpha_frames_by_date={
                    "2024-01-02": [{"asset_id": "US_EQ:AAA", "score": 1.0}],
                },
                legacy_signal_frames_by_date={
                    "2024-01-02": [{"ticker": "BBB", "signal": 1}],
                },
            )

    def test_backtest_graph_revalues_nav_and_persists_daily_records(self):
        conn = get_connection()
        conn.executemany(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('US', ?, ?, 'NYSE', 'Test', 'active', current_timestamp)""",
            [("AAA", "AAA Inc"), ("BBB", "BBB Inc")],
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('US', ?, ?, ?, ?, ?, ?, 1000, 1.0)""",
            [
                ("AAA", "2024-01-02", 100.0, 101.0, 99.0, 100.0),
                ("AAA", "2024-01-03", 110.0, 111.0, 109.0, 110.0),
                ("BBB", "2024-01-02", 50.0, 51.0, 49.0, 50.0),
                ("BBB", "2024-01-03", 50.0, 51.0, 49.0, 50.0),
            ],
        )
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 backtest graph",
            selection_policy={"top_n": 1, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=self.risk["id"],
            execution_policy_spec_id=self.execution["id"],
        )

        result = service.backtest_graph(
            graph["id"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            alpha_frames_by_date={
                "2024-01-02": [{"asset_id": "US_EQ:AAA", "score": 1.0}],
                "2024-01-03": [{"asset_id": "US_EQ:AAA", "score": 1.0}],
            },
            initial_capital=1_000_000,
        )

        self.assertEqual(result["backtest_run"]["status"], "completed")
        self.assertEqual(result["summary"]["days_processed"], 2)
        self.assertAlmostEqual(result["summary"]["final_nav"], 1_097_800.0, places=2)
        self.assertAlmostEqual(result["summary"]["total_cost"], 2_000.0, places=2)
        self.assertEqual(result["daily"][1]["diagnostics"]["valuation"]["status"], "valued")
        counts = conn.execute(
            """SELECT
                    (SELECT COUNT(*) FROM backtest_runs),
                    (SELECT COUNT(*) FROM backtest_daily),
                    (SELECT COUNT(*) FROM backtest_trades)
            """
        ).fetchone()
        self.assertEqual(counts[0], 1)
        self.assertEqual(counts[1], 2)
        self.assertGreaterEqual(counts[2], 1)

    def test_backtest_graph_persists_fills_costs_and_unfilled_diagnostics(self):
        conn = get_connection()
        conn.executemany(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('CN', ?, ?, 'SSE', 'Test', 'active', current_timestamp)""",
            [("sh.600000", "浦发银行"), ("sh.600001", "Blocked")],
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('CN', ?, ?, ?, ?, ?, ?, ?, 1.0)""",
            [
                ("sh.600000", "2024-01-02", 10.0, 10.2, 9.8, 10.0, 100000),
                ("sh.600000", "2024-01-03", 10.0, 10.2, 9.8, 10.0, 100000),
                ("sh.600001", "2024-01-02", 20.0, 20.2, 19.8, 20.0, 100000),
                ("sh.600001", "2024-01-03", 20.0, 20.2, 19.8, 20.0, 100000),
            ],
        )
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 CN fill graph",
            market_profile_id="CN_A",
            selection_policy={"top_n": 2, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=None,
            execution_policy_spec_id=self.execution["id"],
        )
        conn.execute(
            """INSERT INTO trade_status
               (market_profile_id, asset_id, date, is_trading, is_suspended, is_st, limit_up, limit_down, metadata)
               VALUES ('CN_A', 'CN_A:sh.600001', DATE '2024-01-03', false, true, false, NULL, NULL, '{}')"""
        )

        result = service.backtest_graph(
            graph["id"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            alpha_frames_by_date={
                "2024-01-02": [
                    {"asset_id": "CN_A:sh.600000", "score": 1.0},
                    {"asset_id": "CN_A:sh.600001", "score": 0.9},
                ],
                "2024-01-03": [
                    {"asset_id": "CN_A:sh.600000", "score": 1.0},
                    {"asset_id": "CN_A:sh.600001", "score": 0.9},
                ],
            },
            initial_capital=1_000_000,
        )

        rows = conn.execute(
            """SELECT asset_id, side, quantity, price, value, cost, metadata
               FROM backtest_trades
               WHERE backtest_run_id = ?
               ORDER BY asset_id""",
            [result["backtest_run"]["id"]],
        ).fetchall()
        filled = [row for row in rows if row[2] is not None]
        blocked = [row for row in rows if row[2] is None]
        self.assertEqual(len(filled), 1)
        self.assertEqual(filled[0][0], "CN_A:sh.600000")
        self.assertGreaterEqual(filled[0][2] % 100, 0)
        self.assertEqual(filled[0][3], 10.0)
        self.assertGreater(filled[0][5], 0)
        self.assertEqual(len(blocked), 1)
        self.assertIn("suspended", blocked[0][6])
        self.assertEqual(result["summary"]["fill_diagnostics"]["blocked_order_count"], 1)
        self.assertTrue(
            any(
                (item["diagnostics"].get("skipped_execution") or {}).get("outside_backtest_window_count") == 1
                for item in result["daily"]
                if item["date"] == "2024-01-03"
            )
        )

    def test_backtest_graph_planned_price_fills_and_blocks_by_buffered_range(self):
        conn = get_connection()
        conn.executemany(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('US', ?, ?, 'NYSE', 'Test', 'active', current_timestamp)""",
            [("AAA", "AAA Inc"), ("BBB", "BBB Inc")],
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('US', ?, ?, ?, ?, ?, ?, 1000, 1.0)""",
            [
                ("AAA", "2024-01-02", 10.0, 10.2, 9.8, 10.0),
                ("AAA", "2024-01-03", 11.0, 12.2, 10.8, 12.0),
                ("BBB", "2024-01-02", 20.0, 20.2, 19.8, 20.0),
                ("BBB", "2024-01-03", 21.0, 22.0, 20.8, 22.0),
            ],
        )
        planned_execution = self.portfolio_service.create_execution_policy_spec(
            name="M8 planned price",
            policy_type="planned_price",
            params={"planned_price_buffer_bps": 50},
        )
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 planned graph",
            selection_policy={"top_n": 2, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=None,
            execution_policy_spec_id=planned_execution["id"],
        )

        result = service.backtest_graph(
            graph["id"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            alpha_frames_by_date={
                "2024-01-02": [
                    {"asset_id": "US_EQ:AAA", "score": 1.0, "planned_price": 11.5},
                    {"asset_id": "US_EQ:BBB", "score": 0.9, "planned_price": 20.81},
                ],
                "2024-01-03": [
                    {"asset_id": "US_EQ:AAA", "score": 1.0, "planned_price": 11.5},
                    {"asset_id": "US_EQ:BBB", "score": 0.9, "planned_price": 20.81},
                ],
            },
            initial_capital=1_000_000,
        )

        rows = conn.execute(
            """SELECT asset_id, quantity, price, metadata
               FROM backtest_trades
               WHERE backtest_run_id = ?
               ORDER BY asset_id""",
            [result["backtest_run"]["id"]],
        ).fetchall()
        filled = [row for row in rows if row[1] is not None]
        blocked = [row for row in rows if row[1] is None]
        self.assertEqual(len(filled), 1)
        self.assertEqual(filled[0][0], "US_EQ:AAA")
        self.assertEqual(filled[0][2], 11.5)
        self.assertGreaterEqual(len(blocked), 1)
        self.assertTrue(
            any("planned_price_outside_buffered_range" in row[3] for row in blocked)
        )
        self.assertEqual(result["summary"]["fill_diagnostics"]["execution_model"], "planned_price")

    def test_backtest_graph_planned_price_can_fallback_to_next_close(self):
        conn = get_connection()
        conn.execute(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('US', 'AAA', 'AAA Inc', 'NYSE', 'Test', 'active', current_timestamp)"""
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('US', 'AAA', ?, ?, ?, ?, ?, 1000, 1.0)""",
            [
                ("2024-01-02", 10.0, 10.2, 9.8, 10.0),
                ("2024-01-03", 11.0, 12.0, 10.8, 12.0),
            ],
        )
        planned_execution = self.portfolio_service.create_execution_policy_spec(
            name="M8 planned price close fallback",
            policy_type="planned_price",
            params={"planned_price_buffer_bps": 50, "fill_fallback": "next_close"},
        )
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 planned fallback graph",
            selection_policy={"top_n": 1, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=None,
            execution_policy_spec_id=planned_execution["id"],
        )

        result = service.backtest_graph(
            graph["id"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            alpha_frames_by_date={
                "2024-01-02": [
                    {"asset_id": "US_EQ:AAA", "score": 1.0, "planned_price": 10.81},
                ],
                "2024-01-03": [
                    {"asset_id": "US_EQ:AAA", "score": 1.0, "planned_price": 10.81},
                ],
            },
            initial_capital=1_000_000,
        )

        row = conn.execute(
            """SELECT quantity, price, metadata
               FROM backtest_trades
               WHERE backtest_run_id = ?""",
            [result["backtest_run"]["id"]],
        ).fetchone()
        self.assertIsNotNone(row[0])
        self.assertEqual(row[1], 12.0)
        self.assertIn('"fill_type": "fallback_close"', row[2])

    def test_backtest_graph_supports_mixed_order_intents_from_alpha_rows(self):
        conn = get_connection()
        conn.executemany(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('US', ?, ?, 'NYSE', 'Test', 'active', current_timestamp)""",
            [
                ("AAA", "Next close"),
                ("BBB", "Limit"),
                ("CCC", "Stop limit"),
            ],
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('US', ?, ?, ?, ?, ?, ?, 1000, 1.0)""",
            [
                ("AAA", "2024-01-02", 10.0, 10.3, 9.8, 10.0),
                ("AAA", "2024-01-03", 11.0, 11.5, 10.8, 11.2),
                ("BBB", "2024-01-02", 20.0, 20.5, 19.8, 20.0),
                ("BBB", "2024-01-03", 21.0, 21.5, 20.2, 20.8),
                ("CCC", "2024-01-02", 30.0, 30.5, 29.8, 30.0),
                ("CCC", "2024-01-03", 31.0, 33.0, 30.0, 32.0),
            ],
        )
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M5 mixed execution graph",
            selection_policy={"top_n": 3, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=None,
            execution_policy_spec_id=self.execution["id"],
        )

        result = service.backtest_graph(
            graph["id"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            alpha_frames_by_date={
                "2024-01-02": [
                    {"asset_id": "US_EQ:AAA", "score": 1.0, "execution_model": "next_close"},
                    {"asset_id": "US_EQ:BBB", "score": 0.9, "execution_model": "limit", "limit_price": 20.5},
                    {
                        "asset_id": "US_EQ:CCC",
                        "score": 0.8,
                        "execution_model": "stop_limit",
                        "stop_price": 32.0,
                        "limit_price": 29.0,
                    },
                ],
                "2024-01-03": [
                    {"asset_id": "US_EQ:AAA", "score": 1.0, "execution_model": "next_close"},
                    {"asset_id": "US_EQ:BBB", "score": 0.9, "execution_model": "limit", "limit_price": 20.5},
                    {
                        "asset_id": "US_EQ:CCC",
                        "score": 0.8,
                        "execution_model": "stop_limit",
                        "stop_price": 32.0,
                        "limit_price": 29.0,
                    },
                ],
            },
            initial_capital=1_000_000,
        )

        self.assertEqual(result["summary"]["fill_diagnostics"]["execution_model"], "mixed")
        self.assertGreater(result["summary"]["fill_diagnostics"]["path_assumption_warning_count"], 0)
        rows = conn.execute(
            """SELECT asset_id, price, metadata
               FROM backtest_trades
               WHERE backtest_run_id = ?
               ORDER BY asset_id""",
            [result["backtest_run"]["id"]],
        ).fetchall()
        metadata_text = "\n".join(row[2] for row in rows)
        self.assertIn('"execution_model": "next_close"', metadata_text)
        self.assertIn('"fill_type": "limit"', metadata_text)
        self.assertIn("stop_limit_not_reached", metadata_text)
        self.assertIn("daily_bar_no_intraday_path", metadata_text)

    def test_backtest_graph_blocks_cn_st_limit_and_missing_price_orders(self):
        conn = get_connection()
        conn.executemany(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('CN', ?, ?, 'SSE', 'Test', 'active', current_timestamp)""",
            [
                ("sh.600010", "Limit Up"),
                ("sh.600011", "ST Blocked"),
                ("sh.600012", "Missing Price"),
            ],
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('CN', ?, ?, ?, ?, ?, ?, ?, 1.0)""",
            [
                ("sh.600010", "2024-01-02", 10.0, 10.0, 9.8, 10.0, 100000),
                ("sh.600010", "2024-01-03", 11.0, 11.0, 10.8, 11.0, 100000),
                ("sh.600011", "2024-01-02", 20.0, 20.0, 19.8, 20.0, 100000),
                ("sh.600011", "2024-01-03", 20.0, 20.0, 19.8, 20.0, 100000),
            ],
        )
        conn.executemany(
            """INSERT INTO trade_status
               (market_profile_id, asset_id, date, is_trading, is_suspended, is_st, limit_up, limit_down, metadata)
               VALUES ('CN_A', ?, DATE '2024-01-03', true, false, ?, ?, NULL, '{}')""",
            [
                ("CN_A:sh.600010", False, 11.0),
                ("CN_A:sh.600011", True, None),
                ("CN_A:sh.600012", False, None),
            ],
        )
        service = StrategyGraph3Service()
        graph = service.create_builtin_alpha_graph(
            name="M8 CN blocked fills graph",
            market_profile_id="CN_A",
            selection_policy={"top_n": 3, "score_column": "score"},
            portfolio_construction_spec_id=self.portfolio["id"],
            risk_control_spec_id=None,
            execution_policy_spec_id=self.execution["id"],
        )

        result = service.backtest_graph(
            graph["id"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            alpha_frames_by_date={
                "2024-01-02": [
                    {"asset_id": "CN_A:sh.600010", "score": 1.0},
                    {"asset_id": "CN_A:sh.600011", "score": 0.9},
                    {"asset_id": "CN_A:sh.600012", "score": 0.8},
                ],
                "2024-01-03": [
                    {"asset_id": "CN_A:sh.600010", "score": 1.0},
                    {"asset_id": "CN_A:sh.600011", "score": 0.9},
                    {"asset_id": "CN_A:sh.600012", "score": 0.8},
                ],
            },
            initial_capital=1_000_000,
        )

        rows = conn.execute(
            """SELECT metadata
               FROM backtest_trades
               WHERE backtest_run_id = ?
               ORDER BY asset_id""",
            [result["backtest_run"]["id"]],
        ).fetchall()
        metadata_text = "\n".join(row[0] for row in rows)
        self.assertIn("limit_up_buy_blocked", metadata_text)
        self.assertIn("st_buy_blocked", metadata_text)
        self.assertIn("missing_execution_price", metadata_text)
        self.assertEqual(result["summary"]["fill_diagnostics"]["blocked_order_count"], 3)


if __name__ == "__main__":
    unittest.main()
