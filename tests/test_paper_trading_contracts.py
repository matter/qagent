import json
import unittest
from datetime import date, timedelta
from unittest.mock import Mock, patch

from backend.api.paper_trading import AdvanceRequest, advance_session
from backend.services.paper_trading_service import PaperTradingService


class PaperTradingApiContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_advance_session_returns_queued_async_contract(self):
        class FakeExecutor:
            def submit(self, **kwargs):
                self.kwargs = kwargs
                return "task-123"

        class FakeService:
            def advance(self, session_id, target_date=None, steps=0):
                return {}

        fake_executor = FakeExecutor()

        with (
            patch("backend.api.paper_trading._get_executor", return_value=fake_executor),
            patch("backend.api.paper_trading._get_svc", return_value=FakeService()),
        ):
            result = await advance_session(
                "session-1",
                AdvanceRequest(target_date="2026-04-24", steps=0),
            )

        self.assertEqual(result["task_id"], "task-123")
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["task_type"], "paper_trading_advance")
        self.assertTrue(result["async"])
        self.assertEqual(result["poll_url"], "/api/tasks/task-123")


class PaperTradingServiceContractTests(unittest.TestCase):
    def test_daily_series_includes_position_and_trade_counts(self):
        class FakeResult:
            def fetchall(self):
                return [
                    (
                        date(2026, 4, 10),
                        1100.0,
                        100.0,
                        json.dumps({"AAA": {"shares": 10}}),
                        json.dumps([{"ticker": "AAA", "action": "buy"}]),
                    )
                ]

        class FakeConnection:
            def execute(self, query, params=None):
                self.query = query
                self.params = params
                return FakeResult()

        svc = PaperTradingService.__new__(PaperTradingService)
        fake_conn = FakeConnection()

        with patch("backend.services.paper_trading_service.get_connection", return_value=fake_conn):
            rows = svc.get_daily_series("session-1")

        self.assertIn("FROM paper_trading_daily", fake_conn.query)
        self.assertEqual(fake_conn.params, ["session-1", "US"])
        self.assertEqual(rows[0]["position_count"], 1)
        self.assertEqual(rows[0]["trade_count"], 1)

    def test_get_positions_can_return_historical_snapshot_by_date(self):
        class FakeResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def execute(self, query, params=None):
                if "FROM paper_trading_daily" in query:
                    self.snapshot_params = params
                    return FakeResult(row=(
                        json.dumps({"AAA": {"shares": 10, "avg_price": 9.5}}),
                        date(2026, 4, 10),
                        1100.0,
                    ))
                if "FROM daily_bars" in query:
                    self.price_params = params
                    return FakeResult(rows=[("AAA", 11.0)])
                return FakeResult()

        svc = PaperTradingService.__new__(PaperTradingService)
        fake_conn = FakeConnection()

        with (
            patch("backend.services.paper_trading_service.get_connection", return_value=fake_conn),
            patch.object(svc, "get_session", return_value={"market": "US"}),
        ):
            positions = svc.get_positions("session-1", as_of_date="2026-04-10")

        self.assertEqual(fake_conn.snapshot_params, ["session-1", "US", "2026-04-10"])
        self.assertEqual(positions[0]["ticker"], "AAA")
        self.assertEqual(positions[0]["date"], "2026-04-10")
        self.assertEqual(positions[0]["latest_price"], 11.0)

    def test_compare_with_backtest_returns_daily_trade_deltas(self):
        class FakeResult:
            def __init__(self, rows=None):
                self.rows = rows or []

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def execute(self, query, params=None):
                if "FROM paper_trading_daily" in query:
                    return FakeResult(rows=[
                        (
                            date(2026, 4, 7),
                            1010.0,
                            100.0,
                            json.dumps({"AAA": {"shares": 10, "avg_price": 10.0}}),
                            json.dumps([{"ticker": "AAA", "action": "buy"}]),
                        ),
                        (
                            date(2026, 4, 8),
                            1020.0,
                            100.0,
                            json.dumps({"AAA": {"shares": 10, "avg_price": 10.0}}),
                            json.dumps([]),
                        ),
                    ])
                return FakeResult()

        svc = PaperTradingService.__new__(PaperTradingService)
        svc._backtest_service = Mock()
        svc._backtest_service.get_backtest.return_value = {
            "id": "bt-1",
            "summary": {
                "total_trades": 2,
                "rebalance_diagnostics": [
                    {
                        "date": "2026-04-07",
                        "positions_after": {"AAA": 1.0},
                        "turnover": 1.0,
                    }
                ],
            },
            "nav_series": {
                "dates": ["2026-04-07", "2026-04-08"],
                "values": [1015.0, 1030.0],
            },
            "trades": [
                {"date": "2026-04-07", "ticker": "AAA", "action": "buy"},
                {"date": "2026-04-08", "ticker": "BBB", "action": "buy"},
            ],
        }

        with patch("backend.services.paper_trading_service.get_connection", return_value=FakeConnection()):
            comparison = svc.compare_with_backtest("session-1", "bt-1")

        self.assertEqual(comparison["summary"]["paper_total_trades"], 1)
        self.assertEqual(comparison["summary"]["backtest_total_trades"], 2)
        self.assertEqual(comparison["summary"]["trade_delta"], -1)
        self.assertEqual(comparison["summary"]["paper_final_nav"], 1020.0)
        self.assertEqual(comparison["summary"]["backtest_final_nav"], 1030.0)
        self.assertEqual(comparison["summary"]["final_nav_delta"], -10.0)
        self.assertEqual(comparison["daily"][0]["backtest_nav"], 1015.0)
        self.assertEqual(comparison["daily"][1]["backtest_signal_date"], "2026-04-07")
        self.assertEqual(comparison["daily"][1]["backtest_target_positions"], ["AAA"])
        self.assertEqual(comparison["daily"][0]["missing_in_paper"], [])
        self.assertEqual(comparison["daily"][1]["missing_in_paper"], ["BBB:buy"])

    def test_new_session_target_before_start_is_not_reported_up_to_date(self):
        svc = PaperTradingService.__new__(PaperTradingService)
        session = {
            "id": "session-1",
            "current_date": None,
            "start_date": "2026-04-24",
        }

        with self.assertRaisesRegex(ValueError, "before session start_date"):
            svc._resolve_advance_trading_days(
                session=session,
                target_date="2026-04-23",
                steps=0,
            )

    def test_fresh_session_records_initial_day_without_pre_start_execution(self):
        class FakeResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def __init__(self):
                self.snapshots = []
                self.updated = None

            def execute(self, query, params=None):
                if "SELECT MAX(date) FROM daily_bars" in query:
                    return FakeResult(row=(date(2026, 4, 7),))
                if "SELECT date FROM paper_trading_daily" in query:
                    return FakeResult(rows=[])
                if "UPDATE paper_trading_sessions" in query:
                    self.updated = params
                    return FakeResult()
                return FakeResult()

            def executemany(self, query, rows):
                self.snapshots = list(rows)

        svc = PaperTradingService.__new__(PaperTradingService)
        svc._strategy_service = Mock()
        svc._strategy_service.get_strategy.return_value = {
            "source_code": "",
            "position_sizing": "equal_weight",
        }
        svc._group_service = Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]

        first_day = date(2026, 4, 6)
        second_day = date(2026, 4, 7)
        fake_conn = FakeConnection()
        session = {
            "id": "session-1",
            "status": "active",
            "current_date": None,
            "start_date": str(first_day),
            "strategy_id": "strategy-1",
            "universe_group_id": "group-1",
            "initial_capital": 1000.0,
            "config": {"initial_capital": 1000.0},
        }

        def execute_trades(positions, cash, target_weights, trade_date, cost_rate, price_cache, **kwargs):
            return {
                "trades": [{"ticker": "AAA", "action": "buy"}],
                "positions_after": {"AAA": {"shares": 100.0, "avg_price": 10.0}},
                "cash_after": 0.0,
            }

        with (
            patch("backend.services.paper_trading_service.get_connection", return_value=fake_conn),
            patch.object(svc, "get_session", return_value=session),
            patch.object(
                svc,
                "_resolve_advance_trading_days",
                return_value=[first_day, second_day],
            ),
            patch.object(
                svc,
                "_prev_trading_day",
                side_effect=lambda d, market=None: d - timedelta(days=1),
            ),
            patch.object(svc, "_prepare_signal_context", return_value={"ctx": True}) as prepare_ctx,
            patch.object(
                svc,
                "_preload_prices",
                return_value={
                    first_day: {"AAA": (10.0, 10.0)},
                    second_day: {"AAA": (10.0, 10.0)},
                },
            ),
            patch.object(svc, "_load_latest_state", return_value=({}, 1000.0)),
            patch.object(
                svc,
                "_build_portfolio_state_from_memory",
                return_value={"current_weights": {}},
            ),
            patch.object(
                svc,
                "_generate_signal_single_day",
                return_value=[{"ticker": "AAA", "signal": 1, "target_weight": 1.0, "strength": 1.0}],
            ) as generate_signal,
            patch.object(
                svc,
                "_apply_position_sizing_from_signals",
                return_value={"AAA": 1.0},
            ),
            patch.object(svc, "_execute_trades_cached", side_effect=execute_trades) as execute,
            patch.object(svc, "_value_portfolio_cached", return_value=1000.0),
        ):
            result = svc.advance("session-1", target_date=str(second_day), steps=0)

        self.assertEqual(result["days_processed"], 2)
        self.assertEqual(result["new_trades"], 1)
        self.assertEqual(result["baseline_days"], 1)
        self.assertIn("T+1", result["execution_rule"])
        prepare_ctx.assert_called_once()
        self.assertEqual(prepare_ctx.call_args.kwargs["signal_dates"], [first_day])
        generate_signal.assert_called_once()
        execute.assert_called_once()
        self.assertEqual(execute.call_args.args[3], second_day)
        self.assertEqual(fake_conn.snapshots[0][1], "US")
        self.assertEqual(fake_conn.snapshots[0][2], first_day)
        self.assertEqual(json.loads(fake_conn.snapshots[0][6]), [])

    def test_paper_advance_respects_weekly_rebalance_frequency(self):
        class FakeResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def __init__(self):
                self.snapshots = []

            def execute(self, query, params=None):
                if "SELECT MAX(date) FROM daily_bars" in query:
                    return FakeResult(row=(date(2026, 4, 13),))
                if "SELECT date FROM paper_trading_daily" in query:
                    return FakeResult(rows=[])
                return FakeResult()

            def executemany(self, query, rows):
                self.snapshots = list(rows)

        svc = PaperTradingService.__new__(PaperTradingService)
        svc._strategy_service = Mock()
        svc._strategy_service.get_strategy.return_value = {
            "source_code": "",
            "position_sizing": "equal_weight",
        }
        svc._group_service = Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]

        trading_days = [
            date(2026, 4, 6),
            date(2026, 4, 7),
            date(2026, 4, 8),
            date(2026, 4, 9),
            date(2026, 4, 10),
            date(2026, 4, 13),
        ]
        fake_conn = FakeConnection()
        session = {
            "id": "session-1",
            "status": "active",
            "current_date": None,
            "start_date": str(trading_days[0]),
            "strategy_id": "strategy-1",
            "universe_group_id": "group-1",
            "initial_capital": 1000.0,
            "config": {"initial_capital": 1000.0, "rebalance_freq": "weekly"},
        }

        def execute_trades(positions, cash, target_weights, trade_date, cost_rate, price_cache, **kwargs):
            return {
                "trades": [{"ticker": "AAA", "action": "buy"}],
                "positions_after": {"AAA": {"shares": 100.0, "avg_price": 10.0}},
                "cash_after": 0.0,
            }

        with (
            patch("backend.services.paper_trading_service.get_connection", return_value=fake_conn),
            patch.object(svc, "get_session", return_value=session),
            patch.object(
                svc,
                "_resolve_advance_trading_days",
                return_value=trading_days,
            ),
            patch.object(
                svc,
                "_prev_trading_day",
                side_effect=lambda d, market=None: trading_days[trading_days.index(d) - 1],
            ),
            patch.object(svc, "_prepare_signal_context", return_value={"ctx": True}) as prepare_ctx,
            patch.object(
                svc,
                "_preload_prices",
                return_value={d: {"AAA": (10.0, 10.0)} for d in trading_days},
            ),
            patch.object(svc, "_load_latest_state", return_value=({}, 1000.0)),
            patch.object(
                svc,
                "_build_portfolio_state_from_memory",
                return_value={"current_weights": {}},
            ),
            patch.object(
                svc,
                "_generate_signal_single_day",
                return_value=[{"ticker": "AAA", "signal": 1, "target_weight": 1.0, "strength": 1.0}],
            ) as generate_signal,
            patch.object(
                svc,
                "_apply_position_sizing_from_signals",
                return_value={"AAA": 1.0},
            ),
            patch.object(svc, "_execute_trades_cached", side_effect=execute_trades) as execute,
            patch.object(svc, "_value_portfolio_cached", return_value=1000.0),
        ):
            result = svc.advance("session-1", target_date=str(trading_days[-1]), steps=0)

        self.assertEqual(result["days_processed"], len(trading_days))
        self.assertEqual(result["new_trades"], 1)
        self.assertEqual(result["rebalance_freq"], "weekly")
        prepare_ctx.assert_called_once()
        self.assertEqual(prepare_ctx.call_args.kwargs["signal_dates"], [date(2026, 4, 10)])
        generate_signal.assert_called_once()
        execute.assert_called_once()
        self.assertEqual(execute.call_args.args[3], date(2026, 4, 13))
        self.assertEqual(len(fake_conn.snapshots), len(trading_days))

    def test_paper_raw_weight_position_sizing_preserves_cash_budget(self):
        weights = PaperTradingService._apply_position_sizing_from_signals(
            [
                {"ticker": "AAA", "signal": 1, "target_weight": 0.45, "strength": 0.9},
                {"ticker": "BBB", "signal": 1, "target_weight": 0.20, "strength": 0.8},
                {"ticker": "CCC", "signal": 1, "target_weight": 0.10, "strength": 0.7},
            ],
            "raw_weight",
            max_positions=2,
        )

        self.assertEqual(weights, {"AAA": 0.45, "BBB": 0.20})
        self.assertLess(sum(weights.values()), 1.0)

    def test_advance_uses_strategy_target_state_for_next_signal_context(self):
        class FakeResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def __init__(self):
                self.snapshots = []

            def execute(self, query, params=None):
                if "SELECT MAX(date) FROM daily_bars" in query:
                    return FakeResult(row=(date(2026, 4, 8),))
                if "SELECT date FROM paper_trading_daily" in query:
                    return FakeResult(rows=[])
                return FakeResult()

            def executemany(self, query, rows):
                self.snapshots = list(rows)

        svc = PaperTradingService.__new__(PaperTradingService)
        svc._strategy_service = Mock()
        svc._strategy_service.get_strategy.return_value = {
            "source_code": "",
            "position_sizing": "equal_weight",
        }
        svc._group_service = Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA", "BBB"]

        trading_days = [
            date(2026, 4, 6),
            date(2026, 4, 7),
            date(2026, 4, 8),
        ]
        fake_conn = FakeConnection()
        session = {
            "id": "session-1",
            "status": "active",
            "current_date": None,
            "start_date": str(trading_days[0]),
            "strategy_id": "strategy-1",
            "universe_group_id": "group-1",
            "initial_capital": 1000.0,
            "config": {
                "initial_capital": 1000.0,
                "rebalance_freq": "daily",
            },
        }

        captured_states = []

        def generate_signal(batch_ctx, signal_date, portfolio_state):
            captured_states.append((signal_date, portfolio_state))
            if signal_date == trading_days[0]:
                return [{"ticker": "AAA", "signal": 1, "target_weight": 1.0, "strength": 1.0}]
            return [{"ticker": "BBB", "signal": 1, "target_weight": 1.0, "strength": 1.0}]

        def apply_position_sizing(signals, *args, **kwargs):
            return {s["ticker"]: 1.0 for s in signals if s["signal"] == 1}

        def execute_trades(positions, cash, target_weights, trade_date, cost_rate, price_cache, **kwargs):
            ticker = next(iter(target_weights))
            return {
                "trades": [{"ticker": ticker, "action": "buy"}],
                "positions_after": {ticker: {"shares": 60.0, "avg_price": 10.0}},
                "cash_after": 400.0,
            }

        with (
            patch("backend.services.paper_trading_service.get_connection", return_value=fake_conn),
            patch.object(svc, "get_session", return_value=session),
            patch.object(svc, "_resolve_advance_trading_days", return_value=trading_days),
            patch.object(
                svc,
                "_prev_trading_day",
                side_effect=lambda d, market=None: trading_days[trading_days.index(d) - 1],
            ),
            patch.object(svc, "_prepare_signal_context", return_value={"ctx": True}),
            patch.object(
                svc,
                "_preload_prices",
                return_value={d: {"AAA": (10.0, 10.0), "BBB": (10.0, 10.0)} for d in trading_days},
            ),
            patch.object(svc, "_load_latest_state", return_value=({}, 1000.0)),
            patch.object(
                svc,
                "_build_portfolio_state_from_memory",
                return_value={"current_weights": {"AAA": 0.6}},
            ),
            patch.object(svc, "_generate_signal_single_day", side_effect=generate_signal),
            patch.object(svc, "_apply_position_sizing_from_signals", side_effect=apply_position_sizing),
            patch.object(svc, "_execute_trades_cached", side_effect=execute_trades),
            patch.object(svc, "_value_portfolio_cached", return_value=1000.0),
        ):
            svc.advance("session-1", target_date=str(trading_days[-1]), steps=0)

        self.assertEqual(len(captured_states), 2)
        self.assertEqual(captured_states[0][1]["current_weights"], {})
        self.assertEqual(captured_states[1][1]["current_weights"], {"AAA": 1.0})
        self.assertEqual(captured_states[1][1]["holding_days"], {"AAA": 1})

    def test_advance_skips_execution_when_target_weight_is_unchanged(self):
        class FakeResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def execute(self, query, params=None):
                if "SELECT MAX(date) FROM daily_bars" in query:
                    return FakeResult(row=(date(2026, 4, 8),))
                if "SELECT date FROM paper_trading_daily" in query:
                    return FakeResult(rows=[])
                return FakeResult()

            def executemany(self, query, rows):
                self.snapshots = list(rows)

        svc = PaperTradingService.__new__(PaperTradingService)
        svc._strategy_service = Mock()
        svc._strategy_service.get_strategy.return_value = {
            "source_code": "",
            "position_sizing": "equal_weight",
        }
        svc._group_service = Mock()
        svc._group_service.get_group_tickers.return_value = ["AAA"]

        trading_days = [
            date(2026, 4, 6),
            date(2026, 4, 7),
            date(2026, 4, 8),
        ]
        session = {
            "id": "session-1",
            "status": "active",
            "current_date": None,
            "start_date": str(trading_days[0]),
            "strategy_id": "strategy-1",
            "universe_group_id": "group-1",
            "initial_capital": 1000.0,
            "config": {
                "initial_capital": 1000.0,
                "rebalance_freq": "daily",
                "rebalance_buffer": 0.03,
            },
        }

        execute_kwargs = []

        def execute_trades(positions, cash, target_weights, trade_date, cost_rate, price_cache, **kwargs):
            execute_kwargs.append(kwargs)
            if not positions:
                return {
                    "trades": [{"ticker": "AAA", "action": "buy"}],
                    "positions_after": {"AAA": {"shares": 100.0, "avg_price": 10.0}},
                    "cash_after": 0.0,
                }
            return {
                "trades": [],
                "positions_after": positions,
                "cash_after": cash,
            }

        with (
            patch("backend.services.paper_trading_service.get_connection", return_value=FakeConnection()),
            patch.object(svc, "get_session", return_value=session),
            patch.object(svc, "_resolve_advance_trading_days", return_value=trading_days),
            patch.object(
                svc,
                "_prev_trading_day",
                side_effect=lambda d, market=None: trading_days[trading_days.index(d) - 1],
            ),
            patch.object(svc, "_prepare_signal_context", return_value={"ctx": True}),
            patch.object(
                svc,
                "_preload_prices",
                return_value={d: {"AAA": (10.0, 12.0)} for d in trading_days},
            ),
            patch.object(svc, "_load_latest_state", return_value=({}, 1000.0)),
            patch.object(
                svc,
                "_build_portfolio_state_from_memory",
                return_value={"current_weights": {"AAA": 0.98}},
            ),
            patch.object(
                svc,
                "_generate_signal_single_day",
                return_value=[{"ticker": "AAA", "signal": 1, "target_weight": 1.0, "strength": 1.0}],
            ),
            patch.object(svc, "_apply_position_sizing_from_signals", return_value={"AAA": 1.0}),
            patch.object(svc, "_execute_trades_cached", side_effect=execute_trades),
            patch.object(svc, "_value_portfolio_cached", return_value=1000.0),
        ):
            svc.advance("session-1", target_date=str(trading_days[-1]), steps=0)

        self.assertEqual(len(execute_kwargs), 2)
        self.assertEqual(execute_kwargs[1]["no_trade_tickers"], {"AAA"})

    def test_execute_trades_can_skip_buffered_tickers_without_rebalancing_shares(self):
        positions = {"AAA": {"shares": 10.0, "avg_price": 10.0}}
        result = PaperTradingService._execute_trades_cached(
            positions=positions,
            cash=100.0,
            target_weights={"AAA": 0.5},
            trade_date=date(2026, 4, 10),
            cost_rate=0.002,
            price_cache={date(2026, 4, 10): {"AAA": (12.0, 12.0)}},
            no_trade_tickers={"AAA"},
        )

        self.assertEqual(result["trades"], [])
        self.assertEqual(result["positions_after"], positions)
        self.assertEqual(result["cash_after"], 100.0)


if __name__ == "__main__":
    unittest.main()
