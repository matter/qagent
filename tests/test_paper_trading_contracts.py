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

        def execute_trades(positions, cash, target_weights, trade_date, cost_rate, price_cache):
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
                side_effect=lambda d: d - timedelta(days=1),
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
        self.assertEqual(fake_conn.snapshots[0][1], first_day)
        self.assertEqual(json.loads(fake_conn.snapshots[0][5]), [])

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

        def execute_trades(positions, cash, target_weights, trade_date, cost_rate, price_cache):
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
                side_effect=lambda d: trading_days[trading_days.index(d) - 1],
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


if __name__ == "__main__":
    unittest.main()
