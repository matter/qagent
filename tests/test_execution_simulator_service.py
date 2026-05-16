import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.execution_simulator_service import ExecutionSimulatorService


class ExecutionSimulatorServiceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "execution_simulator.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        conn = get_connection()
        conn.executemany(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('US', ?, ?, 'NYSE', 'Test', 'active', current_timestamp)""",
            [
                ("AAA", "Next open"),
                ("BBB", "Next close"),
                ("CCC", "Planned fallback"),
                ("DDD", "Buy limit"),
                ("EEE", "Stop sell"),
                ("FFF", "Stop limit blocked"),
            ],
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('US', ?, ?, ?, ?, ?, ?, 100000, 1.0)""",
            [
                ("AAA", "2024-01-03", 10.0, 10.5, 9.8, 10.2),
                ("BBB", "2024-01-03", 20.0, 21.0, 19.5, 20.5),
                ("CCC", "2024-01-02", 30.0, 30.5, 29.5, 30.0),
                ("CCC", "2024-01-03", 31.0, 32.0, 30.8, 31.5),
                ("DDD", "2024-01-03", 40.0, 41.0, 39.0, 40.5),
                ("EEE", "2024-01-03", 50.0, 52.0, 48.0, 49.0),
                ("FFF", "2024-01-03", 60.0, 62.0, 58.0, 61.0),
            ],
        )

    def test_executes_mixed_daily_orders_and_persists_diagnostics(self):
        service = ExecutionSimulatorService()
        orders = [
            self._order("US_EQ:AAA", "buy", 0.10, "next_open"),
            self._order("US_EQ:BBB", "buy", 0.10, "next_close"),
            {
                **self._order("US_EQ:CCC", "buy", 0.10, "planned_price"),
                "planned_price": 30.1,
                "planned_price_buffer_bps": 50,
                "fill_fallback": "next_close",
            },
            {
                **self._order("US_EQ:DDD", "buy", 0.10, "limit"),
                "limit_price": 39.5,
            },
            {
                **self._order("US_EQ:EEE", "sell", 0.0, "stop"),
                "stop_price": 49.0,
            },
            {
                **self._order("US_EQ:FFF", "buy", 0.10, "stop_limit"),
                "stop_price": 61.0,
                "limit_price": 57.0,
            },
        ]

        result = service.execute_orders(
            backtest_run_id="bt1",
            order_intents=orders,
            market_profile_id="US_EQ",
            nav=1_000_000,
        )

        diagnostics = result["diagnostics"]
        self.assertEqual(result["filled_order_count"], 5)
        self.assertEqual(result["blocked_order_count"], 1)
        self.assertEqual(diagnostics["execution_model"], "mixed")
        self.assertEqual(diagnostics["execution_model_counts"]["next_open"], 1)
        self.assertEqual(diagnostics["execution_model_counts"]["stop_limit"], 1)
        self.assertGreaterEqual(diagnostics["path_assumption_warning_count"], 3)

        filled_by_asset = {item["asset_id"]: item for item in diagnostics["filled"]}
        blocked_by_asset = {item["asset_id"]: item for item in diagnostics["blocked"]}
        self.assertEqual(filled_by_asset["US_EQ:AAA"]["price"], 10.0)
        self.assertEqual(filled_by_asset["US_EQ:BBB"]["price"], 20.5)
        self.assertEqual(filled_by_asset["US_EQ:CCC"]["fill_type"], "fallback_close")
        self.assertEqual(filled_by_asset["US_EQ:CCC"]["price"], 31.5)
        self.assertEqual(filled_by_asset["US_EQ:DDD"]["fill_type"], "limit")
        self.assertEqual(filled_by_asset["US_EQ:EEE"]["fill_type"], "stop")
        self.assertEqual(blocked_by_asset["US_EQ:FFF"]["reason"], "stop_limit_not_reached")
        self.assertIn("daily_bar_no_intraday_path", blocked_by_asset["US_EQ:FFF"]["warnings"])

        rows = get_connection().execute(
            """SELECT asset_id, quantity, price, metadata
               FROM backtest_trades
               WHERE backtest_run_id = ?
               ORDER BY asset_id""",
            ["bt1"],
        ).fetchall()
        self.assertEqual(len(rows), 6)
        self.assertEqual(len([row for row in rows if row[1] is None]), 1)
        self.assertIn('"fill_status": "blocked"', rows[-1][3])

    def _order(self, asset_id: str, side: str, target_weight: float, model: str) -> dict:
        return {
            "decision_date": "2024-01-02",
            "execution_date": "2024-01-03",
            "asset_id": asset_id,
            "side": side,
            "current_weight": 0.0 if side == "buy" else 0.10,
            "target_weight": target_weight,
            "delta_weight": 0.10 if side == "buy" else -0.10,
            "estimated_value": 100000,
            "execution_model": model,
            "execution_policy_id": "policy1",
        }


if __name__ == "__main__":
    unittest.main()
