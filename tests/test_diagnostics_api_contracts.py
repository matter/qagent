import unittest
from datetime import date
from unittest.mock import patch

from backend.api import diagnostics


class DiagnosticsApiContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_daily_bar_diagnostic_reads_through_backend_connection(self):
        conn = _DiagnosticConnection(
            rows=[
                ("AAPL", date(2026, 1, 5), 10.0, 11.0, 9.5, 10.5, 1000, 1.0),
            ]
        )

        with patch("backend.api.diagnostics.get_connection", return_value=conn):
            payload = await diagnostics.diagnostic_daily_bars(
                tickers=["AAPL"],
                target_date=date(2026, 1, 5),
                market="US",
            )

        self.assertEqual(payload["market"], "US")
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["ticker"], "AAPL")
        self.assertIn("FROM daily_bars", conn.sql)
        self.assertNotIn("unnest", conn.sql)

    async def test_factor_value_diagnostic_is_market_scoped(self):
        conn = _DiagnosticConnection(rows=[("sh.600000", date(2026, 1, 5), 1.23)])

        with patch("backend.api.diagnostics.get_connection", return_value=conn):
            payload = await diagnostics.diagnostic_factor_values(
                factor_id="factor_cn",
                tickers=["600000"],
                target_date=date(2026, 1, 5),
                market="CN",
            )

        self.assertEqual(payload["market"], "CN")
        self.assertEqual(payload["factor_id"], "factor_cn")
        self.assertEqual(payload["items"][0]["ticker"], "sh.600000")
        self.assertEqual(conn.params[0], "CN")
        self.assertNotIn("unnest", conn.sql)

    async def test_db_preflight_endpoint_returns_service_payload(self):
        with patch("backend.api.diagnostics.DbPreflightService") as service_cls:
            service_cls.return_value.check_database.return_value = {
                "ok": False,
                "status": "locked",
            }

            payload = await diagnostics.diagnostic_db_preflight()

        self.assertEqual(payload["status"], "locked")


class _DiagnosticConnection:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = None

    def execute(self, sql, params=None):
        self.sql = str(sql)
        self.params = params
        return self

    def fetchall(self):
        return self.rows

    def register(self, table_name, frame):
        self.registered_table = table_name
        self.registered_frame = frame

    def unregister(self, table_name):
        self.unregistered_table = table_name


if __name__ == "__main__":
    unittest.main()
