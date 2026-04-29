from __future__ import annotations

import unittest

import duckdb


class SqlFilterHelperTests(unittest.TestCase):
    def test_registered_values_table_filters_rows_and_cleans_up(self):
        from backend.services.sql_filters import registered_values_table

        conn = duckdb.connect(":memory:")
        self.addCleanup(conn.close)
        conn.execute("CREATE TABLE prices (ticker VARCHAR, close DOUBLE)")
        conn.execute("INSERT INTO prices VALUES ('AAPL', 1), ('MSFT', 2), ('TSLA', 3)")

        with registered_values_table(conn, "ticker", ["MSFT", "AAPL"]) as table_name:
            rows = conn.execute(
                f"""
                SELECT p.ticker, p.close
                FROM prices p
                JOIN {table_name} selected ON p.ticker = selected.ticker
                ORDER BY p.ticker
                """
            ).fetchall()

        self.assertEqual(rows, [("AAPL", 1.0), ("MSFT", 2.0)])
        with self.assertRaises(Exception):
            conn.execute(f"SELECT * FROM {table_name}").fetchall()


if __name__ == "__main__":
    unittest.main()
