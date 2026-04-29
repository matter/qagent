import tempfile
import unittest
from pathlib import Path

import duckdb

from backend.services.schema_migrations import migrate_market_schema, validate_market_schema


class SchemaMigrationContractTests(unittest.TestCase):
    def test_market_schema_migration_preserves_existing_rows(self):
        db_path = self._create_legacy_db()
        conn = duckdb.connect(str(db_path))
        try:
            report = migrate_market_schema(conn, migration_id="test_market_migration")
            validation = validate_market_schema(conn)

            self.assertEqual(report["status"], "applied")
            self.assertEqual(validation["tables"]["stocks"]["row_count"], 2)
            self.assertEqual(validation["tables"]["daily_bars"]["row_count"], 2)
            self.assertEqual(validation["tables"]["stocks"]["null_market_count"], 0)
            self.assertEqual(validation["tables"]["daily_bars"]["null_market_count"], 0)
            self.assertEqual(
                conn.execute("SELECT DISTINCT market FROM stocks ORDER BY market").fetchall(),
                [("US",)],
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE id = ?", ["test_market_migration"]).fetchone()[0],
                1,
            )
        finally:
            conn.close()

    def test_market_schema_migration_dry_run_does_not_modify_tables(self):
        db_path = self._create_legacy_db()
        conn = duckdb.connect(str(db_path))
        try:
            report = migrate_market_schema(conn, migration_id="dry_run", dry_run=True)
            cols = [r[1] for r in conn.execute("PRAGMA table_info('stocks')").fetchall()]

            self.assertEqual(report["status"], "dry_run")
            self.assertNotIn("market", cols)
        finally:
            conn.close()

    def _create_legacy_db(self) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "legacy.duckdb"
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(
                """
                CREATE TABLE stocks (
                    ticker VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    exchange VARCHAR,
                    sector VARCHAR,
                    status VARCHAR DEFAULT 'active',
                    updated_at TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE daily_bars (
                    ticker VARCHAR NOT NULL,
                    date DATE NOT NULL,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    adj_factor DOUBLE DEFAULT 1.0,
                    PRIMARY KEY (ticker, date)
                )
                """
            )
            conn.execute(
                "INSERT INTO stocks (ticker, name, exchange) VALUES ('AAPL', 'Apple', 'NASDAQ'), ('MSFT', 'Microsoft', 'NASDAQ')"
            )
            conn.execute(
                """
                INSERT INTO daily_bars (ticker, date, open, high, low, close, volume, adj_factor)
                VALUES
                ('AAPL', DATE '2024-01-02', 1, 2, 1, 2, 100, 1),
                ('MSFT', DATE '2024-01-02', 3, 4, 3, 4, 200, 1)
                """
            )
        finally:
            conn.close()
        return db_path


if __name__ == "__main__":
    unittest.main()

