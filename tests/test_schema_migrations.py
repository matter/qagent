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

    def test_market_schema_migration_rebuilds_legacy_strategy_constraints(self):
        db_path = self._create_legacy_strategy_db()
        conn = duckdb.connect(str(db_path))
        try:
            report = migrate_market_schema(conn, migration_id="strategy_constraint_market")

            self.assertIn(report["status"], {"applied", "constraints_applied"})
            self.assertEqual(
                conn.execute(
                    "SELECT MAX(version) FROM strategies WHERE market = 'CN' AND name = 'same_name'"
                ).fetchone()[0],
                None,
            )
            conn.execute(
                """
                INSERT INTO strategies (id, market, name, version, source_code)
                VALUES ('cn_strategy', 'CN', 'same_name', 1, 'source')
                """
            )
            rows = conn.execute(
                "SELECT market, name, version FROM strategies ORDER BY market"
            ).fetchall()
            self.assertEqual(rows, [("CN", "same_name", 1), ("US", "same_name", 1)])

            constraints = conn.execute(
                """
                SELECT constraint_type, constraint_column_names
                FROM duckdb_constraints()
                WHERE table_name = 'strategies'
                """
            ).fetchall()
            unique_columns = [
                list(cols)
                for constraint_type, cols in constraints
                if constraint_type == "UNIQUE"
            ]
            self.assertIn(["market", "name", "version"], unique_columns)
            self.assertNotIn(["name", "version"], unique_columns)
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

    def _create_legacy_strategy_db(self) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "legacy_strategy.duckdb"
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(
                """
                CREATE TABLE strategies (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    description TEXT,
                    source_code TEXT NOT NULL,
                    required_factors JSON,
                    required_models JSON,
                    position_sizing VARCHAR DEFAULT 'equal_weight',
                    status VARCHAR DEFAULT 'draft',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, version)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO strategies (id, name, version, source_code)
                VALUES ('us_strategy', 'same_name', 1, 'source')
                """
            )
            conn.execute("ALTER TABLE strategies ADD COLUMN market VARCHAR DEFAULT 'US'")
        finally:
            conn.close()
        return db_path


if __name__ == "__main__":
    unittest.main()
