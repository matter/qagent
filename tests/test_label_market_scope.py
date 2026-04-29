import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from backend.services.label_service import LabelService


class LabelMarketScopeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "labels.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.addCleanup(self.conn.close)
        self._create_schema()

    def test_label_crud_defaults_to_us_and_filters_explicit_market(self):
        svc = LabelService()

        with patch("backend.services.label_service.get_connection", return_value=self.conn):
            us_label = svc.create_label("US return")
            cn_label = svc.create_label("CN return", market="CN")
            us_labels = svc.list_labels()
            cn_labels = svc.list_labels("CN")

            with self.assertRaisesRegex(ValueError, "not found"):
                svc.get_label(cn_label["id"])

            cn_detail = svc.get_label(cn_label["id"], market="CN")

        self.assertEqual(us_label["market"], "US")
        self.assertEqual(cn_label["market"], "CN")
        self.assertEqual([label["id"] for label in us_labels], [us_label["id"]])
        self.assertEqual([label["id"] for label in cn_labels], [cn_label["id"]])
        self.assertEqual(cn_detail["market"], "CN")

    def test_cn_excess_label_rejects_us_benchmark(self):
        svc = LabelService()

        with patch("backend.services.label_service.get_connection", return_value=self.conn):
            with self.assertRaisesRegex(ValueError, "benchmark.*CN"):
                svc.create_label(
                    "Bad CN excess",
                    market="CN",
                    target_type="excess_return",
                    benchmark="SPY",
                )

    def test_cn_presets_do_not_collide_with_legacy_unique_label_names(self):
        legacy_db = Path(self._tmp.name) / "legacy_labels.duckdb"
        legacy_conn = duckdb.connect(str(legacy_db))
        self.addCleanup(legacy_conn.close)
        legacy_conn.execute(
            """
            CREATE TABLE label_definitions (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR UNIQUE,
                description TEXT,
                target_type VARCHAR NOT NULL,
                horizon INTEGER NOT NULL,
                benchmark VARCHAR,
                config TEXT,
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        svc = LabelService()

        with patch("backend.services.label_service.get_connection", return_value=legacy_conn):
            svc.ensure_presets("US")
            svc.ensure_presets("CN")
            cn_names = [
                row[0]
                for row in legacy_conn.execute(
                    "SELECT name FROM label_definitions WHERE market = 'CN' ORDER BY name LIMIT 3"
                ).fetchall()
            ]

        self.assertTrue(cn_names)
        self.assertTrue(all(name.startswith("cn_") for name in cn_names))

    def test_compute_label_values_filters_daily_bars_by_market(self):
        svc = LabelService()
        self._insert_daily_bars()

        with patch("backend.services.label_service.get_connection", return_value=self.conn):
            label = svc.create_label("CN return", market="CN", horizon=1)
            labels = svc.compute_label_values(
                label["id"],
                ["sh.600000"],
                "2024-01-02",
                "2024-01-03",
                market="CN",
            )

        first_value = labels.sort_values("date").iloc[0]["label_value"]
        self.assertAlmostEqual(first_value, 0.1)

    def test_excess_label_uses_market_scoped_benchmark(self):
        svc = LabelService()
        self._insert_daily_bars()
        self.conn.execute(
            """
            INSERT INTO index_bars (market, symbol, date, close)
            VALUES
                ('CN', 'sh.000300', DATE '2024-01-02', 100),
                ('CN', 'sh.000300', DATE '2024-01-03', 110),
                ('US', 'sh.000300', DATE '2024-01-02', 100),
                ('US', 'sh.000300', DATE '2024-01-03', 200)
            """
        )

        with patch("backend.services.label_service.get_connection", return_value=self.conn):
            label = svc.create_label(
                "CN excess",
                market="CN",
                target_type="excess_return",
                horizon=1,
                benchmark="sh.000300",
            )
            labels = svc.compute_label_values(
                label["id"],
                ["sh.600000"],
                "2024-01-02",
                "2024-01-03",
                market="CN",
            )

        first_value = labels.sort_values("date").iloc[0]["label_value"]
        self.assertAlmostEqual(first_value, 0.0)

    def _insert_daily_bars(self):
        self.conn.execute(
            """
            INSERT INTO daily_bars (market, ticker, date, close)
            VALUES
                ('CN', 'sh.600000', DATE '2024-01-02', 10),
                ('CN', 'sh.600000', DATE '2024-01-03', 11),
                ('US', 'sh.600000', DATE '2024-01-02', 100),
                ('US', 'sh.600000', DATE '2024-01-03', 50)
            """
        )

    def _create_schema(self):
        self.conn.execute(
            """
            CREATE TABLE label_definitions (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                description TEXT,
                target_type VARCHAR NOT NULL,
                horizon INTEGER NOT NULL,
                benchmark VARCHAR,
                config TEXT,
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(market, name)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE daily_bars (
                market VARCHAR NOT NULL DEFAULT 'US',
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adj_factor DOUBLE DEFAULT 1.0,
                PRIMARY KEY (market, ticker, date)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE index_bars (
                market VARCHAR NOT NULL DEFAULT 'US',
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                PRIMARY KEY (market, symbol, date)
            )
            """
        )


if __name__ == "__main__":
    unittest.main()
