import asyncio
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.services.data_service import DataService
from backend.services.group_service import GroupService
from backend.api.data import search_stocks


class DataAndGroupMarketScopeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "market_scope.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.addCleanup(self.conn.close)
        self._create_schema()

    def test_data_upserts_preserve_market_values(self):
        svc = DataService(provider=_NoopProvider())

        stocks = pd.DataFrame(
            [
                {
                    "market": "CN",
                    "ticker": "sh.600000",
                    "name": "浦发银行",
                    "exchange": "SH",
                    "sector": "",
                    "status": "active",
                }
            ]
        )
        bars = pd.DataFrame(
            [
                {
                    "market": "CN",
                    "ticker": "sh.600000",
                    "date": date(2024, 1, 2),
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "volume": 1000,
                    "adj_factor": 1.0,
                }
            ]
        )
        index_bars = pd.DataFrame(
            [
                {
                    "market": "CN",
                    "symbol": "sh.000300",
                    "date": date(2024, 1, 2),
                    "open": 3300.0,
                    "high": 3310.0,
                    "low": 3290.0,
                    "close": 3305.0,
                    "volume": 2000,
                }
            ]
        )

        with patch("backend.services.data_service.get_connection", return_value=self.conn):
            svc._upsert_stocks(stocks)
            svc._upsert_daily_bars(bars)
            svc._upsert_index_bars("sh.000300", index_bars)

        self.assertEqual(
            self.conn.execute("SELECT market, ticker FROM stocks").fetchall(),
            [("CN", "sh.600000")],
        )
        self.assertEqual(
            self.conn.execute("SELECT market, ticker FROM daily_bars").fetchall(),
            [("CN", "sh.600000")],
        )
        self.assertEqual(
            self.conn.execute("SELECT market, symbol FROM index_bars").fetchall(),
            [("CN", "sh.000300")],
        )

    def test_data_status_defaults_to_us_and_filters_explicit_market(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', ?),
                ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
            """,
            [datetime.utcnow(), datetime.utcnow()],
        )
        self.conn.execute(
            """
            INSERT INTO daily_bars (market, ticker, date, open, high, low, close, volume, adj_factor)
            VALUES
                ('US', 'AAPL', DATE '2024-01-02', 1, 2, 1, 2, 100, 1),
                ('CN', 'sh.600000', DATE '2024-01-03', 3, 4, 3, 4, 200, 1),
                ('CN', 'sh.600000', DATE '2024-01-04', 4, 5, 4, 5, 300, 1)
            """
        )
        self.conn.execute(
            """
            INSERT INTO data_update_log
                (id, market, update_type, started_at, completed_at, status, total_tickers, success_count, fail_count)
            VALUES
                ('us_run', 'US', 'incremental', ?, ?, 'completed', 1, 1, 0),
                ('cn_run', 'CN', 'incremental', ?, ?, 'completed', 1, 1, 0)
            """,
            [datetime(2024, 1, 2), datetime(2024, 1, 2), datetime(2024, 1, 4), datetime(2024, 1, 4)],
        )

        svc = DataService(provider=_NoopProvider())
        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.data_service.get_latest_trading_day", return_value=date(2024, 1, 5)),
        ):
            us = svc.get_data_status()
            cn = svc.get_data_status("CN")

        self.assertEqual(us["market"], "US")
        self.assertEqual(us["stock_count"], 1)
        self.assertEqual(us["tickers_with_bars"], 1)
        self.assertEqual(us["date_range"]["max"], "2024-01-02")

        self.assertEqual(cn["market"], "CN")
        self.assertEqual(cn["stock_count"], 1)
        self.assertEqual(cn["tickers_with_bars"], 1)
        self.assertEqual(cn["date_range"]["max"], "2024-01-04")
        self.assertEqual(cn["last_update"]["completed_at"], "2024-01-04 00:00:00")

    def test_cn_stock_search_matches_numeric_code_without_exchange_prefix(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
            """,
            [datetime.utcnow()],
        )

        with patch("backend.api.data.get_connection", return_value=self.conn):
            rows = asyncio.run(search_stocks(q="600000", limit=3, market="CN"))

        self.assertEqual(rows[0]["market"], "CN")
        self.assertEqual(rows[0]["ticker"], "sh.600000")

    def test_incremental_update_does_not_skip_empty_market(self):
        provider = _OneCnProvider()
        svc = DataService(provider=provider)

        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.data_service.get_latest_trading_day", return_value=date(2024, 1, 2)),
        ):
            summary = svc.update_data("incremental", market="CN")

        self.assertEqual(summary["market"], "CN")
        self.assertEqual(summary["total"], 1)
        self.assertEqual(provider.stock_list_calls, 1)
        self.assertEqual(
            self.conn.execute("SELECT market, ticker FROM stocks").fetchall(),
            [("CN", "sh.600000")],
        )
        self.assertEqual(
            self.conn.execute("SELECT market, ticker FROM daily_bars").fetchall(),
            [("CN", "sh.600000")],
        )

    def test_cn_manual_group_preserves_ticker_format_and_rejects_cross_market_members(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', ?),
                ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
            """,
            [datetime.utcnow(), datetime.utcnow()],
        )

        svc = GroupService()
        with patch("backend.services.group_service.get_connection", return_value=self.conn):
            group = svc.create_group("CN manual", market="CN", tickers=["sh.600000"])
            cn_groups = svc.list_groups("CN")

            with self.assertRaisesRegex(ValueError, "not found in market CN"):
                svc.create_group("Bad CN", market="CN", tickers=["AAPL"])

        self.assertEqual(group["market"], "CN")
        self.assertEqual(group["tickers"], ["sh.600000"])
        self.assertEqual([g["id"] for g in cn_groups], [group["id"]])

    def test_filter_group_evaluation_is_market_scoped(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', ?),
                ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
            """,
            [datetime.utcnow(), datetime.utcnow()],
        )

        svc = GroupService()
        with patch("backend.services.group_service.get_connection", return_value=self.conn):
            group = svc.create_group(
                "CN active",
                group_type="filter",
                filter_expr="status = 'active'",
                market="CN",
            )

        self.assertEqual(group["market"], "CN")
        self.assertEqual(group["tickers"], ["sh.600000"])

    def _create_schema(self):
        self.conn.execute(
            """
            CREATE TABLE stocks (
                market VARCHAR NOT NULL DEFAULT 'US',
                ticker VARCHAR NOT NULL,
                name VARCHAR,
                exchange VARCHAR,
                sector VARCHAR,
                status VARCHAR DEFAULT 'active',
                updated_at TIMESTAMP,
                PRIMARY KEY (market, ticker)
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
        self.conn.execute(
            """
            CREATE TABLE data_update_log (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                update_type VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                total_tickers INTEGER,
                success_count INTEGER,
                fail_count INTEGER,
                failed_tickers JSON,
                message TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE stock_groups (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                description TEXT,
                group_type VARCHAR DEFAULT 'manual',
                filter_expr TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(market, name)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE stock_group_members (
                group_id VARCHAR NOT NULL,
                market VARCHAR NOT NULL DEFAULT 'US',
                ticker VARCHAR NOT NULL,
                PRIMARY KEY (group_id, market, ticker)
            )
            """
        )


class _NoopProvider:
    def get_stock_list(self):
        return pd.DataFrame()

    def get_daily_bars(self, tickers, start, end):
        return pd.DataFrame()

    def get_index_data(self, symbol, start, end):
        return pd.DataFrame()


class _OneCnProvider:
    def __init__(self):
        self.stock_list_calls = 0

    def get_stock_list(self):
        self.stock_list_calls += 1
        return pd.DataFrame(
            [
                {
                    "market": "CN",
                    "ticker": "sh.600000",
                    "name": "浦发银行",
                    "exchange": "SH",
                    "sector": "",
                    "status": "active",
                }
            ]
        )

    def get_daily_bars(self, tickers, start, end):
        return pd.DataFrame(
            [
                {
                    "market": "CN",
                    "ticker": tickers[0],
                    "date": end,
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "volume": 1000,
                    "adj_factor": 1.0,
                }
            ]
        )

    def get_index_data(self, symbol, start, end):
        return pd.DataFrame(columns=["market", "symbol", "date", "open", "high", "low", "close", "volume"])


if __name__ == "__main__":
    unittest.main()
