import asyncio
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.time_utils import utc_now_naive
from backend.services.data_service import DataService
from backend.services.group_service import GroupService, _fetch_chinext_tickers
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

    def test_missing_index_history_incremental_start_uses_full_history_window(self):
        svc = DataService(provider=_NoopProvider())

        with patch("backend.services.data_service.get_connection", return_value=self.conn):
            start = svc._get_index_incremental_start(
                "sh.000300",
                date(2026, 4, 30),
                mode="incremental",
                market="CN",
                history_years=10,
            )

        self.assertEqual(start, date(2016, 1, 1))

    def test_existing_index_history_incremental_start_continues_after_max_date(self):
        svc = DataService(provider=_NoopProvider())
        self.conn.execute(
            """
            INSERT INTO index_bars (market, symbol, date, close)
            VALUES ('CN', 'sh.000300', DATE '2026-04-24', 3900)
            """
        )

        with patch("backend.services.data_service.get_connection", return_value=self.conn):
            start = svc._get_index_incremental_start(
                "sh.000300",
                date(2026, 4, 30),
                mode="incremental",
                market="CN",
                history_years=10,
            )

        self.assertEqual(start, date(2026, 4, 25))

    def test_data_status_defaults_to_us_and_filters_explicit_market(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', ?),
                ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
            """,
            [utc_now_naive(), utc_now_naive()],
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

    def test_mark_stale_running_updates_clears_data_update_log_and_progress(self):
        progress_path = Path(self._tmp.name) / "update_progress.json"
        progress_path.write_text('{"run_id":"cn_run"}')
        self.conn.execute(
            """
            INSERT INTO data_update_log
                (id, market, update_type, started_at, completed_at, status, total_tickers, success_count, fail_count)
            VALUES
                ('cn_run', 'CN', 'incremental', ?, NULL, 'running', 10, 3, 0),
                ('us_run', 'US', 'incremental', ?, ?, 'completed', 1, 1, 0)
            """,
            [datetime(2026, 4, 30), datetime(2026, 4, 29), datetime(2026, 4, 29)],
        )
        svc = DataService(provider=_NoopProvider())
        svc._progress_path = progress_path

        with patch("backend.services.data_service.get_connection", return_value=self.conn):
            count = svc.mark_stale_running_updates()

        self.assertEqual(count, 1)
        self.assertFalse(progress_path.exists())
        row = self.conn.execute(
            "SELECT status, completed_at IS NOT NULL, message FROM data_update_log WHERE id = 'cn_run'"
        ).fetchone()
        self.assertEqual(row[0], "failed")
        self.assertTrue(row[1])
        self.assertIn("server startup", row[2])

    def test_cn_stock_search_matches_numeric_code_without_exchange_prefix(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
            """,
            [utc_now_naive()],
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
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch("backend.services.data_service.get_latest_trading_day", return_value=date(2024, 1, 2)),
            patch(
                "backend.services.group_service._fetch_cn_index_tickers",
                side_effect=lambda index_id: ["sh.600000"] if index_id == "cn_sz50" else [],
            ),
            patch("backend.services.group_service._load_cn_index_seed_tickers", return_value=[]),
        ):
            summary = svc.update_data("incremental", market="CN")

        self.assertEqual(summary["market"], "CN")
        self.assertEqual(summary["total"], 1)
        self.assertEqual(provider.stock_list_calls, 0)
        self.assertEqual(
            self.conn.execute("SELECT market, ticker FROM stocks").fetchall(),
            [("CN", "sh.600000")],
        )
        self.assertEqual(
            self.conn.execute("SELECT market, ticker FROM daily_bars").fetchall(),
            [("CN", "sh.600000")],
        )
        self.assertEqual(provider.daily_bar_requests[0]["start"], date(2014, 1, 1))

    def test_cn_incremental_new_tickers_default_to_ten_year_history(self):
        provider = _OneCnProvider()
        svc = DataService(provider=provider)

        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch("backend.services.data_service.get_latest_trading_day", return_value=date(2026, 4, 29)),
            patch(
                "backend.services.group_service._fetch_cn_index_tickers",
                side_effect=lambda index_id: ["sh.600000"] if index_id == "cn_sz50" else [],
            ),
            patch("backend.services.group_service._load_cn_index_seed_tickers", return_value=[]),
        ):
            summary = svc.update_data("incremental", market="CN")

        self.assertEqual(summary["total"], 1)
        self.assertEqual(provider.stock_list_calls, 0)
        self.assertEqual(provider.daily_bar_requests[0]["start"], date(2016, 1, 1))

    def test_cn_incremental_uses_core_index_union_instead_of_all_a_stock_list(self):
        provider = _AllAProvider()
        svc = DataService(provider=provider)

        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch("backend.services.data_service.get_latest_trading_day", return_value=date(2026, 4, 29)),
            patch(
                "backend.services.group_service._fetch_cn_index_tickers",
                side_effect=lambda index_id: {
                    "cn_sz50": ["sh.600000"],
                    "cn_hs300": ["sh.600000", "sz.000001"],
                    "cn_zz500": ["sz.000002"],
                    "cn_chinext": ["sz.300001"],
                }[index_id],
            ),
        ):
            summary = svc.update_data("incremental", market="CN")

        self.assertEqual(provider.stock_list_calls, 0)
        self.assertEqual(summary["total"], 4)
        self.assertEqual(
            provider.daily_bar_requests[0]["tickers"],
            ["sh.600000", "sz.000001", "sz.000002", "sz.300001"],
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT ticker FROM stocks WHERE market = 'CN' ORDER BY ticker"
            ).fetchall(),
            [("sh.600000",), ("sz.000001",), ("sz.000002",), ("sz.300001",)],
        )

    def test_cn_incremental_history_years_override_controls_new_ticker_backfill(self):
        provider = _OneCnProvider()
        svc = DataService(provider=provider)

        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch("backend.services.data_service.get_latest_trading_day", return_value=date(2026, 4, 29)),
            patch(
                "backend.services.group_service._fetch_cn_index_tickers",
                side_effect=lambda index_id: ["sh.600000"] if index_id == "cn_sz50" else [],
            ),
            patch("backend.services.group_service._load_cn_index_seed_tickers", return_value=[]),
        ):
            summary = svc.update_data("incremental", market="CN", history_years=3)

        self.assertEqual(summary["history_years"], 3)
        self.assertEqual(provider.stock_list_calls, 0)
        self.assertEqual(provider.daily_bar_requests[0]["start"], date(2023, 1, 1))

    def test_us_incremental_new_tickers_keep_ten_year_history(self):
        provider = _OneUsProvider()
        svc = DataService(provider=provider)

        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.data_service.get_latest_trading_day", return_value=date(2026, 4, 29)),
        ):
            summary = svc.update_data("incremental", market="US")

        self.assertEqual(summary["market"], "US")
        self.assertEqual(provider.daily_bar_requests[0]["start"], date(2016, 1, 1))

    def test_refresh_stock_list_populates_cn_core_index_union_without_downloading_bars(self):
        provider = _TwoCnProvider()
        svc = DataService(provider=provider)

        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch(
                "backend.services.group_service._fetch_cn_index_tickers",
                side_effect=lambda index_id: {
                    "cn_sz50": ["sh.600000"],
                    "cn_hs300": ["sh.600000", "sz.000001"],
                    "cn_zz500": ["sz.000002"],
                    "cn_chinext": ["sz.300001"],
                }[index_id],
            ),
        ):
            summary = svc.refresh_stock_list(market="CN")

        self.assertEqual(summary["market"], "CN")
        self.assertEqual(summary["provider_count"], 0)
        self.assertEqual(summary["universe_count"], 4)
        self.assertEqual(summary["stock_count"], 4)
        self.assertEqual(summary["active_stock_count"], 4)
        self.assertEqual(provider.stock_list_calls, 0)
        self.assertEqual(provider.daily_bar_calls, 0)
        self.assertEqual(
            self.conn.execute(
                "SELECT ticker FROM stock_group_members WHERE group_id = 'cn_a_core_indices_union' AND market = 'CN' ORDER BY ticker"
            ).fetchall(),
            [("sh.600000",), ("sz.000001",), ("sz.000002",), ("sz.300001",)],
        )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0], 0)

    def test_refresh_stock_list_preserves_existing_cn_stock_metadata(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES ('CN', 'sh.600000', '浦发银行', 'SH', 'Bank', 'active', ?)
            """,
            [utc_now_naive()],
        )
        provider = _TwoCnProvider()
        svc = DataService(provider=provider)

        with (
            patch("backend.services.data_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch(
                "backend.services.group_service._fetch_cn_index_tickers",
                side_effect=lambda index_id: {
                    "cn_sz50": ["sh.600000"],
                    "cn_hs300": [],
                    "cn_zz500": [],
                    "cn_chinext": [],
                }[index_id],
            ),
            patch("backend.services.group_service._load_cn_index_seed_tickers", return_value=[]),
        ):
            svc.refresh_stock_list(market="CN")

        row = self.conn.execute(
            "SELECT name, exchange, sector, status FROM stocks WHERE market = 'CN' AND ticker = 'sh.600000'"
        ).fetchone()
        self.assertEqual(row, ("浦发银行", "SH", "Bank", "active"))

    def test_cn_builtin_all_a_refreshes_when_stocks_change(self):
        svc = GroupService()
        with patch("backend.services.group_service.get_connection", return_value=self.conn):
            self.conn.execute(
                """
                INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
                VALUES ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
                """,
                [utc_now_naive()],
            )
            svc.ensure_builtins("CN")
            self.assertEqual(svc.get_group("cn_all_a", market="CN")["member_count"], 1)

            self.conn.execute(
                """
                INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
                VALUES ('CN', 'sz.000001', '平安银行', 'SZ', '', 'active', ?)
                """,
                [utc_now_naive()],
            )
            svc.ensure_builtins("CN")

            refreshed = svc.get_group("cn_all_a", market="CN")

        self.assertEqual(refreshed["member_count"], 2)
        self.assertEqual(refreshed["tickers"], ["sh.600000", "sz.000001"])

    def test_cn_builtins_seed_core_union_when_created(self):
        svc = GroupService()
        with patch("backend.services.group_service.get_connection", return_value=self.conn):
            svc.ensure_builtins("CN")
            groups = {
                group_id: svc.get_group(group_id, market="CN")
                for group_id in [
                    "cn_sz50",
                    "cn_hs300",
                    "cn_zz500",
                    "cn_chinext",
                    "cn_a_core_indices_union",
                ]
            }

        self.assertEqual(groups["cn_sz50"]["member_count"], 50)
        self.assertEqual(groups["cn_hs300"]["member_count"], 300)
        self.assertEqual(groups["cn_zz500"]["member_count"], 500)
        self.assertEqual(groups["cn_chinext"]["member_count"], 100)
        self.assertEqual(groups["cn_a_core_indices_union"]["member_count"], 806)
        self.assertIn("sh.600519", groups["cn_a_core_indices_union"]["tickers"])
        self.assertIn("sz.300750", groups["cn_a_core_indices_union"]["tickers"])

    def test_cn_index_refresh_builds_core_union_from_four_sources(self):
        svc = GroupService()

        with (
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch(
                "backend.services.group_service._fetch_cn_index_tickers",
                side_effect=lambda index_id: {
                    "cn_sz50": ["sh.600000", "sh.600010"],
                    "cn_hs300": ["sh.600000", "sz.000001"],
                    "cn_zz500": ["sz.000001", "sz.000002"],
                    "cn_chinext": ["sz.300001", "sz.300002"],
                }[index_id],
            ),
        ):
            groups = svc.refresh_index_groups("CN")
            union = svc.get_group("cn_a_core_indices_union", market="CN")

        by_id = {group["id"]: group for group in groups}
        self.assertEqual(by_id["cn_sz50"]["member_count"], 2)
        self.assertEqual(by_id["cn_hs300"]["member_count"], 2)
        self.assertEqual(by_id["cn_zz500"]["member_count"], 2)
        self.assertEqual(by_id["cn_chinext"]["member_count"], 2)
        self.assertEqual(
            union["tickers"],
            ["sh.600000", "sh.600010", "sz.000001", "sz.000002", "sz.300001", "sz.300002"],
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM stock_group_members WHERE group_id = 'cn_a_core_indices_union' AND market = 'CN'"
            ).fetchone()[0],
            6,
        )

    def test_cn_index_refresh_uses_real_seed_when_sources_return_empty(self):
        svc = GroupService()

        with (
            patch("backend.services.group_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service._fetch_cn_index_tickers", return_value=[]),
        ):
            groups = svc.refresh_index_groups("CN")
            union = svc.get_group("cn_a_core_indices_union", market="CN")

        by_id = {group["id"]: group for group in groups}
        self.assertEqual(by_id["cn_sz50"]["member_count"], 50)
        self.assertEqual(by_id["cn_hs300"]["member_count"], 300)
        self.assertEqual(by_id["cn_zz500"]["member_count"], 500)
        self.assertEqual(by_id["cn_chinext"]["member_count"], 100)
        self.assertGreaterEqual(union["member_count"], 700)
        self.assertIn("sh.600519", union["tickers"])
        self.assertIn("sz.300750", union["tickers"])

    def test_chinext_fetch_reads_only_component_code_column(self):
        html = """
        <table>
          <tr><td>无关代码</td><td>说明</td></tr>
          <tr><td>300999</td><td>不是成分表</td></tr>
        </table>
        <table>
          <tr><td>品种代码</td><td>品种名称</td><td>纳入日期</td></tr>
          <tr><td>300001</td><td>特锐德</td><td>2010-06-01</td></tr>
          <tr><td>300750</td><td>宁德时代</td><td>2018-06-01</td></tr>
          <tr><td>备注</td><td>302999</td><td>非代码列数字</td></tr>
        </table>
        """

        response = _FakeResponse(html)
        with patch("backend.services.group_service.requests.get", return_value=response):
            tickers = _fetch_chinext_tickers()

        self.assertEqual(tickers, ["sz.300001", "sz.300750"])

    def test_cn_manual_group_preserves_ticker_format_and_rejects_cross_market_members(self):
        self.conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', ?),
                ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', ?)
            """,
            [utc_now_naive(), utc_now_naive()],
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
            [utc_now_naive(), utc_now_naive()],
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
        self.daily_bar_requests = []

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
        self.daily_bar_requests.append({"tickers": tickers, "start": start, "end": end})
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


class _OneUsProvider:
    def __init__(self):
        self.daily_bar_requests = []

    def get_stock_list(self):
        return pd.DataFrame(
            [
                {
                    "market": "US",
                    "ticker": "AAPL",
                    "name": "Apple",
                    "exchange": "NASDAQ",
                    "sector": "Technology",
                    "status": "active",
                }
            ]
        )

    def get_daily_bars(self, tickers, start, end):
        self.daily_bar_requests.append({"tickers": tickers, "start": start, "end": end})
        return pd.DataFrame(
            [
                {
                    "market": "US",
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


class _TwoCnProvider:
    def __init__(self):
        self.daily_bar_calls = 0
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
                },
                {
                    "market": "CN",
                    "ticker": "sz.000001",
                    "name": "平安银行",
                    "exchange": "SZ",
                    "sector": "",
                    "status": "active",
                },
            ]
        )

    def get_daily_bars(self, tickers, start, end):
        self.daily_bar_calls += 1
        return pd.DataFrame()

    def get_index_data(self, symbol, start, end):
        return pd.DataFrame(columns=["market", "symbol", "date", "open", "high", "low", "close", "volume"])


class _AllAProvider:
    def __init__(self):
        self.stock_list_calls = 0
        self.daily_bar_requests = []

    def get_stock_list(self):
        self.stock_list_calls += 1
        return pd.DataFrame(
            [
                {
                    "market": "CN",
                    "ticker": "sh.999999",
                    "name": "不应使用",
                    "exchange": "SH",
                    "sector": "",
                    "status": "active",
                }
            ]
        )

    def get_daily_bars(self, tickers, start, end):
        self.daily_bar_requests.append({"tickers": tickers, "start": start, "end": end})
        return pd.DataFrame(
            [
                {
                    "market": "CN",
                    "ticker": ticker,
                    "date": end,
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "volume": 1000,
                    "adj_factor": 1.0,
                }
                for ticker in tickers
            ]
        )

    def get_index_data(self, symbol, start, end):
        return pd.DataFrame(columns=["market", "symbol", "date", "open", "high", "low", "close", "volume"])


class _FakeResponse:
    def __init__(self, text: str):
        self.text = text
        self.encoding = "utf-8"
        self.apparent_encoding = "utf-8"

    def raise_for_status(self):
        return None


if __name__ == "__main__":
    unittest.main()
