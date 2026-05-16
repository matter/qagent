import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from backend.api import paper_trading as paper_api
from backend.api import signals as signal_api
from backend.services.paper_trading_service import PaperTradingService
from backend.services.signal_service import SignalService
from backend.services.strategy_service import StrategyService


_SIGNAL_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class SingleSignalStrategy(StrategyBase):
    name = "SingleSignal"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        signals = pd.DataFrame(
            [{"signal": 1, "weight": 1.0, "strength": 2.0}],
            index=["sh.600000"],
        )
        return signals
"""


class SignalPaperMarketScopeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "signal_paper.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.addCleanup(self.conn.close)
        self._create_schema()

    def test_signal_generation_persists_run_and_details_by_market(self):
        with self._patch_connections():
            strategy = StrategyService().create_strategy(
                "CN signal strategy",
                _SIGNAL_STRATEGY_SOURCE,
                market="CN",
            )
            self._insert_group("cn_group", "CN", ["sh.600000"])
            self._insert_daily_bars("CN", "sh.600000", close=10.5)
            self._insert_daily_bars("US", "sh.600000", close=100.5)

            result = SignalService().generate_signals(
                strategy_id=strategy["id"],
                target_date="2024-01-02",
                universe_group_id="cn_group",
                market="CN",
            )

        self.assertEqual(result["market"], "CN")
        self.assertEqual(result["signals"][0]["ticker"], "sh.600000")
        self.assertEqual(
            self.conn.execute(
                "SELECT market FROM signal_runs WHERE id = ?",
                [result["run_id"]],
            ).fetchall(),
            [("CN",)],
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT market, ticker FROM signal_details WHERE run_id = ?",
                [result["run_id"]],
            ).fetchall(),
            [("CN", "sh.600000")],
        )
        with self._patch_connections():
            self.assertEqual(SignalService().list_signal_runs(market="CN")[0]["market"], "CN")
            self.assertEqual(SignalService().list_signal_runs(), [])

    def test_signal_api_is_disabled_in_v3_2_before_task_submission(self):
        executor = _FakeExecutor()
        with (
            patch.object(signal_api, "_get_executor", return_value=executor),
            patch.object(signal_api, "_get_service", return_value=_FakeSignalService()),
        ):
            with self.assertRaises(signal_api.HTTPException) as ctx:
                asyncio.run(
                    signal_api.generate_signals(
                        signal_api.GenerateSignalsRequest(
                            market="CN",
                            strategy_id="strategy_cn",
                            target_date="2024-01-02",
                            universe_group_id="cn_group",
                        )
                    )
                )

        self.assertEqual(ctx.exception.status_code, 410)
        self.assertIn("production-signals", str(ctx.exception.detail))
        self.assertIsNone(executor.params)

    def test_paper_session_persists_market_and_rejects_us_strategy_for_cn(self):
        with self._patch_connections():
            us_strategy = StrategyService().create_strategy(
                "US paper strategy",
                _SIGNAL_STRATEGY_SOURCE,
                market="US",
            )
            cn_strategy = StrategyService().create_strategy(
                "CN paper strategy",
                _SIGNAL_STRATEGY_SOURCE,
                market="CN",
            )
            self._insert_group("cn_group", "CN", ["sh.600000"])

            with self.assertRaisesRegex(ValueError, "not found"):
                PaperTradingService().create_session(
                    strategy_id=us_strategy["id"],
                    universe_group_id="cn_group",
                    start_date="2024-01-02",
                    market="CN",
                )

            session = PaperTradingService().create_session(
                strategy_id=cn_strategy["id"],
                universe_group_id="cn_group",
                start_date="2024-01-02",
                market="CN",
            )

        self.assertEqual(session["market"], "CN")
        self.assertEqual(
            self.conn.execute(
                "SELECT market FROM paper_trading_sessions WHERE id = ?",
                [session["id"]],
            ).fetchall(),
            [("CN",)],
        )
        with self._patch_connections():
            self.assertEqual(PaperTradingService().list_sessions(market="CN")[0]["market"], "CN")
            self.assertEqual(PaperTradingService().list_sessions(), [])

    def test_paper_price_preload_filters_by_market(self):
        self._insert_daily_bars("CN", "sh.600000", close=10.5)
        self._insert_daily_bars("US", "sh.600000", close=100.5)

        cache = PaperTradingService._preload_prices(
            ["sh.600000"],
            "2024-01-02",
            "2024-01-02",
            self.conn,
            market="CN",
        )

        self.assertEqual(
            cache[next(iter(cache))]["sh.600000"],
            {"open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5},
        )

    def test_paper_api_is_disabled_in_v3_2_before_task_submission(self):
        executor = _FakeExecutor()
        service = _FakePaperService()

        with (
            patch.object(paper_api, "_get_executor", return_value=executor),
            patch.object(paper_api, "_get_svc", return_value=service),
        ):
            with self.assertRaises(paper_api.HTTPException) as create_ctx:
                asyncio.run(
                    paper_api.create_session(
                        paper_api.CreateSessionRequest(
                            market="CN",
                            strategy_id="strategy_cn",
                            universe_group_id="cn_group",
                            start_date="2024-01-02",
                        )
                    )
                )
            with self.assertRaises(paper_api.HTTPException) as advance_ctx:
                asyncio.run(
                    paper_api.advance_session(
                        "session_cn",
                        paper_api.AdvanceRequest(market="CN", steps=1),
                    )
                )

        self.assertEqual(create_ctx.exception.status_code, 410)
        self.assertEqual(advance_ctx.exception.status_code, 410)
        self.assertIsNone(executor.params)

    def test_legacy_paper_advance_api_no_longer_exposes_staged_domain_write_contract(self):
        executor = _InlineExecutor()
        service = _FakePaperAdvanceStagingService()

        with (
            patch.object(paper_api, "_get_executor", return_value=executor),
            patch.object(paper_api, "_get_svc", return_value=service),
        ):
            with self.assertRaises(paper_api.HTTPException) as ctx:
                asyncio.run(
                    paper_api.advance_session(
                        "session_cn",
                        paper_api.AdvanceRequest(market="CN", steps=1),
                    )
                )

        self.assertEqual(ctx.exception.status_code, 410)
        self.assertEqual(executor.staged, [])
        self.assertIsNone(service.stage_domain_write_seen)

    def test_paper_advance_stages_daily_and_session_updates_until_commit(self):
        with self._patch_connections():
            strategy = StrategyService().create_strategy(
                "CN staged paper strategy",
                _SIGNAL_STRATEGY_SOURCE,
                market="CN",
            )
            self._insert_group("cn_group", "CN", ["sh.600000"])
            self._insert_daily_bars("CN", "sh.600000", close=10.5)
            session = PaperTradingService().create_session(
                strategy_id=strategy["id"],
                universe_group_id="cn_group",
                start_date="2024-01-02",
                market="CN",
            )
            staged = []
            result = PaperTradingService().advance(
                session["id"],
                steps=1,
                market="CN",
                stage_domain_write=lambda table, payload=None, commit=None: staged.append(
                    {"table": table, "payload": payload or {}, "commit": commit}
                ),
            )

        self.assertEqual(result["days_processed"], 1)
        self.assertEqual(staged[0]["table"], "paper_trading_advance")
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM paper_trading_daily WHERE session_id = ? AND market = 'CN'",
                [session["id"]],
            ).fetchone()[0],
            0,
        )

        staged[0]["commit"](conn=self.conn)

        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM paper_trading_daily WHERE session_id = ? AND market = 'CN'",
                [session["id"]],
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            str(
                self.conn.execute(
                    "SELECT current_date FROM paper_trading_sessions WHERE id = ? AND market = 'CN'",
                    [session["id"]],
                ).fetchone()[0]
            ),
            "2024-01-02",
        )

    def _insert_group(self, group_id: str, market: str, tickers: list[str]):
        self.conn.execute(
            """
            INSERT INTO stock_groups (id, market, name, group_type)
            VALUES (?, ?, ?, 'custom')
            """,
            [group_id, market, group_id],
        )
        self.conn.executemany(
            "INSERT INTO stock_group_members (group_id, market, ticker) VALUES (?, ?, ?)",
            [(group_id, market, ticker) for ticker in tickers],
        )

    def _insert_daily_bars(self, market: str, ticker: str, close: float):
        self.conn.execute(
            """
            INSERT INTO daily_bars (market, ticker, date, open, high, low, close, volume)
            VALUES (?, ?, DATE '2024-01-02', ?, ?, ?, ?, 100)
            """,
            [market, ticker, close - 0.5, close + 0.5, close - 1.0, close],
        )

    def _patch_connections(self):
        return _MultiPatch(
            patch("backend.services.strategy_service.get_connection", return_value=self.conn),
            patch("backend.services.signal_service.get_connection", return_value=self.conn),
            patch("backend.services.paper_trading_service.get_connection", return_value=self.conn),
            patch("backend.services.group_service.get_connection", return_value=self.conn),
        )

    def _create_schema(self):
        self.conn.execute(
            """
            CREATE TABLE strategies (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
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
                UNIQUE(market, name, version)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE factors (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                status VARCHAR DEFAULT 'draft',
                source_code TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE models (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                feature_set_id VARCHAR NOT NULL,
                label_id VARCHAR NOT NULL,
                model_type VARCHAR DEFAULT 'lightgbm',
                model_params JSON,
                train_config JSON,
                eval_metrics JSON,
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                group_type VARCHAR DEFAULT 'custom',
                filter_expr TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            CREATE TABLE signal_runs (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                strategy_id VARCHAR NOT NULL,
                strategy_version INTEGER,
                target_date DATE NOT NULL,
                universe_group_id VARCHAR,
                result_level VARCHAR DEFAULT 'exploratory',
                dependency_snapshot JSON,
                signal_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE signal_details (
                run_id VARCHAR NOT NULL,
                market VARCHAR NOT NULL DEFAULT 'US',
                ticker VARCHAR NOT NULL,
                signal INTEGER,
                target_weight DOUBLE,
                strength DOUBLE,
                PRIMARY KEY (run_id, market, ticker)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE paper_trading_sessions (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                strategy_id VARCHAR NOT NULL,
                universe_group_id VARCHAR NOT NULL,
                config JSON,
                status VARCHAR NOT NULL DEFAULT 'active',
                start_date DATE NOT NULL,
                current_date DATE,
                initial_capital DOUBLE NOT NULL DEFAULT 1000000,
                current_nav DOUBLE,
                total_trades INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE paper_trading_daily (
                session_id VARCHAR NOT NULL,
                market VARCHAR NOT NULL DEFAULT 'US',
                date DATE NOT NULL,
                nav DOUBLE NOT NULL,
                cash DOUBLE NOT NULL,
                positions_json JSON,
                trades_json JSON,
                PRIMARY KEY (session_id, market, date)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE paper_trading_signal_cache (
                session_id VARCHAR NOT NULL,
                market VARCHAR NOT NULL DEFAULT 'US',
                signal_date DATE NOT NULL,
                result_json JSON NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (session_id, market, signal_date)
            )
            """
        )


class _MultiPatch:
    def __init__(self, *patches):
        self._patches = patches

    def __enter__(self):
        for item in self._patches:
            item.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        for item in reversed(self._patches):
            item.__exit__(exc_type, exc, tb)


class _FakeStore:
    def find_active_by_type_and_name(self, *args):
        return None


class _FakeExecutor:
    def __init__(self):
        self._store = _FakeStore()
        self.params = None

    def submit(self, task_type, fn, params, timeout, source=None):
        self.params = params
        return "task_cn"


class _FakeSignalService:
    def generate_signals(self, strategy_id, target_date, universe_group_id, market=None):
        return {"run_id": "run_cn", "market": market}


class _FakePaperService:
    def create_session(self, strategy_id, universe_group_id, start_date, name=None, config=None, market=None):
        return {"id": "session_cn", "market": market}

    def advance(self, session_id, target_date=None, steps=0, market=None):
        return {"session_id": session_id, "market": market}


class _FakePaperAdvanceStagingService:
    def __init__(self):
        self.stage_domain_write_seen = None

    def advance(self, session_id, target_date=None, steps=0, market=None, stage_domain_write=None):
        self.stage_domain_write_seen = stage_domain_write
        if callable(stage_domain_write):
            stage_domain_write(
                "paper_trading_advance",
                {"session_id": session_id, "market": market},
            )
        return {
            "session_id": session_id,
            "market": market,
            "staging": {"workflow": "paper_trading_advance"},
        }


class _InlineExecutor:
    def __init__(self):
        self.staged = []
        self.result = None

    def submit(self, task_type, fn, params, timeout, source=None):
        self.result = fn(
            **params,
            stage_domain_write=lambda table, payload=None, commit=None: self.staged.append(
                {"table": table, "payload": payload or {}, "has_commit": callable(commit)}
            ),
        )
        return "task_inline"


if __name__ == "__main__":
    unittest.main()
