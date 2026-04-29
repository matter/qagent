import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from backend.api import factors as factor_api
from backend.api import features as feature_api
from backend import mcp_server
from backend.services.factor_engine import FactorEngine
from backend.services.factor_eval_service import FactorEvalService
from backend.services.factor_service import FactorService
from backend.services.feature_service import FeatureService


_CLOSE_FACTOR_SOURCE = """\
import pandas as pd
from backend.factors.base import FactorBase


class CloseFactor(FactorBase):
    name = "CloseFactor"
    description = "close passthrough"
    category = "custom"

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return data["close"]
"""


class FactorFeatureMarketScopeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "factor_feature.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.addCleanup(self.conn.close)
        self._create_schema()

    def test_factor_engine_reads_and_writes_cache_by_market(self):
        self.conn.execute(
            """
            INSERT INTO daily_bars (market, ticker, date, open, high, low, close, volume)
            VALUES
                ('CN', 'sh.600000', DATE '2024-01-02', 1, 1, 1, 10, 100),
                ('CN', 'sh.600000', DATE '2024-01-03', 1, 1, 1, 11, 100),
                ('CN', 'sh.600000', DATE '2024-01-04', 1, 1, 1, 12, 100),
                ('CN', 'sh.600000', DATE '2024-01-05', 1, 1, 1, 13, 100),
                ('CN', 'sh.600000', DATE '2024-01-08', 1, 1, 1, 14, 100),
                ('US', 'sh.600000', DATE '2024-01-02', 1, 1, 1, 100, 100),
                ('US', 'sh.600000', DATE '2024-01-03', 1, 1, 1, 50, 100),
                ('US', 'sh.600000', DATE '2024-01-04', 1, 1, 1, 40, 100),
                ('US', 'sh.600000', DATE '2024-01-05', 1, 1, 1, 30, 100),
                ('US', 'sh.600000', DATE '2024-01-08', 1, 1, 1, 20, 100)
            """
        )

        with self._patch_connections():
            factor = FactorService().create_factor(
                "CN close",
                _CLOSE_FACTOR_SOURCE,
                market="CN",
            )
            result = FactorEngine().compute_factor(
                factor["id"],
                ["sh.600000"],
                "2024-01-02",
                "2024-01-08",
                market="CN",
            )

        self.assertEqual(result.loc["2024-01-02", "sh.600000"], 10)
        self.assertEqual(result.loc["2024-01-08", "sh.600000"], 14)
        self.assertEqual(
            self.conn.execute(
                "SELECT DISTINCT market FROM factor_values_cache WHERE factor_id = ?",
                [factor["id"]],
            ).fetchall(),
            [("CN",)],
        )

    def test_factor_list_defaults_to_us_and_filters_explicit_market(self):
        with self._patch_connections():
            us_factor = FactorService().create_factor(
                "US close",
                _CLOSE_FACTOR_SOURCE,
                market="US",
            )
            cn_factor = FactorService().create_factor(
                "CN close",
                _CLOSE_FACTOR_SOURCE,
                market="CN",
            )
            us_factors = FactorService().list_factors()
            cn_factors = FactorService().list_factors(market="CN")

        self.assertEqual([row["id"] for row in us_factors], [us_factor["id"]])
        self.assertEqual([row["id"] for row in cn_factors], [cn_factor["id"]])
        self.assertEqual(cn_factors[0]["market"], "CN")

    def test_feature_set_rejects_factor_from_other_market(self):
        with self._patch_connections():
            us_factor = FactorService().create_factor(
                "US close",
                _CLOSE_FACTOR_SOURCE,
                market="US",
            )
            cn_factor = FactorService().create_factor(
                "CN close",
                _CLOSE_FACTOR_SOURCE,
                market="CN",
            )

            with self.assertRaisesRegex(ValueError, "market"):
                FeatureService().create_feature_set(
                    "Bad CN feature set",
                    factor_refs=[{"factor_id": us_factor["id"], "factor_name": "close"}],
                    market="CN",
                )

            feature_set = FeatureService().create_feature_set(
                "CN feature set",
                factor_refs=[{"factor_id": cn_factor["id"], "factor_name": "close"}],
                market="CN",
            )

        self.assertEqual(feature_set["market"], "CN")

    def test_factor_eval_results_are_market_scoped(self):
        svc = FactorEvalService()

        with self._patch_connections():
            self.conn.execute(
                """
                INSERT INTO factors (id, market, name, version, source_code)
                VALUES
                    ('factor_us', 'US', 'US factor', 1, ?),
                    ('factor_cn', 'CN', 'CN factor', 1, ?)
                """,
                [_CLOSE_FACTOR_SOURCE, _CLOSE_FACTOR_SOURCE],
            )
            svc._save_result(
                eval_id="us_eval",
                factor_id="factor_us",
                label_id="label_us",
                universe_group_id="group_us",
                start_date="2024-01-02",
                end_date="2024-01-03",
                summary={"ic_mean": 0.1},
                ic_series=[],
                group_returns={},
                market="US",
            )
            svc._save_result(
                eval_id="cn_eval",
                factor_id="factor_cn",
                label_id="label_cn",
                universe_group_id="group_cn",
                start_date="2024-01-02",
                end_date="2024-01-03",
                summary={"ic_mean": 0.2},
                ic_series=[],
                group_returns={},
                market="CN",
            )
            cn_results = svc.list_all_evaluations(market="CN")

        self.assertEqual([row["id"] for row in cn_results], ["cn_eval"])
        self.assertEqual(cn_results[0]["market"], "CN")

    def test_factor_api_forwards_market_scope(self):
        fake_service = _FakeFactorService()
        fake_eval_service = _FakeFactorEvalService()

        with (
            patch.object(factor_api, "_service", fake_service),
            patch.object(factor_api, "_eval_service", fake_eval_service),
        ):
            created = _run_async(
                factor_api.create_factor(
                    factor_api.CreateFactorRequest(
                        name="CN API factor",
                        source_code=_CLOSE_FACTOR_SOURCE,
                        market="CN",
                    )
                )
            )
            listed = _run_async(factor_api.list_factors(market="CN"))
            detail = _run_async(factor_api.get_factor("factor_cn", market="CN"))
            evaluations = _run_async(factor_api.list_all_evaluations(market="CN"))

        self.assertEqual(created["market"], "CN")
        self.assertEqual(listed[0]["market"], "CN")
        self.assertEqual(detail["market"], "CN")
        self.assertEqual(evaluations[0]["market"], "CN")
        self.assertEqual(fake_service.calls, [("create", "CN"), ("list", "CN"), ("get", "CN")])
        self.assertEqual(fake_eval_service.calls, [("list_all", "CN")])

    def test_feature_api_forwards_market_scope(self):
        fake_service = _FakeFeatureService()

        with patch.object(feature_api, "_service", fake_service):
            created = _run_async(
                feature_api.create_feature_set(
                    feature_api.CreateFeatureSetRequest(
                        name="CN API feature set",
                        factor_refs=[{"factor_id": "factor_cn"}],
                        market="CN",
                    )
                )
            )
            listed = _run_async(feature_api.list_feature_sets(market="CN"))
            detail = _run_async(feature_api.get_feature_set("fs_cn", market="CN"))

        self.assertEqual(created["market"], "CN")
        self.assertEqual(listed[0]["market"], "CN")
        self.assertEqual(detail["market"], "CN")
        self.assertEqual(fake_service.calls, [("create", "CN"), ("list", "CN"), ("get", "CN")])

    def test_factor_mcp_tools_forward_market_scope(self):
        fake_service = _FakeFactorService()

        with patch.object(mcp_server, "_factor_service", return_value=fake_service):
            listed = mcp_server.list_factors(category="custom", market="CN")
            created = mcp_server.create_factor(
                name="CN MCP factor",
                description="close passthrough",
                category="custom",
                source_code=_CLOSE_FACTOR_SOURCE,
                market="CN",
            )

        self.assertEqual(listed[0]["market"], "CN")
        self.assertEqual(created["market"], "CN")
        self.assertEqual(fake_service.calls, [("list", "CN"), ("create", "CN")])

    def _patch_connections(self):
        return _MultiPatch(
            patch("backend.services.factor_service.get_connection", return_value=self.conn),
            patch("backend.services.factor_engine.get_connection", return_value=self.conn),
            patch("backend.services.feature_service.get_connection", return_value=self.conn),
            patch("backend.services.factor_eval_service.get_connection", return_value=self.conn),
        )

    def _create_schema(self):
        self.conn.execute(
            """
            CREATE TABLE factors (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                description TEXT,
                category VARCHAR DEFAULT 'custom',
                source_code TEXT NOT NULL,
                params JSON,
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(market, name, version)
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
            CREATE TABLE factor_values_cache (
                market VARCHAR NOT NULL DEFAULT 'US',
                factor_id VARCHAR NOT NULL,
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,
                value DOUBLE,
                PRIMARY KEY (market, factor_id, ticker, date)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE feature_sets (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                description TEXT,
                factor_refs JSON NOT NULL,
                preprocessing JSON NOT NULL,
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(market, name)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE factor_eval_results (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                factor_id VARCHAR NOT NULL,
                label_id VARCHAR NOT NULL,
                universe_group_id VARCHAR,
                start_date DATE,
                end_date DATE,
                summary JSON,
                ic_series JSON,
                group_returns JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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


def _run_async(coro):
    import asyncio

    return asyncio.run(coro)


class _FakeFactorService:
    def __init__(self):
        self.calls = []

    def ensure_builtin_templates(self, market=None):
        return None

    def create_factor(self, **kwargs):
        self.calls.append(("create", kwargs.get("market")))
        return {"id": "factor_cn", "market": kwargs.get("market")}

    def list_factors(self, **kwargs):
        self.calls.append(("list", kwargs.get("market")))
        return [{"id": "factor_cn", "market": kwargs.get("market")}]

    def get_factor(self, factor_id, market=None):
        self.calls.append(("get", market))
        return {"id": factor_id, "market": market}


class _FakeFactorEvalService:
    def __init__(self):
        self.calls = []

    def list_all_evaluations(self, market=None):
        self.calls.append(("list_all", market))
        return [{"id": "eval_cn", "market": market}]


class _FakeFeatureService:
    def __init__(self):
        self.calls = []

    def create_feature_set(self, **kwargs):
        self.calls.append(("create", kwargs.get("market")))
        return {"id": "fs_cn", "market": kwargs.get("market")}

    def list_feature_sets(self, market=None):
        self.calls.append(("list", market))
        return [{"id": "fs_cn", "market": market}]

    def get_feature_set(self, fs_id, market=None):
        self.calls.append(("get", market))
        return {"id": fs_id, "market": market}


if __name__ == "__main__":
    unittest.main()
