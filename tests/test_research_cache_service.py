import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.db import close_db, get_connection, init_db
from backend.services.feature_service import FeatureService
from backend.services.research_cache_service import ResearchCacheService


class ResearchCacheServiceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "research_cache.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()

    def test_feature_matrix_cache_round_trips_frames_and_updates_stats(self):
        service = ResearchCacheService()
        feature_data = {
            "close": pd.DataFrame(
                {"AAPL": [10.0, 11.0], "MSFT": [20.0, 21.0]},
                index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
            ),
            "volume": pd.DataFrame(
                {"AAPL": [100.0, 110.0], "MSFT": [200.0, 210.0]},
                index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
            ),
        }

        record = service.store_feature_matrix(
            market="US",
            feature_set_id="fs_hot",
            tickers=["MSFT", "AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={"winsorize": False},
            feature_data=feature_data,
            retention_class="daily_hot",
        )
        loaded = service.load_feature_matrix(
            market="US",
            feature_set_id="fs_hot",
            tickers=["AAPL", "MSFT"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={"winsorize": False},
        )

        self.assertIsNotNone(loaded)
        self.assertEqual(record["cache_key"], loaded["record"]["cache_key"])
        pd.testing.assert_frame_equal(loaded["feature_data"]["close"], feature_data["close"])
        pd.testing.assert_frame_equal(loaded["feature_data"]["volume"], feature_data["volume"])

        stats = service.get_cache_stats(record["cache_key"])
        self.assertEqual(stats["hit_count"], 1)
        self.assertEqual(stats["miss_count"], 0)
        self.assertEqual(stats["object_type"], "feature_matrix")
        self.assertGreater(stats["byte_size"], 0)

    def test_feature_matrix_key_changes_with_universe_and_preprocessing(self):
        service = ResearchCacheService()
        base = service.build_feature_matrix_key(
            market="US",
            feature_set_id="fs_hot",
            tickers=["AAPL", "MSFT"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={"scale": "zscore"},
        )
        different_universe = service.build_feature_matrix_key(
            market="US",
            feature_set_id="fs_hot",
            tickers=["AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={"scale": "zscore"},
        )
        different_preprocessing = service.build_feature_matrix_key(
            market="US",
            feature_set_id="fs_hot",
            tickers=["AAPL", "MSFT"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={"scale": "rank"},
        )

        self.assertNotEqual(base, different_universe)
        self.assertNotEqual(base, different_preprocessing)

    def test_apply_expired_cache_cleanup_removes_files_and_marks_records(self):
        service = ResearchCacheService()
        feature_data = {
            "close": pd.DataFrame(
                {"AAPL": [10.0]},
                index=pd.to_datetime(["2024-01-02"]),
            )
        }
        record = service.store_feature_matrix(
            market="US",
            feature_set_id="fs_hot",
            tickers=["AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={},
            feature_data=feature_data,
            ttl_days=1,
        )
        path = Path(record["uri"])
        get_connection().execute(
            "UPDATE research_cache_entries SET expires_at = TIMESTAMP '2000-01-01' WHERE cache_key = ?",
            [record["cache_key"]],
        )

        applied = service.apply_expired_cache_cleanup(limit=10)
        updated = service.get_cache_stats(record["cache_key"])

        self.assertEqual(applied["deleted_entries"], 1)
        self.assertFalse(path.exists())
        self.assertEqual(updated["status"], "deleted")

    def test_feature_service_uses_hot_feature_matrix_before_factor_bulk_load(self):
        conn = get_connection()
        conn.execute(
            """INSERT INTO factors (id, market, name, version, source_code, status)
               VALUES ('f_close', 'US', 'Close', 1, ?, 'active')""",
            [_FACTOR_SOURCE],
        )
        conn.execute(
            """INSERT INTO feature_sets
               (id, market, name, factor_refs, preprocessing, status)
               VALUES ('fs_hot', 'US', 'Hot FS', ?, ?, 'active')""",
            [
                '[{"factor_id": "f_close", "factor_name": "close"}]',
                '{"winsorize": false}',
            ],
        )
        feature_data = {
            "close": pd.DataFrame(
                {"AAPL": [10.0], "MSFT": [20.0]},
                index=pd.to_datetime(["2024-01-03"]),
            )
        }
        ResearchCacheService().store_feature_matrix(
            market="US",
            feature_set_id="fs_hot",
            tickers=["AAPL", "MSFT"],
            start_date="2024-01-03",
            end_date="2024-01-03",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={"winsorize": False},
            feature_data=feature_data,
        )

        engine = _ExplodingFactorEngine()
        service = FeatureService(factor_engine=engine)
        result = service.compute_features_from_cache(
            "fs_hot",
            ["AAPL", "MSFT"],
            "2024-01-03",
            "2024-01-03",
            market="US",
        )

        self.assertEqual(engine.bulk_calls, 0)
        pd.testing.assert_frame_equal(result["close"], feature_data["close"])

    def test_preview_and_apply_draft_factor_cleanup_preserves_referenced_and_active(self):
        conn = get_connection()
        conn.execute(
            """INSERT INTO factors (id, market, name, version, source_code, status)
               VALUES
                   ('draft_unused', 'US', 'Unused draft', 1, ?, 'draft'),
                   ('draft_used', 'US', 'Used draft', 1, ?, 'draft'),
                   ('draft_model', 'US', 'Model draft', 1, ?, 'draft'),
                   ('draft_strategy', 'US', 'Strategy draft', 1, ?, 'draft'),
                   ('active_unused', 'US', 'Active unused', 1, ?, 'active')""",
            [_FACTOR_SOURCE, _FACTOR_SOURCE, _FACTOR_SOURCE, _FACTOR_SOURCE, _FACTOR_SOURCE],
        )
        conn.execute(
            """INSERT INTO feature_sets
               (id, market, name, factor_refs, preprocessing, status)
               VALUES
                   ('fs_ref', 'US', 'Referenced', ?, '{}', 'active'),
                   ('fs_model', 'US', 'Model FS', ?, '{}', 'draft')""",
            [
                '[{"factor_id": "draft_used", "factor_name": "used"}]',
                '[{"factor_id": "draft_model", "factor_name": "model"}]',
            ],
        )
        conn.execute(
            """INSERT INTO models
               (id, market, name, feature_set_id, label_id, model_type, status)
               VALUES ('model_ref', 'US', 'Model', 'fs_model', 'label', 'lightgbm', 'trained')"""
        )
        conn.execute(
            """INSERT INTO strategies
               (id, market, name, version, source_code, required_factors, required_models, status)
               VALUES ('strategy_ref', 'US', 'Strategy', 1, ?, ?, '[]', 'active')""",
            [_STRATEGY_SOURCE, '["Strategy draft"]'],
        )
        conn.execute(
            """INSERT INTO factor_values_cache (market, factor_id, ticker, date, value)
               VALUES
                   ('US', 'draft_unused', 'AAPL', DATE '2024-01-02', 1.0),
                   ('US', 'draft_used', 'AAPL', DATE '2024-01-02', 2.0),
                   ('US', 'draft_model', 'AAPL', DATE '2024-01-02', 4.0),
                   ('US', 'draft_strategy', 'AAPL', DATE '2024-01-02', 5.0),
                   ('US', 'active_unused', 'AAPL', DATE '2024-01-02', 3.0)"""
        )

        service = ResearchCacheService()
        preview = service.preview_factor_cache_cleanup(market="US")
        applied = service.apply_factor_cache_cleanup(market="US")
        remaining = conn.execute(
            "SELECT factor_id FROM factor_values_cache ORDER BY factor_id"
        ).fetchall()

        self.assertEqual(preview["summary"]["candidate_rows"], 1)
        self.assertEqual(preview["candidates"][0]["factor_id"], "draft_unused")
        self.assertEqual(applied["deleted_rows"], 1)
        self.assertEqual(
            remaining,
            [("active_unused",), ("draft_model",), ("draft_strategy",), ("draft_used",)],
        )


class _ExplodingFactorEngine:
    def __init__(self):
        self.bulk_calls = 0

    def load_cached_factors_bulk(self, *args, **kwargs):
        self.bulk_calls += 1
        raise AssertionError("hot feature matrix cache should be checked before bulk factor cache")


_FACTOR_SOURCE = """\
import pandas as pd
from backend.factors.base import FactorBase


class CloseFactor(FactorBase):
    name = "Close"
    description = "close passthrough"
    category = "custom"

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return data["close"]
"""

_STRATEGY_SOURCE = """\
import pandas as pd
from backend.strategies.base import StrategyBase


class Strategy(StrategyBase):
    name = "Strategy"

    def required_factors(self):
        return ["Strategy draft"]

    def generate_signals(self, data, factors, models=None):
        return pd.DataFrame()
"""


if __name__ == "__main__":
    unittest.main()
