import tempfile
import threading
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.config import settings
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
        project_root_patcher = patch.object(settings, "project_root", Path(self._tmp.name))
        project_root_patcher.start()
        self.addCleanup(project_root_patcher.stop)
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

    def test_feature_matrix_default_hot_ttl_is_48_hours(self):
        service = ResearchCacheService()
        feature_data = {
            "close": pd.DataFrame(
                {"AAPL": [10.0]},
                index=pd.to_datetime(["2024-01-02"]),
            )
        }

        record = service.store_feature_matrix(
            market="US",
            feature_set_id="fs_hot_ttl",
            tickers=["AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={},
            feature_data=feature_data,
        )

        created_at = pd.Timestamp(record["created_at"])
        expires_at = pd.Timestamp(record["expires_at"])
        self.assertLess(abs((expires_at - created_at) - timedelta(hours=48)), timedelta(seconds=5))

    def test_label_values_cache_round_trips_frames_and_updates_stats(self):
        service = ResearchCacheService()
        labels = pd.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
                "label_value": [0.03, -0.01],
            }
        )
        label_definition = {
            "id": "label_5d",
            "target_type": "return",
            "horizon": 5,
            "effective_horizon": 5,
            "config": {"vol_adjust": False},
        }

        record = service.store_label_values(
            market="US",
            label_id="label_5d",
            tickers=["MSFT", "AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            label_definition=label_definition,
            label_values=labels,
        )
        loaded = service.load_label_values(
            market="US",
            label_id="label_5d",
            tickers=["AAPL", "MSFT"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            label_definition=label_definition,
        )

        self.assertIsNotNone(loaded)
        self.assertEqual(record["cache_key"], loaded["record"]["cache_key"])
        pd.testing.assert_frame_equal(
            loaded["label_values"].sort_values(["ticker", "date"]).reset_index(drop=True),
            labels.sort_values(["ticker", "date"]).reset_index(drop=True),
        )
        stats = service.get_cache_stats(record["cache_key"])
        self.assertEqual(stats["object_type"], "label_values")
        self.assertEqual(stats["hit_count"], 1)

    def test_label_values_cache_skips_expired_entries(self):
        service = ResearchCacheService()
        labels = pd.DataFrame(
            {
                "ticker": ["AAPL"],
                "date": pd.to_datetime(["2024-01-02"]),
                "label_value": [0.03],
            }
        )
        label_definition = {"id": "label_expired", "target_type": "return", "horizon": 5}
        record = service.store_label_values(
            market="US",
            label_id="label_expired",
            tickers=["AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            label_definition=label_definition,
            label_values=labels,
        )
        get_connection().execute(
            "UPDATE research_cache_entries SET expires_at = TIMESTAMP '2000-01-01' WHERE cache_key = ?",
            [record["cache_key"]],
        )

        loaded = service.load_label_values(
            market="US",
            label_id="label_expired",
            tickers=["AAPL"],
            start_date="2024-01-02",
            end_date="2024-01-02",
            label_definition=label_definition,
        )

        self.assertIsNone(loaded)

    def test_feature_matrix_store_retries_transient_duckdb_conflict(self):
        service = ResearchCacheService()
        feature_data = {
            "close": pd.DataFrame(
                {"AAPL": [10.0, 11.0], "MSFT": [20.0, 21.0]},
                index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
            )
        }
        conn = get_connection()
        attempts = {"count": 0}

        with patch(
            "backend.services.research_cache_service.get_connection",
            return_value=_FlakyResearchCacheConnection(conn, attempts),
        ):
            record = service.store_feature_matrix(
                market="US",
                feature_set_id="fs_retry",
                tickers=["AAPL", "MSFT"],
                start_date="2024-01-02",
                end_date="2024-01-03",
                factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
                preprocessing={},
                feature_data=feature_data,
            )

        self.assertEqual(attempts["count"], 1)
        self.assertEqual(record["object_id"], "fs_retry")

    def test_feature_matrix_store_serializes_same_cache_key_writes(self):
        service = ResearchCacheService()
        feature_data = {
            "close": pd.DataFrame(
                {"AAPL": [10.0, 11.0], "MSFT": [20.0, 21.0]},
                index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
            )
        }
        errors = []

        def write_cache():
            try:
                service.store_feature_matrix(
                    market="US",
                    feature_set_id="fs_lock",
                    tickers=["AAPL", "MSFT"],
                    start_date="2024-01-02",
                    end_date="2024-01-03",
                    factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
                    preprocessing={},
                    feature_data=feature_data,
                )
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write_cache) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        rows = get_connection().execute(
            "SELECT COUNT(*) FROM research_cache_entries WHERE object_id = 'fs_lock'"
        ).fetchone()[0]
        self.assertEqual(errors, [])
        self.assertEqual(rows, 1)

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

    def test_feature_matrix_cache_rejects_unversioned_latest_key(self):
        service = ResearchCacheService()

        with self.assertRaisesRegex(ValueError, "stable as_of_date"):
            service.default_data_version("US", None)

        self.assertEqual(
            service.default_data_version("CN", "2024-01-03"),
            "CN:asof:2024-01-03",
        )

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

    def test_preview_orphan_file_cleanup_finds_untracked_cache_files(self):
        service = ResearchCacheService()
        orphan = Path(self._tmp.name) / "data" / "research_cache" / "feature_matrix" / "US" / "fs_orphan" / "orphan.parquet"
        orphan.parent.mkdir(parents=True)
        orphan.write_bytes(b"orphan")

        preview = service.preview_orphan_file_cleanup(limit=10, min_age_seconds=0)

        self.assertEqual(preview["summary"]["candidate_count"], 1)
        self.assertEqual(preview["summary"]["candidate_bytes"], len(b"orphan"))
        self.assertEqual(preview["candidates"][0]["path"], str(orphan))

    def test_preview_orphan_file_cleanup_skips_fresh_untracked_cache_files(self):
        service = ResearchCacheService()
        fresh = Path(self._tmp.name) / "data" / "research_cache" / "feature_matrix" / "US" / "fs_orphan" / "fresh.parquet"
        fresh.parent.mkdir(parents=True)
        fresh.write_bytes(b"fresh")

        preview = service.preview_orphan_file_cleanup(limit=10, min_age_seconds=3600)

        self.assertEqual(preview["summary"]["candidate_count"], 0)
        self.assertEqual(preview["candidates"], [])

    def test_feature_service_uses_hot_feature_matrix_before_factor_bulk_load(self):
        conn = get_connection()
        ResearchCacheService.clear_process_feature_matrix_cache()
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

    def test_feature_service_reuses_process_feature_matrix_cache(self):
        conn = get_connection()
        ResearchCacheService.clear_process_feature_matrix_cache()
        conn.execute(
            """INSERT INTO feature_sets
               (id, market, name, factor_refs, preprocessing, status)
               VALUES ('fs_hot_process', 'US', 'Hot Process FS', ?, ?, 'active')""",
            [
                '[{"factor_id": "f_close", "factor_name": "close"}]',
                '{"normalize": null}',
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
            feature_set_id="fs_hot_process",
            tickers=["AAPL", "MSFT"],
            start_date="2024-01-03",
            end_date="2024-01-03",
            factor_refs=[{"factor_id": "f_close", "factor_name": "close"}],
            preprocessing={"normalize": None},
            feature_data=feature_data,
        )

        service = FeatureService(factor_engine=_ExplodingFactorEngine())
        first = service.compute_features_from_cache(
            "fs_hot_process",
            ["AAPL", "MSFT"],
            "2024-01-03",
            "2024-01-03",
            market="US",
        )
        with patch(
            "backend.services.research_cache_service.pd.read_parquet",
            side_effect=AssertionError("second load should use process cache"),
        ):
            second = service.compute_features_from_cache(
                "fs_hot_process",
                ["MSFT", "AAPL"],
                "2024-01-03",
                "2024-01-03",
                market="US",
            )

        pd.testing.assert_frame_equal(first["close"], second["close"])

    def test_feature_service_recomputes_partial_bulk_factor_cache(self):
        conn = get_connection()
        conn.execute(
            """INSERT INTO feature_sets
               (id, market, name, factor_refs, preprocessing, status)
               VALUES ('fs_partial', 'US', 'Partial FS', ?, ?, 'active')""",
            [
                '[{"factor_id": "f_close", "factor_name": "close"}]',
                '{}',
            ],
        )

        engine = _PartialBulkCacheFactorEngine()
        service = FeatureService(factor_engine=engine)
        result = service.compute_features_from_cache(
            "fs_partial",
            ["AAPL", "MSFT"],
            "2024-01-02",
            "2024-01-03",
            market="US",
        )

        self.assertEqual(engine.compute_calls, [("f_close", ("AAPL", "MSFT"))])
        self.assertEqual(list(result["close"].columns), ["AAPL", "MSFT"])
        self.assertEqual(result["close"].loc[pd.Timestamp("2024-01-03"), "MSFT"], 21.0)

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


class _PartialBulkCacheFactorEngine:
    def __init__(self):
        self.compute_calls = []

    def load_cached_factors_bulk(self, factor_ids, tickers, start_date, end_date, market=None):
        return {
            "f_close": pd.DataFrame(
                {"AAPL": [10.0, 11.0]},
                index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
            )
        }

    def compute_factor(self, factor_id, tickers, start_date, end_date, market=None):
        self.compute_calls.append((factor_id, tuple(tickers)))
        return pd.DataFrame(
            {"AAPL": [10.0, 11.0], "MSFT": [20.0, 21.0]},
            index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        )


class _FlakyResearchCacheConnection:
    def __init__(self, conn, attempts):
        self.conn = conn
        self.attempts = attempts

    def execute(self, sql, params=None):
        normalized = str(sql).lstrip().upper()
        if (
            normalized.startswith("INSERT OR REPLACE INTO RESEARCH_CACHE_ENTRIES")
            and self.attempts["count"] == 0
        ):
            self.attempts["count"] += 1
            raise RuntimeError("TransactionContext Error: Conflict on tuple deletion!")
        return self.conn.execute(sql, params)


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
