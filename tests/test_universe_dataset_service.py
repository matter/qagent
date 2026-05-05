import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from backend.db import close_db, init_db
from backend.services.dataset_service import DatasetService
from backend.services.factor_service import FactorService
from backend.services.feature_service import FeatureService
from backend.services.label_service import LabelService
from backend.services.universe_service import UniverseService


_CLOSE_FACTOR_SOURCE = """\
import pandas as pd
from backend.factors.base import FactorBase


class DatasetCloseFactor(FactorBase):
    name = "DatasetCloseFactor"
    description = "close passthrough"

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return data["close"]
"""


class UniverseDatasetServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "dataset.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        self._seed_legacy_data()

    def test_universe_materialize_creates_pit_membership_and_artifact(self):
        universe = UniverseService().create_static_universe(
            name="Dataset smoke universe",
            tickers=["AAA", "BBB"],
            project_id="bootstrap_us",
            market_profile_id="US_EQ",
        )

        result = UniverseService().materialize_universe(
            universe["id"],
            start_date="2024-01-02",
            end_date="2024-01-05",
        )
        universes = UniverseService().list_universes(project_id="bootstrap_us")

        self.assertEqual(result["universe"]["id"], universe["id"])
        self.assertEqual(result["materialization"]["member_count"], 8)
        self.assertEqual(result["artifact"]["artifact_type"], "universe_materialization")
        self.assertEqual(result["run"]["run_type"], "universe_materialize")
        self.assertIn("coverage", result["profile"])
        self.assertIn(universe["id"], {item["id"] for item in universes})

    def test_dataset_materialize_profile_sample_and_query(self):
        factor = FactorService().create_factor(
            name="Dataset close factor",
            source_code=_CLOSE_FACTOR_SOURCE,
            market="US",
        )
        feature_set = FeatureService().create_feature_set(
            name="Dataset smoke features",
            factor_refs=[
                {
                    "factor_id": factor["id"],
                    "factor_name": "DatasetCloseFactor",
                    "version": factor["version"],
                }
            ],
            preprocessing={"missing": "forward_fill", "outlier": None, "normalize": None},
            market="US",
        )
        label = LabelService().create_label(
            name="Dataset fwd 1d",
            target_type="return",
            horizon=1,
            market="US",
        )
        universe = UniverseService().create_static_universe(
            name="Dataset sample universe",
            tickers=["AAA", "BBB"],
            project_id="bootstrap_us",
            market_profile_id="US_EQ",
        )

        dataset = DatasetService().create_dataset(
            name="Dataset smoke panel",
            universe_id=universe["id"],
            feature_set_id=feature_set["id"],
            label_id=label["id"],
            start_date="2024-01-02",
            end_date="2024-01-05",
            split_policy={
                "train": {"start": "2024-01-02", "end": "2024-01-03"},
                "valid": {"start": "2024-01-04", "end": "2024-01-04"},
                "test": {"start": "2024-01-05", "end": "2024-01-05"},
                "purge_gap": 1,
            },
        )
        materialized = DatasetService().materialize_dataset(dataset["id"])
        profile = DatasetService().profile_dataset(dataset["id"])
        sample = DatasetService().sample_dataset(dataset["id"], limit=3)
        query = DatasetService().query_dataset(
            dataset["id"],
            start_date="2024-01-03",
            end_date="2024-01-04",
            asset_ids=["US_EQ:AAA"],
        )
        datasets = DatasetService().list_datasets(project_id="bootstrap_us")

        self.assertEqual(materialized["run"]["run_type"], "dataset_materialize")
        self.assertEqual(materialized["artifact"]["artifact_type"], "dataset_panel")
        self.assertEqual(materialized["dataset"]["status"], "materialized")
        self.assertEqual(materialized["profile"]["coverage"]["row_count"], 8)
        self.assertEqual(profile["dataset_id"], dataset["id"])
        self.assertEqual(profile["feature_count"], 1)
        self.assertEqual(len(sample["rows"]), 3)
        self.assertEqual(len(query["rows"]), 2)
        self.assertEqual(query["rows"][0]["asset_id"], "US_EQ:AAA")
        self.assertIn(dataset["id"], {item["id"] for item in datasets})

    def test_dataset_qa_blocks_feature_label_date_overlap(self):
        factor = FactorService().create_factor(
            name="Dataset leak factor",
            source_code=_CLOSE_FACTOR_SOURCE,
            market="US",
        )
        feature_set = FeatureService().create_feature_set(
            name="Dataset leak features",
            factor_refs=[{"factor_id": factor["id"], "factor_name": "DatasetCloseFactor"}],
            preprocessing={"missing": "forward_fill", "outlier": None, "normalize": None},
            market="US",
        )
        label = LabelService().create_label(
            name="Dataset leak label",
            target_type="return",
            horizon=2,
            market="US",
        )
        universe = UniverseService().create_static_universe(
            name="Dataset leak universe",
            tickers=["AAA", "BBB"],
            project_id="bootstrap_us",
            market_profile_id="US_EQ",
        )
        dataset = DatasetService().create_dataset(
            name="Dataset leak panel",
            universe_id=universe["id"],
            feature_set_id=feature_set["id"],
            label_id=label["id"],
            start_date="2024-01-02",
            end_date="2024-01-05",
            split_policy={
                "train": {"start": "2024-01-02", "end": "2024-01-04"},
                "valid": {"start": "2024-01-05", "end": "2024-01-05"},
                "test": {"start": "2024-01-05", "end": "2024-01-05"},
                "purge_gap": 0,
            },
        )

        with self.assertRaisesRegex(ValueError, "purge_gap"):
            DatasetService().materialize_dataset(dataset["id"])

    def _seed_legacy_data(self) -> None:
        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute(
                """
                INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
                VALUES
                    ('US', 'AAA', 'AAA Corp', 'NYSE', 'Technology', 'active', current_timestamp),
                    ('US', 'BBB', 'BBB Corp', 'NASDAQ', 'Healthcare', 'active', current_timestamp)
                """
            )
            conn.execute(
                """
                INSERT INTO daily_bars
                    (market, ticker, date, open, high, low, close, volume, adj_factor)
                VALUES
                    ('US', 'AAA', DATE '2024-01-02', 10, 11, 9, 10, 1000, 1),
                    ('US', 'AAA', DATE '2023-12-29', 9, 10, 8, 9, 1000, 1),
                    ('US', 'AAA', DATE '2024-01-03', 10, 12, 9, 11, 1000, 1),
                    ('US', 'AAA', DATE '2024-01-04', 11, 13, 10, 12, 1000, 1),
                    ('US', 'AAA', DATE '2024-01-05', 12, 14, 11, 13, 1000, 1),
                    ('US', 'AAA', DATE '2024-01-08', 13, 15, 12, 14, 1000, 1),
                    ('US', 'BBB', DATE '2024-01-02', 20, 21, 19, 20, 1000, 1),
                    ('US', 'BBB', DATE '2023-12-29', 21, 22, 20, 21, 1000, 1),
                    ('US', 'BBB', DATE '2024-01-03', 20, 22, 19, 19, 1000, 1),
                    ('US', 'BBB', DATE '2024-01-04', 19, 20, 18, 18, 1000, 1),
                    ('US', 'BBB', DATE '2024-01-05', 18, 19, 17, 17, 1000, 1),
                    ('US', 'BBB', DATE '2024-01-08', 17, 18, 16, 16, 1000, 1)
                """
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
