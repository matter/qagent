import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.dataset_service import DatasetService
from backend.services.factor_service import FactorService
from backend.services.feature_service import FeatureService
from backend.services.label_service import LabelService
from backend.services.model_experiment_3_service import ModelExperiment3Service
from backend.services.universe_service import UniverseService


_FACTOR_SOURCE = """\
import pandas as pd
from backend.factors.base import FactorBase


class Model3CloseMomentum(FactorBase):
    name = "Model3CloseMomentum"
    description = "one day close momentum"

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return data["close"].pct_change(1)
"""


class ModelExperiment3ServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "model3.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        self._seed_legacy_data()

    def test_train_promote_and_predict_from_materialized_dataset(self):
        dataset = self._create_materialized_dataset()
        service = ModelExperiment3Service()

        trained = service.train_experiment(
            name="Model3 smoke experiment",
            dataset_id=dataset["id"],
            model_type="lightgbm",
            objective="regression",
            model_params={"n_estimators": 8, "max_depth": 2, "random_state": 7},
            random_seed=7,
        )
        promoted = service.promote_experiment(
            trained["experiment"]["id"],
            package_name="Model3 smoke package",
            approved_by="unit-test",
            rationale="contract test promotion",
        )
        predicted = service.predict_panel(
            model_package_id=promoted["package"]["id"],
            dataset_id=dataset["id"],
        )

        self.assertEqual(trained["run"]["run_type"], "model_train_experiment")
        self.assertEqual(trained["experiment"]["status"], "completed")
        self.assertEqual(trained["model_artifact"]["artifact_type"], "model_file")
        self.assertEqual(trained["prediction_run"]["status"], "completed")
        self.assertEqual(trained["prediction_artifact"]["artifact_type"], "model_predictions")
        self.assertEqual(trained["experiment"]["dataset_id"], dataset["id"])
        self.assertIn("test_rmse", trained["metrics"])
        self.assertIn("feature_schema", trained)

        self.assertEqual(promoted["package"]["status"], "candidate")
        self.assertEqual(promoted["promotion_record"]["decision"], "promoted")
        self.assertEqual(promoted["package"]["source_experiment_id"], trained["experiment"]["id"])

        self.assertEqual(predicted["prediction_run"]["status"], "completed")
        self.assertEqual(predicted["prediction_artifact"]["artifact_type"], "model_predictions")
        self.assertGreater(predicted["profile"]["row_count"], 0)

        counts = get_connection().execute(
            """SELECT
                    (SELECT COUNT(*) FROM model_experiments),
                    (SELECT COUNT(*) FROM model_packages),
                    (SELECT COUNT(*) FROM promotion_records),
                    (SELECT COUNT(*) FROM prediction_runs)
            """
        ).fetchone()
        self.assertEqual(counts, (1, 1, 1, 2))

    def test_training_requires_materialized_dataset(self):
        dataset = self._create_dataset(materialize=False)

        with self.assertRaisesRegex(ValueError, "materialized"):
            ModelExperiment3Service().train_experiment(
                name="Model3 unmaterialized",
                dataset_id=dataset["id"],
                model_params={"n_estimators": 4},
            )

    def _create_materialized_dataset(self) -> dict:
        dataset = self._create_dataset(materialize=True)
        return dataset

    def _create_dataset(self, *, materialize: bool) -> dict:
        factor = FactorService().create_factor(
            name=f"Model3 factor {materialize}",
            source_code=_FACTOR_SOURCE,
            market="US",
        )
        feature_set = FeatureService().create_feature_set(
            name=f"Model3 features {materialize}",
            factor_refs=[
                {
                    "factor_id": factor["id"],
                    "factor_name": "Model3CloseMomentum",
                    "version": factor["version"],
                }
            ],
            preprocessing={"missing": "forward_fill", "outlier": None, "normalize": None},
            market="US",
        )
        label = LabelService().create_label(
            name=f"Model3 fwd 1d {materialize}",
            target_type="return",
            horizon=1,
            market="US",
        )
        universe = UniverseService().create_static_universe(
            name=f"Model3 universe {materialize}",
            tickers=["AAA", "BBB", "CCC", "DDD", "EEE"],
            project_id="bootstrap_us",
            market_profile_id="US_EQ",
        )
        dataset = DatasetService().create_dataset(
            name=f"Model3 dataset {materialize}",
            universe_id=universe["id"],
            feature_set_id=feature_set["id"],
            label_id=label["id"],
            start_date="2024-01-02",
            end_date="2024-01-12",
            split_policy={
                "train": {"start": "2024-01-02", "end": "2024-01-05"},
                "valid": {"start": "2024-01-08", "end": "2024-01-09"},
                "test": {"start": "2024-01-10", "end": "2024-01-12"},
                "purge_gap": 1,
            },
        )
        if materialize:
            DatasetService().materialize_dataset(dataset["id"])
            return DatasetService().get_dataset(dataset["id"])
        return dataset

    def _seed_legacy_data(self) -> None:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAA', 'AAA Corp', 'NYSE', 'Technology', 'active', current_timestamp),
                ('US', 'BBB', 'BBB Corp', 'NASDAQ', 'Healthcare', 'active', current_timestamp),
                ('US', 'CCC', 'CCC Corp', 'NYSE', 'Financials', 'active', current_timestamp),
                ('US', 'DDD', 'DDD Corp', 'NASDAQ', 'Industrials', 'active', current_timestamp),
                ('US', 'EEE', 'EEE Corp', 'NYSE', 'Consumer', 'active', current_timestamp)
            """
        )
        rows = []
        tickers = ["AAA", "BBB", "CCC", "DDD", "EEE"]
        dates = [
            "2023-12-29",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
            "2024-01-11",
            "2024-01-12",
            "2024-01-16",
        ]
        for ticker_index, ticker in enumerate(tickers):
            base = 20 + ticker_index * 5
            direction = 1 if ticker_index % 2 == 0 else -1
            for day_index, date in enumerate(dates):
                close = base + direction * day_index * (0.4 + ticker_index * 0.03)
                rows.append(
                    (
                        "US",
                        ticker,
                        date,
                        close - 0.2,
                        close + 0.4,
                        close - 0.6,
                        close,
                        1000 + ticker_index,
                        1.0,
                    )
                )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )


if __name__ == "__main__":
    unittest.main()
