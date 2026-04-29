import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.api import models as model_api
from backend.models.base import ModelBase
from backend.services import model_service as model_service_module
from backend.services.model_service import ModelService


class ModelMarketScopeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_path = Path(self._tmp.name)
        self.db_path = self.tmp_path / "models.duckdb"
        self.models_dir = self.tmp_path / "models"
        self.models_dir.mkdir()
        self.conn = duckdb.connect(str(self.db_path))
        self.addCleanup(self.conn.close)
        self._create_schema()

    def test_train_model_persists_market_and_uses_market_scoped_dependencies(self):
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        svc._label_service = _FakeLabelService()
        svc._group_service = _FakeGroupService()

        with (
            patch("backend.services.model_service.get_connection", return_value=self.conn),
            patch.dict(model_service_module._MODEL_REGISTRY, {"fake": _FakeModel}),
            patch.object(model_service_module, "settings", _FakeSettings(self.models_dir)),
        ):
            summary = svc.train_model(
                name="CN model",
                feature_set_id="fs_cn",
                label_id="label_cn",
                model_type="fake",
                train_config={
                    "train_start": "2024-01-02",
                    "train_end": "2024-01-04",
                    "valid_start": "2024-01-05",
                    "valid_end": "2024-01-08",
                    "test_start": "2024-01-09",
                    "test_end": "2024-01-10",
                    "purge_gap": 0,
                },
                universe_group_id="cn_all_a",
                market="CN",
            )

        self.assertEqual(summary["market"], "CN")
        self.assertEqual(svc._group_service.calls, [("tickers", "cn_all_a", "CN")])
        self.assertEqual(
            svc._feature_service.calls,
            [
                ("compute", "fs_cn", "CN"),
                ("get", "fs_cn", "CN"),
            ],
        )
        self.assertEqual(
            svc._label_service.calls,
            [
                ("get", "label_cn", "CN"),
                ("compute", "label_cn", "CN"),
            ],
        )

        db_rows = self.conn.execute(
            "SELECT market, id FROM models WHERE id = ?",
            [summary["model_id"]],
        ).fetchall()
        self.assertEqual(db_rows, [("CN", summary["model_id"])])

        metadata = json.loads((self.models_dir / summary["model_id"] / "metadata.json").read_text())
        self.assertEqual(metadata["market"], "CN")

        with patch("backend.services.model_service.get_connection", return_value=self.conn):
            self.assertEqual(ModelService().list_models("CN")[0]["market"], "CN")
            self.assertEqual(ModelService().list_models(), [])

    def test_model_api_forwards_market_to_training_task(self):
        executor = _FakeExecutor()

        with patch.object(model_api, "_get_executor", return_value=executor):
            result = asyncio.run(
                model_api.train_model(
                    model_api.TrainModelRequest(
                        market="CN",
                        name="CN API model",
                        feature_set_id="fs_cn",
                        label_id="label_cn",
                        model_type="lightgbm",
                        train_config={},
                        universe_group_id="cn_all_a",
                    )
                )
            )

        self.assertEqual(result["market"], "CN")
        self.assertEqual(executor.params["market"], "CN")

    def _create_schema(self):
        self.conn.execute(
            """
            CREATE TABLE models (
                id VARCHAR PRIMARY KEY,
                market VARCHAR NOT NULL DEFAULT 'US',
                name VARCHAR NOT NULL,
                feature_set_id VARCHAR NOT NULL,
                label_id VARCHAR NOT NULL,
                model_type VARCHAR NOT NULL DEFAULT 'lightgbm',
                model_params JSON,
                train_config JSON,
                eval_metrics JSON,
                status VARCHAR DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


class _FakeFeatureService:
    def __init__(self):
        self.calls = []

    def compute_features(self, feature_set_id, tickers, start_date, end_date, market=None):
        self.calls.append(("compute", feature_set_id, market))
        dates = pd.to_datetime(
            [
                "2024-01-02",
                "2024-01-03",
                "2024-01-04",
                "2024-01-05",
                "2024-01-08",
                "2024-01-09",
                "2024-01-10",
            ]
        )
        return {
            "close": pd.DataFrame(
                {
                    "sh.600000": [1, 2, 3, 4, 5, 6, 7],
                    "sh.600001": [2, 3, 4, 5, 6, 7, 8],
                },
                index=dates,
            )
        }

    def get_feature_set(self, feature_set_id, market=None):
        self.calls.append(("get", feature_set_id, market))
        return {
            "id": feature_set_id,
            "market": market,
            "factor_refs": [{"factor_id": "close"}],
        }


class _FakeLabelService:
    def __init__(self):
        self.calls = []

    def get_label(self, label_id, market=None):
        self.calls.append(("get", label_id, market))
        return {
            "id": label_id,
            "market": market,
            "target_type": "return",
            "horizon": 1,
            "config": {},
        }

    def compute_label_values(self, label_id, tickers, start_date, end_date, market=None):
        self.calls.append(("compute", label_id, market))
        rows = []
        dates = pd.to_datetime(
            [
                "2024-01-02",
                "2024-01-03",
                "2024-01-04",
                "2024-01-05",
                "2024-01-08",
                "2024-01-09",
                "2024-01-10",
            ]
        )
        for i, dt in enumerate(dates):
            rows.append({"date": dt, "ticker": "sh.600000", "label_value": float(i)})
            rows.append({"date": dt, "ticker": "sh.600001", "label_value": float(i + 1)})
        return pd.DataFrame(rows)


class _FakeGroupService:
    def __init__(self):
        self.calls = []

    def get_group_tickers(self, group_id, market=None):
        self.calls.append(("tickers", group_id, market))
        return ["sh.600000", "sh.600001"]


class _FakeModel(ModelBase):
    def __init__(self, task="regression", params=None):
        self.task = task
        self.params = params or {}
        self._feature_names = []

    def fit(self, X, y, **kwargs):
        self._feature_names = list(X.columns)
        return self

    def predict(self, X):
        return pd.Series(range(len(X)), index=X.index, dtype=float, name="prediction")

    def get_params(self):
        return {"task": self.task, **self.params}

    def feature_importance(self):
        return pd.Series(1.0, index=self._feature_names)


class _FakeStore:
    def find_active_by_type_and_name(self, *args):
        return None


class _FakeExecutor:
    def __init__(self):
        self._store = _FakeStore()
        self.params = None

    def submit(self, task_type, fn, params, timeout, source):
        self.params = params
        return "task_model_cn"


class _FakeSettings:
    def __init__(self, models_dir):
        self.models_dir = models_dir


if __name__ == "__main__":
    unittest.main()
