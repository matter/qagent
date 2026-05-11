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
from fastapi import HTTPException


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
        self.assertEqual(metadata["feature_names"], ["close"])
        self.assertEqual(
            metadata["feature_lineage"]["trained"],
            [{"factor_id": "factor_close_id", "factor_name": "close"}],
        )
        self.assertEqual(
            metadata["feature_lineage"]["missing"],
            [{"factor_id": "factor_missing_id", "factor_name": "missing_factor"}],
        )

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
                        objective_type="ranking",
                        ranking_config={"query_group": "date", "min_group_size": 5},
                    )
                )
            )

        self.assertEqual(result["market"], "CN")
        self.assertEqual(executor.params["market"], "CN")
        self.assertEqual(executor.params["objective_type"], "ranking")
        self.assertEqual(executor.params["ranking_config"]["min_group_size"], 5)

    def test_distillation_api_generates_prediction_label_before_training(self):
        executor = _FakeExecutor()
        svc = _FakeDistillationService()

        with (
            patch.object(model_api, "_get_service", return_value=svc),
            patch.object(model_api, "_get_executor", return_value=executor),
        ):
            result = asyncio.run(
                model_api.train_model_distillation(
                    model_api.TrainDistillationRequest(
                        market="US",
                        name="Student model",
                        teacher_model_id="teacher_1",
                        student_feature_set_id="fs_student",
                        universe_group_id="sp500",
                        start_date="2025-01-02",
                        end_date="2025-12-31",
                        train_config={
                            "train_start": "2025-01-02",
                            "train_end": "2025-09-30",
                            "valid_start": "2025-10-01",
                            "valid_end": "2025-11-14",
                            "test_start": "2025-11-17",
                            "test_end": "2025-12-31",
                        },
                    )
                )
            )

        self.assertEqual(result["market"], "US")
        self.assertEqual(result["task_type"], "model_distillation_train")
        summary = executor.fn(**executor.params)
        self.assertEqual(svc.calls[0][0], "train_distilled")
        self.assertEqual(svc.calls[0][1]["teacher_model_id"], "teacher_1")
        self.assertEqual(svc.calls[0][1]["student_feature_set_id"], "fs_student")
        self.assertEqual(summary["distillation_label_id"], "distill_label_1")
        self.assertEqual(summary["model_id"], "student_model_1")

    def test_prediction_label_records_teacher_lineage_and_cutoff(self):
        svc = ModelService()
        svc._group_service = _FakeUsGroupService()
        svc._feature_service = _FakeFeatureService()
        svc._label_service = _FakeLabelService()
        svc.predict_batch = lambda **kwargs: {
            "2025-01-02": {"AAA": 0.7, "BBB": 0.3},
            "2025-01-03": {"AAA": 0.8},
        }

        with patch("backend.services.model_service.get_connection", return_value=self.conn):
            self.conn.execute(
                """
                INSERT INTO models
                    (id, market, name, feature_set_id, label_id, model_type, eval_metrics)
                VALUES
                    ('teacher_1', 'US', 'Teacher', 'fs_teacher', 'label_teacher', 'fake',
                     '{"task_type": "regression"}')
                """
            )
            self.conn.execute(
                """
                CREATE TABLE daily_bars (
                    market VARCHAR,
                    ticker VARCHAR,
                    date DATE,
                    close DOUBLE
                )
                """
            )
            self.conn.execute(
                """
                INSERT INTO daily_bars VALUES
                    ('US', 'AAA', DATE '2025-01-02', 10.0),
                    ('US', 'BBB', DATE '2025-01-02', 20.0),
                    ('US', 'AAA', DATE '2025-01-03', 11.0)
                """
            )
            label = svc.create_prediction_label_from_model(
                name="Teacher soft labels",
                teacher_model_id="teacher_1",
                universe_group_id="sp500",
                start_date="2025-01-02",
                end_date="2025-01-03",
                market="US",
            )
            with patch("backend.services.label_service.get_connection", return_value=self.conn):
                values = svc._label_service.compute_label_values(
                    label["id"],
                    ["AAA", "BBB"],
                    "2025-01-02",
                    "2025-01-03",
                    market="US",
                )

        self.assertEqual(label["target_type"], "prediction")
        self.assertEqual(label["horizon"], 0)
        self.assertEqual(label["config"]["teacher_model_id"], "teacher_1")
        self.assertEqual(label["config"]["cutoff_end_date"], "2025-01-03")
        self.assertEqual(label["config"]["source"], "model_prediction")
        self.assertEqual(label["config"]["storage"]["table"], "prediction_label_values")
        self.assertNotIn("values", label["config"])
        self.assertEqual(len(values), 3)
        self.assertEqual(
            values.sort_values(["date", "ticker"])["label_value"].round(3).tolist(),
            [0.7, 0.3, 0.8],
        )

    def test_model_predict_api_rejects_when_prediction_slot_is_full(self):
        acquired = []
        for _ in range(model_api._PREDICT_API_CONCURRENCY_LIMIT):
            if model_api._PREDICT_API_SEMAPHORE.acquire(blocking=False):
                acquired.append(True)
        self.addCleanup(
            lambda: [
                model_api._PREDICT_API_SEMAPHORE.release()
                for _ in acquired
            ]
        )

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                model_api.predict(
                    "model_busy",
                    model_api.PredictRequest(
                        market="US",
                        tickers=["AAPL"],
                        date="2026-01-02",
                    ),
                )
            )

        self.assertEqual(ctx.exception.status_code, 429)

    def test_model_predict_api_offloads_blocking_prediction_to_threadpool(self):
        class _FakePredictService:
            def __init__(self):
                self.inside_threadpool = False

            def predict_detailed(self, **kwargs):
                if not self.inside_threadpool:
                    raise AssertionError("prediction called directly on event loop")
                return pd.DataFrame({"prediction": [0.42]}, index=["AAPL"])

        svc = _FakePredictService()

        async def fake_run_in_threadpool(func, *args, **kwargs):
            svc.inside_threadpool = True
            try:
                return func(*args, **kwargs)
            finally:
                svc.inside_threadpool = False

        with (
            patch.object(model_api, "_get_service", return_value=svc),
            patch.object(
                model_api,
                "run_in_threadpool",
                side_effect=fake_run_in_threadpool,
                create=True,
            ) as offload,
        ):
            result = asyncio.run(
                model_api.predict(
                    "model_ok",
                    model_api.PredictRequest(
                        market="US",
                        tickers=["AAPL"],
                        date="2026-01-02",
                    ),
                )
            )

        self.assertTrue(offload.called)
        self.assertEqual(result["predictions"], {"AAPL": 0.42})

    def test_model_predict_batch_api_offloads_and_uses_same_concurrency_gate(self):
        class _FakePredictService:
            def __init__(self):
                self.inside_threadpool = False

            def predict_batch(self, **kwargs):
                if not self.inside_threadpool:
                    raise AssertionError("batch prediction called directly on event loop")
                return {"2026-01-02": {"AAPL": 0.42}}

        svc = _FakePredictService()

        async def fake_run_in_threadpool(func, *args, **kwargs):
            svc.inside_threadpool = True
            try:
                return func(*args, **kwargs)
            finally:
                svc.inside_threadpool = False

        with (
            patch.object(model_api, "_get_service", return_value=svc),
            patch.object(
                model_api,
                "run_in_threadpool",
                side_effect=fake_run_in_threadpool,
            ) as offload,
        ):
            result = asyncio.run(
                model_api.predict_batch(
                    "model_ok",
                    model_api.PredictBatchRequest(
                        market="US",
                        tickers=["AAPL"],
                        dates=["2026-01-02"],
                    ),
                )
            )

        self.assertTrue(offload.called)
        self.assertEqual(result["predictions"], {"2026-01-02": {"AAPL": 0.42}})

        acquired = []
        for _ in range(model_api._PREDICT_API_CONCURRENCY_LIMIT):
            if model_api._PREDICT_API_SEMAPHORE.acquire(blocking=False):
                acquired.append(True)
        self.addCleanup(
            lambda: [
                model_api._PREDICT_API_SEMAPHORE.release()
                for _ in acquired
            ]
        )

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                model_api.predict_batch(
                    "model_busy",
                    model_api.PredictBatchRequest(
                        market="US",
                        tickers=["AAPL"],
                        dates=["2026-01-02"],
                    ),
                )
            )

        self.assertEqual(ctx.exception.status_code, 429)

    def test_pairwise_objective_uses_ranking_groups_and_metadata(self):
        _FakeModel.last_task = None
        _FakeModel.last_fit_kwargs = None
        _FakeModel.last_params = None
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
                name="CN pairwise model",
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
                objective_type="pairwise",
                ranking_config={"query_group": "date", "min_group_size": 2, "eval_at": [1]},
            )

        self.assertEqual(_FakeModel.last_task, "ranking")
        self.assertEqual(_FakeModel.last_fit_kwargs["group"], [2, 2, 2])
        self.assertEqual(summary["task"], "ranking")
        self.assertEqual(summary["objective_type"], "pairwise")
        self.assertEqual(summary["eval_metrics"]["pairwise_mode"], "lambdarank")
        self.assertIn("test_ndcg@1", summary["eval_metrics"])

    def test_ranking_training_injects_safe_lightgbm_label_gain_length(self):
        _FakeModel.last_params = None
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        svc._label_service = _FakeLabelService()
        svc._group_service = _FakeGroupService()

        with (
            patch("backend.services.model_service.get_connection", return_value=self.conn),
            patch.dict(model_service_module._MODEL_REGISTRY, {"lightgbm": _FakeModel}),
            patch.object(model_service_module, "settings", _FakeSettings(self.models_dir)),
        ):
            summary = svc.train_model(
                name="US ranker",
                feature_set_id="fs_us",
                label_id="label_us",
                model_type="lightgbm",
                model_params={"n_estimators": 3},
                train_config={
                    "train_start": "2024-01-02",
                    "train_end": "2024-01-04",
                    "valid_start": "2024-01-05",
                    "valid_end": "2024-01-08",
                    "test_start": "2024-01-09",
                    "test_end": "2024-01-10",
                    "purge_gap": 0,
                },
                universe_group_id="us_group",
                market="US",
                objective_type="ranking",
                ranking_config={"query_group": "date", "min_group_size": 2},
            )

        self.assertEqual(_FakeModel.last_params["label_gain"], [0, 1])
        ranking_groups = summary["eval_metrics"]["ranking_groups"]
        self.assertEqual(ranking_groups["max_label_gain"], 1)
        self.assertEqual(ranking_groups["lightgbm_label_gain_length"], 2)
        self.assertEqual(ranking_groups["lightgbm_label_gain_source"], "generated")

    def test_ranking_training_rejects_short_lightgbm_label_gain_before_fit(self):
        _FakeModel.last_fit_kwargs = None
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        svc._label_service = _FakeLabelService()
        svc._group_service = _FakeGroupService()

        with (
            patch("backend.services.model_service.get_connection", return_value=self.conn),
            patch.dict(model_service_module._MODEL_REGISTRY, {"lightgbm": _FakeModel}),
            patch.object(model_service_module, "settings", _FakeSettings(self.models_dir)),
        ):
            with self.assertRaisesRegex(ValueError, "label_gain length must be > max ordinal gain"):
                svc.train_model(
                    name="US ranker bad gain",
                    feature_set_id="fs_us",
                    label_id="label_us",
                    model_type="lightgbm",
                    model_params={"label_gain": [0]},
                    train_config={
                        "train_start": "2024-01-02",
                        "train_end": "2024-01-04",
                        "valid_start": "2024-01-05",
                        "valid_end": "2024-01-08",
                        "test_start": "2024-01-09",
                        "test_end": "2024-01-10",
                        "purge_gap": 0,
                    },
                    universe_group_id="us_group",
                    market="US",
                    objective_type="ranking",
                    ranking_config={"query_group": "date", "min_group_size": 2},
                )

        self.assertIsNone(_FakeModel.last_fit_kwargs)

    def test_lightgbm_listwise_training_saves_ranking_task_type(self):
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        svc._label_service = _FakeLabelService()
        svc._group_service = _FakeGroupService()

        with (
            patch("backend.services.model_service.get_connection", return_value=self.conn),
            patch.object(model_service_module, "settings", _FakeSettings(self.models_dir)),
        ):
            summary = svc.train_model(
                name="CN listwise model",
                feature_set_id="fs_cn",
                label_id="label_cn",
                model_type="lightgbm",
                model_params={"n_estimators": 3, "min_child_samples": 1, "num_leaves": 3},
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
                objective_type="listwise",
                ranking_config={"query_group": "date", "min_group_size": 2, "eval_at": [1]},
            )

        self.assertEqual(summary["task"], "ranking")
        self.assertEqual(summary["eval_metrics"]["task_type"], "ranking")
        self.assertEqual(summary["eval_metrics"]["objective_type"], "listwise")
        saved_metrics = self.conn.execute(
            "SELECT eval_metrics FROM models WHERE id = ?",
            [summary["model_id"]],
        ).fetchone()[0]
        if isinstance(saved_metrics, str):
            saved_metrics = json.loads(saved_metrics)
        self.assertEqual(saved_metrics["task_type"], "ranking")

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
        self.conn.execute(
            """
            CREATE TABLE prediction_label_values (
                market VARCHAR NOT NULL DEFAULT 'US',
                label_id VARCHAR NOT NULL,
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,
                label_value DOUBLE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (market, label_id, ticker, date)
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
            "factor_refs": [
                {"factor_id": "factor_close_id", "factor_name": "close"},
                {"factor_id": "factor_missing_id", "factor_name": "missing_factor"},
            ],
        }


class _FakeLabelService:
    def __init__(self):
        self.calls = []
        self.labels = {}

    def get_label(self, label_id, market=None):
        self.calls.append(("get", label_id, market))
        if label_id in self.labels:
            return self.labels[label_id]
        return {
            "id": label_id,
            "market": market,
            "target_type": "return",
            "horizon": 1,
            "config": {},
        }

    def create_label(
        self,
        name,
        description=None,
        target_type="return",
        horizon=5,
        benchmark=None,
        config=None,
        market=None,
    ):
        self.calls.append(("create", name, market))
        label = {
            "id": "distill_label_created",
            "market": market,
            "name": name,
            "description": description,
            "target_type": target_type,
            "horizon": horizon,
            "benchmark": benchmark,
            "config": config or {},
            "status": "draft",
        }
        self.labels[label["id"]] = label
        return label

    def compute_label_values(self, label_id, tickers, start_date, end_date, market=None):
        self.calls.append(("compute", label_id, market))
        label = self.labels.get(label_id)
        if label and label["target_type"] == "prediction":
            from backend.services.label_service import LabelService

            return LabelService._compute_prediction_label_values(
                label,
                tickers,
                start_date,
                end_date,
            )
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


class _FakeUsGroupService:
    def __init__(self):
        self.calls = []

    def get_group_tickers(self, group_id, market=None):
        self.calls.append(("tickers", group_id, market))
        return ["AAA", "BBB"]


class _FakeModel(ModelBase):
    last_task = None
    last_fit_kwargs = None
    last_params = None

    def __init__(self, task="regression", params=None):
        _FakeModel.last_task = task
        _FakeModel.last_params = dict(params or {})
        self.task = task
        self.params = params or {}
        self._feature_names = []

    def fit(self, X, y, **kwargs):
        _FakeModel.last_fit_kwargs = kwargs
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
        self.fn = None
        self.task_type = None

    def submit(self, task_type, fn, params, timeout, source):
        self.task_type = task_type
        self.fn = fn
        self.params = params
        return "task_model_cn"


class _FakeSettings:
    def __init__(self, models_dir):
        self.models_dir = models_dir


class _FakeDistillationService:
    def __init__(self):
        self.calls = []

    def train_distilled_model(self, **kwargs):
        self.calls.append(("train_distilled", kwargs))
        return {
            "model_id": "student_model_1",
            "market": kwargs.get("market"),
            "distillation_label_id": "distill_label_1",
            "distillation": {
                "teacher_model_id": kwargs.get("teacher_model_id"),
                "prediction_label_id": "distill_label_1",
                "cutoff_end_date": kwargs.get("end_date"),
            },
        }

    def create_prediction_label_from_model(self, **kwargs):
        self.calls.append(("distill", kwargs))
        return {
            "id": "distill_label_1",
            "market": kwargs.get("market"),
            "config": {
                "teacher_model_id": kwargs.get("teacher_model_id"),
                "cutoff_end_date": kwargs.get("end_date"),
            },
        }

    def train_model(self, **kwargs):
        self.calls.append(("train", kwargs))
        return {
            "model_id": "student_model_1",
            "market": kwargs.get("market"),
            "eval_metrics": {"time_overlap": False},
        }


if __name__ == "__main__":
    unittest.main()
