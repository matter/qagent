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
                ("compute_from_cache", "fs_cn", "CN"),
                ("get", "fs_cn", "CN"),
            ],
        )
        self.assertEqual(
            svc._label_service.calls,
            [
                ("get", "label_cn", "CN"),
                ("compute_cached", "label_cn", "CN"),
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
        self.assertEqual(metadata["runtime_profile"]["feature_loader"], "compute_features_from_cache")
        self.assertGreaterEqual(metadata["runtime_profile"]["feature_seconds"], 0)
        self.assertGreaterEqual(metadata["runtime_profile"]["fit_seconds"], 0)
        self.assertEqual(summary["runtime_profile"]["feature_loader"], "compute_features_from_cache")
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

    def test_list_models_respects_limit_and_offset(self):
        conn = self.conn
        for idx in range(3):
            conn.execute(
                """INSERT INTO models
                   (id, market, name, feature_set_id, label_id, model_type,
                    model_params, train_config, eval_metrics, status, created_at, updated_at)
                   VALUES (?, 'US', ?, 'fs', 'label', 'lightgbm',
                           '{}', '{}', '{}', 'trained', ?, ?)""",
                [
                    f"model_{idx}",
                    f"Model {idx}",
                    f"2024-01-0{idx + 1} 00:00:00",
                    f"2024-01-0{idx + 1} 00:00:00",
                ],
            )

        with patch("backend.services.model_service.get_connection", return_value=conn):
            models = ModelService().list_models("US", limit=1, offset=1)

        self.assertEqual([model["id"] for model in models], ["model_1"])

    def test_list_models_omits_heavy_audit_fields_by_default(self):
        conn = self.conn
        conn.execute(
            """INSERT INTO models
               (id, market, name, feature_set_id, label_id, model_type,
                model_params, train_config, eval_metrics, status, created_at, updated_at)
               VALUES ('model_heavy', 'US', 'Heavy', 'fs', 'label', 'lightgbm',
                       '{}', '{"train_start":"2024-01-01"}',
                       '{"ic_mean":0.1,"feature_importance":{"close":1.0}}',
                       'trained', '2024-01-01 00:00:00', '2024-01-01 00:00:00')"""
        )

        with patch("backend.services.model_service.get_connection", return_value=conn):
            summary = ModelService().list_models("US")[0]
            detail = ModelService().get_model("model_heavy", market="US")

        self.assertNotIn("metrics", summary)
        self.assertNotIn("metadata", summary)
        self.assertIn("metrics", detail)
        self.assertIn("metadata", detail)

    def test_train_model_reports_coarse_progress_phases(self):
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        svc._label_service = _FakeLabelService()
        svc._group_service = _FakeGroupService()
        progress_events = []

        with (
            patch("backend.services.model_service.get_connection", return_value=self.conn),
            patch.dict(model_service_module._MODEL_REGISTRY, {"fake": _FakeModel}),
            patch.object(model_service_module, "settings", _FakeSettings(self.models_dir)),
        ):
            svc.train_model(
                name="CN model progress",
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
                progress=lambda phase, **payload: progress_events.append((phase, payload)),
            )

        phases = [phase for phase, _payload in progress_events]
        self.assertIn("feature_load", phases)
        self.assertIn("label_load", phases)
        self.assertIn("fit", phases)
        self.assertIn("persist", phases)

    def test_train_model_persists_effective_composite_label_horizon(self):
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        svc._label_service = _FakeLabelService()
        svc._group_service = _FakeGroupService()
        svc._label_service.labels["label_composite"] = {
            "id": "label_composite",
            "market": "CN",
            "target_type": "composite",
            "horizon": 10,
            "effective_horizon": 20,
            "config": {
                "components": [
                    {"label_id": "inner_20d", "weight": 1.0},
                ],
            },
        }

        with (
            patch("backend.services.model_service.get_connection", return_value=self.conn),
            patch.dict(model_service_module._MODEL_REGISTRY, {"fake": _FakeModel}),
            patch.object(model_service_module, "settings", _FakeSettings(self.models_dir)),
        ):
            summary = svc.train_model(
                name="CN composite model",
                feature_set_id="fs_cn",
                label_id="label_composite",
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

        metadata = json.loads((self.models_dir / summary["model_id"] / "metadata.json").read_text())
        self.assertEqual(summary["label_summary"]["horizon"], 10)
        self.assertEqual(summary["label_summary"]["effective_horizon"], 20)
        self.assertEqual(summary["eval_metrics"]["label_horizon"], 20)
        self.assertEqual(summary["eval_metrics"]["effective_label_horizon"], 20)
        self.assertEqual(metadata["label_horizon"], 20)
        self.assertEqual(metadata["effective_label_horizon"], 20)

    def test_model_detail_exposes_top_level_audit_metadata(self):
        model_id = "audit_model"
        self.conn.execute(
            """INSERT INTO models
               (id, market, name, feature_set_id, label_id, model_type,
                model_params, train_config, eval_metrics, status)
               VALUES (?, 'US', 'Audit model', 'fs_audit', 'preset_fwd_rank_20d',
                       'lightgbm', '{}', ?, ?, 'trained')""",
            [
                model_id,
                json.dumps(
                    {
                        "train_start": "2025-01-02",
                        "train_end": "2025-09-30",
                        "valid_start": "2025-10-01",
                        "valid_end": "2025-11-28",
                        "test_start": "2025-12-01",
                        "test_end": "2025-12-31",
                        "purge_gap": 5,
                    }
                ),
                json.dumps(
                    {
                        "label_summary": {
                            "label_id": "preset_fwd_rank_20d",
                            "horizon": 20,
                            "effective_horizon": 20,
                        },
                        "test_rank_ic": 0.12,
                    }
                ),
            ],
        )

        with patch("backend.services.model_service.get_connection", return_value=self.conn):
            detail = ModelService().get_model(model_id, market="US")

        self.assertEqual(detail["train_start"], "2025-01-02")
        self.assertEqual(detail["train_end"], "2025-09-30")
        self.assertEqual(detail["test_start"], "2025-12-01")
        self.assertEqual(detail["test_end"], "2025-12-31")
        self.assertEqual(detail["purge_gap"], 5)
        self.assertEqual(detail["metrics"]["test_rank_ic"], 0.12)
        self.assertEqual(detail["label_horizon"], 20)
        self.assertEqual(detail["effective_label_horizon"], 20)
        self.assertEqual(detail["metadata"]["feature_data_end"], "2025-12-31")
        self.assertEqual(detail["metadata"]["label_data_end"], "2026-01-30")
        self.assertEqual(
            detail["metadata"]["audit"]["cutoff_rule"],
            "label_data_end < backtest_start",
        )

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

    def test_model_predict_batch_api_can_submit_async_task(self):
        class _FakeExecutor:
            def __init__(self):
                self.submission = None

            def submit(self, task_type, fn, params, timeout, source):
                self.submission = {
                    "task_type": task_type,
                    "fn": fn,
                    "params": params,
                    "timeout": timeout,
                    "source": source,
                }
                return "task_predict_batch"

        class _FakePredictService:
            def predict_batch(self, **kwargs):
                return {"2026-01-02": {"AAPL": 0.42}}

        executor = _FakeExecutor()
        svc = _FakePredictService()
        with (
            patch.object(model_api, "_get_service", return_value=svc),
            patch.object(model_api, "_get_executor", return_value=executor),
        ):
            result = asyncio.run(
                model_api.predict_batch(
                    "model_async",
                    model_api.PredictBatchRequest(
                        market="US",
                        tickers=["AAPL"],
                        dates=["2026-01-02"],
                        async_mode=True,
                    ),
                )
            )

        self.assertEqual(result["task_id"], "task_predict_batch")
        self.assertEqual(result["task_type"], "model_predict_batch")
        self.assertEqual(executor.submission["task_type"], "model_predict_batch")
        summary = executor.submission["fn"](**executor.submission["params"])
        self.assertEqual(summary["total_predictions"], 1)
        self.assertEqual(summary["predictions"], {"2026-01-02": {"AAPL": 0.42}})

    def test_predict_batch_runs_model_once_for_all_dates(self):
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        model = _CountingPredictModel()
        svc.get_model = lambda model_id, market=None: {
            "id": model_id,
            "market": "CN",
            "feature_set_id": "fs_cn",
            "task_type": "regression",
        }
        svc.load_model = lambda model_id, market=None: model
        svc._load_frozen_features = lambda model_id: ["close"]

        result = svc.predict_batch(
            "model_cn",
            tickers=["sh.600000", "sh.600001"],
            dates=["2024-01-02", "2024-01-03", "2024-01-04"],
            market="CN",
        )

        self.assertEqual(model.predict_calls, 1)
        self.assertEqual(set(result), {"2024-01-02", "2024-01-03", "2024-01-04"})
        self.assertEqual(set(result["2024-01-02"]), {"sh.600000", "sh.600001"})
        self.assertEqual(set(result["2024-01-03"]), {"sh.600000", "sh.600001"})
        self.assertEqual(set(result["2024-01-04"]), {"sh.600000", "sh.600001"})

    def test_predict_batch_reuses_short_lived_result_cache(self):
        svc = ModelService()
        svc._feature_service = _FakeFeatureService()
        model = _CountingPredictModel()
        svc.get_model = lambda model_id, market=None: {
            "id": model_id,
            "market": "CN",
            "feature_set_id": "fs_cn",
            "task_type": "regression",
        }
        svc.load_model = lambda model_id, market=None: model
        svc._load_frozen_features = lambda model_id: ["close"]

        first = svc.predict_batch(
            "model_cn",
            tickers=["sh.600000", "sh.600001"],
            dates=["2024-01-02", "2024-01-03"],
            market="CN",
        )
        second = svc.predict_batch(
            "model_cn",
            tickers=["sh.600001", "sh.600000"],
            dates=["2024-01-03", "2024-01-02"],
            market="CN",
        )

        self.assertEqual(model.predict_calls, 1)
        self.assertEqual(second, first)

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

    def test_single_date_predict_uses_feature_hot_cache_path(self):
        svc = ModelService()
        feature_service = _FakeFeatureService()
        svc._feature_service = feature_service
        svc._record_cache[("CN", "model_cached")] = {
            "id": "model_cached",
            "market": "CN",
            "feature_set_id": "fs_cn",
            "task_type": "regression",
            "model_type": "fake",
            "eval_metrics": {},
            "model_params": {},
        }
        svc._model_cache["model_cached"] = _FakeModel()

        result = svc.predict(
            "model_cached",
            tickers=["sh.600000", "sh.600001"],
            date="2024-01-03",
            market="CN",
        )

        self.assertEqual(feature_service.calls, [("compute_from_cache", "fs_cn", "CN")])
        self.assertEqual(list(result.index), ["sh.600000", "sh.600001"])

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

    def compute_features_from_cache(self, feature_set_id, tickers, start_date, end_date, market=None):
        self.calls.append(("compute_from_cache", feature_set_id, market))
        return self._feature_data()

    def compute_features(self, feature_set_id, tickers, start_date, end_date, market=None):
        self.calls.append(("compute", feature_set_id, market))
        return self._feature_data()

    @staticmethod
    def _feature_data():
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
        return self._label_values(label_id, tickers, start_date, end_date, market=market)

    def compute_label_values_cached(self, label_id, tickers, start_date, end_date, market=None):
        self.calls.append(("compute_cached", label_id, market))
        return self._label_values(label_id, tickers, start_date, end_date, market=market)

    def _label_values(self, label_id, tickers, start_date, end_date, market=None):
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


class _CountingPredictModel(ModelBase):
    def __init__(self):
        self.predict_calls = 0

    def fit(self, X, y, **kwargs):
        return self

    def predict(self, X):
        self.predict_calls += 1
        return pd.Series(range(len(X)), index=X.index, dtype=float, name="prediction")

    def get_params(self):
        return {}


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
