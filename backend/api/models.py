"""Model training, listing, detail, deletion, and prediction API endpoints."""

from __future__ import annotations

import threading
import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.services.model_service import ModelService
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["models"])

_service: ModelService | None = None
_PREDICT_API_CONCURRENCY_LIMIT = 2
_PREDICT_API_SEMAPHORE = threading.BoundedSemaphore(_PREDICT_API_CONCURRENCY_LIMIT)


def _get_service() -> ModelService:
    global _service
    if _service is None:
        _service = ModelService()
    return _service


def _get_executor() -> TaskExecutor:
    return get_task_executor()


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class TrainModelRequest(BaseModel):
    market: Optional[str] = None
    name: str
    feature_set_id: str
    label_id: str
    model_type: str = "lightgbm"
    model_params: Optional[dict[str, Any]] = None
    train_config: Optional[dict[str, Any]] = None
    universe_group_id: str
    sample_weight_config: Optional[dict[str, Any]] = None
    objective_type: Optional[str] = None
    ranking_config: Optional[dict[str, Any]] = None


class TrainDistillationRequest(BaseModel):
    market: Optional[str] = None
    name: str
    teacher_model_id: str
    student_feature_set_id: str
    universe_group_id: str
    start_date: str
    end_date: str
    model_type: str = "lightgbm"
    model_params: Optional[dict[str, Any]] = None
    train_config: Optional[dict[str, Any]] = None
    sample_weight_config: Optional[dict[str, Any]] = None
    objective_type: Optional[str] = "regression"
    ranking_config: Optional[dict[str, Any]] = None
    prediction_feature_set_id: Optional[str] = None
    label_name: Optional[str] = None


class PredictRequest(BaseModel):
    market: Optional[str] = None
    tickers: list[str]
    date: str
    feature_set_id: Optional[str] = None


class PredictBatchRequest(BaseModel):
    market: Optional[str] = None
    tickers: list[str]
    dates: list[str]
    feature_set_id: Optional[str] = None
    async_mode: bool = False


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/models/train")
async def train_model(body: TrainModelRequest) -> dict:
    """Trigger async model training.

    Returns a task_id to poll for progress.
    If a training task with the same name is already running, returns the existing task.
    """
    svc = _get_service()
    executor = _get_executor()
    try:
        resolved_market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Check for already running/queued task with same model name
    existing = executor._store.find_active_by_type_and_name(
        "model_train", "name", body.name,
    )
    if existing and normalize_market((existing.params or {}).get("market")) == resolved_market:
        log.info("api.model.train_deduplicated", task_id=existing.id, name=body.name)
        return {
            "task_id": existing.id,
            "status": existing.status.value,
            "name": body.name,
            "market": resolved_market,
            "deduplicated": True,
        }

    def _do_train(
        name: str,
        feature_set_id: str,
        label_id: str,
        model_type: str,
        model_params: dict | None,
        train_config: dict | None,
        universe_group_id: str,
        sample_weight_config: dict | None,
        market: str | None,
        objective_type: str | None,
        ranking_config: dict | None,
        progress=None,
        stage_domain_write=None,
    ) -> dict:
        return svc.train_model(
            name=name,
            feature_set_id=feature_set_id,
            label_id=label_id,
            model_type=model_type,
            model_params=model_params,
            train_config=train_config,
            universe_group_id=universe_group_id,
            sample_weight_config=sample_weight_config,
            market=market,
            objective_type=objective_type,
            ranking_config=ranking_config,
            progress=progress,
            stage_domain_write=stage_domain_write,
        )

    task_id = executor.submit(
        task_type="model_train",
        fn=_do_train,
        params={
            "name": body.name,
            "feature_set_id": body.feature_set_id,
            "label_id": body.label_id,
            "model_type": body.model_type,
            "model_params": body.model_params,
            "train_config": body.train_config,
            "universe_group_id": body.universe_group_id,
            "sample_weight_config": body.sample_weight_config,
            "market": resolved_market,
            "objective_type": body.objective_type,
            "ranking_config": body.ranking_config,
        },
        timeout=7200,  # 2 hours max
        source=TaskSource.UI,
    )

    log.info("api.model.train_triggered", task_id=task_id, name=body.name, market=resolved_market)
    return {"task_id": task_id, "status": "queued", "name": body.name, "market": resolved_market}


@router.post("/models/train-distillation")
async def train_model_distillation(body: TrainDistillationRequest) -> dict:
    """Trigger teacher-prediction label generation and student model training."""
    svc = _get_service()
    executor = _get_executor()
    try:
        resolved_market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    def _do_train_distillation(
        name: str,
        teacher_model_id: str,
        student_feature_set_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
        market: str,
        model_type: str,
        model_params: dict | None,
        train_config: dict | None,
        sample_weight_config: dict | None,
        objective_type: str | None,
        ranking_config: dict | None,
        prediction_feature_set_id: str | None,
        label_name: str | None,
    ) -> dict:
        return svc.train_distilled_model(
            name=name,
            teacher_model_id=teacher_model_id,
            student_feature_set_id=student_feature_set_id,
            universe_group_id=universe_group_id,
            start_date=start_date,
            end_date=end_date,
            market=market,
            model_type=model_type,
            model_params=model_params,
            train_config=train_config,
            sample_weight_config=sample_weight_config,
            objective_type=objective_type,
            ranking_config=ranking_config,
            prediction_feature_set_id=prediction_feature_set_id,
            label_name=label_name,
        )

    task_id = executor.submit(
        task_type="model_distillation_train",
        fn=_do_train_distillation,
        params={
            "name": body.name,
            "teacher_model_id": body.teacher_model_id,
            "student_feature_set_id": body.student_feature_set_id,
            "universe_group_id": body.universe_group_id,
            "start_date": body.start_date,
            "end_date": body.end_date,
            "market": resolved_market,
            "model_type": body.model_type,
            "model_params": body.model_params,
            "train_config": body.train_config,
            "sample_weight_config": body.sample_weight_config,
            "objective_type": body.objective_type,
            "ranking_config": body.ranking_config,
            "prediction_feature_set_id": body.prediction_feature_set_id,
            "label_name": body.label_name,
        },
        timeout=7200,
        source=TaskSource.UI,
    )

    log.info(
        "api.model.distillation_train_triggered",
        task_id=task_id,
        name=body.name,
        teacher_model_id=body.teacher_model_id,
        market=resolved_market,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "task_type": "model_distillation_train",
        "name": body.name,
        "market": resolved_market,
    }


@router.get("/models")
async def list_models(
    market: Optional[str] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[dict]:
    """List all trained models."""
    svc = _get_service()
    try:
        return svc.list_models(market=market, limit=limit, offset=offset)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/models/{model_id}")
async def get_model(model_id: str, market: Optional[str] = Query(None)) -> dict:
    """Get model detail including eval_metrics."""
    svc = _get_service()
    try:
        return svc.get_model(model_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/models/{model_id}")
async def delete_model(model_id: str, market: Optional[str] = Query(None)) -> dict:
    """Delete a model and its files."""
    svc = _get_service()
    try:
        svc.delete_model(model_id, market=market)
        return {"status": "deleted", "id": model_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/models/{model_id}/predict")
async def predict(model_id: str, body: PredictRequest) -> dict:
    """Generate predictions for a given date and tickers.

    For classification models, returns prob, label, and raw_score per ticker.
    For regression models, returns prediction per ticker.
    """
    svc = _get_service()
    acquired = _PREDICT_API_SEMAPHORE.acquire(blocking=False)
    if not acquired:
        raise HTTPException(
            status_code=429,
            detail=(
                "Too many concurrent prediction requests; retry after the "
                "current diagnostics finish"
            ),
        )
    try:
        resolved_market = normalize_market(body.market)
        df = await run_in_threadpool(
            svc.predict_detailed,
            model_id=model_id,
            tickers=body.tickers,
            date=body.date,
            feature_set_id=body.feature_set_id,
            market=resolved_market,
        )
        if "prob" in df.columns:
            predictions = {}
            for ticker in df.index:
                row = df.loc[ticker]
                predictions[str(ticker)] = {
                    "prob": round(float(row["prob"]), 6),
                    "label": int(row["label"]),
                    "raw_score": round(float(row["raw_score"]), 6),
                }
            result = {
                "model_id": model_id,
                "market": resolved_market,
                "date": body.date,
                "task": "classification",
                "predictions": predictions,
                "count": len(predictions),
            }
        else:
            result = {
                "model_id": model_id,
                "market": resolved_market,
                "date": body.date,
                "task": "regression",
                "predictions": {
                    str(k): round(float(v), 6)
                    for k, v in df["prediction"].items()
                },
                "count": len(df),
            }
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error("api.model.predict_error", model_id=model_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")
    finally:
        _PREDICT_API_SEMAPHORE.release()


@router.post("/models/{model_id}/predict-batch")
async def predict_batch(model_id: str, body: PredictBatchRequest) -> dict:
    """Batch predictions across multiple dates in a single call.

    Loads the model and features once for the full date range, then
    predicts per date.  Much faster than calling predict() per date.
    """
    svc = _get_service()
    try:
        resolved_market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if body.async_mode:
        executor = _get_executor()

        def _do_predict_batch(
            model_id: str,
            tickers: list[str],
            dates: list[str],
            feature_set_id: str | None,
            market: str,
        ) -> dict:
            started = time.perf_counter()
            results = svc.predict_batch(
                model_id=model_id,
                tickers=tickers,
                dates=dates,
                feature_set_id=feature_set_id,
                market=market,
            )
            total = sum(len(v) for v in results.values())
            return {
                "model_id": model_id,
                "market": market,
                "dates": dates,
                "predictions": results,
                "total_predictions": total,
                "runtime_seconds": round(time.perf_counter() - started, 6),
            }

        task_id = executor.submit(
            task_type="model_predict_batch",
            fn=_do_predict_batch,
            params={
                "model_id": model_id,
                "tickers": body.tickers,
                "dates": body.dates,
                "feature_set_id": body.feature_set_id,
                "market": resolved_market,
            },
            timeout=600,
            source=TaskSource.UI,
        )
        return {
            "task_id": task_id,
            "status": "queued",
            "task_type": "model_predict_batch",
            "model_id": model_id,
            "market": resolved_market,
            "poll_url": f"/api/tasks/{task_id}",
        }

    acquired = _PREDICT_API_SEMAPHORE.acquire(blocking=False)
    if not acquired:
        raise HTTPException(
            status_code=429,
            detail=(
                "Too many concurrent prediction requests; retry after the "
                "current diagnostics finish"
            ),
        )
    try:
        started = time.perf_counter()
        results = await run_in_threadpool(
            svc.predict_batch,
            model_id=model_id,
            tickers=body.tickers,
            dates=body.dates,
            feature_set_id=body.feature_set_id,
            market=resolved_market,
        )
        total = sum(len(v) for v in results.values())
        return {
            "model_id": model_id,
            "market": resolved_market,
            "dates": body.dates,
            "predictions": results,
            "total_predictions": total,
            "runtime_seconds": round(time.perf_counter() - started, 6),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error("api.model.predict_batch_error", model_id=model_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {e}")
    finally:
        _PREDICT_API_SEMAPHORE.release()
