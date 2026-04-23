"""Model training, listing, detail, deletion, and prediction API endpoints."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.model_service import ModelService
from backend.tasks.executor import TaskExecutor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["models"])

_service: ModelService | None = None
_executor: TaskExecutor | None = None


def _get_service() -> ModelService:
    global _service
    if _service is None:
        _service = ModelService()
    return _service


def _get_executor() -> TaskExecutor:
    global _executor
    if _executor is None:
        _executor = TaskExecutor()
    return _executor


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class TrainModelRequest(BaseModel):
    name: str
    feature_set_id: str
    label_id: str
    model_type: str = "lightgbm"
    model_params: Optional[dict[str, Any]] = None
    train_config: Optional[dict[str, Any]] = None
    universe_group_id: str
    sample_weight_config: Optional[dict[str, Any]] = None


class PredictRequest(BaseModel):
    tickers: list[str]
    date: str
    feature_set_id: Optional[str] = None


class PredictBatchRequest(BaseModel):
    tickers: list[str]
    dates: list[str]
    feature_set_id: Optional[str] = None


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

    # Check for already running/queued task with same model name
    existing = executor._store.find_active_by_type_and_name(
        "model_train", "name", body.name,
    )
    if existing:
        log.info("api.model.train_deduplicated", task_id=existing.id, name=body.name)
        return {
            "task_id": existing.id,
            "status": existing.status.value,
            "name": body.name,
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
        },
        timeout=7200,  # 2 hours max
        source=TaskSource.UI,
    )

    log.info("api.model.train_triggered", task_id=task_id, name=body.name)
    return {"task_id": task_id, "status": "queued", "name": body.name}


@router.get("/models")
async def list_models() -> list[dict]:
    """List all trained models."""
    svc = _get_service()
    return svc.list_models()


@router.get("/models/{model_id}")
async def get_model(model_id: str) -> dict:
    """Get model detail including eval_metrics."""
    svc = _get_service()
    try:
        return svc.get_model(model_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/models/{model_id}")
async def delete_model(model_id: str) -> dict:
    """Delete a model and its files."""
    svc = _get_service()
    try:
        svc.delete_model(model_id)
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
    try:
        df = svc.predict_detailed(
            model_id=model_id,
            tickers=body.tickers,
            date=body.date,
            feature_set_id=body.feature_set_id,
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
                "date": body.date,
                "task": "classification",
                "predictions": predictions,
                "count": len(predictions),
            }
        else:
            result = {
                "model_id": model_id,
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


@router.post("/models/{model_id}/predict-batch")
async def predict_batch(model_id: str, body: PredictBatchRequest) -> dict:
    """Batch predictions across multiple dates in a single call.

    Loads the model and features once for the full date range, then
    predicts per date.  Much faster than calling predict() per date.
    """
    svc = _get_service()
    try:
        results = svc.predict_batch(
            model_id=model_id,
            tickers=body.tickers,
            dates=body.dates,
            feature_set_id=body.feature_set_id,
        )
        total = sum(len(v) for v in results.values())
        return {
            "model_id": model_id,
            "dates": body.dates,
            "predictions": results,
            "total_predictions": total,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error("api.model.predict_batch_error", model_id=model_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {e}")
