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


class PredictRequest(BaseModel):
    tickers: list[str]
    date: str
    feature_set_id: Optional[str] = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/models/train")
async def train_model(body: TrainModelRequest) -> dict:
    """Trigger async model training.

    Returns a task_id to poll for progress.
    """
    svc = _get_service()
    executor = _get_executor()

    def _do_train(
        name: str,
        feature_set_id: str,
        label_id: str,
        model_type: str,
        model_params: dict | None,
        train_config: dict | None,
        universe_group_id: str,
    ) -> dict:
        return svc.train_model(
            name=name,
            feature_set_id=feature_set_id,
            label_id=label_id,
            model_type=model_type,
            model_params=model_params,
            train_config=train_config,
            universe_group_id=universe_group_id,
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
    """Generate predictions for a given date and tickers."""
    svc = _get_service()
    try:
        preds = svc.predict(
            model_id=model_id,
            feature_set_id=body.feature_set_id,
            tickers=body.tickers,
            date=body.date,
        )
        # Convert to JSON-friendly format
        result = {
            "model_id": model_id,
            "date": body.date,
            "predictions": {
                str(k): round(float(v), 6) for k, v in preds.items()
            },
            "count": len(preds),
        }
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error("api.model.predict_error", model_id=model_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")
