"""Factor CRUD + template + compute + evaluate API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.factors.builtins import get_template_names, get_template_source
from backend.logger import get_logger
from backend.services.factor_service import FactorService
from backend.services.factor_engine import FactorEngine
from backend.services.factor_eval_service import FactorEvalService
from backend.services.group_service import GroupService
from backend.tasks.executor import TaskExecutor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["factors"])

_service: FactorService | None = None
_engine: FactorEngine | None = None
_eval_service: FactorEvalService | None = None
_executor: TaskExecutor | None = None
_group_service: GroupService | None = None


def _get_service() -> FactorService:
    global _service
    if _service is None:
        _service = FactorService()
    return _service


def _get_engine() -> FactorEngine:
    global _engine
    if _engine is None:
        _engine = FactorEngine()
    return _engine


def _get_eval_service() -> FactorEvalService:
    global _eval_service
    if _eval_service is None:
        _eval_service = FactorEvalService()
    return _eval_service


def _get_executor() -> TaskExecutor:
    global _executor
    if _executor is None:
        _executor = TaskExecutor()
    return _executor


def _get_group_service() -> GroupService:
    global _group_service
    if _group_service is None:
        _group_service = GroupService()
    return _group_service


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class CreateFactorRequest(BaseModel):
    name: str
    source_code: str
    description: Optional[str] = None
    category: str = "custom"
    params: Optional[dict] = None


class UpdateFactorRequest(BaseModel):
    source_code: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    params: Optional[dict] = None
    status: Optional[str] = None


class ComputeFactorRequest(BaseModel):
    universe_group_id: str
    start_date: str
    end_date: str


class EvaluateFactorRequest(BaseModel):
    label_id: str
    universe_group_id: str
    start_date: str
    end_date: str


# ------------------------------------------------------------------
# CRUD Endpoints
# ------------------------------------------------------------------


@router.post("/factors")
async def create_factor(body: CreateFactorRequest) -> dict:
    """Create a new factor definition."""
    svc = _get_service()
    try:
        return svc.create_factor(
            name=body.name,
            source_code=body.source_code,
            description=body.description,
            category=body.category,
            params=body.params,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/factors")
async def list_factors(
    category: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
) -> list[dict]:
    """List all factors, optionally filtered by category or status."""
    svc = _get_service()
    return svc.list_factors(category=category, status=status)


@router.get("/factors/templates")
async def list_templates() -> list[dict]:
    """List available built-in factor templates."""
    names = get_template_names()
    return [{"name": n} for n in names]


@router.get("/factors/templates/{template_name}")
async def get_template(template_name: str) -> dict:
    """Get source code for a built-in factor template."""
    source = get_template_source(template_name)
    if source is None:
        raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found")
    return {"name": template_name, "source_code": source}


# NOTE: evaluation detail endpoint must be defined BEFORE the {factor_id}
# catch-all to avoid routing conflicts.


@router.get("/factors/evaluations")
async def list_all_evaluations() -> list[dict]:
    """List all evaluation results across all factors (single JOIN query)."""
    svc = _get_eval_service()
    return svc.list_all_evaluations()


@router.get("/factors/evaluations/{eval_id}")
async def get_evaluation_detail(eval_id: str) -> dict:
    """Get a specific evaluation result with full detail (ic_series, group_returns)."""
    svc = _get_eval_service()
    try:
        return svc.get_evaluation(eval_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/factors/{factor_id}")
async def get_factor(factor_id: str) -> dict:
    """Get factor definition detail including source code."""
    svc = _get_service()
    try:
        return svc.get_factor(factor_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/factors/{factor_id}")
async def update_factor(factor_id: str, body: UpdateFactorRequest) -> dict:
    """Update a factor -- creates new version if source_code changes."""
    svc = _get_service()
    try:
        return svc.update_factor(
            factor_id=factor_id,
            source_code=body.source_code,
            description=body.description,
            category=body.category,
            params=body.params,
            status=body.status,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/factors/{factor_id}")
async def delete_factor(factor_id: str) -> dict:
    """Delete a factor definition."""
    svc = _get_service()
    try:
        svc.delete_factor(factor_id)
        return {"status": "deleted", "id": factor_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------------------------------------------------
# Compute + Evaluate Endpoints
# ------------------------------------------------------------------


@router.post("/factors/{factor_id}/compute")
async def compute_factor(factor_id: str, body: ComputeFactorRequest) -> dict:
    """Trigger async factor computation for a universe and date range.

    Returns a task_id to poll for progress.
    """
    # Validate factor exists
    svc = _get_service()
    try:
        svc.get_factor(factor_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Resolve tickers
    gsvc = _get_group_service()
    try:
        tickers = gsvc.get_group_tickers(body.universe_group_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not tickers:
        raise HTTPException(status_code=400, detail="Universe group has no members")

    engine = _get_engine()
    executor = _get_executor()

    def _do_compute(
        factor_id: str,
        universe_tickers: list,
        start_date: str,
        end_date: str,
    ) -> dict:
        result_df = engine.compute_factor(factor_id, universe_tickers, start_date, end_date)
        return {
            "factor_id": factor_id,
            "shape": list(result_df.shape),
            "tickers_computed": len(result_df.columns),
            "dates": len(result_df.index),
        }

    task_id = executor.submit(
        task_type="factor_compute",
        fn=_do_compute,
        params={
            "factor_id": factor_id,
            "universe_tickers": tickers,
            "start_date": body.start_date,
            "end_date": body.end_date,
        },
        timeout=3600,  # 1 hour max
        source=TaskSource.UI,
    )

    log.info(
        "api.factor.compute_triggered",
        task_id=task_id,
        factor_id=factor_id,
        tickers=len(tickers),
    )
    return {"task_id": task_id, "status": "queued", "factor_id": factor_id}


@router.post("/factors/{factor_id}/evaluate")
async def evaluate_factor(factor_id: str, body: EvaluateFactorRequest) -> dict:
    """Trigger async factor evaluation against a label.

    Returns a task_id to poll for progress.
    """
    # Validate factor exists
    svc = _get_service()
    try:
        svc.get_factor(factor_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    eval_svc = _get_eval_service()
    executor = _get_executor()

    def _do_evaluate(
        factor_id: str,
        label_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        return eval_svc.evaluate_factor(
            factor_id=factor_id,
            label_id=label_id,
            universe_group_id=universe_group_id,
            start_date=start_date,
            end_date=end_date,
        )

    task_id = executor.submit(
        task_type="factor_evaluate",
        fn=_do_evaluate,
        params={
            "factor_id": factor_id,
            "label_id": body.label_id,
            "universe_group_id": body.universe_group_id,
            "start_date": body.start_date,
            "end_date": body.end_date,
        },
        timeout=3600,  # 1 hour max
        source=TaskSource.UI,
    )

    log.info(
        "api.factor.evaluate_triggered",
        task_id=task_id,
        factor_id=factor_id,
        label_id=body.label_id,
    )
    return {"task_id": task_id, "status": "queued", "factor_id": factor_id}


@router.get("/factors/{factor_id}/evaluations")
async def list_evaluations(factor_id: str) -> list[dict]:
    """List all evaluation results for a factor."""
    eval_svc = _get_eval_service()
    return eval_svc.list_evaluations(factor_id)
