"""Task management API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskSource, TaskStatus
from backend.tasks.store import TaskStore

log = get_logger(__name__)

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

_store: TaskStore | None = None


def _get_executor() -> TaskExecutor:
    return get_task_executor()


def _get_store() -> TaskStore:
    global _store
    if _store is None:
        _store = TaskStore()
    return _store


def _record_to_dict(record) -> dict:
    result = {
        "task_id": record.id,
        "task_type": record.task_type,
        "status": record.status.value,
        "params": record.params,
        "result": record.result_summary,
        "error": record.error_message,
        "created_at": str(record.created_at) if record.created_at else None,
        "started_at": str(record.started_at) if record.started_at else None,
        "completed_at": str(record.completed_at) if record.completed_at else None,
        "source": record.source.value,
    }
    if record.result_summary and isinstance(record.result_summary, dict):
        late_result = record.result_summary.get("late_result")
        if isinstance(late_result, dict):
            for key in ("backtest_id", "model_id", "run_id", "signal_run_id", "id"):
                if key in late_result:
                    payload_key = "late_result_id" if key == "id" else f"late_{key}"
                    if payload_key == "late_backtest_id":
                        payload_key = "late_result_id"
                    result[payload_key] = late_result[key]
                    break
            result["late_result"] = late_result
    return result


class BulkCancelTasksRequest(BaseModel):
    task_type: Optional[str] = None
    status: Optional[str] = None
    source: Optional[TaskSource] = None
    market: Optional[str] = None


class PauseRuleRequest(BaseModel):
    task_type: Optional[str] = None
    source: Optional[TaskSource] = None
    market: Optional[str] = None
    reason: Optional[str] = None


def _validate_market(market: str | None) -> str | None:
    if market is None:
        return None
    try:
        return normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("")
async def list_tasks(
    task_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    source: Optional[TaskSource] = Query(None),
    market: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    """List all tasks, optionally filtered by type and status."""
    store = _get_store()
    task_status = TaskStatus(status) if status else None
    resolved_market = _validate_market(market)
    records = store.list_tasks(
        task_type=task_type,
        status=task_status,
        source=source,
        market=resolved_market,
        limit=limit,
    )
    return [_record_to_dict(r) for r in records]


@router.post("/bulk-cancel")
async def bulk_cancel_tasks(body: BulkCancelTasksRequest) -> dict:
    """Cancel queued/running tasks matching source, market, and type filters."""
    if body.status and body.status not in {"queued", "running"}:
        raise HTTPException(
            status_code=400,
            detail="bulk cancel only supports queued/running tasks",
        )
    resolved_market = _validate_market(body.market)
    executor = _get_executor()
    store = _get_store()
    cancelled = set(
        executor.cancel_matching(
            task_type=body.task_type,
            source=body.source,
            market=resolved_market,
        )
    )
    for task_id in store.mark_matching_active_cancelled(
        task_type=body.task_type,
        source=body.source,
        market=resolved_market,
    ):
        cancelled.add(task_id)
    return {
        "status": "cancelled",
        "cancelled_count": len(cancelled),
        "task_ids": sorted(cancelled),
    }


@router.get("/pause-rules")
async def list_pause_rules(active_only: bool = Query(True)) -> list[dict]:
    """List task submission pause rules."""
    return _get_store().list_pause_rules(active_only=active_only)


@router.post("/pause-rules")
async def create_pause_rule(body: PauseRuleRequest) -> dict:
    """Pause future task submissions matching optional type/source/market filters."""
    return _get_store().create_pause_rule(
        task_type=body.task_type,
        source=body.source,
        market=_validate_market(body.market),
        reason=body.reason,
    )


@router.delete("/pause-rules/{rule_id}")
async def delete_pause_rule(rule_id: str) -> dict:
    """Deactivate a task submission pause rule."""
    ok = _get_store().delete_pause_rule(rule_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Pause rule not found")
    return {"id": rule_id, "status": "deleted"}


@router.post("/{task_id}/cancel")
async def cancel_task(task_id: str) -> dict:
    """Cancel a queued or running task."""
    executor = _get_executor()
    ok = executor.cancel(task_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Task not found or not cancellable")
    return {"task_id": task_id, "status": "cancelled"}


@router.get("/{task_id}")
async def get_task(task_id: str) -> dict:
    """Get task status by ID."""
    executor = _get_executor()
    record = executor.get_task(task_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return _record_to_dict(record)
