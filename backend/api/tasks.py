"""Task management API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.logger import get_logger
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskStatus
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
    return {
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


@router.get("")
async def list_tasks(
    task_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    """List all tasks, optionally filtered by type and status."""
    store = _get_store()
    task_status = TaskStatus(status) if status else None
    records = store.list_tasks(task_type=task_type, status=task_status, limit=limit)
    return [_record_to_dict(r) for r in records]


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
