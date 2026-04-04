"""Task management API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.logger import get_logger
from backend.tasks.executor import TaskExecutor

log = get_logger(__name__)

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

_executor: TaskExecutor | None = None


def _get_executor() -> TaskExecutor:
    global _executor
    if _executor is None:
        _executor = TaskExecutor()
    return _executor


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
    import json
    executor = _get_executor()
    record = executor.get_task(task_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Task not found")
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
    }
