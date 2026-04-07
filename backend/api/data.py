"""Data acquisition and query API endpoints."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.data_service import DataService
from backend.tasks.executor import TaskExecutor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["data"])

# Shared instances (initialised lazily).
_executor: TaskExecutor | None = None
_service: DataService | None = None


def _get_executor() -> TaskExecutor:
    global _executor
    if _executor is None:
        _executor = TaskExecutor()
    return _executor


def _get_service() -> DataService:
    global _service
    if _service is None:
        _service = DataService()
    return _service


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class UpdateRequest(BaseModel):
    mode: str = "incremental"


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.get("/data/status")
async def data_status() -> dict:
    """Return data status summary."""
    svc = _get_service()
    return svc.get_data_status()


@router.post("/data/update")
async def trigger_update(body: UpdateRequest) -> dict:
    """Trigger a data update as a background task."""
    if body.mode not in ("incremental", "full"):
        raise HTTPException(status_code=400, detail="mode must be 'incremental' or 'full'")

    executor = _get_executor()
    svc = _get_service()

    # Reject if a data_update task is already running
    running_id = executor.has_running_task("data_update")
    if running_id:
        raise HTTPException(
            status_code=409,
            detail=f"Data update already running (task_id={running_id}). Cancel it first via POST /api/tasks/{running_id}/cancel",
        )

    task_id = executor.submit(
        task_type="data_update",
        fn=svc.update_data,
        params={"mode": body.mode},
        timeout=7200,  # 2 hours max
        source=TaskSource.UI,
    )

    log.info("api.data.update_triggered", task_id=task_id, mode=body.mode)
    return {"task_id": task_id, "status": "queued", "mode": body.mode}


@router.get("/data/update/progress")
async def update_progress() -> dict:
    """Get progress of the most recent data update task."""
    executor = _get_executor()

    # Find the most recent data_update task
    conn = get_connection()
    row = conn.execute(
        """SELECT id, status, started_at, completed_at, result_summary, error_message
           FROM task_runs
           WHERE task_type = 'data_update'
           ORDER BY created_at DESC
           LIMIT 1"""
    ).fetchone()

    if row is None:
        return {"status": "no_updates", "message": "No data update has been triggered yet"}

    import json

    return {
        "task_id": row[0],
        "status": row[1],
        "started_at": str(row[2]) if row[2] else None,
        "completed_at": str(row[3]) if row[3] else None,
        "result": json.loads(row[4]) if row[4] else None,
        "error": row[5],
    }


@router.get("/data/quality")
async def quality_check() -> dict:
    """Run data quality checks and return report."""
    svc = _get_service()
    return svc.run_quality_check()


@router.delete("/data/bars")
async def delete_bars_by_date(
    target_date: date = Query(..., alias="date", description="Delete all daily bars for this date"),
) -> dict:
    """Delete all daily bar records for a specific date."""
    conn = get_connection()
    count_before = conn.execute(
        "SELECT COUNT(*) FROM daily_bars WHERE date = ?", [target_date]
    ).fetchone()[0]
    if count_before == 0:
        raise HTTPException(status_code=404, detail=f"No bars found for {target_date}")
    conn.execute("DELETE FROM daily_bars WHERE date = ?", [target_date])
    log.info("api.data.bars_deleted", date=str(target_date), count=count_before)
    return {"date": str(target_date), "deleted_rows": count_before}


@router.get("/stocks/search")
async def search_stocks(
    q: str = Query(..., min_length=1, description="Search query for ticker or name"),
    limit: int = Query(20, ge=1, le=100),
) -> list[dict]:
    """Search stocks by ticker or name prefix."""
    conn = get_connection()
    query_upper = q.upper()
    query_like = f"%{q}%"

    rows = conn.execute(
        """SELECT ticker, name, exchange, sector, status
           FROM stocks
           WHERE ticker LIKE ? OR UPPER(name) LIKE UPPER(?)
           ORDER BY
               CASE WHEN ticker = ? THEN 0
                    WHEN ticker LIKE ? THEN 1
                    ELSE 2 END,
               ticker
           LIMIT ?""",
        [f"{query_upper}%", query_like, query_upper, f"{query_upper}%", limit],
    ).fetchall()

    return [
        {
            "ticker": r[0],
            "name": r[1],
            "exchange": r[2],
            "sector": r[3],
            "status": r[4],
        }
        for r in rows
    ]


@router.get("/stocks/{ticker}/daily")
async def get_daily_bars(
    ticker: str,
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
) -> list[dict]:
    """Get daily bars for a specific ticker."""
    conn = get_connection()

    if end is None:
        end = date.today()
    if start is None:
        start = end - timedelta(days=365)

    rows = conn.execute(
        """SELECT date, open, high, low, close, volume, adj_factor
           FROM daily_bars
           WHERE ticker = ? AND date BETWEEN ? AND ?
           ORDER BY date""",
        [ticker.upper(), start, end],
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

    return [
        {
            "date": str(r[0]),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
            "adj_factor": r[6],
        }
        for r in rows
    ]
