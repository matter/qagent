"""Macro data API endpoints."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.macro_data_service import MacroDataService
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskSource

router = APIRouter(prefix="/api/macro-data", tags=["macro-data"])

_service: MacroDataService | None = None


class FredUpdateRequest(BaseModel):
    series_ids: list[str] = Field(..., min_length=1)
    start_date: date | None = None
    end_date: date | None = None
    realtime_start: date | None = None
    realtime_end: date | None = None


def _get_executor() -> TaskExecutor:
    return get_task_executor()


def _get_service() -> MacroDataService:
    global _service
    if _service is None:
        _service = MacroDataService()
    return _service


@router.post("/fred/update")
async def update_fred_series(body: FredUpdateRequest) -> dict:
    service = _get_service()
    executor = _get_executor()
    series_ids = _normalize_series_ids(body.series_ids)
    if not series_ids:
        raise HTTPException(status_code=400, detail="series_ids must not be empty")

    task_id = executor.submit(
        task_type="macro_data_update",
        fn=service.update_fred_series,
        params={
            "series_ids": series_ids,
            "start_date": _date_param(body.start_date),
            "end_date": _date_param(body.end_date),
            "realtime_start": _date_param(body.realtime_start),
            "realtime_end": _date_param(body.realtime_end),
        },
        timeout=1800,
        source=TaskSource.UI,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "provider": "fred",
        "series_ids": series_ids,
        "poll_url": f"/api/tasks/{task_id}",
    }


@router.get("/series")
async def list_macro_series(
    provider: str = Query("fred"),
    limit: int = Query(1000, ge=1, le=10000),
) -> dict:
    return {
        "provider": provider.lower(),
        "series": _get_service().list_series(provider=provider, limit=limit),
    }


@router.get("/observations")
async def query_macro_series(
    series_ids: str = Query(..., description="Comma-separated FRED series ids"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    as_of: str | None = Query(None),
    strict_pit: bool = Query(False),
    limit: int = Query(10000, ge=1, le=100000),
) -> dict:
    normalized = _normalize_series_ids(series_ids.split(","))
    if not normalized:
        raise HTTPException(status_code=400, detail="series_ids must not be empty")
    if strict_pit:
        if not as_of:
            raise HTTPException(status_code=400, detail="strict_pit requires as_of decision time")
        rows = _get_service().query_series_as_of(
            series_ids=normalized,
            start_date=_date_param(start_date),
            end_date=_date_param(end_date),
            decision_time=as_of,
            limit=limit,
        )
    else:
        rows = _get_service().query_series(
            series_ids=normalized,
            start_date=_date_param(start_date),
            end_date=_date_param(end_date),
            as_of=as_of,
            limit=limit,
        )
    return {
        "provider": "fred",
        "series_ids": normalized,
        "strict_pit": strict_pit,
        "observations": rows,
    }


def _normalize_series_ids(series_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in series_ids:
        series_id = str(raw or "").strip().upper()
        if not series_id or series_id in seen:
            continue
        seen.add(series_id)
        normalized.append(series_id)
    return normalized


def _date_param(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
