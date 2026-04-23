"""Signal generation, listing, detail, and export API endpoints."""

from __future__ import annotations

import csv
import io
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.signal_service import SignalService
from backend.tasks.executor import TaskExecutor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api/signals", tags=["signals"])

_service: SignalService | None = None
_executor: TaskExecutor | None = None


def _get_service() -> SignalService:
    global _service
    if _service is None:
        _service = SignalService()
    return _service


def _get_executor() -> TaskExecutor:
    global _executor
    if _executor is None:
        _executor = TaskExecutor()
    return _executor


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class GenerateSignalsRequest(BaseModel):
    strategy_id: str
    target_date: str
    universe_group_id: str


class DiagnoseSignalsRequest(BaseModel):
    strategy_id: str
    target_date: str
    universe_group_id: str
    max_tickers: int = 0
    focus_tickers: list[str] | None = None
    timeout: int = 600


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/generate")
async def generate_signals(body: GenerateSignalsRequest) -> dict:
    """Trigger async signal generation for a strategy.

    Returns a task_id to poll for progress.
    """
    svc = _get_service()
    executor = _get_executor()

    def _do_generate(
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
    ) -> dict:
        return svc.generate_signals(
            strategy_id=strategy_id,
            target_date=target_date,
            universe_group_id=universe_group_id,
        )

    task_id = executor.submit(
        task_type="signal_generate",
        fn=_do_generate,
        params={
            "strategy_id": body.strategy_id,
            "target_date": body.target_date,
            "universe_group_id": body.universe_group_id,
        },
        timeout=3600,  # 1 hour max
        source=TaskSource.UI,
    )

    log.info(
        "api.signals.generate_triggered",
        task_id=task_id,
        strategy_id=body.strategy_id,
        target_date=body.target_date,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "strategy_id": body.strategy_id,
        "target_date": body.target_date,
    }


@router.post("/diagnose")
async def diagnose_signals(body: DiagnoseSignalsRequest) -> dict:
    """Async signal diagnosis: returns model scores, factor snapshots,
    candidate pool, final signals, and eliminated tickers without DB persistence."""
    svc = _get_service()
    executor = _get_executor()

    def _do_diagnose(
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
        max_tickers: int,
        focus_tickers: list[str] | None,
    ) -> dict:
        return svc.diagnose_signals(
            strategy_id=strategy_id,
            target_date=target_date,
            universe_group_id=universe_group_id,
            max_tickers=max_tickers,
            focus_tickers=focus_tickers,
        )

    task_id = executor.submit(
        task_type="signal_diagnose",
        fn=_do_diagnose,
        params={
            "strategy_id": body.strategy_id,
            "target_date": body.target_date,
            "universe_group_id": body.universe_group_id,
            "max_tickers": body.max_tickers,
            "focus_tickers": body.focus_tickers,
        },
        timeout=body.timeout,
        source=TaskSource.UI,
    )

    log.info(
        "api.signals.diagnose_triggered",
        task_id=task_id,
        strategy_id=body.strategy_id,
        target_date=body.target_date,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "strategy_id": body.strategy_id,
        "target_date": body.target_date,
    }


@router.get("")
async def list_signal_runs(
    strategy_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    """List signal runs, optionally filtered by strategy_id."""
    svc = _get_service()
    return svc.list_signal_runs(strategy_id=strategy_id, limit=limit)


@router.get("/{run_id}")
async def get_signal_run(run_id: str) -> dict:
    """Get signal run detail with all signal entries."""
    svc = _get_service()
    try:
        return svc.get_signal_run(run_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{run_id}/export")
async def export_signals(
    run_id: str,
    format: str = Query("csv", description="Export format: csv or json"),
) -> Response:
    """Export signal details as CSV or JSON file."""
    svc = _get_service()
    try:
        run = svc.get_signal_run(run_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    signals = run.get("signals", [])
    target_date = run.get("target_date", "unknown")
    strategy_id = run.get("strategy_id", "unknown")

    if format == "csv":
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=["ticker", "signal", "target_weight", "strength"],
        )
        writer.writeheader()
        for s in signals:
            writer.writerow({
                "ticker": s["ticker"],
                "signal": s["signal"],
                "target_weight": s["target_weight"],
                "strength": s["strength"],
            })
        content = output.getvalue()
        filename = f"signals_{strategy_id}_{target_date}.csv"
        return Response(
            content=content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    elif format == "json":
        export_data = {
            "run_id": run_id,
            "strategy_id": strategy_id,
            "target_date": target_date,
            "result_level": run.get("result_level"),
            "signal_count": len(signals),
            "signals": signals,
        }
        content = json.dumps(export_data, indent=2, default=str)
        filename = f"signals_{strategy_id}_{target_date}.json"
        return Response(
            content=content,
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported export format: {format}. Use 'csv' or 'json'.",
        )
