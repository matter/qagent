"""Signal generation, listing, detail, and export API endpoints."""

from __future__ import annotations

import csv
import io
import json
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.services.signal_service import SignalService
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api/signals", tags=["signals"])

_service: SignalService | None = None

_V32_LEGACY_SIGNAL_REPLACEMENT = "/api/research-assets/production-signals/generate"


def _get_service() -> SignalService:
    global _service
    if _service is None:
        _service = SignalService()
    return _service


def _get_executor() -> TaskExecutor:
    return get_task_executor()


def _raise_v32_legacy_signal_disabled() -> None:
    raise HTTPException(
        status_code=410,
        detail={
            "status": "disabled",
            "message": (
                "Legacy 2.x signal generation is disabled in V3.2. "
                "Generate production signals from a 3.0 StrategyGraph instead."
            ),
            "replacement": _V32_LEGACY_SIGNAL_REPLACEMENT,
        },
    )


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class GenerateSignalsRequest(BaseModel):
    market: Optional[str] = None
    strategy_id: str
    target_date: str
    universe_group_id: str
    constraint_config: dict | None = None


class DiagnoseSignalsRequest(BaseModel):
    market: Optional[str] = None
    strategy_id: str
    target_date: str
    universe_group_id: str
    date_role: Literal["decision", "execution"] = "decision"
    max_tickers: int = 0
    focus_tickers: list[str] | None = None
    timeout: int = 600
    # Portfolio state injection (方案 A)
    current_weights: dict[str, float] | None = None
    holding_days: dict[str, int] | None = None
    avg_entry_price: dict[str, float] | None = None
    unrealized_pnl: dict[str, float] | None = None
    # Backtest replay (方案 B) — overrides explicit state above
    backtest_id: str | None = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/generate")
async def generate_signals(body: GenerateSignalsRequest) -> dict:
    """Trigger async signal generation for a strategy.

    Returns a task_id to poll for progress.
    """
    _raise_v32_legacy_signal_disabled()
    svc = _get_service()
    executor = _get_executor()
    try:
        resolved_market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    def _do_generate(
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
        market: str,
        constraint_config: dict | None,
        stage_domain_write=None,
    ) -> dict:
        return svc.generate_signals(
            strategy_id=strategy_id,
            target_date=target_date,
            universe_group_id=universe_group_id,
            market=market,
            constraint_config=constraint_config,
            stage_domain_write=stage_domain_write,
        )

    task_id = executor.submit(
        task_type="signal_generate",
        fn=_do_generate,
        params={
            "strategy_id": body.strategy_id,
            "market": resolved_market,
            "target_date": body.target_date,
            "universe_group_id": body.universe_group_id,
            "constraint_config": body.constraint_config,
        },
        timeout=3600,  # 1 hour max
        source=TaskSource.UI,
    )

    log.info(
        "api.signals.generate_triggered",
        task_id=task_id,
        strategy_id=body.strategy_id,
        market=resolved_market,
        target_date=body.target_date,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "strategy_id": body.strategy_id,
        "market": resolved_market,
        "target_date": body.target_date,
    }


@router.post("/diagnose")
async def diagnose_signals(body: DiagnoseSignalsRequest) -> dict:
    """Async signal diagnosis: returns model scores, factor snapshots,
    candidate pool, final signals, and eliminated tickers without DB persistence."""
    _raise_v32_legacy_signal_disabled()
    svc = _get_service()
    executor = _get_executor()
    try:
        resolved_market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    def _do_diagnose(
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
        max_tickers: int,
        focus_tickers: list[str] | None,
        current_weights: dict[str, float] | None,
        holding_days: dict[str, int] | None,
        avg_entry_price: dict[str, float] | None,
        unrealized_pnl: dict[str, float] | None,
        backtest_id: str | None,
        date_role: str,
        market: str,
    ) -> dict:
        return svc.diagnose_signals(
            strategy_id=strategy_id,
            target_date=target_date,
            universe_group_id=universe_group_id,
            market=market,
            date_role=date_role,
            max_tickers=max_tickers,
            focus_tickers=focus_tickers,
            current_weights=current_weights,
            holding_days=holding_days,
            avg_entry_price=avg_entry_price,
            unrealized_pnl=unrealized_pnl,
            backtest_id=backtest_id,
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
            "current_weights": body.current_weights,
            "holding_days": body.holding_days,
            "avg_entry_price": body.avg_entry_price,
            "unrealized_pnl": body.unrealized_pnl,
            "backtest_id": body.backtest_id,
            "date_role": body.date_role,
            "market": resolved_market,
        },
        timeout=body.timeout,
        source=TaskSource.UI,
    )

    log.info(
        "api.signals.diagnose_triggered",
        task_id=task_id,
        strategy_id=body.strategy_id,
        target_date=body.target_date,
        market=resolved_market,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "strategy_id": body.strategy_id,
        "market": resolved_market,
        "target_date": body.target_date,
    }


@router.get("")
async def list_signal_runs(
    strategy_id: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    """List signal runs, optionally filtered by strategy_id."""
    _raise_v32_legacy_signal_disabled()
    svc = _get_service()
    return svc.list_signal_runs(strategy_id=strategy_id, limit=limit, market=market)


@router.get("/{run_id}")
async def get_signal_run(
    run_id: str,
    market: Optional[str] = Query(None),
) -> dict:
    """Get signal run detail with all signal entries."""
    _raise_v32_legacy_signal_disabled()
    svc = _get_service()
    try:
        return svc.get_signal_run(run_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{run_id}/export")
async def export_signals(
    run_id: str,
    market: Optional[str] = Query(None),
    format: str = Query("csv", description="Export format: csv or json"),
) -> Response:
    """Export signal details as CSV or JSON file."""
    _raise_v32_legacy_signal_disabled()
    svc = _get_service()
    try:
        run = svc.get_signal_run(run_id, market=market)
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
            "market": run.get("market"),
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
