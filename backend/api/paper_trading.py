"""Paper trading API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.services.paper_trading_service import PaperTradingService
from backend.tasks.executor import TaskExecutor, get_task_executor

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["paper-trading"])

_svc: PaperTradingService | None = None


def _get_svc() -> PaperTradingService:
    global _svc
    if _svc is None:
        _svc = PaperTradingService()
    return _svc


def _get_executor() -> TaskExecutor:
    return get_task_executor()


# ---- Request models ----

class CreateSessionRequest(BaseModel):
    market: str | None = None
    strategy_id: str
    universe_group_id: str
    start_date: str
    name: str | None = None
    config: dict | None = None


class AdvanceRequest(BaseModel):
    market: str | None = None
    target_date: str | None = None
    steps: int = 0  # 0 = advance to target_date/latest; >0 = advance N days


# ---- Endpoints ----

@router.get("/paper-trading/sessions")
async def list_sessions(market: str | None = Query(None)) -> list[dict]:
    return _get_svc().list_sessions(market=market)


@router.post("/paper-trading/sessions")
async def create_session(body: CreateSessionRequest) -> dict:
    try:
        return _get_svc().create_session(
            strategy_id=body.strategy_id,
            universe_group_id=body.universe_group_id,
            start_date=body.start_date,
            name=body.name,
            config=body.config,
            market=body.market,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/paper-trading/sessions/{session_id}")
async def get_session(
    session_id: str,
    market: str | None = Query(None),
) -> dict:
    try:
        return _get_svc().get_session(session_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/paper-trading/sessions/{session_id}")
async def delete_session(
    session_id: str,
    market: str | None = Query(None),
) -> dict:
    try:
        resolved_market = normalize_market(market)
        _get_svc().delete_session(session_id, market=resolved_market)
        return {"status": "deleted", "session_id": session_id, "market": resolved_market}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/paper-trading/sessions/{session_id}/pause")
async def pause_session(
    session_id: str,
    market: str | None = Query(None),
) -> dict:
    try:
        return _get_svc().pause_session(session_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/paper-trading/sessions/{session_id}/resume")
async def resume_session(
    session_id: str,
    market: str | None = Query(None),
) -> dict:
    try:
        return _get_svc().resume_session(session_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/paper-trading/sessions/{session_id}/advance")
async def advance_session(session_id: str, body: AdvanceRequest | None = None) -> dict:
    try:
        target = body.target_date if body else None
        steps = body.steps if body else 0
        resolved_market = normalize_market(body.market if body else None)

        # Submit as async task with extended timeout
        executor = _get_executor()
        svc = _get_svc()
        task_id = executor.submit(
            task_type="paper_trading_advance",
            fn=svc.advance,
            params={
                "session_id": session_id,
                "market": resolved_market,
                "target_date": target,
                "steps": steps,
            },
            timeout=1800,  # 30 minutes for multi-day advance
        )
        return {
            "task_id": task_id,
            "status": "queued",
            "task_type": "paper_trading_advance",
            "market": resolved_market,
            "async": True,
            "poll_url": f"/api/tasks/{task_id}",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/paper-trading/sessions/{session_id}/daily")
async def get_daily_series(
    session_id: str,
    market: str | None = Query(None),
) -> list[dict]:
    return _get_svc().get_daily_series(session_id, market=market)


@router.get("/paper-trading/sessions/{session_id}/positions")
async def get_positions(
    session_id: str,
    market: str | None = Query(None),
    as_of_date: str | None = Query(None, alias="date"),
) -> list[dict]:
    return _get_svc().get_positions(session_id, as_of_date=as_of_date, market=market)


@router.get("/paper-trading/sessions/{session_id}/compare-backtest/{backtest_id}")
async def compare_with_backtest(
    session_id: str,
    backtest_id: str,
    market: str | None = Query(None),
) -> dict:
    return _get_svc().compare_with_backtest(session_id, backtest_id, market=market)


@router.get("/paper-trading/sessions/{session_id}/trades")
async def get_trades(
    session_id: str,
    market: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
) -> list[dict]:
    return _get_svc().get_trades(session_id, limit, market=market)


@router.get("/paper-trading/sessions/{session_id}/summary")
async def get_summary(
    session_id: str,
    market: str | None = Query(None),
) -> dict:
    try:
        return _get_svc().get_summary(session_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/paper-trading/sessions/{session_id}/signals")
async def get_latest_signals(
    session_id: str,
    market: str | None = Query(None),
) -> dict:
    try:
        svc = _get_svc()
        resolved_market = normalize_market(market)
        # Return cached result immediately if available
        cached = svc.get_cached_signals(session_id, market=resolved_market)
        if cached:
            return cached

        # Submit as async task with extended timeout for large universes
        executor = _get_executor()
        task_id = executor.submit(
            task_type="paper_trading_signals",
            fn=svc.get_latest_signals,
            params={"session_id": session_id, "market": resolved_market},
            timeout=900,
        )
        return {"task_id": task_id, "status": "running", "market": resolved_market}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/paper-trading/sessions/{session_id}/stock/{ticker}")
async def get_stock_chart(
    session_id: str,
    ticker: str,
    market: str | None = Query(None),
) -> dict:
    try:
        return _get_svc().get_stock_chart(session_id, ticker, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
