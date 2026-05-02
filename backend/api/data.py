"""Data acquisition and query API endpoints."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.params import Query as QueryParam
from pydantic import BaseModel, Field

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.data_service import DataService
from backend.services.group_service import GroupService
from backend.services.market_context import normalize_market, normalize_ticker
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["data"])

# Shared instances (initialised lazily).
_service: DataService | None = None


def _get_executor() -> TaskExecutor:
    return get_task_executor()


def _get_service() -> DataService:
    global _service
    if _service is None:
        _service = DataService()
    return _service


_group_service: GroupService | None = None


def _get_group_service() -> GroupService:
    global _group_service
    if _group_service is None:
        _group_service = GroupService()
    return _group_service


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class UpdateRequest(BaseModel):
    mode: str = "incremental"
    market: Optional[str] = None
    history_years: Optional[int] = Field(None, ge=1, le=30)


class RefreshStockListRequest(BaseModel):
    market: Optional[str] = None


class UpdateTickersRequest(BaseModel):
    tickers: list[str]
    market: Optional[str] = None


class UpdateGroupRequest(BaseModel):
    group_id: str
    market: Optional[str] = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.get("/data/status")
async def data_status(market: Optional[str] = Query(None)) -> dict:
    """Return data status summary."""
    svc = _get_service()
    try:
        return svc.get_data_status(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/data/update")
async def trigger_update(body: UpdateRequest) -> dict:
    """Trigger a data update as a background task."""
    if body.mode not in ("incremental", "full"):
        raise HTTPException(status_code=400, detail="mode must be 'incremental' or 'full'")
    try:
        market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

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
        params={"mode": body.mode, "market": market, "history_years": body.history_years},
        timeout=7200,  # 2 hours max
        source=TaskSource.UI,
    )

    log.info(
        "api.data.update_triggered",
        task_id=task_id,
        market=market,
        mode=body.mode,
        history_years=body.history_years,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "market": market,
        "mode": body.mode,
        "history_years": body.history_years,
    }


@router.post("/data/refresh-stock-list")
async def refresh_stock_list(body: RefreshStockListRequest) -> dict:
    """Refresh the stock universe without downloading daily bars."""
    try:
        market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    executor = _get_executor()
    svc = _get_service()

    running_id = executor.has_running_task("stock_list_refresh")
    if running_id:
        raise HTTPException(
            status_code=409,
            detail=f"Stock list refresh already running (task_id={running_id})",
        )

    task_id = executor.submit(
        task_type="stock_list_refresh",
        fn=svc.refresh_stock_list,
        params={"market": market},
        timeout=600,
        source=TaskSource.UI,
    )

    log.info("api.data.refresh_stock_list_triggered", task_id=task_id, market=market)
    return {"task_id": task_id, "status": "queued", "market": market}


@router.post("/data/update/tickers")
async def update_tickers(body: UpdateTickersRequest) -> dict:
    """Update data for specific tickers as a background task."""
    if not body.tickers:
        raise HTTPException(status_code=400, detail="tickers list is empty")
    try:
        market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    executor = _get_executor()
    svc = _get_service()

    running_id = executor.has_running_task("data_update")
    if running_id:
        raise HTTPException(
            status_code=409,
            detail=f"数据更新正在运行 (task_id={running_id})，请等待完成",
        )

    task_id = executor.submit(
        task_type="data_update",
        fn=svc.update_tickers,
        params={"tickers": [normalize_ticker(t, market) for t in body.tickers], "market": market},
        timeout=600,
        source=TaskSource.UI,
    )

    log.info("api.data.update_tickers", task_id=task_id, market=market, count=len(body.tickers))
    return {"task_id": task_id, "status": "queued", "market": market, "tickers": len(body.tickers)}


@router.post("/data/update/group")
async def update_group(body: UpdateGroupRequest) -> dict:
    """Update data for all tickers in a stock group."""
    gsvc = _get_group_service()
    try:
        group = gsvc.get_group(body.group_id, market=body.market)
        tickers = group["tickers"]
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not tickers:
        raise HTTPException(status_code=400, detail="分组无成员")

    executor = _get_executor()
    svc = _get_service()

    running_id = executor.has_running_task("data_update")
    if running_id:
        raise HTTPException(
            status_code=409,
            detail=f"数据更新正在运行 (task_id={running_id})，请等待完成",
        )

    task_id = executor.submit(
        task_type="data_update",
        fn=svc.update_tickers,
        params={"tickers": tickers, "market": group["market"]},
        timeout=3600,
        source=TaskSource.UI,
    )

    log.info("api.data.update_group", task_id=task_id, market=group["market"], group=body.group_id, tickers=len(tickers))
    return {"task_id": task_id, "status": "queued", "market": group["market"], "tickers": len(tickers)}


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
async def quality_check(market: Optional[str] = Query(None)) -> dict:
    """Run data quality checks and return report."""
    svc = _get_service()
    try:
        return svc.run_quality_check(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/data/bars")
async def delete_bars_by_date(
    target_date: date = Query(..., alias="date", description="Delete all daily bars for this date"),
    market: Optional[str] = Query(None),
) -> dict:
    """Delete all daily bar records for a specific date."""
    try:
        resolved_market = normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    conn = get_connection()
    count_before = conn.execute(
        "SELECT COUNT(*) FROM daily_bars WHERE market = ? AND date = ?",
        [resolved_market, target_date],
    ).fetchone()[0]
    if count_before == 0:
        raise HTTPException(status_code=404, detail=f"No bars found for {target_date}")
    conn.execute(
        "DELETE FROM daily_bars WHERE market = ? AND date = ?",
        [resolved_market, target_date],
    )
    log.info("api.data.bars_deleted", market=resolved_market, date=str(target_date), count=count_before)
    return {"market": resolved_market, "date": str(target_date), "deleted_rows": count_before}


@router.get("/stocks/search")
async def search_stocks(
    q: str = Query(..., min_length=1, description="Search query for ticker or name"),
    limit: int = Query(20, ge=1, le=100),
    market: Optional[str] = Query(None),
) -> list[dict]:
    """Search stocks by ticker or name prefix."""
    try:
        resolved_market = normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    conn = get_connection()
    query_ticker = normalize_ticker(q, resolved_market)
    query_like = f"%{q}%"
    ticker_contains = f"%{query_ticker}%"

    rows = conn.execute(
        """SELECT market, ticker, name, exchange, sector, status
           FROM stocks
           WHERE market = ?
             AND (ticker LIKE ? OR ticker LIKE ? OR UPPER(name) LIKE UPPER(?))
           ORDER BY
               CASE WHEN ticker = ? THEN 0
                    WHEN ticker LIKE ? THEN 1
                    WHEN ticker LIKE ? THEN 2
                    ELSE 3 END,
               ticker
           LIMIT ?""",
        [
            resolved_market,
            f"{query_ticker}%",
            ticker_contains,
            query_like,
            query_ticker,
            f"{query_ticker}%",
            ticker_contains,
            limit,
        ],
    ).fetchall()

    return [
        {
            "market": r[0],
            "ticker": r[1],
            "name": r[2],
            "exchange": r[3],
            "sector": r[4],
            "status": r[5],
        }
        for r in rows
    ]


@router.get("/stocks/{ticker}/daily")
async def get_daily_bars(
    ticker: str,
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    market: Optional[str] = Query(None),
) -> list[dict]:
    """Get daily bars for a specific ticker."""
    try:
        resolved_market = normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    conn = get_connection()

    if end is None:
        end = date.today()
    if start is None:
        start = end - timedelta(days=365)

    resolved_ticker = normalize_ticker(ticker, resolved_market)
    rows = conn.execute(
        """SELECT date, open, high, low, close, volume, adj_factor
           FROM daily_bars
           WHERE market = ? AND ticker = ? AND date BETWEEN ? AND ?
           ORDER BY date""",
        [resolved_market, resolved_ticker, start, end],
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

    return [
        {
            "market": resolved_market,
            "ticker": resolved_ticker,
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


@router.get("/data/index-bars/{symbol}")
async def get_index_bars(
    symbol: str,
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    market: Optional[str] = Query(None),
) -> list[dict]:
    """Get daily bars for a benchmark/index symbol."""
    try:
        resolved_market = normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if end is None:
        end = date.today()
    if start is None:
        start = end - timedelta(days=365)

    conn = get_connection()
    resolved_symbol = normalize_ticker(symbol, resolved_market)
    rows = conn.execute(
        """SELECT date, open, high, low, close, volume
           FROM index_bars
           WHERE market = ? AND symbol = ? AND date BETWEEN ? AND ?
           ORDER BY date""",
        [resolved_market, resolved_symbol, start, end],
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No index data found for {symbol}")

    return [
        {
            "market": resolved_market,
            "symbol": resolved_symbol,
            "date": str(r[0]),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
        }
        for r in rows
    ]


@router.get("/data/groups/{group_id}/daily-snapshot")
async def get_group_daily_snapshot(
    group_id: str,
    target_date: date = Query(..., alias="date"),
    market: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
) -> dict:
    """Return market-scoped group bar coverage for one date."""
    try:
        resolved_market = normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if isinstance(limit, QueryParam):
        limit = 500

    conn = get_connection()
    group_exists = conn.execute(
        "SELECT 1 FROM stock_groups WHERE id = ? AND market = ?",
        [group_id, resolved_market],
    ).fetchone()
    if group_exists is None:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")

    rows = conn.execute(
        """SELECT m.ticker,
                  b.open,
                  b.high,
                  b.low,
                  b.close,
                  b.volume,
                  b.adj_factor
           FROM stock_group_members m
           LEFT JOIN daily_bars b
             ON b.market = m.market
            AND b.ticker = m.ticker
            AND b.date = ?
           WHERE m.group_id = ? AND m.market = ?
           ORDER BY m.ticker
           LIMIT ?""",
        [target_date, group_id, resolved_market, limit],
    ).fetchall()

    items = [
        {
            "market": resolved_market,
            "ticker": row[0],
            "date": str(target_date),
            "has_bar": row[4] is not None,
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "adj_factor": row[6],
        }
        for row in rows
    ]
    missing = [item["ticker"] for item in items if not item["has_bar"]]
    return {
        "market": resolved_market,
        "group_id": group_id,
        "date": str(target_date),
        "total_tickers": len(items),
        "tickers_with_bars": len(items) - len(missing),
        "missing_count": len(missing),
        "missing_tickers": missing,
        "items": items,
    }
