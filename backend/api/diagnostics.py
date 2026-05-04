"""Read-only diagnostic API for agent research."""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.db import get_connection
from backend.services.market_context import normalize_market, normalize_ticker
from backend.services.sql_filters import registered_values_table

router = APIRouter(prefix="/api/diagnostics", tags=["diagnostics"])


@router.get("/daily-bars")
async def diagnostic_daily_bars(
    tickers: list[str] = Query(...),
    target_date: date = Query(..., alias="date"),
    market: Optional[str] = Query(None),
) -> dict:
    """Read daily bars for a small ticker/date slice through the backend connection."""
    try:
        resolved_market = normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    normalized_tickers = [normalize_ticker(ticker, resolved_market) for ticker in tickers]
    if len(normalized_tickers) > 200:
        raise HTTPException(status_code=400, detail="diagnostic query supports at most 200 tickers")

    conn = get_connection()
    with registered_values_table(conn, "ticker", normalized_tickers) as ticker_table:
        rows = conn.execute(
            f"""SELECT b.ticker, b.date, b.open, b.high, b.low, b.close, b.volume, b.adj_factor
               FROM daily_bars b
               INNER JOIN {ticker_table} t ON b.ticker = t.ticker
               WHERE b.market = ?
                 AND b.date = ?
               ORDER BY b.ticker""",
            [resolved_market, target_date],
        ).fetchall()
    return {
        "market": resolved_market,
        "date": str(target_date),
        "count": len(rows),
        "items": [
            {
                "ticker": row[0],
                "date": str(row[1]),
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "volume": row[6],
                "adj_factor": row[7],
            }
            for row in rows
        ],
    }


@router.get("/factor-values")
async def diagnostic_factor_values(
    factor_id: str,
    tickers: list[str] = Query(...),
    target_date: date = Query(..., alias="date"),
    market: Optional[str] = Query(None),
) -> dict:
    """Read cached factor values for a small ticker/date slice."""
    try:
        resolved_market = normalize_market(market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    normalized_tickers = [normalize_ticker(ticker, resolved_market) for ticker in tickers]
    if len(normalized_tickers) > 200:
        raise HTTPException(status_code=400, detail="diagnostic query supports at most 200 tickers")

    conn = get_connection()
    with registered_values_table(conn, "ticker", normalized_tickers) as ticker_table:
        rows = conn.execute(
            f"""SELECT f.ticker, f.date, f.value
               FROM factor_values_cache f
               INNER JOIN {ticker_table} t ON f.ticker = t.ticker
               WHERE f.market = ?
                 AND f.factor_id = ?
                 AND f.date = ?
               ORDER BY f.ticker""",
            [resolved_market, factor_id, target_date],
        ).fetchall()
    return {
        "market": resolved_market,
        "factor_id": factor_id,
        "date": str(target_date),
        "count": len(rows),
        "items": [
            {"ticker": row[0], "date": str(row[1]), "value": row[2]}
            for row in rows
        ],
    }
