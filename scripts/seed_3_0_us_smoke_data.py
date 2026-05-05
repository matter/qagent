#!/usr/bin/env python3
"""Seed deterministic US smoke data for QAgent 3.0 development.

This is not production market data.  It creates a clearly named `test20`
universe plus deterministic daily bars and SPY index bars so the local full
research loop can be validated without network/provider dependencies.
"""

from __future__ import annotations

import math
from datetime import date

import pandas as pd

from backend.db import get_connection, init_db
from backend.services.calendar_service import get_trading_days
from backend.services.market_data_foundation_service import MarketDataFoundationService
from backend.services.research_kernel_service import ResearchKernelService
from backend.time_utils import utc_now_naive


TICKERS = [
    ("AAPL", "Apple Inc.", "NASDAQ", "Technology"),
    ("MSFT", "Microsoft Corporation", "NASDAQ", "Technology"),
    ("NVDA", "NVIDIA Corporation", "NASDAQ", "Technology"),
    ("AMZN", "Amazon.com Inc.", "NASDAQ", "Consumer Cyclical"),
    ("META", "Meta Platforms Inc.", "NASDAQ", "Communication Services"),
    ("GOOGL", "Alphabet Inc.", "NASDAQ", "Communication Services"),
    ("JPM", "JPMorgan Chase & Co.", "NYSE", "Financial Services"),
    ("UNH", "UnitedHealth Group Inc.", "NYSE", "Healthcare"),
    ("XOM", "Exxon Mobil Corporation", "NYSE", "Energy"),
    ("JNJ", "Johnson & Johnson", "NYSE", "Healthcare"),
    ("PG", "Procter & Gamble Co.", "NYSE", "Consumer Defensive"),
    ("HD", "Home Depot Inc.", "NYSE", "Consumer Cyclical"),
    ("MA", "Mastercard Inc.", "NYSE", "Financial Services"),
    ("BAC", "Bank of America Corp.", "NYSE", "Financial Services"),
    ("KO", "Coca-Cola Co.", "NYSE", "Consumer Defensive"),
    ("PEP", "PepsiCo Inc.", "NASDAQ", "Consumer Defensive"),
    ("COST", "Costco Wholesale Corp.", "NASDAQ", "Consumer Defensive"),
    ("AVGO", "Broadcom Inc.", "NASDAQ", "Technology"),
    ("CRM", "Salesforce Inc.", "NYSE", "Technology"),
    ("NFLX", "Netflix Inc.", "NASDAQ", "Communication Services"),
]

START = date(2020, 1, 2)
END = date(2026, 4, 10)


def _market_return(day_index: int) -> float:
    cyclical = 0.0018 * math.sin(day_index / 17.0) + 0.0012 * math.sin(day_index / 53.0)
    regime = 0.00035 if day_index < 520 else (-0.00018 if day_index < 760 else 0.00028)
    return regime + cyclical


def _ticker_return(day_index: int, ticker_index: int, previous_return: float) -> float:
    style = (ticker_index % 5 - 2) * 0.00018
    phase = 0.0016 * math.sin(day_index / (11.0 + ticker_index % 7) + ticker_index * 0.71)
    rotation = 0.0011 * math.cos(day_index / 29.0 + ticker_index * 0.37)
    momentum = 0.18 * previous_return
    return _market_return(day_index) + style + phase + rotation + momentum


def _build_stocks() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "market": "US",
                "ticker": ticker,
                "name": name,
                "exchange": exchange,
                "sector": sector,
                "status": "active",
                "updated_at": utc_now_naive(),
            }
            for ticker, name, exchange, sector in TICKERS
        ]
    )


def _build_bars(days: list[date]) -> pd.DataFrame:
    rows: list[dict] = []
    for ticker_index, (ticker, _name, _exchange, _sector) in enumerate(TICKERS):
        close = 45.0 + ticker_index * 8.0
        prev_return = 0.0
        for day_index, session in enumerate(days):
            ret = _ticker_return(day_index, ticker_index, prev_return)
            prev_close = close
            close = max(5.0, close * (1.0 + ret))
            overnight = 0.0007 * math.sin(day_index / 7.0 + ticker_index)
            open_price = max(1.0, prev_close * (1.0 + overnight))
            range_pad = 0.004 + abs(ret) * 0.8
            high = max(open_price, close) * (1.0 + range_pad)
            low = min(open_price, close) * (1.0 - range_pad)
            volume = int(
                1_000_000
                + ticker_index * 75_000
                + 280_000 * (1.0 + math.sin(day_index / 13.0 + ticker_index))
            )
            rows.append(
                {
                    "market": "US",
                    "ticker": ticker,
                    "date": session,
                    "open": round(open_price, 4),
                    "high": round(high, 4),
                    "low": round(low, 4),
                    "close": round(close, 4),
                    "volume": volume,
                    "adj_factor": 1.0,
                }
            )
            prev_return = ret
    return pd.DataFrame(rows)


def _build_index_bars(days: list[date]) -> pd.DataFrame:
    close = 320.0
    rows: list[dict] = []
    for day_index, session in enumerate(days):
        ret = _market_return(day_index) + 0.0004 * math.sin(day_index / 31.0)
        prev_close = close
        close = max(100.0, close * (1.0 + ret))
        open_price = prev_close * (1.0 + 0.0004 * math.sin(day_index / 9.0))
        range_pad = 0.0035 + abs(ret) * 0.5
        rows.append(
            {
                "market": "US",
                "symbol": "SPY",
                "date": session,
                "open": round(open_price, 4),
                "high": round(max(open_price, close) * (1.0 + range_pad), 4),
                "low": round(min(open_price, close) * (1.0 - range_pad), 4),
                "close": round(close, 4),
                "volume": int(40_000_000 + 3_000_000 * (1.0 + math.sin(day_index / 19.0))),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    init_db()
    conn = get_connection()
    days = get_trading_days(START, END, market="US")
    if not days:
        raise RuntimeError("No US trading days resolved for smoke fixture range")

    stocks = _build_stocks()
    bars = _build_bars(days)
    index_bars = _build_index_bars(days)

    conn.register("_smoke_stocks", stocks)
    try:
        conn.execute(
            """INSERT OR REPLACE INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               SELECT market, ticker, name, exchange, sector, status, updated_at
               FROM _smoke_stocks"""
        )
    finally:
        conn.unregister("_smoke_stocks")

    conn.register("_smoke_bars", bars)
    try:
        conn.execute(
            """INSERT OR REPLACE INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               SELECT market, ticker, date, open, high, low, close, volume, adj_factor
               FROM _smoke_bars"""
        )
    finally:
        conn.unregister("_smoke_bars")

    conn.register("_smoke_index_bars", index_bars)
    try:
        conn.execute(
            """INSERT OR REPLACE INTO index_bars
               (market, symbol, date, open, high, low, close, volume)
               SELECT market, symbol, date, open, high, low, close, volume
               FROM _smoke_index_bars"""
        )
    finally:
        conn.unregister("_smoke_index_bars")

    now = utc_now_naive()
    conn.execute("DELETE FROM stock_group_members WHERE group_id = 'test20' AND market = 'US'")
    conn.execute("DELETE FROM stock_groups WHERE id = 'test20' AND market = 'US'")
    conn.execute(
        """INSERT INTO stock_groups
           (id, market, name, description, group_type, filter_expr, created_at, updated_at)
           VALUES ('test20', 'US', 'US Smoke Test 20',
                   'Deterministic demo universe for local full-flow smoke tests',
                   'manual', NULL, ?, ?)""",
        [now, now],
    )
    conn.executemany(
        "INSERT INTO stock_group_members (group_id, market, ticker) VALUES ('test20', 'US', ?)",
        [(ticker,) for ticker, *_ in TICKERS],
    )

    MarketDataFoundationService().sync_assets_from_legacy_stocks("US_EQ")
    kernel = ResearchKernelService()
    run = kernel.create_run(
        run_type="smoke_data_seed",
        params={
            "market": "US",
            "group_id": "test20",
            "start": str(START),
            "end": str(END),
            "synthetic": True,
        },
        lifecycle_stage="scratch",
        retention_class="rebuildable",
        created_by="system",
        status="completed",
    )
    artifact = kernel.create_json_artifact(
        run_id=run["id"],
        artifact_type="smoke_data_manifest",
        payload={
            "market": "US",
            "group_id": "test20",
            "tickers": [ticker for ticker, *_ in TICKERS],
            "start": str(days[0]),
            "end": str(days[-1]),
            "stock_rows": len(stocks),
            "bar_rows": len(bars),
            "index_bar_rows": len(index_bars),
            "purpose": "local deterministic full-flow smoke testing",
        },
        lifecycle_stage="scratch",
        retention_class="rebuildable",
        metadata={"synthetic": True, "official_result": False},
    )

    print(
        {
            "group_id": "test20",
            "tickers": len(TICKERS),
            "trading_days": len(days),
            "bar_rows": len(bars),
            "index_bar_rows": len(index_bars),
            "run_id": run["id"],
            "artifact_id": artifact["id"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
