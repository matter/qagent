"""Data acquisition and storage service."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from backend.config import settings
from backend.db import get_connection
from backend.logger import get_logger
from backend.providers.base import DataProvider
from backend.providers.yfinance_provider import YFinanceProvider
from backend.services.calendar_service import get_latest_trading_day, get_trading_days, snap_to_trading_day

log = get_logger(__name__)

_FULL_HISTORY_YEARS = 10
_PROGRESS_FILE = "update_progress.json"


def _get_provider() -> DataProvider:
    """Instantiate the configured data provider."""
    name = settings.data.provider
    if name == "yfinance":
        return YFinanceProvider()
    raise ValueError(f"Unknown data provider: {name}")


class DataService:
    """Orchestrate data fetching, storage, and quality checks."""

    def __init__(self, provider: DataProvider | None = None) -> None:
        self._provider = provider or _get_provider()
        self._progress_path = settings.project_root / "data" / _PROGRESS_FILE

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_data(self, mode: str = "incremental") -> dict:
        """Main entry point: fetch and store market data.

        Args:
            mode: 'incremental' (fetch only new bars) or 'full' (10-year refetch).

        Returns:
            Summary dict with total, success, failed, duration.
        """
        run_id = uuid.uuid4().hex
        started_at = datetime.utcnow()
        conn = get_connection()

        # --- For incremental mode, check if data is already up to date ---
        if mode == "incremental":
            latest_completed = get_latest_trading_day()
            max_bar_row = conn.execute(
                "SELECT MAX(date) FROM daily_bars"
            ).fetchone()
            if max_bar_row and max_bar_row[0]:
                max_bar_date = max_bar_row[0]
                if isinstance(max_bar_date, str):
                    max_bar_date = date.fromisoformat(max_bar_date)
                elif hasattr(max_bar_date, "date") and callable(max_bar_date.date):
                    max_bar_date = max_bar_date.date()
                if max_bar_date >= latest_completed:
                    log.info("data.update.already_up_to_date", max_date=str(max_bar_date))
                    return {
                        "run_id": run_id,
                        "mode": mode,
                        "total": 0,
                        "success": 0,
                        "failed": 0,
                        "duration_seconds": 0.0,
                        "message": "Data is already up to date",
                    }

        # Log the start
        conn.execute(
            """INSERT INTO data_update_log
               (id, update_type, started_at, status, total_tickers,
                success_count, fail_count)
               VALUES (?, ?, ?, 'running', 0, 0, 0)""",
            [run_id, mode, started_at],
        )

        try:
            # --- Step 1: Update stock list (skip if updated within 24 hours) ---
            log.info("data.update.stock_list")
            if mode == "incremental" and self._stock_list_is_fresh(conn):
                log.info("data.update.stock_list.cached", msg="Stock list updated within 24h, skipping")
                tickers = [r[0] for r in conn.execute("SELECT ticker FROM stocks").fetchall()]
            else:
                stock_df = self._provider.get_stock_list()
                self._upsert_stocks(stock_df)
                tickers = stock_df["ticker"].tolist()

            # --- Step 2: Determine date ranges ---
            end_date = get_latest_trading_day()
            if mode == "full":
                start_date = date(end_date.year - _FULL_HISTORY_YEARS, 1, 1)
                ticker_starts = {t: start_date for t in tickers}
            else:
                ticker_starts = self._get_incremental_starts(tickers, end_date)

            # Filter out tickers that are already up-to-date
            tickers_to_update = [
                t for t in tickers if t in ticker_starts
            ]

            # Update log with total count
            conn.execute(
                "UPDATE data_update_log SET total_tickers = ? WHERE id = ?",
                [len(tickers_to_update), run_id],
            )

            # --- Step 3: Check for resume ---
            progress = self._load_progress()
            completed_batches: set[int] = set()
            if progress and progress.get("run_id") == run_id:
                completed_batches = set(progress.get("completed_batches", []))
                log.info("data.update.resume", completed_batches=len(completed_batches))

            # --- Step 4: Batch download ---
            batch_size = 500
            success_count = 0
            fail_count = 0
            failed_tickers: list[str] = []

            for batch_idx in range(0, len(tickers_to_update), batch_size):
                batch_num = batch_idx // batch_size
                if batch_num in completed_batches:
                    log.info("data.update.skip_batch", batch=batch_num)
                    continue

                batch = tickers_to_update[batch_idx : batch_idx + batch_size]

                # Find the earliest start date in this batch
                batch_start = min(ticker_starts[t] for t in batch)

                log.info(
                    "data.update.batch",
                    batch=batch_num,
                    size=len(batch),
                    start=str(batch_start),
                    end=str(end_date),
                )

                try:
                    bars_df = self._provider.get_daily_bars(batch, batch_start, end_date)
                    if not bars_df.empty:
                        self._upsert_daily_bars(bars_df)
                        downloaded_tickers = set(bars_df["ticker"].unique())
                        success_count += len(downloaded_tickers)
                        batch_failed = [t for t in batch if t not in downloaded_tickers]
                    else:
                        batch_failed = batch

                    fail_count += len(batch_failed)
                    failed_tickers.extend(batch_failed)
                except Exception as e:
                    log.error("data.update.batch_error", batch=batch_num, error=str(e))
                    fail_count += len(batch)
                    failed_tickers.extend(batch)

                # Save progress
                completed_batches.add(batch_num)
                self._save_progress(
                    {
                        "run_id": run_id,
                        "mode": mode,
                        "completed_batches": list(completed_batches),
                        "success_count": success_count,
                        "fail_count": fail_count,
                    }
                )

            # --- Step 5: Update index data ---
            log.info("data.update.index", symbol="SPY")
            try:
                idx_start = date(end_date.year - _FULL_HISTORY_YEARS, 1, 1) if mode == "full" else (end_date - timedelta(days=7))
                idx_df = self._provider.get_index_data("SPY", idx_start, end_date)
                if not idx_df.empty:
                    self._upsert_index_bars("SPY", idx_df)
            except Exception as e:
                log.warning("data.update.index_error", error=str(e))

            # --- Step 6: Finalize ---
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()

            conn.execute(
                """UPDATE data_update_log
                   SET status = 'completed', completed_at = ?,
                       success_count = ?, fail_count = ?,
                       failed_tickers = ?, message = ?
                   WHERE id = ?""",
                [
                    completed_at,
                    success_count,
                    fail_count,
                    json.dumps(failed_tickers[:100]),  # cap stored list
                    f"Completed in {duration:.1f}s",
                    run_id,
                ],
            )

            # Clean up progress file
            if self._progress_path.exists():
                self._progress_path.unlink()

            summary = {
                "run_id": run_id,
                "mode": mode,
                "total": len(tickers_to_update),
                "success": success_count,
                "failed": fail_count,
                "duration_seconds": round(duration, 1),
                "failed_tickers_sample": failed_tickers[:20],
            }
            log.info("data.update.done", **summary)
            return summary

        except Exception as e:
            completed_at = datetime.utcnow()
            conn.execute(
                """UPDATE data_update_log
                   SET status = 'failed', completed_at = ?, message = ?
                   WHERE id = ?""",
                [completed_at, str(e), run_id],
            )
            log.error("data.update.fatal", error=str(e))
            raise

    def get_data_status(self) -> dict:
        """Return summary of current data state."""
        conn = get_connection()

        stock_count = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]

        bar_stats = conn.execute(
            """SELECT COUNT(DISTINCT ticker), MIN(date), MAX(date), COUNT(*)
               FROM daily_bars"""
        ).fetchone()

        last_update = conn.execute(
            """SELECT completed_at, status, update_type
               FROM data_update_log
               ORDER BY started_at DESC LIMIT 1"""
        ).fetchone()

        # Count tickers with data gaps (missing recent days)
        latest_trading = get_latest_trading_day()
        stale = conn.execute(
            """SELECT COUNT(DISTINCT ticker) FROM daily_bars
               GROUP BY ticker
               HAVING MAX(date) < ?""",
            [latest_trading - timedelta(days=3)],
        ).fetchall()

        return {
            "stock_count": stock_count,
            "tickers_with_bars": bar_stats[0] if bar_stats[0] else 0,
            "date_range": {
                "min": str(bar_stats[1]) if bar_stats[1] else None,
                "max": str(bar_stats[2]) if bar_stats[2] else None,
            },
            "total_bars": bar_stats[3] if bar_stats[3] else 0,
            "stale_tickers": len(stale),
            "latest_trading_day": str(latest_trading),
            "last_update": {
                "completed_at": str(last_update[0]) if last_update else None,
                "status": last_update[1] if last_update else None,
                "type": last_update[2] if last_update else None,
            },
        }

    def run_quality_check(self) -> dict:
        """Check for data quality issues."""
        conn = get_connection()
        issues: list[dict] = []

        # 1. Price jumps > 50%
        jumps = conn.execute(
            """SELECT ticker, date, close, prev_close,
                      ABS(close - prev_close) / prev_close AS pct_change
               FROM (
                   SELECT ticker, date, close,
                          LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
                   FROM daily_bars
               ) sub
               WHERE prev_close > 0 AND ABS(close - prev_close) / prev_close > 0.5
               ORDER BY date DESC
               LIMIT 50"""
        ).fetchall()
        for row in jumps:
            issues.append(
                {
                    "type": "price_jump",
                    "ticker": row[0],
                    "date": str(row[1]),
                    "close": row[2],
                    "prev_close": row[3],
                    "pct_change": round(row[4] * 100, 1),
                }
            )

        # 2. Zero volume
        zero_vol = conn.execute(
            """SELECT ticker, COUNT(*) AS cnt
               FROM daily_bars
               WHERE volume = 0
               GROUP BY ticker
               HAVING cnt > 10
               ORDER BY cnt DESC
               LIMIT 20"""
        ).fetchall()
        for row in zero_vol:
            issues.append(
                {"type": "zero_volume", "ticker": row[0], "count": row[1]}
            )

        # 3. Date gaps (tickers missing > 5 consecutive trading days)
        gap_check = conn.execute(
            """SELECT ticker, date,
                      LEAD(date) OVER (PARTITION BY ticker ORDER BY date) AS next_date,
                      LEAD(date) OVER (PARTITION BY ticker ORDER BY date) - date AS gap_days
               FROM daily_bars
               HAVING gap_days > 7
               ORDER BY gap_days DESC
               LIMIT 20"""
        ).fetchall()
        for row in gap_check:
            issues.append(
                {
                    "type": "date_gap",
                    "ticker": row[0],
                    "from_date": str(row[1]),
                    "to_date": str(row[2]),
                    "gap_days": row[3],
                }
            )

        return {
            "total_issues": len(issues),
            "issues": issues,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _upsert_stocks(self, df: pd.DataFrame) -> None:
        """Insert or update the stocks table."""
        conn = get_connection()
        now = datetime.utcnow()
        for _, row in df.iterrows():
            conn.execute(
                """INSERT OR REPLACE INTO stocks (ticker, name, exchange, sector, status, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [row["ticker"], row["name"], row["exchange"], row["sector"], row["status"], now],
            )
        log.info("data.upsert_stocks", count=len(df))

    def _upsert_daily_bars(self, df: pd.DataFrame) -> None:
        """Bulk insert/replace daily bars."""
        conn = get_connection()
        # Register DataFrame and use INSERT OR REPLACE
        conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_bars AS SELECT * FROM df")
        conn.execute(
            """INSERT OR REPLACE INTO daily_bars
               (ticker, date, open, high, low, close, volume, adj_factor)
               SELECT ticker, date, open, high, low, close, volume, adj_factor
               FROM _tmp_bars"""
        )
        conn.execute("DROP TABLE IF EXISTS _tmp_bars")
        log.info("data.upsert_daily_bars", rows=len(df))

    def _upsert_index_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Insert/replace index bars."""
        conn = get_connection()
        df_with_sym = df.copy()
        df_with_sym["symbol"] = symbol
        conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_idx AS SELECT * FROM df_with_sym")
        conn.execute(
            """INSERT OR REPLACE INTO index_bars
               (symbol, date, open, high, low, close, volume)
               SELECT symbol, date, open, high, low, close, volume
               FROM _tmp_idx"""
        )
        conn.execute("DROP TABLE IF EXISTS _tmp_idx")
        log.info("data.upsert_index_bars", symbol=symbol, rows=len(df))

    def _get_incremental_starts(
        self, tickers: list[str], end_date: date
    ) -> dict[str, date]:
        """For each ticker, find what date to start fetching from.

        Returns only tickers that need updating.
        """
        conn = get_connection()
        # Get max date per ticker
        rows = conn.execute(
            "SELECT ticker, MAX(date) AS max_date FROM daily_bars GROUP BY ticker"
        ).fetchall()
        max_dates = {row[0]: row[1] for row in rows}

        result: dict[str, date] = {}
        for t in tickers:
            if t in max_dates:
                last = max_dates[t]
                if isinstance(last, str):
                    last = date.fromisoformat(last)
                elif hasattr(last, "date"):
                    last = last.date() if callable(last.date) else last
                # Only update if there's a gap
                if last < end_date:
                    result[t] = last + timedelta(days=1)
            else:
                # New ticker with no local data: fetch full history
                result[t] = date(end_date.year - _FULL_HISTORY_YEARS, 1, 1)

        return result

    @staticmethod
    def _stock_list_is_fresh(conn) -> bool:
        """Return True if the stocks table was updated within the last 24 hours."""
        row = conn.execute(
            "SELECT MAX(updated_at) FROM stocks"
        ).fetchone()
        if row and row[0]:
            last_updated = row[0]
            if isinstance(last_updated, str):
                last_updated = datetime.fromisoformat(last_updated)
            if (datetime.utcnow() - last_updated) < timedelta(hours=24):
                return True
        return False

    def _load_progress(self) -> dict | None:
        """Load resume progress from disk."""
        if self._progress_path.exists():
            try:
                return json.loads(self._progress_path.read_text())
            except Exception:
                return None
        return None

    def _save_progress(self, data: dict) -> None:
        """Persist progress to disk for resume capability."""
        self._progress_path.parent.mkdir(parents=True, exist_ok=True)
        self._progress_path.write_text(json.dumps(data))
