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

    # Batch sizes tuned by date range — fewer API calls for short ranges,
    # gentler on Yahoo for long ranges.
    _SHORT_RANGE_BATCH = 500   # <= 30 days: large batches, minimal data per ticker
    _MEDIUM_RANGE_BATCH = 200  # 30-365 days: medium batches
    _LONG_RANGE_BATCH = 50     # > 365 days: small batches, heavy downloads
    _FLUSH_THRESHOLD = 200_000  # flush accumulated rows to DB periodically

    def __init__(self, provider: DataProvider | None = None) -> None:
        self._provider = provider or _get_provider()
        self._progress_path = settings.project_root / "data" / _PROGRESS_FILE

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_tickers(self, tickers: list[str]) -> dict:
        """Update market data for a specific list of tickers (incremental).

        Groups tickers by date range and uses appropriate batch sizes:
        large batches for recent data, small batches for long history.
        """
        if not tickers:
            return {"total": 0, "success": 0, "failed": 0, "duration_seconds": 0}

        from datetime import datetime as _dt
        started = _dt.utcnow()
        end_date = get_latest_trading_day()

        # Determine start dates per ticker
        ticker_starts = self._get_incremental_starts(tickers, end_date)
        tickers_to_update = [t for t in tickers if t in ticker_starts]

        if not tickers_to_update:
            return {
                "total": len(tickers),
                "success": len(tickers),
                "failed": 0,
                "duration_seconds": 0,
                "message": "所有股票数据已是最新",
            }

        # Group tickers by date range and build batches
        batches = self._build_smart_batches(tickers_to_update, ticker_starts, end_date)

        success = 0
        failed = 0
        failed_tickers: list[str] = []
        all_frames: list[pd.DataFrame] = []

        for batch, batch_start in batches:
            bars_df = self._provider.get_daily_bars(batch, batch_start, end_date)
            if bars_df is not None and not bars_df.empty:
                all_frames.append(bars_df)
                downloaded = set(bars_df["ticker"].unique())
                success += len(downloaded)
                batch_failed = [t for t in batch if t not in downloaded]
            else:
                batch_failed = batch
            failed += len(batch_failed)
            failed_tickers.extend(batch_failed)

        if all_frames:
            combined = pd.concat(all_frames, ignore_index=True)
            self._upsert_daily_bars(combined)

        duration = (_dt.utcnow() - started).total_seconds()
        return {
            "total": len(tickers),
            "success": success,
            "failed": failed,
            "duration_seconds": round(duration, 1),
            "failed_tickers_sample": failed_tickers[:20],
        }

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

        # --- Fast check: if ALL tickers already have the latest bar, skip ---
        if mode == "incremental":
            latest_completed = get_latest_trading_day()
            stale_count = conn.execute(
                """SELECT COUNT(*) FROM stocks s
                   WHERE NOT EXISTS (
                       SELECT 1 FROM daily_bars b
                       WHERE b.ticker = s.ticker AND b.date >= ?
                   )""",
                [latest_completed],
            ).fetchone()[0]
            if stale_count == 0:
                log.info("data.update.already_up_to_date")
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

            tickers_to_update = [t for t in tickers if t in ticker_starts]

            conn.execute(
                "UPDATE data_update_log SET total_tickers = ? WHERE id = ?",
                [len(tickers_to_update), run_id],
            )

            if not tickers_to_update:
                conn.execute(
                    """UPDATE data_update_log
                       SET status = 'completed', completed_at = ?, message = ?
                       WHERE id = ?""",
                    [datetime.utcnow(), "Nothing to update", run_id],
                )
                return {
                    "run_id": run_id, "mode": mode,
                    "total": 0, "success": 0, "failed": 0,
                    "duration_seconds": 0.0,
                    "message": "Data is already up to date",
                }

            # --- Step 3: Check for resume ---
            progress = self._load_progress()
            completed_batches: set[int] = set()
            if progress and progress.get("run_id") == run_id:
                completed_batches = set(progress.get("completed_batches", []))
                log.info("data.update.resume", completed_batches=len(completed_batches))

            # --- Step 4: Build smart batches (grouped by date range) ---
            batches = self._build_smart_batches(tickers_to_update, ticker_starts, end_date)

            log.info(
                "data.update.plan",
                tickers=len(tickers_to_update),
                batches=len(batches),
            )

            success_count = 0
            fail_count = 0
            failed_tickers: list[str] = []
            pending_frames: list[pd.DataFrame] = []
            pending_rows = 0

            for batch_num, (batch, batch_start) in enumerate(batches):
                if batch_num in completed_batches:
                    continue

                log.info(
                    "data.update.batch",
                    batch=f"{batch_num + 1}/{len(batches)}",
                    size=len(batch),
                    start=str(batch_start),
                )

                # Provider already handles retries + rate limiting
                try:
                    bars_df = self._provider.get_daily_bars(batch, batch_start, end_date)
                except Exception as e:
                    log.error("data.update.batch_error", batch=batch_num, error=str(e))
                    fail_count += len(batch)
                    failed_tickers.extend(batch)
                    bars_df = None

                if bars_df is not None and not bars_df.empty:
                    pending_frames.append(bars_df)
                    pending_rows += len(bars_df)
                    downloaded_tickers = set(bars_df["ticker"].unique())
                    success_count += len(downloaded_tickers)
                    batch_failed = [t for t in batch if t not in downloaded_tickers]
                    fail_count += len(batch_failed)
                    failed_tickers.extend(batch_failed)
                elif bars_df is not None:
                    # Empty result (no error)
                    fail_count += len(batch)
                    failed_tickers.extend(batch)

                # Flush accumulated data to DB periodically
                if pending_rows >= self._FLUSH_THRESHOLD:
                    combined = pd.concat(pending_frames, ignore_index=True)
                    self._upsert_daily_bars(combined)
                    pending_frames.clear()
                    pending_rows = 0

                # Save progress
                completed_batches.add(batch_num)
                self._save_progress({
                    "run_id": run_id,
                    "mode": mode,
                    "completed_batches": list(completed_batches),
                    "success_count": success_count,
                    "fail_count": fail_count,
                })

            # Flush remaining
            if pending_frames:
                combined = pd.concat(pending_frames, ignore_index=True)
                self._upsert_daily_bars(combined)

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
                    completed_at, success_count, fail_count,
                    json.dumps(failed_tickers[:100]),
                    f"Completed in {duration:.1f}s",
                    run_id,
                ],
            )

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
            """SELECT ticker, date, next_date, gap_days FROM (
                   SELECT ticker, date,
                          LEAD(date) OVER (PARTITION BY ticker ORDER BY date) AS next_date,
                          LEAD(date) OVER (PARTITION BY ticker ORDER BY date) - date AS gap_days
                   FROM daily_bars
               ) sub
               WHERE gap_days > 7
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

    @classmethod
    def _build_smart_batches(
        cls,
        tickers: list[str],
        ticker_starts: dict[str, date],
        end_date: date,
    ) -> list[tuple[list[str], date]]:
        """Group tickers by date-range bucket and build appropriately-sized batches.

        Short ranges (<=30 days) get large batches (500 tickers) because
        yfinance downloads very little data per ticker.
        Medium ranges (30-365 days) get medium batches (200).
        Long ranges (>365 days, typically new tickers) get small batches (50)
        to avoid timeouts and rate limits on heavy downloads.

        Within each bucket, tickers sharing the same start date are grouped
        together so the yfinance request doesn't over-fetch.
        """
        # Classify tickers into buckets
        short: dict[date, list[str]] = {}   # <= 30 days
        medium: dict[date, list[str]] = {}  # 30-365 days
        long: dict[date, list[str]] = {}    # > 365 days

        for t in tickers:
            start = ticker_starts[t]
            days_back = (end_date - start).days
            if days_back <= 30:
                short.setdefault(start, []).append(t)
            elif days_back <= 365:
                medium.setdefault(start, []).append(t)
            else:
                long.setdefault(start, []).append(t)

        batches: list[tuple[list[str], date]] = []

        # Short range: merge all start dates within 7 days of each other
        # to reduce number of API calls
        batches.extend(
            cls._merge_and_chunk(short, cls._SHORT_RANGE_BATCH, merge_window_days=7)
        )
        batches.extend(
            cls._merge_and_chunk(medium, cls._MEDIUM_RANGE_BATCH, merge_window_days=30)
        )
        batches.extend(
            cls._merge_and_chunk(long, cls._LONG_RANGE_BATCH, merge_window_days=0)
        )

        return batches

    @staticmethod
    def _merge_and_chunk(
        date_groups: dict[date, list[str]],
        batch_size: int,
        merge_window_days: int = 0,
    ) -> list[tuple[list[str], date]]:
        """Merge nearby date groups and chunk into batches.

        If merge_window_days > 0, groups whose start dates are within
        that window are merged (using the earliest date as start).
        Then the merged list is chunked into batches of batch_size.
        """
        if not date_groups:
            return []

        sorted_dates = sorted(date_groups.keys())
        merged: list[tuple[date, list[str]]] = []

        current_start = sorted_dates[0]
        current_tickers: list[str] = list(date_groups[sorted_dates[0]])

        for d in sorted_dates[1:]:
            if merge_window_days > 0 and (d - current_start).days <= merge_window_days:
                # Merge: keep earliest start, combine tickers
                current_tickers.extend(date_groups[d])
            else:
                merged.append((current_start, current_tickers))
                current_start = d
                current_tickers = list(date_groups[d])
        merged.append((current_start, current_tickers))

        # Chunk each merged group
        batches: list[tuple[list[str], date]] = []
        for start, tickers_list in merged:
            for i in range(0, len(tickers_list), batch_size):
                batches.append((tickers_list[i : i + batch_size], start))

        return batches
