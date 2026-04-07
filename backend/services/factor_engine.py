"""Factor computation engine – compute factor values across a universe of tickers."""

from __future__ import annotations

import math
from datetime import date as date_type, datetime

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.factors.loader import load_factor_from_code
from backend.logger import get_logger
from backend.services.calendar_service import snap_to_trading_day
from backend.services.factor_service import FactorService

log = get_logger(__name__)

# How many tickers to process per batch to manage memory.
_BATCH_SIZE = 200


class FactorEngine:
    """Compute factor values for a universe of tickers, with caching."""

    def __init__(self) -> None:
        self._factor_service = FactorService()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_factor(
        self,
        factor_id: str,
        universe_tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Compute factor values and return DataFrame (index=dates, columns=tickers).

        Checks cache first, only computes missing (ticker, date) pairs,
        then writes new values back to the cache.

        Args:
            factor_id: ID of the factor definition.
            universe_tickers: List of ticker symbols.
            start_date: Start date string (YYYY-MM-DD).
            end_date: End date string (YYYY-MM-DD).

        Returns:
            DataFrame with DatetimeIndex rows, ticker columns, factor values.
        """
        if not universe_tickers:
            return pd.DataFrame()

        # Snap user-provided dates to nearest trading days
        from datetime import date as _date
        _start = _date.fromisoformat(start_date)
        _end = _date.fromisoformat(end_date)
        _start = snap_to_trading_day(_start, direction="forward")
        _end = snap_to_trading_day(_end, direction="backward")
        start_date = str(_start)
        end_date = str(_end)

        if _start > _end:
            log.warning("factor_engine.compute.invalid_range", start=start_date, end=end_date)
            return pd.DataFrame()

        # 1. Load factor definition and compile
        factor_def = self._factor_service.get_factor(factor_id)
        source_code = factor_def["source_code"]
        factor_instance = load_factor_from_code(source_code)

        log.info(
            "factor_engine.compute.start",
            factor_id=factor_id,
            factor_name=factor_def["name"],
            tickers=len(universe_tickers),
            start=start_date,
            end=end_date,
        )

        # 2. Load cached values
        cached_df = self._load_cached_values(factor_id, universe_tickers, start_date, end_date)

        # 3. Determine which tickers need computation
        tickers_to_compute = self._find_missing_tickers(
            cached_df, universe_tickers, start_date, end_date
        )

        log.info(
            "factor_engine.compute.cache_status",
            cached_tickers=len(universe_tickers) - len(tickers_to_compute),
            to_compute=len(tickers_to_compute),
        )

        # 4. Compute missing values in batches
        new_values_frames: list[pd.DataFrame] = []
        total_batches = math.ceil(len(tickers_to_compute) / _BATCH_SIZE)

        for batch_idx in range(0, len(tickers_to_compute), _BATCH_SIZE):
            batch_num = batch_idx // _BATCH_SIZE + 1
            batch_tickers = tickers_to_compute[batch_idx : batch_idx + _BATCH_SIZE]

            log.info(
                "factor_engine.compute.batch",
                batch=f"{batch_num}/{total_batches}",
                size=len(batch_tickers),
            )

            batch_result = self._compute_batch(
                factor_instance, batch_tickers, start_date, end_date
            )
            if not batch_result.empty:
                new_values_frames.append(batch_result)

        # 5. Combine cached + newly computed
        all_frames: list[pd.DataFrame] = []
        if not cached_df.empty:
            all_frames.append(cached_df)
        if new_values_frames:
            new_df = pd.concat(new_values_frames, axis=1)
            all_frames.append(new_df)

            # 6. Write new values to cache
            self._write_cache(factor_id, new_df)

        if not all_frames:
            log.warning("factor_engine.compute.no_data", factor_id=factor_id)
            return pd.DataFrame()

        result = pd.concat(all_frames, axis=1)
        # Ensure no duplicate columns (in case of overlap)
        result = result.loc[:, ~result.columns.duplicated()]
        # Filter to requested date range
        result = result.sort_index()
        result = result.loc[start_date:end_date]

        log.info(
            "factor_engine.compute.done",
            factor_id=factor_id,
            shape=result.shape,
        )
        return result

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _load_cached_values(
        self,
        factor_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Load cached factor values from factor_values_cache table.

        Returns a DataFrame with DatetimeIndex and ticker columns.
        """
        conn = get_connection()

        # Build IN clause for tickers
        placeholders = ",".join(f"'{t}'" for t in tickers)
        query = f"""
            SELECT ticker, date, value
            FROM factor_values_cache
            WHERE factor_id = ?
              AND ticker IN ({placeholders})
              AND date >= ?
              AND date <= ?
            ORDER BY date, ticker
        """
        rows = conn.execute(query, [factor_id, start_date, end_date]).fetchdf()

        if rows.empty:
            return pd.DataFrame()

        # Pivot to (date x ticker)
        pivot = rows.pivot(index="date", columns="ticker", values="value")
        pivot.index = pd.to_datetime(pivot.index)
        return pivot

    def _find_missing_tickers(
        self,
        cached_df: pd.DataFrame,
        all_tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> list[str]:
        """Determine which tickers need fresh computation.

        A ticker needs computation if:
        - It has no cached data at all
        - The cached data is all NaN
        - The cached date range does not cover the requested [start, end]
        """
        if cached_df.empty:
            return list(all_tickers)

        cached_tickers = set(cached_df.columns)
        req_start = pd.Timestamp(start_date)
        req_end = pd.Timestamp(end_date)
        missing: list[str] = []

        for t in all_tickers:
            if t not in cached_tickers:
                missing.append(t)
                continue

            col = cached_df[t]
            valid = col.dropna()
            if valid.empty:
                missing.append(t)
                continue

            # Check that cached data covers the requested date range
            cached_min = valid.index.min()
            cached_max = valid.index.max()
            # Allow 5 trading-day tolerance for edge alignment
            if cached_min > req_start + pd.Timedelta(days=7) or cached_max < req_end - pd.Timedelta(days=7):
                missing.append(t)

        return missing

    def _write_cache(self, factor_id: str, df: pd.DataFrame) -> None:
        """Write computed factor values to the cache table."""
        conn = get_connection()

        # Convert wide DataFrame to long format for insertion
        records: list[tuple] = []
        for ticker in df.columns:
            series = df[ticker].dropna()
            for dt, val in series.items():
                date_str = pd.Timestamp(dt).strftime("%Y-%m-%d")
                records.append((factor_id, ticker, date_str, float(val)))

        if not records:
            return

        # Batch insert using INSERT OR REPLACE
        batch_size = 5000
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            insert_df = pd.DataFrame(
                batch, columns=["factor_id", "ticker", "date", "value"]
            )
            conn.execute(
                "CREATE OR REPLACE TEMP TABLE _tmp_fv AS SELECT * FROM insert_df"
            )
            conn.execute(
                """INSERT OR REPLACE INTO factor_values_cache
                   (factor_id, ticker, date, value)
                   SELECT factor_id, ticker, date, value FROM _tmp_fv"""
            )
            conn.execute("DROP TABLE IF EXISTS _tmp_fv")

        log.info(
            "factor_engine.cache.written",
            factor_id=factor_id,
            records=len(records),
        )

    # ------------------------------------------------------------------
    # Computation helpers
    # ------------------------------------------------------------------

    def _compute_batch(
        self,
        factor_instance,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Compute factor values for a batch of tickers.

        Returns a DataFrame with DatetimeIndex and ticker columns.
        Skips tickers that fail and logs warnings.
        """
        conn = get_connection()

        # Load OHLCV data for all tickers in this batch.
        # We request extra history before start_date for warm-up periods.
        # Many indicators (e.g. 200-day MA) need ~250 bars of warm-up.
        from datetime import timedelta
        sd = start_date if isinstance(start_date, date_type) else date_type.fromisoformat(str(start_date))
        ed = end_date if isinstance(end_date, date_type) else date_type.fromisoformat(str(end_date))
        warm_start = sd - timedelta(days=400)
        warm_up_query = f"""
            SELECT ticker, date, open, high, low, close, volume
            FROM daily_bars
            WHERE ticker IN ({','.join(f"'{t}'" for t in tickers)})
              AND date >= ?
              AND date <= ?
            ORDER BY ticker, date
        """
        try:
            all_bars = conn.execute(warm_up_query, [warm_start, ed]).fetchdf()
        except Exception as exc:
            log.error("factor_engine.compute.query_failed", error=str(exc))
            return pd.DataFrame()

        if all_bars.empty:
            return pd.DataFrame()

        result_series: dict[str, pd.Series] = {}

        for ticker, grp in all_bars.groupby("ticker"):
            try:
                ohlcv = grp.set_index("date").sort_index()
                ohlcv.index = pd.to_datetime(ohlcv.index)
                ohlcv = ohlcv[["open", "high", "low", "close", "volume"]].astype(float)

                if ohlcv.empty or len(ohlcv) < 5:
                    log.debug("factor_engine.compute.skip_short", ticker=ticker, rows=len(ohlcv))
                    continue

                factor_values = factor_instance.compute(ohlcv)

                # Filter to the requested date range (drop warm-up period)
                factor_values = factor_values.loc[start_date:end_date]
                factor_values.name = ticker

                result_series[ticker] = factor_values

            except Exception as exc:
                log.warning(
                    "factor_engine.compute.ticker_failed",
                    ticker=ticker,
                    error=str(exc),
                )
                continue

        if not result_series:
            return pd.DataFrame()

        df = pd.DataFrame(result_series)
        df.index = pd.to_datetime(df.index)
        return df
