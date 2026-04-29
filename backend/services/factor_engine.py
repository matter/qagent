"""Factor computation engine – compute factor values across a universe of tickers."""

from __future__ import annotations

import math
from datetime import date as date_type, timedelta

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.factors.loader import load_factor_from_code
from backend.logger import get_logger
from backend.services.calendar_service import snap_to_trading_day
from backend.services.factor_service import FactorService
from backend.services.market_context import normalize_market, normalize_ticker

log = get_logger(__name__)

# How many tickers to process per batch to manage memory.
_BATCH_SIZE = 500


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
        market: str | None = None,
    ) -> pd.DataFrame:
        """Compute factor values and return DataFrame (index=dates, columns=tickers).

        Checks cache first, only computes missing (ticker, date) pairs,
        then writes new values back to the cache.
        """
        if not universe_tickers:
            return pd.DataFrame()
        resolved_market = normalize_market(market)
        universe_tickers = [normalize_ticker(t, resolved_market) for t in universe_tickers]

        # Snap user-provided dates to nearest trading days
        _start = date_type.fromisoformat(start_date)
        _end = date_type.fromisoformat(end_date)
        _start = snap_to_trading_day(_start, direction="forward", market=resolved_market)
        _end = snap_to_trading_day(_end, direction="backward", market=resolved_market)
        start_date = str(_start)
        end_date = str(_end)

        if _start > _end:
            log.warning("factor_engine.compute.invalid_range", start=start_date, end=end_date)
            return pd.DataFrame()

        # 1. Load factor definition and compile
        factor_def = self._factor_service.get_factor(factor_id, market=resolved_market)
        source_code = factor_def["source_code"]
        factor_instance = load_factor_from_code(source_code)

        log.info(
            "factor_engine.compute.start",
            factor_id=factor_id,
            market=resolved_market,
            factor_name=factor_def["name"],
            tickers=len(universe_tickers),
            start=start_date,
            end=end_date,
        )

        # 2. Lightweight cache coverage check (metadata only, no full data load)
        tickers_to_compute = self._find_uncovered_tickers(
            factor_id, universe_tickers, start_date, end_date, market=resolved_market
        )
        cached_tickers = [t for t in universe_tickers if t not in set(tickers_to_compute)]

        log.info(
            "factor_engine.compute.cache_status",
            cached_tickers=len(cached_tickers),
            to_compute=len(tickers_to_compute),
        )

        # 3. Load cached data only for covered tickers (skip if none)
        cached_df = pd.DataFrame()
        if cached_tickers:
            cached_df = self._load_cached_values(
                factor_id, cached_tickers, start_date, end_date, market=resolved_market
            )

        # 4. Compute missing values in batches
        new_values_frames: list[pd.DataFrame] = []
        total_batches = math.ceil(len(tickers_to_compute) / _BATCH_SIZE) if tickers_to_compute else 0

        for batch_idx in range(0, len(tickers_to_compute), _BATCH_SIZE):
            batch_num = batch_idx // _BATCH_SIZE + 1
            batch_tickers = tickers_to_compute[batch_idx : batch_idx + _BATCH_SIZE]

            log.info(
                "factor_engine.compute.batch",
                batch=f"{batch_num}/{total_batches}",
                size=len(batch_tickers),
            )

            batch_result = self._compute_batch(
                factor_instance, batch_tickers, start_date, end_date, market=resolved_market
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
            self._write_cache(factor_id, new_df, market=resolved_market)

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
            market=resolved_market,
            shape=result.shape,
        )
        return result

    def load_cached_factors_bulk(
        self,
        factor_ids: list[str],
        tickers: list[str],
        start_date: str,
        end_date: str,
        market: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Load cached values for multiple factors in a single DB query.

        Returns dict[factor_id -> DataFrame(dates x tickers)].
        Only returns factors that have cached data; missing factors are omitted.
        """
        if not factor_ids or not tickers:
            return {}
        resolved_market = normalize_market(market)
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]

        conn = get_connection()
        fid_placeholders = ",".join("?" for _ in factor_ids)
        tk_placeholders = ",".join("?" for _ in tickers)

        query = f"""
            SELECT factor_id, ticker, date, value
            FROM factor_values_cache
            WHERE market = ?
              AND factor_id IN ({fid_placeholders})
              AND ticker IN ({tk_placeholders})
              AND date >= ? AND date <= ?
            ORDER BY factor_id, date, ticker
        """
        df = conn.execute(
            query,
            [resolved_market, *factor_ids, *tickers, start_date, end_date],
        ).fetchdf()
        if df.empty:
            return {}

        df["date"] = pd.to_datetime(df["date"])
        result: dict[str, pd.DataFrame] = {}
        for fid, grp in df.groupby("factor_id"):
            pivot = grp.pivot(index="date", columns="ticker", values="value")
            result[str(fid)] = pivot

        log.info(
            "factor_engine.bulk_cache.loaded",
            market=resolved_market,
            requested=len(factor_ids),
            loaded=len(result),
            rows=len(df),
        )
        return result

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _find_uncovered_tickers(
        self,
        factor_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        market: str | None = None,
    ) -> list[str]:
        """Fast metadata-only check: which tickers lack sufficient cache coverage?

        Queries only MIN/MAX date and non-null count per ticker from the cache
        table — never loads the actual values.  This is O(index scan) even for
        very large universes.
        """
        resolved_market = normalize_market(market)
        conn = get_connection()

        # Single aggregate query — DuckDB handles the GROUP BY efficiently
        # even with the PRIMARY KEY (factor_id, ticker, date).
        meta_rows = conn.execute(
            """SELECT ticker, MIN(date) AS min_d, MAX(date) AS max_d,
                      COUNT(*) AS cnt
               FROM factor_values_cache
               WHERE market = ?
                 AND factor_id = ?
                 AND date >= ? AND date <= ?
               GROUP BY ticker""",
            [resolved_market, factor_id, start_date, end_date],
        ).fetchall()

        # Build lookup: ticker -> (min_date, max_date, count)
        coverage: dict[str, tuple] = {}
        for r in meta_rows:
            coverage[r[0]] = (r[1], r[2], r[3])

        req_start = date_type.fromisoformat(start_date)
        req_end = date_type.fromisoformat(end_date)
        tolerance = timedelta(days=7)

        missing: list[str] = []
        for t in tickers:
            info = coverage.get(t)
            if info is None:
                missing.append(t)
                continue
            min_d, max_d, cnt = info
            if not isinstance(min_d, date_type):
                min_d = date_type.fromisoformat(str(min_d))
            if not isinstance(max_d, date_type):
                max_d = date_type.fromisoformat(str(max_d))
            # Must cover the requested range (with tolerance) and have
            # a reasonable number of rows (at least 1 row per 2 calendar days)
            if min_d > req_start + tolerance or max_d < req_end - tolerance:
                missing.append(t)
            elif cnt < 5:
                missing.append(t)

        return missing

    def _load_cached_values(
        self,
        factor_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        market: str | None = None,
    ) -> pd.DataFrame:
        """Load cached factor values from factor_values_cache table.

        Returns a DataFrame with DatetimeIndex and ticker columns.
        Only called for tickers known to have coverage.
        """
        resolved_market = normalize_market(market)
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]
        conn = get_connection()

        # Build IN clause for tickers
        placeholders = ",".join("?" for _ in tickers)
        query = f"""
            SELECT ticker, date, value
            FROM factor_values_cache
            WHERE market = ?
              AND factor_id = ?
              AND ticker IN ({placeholders})
              AND date >= ?
              AND date <= ?
            ORDER BY date, ticker
        """
        rows = conn.execute(
            query,
            [resolved_market, factor_id, *tickers, start_date, end_date],
        ).fetchdf()

        if rows.empty:
            return pd.DataFrame()

        # Pivot to (date x ticker)
        pivot = rows.pivot(index="date", columns="ticker", values="value")
        pivot.index = pd.to_datetime(pivot.index)
        return pivot

    def _write_cache(self, factor_id: str, df: pd.DataFrame, market: str | None = None) -> None:
        """Write computed factor values to the cache table using vectorized ops."""
        if df.empty:
            return
        resolved_market = normalize_market(market)

        conn = get_connection()

        # Vectorized wide -> long.  stack() without dropna for pandas 2.1+
        # compatibility (the new stack implementation removed the dropna param);
        # filter NaN rows explicitly instead.
        long = df.stack().reset_index()
        long.columns = ["date", "ticker", "value"]
        long = long.dropna(subset=["value"])
        long["market"] = resolved_market
        long["factor_id"] = factor_id
        long["date"] = pd.to_datetime(long["date"]).dt.strftime("%Y-%m-%d")
        long["ticker"] = long["ticker"].map(lambda t: normalize_ticker(t, resolved_market))
        long["value"] = long["value"].astype(float)
        long = long[["market", "factor_id", "ticker", "date", "value"]]

        if long.empty:
            return

        # Single bulk upsert via DuckDB DataFrame scan
        conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_fv AS SELECT * FROM long")
        conn.execute(
            """INSERT OR REPLACE INTO factor_values_cache
               (market, factor_id, ticker, date, value)
               SELECT market, factor_id, ticker, date, value FROM _tmp_fv"""
        )
        conn.execute("DROP TABLE IF EXISTS _tmp_fv")

        log.info(
            "factor_engine.cache.written",
            factor_id=factor_id,
            market=resolved_market,
            records=len(long),
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
        market: str | None = None,
    ) -> pd.DataFrame:
        """Compute factor values for a batch of tickers.

        Returns a DataFrame with DatetimeIndex and ticker columns.
        Skips tickers that fail and logs warnings.
        """
        resolved_market = normalize_market(market)
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]
        conn = get_connection()

        # Load OHLCV data for all tickers in this batch.
        # We request extra history before start_date for warm-up periods.
        # Many indicators (e.g. 200-day MA) need ~250 bars of warm-up.
        sd = date_type.fromisoformat(str(start_date))
        ed = date_type.fromisoformat(str(end_date))
        warm_start = sd - timedelta(days=400)
        placeholders = ",".join("?" for _ in tickers)
        warm_up_query = f"""
            SELECT ticker, date, open, high, low, close, volume
            FROM daily_bars
            WHERE market = ?
              AND ticker IN ({placeholders})
              AND date >= ?
              AND date <= ?
            ORDER BY ticker, date
        """
        try:
            all_bars = conn.execute(
                warm_up_query,
                [resolved_market, *tickers, warm_start, ed],
            ).fetchdf()
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
