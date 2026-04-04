"""YFinance-based data provider for US equities."""

from __future__ import annotations

import io
import random
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import requests
import yfinance as yf

from backend.config import settings
from backend.logger import get_logger
from backend.providers.base import DataProvider

log = get_logger(__name__)

_BATCH_SIZE = 500
_BASE_DELAY = 2.0
_JITTER_RANGE = (0.5, 1.5)
_BACKOFF_BASE = 5.0
_BACKOFF_MAX = 120.0
_MAX_RETRIES = 4
_RATE_LIMIT_WAIT = 60

_NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
_OTHER_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"


class YFinanceProvider(DataProvider):
    """Fetch US equity data via yfinance + NASDAQ trader files."""

    def __init__(self) -> None:
        self._cache_dir = settings.project_root / "data" / "cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._stock_list_cache = self._cache_dir / "stock_list.parquet"
        self._cache_ttl = timedelta(hours=24)

    # ------------------------------------------------------------------
    # DataProvider interface
    # ------------------------------------------------------------------

    def get_stock_list(self) -> pd.DataFrame:
        """Download stock lists from nasdaqtrader.com, merge, and return."""
        # Check local cache
        if self._stock_list_cache.exists():
            mtime = self._stock_list_cache.stat().st_mtime
            age = time.time() - mtime
            if age < self._cache_ttl.total_seconds():
                log.info("provider.stock_list.cache_hit")
                return pd.read_parquet(self._stock_list_cache)

        log.info("provider.stock_list.download")
        frames: list[pd.DataFrame] = []

        # --- NASDAQ listed ---
        try:
            df_nasdaq = self._download_nasdaq_file(_NASDAQ_URL)
            df_nasdaq = df_nasdaq.rename(
                columns={"Symbol": "ticker", "Security Name": "name"}
            )
            df_nasdaq["exchange"] = "NASDAQ"
            df_nasdaq["sector"] = ""
            df_nasdaq["status"] = "active"
            # Filter out test issues
            if "Test Issue" in df_nasdaq.columns:
                df_nasdaq = df_nasdaq[df_nasdaq["Test Issue"] == "N"]
            df_nasdaq = df_nasdaq[["ticker", "name", "exchange", "sector", "status"]]
            frames.append(df_nasdaq)
        except Exception as e:
            log.warning("provider.stock_list.nasdaq_failed", error=str(e))

        # --- Other listed (NYSE, AMEX, etc.) ---
        try:
            df_other = self._download_nasdaq_file(_OTHER_URL)
            df_other = df_other.rename(
                columns={
                    "ACT Symbol": "ticker",
                    "Security Name": "name",
                    "Exchange": "exchange",
                }
            )
            # Map exchange codes
            exchange_map = {
                "N": "NYSE",
                "A": "AMEX",
                "P": "ARCA",
                "Z": "BATS",
                "V": "IEXG",
            }
            df_other["exchange"] = df_other["exchange"].map(exchange_map).fillna("OTHER")
            df_other["sector"] = ""
            df_other["status"] = "active"
            if "Test Issue" in df_other.columns:
                df_other = df_other[df_other["Test Issue"] == "N"]
            df_other = df_other[["ticker", "name", "exchange", "sector", "status"]]
            frames.append(df_other)
        except Exception as e:
            log.warning("provider.stock_list.other_failed", error=str(e))

        if not frames:
            raise RuntimeError("Failed to download any stock list")

        df = pd.concat(frames, ignore_index=True)
        # Remove tickers with special characters (warrants, units, etc.)
        df = df[df["ticker"].str.match(r"^[A-Z]{1,5}$")]
        df = df.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

        # Cache
        df.to_parquet(self._stock_list_cache, index=False)
        log.info("provider.stock_list.done", count=len(df))
        return df

    def get_daily_bars(
        self, tickers: List[str], start: date, end: date
    ) -> pd.DataFrame:
        """Download daily bars in batches via yfinance."""
        all_frames: list[pd.DataFrame] = []
        failed_tickers: list[str] = []

        for i in range(0, len(tickers), _BATCH_SIZE):
            batch = tickers[i : i + _BATCH_SIZE]
            batch_num = i // _BATCH_SIZE + 1
            total_batches = (len(tickers) + _BATCH_SIZE - 1) // _BATCH_SIZE
            log.info(
                "provider.daily_bars.batch",
                batch=batch_num,
                total=total_batches,
                size=len(batch),
            )

            df = self._download_batch(batch, start, end)
            if df is not None and not df.empty:
                all_frames.append(df)
            else:
                failed_tickers.extend(batch)

            # Delay between batches (skip after last batch)
            if i + _BATCH_SIZE < len(tickers):
                delay = _BASE_DELAY + random.uniform(*_JITTER_RANGE)
                time.sleep(delay)

        if failed_tickers:
            log.warning(
                "provider.daily_bars.failures",
                count=len(failed_tickers),
                sample=failed_tickers[:10],
            )

        if not all_frames:
            return pd.DataFrame(
                columns=["date", "ticker", "open", "high", "low", "close", "volume", "adj_factor"]
            )

        result = pd.concat(all_frames, ignore_index=True)
        log.info("provider.daily_bars.done", rows=len(result))
        return result

    def get_index_data(
        self, symbol: str, start: date, end: date
    ) -> pd.DataFrame:
        """Download daily bars for a single index/ETF symbol."""
        log.info("provider.index_data", symbol=symbol)
        try:
            raw = yf.download(
                symbol,
                start=str(start),
                end=str(end + timedelta(days=1)),
                auto_adjust=False,
                progress=False,
            )
            if raw.empty:
                return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

            # Flatten MultiIndex columns if present
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)

            df = pd.DataFrame(
                {
                    "date": raw.index.date,
                    "open": raw["Open"].values,
                    "high": raw["High"].values,
                    "low": raw["Low"].values,
                    "close": raw["Close"].values,
                    "volume": raw["Volume"].astype("int64").values,
                }
            )
            return df

        except Exception as e:
            log.error("provider.index_data.error", symbol=symbol, error=str(e))
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _download_nasdaq_file(self, url: str) -> pd.DataFrame:
        """Download and parse a NASDAQ trader pipe-delimited file."""
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        resp = session.get(url, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text), sep="|")
        # Last row is usually "File Creation Time: ..."
        df = df[df.iloc[:, 0].str.len() > 0]
        df = df[~df.iloc[:, 0].str.startswith("File")]
        return df

    def _download_batch(
        self, tickers: list[str], start: date, end: date
    ) -> pd.DataFrame | None:
        """Download a single batch with retry + exponential backoff."""
        for attempt in range(_MAX_RETRIES):
            try:
                ticker_str = " ".join(tickers)
                raw = yf.download(
                    ticker_str,
                    start=str(start),
                    end=str(end + timedelta(days=1)),
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )

                if raw.empty:
                    return None

                return self._parse_multi_ticker(raw, tickers)

            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "rate" in err_str:
                    log.warning("provider.rate_limited", attempt=attempt)
                    time.sleep(_RATE_LIMIT_WAIT)
                else:
                    wait = min(_BACKOFF_BASE * (2**attempt), _BACKOFF_MAX)
                    log.warning(
                        "provider.batch_error",
                        attempt=attempt,
                        wait=wait,
                        error=str(e),
                    )
                    time.sleep(wait)

        log.error("provider.batch_exhausted", tickers_count=len(tickers))
        return None

    def _parse_multi_ticker(
        self, raw: pd.DataFrame, tickers: list[str]
    ) -> pd.DataFrame:
        """Convert yfinance multi-ticker output to long-format DataFrame."""
        frames: list[pd.DataFrame] = []

        if len(tickers) == 1:
            # Single-ticker download: columns are just OHLCV
            ticker = tickers[0]
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            try:
                close_vals = raw["Close"].values
                adj_close_vals = raw["Adj Close"].values
                adj_factor = adj_close_vals / close_vals
                adj_factor = pd.Series(adj_factor).fillna(1.0).values

                df = pd.DataFrame(
                    {
                        "date": raw.index.date,
                        "ticker": ticker,
                        "open": raw["Open"].values,
                        "high": raw["High"].values,
                        "low": raw["Low"].values,
                        "close": close_vals,
                        "volume": raw["Volume"].astype("int64").values,
                        "adj_factor": adj_factor,
                    }
                )
                frames.append(df)
            except Exception as e:
                log.warning("provider.parse_single_error", ticker=ticker, error=str(e))
        else:
            # Multi-ticker download: MultiIndex columns (Ticker, Price)
            for ticker in tickers:
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        if ticker not in raw.columns.get_level_values(0):
                            continue
                        sub = raw[ticker].dropna(how="all")
                    else:
                        continue

                    if sub.empty:
                        continue

                    close_vals = sub["Close"].values
                    adj_close_vals = sub["Adj Close"].values
                    adj_factor = adj_close_vals / close_vals
                    adj_factor = pd.Series(adj_factor).fillna(1.0).values

                    df = pd.DataFrame(
                        {
                            "date": sub.index.date,
                            "ticker": ticker,
                            "open": sub["Open"].values,
                            "high": sub["High"].values,
                            "low": sub["Low"].values,
                            "close": close_vals,
                            "volume": sub["Volume"].astype("int64").values,
                            "adj_factor": adj_factor,
                        }
                    )
                    frames.append(df)
                except Exception as e:
                    log.warning("provider.parse_error", ticker=ticker, error=str(e))

        if not frames:
            return pd.DataFrame(
                columns=["date", "ticker", "open", "high", "low", "close", "volume", "adj_factor"]
            )
        return pd.concat(frames, ignore_index=True)
