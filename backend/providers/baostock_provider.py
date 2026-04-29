"""BaoStock data provider for China A-share daily data."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Iterable, List

import pandas as pd

from backend.logger import get_logger
from backend.providers.base import DataProvider

log = get_logger(__name__)

_HISTORY_FIELDS = ",".join(
    [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "tradestatus",
        "pctChg",
        "isST",
    ]
)


class BaoStockProvider(DataProvider):
    """Fetch China A-share data through BaoStock."""

    market = "CN"

    def __init__(self, client=None) -> None:
        self._client = client

    def get_stock_list(self) -> pd.DataFrame:
        with self._session() as bs:
            result = bs.query_all_stock(day=None)
            self._ensure_success(result, "query_all_stock")
            raw = self._result_to_frame(result)

        if raw.empty:
            return pd.DataFrame(columns=["market", "ticker", "name", "exchange", "sector", "status"])

        df = raw.rename(columns={"code": "ticker", "code_name": "name"}).copy()
        df["market"] = "CN"
        df["exchange"] = df["ticker"].map(_exchange_from_code)
        df["sector"] = ""
        df["status"] = df.get("tradeStatus", "1").map(lambda v: "active" if str(v) == "1" else "inactive")
        df = df[df["ticker"].map(_is_a_share_code)]
        return df[["market", "ticker", "name", "exchange", "sector", "status"]].reset_index(drop=True)

    def get_daily_bars(
        self, tickers: List[str], start: date, end: date
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        with self._session() as bs:
            for ticker in tickers:
                result = bs.query_history_k_data_plus(
                    ticker,
                    _HISTORY_FIELDS,
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                    frequency="d",
                    adjustflag="2",
                )
                self._ensure_success(result, f"query_history_k_data_plus({ticker})")
                frame = self._history_to_bars(ticker, result)
                if not frame.empty:
                    frames.append(frame)

        if not frames:
            return _empty_daily_bars()
        return pd.concat(frames, ignore_index=True)

    def get_index_data(
        self, symbol: str, start: date, end: date
    ) -> pd.DataFrame:
        bars = self.get_daily_bars([symbol], start, end)
        if bars.empty:
            return pd.DataFrame(columns=["market", "symbol", "date", "open", "high", "low", "close", "volume"])
        return (
            bars.rename(columns={"ticker": "symbol"})[
                ["market", "symbol", "date", "open", "high", "low", "close", "volume"]
            ]
            .reset_index(drop=True)
        )

    @contextmanager
    def _session(self):
        bs = self._client or _load_baostock()
        login = bs.login()
        self._ensure_success(login, "baostock.login")
        try:
            yield bs
        finally:
            try:
                bs.logout()
            except Exception:
                log.warning("baostock.logout_failed")

    def _history_to_bars(self, ticker: str, result) -> pd.DataFrame:
        raw = self._result_to_frame(result)
        if raw.empty:
            return _empty_daily_bars()

        df = raw.rename(columns={"code": "ticker"}).copy()
        df["market"] = "CN"
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"]).dt.date
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
        df["adj_factor"] = 1.0
        if "tradestatus" in df.columns:
            df = df[df["tradestatus"].astype(str) == "1"]
        return df[["market", "date", "ticker", "open", "high", "low", "close", "volume", "adj_factor"]].reset_index(drop=True)

    def _result_to_frame(self, result) -> pd.DataFrame:
        rows: list[list[str]] = []
        while result.next():
            rows.append(result.get_row_data())
        return pd.DataFrame(rows, columns=list(result.fields))

    def _ensure_success(self, result, action: str) -> None:
        if getattr(result, "error_code", "0") != "0":
            message = getattr(result, "error_msg", "")
            raise RuntimeError(f"BaoStock {action} failed: {result.error_code} {message}")


def _load_baostock():
    try:
        import baostock as bs
    except Exception as exc:
        raise RuntimeError("BaoStock provider is not available; install the 'baostock' package") from exc
    return bs


def _empty_daily_bars() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["market", "date", "ticker", "open", "high", "low", "close", "volume", "adj_factor"]
    )


def _exchange_from_code(code: str) -> str:
    prefix = str(code).split(".", 1)[0].lower()
    return {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(prefix, prefix.upper())


def _is_a_share_code(code: str) -> bool:
    code = str(code).lower()
    return code.startswith(("sh.60", "sh.68", "sz.00", "sz.30", "bj."))

