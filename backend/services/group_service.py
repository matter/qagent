"""Stock group management service."""

from __future__ import annotations

import uuid
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import infer_ticker_market, normalize_market, normalize_ticker

log = get_logger(__name__)

_BUILTIN_ALL_MARKET = "all_market"
_BUILTIN_ALL_MARKET_NAME = "全市场"

# Built-in index groups: id -> (name, description, filter_expr or None, tickers_fetcher)
_BUILTIN_INDEX_GROUPS = {
    "sp500": ("标普500", "S&P 500 成分股", "sp500"),
    "sp400": ("标普中型股400", "S&P MidCap 400 成分股", "sp400"),
    "nasdaq100": ("纳斯达克100", "NASDAQ 100 成分股", "nasdaq100"),
    "russell3000": ("罗素3000", "Russell 3000 成分股", "russell3000"),
}

_US_ALIAS_INDEX_GROUPS = {
    "us_sp500": ("美股标普500", "S&P 500 成分股", "sp500"),
    "us_nasdaq100": ("美股纳斯达克100", "NASDAQ 100 成分股", "nasdaq100"),
}

_CN_BUILTIN_GROUPS = {
    "cn_all_a": ("A股全市场", "包含所有已录入 A 股", "status = 'active'"),
    "cn_hs300": ("沪深300", "沪深300成分股（待接入成分股刷新）", None),
}

# Safe columns that filter expressions may reference.
_ALLOWED_FILTER_COLUMNS = {"ticker", "name", "exchange", "sector", "status"}


def _validate_filter_expr(expr: str) -> str:
    """Basic validation for filter expressions used as SQL WHERE clauses."""
    if not expr or not expr.strip():
        raise ValueError("filter_expr must not be empty")

    forbidden = [";", "--", "/*", "*/", "DROP", "DELETE", "INSERT", "UPDATE",
                 "ALTER", "CREATE", "EXEC", "UNION", "INTO"]
    upper = expr.upper()
    for token in forbidden:
        if token in upper:
            raise ValueError(f"filter expression contains forbidden token: {token}")

    return expr.strip()


_WIKI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
}


def _wiki_read_html(url: str) -> list[pd.DataFrame]:
    """Download a Wikipedia page with proper User-Agent and parse HTML tables."""
    resp = requests.get(url, headers=_WIKI_HEADERS, timeout=20)
    resp.raise_for_status()
    return pd.read_html(StringIO(resp.text))


def _fetch_index_tickers(index_id: str) -> list[str]:
    """Fetch latest index constituents from Wikipedia. Returns ticker list."""
    try:
        if index_id == "sp500":
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            tables = _wiki_read_html(url)
            df = tables[0]
            tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
        elif index_id == "nasdaq100":
            url = "https://en.wikipedia.org/wiki/Nasdaq-100"
            tables = _wiki_read_html(url)
            # Find the table with a "Ticker" or "Symbol" column
            df = None
            col = None
            for t in tables:
                str_cols = [c for c in t.columns if isinstance(c, str)]
                str_cols_lower = [c.lower() for c in str_cols]
                if "ticker" in str_cols_lower:
                    df = t
                    col = str_cols[str_cols_lower.index("ticker")]
                    break
                if "symbol" in str_cols_lower:
                    df = t
                    col = str_cols[str_cols_lower.index("symbol")]
                    break
            if df is None or col is None:
                log.warning("group.index_fetch.no_table", index=index_id)
                return []
            tickers = df[col].str.replace(".", "-", regex=False).tolist()
        elif index_id == "sp400":
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
            tables = _wiki_read_html(url)
            df = tables[0]
            # Find Symbol/Ticker column
            col = None
            for c in df.columns:
                if isinstance(c, str) and c.lower() in ("symbol", "ticker"):
                    col = c
                    break
            if col is None:
                log.warning("group.index_fetch.no_table", index=index_id)
                return []
            tickers = df[col].str.replace(".", "-", regex=False).tolist()
        elif index_id == "russell3000":
            # Russell 3000 is ~3000 stocks; approximate with all active NYSE+NASDAQ stocks
            log.info("group.russell3000.using_filter", msg="Using all active stocks as Russell 3000 proxy")
            return []  # Will be created as filter group instead
        else:
            return []

        # Clean up tickers
        tickers = [t.strip() for t in tickers if isinstance(t, str) and t.strip()]
        log.info("group.index_fetch.done", index=index_id, count=len(tickers))
        return tickers

    except Exception as e:
        log.warning("group.index_fetch.failed", index=index_id, error=str(e))
        return []


class GroupService:
    """CRUD operations for stock groups."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_builtins(self, market: str | None = None) -> None:
        """Create built-in groups if they do not exist."""
        resolved_market = normalize_market(market)
        conn = get_connection()

        if resolved_market == "US":
            self._ensure_filter_builtin(
                conn,
                _BUILTIN_ALL_MARKET,
                resolved_market,
                _BUILTIN_ALL_MARKET_NAME,
                "包含所有已录入股票",
                "1=1",
            )
            self._ensure_filter_builtin(
                conn,
                "us_all_market",
                resolved_market,
                "美股全市场",
                "包含所有已录入美股",
                "1=1",
            )

            for group_id, (name, desc, fetch_key) in {
                **_BUILTIN_INDEX_GROUPS,
                **_US_ALIAS_INDEX_GROUPS,
            }.items():
                existing = conn.execute(
                    "SELECT id FROM stock_groups WHERE id = ?", [group_id]
                ).fetchone()
                if existing is not None:
                    continue

                tickers = _fetch_index_tickers(fetch_key)

                if fetch_key == "russell3000" and not tickers:
                    filter_expr = "exchange IN ('NYSE', 'NASDAQ') AND status = 'active'"
                    conn.execute(
                        """INSERT INTO stock_groups (id, market, name, description, group_type, filter_expr)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [
                            group_id,
                            resolved_market,
                            name,
                            desc + "（近似：全部活跃 NYSE+NASDAQ 股票）",
                            "builtin",
                            filter_expr,
                        ],
                    )
                    self._evaluate_filter(group_id, filter_expr, market=resolved_market)
                elif tickers:
                    conn.execute(
                        """INSERT INTO stock_groups (id, market, name, description, group_type, filter_expr)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [group_id, resolved_market, name, desc, "builtin", None],
                    )
                    self._set_members(group_id, tickers, market=resolved_market, validate_members=False)
                else:
                    conn.execute(
                        """INSERT INTO stock_groups (id, market, name, description, group_type, filter_expr)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [
                            group_id,
                            resolved_market,
                            name,
                            desc + "（成分股获取失败，请手动刷新）",
                            "builtin",
                            None,
                        ],
                    )

                log.info("group.builtin_created", market=resolved_market, name=name, tickers=len(tickers) if tickers else 0)
            return

        for group_id, (name, desc, filter_expr) in _CN_BUILTIN_GROUPS.items():
            if filter_expr:
                self._ensure_filter_builtin(conn, group_id, resolved_market, name, desc, filter_expr)
            else:
                existing = conn.execute(
                    "SELECT id FROM stock_groups WHERE id = ?", [group_id]
                ).fetchone()
                if existing is None:
                    conn.execute(
                        """INSERT INTO stock_groups (id, market, name, description, group_type, filter_expr)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [group_id, resolved_market, name, desc, "builtin", None],
                    )
                    log.info("group.builtin_created", market=resolved_market, name=name, tickers=0)

    def create_group(
        self,
        name: str,
        description: str | None = None,
        group_type: str = "manual",
        tickers: list[str] | None = None,
        filter_expr: str | None = None,
        market: str | None = None,
    ) -> dict:
        """Create a new stock group."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        group_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()

        if group_type == "filter":
            if not filter_expr:
                raise ValueError("filter_expr is required for filter-type groups")
            filter_expr = _validate_filter_expr(filter_expr)

        conn.execute(
            """INSERT INTO stock_groups (id, market, name, description, group_type, filter_expr, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [group_id, resolved_market, name, description, group_type, filter_expr, now, now],
        )

        # For manual groups, insert explicit members.
        if group_type == "manual" and tickers:
            self._set_members(group_id, tickers, market=resolved_market)

        # For filter groups, evaluate immediately.
        if group_type == "filter" and filter_expr:
            self._evaluate_filter(group_id, filter_expr, market=resolved_market)

        log.info("group.created", id=group_id, market=resolved_market, name=name, type=group_type)
        return self.get_group(group_id, market=resolved_market)

    def update_group(
        self,
        group_id: str,
        name: str | None = None,
        description: str | None = None,
        tickers: list[str] | None = None,
        filter_expr: str | None = None,
        market: str | None = None,
    ) -> dict:
        """Update an existing group."""
        conn = get_connection()
        group = self._fetch_group_row(group_id, market)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        if group["group_type"] == "builtin":
            raise ValueError("Cannot modify built-in groups")

        now = datetime.utcnow()
        sets: list[str] = ["updated_at = ?"]
        params: list = [now]

        if name is not None:
            sets.append("name = ?")
            params.append(name)
        if description is not None:
            sets.append("description = ?")
            params.append(description)
        if filter_expr is not None:
            filter_expr = _validate_filter_expr(filter_expr)
            sets.append("filter_expr = ?")
            params.append(filter_expr)

        params.append(group_id)
        conn.execute(
            f"UPDATE stock_groups SET {', '.join(sets)} WHERE id = ?", params
        )

        if group["group_type"] == "manual" and tickers is not None:
            self._set_members(group_id, tickers, market=group["market"])
        if group["group_type"] == "filter" and filter_expr is not None:
            self._evaluate_filter(group_id, filter_expr, market=group["market"])

        log.info("group.updated", id=group_id)
        return self.get_group(group_id, market=group["market"])

    def delete_group(self, group_id: str, market: str | None = None) -> None:
        """Delete a group and its membership records."""
        conn = get_connection()
        group = self._fetch_group_row(group_id, market)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        if group["group_type"] == "builtin":
            raise ValueError("Cannot delete built-in groups")

        conn.execute(
            "DELETE FROM stock_group_members WHERE group_id = ? AND market = ?",
            [group_id, group["market"]],
        )
        conn.execute("DELETE FROM stock_groups WHERE id = ?", [group_id])
        log.info("group.deleted", id=group_id)

    def get_group(self, group_id: str, market: str | None = None) -> dict:
        """Return group info including member tickers."""
        group = self._fetch_group_row(group_id, market)
        if group is None:
            raise ValueError(f"Group {group_id} not found")

        tickers = self.get_group_tickers(group_id, market=group["market"])
        return {**group, "tickers": tickers, "member_count": len(tickers)}

    def list_groups(self, market: str | None = None) -> list[dict]:
        """List all groups with member counts."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        rows = conn.execute(
            """SELECT g.id, g.market, g.name, g.description, g.group_type, g.filter_expr,
                      g.created_at, g.updated_at, COUNT(m.ticker) AS member_count
               FROM stock_groups g
               LEFT JOIN stock_group_members m
                    ON g.id = m.group_id AND g.market = m.market
               WHERE g.market = ?
               GROUP BY g.id, g.market, g.name, g.description, g.group_type, g.filter_expr,
                        g.created_at, g.updated_at
               ORDER BY g.created_at""",
            [resolved_market],
        ).fetchall()

        return [
            {
                "id": r[0],
                "market": r[1],
                "name": r[2],
                "description": r[3],
                "group_type": r[4],
                "filter_expr": r[5],
                "created_at": str(r[6]) if r[6] else None,
                "updated_at": str(r[7]) if r[7] else None,
                "member_count": r[8],
            }
            for r in rows
        ]

    def get_group_tickers(self, group_id: str, market: str | None = None) -> list[str]:
        """Return the ticker list for a group."""
        group = self._fetch_group_row(group_id, market)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        conn = get_connection()
        rows = conn.execute(
            """SELECT ticker FROM stock_group_members
               WHERE group_id = ? AND market = ?
               ORDER BY ticker""",
            [group_id, group["market"]],
        ).fetchall()
        return [r[0] for r in rows]

    def refresh_filter(self, group_id: str, market: str | None = None) -> dict:
        """Re-evaluate the filter expression for a filter/builtin group."""
        group = self._fetch_group_row(group_id, market)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        if group["group_type"] not in ("filter", "builtin"):
            raise ValueError("Only filter/builtin groups can be refreshed")
        if not group["filter_expr"]:
            raise ValueError("Group has no filter expression")

        self._evaluate_filter(group_id, group["filter_expr"], market=group["market"])

        conn = get_connection()
        conn.execute(
            "UPDATE stock_groups SET updated_at = ? WHERE id = ?",
            [datetime.utcnow(), group_id],
        )

        log.info("group.filter_refreshed", id=group_id)
        return self.get_group(group_id, market=group["market"])

    def refresh_index_groups(self, market: str | None = None) -> list[dict]:
        """Re-fetch S&P 500, NASDAQ 100, and Russell 3000 constituents from Wikipedia.

        For each built-in index group, re-downloads the ticker list and updates
        the group membership.  If the fetch fails for a particular index, its
        membership is left unchanged.

        Returns:
            List of updated group dicts.
        """
        resolved_market = normalize_market(market)
        if resolved_market != "US":
            self.ensure_builtins(resolved_market)
            return [self.get_group(group_id, market=resolved_market) for group_id in _CN_BUILTIN_GROUPS]

        conn = get_connection()
        results: list[dict] = []

        for group_id, (name, desc, fetch_key) in _BUILTIN_INDEX_GROUPS.items():
            # Ensure the group row exists
            existing = conn.execute(
                "SELECT id FROM stock_groups WHERE id = ?", [group_id]
            ).fetchone()
            if existing is None:
                # Create a placeholder row so we can populate it
                conn.execute(
                    """INSERT INTO stock_groups (id, market, name, description, group_type, filter_expr)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    [group_id, resolved_market, name, desc, "builtin", None],
                )

            if fetch_key == "russell3000":
                # Russell 3000: use filter as documented
                filter_expr = "exchange IN ('NYSE', 'NASDAQ') AND status = 'active'"
                conn.execute(
                    "UPDATE stock_groups SET filter_expr = ?, description = ?, updated_at = ? WHERE id = ?",
                    [filter_expr, desc, datetime.utcnow(), group_id],
                )
                self._evaluate_filter(group_id, filter_expr, market=resolved_market)
                log.info("group.index_refresh.done", index=group_id, method="filter")
            else:
                tickers = _fetch_index_tickers(fetch_key)
                if tickers:
                    self._set_members(group_id, tickers, market=resolved_market, validate_members=False)
                    conn.execute(
                        "UPDATE stock_groups SET description = ?, updated_at = ? WHERE id = ?",
                        [desc, datetime.utcnow(), group_id],
                    )
                    log.info("group.index_refresh.done", index=group_id, count=len(tickers))
                else:
                    log.warning("group.index_refresh.fetch_failed", index=group_id)

            results.append(self.get_group(group_id, market=resolved_market))

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_group_row(self, group_id: str, market: str | None = None) -> dict | None:
        conn = get_connection()
        if market is None:
            row = conn.execute(
                """SELECT id, market, name, description, group_type, filter_expr,
                          created_at, updated_at
                   FROM stock_groups WHERE id = ?""",
                [group_id],
            ).fetchone()
        else:
            resolved_market = normalize_market(market)
            row = conn.execute(
                """SELECT id, market, name, description, group_type, filter_expr,
                          created_at, updated_at
                   FROM stock_groups WHERE id = ? AND market = ?""",
                [group_id, resolved_market],
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "market": row[1],
            "name": row[2],
            "description": row[3],
            "group_type": row[4],
            "filter_expr": row[5],
            "created_at": str(row[6]) if row[6] else None,
            "updated_at": str(row[7]) if row[7] else None,
        }

    def _set_members(
        self,
        group_id: str,
        tickers: list[str],
        market: str | None = None,
        validate_members: bool = True,
    ) -> None:
        group = self._fetch_group_row(group_id, market)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        resolved_market = group["market"]
        normalized = [normalize_ticker(t, resolved_market) for t in tickers if str(t).strip()]
        if validate_members:
            self._validate_member_markets(normalized, resolved_market)

        conn = get_connection()
        conn.execute(
            "DELETE FROM stock_group_members WHERE group_id = ? AND market = ?",
            [group_id, resolved_market],
        )
        for t in normalized:
            conn.execute(
                "INSERT INTO stock_group_members (group_id, market, ticker) VALUES (?, ?, ?)",
                [group_id, resolved_market, t],
            )

    def _evaluate_filter(self, group_id: str, filter_expr: str, market: str | None = None) -> None:
        """Evaluate a filter expression against the stocks table and store members."""
        group = self._fetch_group_row(group_id, market)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        resolved_market = group["market"]
        conn = get_connection()
        try:
            rows = conn.execute(
                f"SELECT ticker FROM stocks WHERE market = ? AND ({filter_expr})",  # noqa: S608
                [resolved_market],
            ).fetchall()
        except Exception as e:
            log.error("group.filter_eval_error", id=group_id, error=str(e))
            raise ValueError(f"Filter expression error: {e}") from e

        tickers = [r[0] for r in rows]
        self._set_members(group_id, tickers, market=resolved_market, validate_members=False)
        log.info("group.filter_evaluated", id=group_id, market=resolved_market, count=len(tickers))

    def _ensure_filter_builtin(
        self,
        conn,
        group_id: str,
        market: str,
        name: str,
        description: str,
        filter_expr: str,
    ) -> None:
        existing = conn.execute("SELECT id FROM stock_groups WHERE id = ?", [group_id]).fetchone()
        if existing is not None:
            return
        conn.execute(
            """INSERT INTO stock_groups (id, market, name, description, group_type, filter_expr)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [group_id, market, name, description, "builtin", filter_expr],
        )
        self._evaluate_filter(group_id, filter_expr, market=market)
        log.info("group.builtin_created", market=market, name=name)

    def _validate_member_markets(self, tickers: list[str], market: str) -> None:
        conn = get_connection()
        for ticker in tickers:
            hinted_market = infer_ticker_market(ticker)
            if hinted_market is not None and hinted_market != market:
                raise ValueError(f"Ticker {ticker} not found in market {market}")

            rows = conn.execute(
                "SELECT DISTINCT market FROM stocks WHERE ticker = ?",
                [ticker],
            ).fetchall()
            known_markets = {r[0] for r in rows}
            if known_markets and market not in known_markets:
                raise ValueError(f"Ticker {ticker} not found in market {market}")
