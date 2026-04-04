"""Stock group management service."""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger

log = get_logger(__name__)

_BUILTIN_ALL_MARKET = "all_market"
_BUILTIN_ALL_MARKET_NAME = "全市场"

# Built-in index groups: id -> (name, description, filter_expr or None, tickers_fetcher)
_BUILTIN_INDEX_GROUPS = {
    "sp500": ("标普500", "S&P 500 成分股", "sp500"),
    "nasdaq100": ("纳斯达克100", "NASDAQ 100 成分股", "nasdaq100"),
    "russell3000": ("罗素3000", "Russell 3000 成分股", "russell3000"),
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


def _fetch_index_tickers(index_id: str) -> list[str]:
    """Fetch latest index constituents from Wikipedia. Returns ticker list."""
    try:
        if index_id == "sp500":
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            tables = pd.read_html(url)
            df = tables[0]
            tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
        elif index_id == "nasdaq100":
            url = "https://en.wikipedia.org/wiki/Nasdaq-100"
            tables = pd.read_html(url)
            # Find the table with a "Ticker" or "Symbol" column
            df = None
            for t in tables:
                cols = [c.lower() for c in t.columns]
                if "ticker" in cols:
                    df = t
                    col = t.columns[[c.lower() for c in t.columns].index("ticker")]
                    break
                if "symbol" in cols:
                    df = t
                    col = t.columns[[c.lower() for c in t.columns].index("symbol")]
                    break
            if df is None:
                log.warning("group.index_fetch.no_table", index=index_id)
                return []
            tickers = df[col].str.replace(".", "-", regex=False).tolist()
        elif index_id == "russell3000":
            # Russell 3000 is not on Wikipedia in a clean table.
            # Use the iShares Russell 3000 ETF holdings page as proxy.
            # Fallback: return empty and log, user can manually populate.
            try:
                url = "https://en.wikipedia.org/wiki/Russell_3000_Index"
                tables = pd.read_html(url)
                # Try to find a constituents table
                for t in tables:
                    cols = [c.lower() for c in t.columns]
                    if "ticker" in cols or "symbol" in cols:
                        col_name = "ticker" if "ticker" in cols else "symbol"
                        col = t.columns[[c.lower() for c in t.columns].index(col_name)]
                        return t[col].str.replace(".", "-", regex=False).tolist()
            except Exception:
                pass
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

    def ensure_builtins(self) -> None:
        """Create built-in groups if they do not exist."""
        conn = get_connection()

        # --- "全市场" group ---
        row = conn.execute(
            "SELECT id FROM stock_groups WHERE id = ?",
            [_BUILTIN_ALL_MARKET],
        ).fetchone()
        if row is None:
            conn.execute(
                """INSERT INTO stock_groups (id, name, description, group_type, filter_expr)
                   VALUES (?, ?, ?, ?, ?)""",
                [
                    _BUILTIN_ALL_MARKET,
                    _BUILTIN_ALL_MARKET_NAME,
                    "包含所有已录入股票",
                    "builtin",
                    "1=1",
                ],
            )
            log.info("group.builtin_created", name=_BUILTIN_ALL_MARKET_NAME)

        # --- Index constituent groups (S&P 500, NASDAQ 100, Russell 3000) ---
        for group_id, (name, desc, fetch_key) in _BUILTIN_INDEX_GROUPS.items():
            existing = conn.execute(
                "SELECT id FROM stock_groups WHERE id = ?", [group_id]
            ).fetchone()
            if existing is not None:
                continue

            tickers = _fetch_index_tickers(fetch_key)

            if fetch_key == "russell3000" and not tickers:
                # Fallback: use filter for all active NYSE+NASDAQ stocks
                conn.execute(
                    """INSERT INTO stock_groups (id, name, description, group_type, filter_expr)
                       VALUES (?, ?, ?, ?, ?)""",
                    [group_id, name, desc + "（近似：全部活跃 NYSE+NASDAQ 股票）",
                     "builtin", "exchange IN ('NYSE', 'NASDAQ') AND status = 'active'"],
                )
                self._evaluate_filter(group_id, "exchange IN ('NYSE', 'NASDAQ') AND status = 'active'")
            elif tickers:
                conn.execute(
                    """INSERT INTO stock_groups (id, name, description, group_type, filter_expr)
                       VALUES (?, ?, ?, ?, ?)""",
                    [group_id, name, desc, "builtin", None],
                )
                self._set_members(group_id, tickers)
            else:
                # Fetch failed, create empty group that can be refreshed later
                conn.execute(
                    """INSERT INTO stock_groups (id, name, description, group_type, filter_expr)
                       VALUES (?, ?, ?, ?, ?)""",
                    [group_id, name, desc + "（成分股获取失败，请手动刷新）", "builtin", None],
                )

            log.info("group.builtin_created", name=name, tickers=len(tickers) if tickers else 0)

    def create_group(
        self,
        name: str,
        description: str | None = None,
        group_type: str = "manual",
        tickers: list[str] | None = None,
        filter_expr: str | None = None,
    ) -> dict:
        """Create a new stock group."""
        conn = get_connection()
        group_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()

        if group_type == "filter":
            if not filter_expr:
                raise ValueError("filter_expr is required for filter-type groups")
            filter_expr = _validate_filter_expr(filter_expr)

        conn.execute(
            """INSERT INTO stock_groups (id, name, description, group_type, filter_expr, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [group_id, name, description, group_type, filter_expr, now, now],
        )

        # For manual groups, insert explicit members.
        if group_type == "manual" and tickers:
            self._set_members(group_id, tickers)

        # For filter groups, evaluate immediately.
        if group_type == "filter" and filter_expr:
            self._evaluate_filter(group_id, filter_expr)

        log.info("group.created", id=group_id, name=name, type=group_type)
        return self.get_group(group_id)

    def update_group(
        self,
        group_id: str,
        name: str | None = None,
        description: str | None = None,
        tickers: list[str] | None = None,
        filter_expr: str | None = None,
    ) -> dict:
        """Update an existing group."""
        conn = get_connection()
        group = self._fetch_group_row(group_id)
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
            self._set_members(group_id, tickers)
        if group["group_type"] == "filter" and filter_expr is not None:
            self._evaluate_filter(group_id, filter_expr)

        log.info("group.updated", id=group_id)
        return self.get_group(group_id)

    def delete_group(self, group_id: str) -> None:
        """Delete a group and its membership records."""
        conn = get_connection()
        group = self._fetch_group_row(group_id)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        if group["group_type"] == "builtin":
            raise ValueError("Cannot delete built-in groups")

        conn.execute("DELETE FROM stock_group_members WHERE group_id = ?", [group_id])
        conn.execute("DELETE FROM stock_groups WHERE id = ?", [group_id])
        log.info("group.deleted", id=group_id)

    def get_group(self, group_id: str) -> dict:
        """Return group info including member tickers."""
        group = self._fetch_group_row(group_id)
        if group is None:
            raise ValueError(f"Group {group_id} not found")

        tickers = self.get_group_tickers(group_id)
        return {**group, "tickers": tickers, "member_count": len(tickers)}

    def list_groups(self) -> list[dict]:
        """List all groups with member counts."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT g.id, g.name, g.description, g.group_type, g.filter_expr,
                      g.created_at, g.updated_at, COUNT(m.ticker) AS member_count
               FROM stock_groups g
               LEFT JOIN stock_group_members m ON g.id = m.group_id
               GROUP BY g.id, g.name, g.description, g.group_type, g.filter_expr,
                        g.created_at, g.updated_at
               ORDER BY g.created_at"""
        ).fetchall()

        return [
            {
                "id": r[0],
                "name": r[1],
                "description": r[2],
                "group_type": r[3],
                "filter_expr": r[4],
                "created_at": str(r[5]) if r[5] else None,
                "updated_at": str(r[6]) if r[6] else None,
                "member_count": r[7],
            }
            for r in rows
        ]

    def get_group_tickers(self, group_id: str) -> list[str]:
        """Return the ticker list for a group."""
        conn = get_connection()
        rows = conn.execute(
            "SELECT ticker FROM stock_group_members WHERE group_id = ? ORDER BY ticker",
            [group_id],
        ).fetchall()
        return [r[0] for r in rows]

    def refresh_filter(self, group_id: str) -> dict:
        """Re-evaluate the filter expression for a filter/builtin group."""
        group = self._fetch_group_row(group_id)
        if group is None:
            raise ValueError(f"Group {group_id} not found")
        if group["group_type"] not in ("filter", "builtin"):
            raise ValueError("Only filter/builtin groups can be refreshed")
        if not group["filter_expr"]:
            raise ValueError("Group has no filter expression")

        self._evaluate_filter(group_id, group["filter_expr"])

        conn = get_connection()
        conn.execute(
            "UPDATE stock_groups SET updated_at = ? WHERE id = ?",
            [datetime.utcnow(), group_id],
        )

        log.info("group.filter_refreshed", id=group_id)
        return self.get_group(group_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_group_row(self, group_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, description, group_type, filter_expr,
                      created_at, updated_at
               FROM stock_groups WHERE id = ?""",
            [group_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "group_type": row[3],
            "filter_expr": row[4],
            "created_at": str(row[5]) if row[5] else None,
            "updated_at": str(row[6]) if row[6] else None,
        }

    def _set_members(self, group_id: str, tickers: list[str]) -> None:
        conn = get_connection()
        conn.execute("DELETE FROM stock_group_members WHERE group_id = ?", [group_id])
        for t in tickers:
            conn.execute(
                "INSERT INTO stock_group_members (group_id, ticker) VALUES (?, ?)",
                [group_id, t.upper()],
            )

    def _evaluate_filter(self, group_id: str, filter_expr: str) -> None:
        """Evaluate a filter expression against the stocks table and store members."""
        conn = get_connection()
        try:
            rows = conn.execute(
                f"SELECT ticker FROM stocks WHERE {filter_expr}"  # noqa: S608
            ).fetchall()
        except Exception as e:
            log.error("group.filter_eval_error", id=group_id, error=str(e))
            raise ValueError(f"Filter expression error: {e}") from e

        tickers = [r[0] for r in rows]
        self._set_members(group_id, tickers)
        log.info("group.filter_evaluated", id=group_id, count=len(tickers))
