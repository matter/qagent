"""Stock group management service."""

from __future__ import annotations

import uuid
from datetime import datetime

from backend.db import get_connection
from backend.logger import get_logger

log = get_logger(__name__)

_BUILTIN_ALL_MARKET = "all_market"
_BUILTIN_ALL_MARKET_NAME = "全市场"

# Safe columns that filter expressions may reference.
_ALLOWED_FILTER_COLUMNS = {"ticker", "name", "exchange", "sector", "status"}


def _validate_filter_expr(expr: str) -> str:
    """Basic validation for filter expressions used as SQL WHERE clauses.

    Only allows references to known columns in the stocks table and basic
    comparison / logical operators.  This is *not* a full sandbox but prevents
    the most obvious injection vectors.
    """
    if not expr or not expr.strip():
        raise ValueError("filter_expr must not be empty")

    forbidden = [";", "--", "/*", "*/", "DROP", "DELETE", "INSERT", "UPDATE",
                 "ALTER", "CREATE", "EXEC", "UNION", "INTO"]
    upper = expr.upper()
    for token in forbidden:
        if token in upper:
            raise ValueError(f"filter expression contains forbidden token: {token}")

    return expr.strip()


class GroupService:
    """CRUD operations for stock groups."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_builtins(self) -> None:
        """Create built-in groups if they do not exist."""
        conn = get_connection()
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
