"""DuckDB schema migration helpers.

The V2 market migration is intentionally conservative for existing databases:
it backfills a non-null `market` column and validates intended market-aware keys
without rewriting large historical tables during application startup.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

MARKET_SCHEMA_MIGRATION_ID = "20260429_market_scope_v1"

_SCHEMA_MIGRATIONS_DDL = """\
CREATE TABLE IF NOT EXISTS schema_migrations (
    id                  VARCHAR PRIMARY KEY,
    applied_at          TIMESTAMP NOT NULL DEFAULT current_timestamp,
    code_version        VARCHAR,
    preflight_summary   JSON,
    validation_summary  JSON
);
"""

_MARKET_TABLE_KEYS: dict[str, list[str]] = {
    "stocks": ["ticker"],
    "daily_bars": ["ticker", "date"],
    "index_bars": ["symbol", "date"],
    "data_update_log": ["id"],
    "stock_groups": ["id"],
    "stock_group_members": ["group_id", "ticker"],
    "label_definitions": ["id"],
    "factors": ["id"],
    "factor_values_cache": ["factor_id", "ticker", "date"],
    "factor_eval_results": ["id"],
    "feature_sets": ["id"],
    "models": ["id"],
    "strategies": ["id"],
    "backtest_results": ["id"],
    "signal_runs": ["id"],
    "signal_details": ["run_id", "ticker"],
    "paper_trading_sessions": ["id"],
    "paper_trading_daily": ["session_id", "date"],
    "paper_trading_signal_cache": ["session_id", "signal_date"],
}


def migrate_market_schema(
    conn,
    migration_id: str = MARKET_SCHEMA_MIGRATION_ID,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Backfill market columns and record an auditable migration report."""
    preflight = validate_market_schema(conn)
    if dry_run:
        return {
            "id": migration_id,
            "status": "dry_run",
            "preflight_summary": preflight,
            "validation_summary": preflight,
        }

    conn.execute(_SCHEMA_MIGRATIONS_DDL)
    existing = conn.execute(
        "SELECT id FROM schema_migrations WHERE id = ?",
        [migration_id],
    ).fetchone()
    if existing:
        return {
            "id": migration_id,
            "status": "already_applied",
            "preflight_summary": preflight,
            "validation_summary": validate_market_schema(conn),
        }

    for table in _MARKET_TABLE_KEYS:
        if not _table_exists(conn, table):
            continue
        if not _column_exists(conn, table, "market"):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN market VARCHAR DEFAULT 'US'")
        conn.execute(f"UPDATE {table} SET market = 'US' WHERE market IS NULL")

    validation = validate_market_schema(conn)
    _raise_if_invalid(validation)
    conn.execute(
        """INSERT INTO schema_migrations
           (id, applied_at, code_version, preflight_summary, validation_summary)
           VALUES (?, ?, ?, ?, ?)""",
        [
            migration_id,
            datetime.now(UTC).replace(tzinfo=None),
            "v2-market-scope",
            json.dumps(preflight, default=str),
            json.dumps(validation, default=str),
        ],
    )
    return {
        "id": migration_id,
        "status": "applied",
        "preflight_summary": preflight,
        "validation_summary": validation,
    }


def validate_market_schema(conn) -> dict[str, Any]:
    """Return row, market-null, and target-key uniqueness checks."""
    tables: dict[str, dict[str, Any]] = {}
    for table, key_columns in _MARKET_TABLE_KEYS.items():
        if not _table_exists(conn, table):
            continue
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        has_market = _column_exists(conn, table, "market")
        null_market_count = (
            conn.execute(f"SELECT COUNT(*) FROM {table} WHERE market IS NULL").fetchone()[0]
            if has_market
            else row_count
        )
        duplicate_key_groups = _duplicate_group_count(
            conn,
            table,
            (["market"] if has_market else []) + key_columns,
        )
        tables[table] = {
            "row_count": row_count,
            "has_market": has_market,
            "null_market_count": null_market_count,
            "target_key_columns": (["market"] if has_market else []) + key_columns,
            "duplicate_target_key_groups": duplicate_key_groups,
        }

    return {
        "tables": tables,
        "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def _raise_if_invalid(validation: dict[str, Any]) -> None:
    failures: list[str] = []
    for table, result in validation["tables"].items():
        if not result["has_market"]:
            failures.append(f"{table}: missing market column")
        if result["null_market_count"]:
            failures.append(f"{table}: {result['null_market_count']} null market values")
        if result["duplicate_target_key_groups"]:
            failures.append(
                f"{table}: {result['duplicate_target_key_groups']} duplicate target key groups"
            )
    if failures:
        raise RuntimeError("Market schema migration validation failed: " + "; ".join(failures))


def _table_exists(conn, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
            [table],
        ).fetchone()
    )


def _column_exists(conn, table: str, column: str) -> bool:
    return bool(
        conn.execute(
            """SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'main' AND table_name = ? AND column_name = ?""",
            [table, column],
        ).fetchone()
    )


def _duplicate_group_count(conn, table: str, columns: list[str]) -> int:
    if not columns:
        return 0
    existing_pk = set(_primary_key_columns(conn, table))
    columns_without_market = [col for col in columns if col != "market"]
    if columns_without_market and set(columns_without_market).issubset(existing_pk):
        return 0
    expr = ", ".join(columns)
    return conn.execute(
        f"SELECT COUNT(*) FROM (SELECT {expr}, COUNT(*) c FROM {table} GROUP BY {expr} HAVING COUNT(*) > 1)"
    ).fetchone()[0]


def _primary_key_columns(conn, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall() if row[5]]
