#!/usr/bin/env python3
"""Generate a QAgent 3.0 baseline report for the current 2.0 database.

This script is intentionally read-only. It records table counts, date ranges,
and local artifact directory sizes so M0 has a stable reference before schema
work begins.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "qagent.duckdb"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "docs" / "reports"

TABLES = [
    "stocks",
    "daily_bars",
    "index_bars",
    "stock_groups",
    "stock_group_members",
    "data_update_log",
    "label_definitions",
    "factors",
    "factor_values_cache",
    "factor_eval_results",
    "feature_sets",
    "models",
    "strategies",
    "backtest_results",
    "signal_runs",
    "signal_details",
    "paper_trading_sessions",
    "paper_trading_daily",
    "paper_trading_signal_cache",
    "task_runs",
]


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _dir_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "files": 0, "bytes": 0}
    files = [p for p in path.rglob("*") if p.is_file()]
    return {
        "exists": True,
        "files": len(files),
        "bytes": sum(p.stat().st_size for p in files),
    }


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
          FROM information_schema.tables
         WHERE table_schema = 'main'
           AND table_name = ?
        """,
        [table],
    ).fetchone()
    return bool(row and row[0])


def _safe_scalar(conn: duckdb.DuckDBPyConnection, query: str) -> Any:
    try:
        row = conn.execute(query).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def build_report(db_path: Path) -> dict[str, Any]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables: dict[str, dict[str, Any]] = {}
        for table in TABLES:
            if not _table_exists(conn, table):
                tables[table] = {"exists": False, "rows": 0}
                continue
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            tables[table] = {"exists": True, "rows": int(count)}

        market_counts: dict[str, Any] = {}
        for table in ("stocks", "daily_bars", "factors", "models", "strategies"):
            if _table_exists(conn, table):
                try:
                    rows = conn.execute(
                        f"SELECT market, COUNT(*) FROM {table} GROUP BY market ORDER BY market"
                    ).fetchall()
                    market_counts[table] = {str(m): int(c) for m, c in rows}
                except Exception:
                    market_counts[table] = {}

        date_ranges: dict[str, Any] = {}
        for table in ("daily_bars", "index_bars", "factor_values_cache"):
            if _table_exists(conn, table):
                date_ranges[table] = {
                    "min_date": str(_safe_scalar(conn, f"SELECT MIN(date) FROM {table}")),
                    "max_date": str(_safe_scalar(conn, f"SELECT MAX(date) FROM {table}")),
                }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path),
            "db_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "tables": tables,
            "market_counts": market_counts,
            "date_ranges": date_ranges,
            "directories": {
                "data/models": _dir_summary(PROJECT_ROOT / "data" / "models"),
                "data/factors": _dir_summary(PROJECT_ROOT / "data" / "factors"),
                "data/strategies": _dir_summary(PROJECT_ROOT / "data" / "strategies"),
            },
        }
    finally:
        conn.close()


def write_markdown(report: dict[str, Any], out_path: Path) -> None:
    lines = [
        "# QAgent 3.0 Baseline Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Database: `{report['db_path']}`",
        f"- Database size: `{report['db_bytes']}` bytes",
        "",
        "## Table Counts",
        "",
        "| Table | Exists | Rows |",
        "| --- | --- | ---: |",
    ]
    for table, info in report["tables"].items():
        lines.append(f"| `{table}` | {info['exists']} | {info['rows']} |")

    lines.extend(["", "## Market Counts", ""])
    if report["market_counts"]:
        lines.extend(["| Table | Counts |", "| --- | --- |"])
        for table, counts in report["market_counts"].items():
            lines.append(f"| `{table}` | `{json.dumps(counts, ensure_ascii=False)}` |")
    else:
        lines.append("- No market-scoped rows found.")

    lines.extend(["", "## Date Ranges", "", "| Table | Min Date | Max Date |", "| --- | --- | --- |"])
    for table, info in report["date_ranges"].items():
        lines.append(f"| `{table}` | {info['min_date']} | {info['max_date']} |")

    lines.extend(["", "## Local Artifact Directories", "", "| Path | Exists | Files | Bytes |", "| --- | --- | ---: | ---: |"])
    for path, info in report["directories"].items():
        lines.append(f"| `{path}` | {info['exists']} | {info['files']} | {info['bytes']} |")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_DIR))
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    report = build_report(db_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = _now_stamp()
    json_path = out_dir / f"3.0-baseline-{stamp}.json"
    md_path = out_dir / f"3.0-baseline-{stamp}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(report, md_path)

    print(f"Baseline JSON: {json_path}")
    print(f"Baseline Markdown: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
