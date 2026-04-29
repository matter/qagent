#!/usr/bin/env python3
"""Validate the V2 market schema migration on a working DB or copied DB."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import duckdb

from backend.config import settings
from backend.services.schema_migrations import migrate_market_schema, validate_market_schema


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=settings.db_path, help="Source DuckDB path")
    parser.add_argument(
        "--database-copy",
        type=Path,
        help="Copy source DB to this path and apply the migration to the copy",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without changing the selected database",
    )
    args = parser.parse_args()

    db_path = args.source
    if args.database_copy:
        db_path = args.database_copy
        if not db_path.exists():
            db_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(args.source, db_path)
            print(f"copied_source={args.source}", file=sys.stderr)
        print(f"database_copy={db_path}", file=sys.stderr)

    conn = duckdb.connect(str(db_path))
    try:
        report = migrate_market_schema(
            conn,
            dry_run=args.dry_run and not args.database_copy,
        )
        validation = validate_market_schema(conn)
    finally:
        conn.close()

    print(json.dumps({
        "database": str(db_path),
        "migration": report,
        "validation": validation,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
