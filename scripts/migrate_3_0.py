#!/usr/bin/env python3
"""QAgent 2.0 -> 3.0 migration utility.

The tool supports two modes:
- --dry-run: inspect legacy tables and write a migration report
- --apply: materialize a 3.0 migration run and artifact
"""

from __future__ import annotations

import argparse
from pathlib import Path

from backend.services.migration_service import MigrationService


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "qagent.duckdb"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "docs" / "reports"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if args.dry_run and args.apply:
        raise SystemExit("Choose either --dry-run or --apply, not both")
    if not args.dry_run and not args.apply:
        args.dry_run = True

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    svc = MigrationService()
    if args.apply:
        result = svc.apply_migration(db_path)
        report = result["report"]
        report["mode"] = "apply"
        report["application"] = {
            "run_id": result["run"]["id"],
            "artifact_id": result["artifact"]["id"],
            "asset_sync": result["asset_sync"],
        }
    else:
        report = svc.build_report(db_path)

    json_path, md_path = svc.write_report_files(report, out_dir=out_dir)
    print(f"Migration JSON: {json_path}")
    print(f"Migration Markdown: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
