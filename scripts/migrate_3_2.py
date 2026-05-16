#!/usr/bin/env python3
"""QAgent V3.2 old-architecture separation dry-run utility."""

from __future__ import annotations

import argparse
from pathlib import Path

from backend.services.migration_3_2_service import Migration32Service


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "qagent.duckdb"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "docs" / "v3.2" / "reports"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--dry-run", action="store_true", help="Build a read-only inventory manifest")
    parser.add_argument(
        "--apply-basic-assets",
        action="store_true",
        help="Idempotently import/re-enter stocks, factor specs, and universes into 3.0 tables",
    )
    parser.add_argument(
        "--apply-market-data-snapshots",
        action="store_true",
        help="Idempotently register daily_bars coverage as 3.0 market_data_snapshots",
    )
    parser.add_argument(
        "--apply-dependency-assets",
        action="store_true",
        help="Idempotently rebuild feature/model/strategy/paper dependency assets into 3.0 descriptors",
    )
    parser.add_argument("--apply", action="store_true", help="Reserved; V3.2 apply is intentionally not implemented yet")
    args = parser.parse_args()

    if args.apply:
        raise SystemExit("V3.2 apply is not implemented. Use --dry-run to generate a manifest.")
    modes = [args.dry_run, args.apply_basic_assets, args.apply_market_data_snapshots, args.apply_dependency_assets]
    if sum(1 for mode in modes if mode) > 1:
        raise SystemExit(
            "Choose only one of --dry-run, --apply-basic-assets, "
            "--apply-market-data-snapshots, or --apply-dependency-assets"
        )
    if not any(modes):
        args.dry_run = True

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    service = Migration32Service()
    if args.apply_basic_assets:
        result = service.apply_basic_assets(db_path=db_path)
        print("V3.2 basic asset migration:")
        print(f"  manifest_id: {result['manifest_id']}")
        print(f"  assets inserted: {result['assets']['inserted']}")
        print(f"  factor_specs inserted: {result['factor_specs']['inserted']}")
        print(f"  universes inserted: {result['universes']['inserted']}")
    elif args.apply_market_data_snapshots:
        result = service.apply_market_data_snapshots(db_path=db_path)
        print("V3.2 market data snapshot migration:")
        print(f"  manifest_id: {result['manifest_id']}")
        print(f"  snapshots inserted: {result['snapshots']['inserted']}")
        for market, info in result["markets"].items():
            print(
                f"  {market}: rows={info['row_count']} "
                f"mapped={info['mapped_row_count']} "
                f"unmapped_tickers={info['unmapped_ticker_count']}"
            )
    elif args.apply_dependency_assets:
        result = service.apply_dependency_assets(db_path=db_path)
        print("V3.2 dependency asset migration:")
        print(f"  manifest_id: {result['manifest_id']}")
        print(f"  label_specs inserted: {result['label_specs']['inserted']}")
        print(f"  feature_pipelines inserted: {result['feature_pipelines']['inserted']}")
        print(f"  model_specs inserted: {result['model_specs']['inserted']}")
        print(f"  model_packages inserted: {result['model_packages']['inserted']}")
        print(f"  strategy_graphs inserted: {result['strategy_graphs']['inserted']}")
        print(f"  paper_sessions inserted: {result['paper_sessions']['inserted']}")
    else:
        manifest = service.build_dry_run_manifest(db_path=db_path)
        json_path, md_path = service.write_manifest_files(manifest, out_dir=Path(args.out_dir))
        print(f"V3.2 migration manifest JSON: {json_path}")
        print(f"V3.2 migration manifest Markdown: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
