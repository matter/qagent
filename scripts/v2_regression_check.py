#!/usr/bin/env python3
"""Run the V2 market-scope regression gate.

The default command checks old no-market API compatibility, local service
defaults, US end-to-end flow, and optional V2 checks with explicit skip
reasons. It expects the backend to be running on --base-url.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Literal, NamedTuple

import duckdb
import requests

from backend.config import settings

Status = Literal["passed", "failed", "skipped"]


class CheckResult(NamedTuple):
    name: str
    status: Status
    detail: str
    data: dict[str, Any] | None = None


def summarize_results(results: list[CheckResult]) -> dict[str, Any]:
    passed = sum(1 for result in results if result.status == "passed")
    failed = sum(1 for result in results if result.status == "failed")
    skipped = sum(1 for result in results if result.status == "skipped")
    return {
        "overall_status": "failed" if failed else "passed",
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "checks": [
            {
                "name": result.name,
                "status": result.status,
                "detail": result.detail,
                "data": result.data or {},
            }
            for result in results
        ],
    }


def exit_code_for_results(results: list[CheckResult]) -> int:
    return 1 if any(result.status == "failed" for result in results) else 0


def check_market_context_defaults() -> CheckResult:
    try:
        from backend.services.market_context import (
            get_default_benchmark,
            get_default_group,
            normalize_market,
        )

        checks = {
            "missing_market": normalize_market(None),
            "blank_market": normalize_market(""),
            "cn_alias": normalize_market("a_share"),
            "us_benchmark": get_default_benchmark("US"),
            "cn_benchmark": get_default_benchmark("CN"),
            "us_group": get_default_group("US"),
            "cn_group": get_default_group("CN"),
        }
        expected = {
            "missing_market": "US",
            "blank_market": "US",
            "cn_alias": "CN",
            "us_benchmark": "SPY",
            "cn_benchmark": "sh.000300",
            "us_group": "us_all_market",
            "cn_group": "cn_a_core_indices_union",
        }
        mismatches = {
            key: {"actual": checks[key], "expected": expected[key]}
            for key in expected
            if checks[key] != expected[key]
        }
        if mismatches:
            return CheckResult(
                "market_context_defaults",
                "failed",
                "market defaults changed",
                {"mismatches": mismatches},
            )
        return CheckResult("market_context_defaults", "passed", "missing market defaults to US", checks)
    except Exception as exc:
        return CheckResult("market_context_defaults", "failed", str(exc))


def check_api_us_defaults(base_url: str, session: requests.Session | None = None) -> CheckResult:
    session = session or requests.Session()
    base = base_url.rstrip("/")
    try:
        health = _get_json(session, f"{base}/api/health")
        status = _get_json(session, f"{base}/api/data/status")
        search = _get_json(session, f"{base}/api/stocks/search?q=AAPL&limit=1")

        if health.get("status") != "ok":
            return CheckResult("api_us_defaults", "failed", "health endpoint did not return ok", health)
        if status.get("market") != "US":
            return CheckResult("api_us_defaults", "failed", "GET /api/data/status did not default to US", status)
        if not isinstance(search, list) or not search:
            return CheckResult("api_us_defaults", "failed", "GET /api/stocks/search returned no AAPL rows")
        markets = {row.get("market") for row in search if isinstance(row, dict)}
        if markets != {"US"}:
            return CheckResult(
                "api_us_defaults",
                "failed",
                "old no-market stock search leaked non-US rows",
                {"markets": sorted(str(m) for m in markets)},
            )
        return CheckResult(
            "api_us_defaults",
            "passed",
            "old REST calls without market resolved to US",
            {"data_status_market": status.get("market"), "search_markets": sorted(markets)},
        )
    except Exception as exc:
        return CheckResult("api_us_defaults", "failed", str(exc))


def check_cn_provider_failure_does_not_block_us(
    base_url: str,
    session: requests.Session | None = None,
) -> CheckResult:
    try:
        from backend.providers.baostock_provider import BaoStockProvider

        class FailingBaoStockClient:
            def login(self):
                raise RuntimeError("simulated BaoStock login failure")

        try:
            BaoStockProvider(client=FailingBaoStockClient()).get_stock_list()
            return CheckResult(
                "cn_provider_failure_isolation",
                "failed",
                "simulated BaoStock failure did not fail as expected",
            )
        except RuntimeError:
            pass

        base = base_url.rstrip("/")
        us_status = _get_json(session or requests.Session(), f"{base}/api/data/status")
        if us_status.get("market") != "US":
            return CheckResult(
                "cn_provider_failure_isolation",
                "failed",
                "US API status failed after simulated CN provider failure",
                us_status,
            )
        return CheckResult(
            "cn_provider_failure_isolation",
            "passed",
            "simulated CN provider failure did not block US API status",
            {"us_stock_count": us_status.get("stock_count", 0)},
        )
    except Exception as exc:
        return CheckResult("cn_provider_failure_isolation", "failed", str(exc))


def check_migration_copy(source_db: Path, database_copy: Path | None) -> CheckResult:
    if database_copy is None:
        return CheckResult(
            "migration_copy_validation",
            "skipped",
            "pass --migration-copy PATH to validate migration on a copied DuckDB file",
        )

    try:
        from backend.services.schema_migrations import migrate_market_schema, validate_market_schema

        if not source_db.exists():
            return CheckResult("migration_copy_validation", "failed", f"source DB not found: {source_db}")
        if not database_copy.exists():
            database_copy.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_db, database_copy)

        conn = duckdb.connect(str(database_copy))
        try:
            report = migrate_market_schema(conn)
            validation = validate_market_schema(conn)
        finally:
            conn.close()

        invalid_tables = {
            table: result
            for table, result in validation.get("tables", {}).items()
            if not result.get("has_market")
            or result.get("null_market_count")
            or result.get("duplicate_target_key_groups")
        }
        if invalid_tables:
            return CheckResult(
                "migration_copy_validation",
                "failed",
                "copied DB migration validation found invalid tables",
                {"invalid_tables": invalid_tables},
            )

        return CheckResult(
            "migration_copy_validation",
            "passed",
            "copied DB migration validation passed",
            {
                "database_copy": str(database_copy),
                "migration_status": report.get("status"),
                "tables_checked": len(validation.get("tables", {})),
            },
        )
    except Exception as exc:
        return CheckResult("migration_copy_validation", "failed", str(exc))


def check_us_e2e(skip: bool, timeout: int) -> CheckResult:
    if skip:
        return CheckResult("us_e2e_flow", "skipped", "skipped by --skip-us-e2e")

    cmd = [sys.executable, "scripts/e2e_demo.py"]
    try:
        completed = subprocess.run(
            cmd,
            cwd=settings.project_root,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return CheckResult("us_e2e_flow", "failed", f"timed out after {timeout}s", {"cmd": cmd, "output": exc.stdout})

    output = (completed.stdout or "") + (completed.stderr or "")
    tail = "\n".join(output.splitlines()[-20:])
    if completed.returncode != 0:
        return CheckResult(
            "us_e2e_flow",
            "failed",
            f"scripts/e2e_demo.py exited {completed.returncode}",
            {"tail": tail},
        )
    return CheckResult("us_e2e_flow", "passed", "old US e2e flow completed", {"tail": tail})


def check_optional_cn_provider_smoke(run_smoke: bool) -> CheckResult:
    if not run_smoke:
        return CheckResult(
            "cn_baostock_provider_smoke",
            "skipped",
            "pass --cn-provider-smoke to run the optional BaoStock network check",
        )

    try:
        import baostock  # noqa: F401
    except Exception as exc:
        return CheckResult("cn_baostock_provider_smoke", "skipped", f"BaoStock package unavailable: {exc}")

    try:
        from datetime import date

        from backend.providers.registry import get_provider

        provider = get_provider("CN")
        bars = provider.get_daily_bars(["sh.600000"], date(2024, 1, 2), date(2024, 1, 3))
        if bars.empty:
            return CheckResult("cn_baostock_provider_smoke", "skipped", "BaoStock returned no bars for sh.600000")
        if set(bars["market"].unique()) != {"CN"}:
            return CheckResult(
                "cn_baostock_provider_smoke",
                "failed",
                "BaoStock bars did not carry market=CN",
                {"markets": sorted(str(m) for m in bars["market"].unique())},
            )
        return CheckResult(
            "cn_baostock_provider_smoke",
            "passed",
            "BaoStock returned CN daily bars",
            {"rows": len(bars)},
        )
    except Exception as exc:
        return CheckResult("cn_baostock_provider_smoke", "skipped", f"BaoStock smoke unavailable: {exc}")


def render_results(results: list[CheckResult], *, as_json: bool = False) -> str:
    summary = summarize_results(results)
    if as_json:
        return json.dumps(summary, ensure_ascii=False, indent=2)

    lines = ["QAgent V2 regression check"]
    for result in results:
        lines.append(f"[{result.status.upper()}] {result.name}: {result.detail}")
    lines.append(
        "Summary: "
        f"{summary['overall_status']} "
        f"(passed={summary['passed']}, failed={summary['failed']}, skipped={summary['skipped']})"
    )
    return "\n".join(lines)


def run_checks(args: argparse.Namespace) -> list[CheckResult]:
    return [
        check_market_context_defaults(),
        check_api_us_defaults(args.base_url),
        check_cn_provider_failure_does_not_block_us(args.base_url),
        check_migration_copy(args.source_db, args.migration_copy),
        check_optional_cn_provider_smoke(args.cn_provider_smoke),
        check_us_e2e(args.skip_us_e2e, args.e2e_timeout),
    ]


def _get_json(session: requests.Session, url: str) -> Any:
    response = session.get(url, timeout=10)
    if not response.ok:
        raise RuntimeError(f"GET {url} failed: HTTP {response.status_code} {response.text[:300]}")
    return response.json()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Running backend base URL")
    parser.add_argument("--source-db", type=Path, default=settings.db_path, help="Source DuckDB path")
    parser.add_argument(
        "--migration-copy",
        type=Path,
        help="Optional copied DuckDB path used for migration validation",
    )
    parser.add_argument(
        "--cn-provider-smoke",
        action="store_true",
        help="Run optional BaoStock network smoke for sh.600000",
    )
    parser.add_argument("--skip-us-e2e", action="store_true", help="Skip scripts/e2e_demo.py")
    parser.add_argument("--e2e-timeout", type=int, default=900, help="US e2e timeout in seconds")
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    results = run_checks(args)
    print(render_results(results, as_json=args.json))
    return exit_code_for_results(results)


if __name__ == "__main__":
    raise SystemExit(main())
