#!/usr/bin/env python3
"""HTTP smoke test for the QAgent 3.0 Migration API."""

from __future__ import annotations

import requests


BASE = "http://127.0.0.1:8000"


def api(method: str, path: str, **kwargs):
    response = getattr(requests, method)(f"{BASE}{path}", timeout=30, **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> int:
    health = api("get", "/api/health")
    assert health["status"] == "ok"

    report = api("post", "/api/migration/report", json={})
    assert report["mode"] == "dry-run"
    assert "source_tables" in report
    assert "legacy_signatures" in report

    applied = api("post", "/api/migration/apply", json={})
    assert applied["run"]["run_type"] == "migration_apply"
    assert applied["artifact"]["artifact_type"] == "migration_report"

    print(
        {
            "tables": len(report["source_tables"]),
            "migration_run": applied["run"]["id"],
            "artifact_id": applied["artifact"]["id"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
