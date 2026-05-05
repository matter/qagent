#!/usr/bin/env python3
"""HTTP smoke test for the QAgent 3.0 Research Kernel API."""

from __future__ import annotations

import requests


BASE = "http://127.0.0.1:8000"


def api(method: str, path: str, **kwargs):
    response = getattr(requests, method)(f"{BASE}{path}", timeout=10, **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> int:
    health = api("get", "/api/health")
    assert health["status"] == "ok"

    project = api("get", "/api/research/projects/bootstrap")
    assert project["id"] == "bootstrap_us"

    run = api(
        "post",
        "/api/research/runs",
        json={
            "run_type": "research_api_smoke",
            "lifecycle_stage": "scratch",
            "params": {"purpose": "api smoke"},
        },
    )
    assert run["project_id"] == project["id"]

    artifact = api(
        "post",
        "/api/research/artifacts/json",
        json={
            "run_id": run["id"],
            "artifact_type": "api_smoke_payload",
            "payload": {"ok": True},
            "lifecycle_stage": "scratch",
        },
    )
    assert artifact["run_id"] == run["id"]

    cleanup_preview = api(
        "post",
        "/api/research/artifacts/cleanup-preview",
        json={
            "project_id": project["id"],
            "run_id": run["id"],
            "artifact_ids": [artifact["id"]],
            "limit": 10,
        },
    )
    assert cleanup_preview["summary"]["matched_count"] == 1
    assert cleanup_preview["summary"]["protected_count"] == 1
    assert cleanup_preview["protected"][0]["artifact"]["id"] == artifact["id"]
    assert "standard_retention" in cleanup_preview["protected"][0]["reasons"]

    scratch_run = api(
        "post",
        "/api/research/runs",
        json={
            "run_type": "research_api_archive_smoke",
            "lifecycle_stage": "scratch",
            "retention_class": "scratch",
            "params": {"purpose": "archive api smoke"},
        },
    )
    scratch_artifact = api(
        "post",
        "/api/research/artifacts/json",
        json={
            "run_id": scratch_run["id"],
            "artifact_type": "api_archive_payload",
            "payload": {"archive": True},
            "lifecycle_stage": "scratch",
            "retention_class": "scratch",
        },
    )
    archived = api(
        "post",
        f"/api/research/artifacts/{scratch_artifact['id']}/archive",
        json={"archive_reason": "research API smoke"},
    )
    assert archived["id"] == scratch_artifact["id"]
    assert archived["lifecycle_stage"] == "archived"
    assert archived["retention_class"] == "archived"

    fetched_run = api("get", f"/api/research/runs/{run['id']}")
    assert any(ref["id"] == artifact["id"] for ref in fetched_run["output_refs"])

    runs = api("get", "/api/research/runs", params={"project_id": project["id"], "run_type": "research_api_smoke"})
    assert any(item["id"] == run["id"] for item in runs)

    artifacts = api("get", "/api/research/artifacts", params={"project_id": project["id"], "run_id": run["id"]})
    assert any(item["id"] == artifact["id"] for item in artifacts)

    lineage = api("get", f"/api/research/lineage/{run['id']}")
    assert any(edge["to_id"] == artifact["id"] for edge in lineage["edges"])

    qa = api(
        "post",
        "/api/research/agent/qa",
        json={
            "project_id": project["id"],
            "source_type": "strategy_graph",
            "source_id": "api_smoke_strategy_graph",
            "metrics": {
                "coverage": 1.0,
                "sharpe": 1.0,
                "max_drawdown": -0.1,
                "annual_turnover": 5.0,
            },
            "artifact_refs": [{"type": "artifact", "id": artifact["id"]}],
        },
    )
    assert qa["status"] in {"pass", "warning"}
    assert qa["blocking"] is False
    promotion = api(
        "post",
        "/api/research/agent/promotion",
        json={
            "source_type": "strategy_graph",
            "source_id": "api_smoke_strategy_graph",
            "qa_report_id": qa["id"],
            "approved_by": "api-smoke",
            "rationale": "promotion list smoke",
        },
    )
    assert promotion["decision"] == "promoted"
    promotions = api(
        "get",
        "/api/research/promotions",
        params={"project_id": project["id"], "source_type": "strategy_graph", "source_id": "api_smoke_strategy_graph"},
    )
    assert any(item["id"] == promotion["id"] for item in promotions)

    print(
        {
            "project_id": project["id"],
            "run_id": run["id"],
            "artifact_id": artifact["id"],
            "archived_artifact_id": archived["id"],
            "promotion_id": promotion["id"],
            "runs_listed": len(runs),
            "artifacts_listed": len(artifacts),
            "cleanup_preview_protected": cleanup_preview["summary"]["protected_count"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
