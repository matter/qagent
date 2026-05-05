#!/usr/bin/env python3
"""HTTP smoke test for QAgent 3.0 Factor Engine APIs."""

from __future__ import annotations

import time

import requests


BASE = "http://127.0.0.1:8000"

FACTOR_SOURCE = '''
from backend.factors.base import FactorBase
import pandas as pd

class Factor3ApiSmokeMomentum(FactorBase):
    name = "Factor3ApiSmokeMomentum"
    description = "5-day close momentum for M5 API smoke"
    params = {"window": 5}
    category = "momentum"

    def compute(self, data):
        return data["close"].pct_change(5)
'''


def api(method: str, path: str, **kwargs):
    response = getattr(requests, method)(f"{BASE}{path}", timeout=30, **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> int:
    health = api("get", "/api/health")
    assert health["status"] == "ok"

    legacy_factor = _ensure_legacy_factor()
    label = _ensure_label()
    universe = api(
        "post",
        "/api/research-assets/universes/legacy-group",
        json={
            "legacy_group_id": "test20",
            "market": "US",
            "project_id": "bootstrap_us",
            "name": "M5 API Smoke test20 universe",
        },
    )
    api(
        "post",
        f"/api/research-assets/universes/{universe['id']}/materialize",
        json={"start_date": "2025-01-02", "end_date": "2025-03-31"},
    )

    spec = api(
        "post",
        "/api/research-assets/factor-specs/legacy",
        json={
            "legacy_factor_id": legacy_factor["id"],
            "market": "US",
            "project_id": "bootstrap_us",
            "name": "M5 API Smoke FactorSpec",
        },
    )
    preview = api(
        "post",
        f"/api/research-assets/factor-specs/{spec['id']}/preview",
        json={
            "universe_id": universe["id"],
            "start_date": "2025-01-02",
            "end_date": "2025-03-31",
        },
    )
    task = api(
        "post",
        f"/api/research-assets/factor-specs/{spec['id']}/materialize",
        json={
            "universe_id": universe["id"],
            "start_date": "2025-01-02",
            "end_date": "2025-03-31",
        },
    )
    completed = _wait_task(task["task_id"])
    assert completed["status"] == "completed"

    runs = api(
        "get",
        "/api/research-assets/factor-runs",
        params={"factor_spec_id": spec["id"], "mode": "materialize"},
    )
    assert runs
    factor_run = runs[0]
    sample = api("get", f"/api/research-assets/factor-runs/{factor_run['id']}/sample", params={"limit": 5})
    evaluated = api(
        "post",
        f"/api/research-assets/factor-runs/{factor_run['id']}/evaluate",
        json={"label_id": label["id"]},
    )

    assert spec["source_type"] == "legacy_factor"
    assert preview["artifact"]["artifact_type"] == "factor_preview"
    assert factor_run["status"] == "completed"
    assert len(sample["rows"]) == 5
    assert evaluated["evaluation_artifact"]["artifact_type"] == "factor_evaluation"

    print(
        {
            "factor_spec_id": spec["id"],
            "factor_run_id": factor_run["id"],
            "task_id": task["task_id"],
            "api_run_id": task["run_id"],
            "sample_rows": len(sample["rows"]),
            "evaluation_artifact_id": evaluated["evaluation_artifact"]["id"],
        }
    )
    return 0


def _wait_task(task_id: str) -> dict:
    deadline = time.time() + 120
    while time.time() < deadline:
        task = api("get", f"/api/tasks/{task_id}")
        if task["status"] in {"completed", "failed", "timeout"}:
            if task["status"] != "completed":
                raise RuntimeError(task.get("error_message") or task)
            return task
        time.sleep(0.5)
    raise TimeoutError(task_id)


def _ensure_legacy_factor() -> dict:
    for factor in api("get", "/api/factors", params={"market": "US"}):
        if factor["name"] == "Factor3ApiSmokeMomentum":
            return factor
    return api(
        "post",
        "/api/factors",
        json={
            "name": "Factor3ApiSmokeMomentum",
            "description": "M5 API smoke legacy factor",
            "category": "custom",
            "source_code": FACTOR_SOURCE,
            "market": "US",
        },
    )


def _ensure_label() -> dict:
    for label in api("get", "/api/labels", params={"market": "US"}):
        if label["name"] == "fwd_return_5d":
            return label
    raise RuntimeError("Preset label fwd_return_5d was not seeded")


if __name__ == "__main__":
    raise SystemExit(main())
