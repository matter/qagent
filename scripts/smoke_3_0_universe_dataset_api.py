#!/usr/bin/env python3
"""HTTP smoke test for QAgent 3.0 universe/dataset APIs."""

from __future__ import annotations

import time

import requests


BASE = "http://127.0.0.1:8000"

FACTOR_SOURCE = '''
from backend.factors.base import FactorBase
import pandas as pd

class DatasetSmokeMomentum(FactorBase):
    name = "DatasetSmokeMomentum"
    description = "5-day close momentum for M4 API smoke"

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

    factor = _ensure_factor()
    feature_set = _ensure_feature_set(factor)
    label = _ensure_label()

    universe = api(
        "post",
        "/api/research-assets/universes/legacy-group",
        json={
            "legacy_group_id": "test20",
            "market": "US",
            "project_id": "bootstrap_us",
            "name": "M4 API Smoke test20 universe",
        },
    )
    universes = api("get", "/api/research-assets/universes", params={"project_id": "bootstrap_us"})
    assert any(item["id"] == universe["id"] for item in universes)

    materialized_universe = api(
        "post",
        f"/api/research-assets/universes/{universe['id']}/materialize",
        json={"start_date": "2025-01-02", "end_date": "2025-03-31"},
    )
    assert materialized_universe["materialization"]["asset_count"] >= 10

    dataset = api(
        "post",
        "/api/research-assets/datasets",
        json={
            "name": "M4 API Smoke Dataset",
            "universe_id": universe["id"],
            "feature_set_id": feature_set["id"],
            "label_id": label["id"],
            "start_date": "2025-01-02",
            "end_date": "2025-03-31",
            "split_policy": {
                "train": {"start": "2025-01-02", "end": "2025-02-14"},
                "valid": {"start": "2025-02-18", "end": "2025-03-07"},
                "test": {"start": "2025-03-10", "end": "2025-03-31"},
                "purge_gap": 5,
            },
        },
    )
    datasets = api("get", "/api/research-assets/datasets", params={"project_id": "bootstrap_us"})
    assert any(item["id"] == dataset["id"] for item in datasets)

    task = api("post", f"/api/research-assets/datasets/{dataset['id']}/materialize")
    assert task["task_id"]
    assert task["run_id"]
    completed = _wait_task(task["task_id"])
    assert completed["status"] == "completed"

    profile = api("get", f"/api/research-assets/datasets/{dataset['id']}/profile")
    sample = api("get", f"/api/research-assets/datasets/{dataset['id']}/sample", params={"limit": 5})
    query = api(
        "post",
        f"/api/research-assets/datasets/{dataset['id']}/query",
        json={
            "start_date": "2025-03-03",
            "end_date": "2025-03-07",
            "columns": ["DatasetSmokeMomentum", "label"],
            "limit": 10,
        },
    )

    assert profile["coverage"]["row_count"] > 0
    assert len(sample["rows"]) == 5
    assert query["count"] > 0

    print(
        {
            "universe_id": universe["id"],
            "dataset_id": dataset["id"],
            "task_id": task["task_id"],
            "run_id": task["run_id"],
            "rows": profile["coverage"]["row_count"],
            "universes_listed": len(universes),
            "datasets_listed": len(datasets),
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


def _ensure_factor() -> dict:
    factors = api("get", "/api/factors")
    for factor in factors:
        if factor["name"] == "DatasetSmokeMomentum":
            return factor
    return api(
        "post",
        "/api/factors",
        json={
            "name": "DatasetSmokeMomentum",
            "description": "M4 API smoke factor",
            "category": "custom",
            "source_code": FACTOR_SOURCE,
            "market": "US",
        },
    )


def _ensure_feature_set(factor: dict) -> dict:
    feature_sets = api("get", "/api/feature-sets", params={"market": "US"})
    for feature_set in feature_sets:
        if feature_set["name"] == "M4 API Smoke Feature Set":
            return feature_set
    return api(
        "post",
        "/api/feature-sets",
        json={
            "market": "US",
            "name": "M4 API Smoke Feature Set",
            "description": "M4 API dataset smoke feature set",
            "factor_refs": [
                {
                    "factor_id": factor["id"],
                    "factor_name": "DatasetSmokeMomentum",
                    "version": factor["version"],
                }
            ],
            "preprocessing": {
                "missing": "forward_fill",
                "outlier": None,
                "normalize": "rank",
                "neutralize": None,
            },
        },
    )


def _ensure_label() -> dict:
    labels = api("get", "/api/labels", params={"market": "US"})
    for label in labels:
        if label["name"] == "fwd_return_5d":
            return label
    raise RuntimeError("Preset label fwd_return_5d was not seeded")


if __name__ == "__main__":
    raise SystemExit(main())
