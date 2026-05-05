#!/usr/bin/env python3
"""HTTP smoke test for QAgent 3.0 model experiment APIs."""

from __future__ import annotations

import time

import requests


BASE = "http://127.0.0.1:8000"

FACTOR_SOURCE = '''
from backend.factors.base import FactorBase
import pandas as pd

class Model3ApiSmokeMomentum(FactorBase):
    name = "Model3ApiSmokeMomentum"
    description = "5-day close momentum for M6 API smoke"

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
            "name": "M6 API Smoke test20 universe",
        },
    )
    dataset = api(
        "post",
        "/api/research-assets/datasets",
        json={
            "name": "M6 API Smoke Dataset",
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
    dataset_task = api("post", f"/api/research-assets/datasets/{dataset['id']}/materialize")
    _wait_task(dataset_task["task_id"])

    train_task = api(
        "post",
        "/api/research-assets/model-experiments/train",
        json={
            "name": "M6 API Smoke Model Experiment",
            "dataset_id": dataset["id"],
            "model_params": {"n_estimators": 12, "max_depth": 3, "random_state": 42},
            "random_seed": 42,
        },
    )
    _wait_task(train_task["task_id"])
    experiments = api("get", "/api/research-assets/model-experiments", params={"dataset_id": dataset["id"]})
    assert experiments
    experiment = experiments[0]
    promoted = api(
        "post",
        f"/api/research-assets/model-experiments/{experiment['id']}/promote",
        json={
            "package_name": "M6 API Smoke Model Package",
            "approved_by": "api-smoke",
            "rationale": "M6 API smoke promotion",
        },
    )
    predicted = api(
        "post",
        f"/api/research-assets/model-packages/{promoted['package']['id']}/predict-panel",
        json={"dataset_id": dataset["id"]},
    )

    assert experiment["status"] == "completed"
    assert promoted["promotion_record"]["decision"] == "promoted"
    assert predicted["profile"]["row_count"] > 0

    print(
        {
            "dataset_id": dataset["id"],
            "experiment_id": experiment["id"],
            "package_id": promoted["package"]["id"],
            "train_task_id": train_task["task_id"],
            "prediction_rows": predicted["profile"]["row_count"],
        }
    )
    return 0


def _wait_task(task_id: str) -> dict:
    deadline = time.time() + 180
    while time.time() < deadline:
        task = api("get", f"/api/tasks/{task_id}")
        if task["status"] in {"completed", "failed", "timeout"}:
            if task["status"] != "completed":
                raise RuntimeError(task.get("error_message") or task)
            return task
        time.sleep(0.5)
    raise TimeoutError(task_id)


def _ensure_factor() -> dict:
    for factor in api("get", "/api/factors", params={"market": "US"}):
        if factor["name"] == "Model3ApiSmokeMomentum":
            return factor
    return api(
        "post",
        "/api/factors",
        json={
            "name": "Model3ApiSmokeMomentum",
            "description": "M6 API smoke factor",
            "category": "custom",
            "source_code": FACTOR_SOURCE,
            "market": "US",
        },
    )


def _ensure_feature_set(factor: dict) -> dict:
    for feature_set in api("get", "/api/feature-sets", params={"market": "US"}):
        if feature_set["name"] == "M6 API Smoke Feature Set":
            return feature_set
    return api(
        "post",
        "/api/feature-sets",
        json={
            "market": "US",
            "name": "M6 API Smoke Feature Set",
            "description": "M6 API model smoke feature set",
            "factor_refs": [{"factor_id": factor["id"], "factor_name": "Model3ApiSmokeMomentum", "version": 1}],
            "preprocessing": {"missing": "forward_fill", "outlier": None, "normalize": None, "neutralize": None},
        },
    )


def _ensure_label() -> dict:
    for label in api("get", "/api/labels", params={"market": "US"}):
        if label["name"] == "fwd_return_5d":
            return label
    raise RuntimeError("Preset label fwd_return_5d was not seeded")


if __name__ == "__main__":
    raise SystemExit(main())
