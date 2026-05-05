#!/usr/bin/env python3
"""Smoke test for the QAgent 3.0 Research Kernel.

This is intentionally small and service-level. It verifies the M1 contract:
bootstrap project, run creation, artifact persistence, lineage, and task/run
linkage without depending on market data.
"""

from __future__ import annotations

import time

from backend.db import init_db
from backend.services.research_kernel_service import ResearchKernelService
from backend.tasks.models import TaskStatus
from backend.tasks.executor import TaskExecutor


def _demo_task() -> dict:
    return {"ok": True, "message": "kernel smoke"}


def main() -> int:
    init_db()
    kernel = ResearchKernelService()

    project = kernel.get_bootstrap_project()
    assert project["id"], "bootstrap project must have an id"
    assert project["name"] == "US Research"

    run = kernel.create_run(
        run_type="kernel_smoke",
        params={"purpose": "M1 smoke"},
        lifecycle_stage="scratch",
        created_by="system",
    )
    assert run["project_id"] == project["id"]
    assert run["status"] == "queued"

    artifact = kernel.create_json_artifact(
        run_id=run["id"],
        artifact_type="smoke_result",
        payload={"hello": "world"},
        lifecycle_stage="scratch",
    )
    assert artifact["run_id"] == run["id"]
    assert artifact["content_hash"]

    edges = kernel.get_lineage(run["id"])
    assert any(edge["to_id"] == artifact["id"] for edge in edges["edges"])

    executor = TaskExecutor()
    task_id = executor.submit(
        "kernel_smoke_task",
        _demo_task,
        run_id=run["id"],
        timeout=30,
    )
    task = executor.get_task(task_id)
    assert task is not None
    assert task.run_id == run["id"]

    for _ in range(30):
        task = executor.get_task(task_id)
        if task and task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT):
            break
        time.sleep(0.1)
    task = executor.get_task(task_id)
    assert task is not None
    assert task.run_id == run["id"]
    assert task.status == TaskStatus.COMPLETED
    assert task.result_summary and task.result_summary.get("ok") is True

    print(
        {
            "project_id": project["id"],
            "run_id": run["id"],
            "artifact_id": artifact["id"],
            "task_id": task_id,
        }
    )
    executor.shutdown(wait=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
