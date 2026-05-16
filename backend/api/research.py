"""QAgent 3.0 Research Kernel API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.services.research_kernel_service import ResearchKernelService

router = APIRouter(prefix="/api/research", tags=["research"])


class CreateRunRequest(BaseModel):
    run_type: str
    project_id: str | None = None
    market_profile_id: str | None = None
    lifecycle_stage: str = "experiment"
    retention_class: str = "standard"
    created_by: str = "system"
    params: dict | None = None


class CreateJsonArtifactRequest(BaseModel):
    run_id: str
    artifact_type: str
    payload: dict
    lifecycle_stage: str | None = None
    retention_class: str = "standard"
    metadata: dict | None = None
    rebuildable: bool = True


class ListRunsQuery(BaseModel):
    project_id: str | None = None
    run_type: str | None = None
    status: str | None = None
    lifecycle_stage: str | None = None
    created_by: str | None = None
    limit: int = 50


class ListArtifactsQuery(BaseModel):
    project_id: str | None = None
    run_id: str | None = None
    artifact_type: str | None = None
    lifecycle_stage: str | None = None
    retention_class: str | None = None
    limit: int = 50


class CleanupPreviewRequest(BaseModel):
    project_id: str | None = None
    run_id: str | None = None
    artifact_ids: list[str] | None = None
    lifecycle_stage: str | None = None
    retention_class: str | None = None
    artifact_type: str | None = None
    include_published: bool = False
    limit: int = 500


class CleanupApplyRequest(CleanupPreviewRequest):
    confirm: bool = False
    archive_reason: str | None = None


class ArchiveArtifactRequest(BaseModel):
    retention_class: str = "archived"
    archive_reason: str | None = None


def _svc() -> ResearchKernelService:
    return ResearchKernelService()


@router.get("/projects/bootstrap")
async def get_bootstrap_project() -> dict:
    return _svc().get_bootstrap_project()


@router.post("/runs")
async def create_run(body: CreateRunRequest) -> dict:
    try:
        return _svc().create_run(
            run_type=body.run_type,
            params=body.params,
            project_id=body.project_id,
            market_profile_id=body.market_profile_id,
            lifecycle_stage=body.lifecycle_stage,
            retention_class=body.retention_class,
            created_by=body.created_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}")
async def get_run(run_id: str) -> dict:
    try:
        return _svc().get_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs")
async def list_runs(
    project_id: str | None = None,
    run_type: str | None = None,
    status: str | None = None,
    lifecycle_stage: str | None = None,
    created_by: str | None = None,
    limit: int = 50,
) -> list[dict]:
    return _svc().list_runs(
        project_id=project_id,
        run_type=run_type,
        status=status,
        lifecycle_stage=lifecycle_stage,
        created_by=created_by,
        limit=limit,
    )


@router.post("/artifacts/json")
async def create_json_artifact(body: CreateJsonArtifactRequest) -> dict:
    try:
        return _svc().create_json_artifact(
            run_id=body.run_id,
            artifact_type=body.artifact_type,
            payload=body.payload,
            lifecycle_stage=body.lifecycle_stage,
            retention_class=body.retention_class,
            metadata=body.metadata,
            rebuildable=body.rebuildable,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/artifacts")
async def list_artifacts(
    project_id: str | None = None,
    run_id: str | None = None,
    artifact_type: str | None = None,
    lifecycle_stage: str | None = None,
    retention_class: str | None = None,
    limit: int = 50,
) -> list[dict]:
    return _svc().list_artifacts(
        project_id=project_id,
        run_id=run_id,
        artifact_type=artifact_type,
        lifecycle_stage=lifecycle_stage,
        retention_class=retention_class,
        limit=limit,
    )


@router.post("/artifacts/cleanup-preview")
async def preview_artifact_cleanup(body: CleanupPreviewRequest) -> dict:
    return _svc().preview_artifact_cleanup(
        project_id=body.project_id,
        run_id=body.run_id,
        artifact_ids=body.artifact_ids,
        lifecycle_stage=body.lifecycle_stage,
        retention_class=body.retention_class,
        artifact_type=body.artifact_type,
        include_published=body.include_published,
        limit=body.limit,
    )


@router.post("/artifacts/cleanup-apply")
async def apply_artifact_cleanup(body: CleanupApplyRequest) -> dict:
    try:
        return _svc().apply_artifact_cleanup(
            project_id=body.project_id,
            run_id=body.run_id,
            artifact_ids=body.artifact_ids,
            lifecycle_stage=body.lifecycle_stage,
            retention_class=body.retention_class,
            artifact_type=body.artifact_type,
            include_published=body.include_published,
            limit=body.limit,
            confirm=body.confirm,
            archive_reason=body.archive_reason,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/artifacts/{artifact_id}/archive")
async def archive_artifact(artifact_id: str, body: ArchiveArtifactRequest) -> dict:
    try:
        return _svc().archive_artifact(
            artifact_id,
            retention_class=body.retention_class,
            archive_reason=body.archive_reason,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/artifacts/{artifact_id}")
async def get_artifact(artifact_id: str) -> dict:
    try:
        return _svc().get_artifact(artifact_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/promotions")
async def list_promotion_records(
    project_id: str | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    target_type: str | None = None,
    target_id: str | None = None,
    decision: str | None = None,
    limit: int = 50,
) -> list[dict]:
    return _svc().list_promotion_records(
        project_id=project_id,
        source_type=source_type,
        source_id=source_id,
        target_type=target_type,
        target_id=target_id,
        decision=decision,
        limit=limit,
    )


@router.get("/promotions/{promotion_id}")
async def get_promotion_record(promotion_id: str) -> dict:
    try:
        return _svc().get_promotion_record(promotion_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/lineage/{run_id}")
async def get_lineage(run_id: str) -> dict:
    return _svc().get_lineage(run_id)
