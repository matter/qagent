"""QAgent 3.0 factor asset API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.factor_engine_3_service import FactorEngine3Service
from backend.services.research_kernel_service import ResearchKernelService
from backend.tasks.executor import get_task_executor
from backend.tasks.models import TaskSource

router = APIRouter(prefix="/api/research-assets", tags=["research-assets"])


class ImportLegacyFactorRequest(BaseModel):
    legacy_factor_id: str
    project_id: str | None = None
    market: str | None = None
    name: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"
    semantic_tags: list[str] | None = None
    metadata: dict | None = None


class CreateFactorSpecRequest(BaseModel):
    project_id: str | None = None
    market_profile_id: str | None = None
    name: str
    description: str | None = None
    source_code: str
    params_schema: dict | None = None
    default_params: dict | None = None
    required_inputs: list[str] | None = None
    compute_mode: str = "time_series"
    expected_warmup: int = 0
    applicable_profiles: list[str] | None = None
    semantic_tags: list[str] | None = None
    lifecycle_stage: str = "experiment"
    status: str = "draft"
    metadata: dict | None = None


class FactorComputeRequest(BaseModel):
    universe_id: str
    start_date: str
    end_date: str
    params: dict | None = None


class FactorMaterializeRequest(FactorComputeRequest):
    lifecycle_stage: str | None = None


class FactorEvaluateRequest(BaseModel):
    label_id: str
    start_date: str | None = None
    end_date: str | None = None


def _svc() -> FactorEngine3Service:
    return FactorEngine3Service()


def _executor():
    return get_task_executor()


@router.post("/factor-specs/legacy")
async def import_legacy_factor(body: ImportLegacyFactorRequest) -> dict:
    try:
        return _svc().create_spec_from_legacy_factor(
            legacy_factor_id=body.legacy_factor_id,
            project_id=body.project_id,
            market=body.market,
            name=body.name,
            description=body.description,
            lifecycle_stage=body.lifecycle_stage,
            semantic_tags=body.semantic_tags,
            metadata=body.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/factor-specs")
async def create_factor_spec(body: CreateFactorSpecRequest) -> dict:
    try:
        return _svc().create_python_spec(
            name=body.name,
            source_code=body.source_code,
            project_id=body.project_id,
            market_profile_id=body.market_profile_id,
            description=body.description,
            params_schema=body.params_schema,
            default_params=body.default_params,
            required_inputs=body.required_inputs,
            compute_mode=body.compute_mode,
            expected_warmup=body.expected_warmup,
            applicable_profiles=body.applicable_profiles,
            semantic_tags=body.semantic_tags,
            lifecycle_stage=body.lifecycle_stage,
            status=body.status,
            metadata=body.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/factor-specs")
async def list_factor_specs(
    project_id: str | None = Query(None),
    market_profile_id: str | None = Query(None),
    status: str | None = Query(None),
) -> list[dict]:
    try:
        return _svc().list_factor_specs(
            project_id=project_id,
            market_profile_id=market_profile_id,
            status=status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/factor-specs/{factor_spec_id}")
async def get_factor_spec(factor_spec_id: str) -> dict:
    try:
        return _svc().get_factor_spec(factor_spec_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/factor-specs/{factor_spec_id}/preview")
async def preview_factor(factor_spec_id: str, body: FactorComputeRequest) -> dict:
    try:
        return _svc().preview_factor(
            factor_spec_id=factor_spec_id,
            universe_id=body.universe_id,
            start_date=body.start_date,
            end_date=body.end_date,
            params=body.params,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/factor-specs/{factor_spec_id}/materialize")
async def materialize_factor(factor_spec_id: str, body: FactorMaterializeRequest) -> dict:
    try:
        svc = _svc()
        spec = svc.get_factor_spec(factor_spec_id)
        universe_id = body.universe_id
        run = ResearchKernelService().create_run(
            run_type="factor_materialize",
            project_id=spec["project_id"],
            market_profile_id=spec["market_profile_id"],
            lifecycle_stage=body.lifecycle_stage or spec["lifecycle_stage"],
            retention_class="rebuildable",
            created_by="api",
            params={
                "factor_spec_id": factor_spec_id,
                "universe_id": universe_id,
                "start_date": body.start_date,
                "end_date": body.end_date,
                "params": body.params or {},
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    def _do_materialize(
        factor_spec_id: str,
        universe_id: str,
        start_date: str,
        end_date: str,
        params: dict | None,
        lifecycle_stage: str | None,
    ) -> dict:
        # M5 service creates its own domain run.  The API-created run gives
        # callers an immediate task/run binding while keeping service logic
        # side-effect complete and reusable by MCP.
        result = FactorEngine3Service().materialize_factor(
            factor_spec_id=factor_spec_id,
            universe_id=universe_id,
            start_date=start_date,
            end_date=end_date,
            params=params,
            lifecycle_stage=lifecycle_stage,
        )
        ResearchKernelService().update_run_status(
            run["id"],
            status="completed",
            metrics_summary={
                "factor_run_id": result["factor_run"]["id"],
                "artifact_id": result["artifact"]["id"],
                "rows": result["profile"]["coverage"]["row_count"],
            },
            qa_summary=result["qa"],
        )
        return result

    task_id = _executor().submit(
        task_type="factor_materialize_3_0",
        fn=_do_materialize,
        params={
            "factor_spec_id": factor_spec_id,
            "universe_id": body.universe_id,
            "start_date": body.start_date,
            "end_date": body.end_date,
            "params": body.params,
            "lifecycle_stage": body.lifecycle_stage,
        },
        run_id=run["id"],
        timeout=3600,
        source=TaskSource.AGENT,
    )
    return {
        "task_id": task_id,
        "run_id": run["id"],
        "status": "queued",
        "task_type": "factor_materialize_3_0",
        "poll_url": f"/api/tasks/{task_id}",
    }


@router.post("/factor-runs/{factor_run_id}/evaluate")
async def evaluate_factor_run(factor_run_id: str, body: FactorEvaluateRequest) -> dict:
    try:
        return _svc().evaluate_factor_run(
            factor_run_id=factor_run_id,
            label_id=body.label_id,
            start_date=body.start_date,
            end_date=body.end_date,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/factor-runs")
async def list_factor_runs(
    factor_spec_id: str | None = Query(None),
    universe_id: str | None = Query(None),
    mode: str | None = Query(None),
) -> list[dict]:
    return _svc().list_factor_runs(
        factor_spec_id=factor_spec_id,
        universe_id=universe_id,
        mode=mode,
    )


@router.get("/factor-runs/{factor_run_id}")
async def get_factor_run(factor_run_id: str) -> dict:
    try:
        return _svc().get_factor_run(factor_run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/factor-runs/{factor_run_id}/sample")
async def sample_factor_run(
    factor_run_id: str,
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    try:
        return _svc().sample_factor_run(factor_run_id, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
