"""QAgent 3.0 universe and dataset API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.research_kernel_service import ResearchKernelService
from backend.services.dataset_service import DatasetService
from backend.services.universe_service import UniverseService
from backend.tasks.executor import get_task_executor
from backend.tasks.models import TaskSource

router = APIRouter(prefix="/api/research-assets", tags=["research-assets"])


class CreateStaticUniverseRequest(BaseModel):
    project_id: str | None = None
    market_profile_id: str | None = None
    name: str
    description: str | None = None
    tickers: list[str]
    lifecycle_stage: str = "experiment"
    metadata: dict | None = None


class CreateLegacyUniverseRequest(BaseModel):
    project_id: str | None = None
    market: str | None = None
    legacy_group_id: str
    name: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"


class MaterializeUniverseRequest(BaseModel):
    start_date: str
    end_date: str
    lifecycle_stage: str = "experiment"


class CreateDatasetRequest(BaseModel):
    project_id: str | None = None
    market_profile_id: str | None = None
    name: str
    description: str | None = None
    universe_id: str
    feature_pipeline_id: str | None = None
    feature_set_id: str | None = None
    label_spec_id: str | None = None
    label_id: str | None = None
    start_date: str
    end_date: str
    split_policy: dict | None = None
    lifecycle_stage: str = "experiment"
    retention_class: str = "standard"
    metadata: dict | None = None


class QueryDatasetRequest(BaseModel):
    start_date: str | None = None
    end_date: str | None = None
    asset_ids: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    limit: int = Field(default=1000, ge=1, le=100000)


def _universe() -> UniverseService:
    return UniverseService()


def _dataset() -> DatasetService:
    return DatasetService()


def _get_executor():
    return get_task_executor()


@router.post("/universes/static")
async def create_static_universe(body: CreateStaticUniverseRequest) -> dict:
    try:
        return _universe().create_static_universe(
            name=body.name,
            tickers=body.tickers,
            project_id=body.project_id,
            market_profile_id=body.market_profile_id,
            description=body.description,
            lifecycle_stage=body.lifecycle_stage,
            metadata=body.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/universes/legacy-group")
async def create_universe_from_legacy_group(body: CreateLegacyUniverseRequest) -> dict:
    try:
        return _universe().create_from_legacy_group(
            legacy_group_id=body.legacy_group_id,
            project_id=body.project_id,
            market=body.market,
            name=body.name,
            description=body.description,
            lifecycle_stage=body.lifecycle_stage,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/universes/{universe_id}")
async def get_universe(universe_id: str) -> dict:
    try:
        return _universe().get_universe(universe_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/universes")
async def list_universes(
    project_id: str | None = None,
    market_profile_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    return _universe().list_universes(
        project_id=project_id,
        market_profile_id=market_profile_id,
        status=status,
        limit=limit,
    )


@router.post("/universes/{universe_id}/materialize")
async def materialize_universe(universe_id: str, body: MaterializeUniverseRequest) -> dict:
    try:
        return _universe().materialize_universe(
            universe_id,
            start_date=body.start_date,
            end_date=body.end_date,
            lifecycle_stage=body.lifecycle_stage,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/universes/{universe_id}/profile")
async def profile_universe(universe_id: str, run_id: str | None = Query(None)) -> dict:
    try:
        return _universe().profile_universe(universe_id, run_id=run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/datasets")
async def create_dataset(body: CreateDatasetRequest) -> dict:
    try:
        return _dataset().create_dataset(
            name=body.name,
            universe_id=body.universe_id,
            feature_pipeline_id=body.feature_pipeline_id,
            feature_set_id=body.feature_set_id,
            label_spec_id=body.label_spec_id,
            label_id=body.label_id,
            start_date=body.start_date,
            end_date=body.end_date,
            project_id=body.project_id,
            market_profile_id=body.market_profile_id,
            split_policy=body.split_policy,
            description=body.description,
            lifecycle_stage=body.lifecycle_stage,
            retention_class=body.retention_class,
            metadata=body.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: str) -> dict:
    try:
        return _dataset().get_dataset(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/datasets")
async def list_datasets(
    project_id: str | None = None,
    market_profile_id: str | None = None,
    universe_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    return _dataset().list_datasets(
        project_id=project_id,
        market_profile_id=market_profile_id,
        universe_id=universe_id,
        status=status,
        limit=limit,
    )


@router.post("/datasets/{dataset_id}/materialize")
async def materialize_dataset(dataset_id: str) -> dict:
    try:
        svc = _dataset()
        dataset = svc.get_dataset(dataset_id)
        run = ResearchKernelService().create_run(
            run_type="dataset_materialize",
            project_id=dataset["project_id"],
            market_profile_id=dataset["market_profile_id"],
            lifecycle_stage=dataset["lifecycle_stage"],
            retention_class=dataset["retention_class"],
            created_by="api",
            params={
                "dataset_id": dataset_id,
                "start_date": dataset["start_date"],
                "end_date": dataset["end_date"],
                "universe_id": dataset["universe_id"],
                "feature_pipeline_id": dataset["feature_pipeline_id"],
                "label_spec_id": dataset["label_spec_id"],
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    executor = _get_executor()

    task_id = executor.submit(
        task_type="dataset_materialize",
        fn=svc.materialize_dataset,
        params={"dataset_id": dataset_id, "run_id": run["id"]},
        run_id=run["id"],
        timeout=3600,
        source=TaskSource.AGENT,
    )
    return {
        "task_id": task_id,
        "run_id": run["id"],
        "status": "queued",
        "task_type": "dataset_materialize",
        "poll_url": f"/api/tasks/{task_id}",
    }


@router.get("/datasets/{dataset_id}/profile")
async def profile_dataset(dataset_id: str) -> dict:
    try:
        return _dataset().profile_dataset(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/datasets/{dataset_id}/sample")
async def sample_dataset(
    dataset_id: str,
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    try:
        return _dataset().sample_dataset(dataset_id, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/datasets/{dataset_id}/query")
async def query_dataset(dataset_id: str, body: QueryDatasetRequest) -> dict:
    try:
        return _dataset().query_dataset(
            dataset_id,
            start_date=body.start_date,
            end_date=body.end_date,
            asset_ids=body.asset_ids,
            columns=body.columns,
            limit=body.limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
