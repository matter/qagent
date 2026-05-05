"""QAgent 3.0 model experiment API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.services.model_experiment_3_service import ModelExperiment3Service
from backend.services.research_kernel_service import ResearchKernelService
from backend.tasks.executor import get_task_executor
from backend.tasks.models import TaskSource

router = APIRouter(prefix="/api/research-assets", tags=["research-assets"])


class TrainExperimentRequest(BaseModel):
    name: str
    dataset_id: str
    model_type: str = "lightgbm"
    objective: str = "regression"
    model_params: dict | None = None
    random_seed: int = 42
    lifecycle_stage: str = "experiment"


class PromoteExperimentRequest(BaseModel):
    package_name: str | None = None
    approved_by: str = "api"
    rationale: str | None = None
    lifecycle_stage: str = "candidate"


class PredictPanelRequest(BaseModel):
    dataset_id: str


def _svc() -> ModelExperiment3Service:
    return ModelExperiment3Service()


def _executor():
    return get_task_executor()


@router.post("/model-experiments/train")
async def train_model_experiment(body: TrainExperimentRequest) -> dict:
    try:
        dataset = _svc()._datasets.get_dataset(body.dataset_id)  # noqa: SLF001
        run = ResearchKernelService().create_run(
            run_type="model_train_experiment",
            project_id=dataset["project_id"],
            market_profile_id=dataset["market_profile_id"],
            lifecycle_stage=body.lifecycle_stage,
            retention_class="standard",
            created_by="api",
            params=body.model_dump(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    def _do_train(payload: dict) -> dict:
        result = ModelExperiment3Service().train_experiment(**payload)
        ResearchKernelService().update_run_status(
            run["id"],
            status="completed",
            metrics_summary=result["metrics"],
            qa_summary=result["qa"],
        )
        return result

    task_id = _executor().submit(
        task_type="model_train_experiment_3_0",
        fn=_do_train,
        params={"payload": body.model_dump()},
        run_id=run["id"],
        timeout=7200,
        source=TaskSource.AGENT,
    )
    return {
        "task_id": task_id,
        "run_id": run["id"],
        "status": "queued",
        "task_type": "model_train_experiment_3_0",
        "poll_url": f"/api/tasks/{task_id}",
    }


@router.get("/model-experiments")
async def list_model_experiments(dataset_id: str | None = Query(None)) -> list[dict]:
    return _svc().list_experiments(dataset_id=dataset_id)


@router.get("/model-experiments/{experiment_id}")
async def get_model_experiment(experiment_id: str) -> dict:
    try:
        return _svc().get_experiment(experiment_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/model-experiments/{experiment_id}/promote")
async def promote_model_experiment(experiment_id: str, body: PromoteExperimentRequest) -> dict:
    try:
        return _svc().promote_experiment(
            experiment_id,
            package_name=body.package_name,
            approved_by=body.approved_by,
            rationale=body.rationale,
            lifecycle_stage=body.lifecycle_stage,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/model-packages/{package_id}")
async def get_model_package(package_id: str) -> dict:
    try:
        return _svc().get_model_package(package_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/model-packages/{package_id}/predict-panel")
async def predict_model_package_panel(package_id: str, body: PredictPanelRequest) -> dict:
    try:
        return _svc().predict_panel(model_package_id=package_id, dataset_id=body.dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
