"""QAgent 3.0 agent research plan, QA gate, and playbook API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.services.agent_research_3_service import AgentResearch3Service

router = APIRouter(prefix="/api/research/agent", tags=["research"])


class CreateResearchPlanRequest(BaseModel):
    hypothesis: str
    playbook_id: str | None = None
    search_space: dict | None = None
    budget: dict | None = None
    stop_conditions: dict | None = None
    project_id: str | None = None
    market_profile_id: str | None = None
    created_by: str = "agent"
    metadata: dict | None = None


class RecordTrialRequest(BaseModel):
    trial_type: str
    params: dict | None = None
    result_refs: list[dict] | None = None
    metrics: dict | None = None
    qa_report_id: str | None = None
    status: str = "completed"


class RecordTrialsRequest(BaseModel):
    trials: list[RecordTrialRequest]
    dedupe_by_params: bool = True


class EvaluateQaRequest(BaseModel):
    source_type: str
    source_id: str
    metrics: dict | None = None
    artifact_refs: list[dict] | None = None
    project_id: str | None = None
    market_profile_id: str | None = None


class EvaluatePromotionRequest(BaseModel):
    source_type: str
    source_id: str
    qa_report_id: str
    metrics: dict | None = None
    policy_id: str | None = None
    approved_by: str = "agent"
    rationale: str | None = None


def _svc() -> AgentResearch3Service:
    return AgentResearch3Service()


@router.post("/playbooks/ensure-builtins")
async def ensure_builtin_playbooks() -> list[dict]:
    return _svc().ensure_builtin_playbooks()


@router.get("/playbooks")
async def list_playbooks() -> list[dict]:
    return _svc().list_playbooks()


@router.get("/playbooks/{playbook_id}")
async def get_playbook(playbook_id: str) -> dict:
    try:
        return _svc().get_playbook(playbook_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/plans")
async def create_research_plan(body: CreateResearchPlanRequest) -> dict:
    try:
        return _svc().create_research_plan(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/plans")
async def list_research_plans(
    project_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    return _svc().list_plans(project_id=project_id, status=status, limit=limit)


@router.get("/plans/{plan_id}")
async def get_research_plan(plan_id: str) -> dict:
    try:
        return _svc().get_plan(plan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/plans/{plan_id}/performance")
async def get_plan_performance(
    plan_id: str,
    primary_metric: str = Query("sharpe"),
    top_n: int = Query(10, ge=1, le=100),
) -> dict:
    try:
        return _svc().get_plan_performance(
            plan_id,
            primary_metric=primary_metric,
            top_n=top_n,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/plans/{plan_id}/budget")
async def check_plan_budget(plan_id: str) -> dict:
    try:
        return _svc().check_budget(plan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/plans/{plan_id}/trials")
async def record_research_trial(plan_id: str, body: RecordTrialRequest) -> dict:
    try:
        return _svc().record_trial(plan_id, **body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/plans/{plan_id}/trials/batch")
async def record_research_trials_batch(plan_id: str, body: RecordTrialsRequest) -> dict:
    try:
        return _svc().record_trials(
            plan_id,
            trials=[item.model_dump() for item in body.trials],
            dedupe_by_params=body.dedupe_by_params,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/plans/{plan_id}/trials")
async def list_research_trials(
    plan_id: str,
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    try:
        return _svc().list_trials(plan_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/qa")
async def evaluate_qa(body: EvaluateQaRequest) -> dict:
    try:
        return _svc().evaluate_qa(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/qa")
async def list_qa_reports(
    source_type: str | None = Query(None),
    source_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    return _svc().list_qa_reports(
        source_type=source_type,
        source_id=source_id,
        status=status,
        limit=limit,
    )


@router.get("/qa/{qa_report_id}")
async def get_qa_report(qa_report_id: str) -> dict:
    try:
        return _svc().get_qa_report(qa_report_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/promotion")
async def evaluate_promotion(body: EvaluatePromotionRequest) -> dict:
    try:
        return _svc().evaluate_promotion(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/promotion-policies/default")
async def ensure_default_promotion_policy(
    project_id: str | None = None,
    market_profile_id: str | None = None,
) -> dict:
    try:
        return _svc().ensure_default_promotion_policy(
            project_id=project_id,
            market_profile_id=market_profile_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
