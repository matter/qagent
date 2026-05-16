"""QAgent 3.0 portfolio/risk/execution asset API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.services.portfolio_assets_3_service import PortfolioAssets3Service

router = APIRouter(prefix="/api/research-assets", tags=["research-assets"])


class PortfolioConstructionSpecRequest(BaseModel):
    name: str
    method: str
    params: dict | None = None
    project_id: str | None = None
    market_profile_id: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"
    status: str = "draft"
    metadata: dict | None = None


class RiskControlSpecRequest(BaseModel):
    name: str
    rules: list[dict] | None = None
    params: dict | None = None
    project_id: str | None = None
    market_profile_id: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"
    status: str = "draft"
    metadata: dict | None = None


class PolicySpecRequest(BaseModel):
    name: str
    policy_type: str
    params: dict | None = None
    project_id: str | None = None
    market_profile_id: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"
    status: str = "draft"
    metadata: dict | None = None


class PositionControllerSpecRequest(BaseModel):
    name: str
    controller_type: str = "threshold"
    params: dict | None = None
    project_id: str | None = None
    market_profile_id: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"
    status: str = "draft"
    metadata: dict | None = None


class ConstructPortfolioRequest(BaseModel):
    decision_date: str
    alpha_frame: list[dict]
    portfolio_spec_id: str
    risk_control_spec_id: str | None = None
    rebalance_policy_spec_id: str | None = None
    position_controller_spec_id: str | None = None
    execution_policy_spec_id: str | None = None
    state_policy_spec_id: str | None = None
    current_weights: dict[str, float] | None = None
    portfolio_value: float = 1_000_000
    lifecycle_stage: str = "experiment"


class ComparePortfolioBuildersRequest(BaseModel):
    decision_date: str
    alpha_frame: list[dict]
    portfolio_spec_ids: list[str]
    risk_control_spec_id: str | None = None
    current_weights: dict[str, float] | None = None


def _svc() -> PortfolioAssets3Service:
    return PortfolioAssets3Service()


@router.post("/portfolio-construction-specs")
async def create_portfolio_construction_spec(body: PortfolioConstructionSpecRequest) -> dict:
    try:
        return _svc().create_portfolio_construction_spec(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolio-construction-specs/{spec_id}")
async def get_portfolio_construction_spec(spec_id: str) -> dict:
    try:
        return _svc().get_portfolio_construction_spec(spec_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/risk-control-specs")
async def create_risk_control_spec(body: RiskControlSpecRequest) -> dict:
    try:
        return _svc().create_risk_control_spec(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/risk-control-specs/{spec_id}")
async def get_risk_control_spec(spec_id: str) -> dict:
    try:
        return _svc().get_risk_control_spec(spec_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/rebalance-policy-specs")
async def create_rebalance_policy_spec(body: PolicySpecRequest) -> dict:
    try:
        return _svc().create_rebalance_policy_spec(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/rebalance-policy-specs/{spec_id}")
async def get_rebalance_policy_spec(spec_id: str) -> dict:
    try:
        return _svc().get_rebalance_policy_spec(spec_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/position-controller-specs")
async def create_position_controller_spec(body: PositionControllerSpecRequest) -> dict:
    try:
        return _svc().create_position_controller_spec(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/position-controller-specs/{spec_id}")
async def get_position_controller_spec(spec_id: str) -> dict:
    try:
        return _svc().get_position_controller_spec(spec_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/execution-policy-specs")
async def create_execution_policy_spec(body: PolicySpecRequest) -> dict:
    try:
        return _svc().create_execution_policy_spec(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/execution-policy-specs/{spec_id}")
async def get_execution_policy_spec(spec_id: str) -> dict:
    try:
        return _svc().get_execution_policy_spec(spec_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/state-policy-specs")
async def create_state_policy_spec(body: PolicySpecRequest) -> dict:
    try:
        return _svc().create_state_policy_spec(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/portfolio-runs/construct")
async def construct_portfolio(body: ConstructPortfolioRequest) -> dict:
    try:
        return _svc().construct_portfolio(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/portfolio-runs/compare-builders")
async def compare_portfolio_builders(body: ComparePortfolioBuildersRequest) -> dict:
    try:
        return _svc().compare_builders(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolio-runs")
async def list_portfolio_runs(limit: int = Query(50, ge=1, le=500)) -> list[dict]:
    return _svc().list_portfolio_runs(limit=limit)


@router.get("/portfolio-runs/{portfolio_run_id}")
async def get_portfolio_run(portfolio_run_id: str) -> dict:
    try:
        return _svc().get_portfolio_run(portfolio_run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
