"""QAgent 3.0 StrategyGraph runtime API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.services.strategy_graph_3_service import StrategyGraph3Service

router = APIRouter(prefix="/api/research-assets", tags=["research-assets"])


class BuiltinAlphaGraphRequest(BaseModel):
    name: str
    selection_policy: dict | None = None
    portfolio_construction_spec_id: str
    risk_control_spec_id: str | None = None
    rebalance_policy_spec_id: str | None = None
    execution_policy_spec_id: str | None = None
    state_policy_spec_id: str | None = None
    project_id: str | None = None
    market_profile_id: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"
    status: str = "draft"
    metadata: dict | None = None


class LegacyAdapterGraphRequest(BaseModel):
    name: str
    legacy_strategy_id: str
    portfolio_construction_spec_id: str
    risk_control_spec_id: str | None = None
    rebalance_policy_spec_id: str | None = None
    execution_policy_spec_id: str | None = None
    state_policy_spec_id: str | None = None
    project_id: str | None = None
    market_profile_id: str | None = None
    description: str | None = None
    lifecycle_stage: str = "experiment"
    status: str = "draft"
    metadata: dict | None = None


class SimulateDayRequest(BaseModel):
    decision_date: str
    alpha_frame: list[dict] | None = None
    legacy_signal_frame: list[dict] | None = None
    current_weights: dict[str, float] | None = None
    portfolio_value: float = 1_000_000
    lifecycle_stage: str = "experiment"


def _svc() -> StrategyGraph3Service:
    return StrategyGraph3Service()


@router.post("/strategy-graphs/builtin-alpha")
async def create_builtin_alpha_graph(body: BuiltinAlphaGraphRequest) -> dict:
    try:
        return _svc().create_builtin_alpha_graph(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/strategy-graphs/legacy-adapter")
async def create_legacy_adapter_graph(body: LegacyAdapterGraphRequest) -> dict:
    try:
        return _svc().create_legacy_strategy_adapter_graph(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/strategy-graphs")
async def list_strategy_graphs(
    project_id: str | None = Query(None),
    status: str | None = Query(None),
) -> list[dict]:
    return _svc().list_graphs(project_id=project_id, status=status)


@router.get("/strategy-graphs/{strategy_graph_id}")
async def get_strategy_graph(strategy_graph_id: str) -> dict:
    try:
        return _svc().get_graph(strategy_graph_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/strategy-graphs/{strategy_graph_id}/simulate-day")
async def simulate_strategy_graph_day(strategy_graph_id: str, body: SimulateDayRequest) -> dict:
    try:
        return _svc().simulate_day(strategy_graph_id, **body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/strategy-signals/{strategy_signal_id}/explain")
async def explain_strategy_signal(strategy_signal_id: str) -> dict:
    try:
        return _svc().explain_day(strategy_signal_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
