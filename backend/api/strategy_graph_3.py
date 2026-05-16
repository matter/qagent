"""QAgent 3.0 StrategyGraph runtime API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.services.strategy_graph_3_service import StrategyGraph3Service
from backend.tasks.executor import get_task_executor
from backend.tasks.models import TaskSource

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


class SimulateDayRequest(BaseModel):
    decision_date: str
    alpha_frame: list[dict] | None = None
    current_weights: dict[str, float] | None = None
    portfolio_value: float = 1_000_000
    lifecycle_stage: str = "experiment"


class BacktestGraphRequest(BaseModel):
    start_date: str
    end_date: str
    alpha_frames_by_date: dict[str, list[dict]] | None = None
    initial_capital: float = 1_000_000
    lifecycle_stage: str = "experiment"
    price_field: str = "close"


def _svc() -> StrategyGraph3Service:
    return StrategyGraph3Service()


def _executor():
    return get_task_executor()


@router.post("/strategy-graphs/builtin-alpha")
async def create_builtin_alpha_graph(body: BuiltinAlphaGraphRequest) -> dict:
    try:
        return _svc().create_builtin_alpha_graph(**body.model_dump())
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


@router.post("/strategy-graphs/{strategy_graph_id}/backtest")
async def backtest_strategy_graph(strategy_graph_id: str, body: BacktestGraphRequest) -> dict:
    try:
        task_id = _executor().submit(
            task_type="strategy_graph_backtest",
            fn=_svc().backtest_graph,
            params={"strategy_graph_id": strategy_graph_id, **body.model_dump()},
            timeout=3600,
            source=TaskSource.UI,
        )
        return {
            "task_id": task_id,
            "status": "queued",
            "task_type": "strategy_graph_backtest",
            "strategy_graph_id": strategy_graph_id,
            "poll_url": f"/api/tasks/{task_id}",
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/strategy-graphs/{strategy_graph_id}/backtests")
async def list_strategy_graph_backtests(
    strategy_graph_id: str,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    return _svc().list_backtest_runs(strategy_graph_id=strategy_graph_id, limit=limit)


@router.get("/backtests/{backtest_run_id}")
async def get_strategy_graph_backtest(backtest_run_id: str) -> dict:
    try:
        return _svc().get_backtest_run(backtest_run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/strategy-signals/{strategy_signal_id}/explain")
async def explain_strategy_signal(strategy_signal_id: str) -> dict:
    try:
        return _svc().explain_day(strategy_signal_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
