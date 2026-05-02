"""Strategy + Backtest API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.backtest_service import BacktestService
from backend.services.market_context import get_default_benchmark, normalize_market
from backend.services.strategy_service import StrategyService
from backend.strategies.builtins import get_template_names, get_template_source
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["strategies"])

_strategy_service: StrategyService | None = None
_backtest_service: BacktestService | None = None


def _get_strategy_service() -> StrategyService:
    global _strategy_service
    if _strategy_service is None:
        _strategy_service = StrategyService()
    return _strategy_service


def _get_backtest_service() -> BacktestService:
    global _backtest_service
    if _backtest_service is None:
        _backtest_service = BacktestService()
    return _backtest_service


def _get_executor() -> TaskExecutor:
    return get_task_executor()


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class CreateStrategyRequest(BaseModel):
    market: Optional[str] = None
    name: str
    source_code: str
    description: Optional[str] = None
    position_sizing: str = "equal_weight"


class UpdateStrategyRequest(BaseModel):
    market: Optional[str] = None
    source_code: Optional[str] = None
    description: Optional[str] = None
    position_sizing: Optional[str] = None
    status: Optional[str] = None


class RunBacktestRequest(BaseModel):
    market: Optional[str] = None
    config: dict = {}
    universe_group_id: str


class CompareBacktestsRequest(BaseModel):
    market: Optional[str] = None
    backtest_ids: list[str]


# ------------------------------------------------------------------
# Strategy CRUD
# ------------------------------------------------------------------


@router.post("/strategies")
async def create_strategy(body: CreateStrategyRequest) -> dict:
    """Create a new strategy definition."""
    svc = _get_strategy_service()
    try:
        return svc.create_strategy(
            name=body.name,
            source_code=body.source_code,
            description=body.description,
            position_sizing=body.position_sizing,
            market=body.market,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        resolved_market = body.market
        try:
            resolved_market = normalize_market(body.market)
        except ValueError:
            pass
        log.error(
            "api.strategy.create_failed",
            market=resolved_market,
            name=body.name,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create strategy: {e}",
        )


@router.get("/strategies")
async def list_strategies(market: Optional[str] = Query(None)) -> list[dict]:
    """List all strategies."""
    svc = _get_strategy_service()
    try:
        return svc.list_strategies(market=market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/strategies/templates")
async def list_templates() -> list[dict]:
    """List available built-in strategy templates."""
    names = get_template_names()
    return [{"name": n} for n in names]


@router.get("/strategies/templates/{template_name}")
async def get_template(template_name: str) -> dict:
    """Get source code for a built-in strategy template."""
    source = get_template_source(template_name)
    if source is None:
        raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found")
    return {"name": template_name, "source_code": source}


# NOTE: backtest list/detail endpoints must be defined BEFORE the
# {strategy_id} catch-all to avoid routing conflicts.


@router.get("/strategies/backtests")
async def list_all_backtests(
    strategy_id: Optional[str] = None,
    market: Optional[str] = Query(None),
) -> list[dict]:
    """List all backtest results, optionally filtered by strategy_id."""
    svc = _get_backtest_service()
    try:
        return svc.list_backtests(strategy_id=strategy_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/strategies/backtests/{backtest_id}")
async def get_backtest(
    backtest_id: str,
    market: Optional[str] = Query(None),
) -> dict:
    """Get full backtest result."""
    svc = _get_backtest_service()
    try:
        return svc.get_backtest(backtest_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/strategies/backtests/{backtest_id}/rebalance-diagnostics")
async def get_backtest_rebalance_diagnostics(
    backtest_id: str,
    market: Optional[str] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
) -> dict:
    """Get paginated per-rebalance diagnostics for a backtest."""
    svc = _get_backtest_service()
    try:
        return svc.get_rebalance_diagnostics(
            backtest_id,
            market=market,
            offset=offset,
            limit=limit,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/strategies/backtests/{backtest_id}")
async def delete_backtest(
    backtest_id: str,
    market: Optional[str] = Query(None),
) -> dict:
    """Delete a backtest result."""
    svc = _get_backtest_service()
    try:
        resolved_market = normalize_market(market)
        svc.delete_backtest(backtest_id, market=resolved_market)
        return {"status": "deleted", "id": backtest_id, "market": resolved_market}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/strategies/backtests/{backtest_id}/stock/{ticker}")
async def get_backtest_stock_chart(
    backtest_id: str,
    ticker: str,
    market: Optional[str] = Query(None),
) -> dict:
    """Get daily bars and trade markers for a single stock within a backtest."""
    svc = _get_backtest_service()
    try:
        return svc.get_stock_chart_data(backtest_id, ticker, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/strategies/backtests/compare")
async def compare_backtests(body: CompareBacktestsRequest) -> dict:
    """Compare multiple backtest results."""
    svc = _get_backtest_service()
    try:
        return svc.compare_strategies(body.backtest_ids, market=body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/strategies/{strategy_id}")
async def get_strategy(
    strategy_id: str,
    market: Optional[str] = Query(None),
) -> dict:
    """Get strategy definition detail including source code."""
    svc = _get_strategy_service()
    try:
        return svc.get_strategy(strategy_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/strategies/{strategy_id}")
async def update_strategy(strategy_id: str, body: UpdateStrategyRequest) -> dict:
    """Update a strategy -- creates new version if source_code changes."""
    svc = _get_strategy_service()
    try:
        return svc.update_strategy(
            strategy_id=strategy_id,
            source_code=body.source_code,
            description=body.description,
            position_sizing=body.position_sizing,
            status=body.status,
            market=body.market,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/strategies/{strategy_id}")
async def delete_strategy(
    strategy_id: str,
    market: Optional[str] = Query(None),
) -> dict:
    """Delete a strategy definition."""
    svc = _get_strategy_service()
    try:
        resolved_market = normalize_market(market)
        svc.delete_strategy(strategy_id, market=resolved_market)
        return {"status": "deleted", "id": strategy_id, "market": resolved_market}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------------------------------------------------
# Backtest execution (async via TaskExecutor)
# ------------------------------------------------------------------


@router.post("/strategies/{strategy_id}/backtest")
async def run_backtest(strategy_id: str, body: RunBacktestRequest) -> dict:
    """Trigger async backtest for a strategy.

    Returns a task_id to poll for progress.
    """
    try:
        resolved_market = normalize_market(body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Validate strategy exists
    svc = _get_strategy_service()
    try:
        svc.get_strategy(strategy_id, market=resolved_market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    bt_svc = _get_backtest_service()
    try:
        benchmark = body.config.get("benchmark") or get_default_benchmark(resolved_market)
        BacktestService._validate_benchmark_market(benchmark, resolved_market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    executor = _get_executor()

    def _do_backtest(
        strategy_id: str,
        config: dict,
        universe_group_id: str,
        market: str,
    ) -> dict:
        full = bt_svc.run_backtest(
            strategy_id=strategy_id,
            config_dict=config,
            universe_group_id=universe_group_id,
            market=market,
        )
        # Return compact summary for task store; full result is persisted in
        # backtest_results table and retrievable via GET /backtests/{id}.
        summary = {
            "backtest_id": full.get("backtest_id"),
            "id": full.get("backtest_id"),
            "market": full.get("market"),
            "strategy_id": full.get("strategy_id"),
            "strategy_name": full.get("strategy_name"),
            "result_level": full.get("result_level"),
            "universe_group_id": full.get("universe_group_id"),
        }
        for key in (
            "total_return", "annual_return", "sharpe_ratio", "max_drawdown",
            "win_rate", "total_trades", "start_date", "end_date",
            "leakage_warnings", "requested_start_date", "requested_end_date",
            "effective_start_date", "effective_end_date", "date_adjustment",
        ):
            if key in full:
                summary[key] = full[key]
        config = full.get("config")
        if isinstance(config, dict):
            for key in (
                "requested_start_date", "requested_end_date",
                "effective_start_date", "effective_end_date", "date_adjustment",
            ):
                if key in config and key not in summary:
                    summary[key] = config[key]
        return summary

    task_id = executor.submit(
        task_type="strategy_backtest",
        fn=_do_backtest,
        params={
            "strategy_id": strategy_id,
            "market": resolved_market,
            "config": body.config,
            "universe_group_id": body.universe_group_id,
        },
        timeout=3600,  # 1 hour max
        source=TaskSource.UI,
    )

    log.info(
        "api.strategy.backtest_triggered",
        task_id=task_id,
        strategy_id=strategy_id,
        market=resolved_market,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "strategy_id": strategy_id,
        "market": resolved_market,
    }
