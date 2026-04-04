"""MCP Server for QAgent -- exposes system capabilities as MCP tools.

Uses the same service layer as the REST API to ensure consistent behavior.
Long-running operations return task_id for async polling.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from backend.logger import get_logger

log = get_logger(__name__)

mcp = FastMCP("qagent", stateless_http=True)


# ======================================================================
# Lazy service accessors (avoid import-time DB initialization)
# ======================================================================

def _data_service():
    from backend.services.data_service import DataService
    return DataService()


def _factor_service():
    from backend.services.factor_service import FactorService
    return FactorService()


def _model_service():
    from backend.services.model_service import ModelService
    return ModelService()


def _strategy_service():
    from backend.services.strategy_service import StrategyService
    return StrategyService()


def _backtest_service():
    from backend.services.backtest_service import BacktestService
    return BacktestService()


def _signal_service():
    from backend.services.signal_service import SignalService
    return SignalService()


def _group_service():
    from backend.services.group_service import GroupService
    return GroupService()


def _task_executor():
    from backend.tasks.executor import TaskExecutor
    return TaskExecutor()


# ======================================================================
# Data tools
# ======================================================================


@mcp.tool()
def get_stock_data(ticker: str, start_date: str, end_date: str) -> list[dict]:
    """Retrieve OHLCV daily bar data for a stock ticker.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL").
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns:
        List of daily bar records with date, open, high, low, close, volume.
    """
    from backend.db import get_connection
    conn = get_connection()
    rows = conn.execute(
        """SELECT date, open, high, low, close, volume
           FROM daily_bars
           WHERE ticker = ? AND date BETWEEN ? AND ?
           ORDER BY date""",
        [ticker.upper(), start_date, end_date],
    ).fetchall()

    return [
        {
            "date": str(r[0]),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
        }
        for r in rows
    ]


@mcp.tool()
def search_stocks(query: str, limit: int = 20) -> list[dict]:
    """Search for stocks by ticker symbol or company name.

    Args:
        query: Search term to match against ticker or name.
        limit: Maximum number of results to return (default 20).

    Returns:
        List of matching stock records with ticker, name, exchange, sector.
    """
    from backend.db import get_connection
    conn = get_connection()
    query_upper = query.upper()
    query_like = f"%{query}%"

    rows = conn.execute(
        """SELECT ticker, name, exchange, sector, status
           FROM stocks
           WHERE ticker LIKE ? OR UPPER(name) LIKE UPPER(?)
           ORDER BY
               CASE WHEN ticker = ? THEN 0
                    WHEN ticker LIKE ? THEN 1
                    ELSE 2 END,
               ticker
           LIMIT ?""",
        [f"{query_upper}%", query_like, query_upper, f"{query_upper}%", limit],
    ).fetchall()

    return [
        {
            "ticker": r[0],
            "name": r[1],
            "exchange": r[2],
            "sector": r[3],
            "status": r[4],
        }
        for r in rows
    ]


@mcp.tool()
def get_data_status() -> dict:
    """Get current data freshness and coverage information.

    Returns:
        Dict with stock_count, date_range, total_bars, stale_tickers,
        latest_trading_day, and last_update info.
    """
    svc = _data_service()
    return svc.get_data_status()


@mcp.tool()
def update_data(mode: str = "incremental") -> dict:
    """Trigger a data update task to fetch latest market data.

    Args:
        mode: Update mode - "incremental" (only new bars) or "full" (re-fetch all).

    Returns:
        Dict with task_id and status for tracking the background task.
    """
    from backend.tasks.models import TaskSource
    svc = _data_service()
    executor = _task_executor()

    task_id = executor.submit(
        task_type="data_update",
        fn=svc.update_data,
        params={"mode": mode},
        timeout=7200,
        source=TaskSource.AGENT,
    )

    return {"task_id": task_id, "status": "queued", "mode": mode}


# ======================================================================
# Factor tools
# ======================================================================


@mcp.tool()
def list_factors(category: str | None = None) -> list[dict]:
    """List all available factors in the factor library.

    Args:
        category: Optional category filter (e.g. "momentum", "value", "custom").

    Returns:
        List of factor definitions with id, name, version, category, status.
    """
    svc = _factor_service()
    return svc.list_factors(category=category)


@mcp.tool()
def evaluate_factor(
    factor_id: str,
    label_id: str,
    universe_group_id: str,
    start_date: str,
    end_date: str,
) -> dict:
    """Trigger factor evaluation against a label definition.

    Computes IC, IR, and group return metrics for the factor.

    Args:
        factor_id: ID of the factor to evaluate.
        label_id: ID of the label definition to evaluate against.
        universe_group_id: ID of the stock group for the universe.
        start_date: Evaluation start date (YYYY-MM-DD).
        end_date: Evaluation end date (YYYY-MM-DD).

    Returns:
        Dict with task_id for tracking the background evaluation task.
    """
    from backend.services.factor_eval_service import FactorEvalService
    from backend.tasks.models import TaskSource

    eval_svc = FactorEvalService()
    executor = _task_executor()

    def _do_evaluate(
        factor_id: str,
        label_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        return eval_svc.evaluate_factor(
            factor_id=factor_id,
            label_id=label_id,
            universe_group_id=universe_group_id,
            start_date=start_date,
            end_date=end_date,
        )

    task_id = executor.submit(
        task_type="factor_evaluate",
        fn=_do_evaluate,
        params={
            "factor_id": factor_id,
            "label_id": label_id,
            "universe_group_id": universe_group_id,
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=3600,
        source=TaskSource.AGENT,
    )

    return {"task_id": task_id, "status": "queued", "factor_id": factor_id}


@mcp.tool()
def create_factor(
    name: str,
    description: str,
    category: str,
    source_code: str,
) -> dict:
    """Create a new factor definition in the factor library.

    The source_code must define a class that inherits from FactorBase
    and implements the compute(ohlcv) method.

    Args:
        name: Unique factor name.
        description: Human-readable description of the factor logic.
        category: Factor category (e.g. "momentum", "value", "volatility", "custom").
        source_code: Python source code implementing the factor.

    Returns:
        The created factor record with id, name, version, status.
    """
    svc = _factor_service()
    return svc.create_factor(
        name=name,
        source_code=source_code,
        description=description,
        category=category,
    )


# ======================================================================
# Model tools
# ======================================================================


@mcp.tool()
def list_models() -> list[dict]:
    """List all trained ML models.

    Returns:
        List of model records with id, name, model_type, eval_metrics, status.
    """
    svc = _model_service()
    return svc.list_models()


@mcp.tool()
def train_model(
    name: str,
    feature_set_id: str,
    label_id: str,
    model_type: str,
    model_params: dict | None,
    train_config: dict | None,
    universe_group_id: str,
) -> dict:
    """Trigger model training as a background task.

    Args:
        name: Human-readable model name.
        feature_set_id: ID of the feature set to use.
        label_id: ID of the label definition (prediction target).
        model_type: Model algorithm type (e.g. "lightgbm").
        model_params: Optional model hyperparameters dict.
        train_config: Training configuration with date splits
            (must include train_start, train_end, valid_start, valid_end,
            test_start, test_end).
        universe_group_id: ID of the stock group for the training universe.

    Returns:
        Dict with task_id for tracking the background training task.
    """
    from backend.tasks.models import TaskSource
    svc = _model_service()
    executor = _task_executor()

    def _do_train(
        name: str,
        feature_set_id: str,
        label_id: str,
        model_type: str,
        model_params: dict | None,
        train_config: dict | None,
        universe_group_id: str,
    ) -> dict:
        return svc.train_model(
            name=name,
            feature_set_id=feature_set_id,
            label_id=label_id,
            model_type=model_type,
            model_params=model_params,
            train_config=train_config,
            universe_group_id=universe_group_id,
        )

    task_id = executor.submit(
        task_type="model_train",
        fn=_do_train,
        params={
            "name": name,
            "feature_set_id": feature_set_id,
            "label_id": label_id,
            "model_type": model_type,
            "model_params": model_params,
            "train_config": train_config,
            "universe_group_id": universe_group_id,
        },
        timeout=7200,
        source=TaskSource.AGENT,
    )

    return {"task_id": task_id, "status": "queued", "name": name}


# ======================================================================
# Strategy tools
# ======================================================================


@mcp.tool()
def list_strategies() -> list[dict]:
    """List all registered trading strategies.

    Returns:
        List of strategy records with id, name, version, required_factors,
        required_models, position_sizing, status.
    """
    svc = _strategy_service()
    return svc.list_strategies()


@mcp.tool()
def run_backtest(
    strategy_id: str,
    config_json: str,
    universe_group_id: str,
) -> dict:
    """Trigger a backtest for a strategy as a background task.

    Args:
        strategy_id: ID of the strategy to backtest.
        config_json: JSON string with backtest configuration. Keys:
            initial_capital, start_date, end_date, benchmark,
            commission_rate, slippage_rate, max_positions, rebalance_freq.
        universe_group_id: ID of the stock group for the backtest universe.

    Returns:
        Dict with task_id for tracking the background backtest task.
    """
    import json as _json
    from backend.tasks.models import TaskSource

    try:
        config = _json.loads(config_json) if isinstance(config_json, str) else config_json
    except _json.JSONDecodeError as e:
        return {"error": f"Invalid config_json: {e}"}

    bt_svc = _backtest_service()
    executor = _task_executor()

    def _do_backtest(
        strategy_id: str,
        config: dict,
        universe_group_id: str,
    ) -> dict:
        return bt_svc.run_backtest(
            strategy_id=strategy_id,
            config_dict=config,
            universe_group_id=universe_group_id,
        )

    task_id = executor.submit(
        task_type="strategy_backtest",
        fn=_do_backtest,
        params={
            "strategy_id": strategy_id,
            "config": config,
            "universe_group_id": universe_group_id,
        },
        timeout=3600,
        source=TaskSource.AGENT,
    )

    return {"task_id": task_id, "status": "queued", "strategy_id": strategy_id}


@mcp.tool()
def generate_signals(
    strategy_id: str,
    target_date: str,
    universe_group_id: str,
) -> dict:
    """Trigger signal generation for a strategy as a background task.

    Runs the full pipeline: dependency validation -> factors -> models -> signals.
    The result includes dependency chain validation and result_level classification.

    Args:
        strategy_id: ID of the strategy to generate signals for.
        target_date: Target date for signal generation (YYYY-MM-DD).
        universe_group_id: ID of the stock group for the signal universe.

    Returns:
        Dict with task_id for tracking the background signal generation task.
    """
    from backend.tasks.models import TaskSource

    sig_svc = _signal_service()
    executor = _task_executor()

    def _do_generate(
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
    ) -> dict:
        return sig_svc.generate_signals(
            strategy_id=strategy_id,
            target_date=target_date,
            universe_group_id=universe_group_id,
        )

    task_id = executor.submit(
        task_type="signal_generate",
        fn=_do_generate,
        params={
            "strategy_id": strategy_id,
            "target_date": target_date,
            "universe_group_id": universe_group_id,
        },
        timeout=3600,
        source=TaskSource.AGENT,
    )

    return {
        "task_id": task_id,
        "status": "queued",
        "strategy_id": strategy_id,
        "target_date": target_date,
    }


# ======================================================================
# Task tools
# ======================================================================


@mcp.tool()
def get_task_status(task_id: str) -> dict:
    """Get the status of a background task.

    Args:
        task_id: The task ID returned when the task was submitted.

    Returns:
        Dict with task_id, task_type, status, params, result, error,
        and timestamps (created_at, started_at, completed_at).
    """
    executor = _task_executor()
    record = executor.get_task(task_id)
    if record is None:
        return {"error": f"Task {task_id} not found"}

    return {
        "task_id": record.id,
        "task_type": record.task_type,
        "status": record.status.value,
        "params": record.params,
        "result": record.result_summary,
        "error": record.error_message,
        "created_at": str(record.created_at) if record.created_at else None,
        "started_at": str(record.started_at) if record.started_at else None,
        "completed_at": str(record.completed_at) if record.completed_at else None,
    }


@mcp.tool()
def cancel_task(task_id: str) -> dict:
    """Cancel a queued or running background task.

    Args:
        task_id: The task ID to cancel.

    Returns:
        Dict with task_id and cancellation status.
    """
    executor = _task_executor()
    ok = executor.cancel(task_id)
    if not ok:
        return {"task_id": task_id, "status": "not_cancelled", "reason": "Task not found or not cancellable"}
    return {"task_id": task_id, "status": "cancelled"}


# ======================================================================
# Group tools
# ======================================================================


@mcp.tool()
def list_groups() -> list[dict]:
    """List all stock groups with member counts.

    Returns:
        List of stock group records with id, name, description,
        group_type, member_count.
    """
    svc = _group_service()
    return svc.list_groups()


@mcp.tool()
def create_group(
    name: str,
    description: str,
    group_type: str,
    tickers: list[str] | None = None,
    filter_expr: str | None = None,
) -> dict:
    """Create a new stock group.

    Args:
        name: Unique group name.
        description: Human-readable description.
        group_type: Type of group - "manual" (explicit ticker list)
            or "filter" (SQL filter expression against stocks table).
        tickers: List of ticker symbols (for manual groups).
        filter_expr: SQL WHERE clause filter (for filter groups,
            e.g. "sector = 'Technology' AND status = 'active'").

    Returns:
        The created group record with id, name, member_count.
    """
    svc = _group_service()
    return svc.create_group(
        name=name,
        description=description,
        group_type=group_type,
        tickers=tickers,
        filter_expr=filter_expr,
    )
