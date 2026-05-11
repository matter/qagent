"""MCP Server for QAgent -- exposes system capabilities as MCP tools.

Uses the same service layer as the REST API to ensure consistent behavior.
Long-running operations return task_id for async polling.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import normalize_market, normalize_ticker

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


def _label_service():
    from backend.services.label_service import LabelService
    return LabelService()


def _feature_service():
    from backend.services.feature_service import FeatureService
    return FeatureService()


def _paper_service():
    from backend.services.paper_trading_service import PaperTradingService
    return PaperTradingService()


def _market_data_foundation_service():
    from backend.services.market_data_foundation_service import MarketDataFoundationService
    return MarketDataFoundationService()


def _data_quality_service():
    from backend.services.data_quality_service import DataQualityService
    return DataQualityService()


def _macro_data_service():
    from backend.services.macro_data_service import MacroDataService
    return MacroDataService()


def _migration_service():
    from backend.services.migration_service import MigrationService
    return MigrationService()


def _universe_service():
    from backend.services.universe_service import UniverseService
    return UniverseService()


def _dataset_service_3_0():
    from backend.services.dataset_service import DatasetService
    return DatasetService()


def _factor_engine_3_service():
    from backend.services.factor_engine_3_service import FactorEngine3Service
    return FactorEngine3Service()


def _model_experiment_3_service():
    from backend.services.model_experiment_3_service import ModelExperiment3Service
    return ModelExperiment3Service()


def _portfolio_assets_3_service():
    from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
    return PortfolioAssets3Service()


def _strategy_graph_3_service():
    from backend.services.strategy_graph_3_service import StrategyGraph3Service
    return StrategyGraph3Service()


def _agent_research_3_service():
    from backend.services.agent_research_3_service import AgentResearch3Service
    return AgentResearch3Service()


def _production_signal_3_service():
    from backend.services.production_signal_3_service import ProductionSignal3Service
    return ProductionSignal3Service()


def _task_executor():
    from backend.tasks.executor import get_task_executor
    return get_task_executor()


def _research_kernel_service():
    from backend.services.research_kernel_service import ResearchKernelService
    return ResearchKernelService()


def _task_response(
    *,
    task_id: str,
    task_type: str,
    market: str,
    **extra,
) -> dict:
    return {
        "task_id": task_id,
        "status": "queued",
        "task_type": task_type,
        "market": market,
        "asset_scope": {"market": market},
        "poll_url": f"/api/tasks/{task_id}",
        **extra,
    }


def _resolve_market(market: str | None) -> str:
    try:
        return normalize_market(market)
    except ValueError as exc:
        raise ValueError(
            f"Invalid MCP request: market must be one of US, CN. {exc}"
        ) from exc


# ======================================================================
# Research kernel tools
# ======================================================================


@mcp.tool()
def get_bootstrap_project_3_0() -> dict:
    """Return the bootstrap 3.0 project."""
    return _research_kernel_service().get_bootstrap_project()


@mcp.tool()
def list_research_runs_3_0(
    project_id: str | None = None,
    run_type: str | None = None,
    status: str | None = None,
    lifecycle_stage: str | None = None,
    created_by: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List recent 3.0 research runs."""
    return _research_kernel_service().list_runs(
        project_id=project_id,
        run_type=run_type,
        status=status,
        lifecycle_stage=lifecycle_stage,
        created_by=created_by,
        limit=limit,
    )


@mcp.tool()
def get_research_run_3_0(run_id: str) -> dict:
    """Get a single 3.0 research run."""
    return _research_kernel_service().get_run(run_id)


@mcp.tool()
def list_research_artifacts_3_0(
    project_id: str | None = None,
    run_id: str | None = None,
    artifact_type: str | None = None,
    lifecycle_stage: str | None = None,
    retention_class: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List recent 3.0 artifacts."""
    return _research_kernel_service().list_artifacts(
        project_id=project_id,
        run_id=run_id,
        artifact_type=artifact_type,
        lifecycle_stage=lifecycle_stage,
        retention_class=retention_class,
        limit=limit,
    )


@mcp.tool()
def get_research_artifact_3_0(artifact_id: str) -> dict:
    """Get a single 3.0 artifact."""
    return _research_kernel_service().get_artifact(artifact_id)


@mcp.tool()
def get_research_lineage_3_0(run_id: str) -> dict:
    """Get lineage edges for a 3.0 run."""
    return _research_kernel_service().get_lineage(run_id)


@mcp.tool()
def preview_artifact_cleanup_3_0(
    project_id: str | None = None,
    run_id: str | None = None,
    artifact_ids: list[str] | None = None,
    lifecycle_stage: str | None = None,
    retention_class: str | None = None,
    artifact_type: str | None = None,
    include_published: bool = False,
    limit: int = 500,
) -> dict:
    """Preview cleanup impact without deleting artifacts."""
    return _research_kernel_service().preview_artifact_cleanup(
        project_id=project_id,
        run_id=run_id,
        artifact_ids=artifact_ids,
        lifecycle_stage=lifecycle_stage,
        retention_class=retention_class,
        artifact_type=artifact_type,
        include_published=include_published,
        limit=limit,
    )


@mcp.tool()
def archive_research_artifact_3_0(
    artifact_id: str,
    retention_class: str = "archived",
    archive_reason: str | None = None,
) -> dict:
    """Archive a 3.0 artifact without deleting lineage or summary metadata."""
    return _research_kernel_service().archive_artifact(
        artifact_id,
        retention_class=retention_class,
        archive_reason=archive_reason,
    )


@mcp.tool()
def list_promotion_records_3_0(
    project_id: str | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    target_type: str | None = None,
    target_id: str | None = None,
    decision: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List 3.0 promotion decisions for workbench and agent review."""
    return _research_kernel_service().list_promotion_records(
        project_id=project_id,
        source_type=source_type,
        source_id=source_id,
        target_type=target_type,
        target_id=target_id,
        decision=decision,
        limit=limit,
    )


# ======================================================================
# Data tools
# ======================================================================


@mcp.tool()
def get_stock_data(
    ticker: str,
    start_date: str,
    end_date: str,
    market: str | None = None,
) -> list[dict]:
    """Retrieve OHLCV daily bar data for a stock ticker.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL").
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of daily bar records with date, open, high, low, close, volume.
    """
    resolved_market = _resolve_market(market)
    normalized_ticker = normalize_ticker(ticker, resolved_market)
    conn = get_connection()
    rows = conn.execute(
        """SELECT date, open, high, low, close, volume
           FROM daily_bars
           WHERE market = ? AND ticker = ? AND date BETWEEN ? AND ?
           ORDER BY date""",
        [resolved_market, normalized_ticker, start_date, end_date],
    ).fetchall()

    return [
        {
            "market": resolved_market,
            "ticker": normalized_ticker,
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
def search_stocks(query: str, limit: int = 20, market: str | None = None) -> list[dict]:
    """Search for stocks by ticker symbol or company name.

    Args:
        query: Search term to match against ticker or name.
        limit: Maximum number of results to return (default 20).
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of matching stock records with ticker, name, exchange, sector.
    """
    resolved_market = _resolve_market(market)
    conn = get_connection()
    query_upper = query.upper()
    query_like = f"%{query}%"
    ticker_like = f"%{query_upper}%"

    rows = conn.execute(
        """SELECT ticker, name, exchange, sector, status
           FROM stocks
           WHERE market = ?
             AND (UPPER(ticker) LIKE ? OR UPPER(name) LIKE UPPER(?))
           ORDER BY
               CASE WHEN ticker = ? THEN 0
                    WHEN UPPER(ticker) LIKE ? THEN 1
                    ELSE 2 END,
               ticker
           LIMIT ?""",
        [resolved_market, ticker_like, query_like, query_upper, ticker_like, limit],
    ).fetchall()

    return [
        {
            "market": resolved_market,
            "ticker": r[0],
            "name": r[1],
            "exchange": r[2],
            "sector": r[3],
            "status": r[4],
        }
        for r in rows
    ]


@mcp.tool()
def get_data_status(market: str | None = None) -> dict:
    """Get current data freshness and coverage information.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Dict with stock_count, date_range, total_bars, stale_tickers,
        latest_trading_day, and last_update info.
    """
    resolved_market = _resolve_market(market)
    svc = _data_service()
    result = svc.get_data_status(market=resolved_market)
    result.setdefault("market", resolved_market)
    return result


@mcp.tool()
def list_market_profiles() -> list[dict]:
    """List 3.0 market profiles such as US_EQ and CN_A."""
    return _market_data_foundation_service().list_market_profiles()


@mcp.tool()
def get_market_profile(profile_id: str) -> dict:
    """Get a 3.0 market profile with data, trading, cost, and benchmark policies."""
    return _market_data_foundation_service().get_market_profile(profile_id)


@mcp.tool()
def get_project_data_status(project_id: str = "bootstrap_us") -> dict:
    """Get project-scoped 3.0 data status and market semantics."""
    return _market_data_foundation_service().get_project_data_status(project_id)


@mcp.tool()
def list_provider_capabilities_3_0(
    provider: str | None = None,
    market_profile_id: str | None = None,
    dataset: str | None = None,
) -> list[dict]:
    """List declared provider/data quality capabilities for free data sources."""
    return _data_quality_service().list_provider_capabilities(
        provider=provider,
        market_profile_id=market_profile_id,
        dataset=dataset,
    )


@mcp.tool()
def get_data_quality_contract_3_0(market_profile_id: str | None = None) -> dict:
    """Return data source quality policy and capability summary."""
    return _data_quality_service().get_data_quality_contract(
        market_profile_id=market_profile_id,
    )


@mcp.tool()
def search_assets_3_0(
    query: str,
    project_id: str = "bootstrap_us",
    limit: int = 20,
) -> list[dict]:
    """Search project assets by stable 3.0 asset_id."""
    return _market_data_foundation_service().search_assets(
        project_id=project_id,
        query=query,
        limit=limit,
    )


@mcp.tool()
def query_bars_3_0(
    asset_ids: list[str],
    start_date: str,
    end_date: str,
    project_id: str = "bootstrap_us",
    limit: int = 10000,
) -> dict:
    """Query daily bars by 3.0 asset_id for a project."""
    return _market_data_foundation_service().query_bars(
        project_id=project_id,
        asset_ids=asset_ids,
        start=start_date,
        end=end_date,
        limit=limit,
    )


@mcp.tool()
def update_fred_series(
    series_ids: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Trigger a FRED macro series update task."""
    from backend.tasks.models import TaskSource

    svc = _macro_data_service()
    executor = _task_executor()
    task_id = executor.submit(
        task_type="macro_data_update",
        fn=svc.update_fred_series,
        params={
            "series_ids": series_ids,
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=1800,
        source=TaskSource.AGENT,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "task_type": "macro_data_update",
        "provider": "fred",
        "series_ids": series_ids,
        "poll_url": f"/api/tasks/{task_id}",
    }


@mcp.tool()
def query_macro_series(
    series_ids: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    as_of: str | None = None,
    limit: int = 10000,
) -> dict:
    """Query persisted macro observations from FRED."""
    rows = _macro_data_service().query_series(
        series_ids=series_ids,
        start_date=start_date,
        end_date=end_date,
        as_of=as_of,
        limit=limit,
    )
    return {
        "provider": "fred",
        "series_ids": series_ids,
        "observations": rows,
    }


@mcp.tool()
def build_migration_report(db_path: str | None = None) -> dict:
    """Build a side-by-side 2.0 -> 3.0 migration report."""
    from pathlib import Path

    path = Path(db_path) if db_path else None
    return _migration_service().build_report(path)


@mcp.tool()
def apply_migration(db_path: str | None = None) -> dict:
    """Materialize a 3.0 migration run and artifact."""
    from pathlib import Path

    path = Path(db_path) if db_path else None
    return _migration_service().apply_migration(path)


@mcp.tool()
def preview_legacy_factor_3_0(
    factor_id: str,
    universe_group_id: str,
    start_date: str,
    end_date: str,
    market: str | None = None,
    project_id: str | None = None,
) -> dict:
    """Preview a legacy factor as a 3.0 research run."""
    return _migration_service().preview_legacy_factor(
        factor_id=factor_id,
        universe_group_id=universe_group_id,
        start_date=start_date,
        end_date=end_date,
        market=market,
        project_id=project_id,
    )


@mcp.tool()
def materialize_legacy_universe_3_0(
    universe_group_id: str,
    market: str | None = None,
    project_id: str | None = None,
) -> dict:
    """Materialize a legacy universe as a 3.0 artifact."""
    return _migration_service().materialize_legacy_universe(
        universe_group_id=universe_group_id,
        market=market,
        project_id=project_id,
    )


@mcp.tool()
def backtest_legacy_strategy_3_0(
    strategy_id: str,
    universe_group_id: str,
    config: dict,
    market: str | None = None,
    project_id: str | None = None,
) -> dict:
    """Run a legacy strategy backtest and persist a 3.0 trace."""
    return _migration_service().run_legacy_strategy_backtest(
        strategy_id=strategy_id,
        universe_group_id=universe_group_id,
        config=config,
        market=market,
        project_id=project_id,
    )


@mcp.tool()
def create_static_universe_3_0(
    name: str,
    tickers: list[str],
    project_id: str = "bootstrap_us",
    market_profile_id: str = "US_EQ",
    description: str | None = None,
) -> dict:
    """Create a 3.0 static universe asset from ticker symbols."""
    return _universe_service().create_static_universe(
        name=name,
        tickers=tickers,
        project_id=project_id,
        market_profile_id=market_profile_id,
        description=description,
    )


@mcp.tool()
def list_universes_3_0(
    project_id: str | None = None,
    market_profile_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List 3.0 universes."""
    return _universe_service().list_universes(
        project_id=project_id,
        market_profile_id=market_profile_id,
        status=status,
        limit=limit,
    )


@mcp.tool()
def create_universe_from_legacy_group_3_0(
    legacy_group_id: str,
    market: str | None = None,
    project_id: str = "bootstrap_us",
    name: str | None = None,
) -> dict:
    """Create a 3.0 universe asset from a legacy stock group."""
    return _universe_service().create_from_legacy_group(
        legacy_group_id=legacy_group_id,
        market=market,
        project_id=project_id,
        name=name,
    )


@mcp.tool()
def materialize_universe_3_0(
    universe_id: str,
    start_date: str,
    end_date: str,
) -> dict:
    """Materialize a 3.0 universe into date/asset memberships."""
    return _universe_service().materialize_universe(
        universe_id,
        start_date=start_date,
        end_date=end_date,
    )


@mcp.tool()
def profile_universe_3_0(universe_id: str) -> dict:
    """Profile a materialized 3.0 universe."""
    return _universe_service().profile_universe(universe_id)


@mcp.tool()
def create_dataset_3_0(
    name: str,
    universe_id: str,
    feature_set_id: str,
    label_id: str,
    start_date: str,
    end_date: str,
    split_policy: dict,
    project_id: str = "bootstrap_us",
) -> dict:
    """Create a 3.0 dataset asset backed by a legacy feature set and label."""
    return _dataset_service_3_0().create_dataset(
        name=name,
        universe_id=universe_id,
        feature_set_id=feature_set_id,
        label_id=label_id,
        start_date=start_date,
        end_date=end_date,
        split_policy=split_policy,
        project_id=project_id,
    )


@mcp.tool()
def list_datasets_3_0(
    project_id: str | None = None,
    market_profile_id: str | None = None,
    universe_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List 3.0 datasets."""
    return _dataset_service_3_0().list_datasets(
        project_id=project_id,
        market_profile_id=market_profile_id,
        universe_id=universe_id,
        status=status,
        limit=limit,
    )


@mcp.tool()
def materialize_dataset_3_0(dataset_id: str) -> dict:
    """Materialize a 3.0 dataset panel and profile artifacts."""
    return _dataset_service_3_0().materialize_dataset(dataset_id)


@mcp.tool()
def profile_dataset_3_0(dataset_id: str) -> dict:
    """Return coverage, missingness, label distribution, and QA for a dataset."""
    return _dataset_service_3_0().profile_dataset(dataset_id)


@mcp.tool()
def sample_dataset_3_0(dataset_id: str, limit: int = 20, offset: int = 0) -> dict:
    """Return a small sample of a materialized dataset panel."""
    return _dataset_service_3_0().sample_dataset(dataset_id, limit=limit, offset=offset)


@mcp.tool()
def query_dataset_3_0(
    dataset_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    asset_ids: list[str] | None = None,
    columns: list[str] | None = None,
    limit: int = 1000,
) -> dict:
    """Query rows from a materialized dataset panel."""
    return _dataset_service_3_0().query_dataset(
        dataset_id,
        start_date=start_date,
        end_date=end_date,
        asset_ids=asset_ids,
        columns=columns,
        limit=limit,
    )


@mcp.tool()
def import_legacy_factor_spec_3_0(
    legacy_factor_id: str,
    market: str | None = None,
    project_id: str = "bootstrap_us",
    name: str | None = None,
) -> dict:
    """Import a legacy 2.0 factor as a 3.0 FactorSpec with code snapshot."""
    return _factor_engine_3_service().create_spec_from_legacy_factor(
        legacy_factor_id=legacy_factor_id,
        market=market,
        project_id=project_id,
        name=name,
    )


@mcp.tool()
def create_factor_spec_3_0(
    name: str,
    source_code: str,
    project_id: str = "bootstrap_us",
    market_profile_id: str = "US_EQ",
    description: str | None = None,
    compute_mode: str = "time_series",
) -> dict:
    """Create a 3.0 Python FactorSpec."""
    return _factor_engine_3_service().create_python_spec(
        name=name,
        source_code=source_code,
        project_id=project_id,
        market_profile_id=market_profile_id,
        description=description,
        compute_mode=compute_mode,
    )


@mcp.tool()
def preview_factor_3_0(
    factor_spec_id: str,
    universe_id: str,
    start_date: str,
    end_date: str,
    limit: int = 5000,
) -> dict:
    """Preview a 3.0 factor in a scratch run without writing official values."""
    return _factor_engine_3_service().preview_factor(
        factor_spec_id=factor_spec_id,
        universe_id=universe_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )


@mcp.tool()
def materialize_factor_3_0(
    factor_spec_id: str,
    universe_id: str,
    start_date: str,
    end_date: str,
) -> dict:
    """Materialize a 3.0 factor run and official asset-id keyed values."""
    return _factor_engine_3_service().materialize_factor(
        factor_spec_id=factor_spec_id,
        universe_id=universe_id,
        start_date=start_date,
        end_date=end_date,
    )


@mcp.tool()
def evaluate_factor_run_3_0(
    factor_run_id: str,
    label_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Evaluate a materialized 3.0 factor run against a label."""
    return _factor_engine_3_service().evaluate_factor_run(
        factor_run_id=factor_run_id,
        label_id=label_id,
        start_date=start_date,
        end_date=end_date,
    )


@mcp.tool()
def sample_factor_run_3_0(factor_run_id: str, limit: int = 20, offset: int = 0) -> dict:
    """Return rows from a materialized or previewed 3.0 factor artifact."""
    return _factor_engine_3_service().sample_factor_run(
        factor_run_id,
        limit=limit,
        offset=offset,
    )


@mcp.tool()
def train_model_experiment_3_0(
    name: str,
    dataset_id: str,
    model_type: str = "lightgbm",
    objective: str = "regression",
    model_params: dict | None = None,
    random_seed: int = 42,
) -> dict:
    """Train a 3.0 model experiment from a materialized Dataset artifact."""
    return _model_experiment_3_service().train_experiment(
        name=name,
        dataset_id=dataset_id,
        model_type=model_type,
        objective=objective,
        model_params=model_params,
        random_seed=random_seed,
    )


@mcp.tool()
def promote_model_experiment_3_0(
    experiment_id: str,
    package_name: str | None = None,
    approved_by: str = "mcp",
    rationale: str | None = None,
) -> dict:
    """Promote a completed model experiment into a candidate ModelPackage."""
    return _model_experiment_3_service().promote_experiment(
        experiment_id,
        package_name=package_name,
        approved_by=approved_by,
        rationale=rationale,
    )


@mcp.tool()
def predict_model_package_panel_3_0(model_package_id: str, dataset_id: str) -> dict:
    """Run a promoted model package on a materialized Dataset panel."""
    return _model_experiment_3_service().predict_panel(
        model_package_id=model_package_id,
        dataset_id=dataset_id,
    )


@mcp.tool()
def create_portfolio_construction_spec_3_0(
    name: str,
    method: str,
    params: dict | None = None,
    project_id: str | None = None,
    market_profile_id: str | None = None,
) -> dict:
    """Create a reusable 3.0 portfolio construction spec."""
    return _portfolio_assets_3_service().create_portfolio_construction_spec(
        name=name,
        method=method,
        params=params,
        project_id=project_id,
        market_profile_id=market_profile_id,
    )


@mcp.tool()
def create_risk_control_spec_3_0(
    name: str,
    rules: list[dict] | None = None,
    params: dict | None = None,
    project_id: str | None = None,
    market_profile_id: str | None = None,
) -> dict:
    """Create reusable 3.0 risk/constraint rules."""
    return _portfolio_assets_3_service().create_risk_control_spec(
        name=name,
        rules=rules,
        params=params,
        project_id=project_id,
        market_profile_id=market_profile_id,
    )


@mcp.tool()
def create_rebalance_policy_spec_3_0(
    name: str,
    policy_type: str = "band",
    params: dict | None = None,
    project_id: str | None = None,
    market_profile_id: str | None = None,
) -> dict:
    """Create a reusable 3.0 rebalance policy."""
    return _portfolio_assets_3_service().create_rebalance_policy_spec(
        name=name,
        policy_type=policy_type,
        params=params,
        project_id=project_id,
        market_profile_id=market_profile_id,
    )


@mcp.tool()
def create_execution_policy_spec_3_0(
    name: str,
    policy_type: str = "next_open",
    params: dict | None = None,
    project_id: str | None = None,
    market_profile_id: str | None = None,
) -> dict:
    """Create a reusable 3.0 execution policy.

    policy_type may be "next_open" or "planned_price". For planned-price
    execution, params supports planned_price_buffer_bps (default 50),
    fallback="decision_close", and order_ttl="same_day".
    """
    return _portfolio_assets_3_service().create_execution_policy_spec(
        name=name,
        policy_type=policy_type,
        params=params,
        project_id=project_id,
        market_profile_id=market_profile_id,
    )


@mcp.tool()
def construct_portfolio_3_0(
    decision_date: str,
    alpha_frame: list[dict],
    portfolio_spec_id: str,
    risk_control_spec_id: str | None = None,
    rebalance_policy_spec_id: str | None = None,
    execution_policy_spec_id: str | None = None,
    current_weights: dict[str, float] | None = None,
    portfolio_value: float = 1_000_000,
) -> dict:
    """Construct portfolio targets, constraint trace, and order intents from alpha.

    When the execution policy is planned_price, alpha_frame rows may include
    planned_price. Missing planned_price falls back to decision-date close at
    execution time.
    """
    return _portfolio_assets_3_service().construct_portfolio(
        decision_date=decision_date,
        alpha_frame=alpha_frame,
        portfolio_spec_id=portfolio_spec_id,
        risk_control_spec_id=risk_control_spec_id,
        rebalance_policy_spec_id=rebalance_policy_spec_id,
        execution_policy_spec_id=execution_policy_spec_id,
        current_weights=current_weights,
        portfolio_value=portfolio_value,
    )


@mcp.tool()
def compare_portfolio_builders_3_0(
    decision_date: str,
    alpha_frame: list[dict],
    portfolio_spec_ids: list[str],
    risk_control_spec_id: str | None = None,
) -> dict:
    """Compare multiple portfolio construction specs on the same alpha frame."""
    return _portfolio_assets_3_service().compare_builders(
        decision_date=decision_date,
        alpha_frame=alpha_frame,
        portfolio_spec_ids=portfolio_spec_ids,
        risk_control_spec_id=risk_control_spec_id,
    )


@mcp.tool()
def create_builtin_alpha_strategy_graph_3_0(
    name: str,
    portfolio_construction_spec_id: str,
    selection_policy: dict | None = None,
    risk_control_spec_id: str | None = None,
    rebalance_policy_spec_id: str | None = None,
    execution_policy_spec_id: str | None = None,
) -> dict:
    """Create a StrategyGraph that accepts an alpha frame as input."""
    return _strategy_graph_3_service().create_builtin_alpha_graph(
        name=name,
        selection_policy=selection_policy,
        portfolio_construction_spec_id=portfolio_construction_spec_id,
        risk_control_spec_id=risk_control_spec_id,
        rebalance_policy_spec_id=rebalance_policy_spec_id,
        execution_policy_spec_id=execution_policy_spec_id,
    )


@mcp.tool()
def create_legacy_strategy_adapter_graph_3_0(
    name: str,
    legacy_strategy_id: str,
    portfolio_construction_spec_id: str,
    risk_control_spec_id: str | None = None,
    execution_policy_spec_id: str | None = None,
) -> dict:
    """Create a StrategyGraph wrapper for legacy strategy signal frames."""
    return _strategy_graph_3_service().create_legacy_strategy_adapter_graph(
        name=name,
        legacy_strategy_id=legacy_strategy_id,
        portfolio_construction_spec_id=portfolio_construction_spec_id,
        risk_control_spec_id=risk_control_spec_id,
        execution_policy_spec_id=execution_policy_spec_id,
    )


@mcp.tool()
def simulate_strategy_graph_day_3_0(
    strategy_graph_id: str,
    decision_date: str,
    alpha_frame: list[dict] | None = None,
    legacy_signal_frame: list[dict] | None = None,
    current_weights: dict[str, float] | None = None,
) -> dict:
    """Run one StrategyGraph decision day and return stage outputs."""
    return _strategy_graph_3_service().simulate_day(
        strategy_graph_id,
        decision_date=decision_date,
        alpha_frame=alpha_frame,
        legacy_signal_frame=legacy_signal_frame,
        current_weights=current_weights,
    )


@mcp.tool()
def backtest_strategy_graph_3_0(
    strategy_graph_id: str,
    start_date: str,
    end_date: str,
    alpha_frames_by_date: dict[str, list[dict]] | None = None,
    legacy_signal_frames_by_date: dict[str, list[dict]] | None = None,
    initial_capital: float = 1_000_000,
    price_field: str = "close",
) -> dict:
    """Trigger a StrategyGraph historical backtest task.

    Planned-price StrategyGraphs use their execution policy and optional
    planned_price values in alpha_frames_by_date rows; fills are evaluated
    against execution-day high/low and written to fill diagnostics.
    """
    from backend.tasks.models import TaskSource

    params = {
        "strategy_graph_id": strategy_graph_id,
        "start_date": start_date,
        "end_date": end_date,
        "alpha_frames_by_date": alpha_frames_by_date,
        "legacy_signal_frames_by_date": legacy_signal_frames_by_date,
        "initial_capital": initial_capital,
        "price_field": price_field,
    }
    task_id = _task_executor().submit(
        task_type="strategy_graph_backtest",
        fn=_strategy_graph_3_service().backtest_graph,
        params=params,
        timeout=3600,
        source=TaskSource.AGENT,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "task_type": "strategy_graph_backtest",
        "strategy_graph_id": strategy_graph_id,
        "poll_url": f"/api/tasks/{task_id}",
    }


@mcp.tool()
def list_strategy_graph_backtests_3_0(
    strategy_graph_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List persisted StrategyGraph backtest runs."""
    return _strategy_graph_3_service().list_backtest_runs(
        strategy_graph_id=strategy_graph_id,
        limit=limit,
    )


@mcp.tool()
def get_strategy_graph_backtest_3_0(backtest_run_id: str) -> dict:
    """Get one persisted StrategyGraph backtest run."""
    return _strategy_graph_3_service().get_backtest_run(backtest_run_id)


@mcp.tool()
def explain_strategy_signal_3_0(strategy_signal_id: str) -> dict:
    """Return the persisted StrategyGraph single-day explanation."""
    return _strategy_graph_3_service().explain_day(strategy_signal_id)


@mcp.tool()
def list_research_playbooks_3_0() -> list[dict]:
    """List built-in agent research playbooks for the 3.0 workflow."""
    return _agent_research_3_service().list_playbooks()


@mcp.tool()
def create_agent_research_plan_3_0(
    hypothesis: str,
    playbook_id: str | None = None,
    search_space: dict | None = None,
    budget: dict | None = None,
    stop_conditions: dict | None = None,
    project_id: str | None = None,
    market_profile_id: str | None = None,
    created_by: str = "agent",
) -> dict:
    """Create a controlled agent research plan with budget and stop rules."""
    return _agent_research_3_service().create_research_plan(
        hypothesis=hypothesis,
        playbook_id=playbook_id,
        search_space=search_space,
        budget=budget,
        stop_conditions=stop_conditions,
        project_id=project_id,
        market_profile_id=market_profile_id,
        created_by=created_by,
    )


@mcp.tool()
def record_agent_research_trial_3_0(
    plan_id: str,
    trial_type: str,
    params: dict | None = None,
    result_refs: list[dict] | None = None,
    metrics: dict | None = None,
    qa_report_id: str | None = None,
    status: str = "completed",
) -> dict:
    """Record one agent research trial under a bounded 3.0 plan."""
    return _agent_research_3_service().record_trial(
        plan_id,
        trial_type=trial_type,
        params=params,
        result_refs=result_refs,
        metrics=metrics,
        qa_report_id=qa_report_id,
        status=status,
    )


@mcp.tool()
def record_agent_research_trials_batch_3_0(
    plan_id: str,
    trials: list[dict],
    dedupe_by_params: bool = True,
) -> dict:
    """Record many agent research trials in one bounded 3.0 write."""
    return _agent_research_3_service().record_trials(
        plan_id,
        trials=trials,
        dedupe_by_params=dedupe_by_params,
    )


@mcp.tool()
def check_agent_research_budget_3_0(plan_id: str) -> dict:
    """Return remaining trial budget for a 3.0 agent research plan."""
    return _agent_research_3_service().check_budget(plan_id)


@mcp.tool()
def get_agent_research_plan_performance_3_0(
    plan_id: str,
    primary_metric: str = "sharpe",
    top_n: int = 10,
) -> dict:
    """Return compact trial ranking and metric ranges for an agent research plan."""
    return _agent_research_3_service().get_plan_performance(
        plan_id,
        primary_metric=primary_metric,
        top_n=top_n,
    )


@mcp.tool()
def evaluate_qa_gate_3_0(
    source_type: str,
    source_id: str,
    metrics: dict | None = None,
    artifact_refs: list[dict] | None = None,
    project_id: str | None = None,
    market_profile_id: str | None = None,
) -> dict:
    """Run the 3.0 QA gate for backtests, models, factors, or strategy graphs."""
    return _agent_research_3_service().evaluate_qa(
        source_type=source_type,
        source_id=source_id,
        metrics=metrics,
        artifact_refs=artifact_refs,
        project_id=project_id,
        market_profile_id=market_profile_id,
    )


@mcp.tool()
def evaluate_research_promotion_3_0(
    source_type: str,
    source_id: str,
    qa_report_id: str,
    metrics: dict | None = None,
    policy_id: str | None = None,
    approved_by: str = "agent",
    rationale: str | None = None,
) -> dict:
    """Evaluate whether a 3.0 research result can be promoted."""
    return _agent_research_3_service().evaluate_promotion(
        source_type=source_type,
        source_id=source_id,
        qa_report_id=qa_report_id,
        metrics=metrics,
        policy_id=policy_id,
        approved_by=approved_by,
        rationale=rationale,
    )


@mcp.tool()
def generate_production_signal_3_0(
    strategy_graph_id: str,
    decision_date: str,
    alpha_frame: list[dict] | None = None,
    legacy_signal_frame: list[dict] | None = None,
    current_weights: dict[str, float] | None = None,
    portfolio_value: float = 1_000_000,
    qa_report_id: str | None = None,
    approved_by: str = "agent",
) -> dict:
    """Generate a validated/published production signal from a StrategyGraph."""
    return _production_signal_3_service().generate_production_signal(
        strategy_graph_id=strategy_graph_id,
        decision_date=decision_date,
        alpha_frame=alpha_frame,
        legacy_signal_frame=legacy_signal_frame,
        current_weights=current_weights,
        portfolio_value=portfolio_value,
        qa_report_id=qa_report_id,
        approved_by=approved_by,
    )


@mcp.tool()
def create_paper_session_3_0(
    strategy_graph_id: str,
    start_date: str,
    name: str | None = None,
    initial_capital: float = 1_000_000,
    config: dict | None = None,
) -> dict:
    """Create a 3.0 paper session that shares the StrategyGraph runtime."""
    return _production_signal_3_service().create_paper_session(
        strategy_graph_id=strategy_graph_id,
        start_date=start_date,
        name=name,
        initial_capital=initial_capital,
        config=config,
    )


@mcp.tool()
def advance_paper_session_3_0(
    session_id: str,
    decision_date: str,
    alpha_frame: list[dict] | None = None,
    legacy_signal_frame: list[dict] | None = None,
) -> dict:
    """Advance a 3.0 paper session by one day using the same runtime as production signal."""
    return _production_signal_3_service().advance_paper_session(
        session_id,
        decision_date=decision_date,
        alpha_frame=alpha_frame,
        legacy_signal_frame=legacy_signal_frame,
    )


@mcp.tool()
def export_reproducibility_bundle_3_0(
    source_type: str,
    source_id: str,
    name: str | None = None,
) -> dict:
    """Export a reproducibility bundle for a published 3.0 research asset."""
    return _production_signal_3_service().export_reproducibility_bundle(
        source_type=source_type,
        source_id=source_id,
        name=name,
    )


@mcp.tool()
def update_data(
    mode: str = "incremental",
    market: str | None = None,
    history_years: int | None = None,
    start_date: str | None = None,
) -> dict:
    """Trigger a data update task to fetch latest market data.

    Args:
        mode: Update mode - "incremental" (only new bars) or "full" (re-fetch all).
        market: Market scope. Defaults to "US" for compatibility.
        history_years: Optional backfill window override for newly initialized tickers.
        start_date: Optional explicit backfill start date (YYYY-MM-DD).

    Returns:
        Dict with task_id and status for tracking the background task.
    """
    from backend.tasks.models import TaskSource
    resolved_market = _resolve_market(market)
    svc = _data_service()
    executor = _task_executor()

    task_id = executor.submit(
        task_type="data_update",
        fn=svc.update_data,
        params={
            "mode": mode,
            "market": resolved_market,
            "history_years": history_years,
            "start_date": start_date,
        },
        timeout=7200,
        source=TaskSource.AGENT,
    )

    return _task_response(
        task_id=task_id,
        task_type="data_update",
        market=resolved_market,
        mode=mode,
        history_years=history_years,
        start_date=start_date,
    )


@mcp.tool()
def update_data_markets(
    markets: list[str],
    mode: str = "incremental",
    history_years: int | None = None,
    start_date: str | None = None,
) -> dict:
    """Trigger a sequential multi-market data update task.

    Args:
        markets: Market scopes to update, e.g. ["US", "CN"].
        mode: Update mode - "incremental" or "full".
        history_years: Optional backfill window override.
        start_date: Optional explicit backfill start date (YYYY-MM-DD).

    Returns:
        Dict with task_id and status for tracking the background task.
    """
    from backend.tasks.models import TaskSource
    resolved_markets = list(dict.fromkeys(_resolve_market(market) for market in markets))
    svc = _data_service()
    executor = _task_executor()

    task_id = executor.submit(
        task_type="data_update",
        fn=svc.update_markets,
        params={
            "mode": mode,
            "markets": resolved_markets,
            "history_years": history_years,
            "start_date": start_date,
        },
        timeout=14400,
        source=TaskSource.AGENT,
    )

    return {
        "task_id": task_id,
        "status": "queued",
        "task_type": "data_update",
        "markets": resolved_markets,
        "asset_scope": {"markets": resolved_markets},
        "poll_url": f"/api/tasks/{task_id}",
        "mode": mode,
        "history_years": history_years,
        "start_date": start_date,
    }


@mcp.tool()
def refresh_stock_list(market: str | None = None) -> dict:
    """Trigger a stock-universe refresh without downloading daily bars.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Dict with task_id and status for tracking the background task.
    """
    from backend.tasks.models import TaskSource
    resolved_market = _resolve_market(market)
    svc = _data_service()
    executor = _task_executor()

    task_id = executor.submit(
        task_type="stock_list_refresh",
        fn=svc.refresh_stock_list,
        params={"market": resolved_market},
        timeout=600,
        source=TaskSource.AGENT,
    )

    return _task_response(
        task_id=task_id,
        task_type="stock_list_refresh",
        market=resolved_market,
    )


# ======================================================================
# Factor tools
# ======================================================================


@mcp.tool()
def list_factors(category: str | None = None, market: str | None = None) -> list[dict]:
    """List all available factors in the factor library.

    Args:
        category: Optional category filter (e.g. "momentum", "value", "custom").
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of factor definitions with id, name, version, category, status.
    """
    resolved_market = _resolve_market(market)
    svc = _factor_service()
    svc.ensure_builtin_templates(resolved_market)
    return svc.list_factors(category=category, market=resolved_market)


@mcp.tool()
def evaluate_factor(
    factor_id: str,
    label_id: str,
    universe_group_id: str,
    start_date: str,
    end_date: str,
    market: str | None = None,
) -> dict:
    """Trigger factor evaluation against a label definition.

    Computes IC, IR, and group return metrics for the factor.

    Args:
        factor_id: ID of the factor to evaluate.
        label_id: ID of the label definition to evaluate against.
        universe_group_id: ID of the stock group for the universe.
        start_date: Evaluation start date (YYYY-MM-DD).
        end_date: Evaluation end date (YYYY-MM-DD).
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Dict with task_id for tracking the background evaluation task.
    """
    from backend.services.factor_eval_service import FactorEvalService
    from backend.tasks.models import TaskSource

    resolved_market = _resolve_market(market)
    eval_svc = FactorEvalService()
    executor = _task_executor()

    def _do_evaluate(
        factor_id: str,
        label_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
        market: str,
    ) -> dict:
        return eval_svc.evaluate_factor(
            factor_id=factor_id,
            label_id=label_id,
            universe_group_id=universe_group_id,
            start_date=start_date,
            end_date=end_date,
            market=market,
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
            "market": resolved_market,
        },
        timeout=3600,
        source=TaskSource.AGENT,
    )

    return _task_response(
        task_id=task_id,
        task_type="factor_evaluate",
        market=resolved_market,
        factor_id=factor_id,
    )


@mcp.tool()
def create_factor(
    name: str,
    description: str,
    category: str,
    source_code: str,
    market: str | None = None,
) -> dict:
    """Create a new factor definition in the factor library.

    The source_code must define a class that inherits from FactorBase
    and implements the compute(ohlcv) method.

    Args:
        name: Unique factor name.
        description: Human-readable description of the factor logic.
        category: Factor category (e.g. "momentum", "value", "volatility", "custom").
        source_code: Python source code implementing the factor.
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        The created factor record with id, name, version, status.
    """
    resolved_market = _resolve_market(market)
    svc = _factor_service()
    return svc.create_factor(
        name=name,
        source_code=source_code,
        description=description,
        category=category,
        market=resolved_market,
    )


# ======================================================================
# Model tools
# ======================================================================


@mcp.tool()
def list_models(market: str | None = None) -> list[dict]:
    """List all trained ML models.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of model records with id, name, model_type, eval_metrics, status.
    """
    resolved_market = _resolve_market(market)
    svc = _model_service()
    return svc.list_models(market=resolved_market)


@mcp.tool()
def train_model(
    name: str,
    feature_set_id: str,
    label_id: str,
    model_type: str,
    model_params: dict | None,
    train_config: dict | None,
    universe_group_id: str,
    market: str | None = None,
    objective_type: str | None = None,
    ranking_config: dict | None = None,
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
        market: Market scope. Defaults to "US" for compatibility.
        objective_type: Optional objective: regression, classification, ranking,
            pairwise, or listwise.
        ranking_config: Optional ranking objective configuration.

    Returns:
        Dict with task_id for tracking the background training task.
    """
    from backend.tasks.models import TaskSource
    resolved_market = _resolve_market(market)
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
        market: str,
        objective_type: str | None,
        ranking_config: dict | None,
    ) -> dict:
        return svc.train_model(
            name=name,
            feature_set_id=feature_set_id,
            label_id=label_id,
            model_type=model_type,
            model_params=model_params,
            train_config=train_config,
            universe_group_id=universe_group_id,
            market=market,
            objective_type=objective_type,
            ranking_config=ranking_config,
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
            "market": resolved_market,
            "objective_type": objective_type,
            "ranking_config": ranking_config,
        },
        timeout=7200,
        source=TaskSource.AGENT,
    )

    return _task_response(
        task_id=task_id,
        task_type="model_train",
        market=resolved_market,
        name=name,
    )


@mcp.tool()
def train_model_distillation(
    name: str,
    teacher_model_id: str,
    student_feature_set_id: str,
    universe_group_id: str,
    start_date: str,
    end_date: str,
    market: str | None = None,
    model_type: str = "lightgbm",
    model_params: dict | None = None,
    train_config: dict | None = None,
    objective_type: str | None = "regression",
    ranking_config: dict | None = None,
    prediction_feature_set_id: str | None = None,
    label_name: str | None = None,
) -> dict:
    """Train a student model from frozen teacher prediction labels."""
    from backend.tasks.models import TaskSource
    resolved_market = _resolve_market(market)
    svc = _model_service()
    executor = _task_executor()

    def _do_train_distillation(
        name: str,
        teacher_model_id: str,
        student_feature_set_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
        market: str,
        model_type: str,
        model_params: dict | None,
        train_config: dict | None,
        objective_type: str | None,
        ranking_config: dict | None,
        prediction_feature_set_id: str | None,
        label_name: str | None,
    ) -> dict:
        return svc.train_distilled_model(
            name=name,
            teacher_model_id=teacher_model_id,
            student_feature_set_id=student_feature_set_id,
            universe_group_id=universe_group_id,
            start_date=start_date,
            end_date=end_date,
            market=market,
            objective_type=objective_type,
            model_type=model_type,
            model_params=model_params,
            train_config=train_config,
            ranking_config=ranking_config,
            prediction_feature_set_id=prediction_feature_set_id,
            label_name=label_name,
        )

    task_id = executor.submit(
        task_type="model_distillation_train",
        fn=_do_train_distillation,
        params={
            "name": name,
            "teacher_model_id": teacher_model_id,
            "student_feature_set_id": student_feature_set_id,
            "universe_group_id": universe_group_id,
            "start_date": start_date,
            "end_date": end_date,
            "market": resolved_market,
            "model_type": model_type,
            "model_params": model_params,
            "train_config": train_config,
            "objective_type": objective_type,
            "ranking_config": ranking_config,
            "prediction_feature_set_id": prediction_feature_set_id,
            "label_name": label_name,
        },
        timeout=7200,
        source=TaskSource.AGENT,
    )

    return _task_response(
        task_id=task_id,
        task_type="model_distillation_train",
        market=resolved_market,
        name=name,
    )


# ======================================================================
# Strategy tools
# ======================================================================


@mcp.tool()
def list_strategies(market: str | None = None) -> list[dict]:
    """List all registered trading strategies.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of strategy records with id, name, version, required_factors,
        required_models, position_sizing, status.
    """
    resolved_market = _resolve_market(market)
    svc = _strategy_service()
    return svc.list_strategies(market=resolved_market)


@mcp.tool()
def create_strategy(
    name: str,
    source_code: str,
    description: str | None = None,
    position_sizing: str = "equal_weight",
    constraint_config: dict | None = None,
    market: str | None = None,
) -> dict:
    """Create a market-scoped strategy definition.

    Args:
        name: Strategy name.
        source_code: Python source implementing StrategyBase.
        description: Optional description.
        position_sizing: Position sizing mode: equal_weight, signal_weight, max_position, or raw_weight.
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Created strategy record.
    """
    resolved_market = _resolve_market(market)
    svc = _strategy_service()
    return svc.create_strategy(
        name=name,
        source_code=source_code,
        description=description,
        position_sizing=position_sizing,
        constraint_config=constraint_config,
        market=resolved_market,
    )


@mcp.tool()
def run_backtest(
    strategy_id: str,
    config_json: str,
    universe_group_id: str,
    market: str | None = None,
) -> dict:
    """Trigger a backtest for a strategy as a background task.

    Args:
        strategy_id: ID of the strategy to backtest.
        config_json: JSON string with backtest configuration. Keys:
            initial_capital, start_date, end_date, benchmark,
            commission_rate, slippage_rate, max_positions, rebalance_freq,
            rebalance_buffer, min_holding_days, reentry_cooldown_days,
            execution_model ("next_open" or "planned_price"), and
            planned_price_buffer_bps (default 50 for planned_price).
        universe_group_id: ID of the stock group for the backtest universe.
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Dict with task_id for tracking the background backtest task.
    """
    import json as _json
    from backend.tasks.models import TaskSource
    resolved_market = _resolve_market(market)

    try:
        config = _json.loads(config_json) if isinstance(config_json, str) else config_json
    except _json.JSONDecodeError as e:
        return {"error": f"Invalid config_json: {e}", "market": resolved_market}

    bt_svc = _backtest_service()
    executor = _task_executor()

    def _do_backtest(
        strategy_id: str,
        config: dict,
        universe_group_id: str,
        market: str,
    ) -> dict:
        return bt_svc.run_backtest(
            strategy_id=strategy_id,
            config_dict=config,
            universe_group_id=universe_group_id,
            market=market,
        )

    task_id = executor.submit(
        task_type="strategy_backtest",
        fn=_do_backtest,
        params={
            "strategy_id": strategy_id,
            "config": config,
            "universe_group_id": universe_group_id,
            "market": resolved_market,
        },
        timeout=3600,
        source=TaskSource.AGENT,
    )

    return _task_response(
        task_id=task_id,
        task_type="strategy_backtest",
        market=resolved_market,
        strategy_id=strategy_id,
    )


@mcp.tool()
def get_backtest_debug_replay(
    backtest_id: str,
    market: str | None = None,
    date: str | None = None,
    ticker: str | None = None,
) -> dict:
    """Load a temporary debug replay bundle produced by debug_mode backtest."""
    resolved_market = _resolve_market(market)
    return _backtest_service().get_debug_replay(
        backtest_id,
        market=resolved_market,
        date=date,
        ticker=ticker,
    )


@mcp.tool()
def cleanup_backtest_debug_replay(ttl_hours: int = 24) -> dict:
    """Delete expired temporary backtest debug replay bundles."""
    return _backtest_service().cleanup_debug_replay(ttl_hours=ttl_hours)


@mcp.tool()
def generate_signals(
    strategy_id: str,
    target_date: str,
    universe_group_id: str,
    constraint_config: dict | None = None,
    market: str | None = None,
) -> dict:
    """Trigger signal generation for a strategy as a background task.

    Runs the full pipeline: dependency validation -> factors -> models -> signals.
    The result includes dependency chain validation and result_level classification.

    Args:
        strategy_id: ID of the strategy to generate signals for.
        target_date: Target date for signal generation (YYYY-MM-DD).
        universe_group_id: ID of the stock group for the signal universe.
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Dict with task_id for tracking the background signal generation task.
    """
    from backend.tasks.models import TaskSource
    resolved_market = _resolve_market(market)

    sig_svc = _signal_service()
    executor = _task_executor()

    def _do_generate(
        strategy_id: str,
        target_date: str,
        universe_group_id: str,
        market: str,
        constraint_config: dict | None,
    ) -> dict:
        return sig_svc.generate_signals(
            strategy_id=strategy_id,
            target_date=target_date,
            universe_group_id=universe_group_id,
            market=market,
            constraint_config=constraint_config,
        )

    task_id = executor.submit(
        task_type="signal_generate",
        fn=_do_generate,
        params={
            "strategy_id": strategy_id,
            "target_date": target_date,
            "universe_group_id": universe_group_id,
            "market": resolved_market,
            "constraint_config": constraint_config,
        },
        timeout=3600,
        source=TaskSource.AGENT,
    )

    return _task_response(
        task_id=task_id,
        task_type="signal_generate",
        market=resolved_market,
        strategy_id=strategy_id,
        target_date=target_date,
    )


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

    result = {
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
    if record.result_summary and isinstance(record.result_summary, dict):
        if record.result_summary.get("cancel_requested"):
            result["cancel_requested"] = True
        if record.result_summary.get("compute_may_continue"):
            result["compute_may_continue"] = True
        if record.result_summary.get("authoritative_terminal"):
            result["authoritative_terminal"] = True
        if record.result_summary.get("late_result_quarantined"):
            result["late_result_quarantined"] = True
        late_diagnostics = record.result_summary.get("late_result_diagnostics")
        if isinstance(late_diagnostics, dict):
            result["late_result_diagnostics"] = late_diagnostics
    return result


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
def list_groups(market: str | None = None) -> list[dict]:
    """List all stock groups with member counts.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of stock group records with id, name, description,
        group_type, member_count.
    """
    resolved_market = _resolve_market(market)
    svc = _group_service()
    svc.ensure_builtins(resolved_market)
    return svc.list_groups(market=resolved_market)


@mcp.tool()
def create_group(
    name: str,
    description: str,
    group_type: str,
    tickers: list[str] | None = None,
    filter_expr: str | None = None,
    market: str | None = None,
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
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        The created group record with id, name, member_count.
    """
    resolved_market = _resolve_market(market)
    svc = _group_service()
    return svc.create_group(
        name=name,
        description=description,
        group_type=group_type,
        tickers=tickers,
        filter_expr=filter_expr,
        market=resolved_market,
    )


@mcp.tool()
def refresh_index_groups(market: str | None = None) -> list[dict]:
    """Refresh built-in index constituent groups in one market.

    For CN this builds 上证50、沪深300、中证500、创业板指 and
    their de-duplicated union group.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of refreshed built-in index group records.
    """
    resolved_market = _resolve_market(market)
    svc = _group_service()
    return svc.refresh_index_groups(market=resolved_market)


# ======================================================================
# Label and feature tools
# ======================================================================


@mcp.tool()
def list_labels(market: str | None = None) -> list[dict]:
    """List label definitions in one market.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of label definitions.
    """
    resolved_market = _resolve_market(market)
    svc = _label_service()
    svc.ensure_presets(resolved_market)
    return svc.list_labels(market=resolved_market)


@mcp.tool()
def create_label(
    name: str,
    target_type: str = "return",
    horizon: int = 5,
    description: str | None = None,
    benchmark: str | None = None,
    config: dict | None = None,
    market: str | None = None,
) -> dict:
    """Create a market-scoped label definition.

    Args:
        name: Label name.
        target_type: Label target type.
        horizon: Forecast horizon in trading days.
        description: Optional description.
        benchmark: Optional benchmark for excess-return labels.
        config: Optional label config.
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Created label definition.
    """
    resolved_market = _resolve_market(market)
    svc = _label_service()
    return svc.create_label(
        name=name,
        description=description,
        target_type=target_type,
        horizon=horizon,
        benchmark=benchmark,
        config=config,
        market=resolved_market,
    )


@mcp.tool()
def list_feature_sets(market: str | None = None) -> list[dict]:
    """List feature sets in one market.

    Args:
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        List of feature-set definitions.
    """
    resolved_market = _resolve_market(market)
    svc = _feature_service()
    return svc.list_feature_sets(market=resolved_market)


@mcp.tool()
def create_feature_set(
    name: str,
    factor_refs: list[dict],
    description: str | None = None,
    preprocessing: dict | None = None,
    market: str | None = None,
) -> dict:
    """Create a market-scoped feature set.

    Args:
        name: Feature-set name.
        factor_refs: Factor references with factor_id/factor_name/version.
        description: Optional description.
        preprocessing: Optional preprocessing config.
        market: Market scope. Defaults to "US" for compatibility.

    Returns:
        Created feature-set definition.
    """
    resolved_market = _resolve_market(market)
    svc = _feature_service()
    return svc.create_feature_set(
        name=name,
        description=description,
        factor_refs=factor_refs,
        preprocessing=preprocessing,
        market=resolved_market,
    )


# ======================================================================
# Paper trading tools
# ======================================================================


@mcp.tool()
def list_paper_sessions(market: str | None = None) -> list[dict]:
    """List paper-trading sessions in one market."""
    resolved_market = _resolve_market(market)
    svc = _paper_service()
    return svc.list_sessions(market=resolved_market)


@mcp.tool()
def create_paper_session(
    strategy_id: str,
    universe_group_id: str,
    start_date: str,
    name: str | None = None,
    config: dict | None = None,
    market: str | None = None,
) -> dict:
    """Create a market-scoped paper-trading session.

    config may include execution_model="planned_price" and
    planned_price_buffer_bps. Missing strategy planned_price values fall back
    to decision-date close; failed planned orders are canceled for that day.
    """
    resolved_market = _resolve_market(market)
    svc = _paper_service()
    return svc.create_session(
        strategy_id=strategy_id,
        universe_group_id=universe_group_id,
        start_date=start_date,
        name=name,
        config=config,
        market=resolved_market,
    )


@mcp.tool()
def advance_paper_session(
    session_id: str,
    target_date: str | None = None,
    steps: int = 0,
    market: str | None = None,
) -> dict:
    """Advance a paper-trading session as a background task."""
    from backend.tasks.models import TaskSource

    resolved_market = _resolve_market(market)
    svc = _paper_service()
    executor = _task_executor()
    task_id = executor.submit(
        task_type="paper_trading_advance",
        fn=svc.advance,
        params={
            "session_id": session_id,
            "target_date": target_date,
            "steps": steps,
            "market": resolved_market,
        },
        timeout=1800,
        source=TaskSource.AGENT,
    )
    return _task_response(
        task_id=task_id,
        task_type="paper_trading_advance",
        market=resolved_market,
        session_id=session_id,
    )
