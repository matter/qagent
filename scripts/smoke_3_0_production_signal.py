#!/usr/bin/env python3
"""Service smoke test for QAgent 3.0 production signal and paper runtime."""

from __future__ import annotations

from backend.db import init_db
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
from backend.services.production_signal_3_service import ProductionSignal3Service
from backend.services.strategy_graph_3_service import StrategyGraph3Service


def main() -> int:
    init_db()
    portfolio_service = PortfolioAssets3Service()
    portfolio = portfolio_service.create_portfolio_construction_spec(
        name="M10 smoke equal weight",
        method="equal_weight",
        params={"top_n": 5},
    )
    risk = portfolio_service.create_risk_control_spec(
        name="M10 smoke risk",
        rules=[
            {"rule": "max_positions", "max_positions": 3},
            {"rule": "max_single_weight", "max_weight": 0.40},
        ],
    )
    execution = portfolio_service.create_execution_policy_spec(
        name="M10 smoke next open",
        policy_type="next_open",
        params={"price_field": "open"},
    )
    graph_service = StrategyGraph3Service(portfolio_service=portfolio_service)
    graph = graph_service.create_builtin_alpha_graph(
        name="M10 smoke graph",
        selection_policy={"top_n": 4, "score_column": "score"},
        portfolio_construction_spec_id=portfolio["id"],
        risk_control_spec_id=risk["id"],
        execution_policy_spec_id=execution["id"],
        lifecycle_stage="validated",
        status="active",
    )
    service = ProductionSignal3Service(
        graph_service=graph_service,
    )
    alpha = [
        {"asset_id": "US_EQ:AAPL", "score": 0.91},
        {"asset_id": "US_EQ:MSFT", "score": 0.86},
        {"asset_id": "US_EQ:NVDA", "score": 0.82},
        {"asset_id": "US_EQ:AMZN", "score": 0.77},
        {"asset_id": "US_EQ:META", "score": 0.69},
    ]
    result = service.generate_production_signal(
        strategy_graph_id=graph["id"],
        decision_date="2025-01-02",
        alpha_frame=alpha,
        current_weights={"US_EQ:AAPL": 0.05},
        approved_by="service-smoke",
    )
    session = service.create_paper_session(
        strategy_graph_id=graph["id"],
        start_date="2025-01-02",
        initial_capital=500_000,
    )
    advanced = service.advance_paper_session(
        session["id"],
        decision_date="2025-01-02",
        alpha_frame=alpha,
    )
    bundle = service.export_reproducibility_bundle(
        source_type="strategy_graph",
        source_id=graph["id"],
        name="M10 smoke bundle",
    )

    assert result["production_signal_run"]["status"] == "completed"
    assert result["production_signal_run"]["strategy_graph_id"] == graph["id"]
    assert advanced["paper_daily"]["production_signal_run_id"] == advanced["production_signal_run"]["id"]
    assert bundle["source_id"] == graph["id"]
    assert bundle["bundle_artifact_id"]
    print(
        {
            "strategy_graph_id": graph["id"],
            "production_signal_run_id": result["production_signal_run"]["id"],
            "paper_session_id": session["id"],
            "paper_daily_nav": advanced["paper_daily"]["nav"],
            "bundle_id": bundle["id"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
