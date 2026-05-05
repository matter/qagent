#!/usr/bin/env python3
"""Service smoke test for QAgent 3.0 StrategyGraph runtime."""

from __future__ import annotations

from backend.db import init_db
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
from backend.services.strategy_graph_3_service import StrategyGraph3Service


def main() -> int:
    init_db()
    portfolio_service = PortfolioAssets3Service()
    portfolio = portfolio_service.create_portfolio_construction_spec(
        name="M8 Smoke Equal Weight",
        method="equal_weight",
        params={"top_n": 5},
    )
    risk = portfolio_service.create_risk_control_spec(
        name="M8 Smoke Risk",
        rules=[
            {"rule": "max_positions", "max_positions": 3},
            {"rule": "max_single_weight", "max_weight": 0.40},
        ],
    )
    execution = portfolio_service.create_execution_policy_spec(
        name="M8 Smoke Next Open",
        policy_type="next_open",
        params={"price_field": "open"},
    )
    service = StrategyGraph3Service(portfolio_service=portfolio_service)
    graph = service.create_builtin_alpha_graph(
        name="M8 Smoke StrategyGraph",
        selection_policy={"top_n": 4, "score_column": "score"},
        portfolio_construction_spec_id=portfolio["id"],
        risk_control_spec_id=risk["id"],
        execution_policy_spec_id=execution["id"],
    )
    result = service.simulate_day(
        graph["id"],
        decision_date="2025-01-02",
        alpha_frame=[
            {"asset_id": "US_EQ:AAPL", "score": 0.91},
            {"asset_id": "US_EQ:MSFT", "score": 0.86},
            {"asset_id": "US_EQ:NVDA", "score": 0.82},
            {"asset_id": "US_EQ:AMZN", "score": 0.77},
            {"asset_id": "US_EQ:META", "score": 0.69},
        ],
        current_weights={"US_EQ:AAPL": 0.05},
    )
    explain = service.explain_day(result["strategy_signal"]["id"])
    assert result["strategy_signal"]["status"] == "completed"
    assert result["profile"]["active_positions"] <= 3
    assert explain["stages"]["portfolio"]["targets"]
    print(
        {
            "strategy_graph_id": graph["id"],
            "strategy_signal_id": result["strategy_signal"]["id"],
            "portfolio_run_id": result["portfolio_run"]["id"],
            "selected_count": result["profile"]["selected_count"],
            "order_count": result["profile"]["order_intent_count"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
