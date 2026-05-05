#!/usr/bin/env python3
"""Service smoke test for QAgent 3.0 portfolio/risk/execution assets."""

from __future__ import annotations

from backend.db import init_db
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service


def main() -> int:
    init_db()
    service = PortfolioAssets3Service()
    alpha = [
        {"asset_id": "US_EQ:AAPL", "score": 0.91, "volatility": 0.24},
        {"asset_id": "US_EQ:MSFT", "score": 0.86, "volatility": 0.18},
        {"asset_id": "US_EQ:NVDA", "score": 0.82, "volatility": 0.35},
        {"asset_id": "US_EQ:AMZN", "score": 0.77, "volatility": 0.21},
        {"asset_id": "US_EQ:META", "score": 0.69, "volatility": 0.28},
    ]
    portfolio = service.create_portfolio_construction_spec(
        name="M7 Smoke Equal Weight",
        method="equal_weight",
        params={"top_n": 5},
    )
    risk = service.create_risk_control_spec(
        name="M7 Smoke Risk",
        rules=[
            {"rule": "max_positions", "max_positions": 4},
            {"rule": "max_single_weight", "max_weight": 0.35},
        ],
    )
    rebalance = service.create_rebalance_policy_spec(
        name="M7 Smoke Band",
        policy_type="band",
        params={"band": 0.01},
    )
    execution = service.create_execution_policy_spec(
        name="M7 Smoke Next Open",
        policy_type="next_open",
        params={"price_field": "open"},
    )
    result = service.construct_portfolio(
        decision_date="2025-01-02",
        alpha_frame=alpha,
        portfolio_spec_id=portfolio["id"],
        risk_control_spec_id=risk["id"],
        rebalance_policy_spec_id=rebalance["id"],
        execution_policy_spec_id=execution["id"],
        current_weights={"US_EQ:AAPL": 0.05},
    )
    assert result["portfolio_run"]["status"] == "completed"
    assert result["profile"]["active_positions"] <= 4
    assert result["constraint_trace"]
    assert result["order_intents"]
    print(
        {
            "portfolio_spec_id": portfolio["id"],
            "risk_control_spec_id": risk["id"],
            "portfolio_run_id": result["portfolio_run"]["id"],
            "active_positions": result["profile"]["active_positions"],
            "trace_count": len(result["constraint_trace"]),
            "order_count": len(result["order_intents"]),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
