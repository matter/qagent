#!/usr/bin/env python3
"""HTTP smoke test for QAgent 3.0 portfolio/risk/execution asset APIs."""

from __future__ import annotations

import requests


BASE = "http://127.0.0.1:8000"


def api(method: str, path: str, **kwargs):
    response = getattr(requests, method)(f"{BASE}{path}", timeout=30, **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> int:
    health = api("get", "/api/health")
    assert health["status"] == "ok"

    alpha = [
        {"asset_id": "US_EQ:AAPL", "score": 0.91, "volatility": 0.24},
        {"asset_id": "US_EQ:MSFT", "score": 0.86, "volatility": 0.18},
        {"asset_id": "US_EQ:NVDA", "score": 0.82, "volatility": 0.35},
        {"asset_id": "US_EQ:AMZN", "score": 0.77, "volatility": 0.21},
        {"asset_id": "US_EQ:META", "score": 0.69, "volatility": 0.28},
    ]
    equal = api(
        "post",
        "/api/research-assets/portfolio-construction-specs",
        json={
            "name": "M7 API Smoke Equal Weight",
            "method": "equal_weight",
            "params": {"top_n": 5},
        },
    )
    inverse_vol = api(
        "post",
        "/api/research-assets/portfolio-construction-specs",
        json={
            "name": "M7 API Smoke Inverse Vol",
            "method": "inverse_vol",
            "params": {"top_n": 5, "volatility_column": "volatility"},
        },
    )
    risk = api(
        "post",
        "/api/research-assets/risk-control-specs",
        json={
            "name": "M7 API Smoke Risk",
            "rules": [
                {"rule": "max_positions", "max_positions": 4},
                {"rule": "max_single_weight", "max_weight": 0.35},
            ],
        },
    )
    rebalance = api(
        "post",
        "/api/research-assets/rebalance-policy-specs",
        json={
            "name": "M7 API Smoke Band",
            "policy_type": "band",
            "params": {"band": 0.01},
        },
    )
    execution = api(
        "post",
        "/api/research-assets/execution-policy-specs",
        json={
            "name": "M7 API Smoke Next Open",
            "policy_type": "next_open",
            "params": {"price_field": "open"},
        },
    )
    result = api(
        "post",
        "/api/research-assets/portfolio-runs/construct",
        json={
            "decision_date": "2025-01-02",
            "alpha_frame": alpha,
            "portfolio_spec_id": equal["id"],
            "risk_control_spec_id": risk["id"],
            "rebalance_policy_spec_id": rebalance["id"],
            "execution_policy_spec_id": execution["id"],
            "current_weights": {"US_EQ:AAPL": 0.05},
        },
    )
    comparison = api(
        "post",
        "/api/research-assets/portfolio-runs/compare-builders",
        json={
            "decision_date": "2025-01-02",
            "alpha_frame": alpha,
            "portfolio_spec_ids": [equal["id"], inverse_vol["id"]],
            "risk_control_spec_id": risk["id"],
        },
    )
    assert result["portfolio_run"]["status"] == "completed"
    assert result["profile"]["active_positions"] <= 4
    assert len(comparison["comparisons"]) == 2
    print(
        {
            "portfolio_spec_id": equal["id"],
            "risk_control_spec_id": risk["id"],
            "portfolio_run_id": result["portfolio_run"]["id"],
            "active_positions": result["profile"]["active_positions"],
            "trace_count": len(result["constraint_trace"]),
            "comparison_count": len(comparison["comparisons"]),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
