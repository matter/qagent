#!/usr/bin/env python3
"""HTTP smoke test for QAgent 3.0 StrategyGraph runtime APIs."""

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

    portfolio = api(
        "post",
        "/api/research-assets/portfolio-construction-specs",
        json={"name": "M8 API Smoke Equal Weight", "method": "equal_weight", "params": {"top_n": 5}},
    )
    risk = api(
        "post",
        "/api/research-assets/risk-control-specs",
        json={
            "name": "M8 API Smoke Risk",
            "rules": [
                {"rule": "max_positions", "max_positions": 3},
                {"rule": "max_single_weight", "max_weight": 0.40},
            ],
        },
    )
    execution = api(
        "post",
        "/api/research-assets/execution-policy-specs",
        json={"name": "M8 API Smoke Next Open", "policy_type": "next_open", "params": {"price_field": "open"}},
    )
    graph = api(
        "post",
        "/api/research-assets/strategy-graphs/builtin-alpha",
        json={
            "name": "M8 API Smoke StrategyGraph",
            "selection_policy": {"top_n": 4, "score_column": "score"},
            "portfolio_construction_spec_id": portfolio["id"],
            "risk_control_spec_id": risk["id"],
            "execution_policy_spec_id": execution["id"],
        },
    )
    result = api(
        "post",
        f"/api/research-assets/strategy-graphs/{graph['id']}/simulate-day",
        json={
            "decision_date": "2025-01-02",
            "alpha_frame": [
                {"asset_id": "US_EQ:AAPL", "score": 0.91},
                {"asset_id": "US_EQ:MSFT", "score": 0.86},
                {"asset_id": "US_EQ:NVDA", "score": 0.82},
                {"asset_id": "US_EQ:AMZN", "score": 0.77},
                {"asset_id": "US_EQ:META", "score": 0.69},
            ],
            "current_weights": {"US_EQ:AAPL": 0.05},
        },
    )
    explain = api(
        "get",
        f"/api/research-assets/strategy-signals/{result['strategy_signal']['id']}/explain",
    )
    assert graph["graph_type"] == "builtin_alpha_graph"
    assert len(graph["nodes"]) == 6
    assert result["strategy_signal"]["status"] == "completed"
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
