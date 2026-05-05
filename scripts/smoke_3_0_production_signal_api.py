#!/usr/bin/env python3
"""HTTP smoke test for QAgent 3.0 production signal and paper runtime APIs."""

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
        json={"name": "M10 API equal weight", "method": "equal_weight", "params": {"top_n": 5}},
    )
    risk = api(
        "post",
        "/api/research-assets/risk-control-specs",
        json={
            "name": "M10 API risk",
            "rules": [
                {"rule": "max_positions", "max_positions": 3},
                {"rule": "max_single_weight", "max_weight": 0.40},
            ],
        },
    )
    execution = api(
        "post",
        "/api/research-assets/execution-policy-specs",
        json={"name": "M10 API next open", "policy_type": "next_open", "params": {"price_field": "open"}},
    )
    graph = api(
        "post",
        "/api/research-assets/strategy-graphs/builtin-alpha",
        json={
            "name": "M10 API graph",
            "selection_policy": {"top_n": 4, "score_column": "score"},
            "portfolio_construction_spec_id": portfolio["id"],
            "risk_control_spec_id": risk["id"],
            "execution_policy_spec_id": execution["id"],
            "lifecycle_stage": "validated",
            "status": "active",
        },
    )
    alpha = [
        {"asset_id": "US_EQ:AAPL", "score": 0.91},
        {"asset_id": "US_EQ:MSFT", "score": 0.86},
        {"asset_id": "US_EQ:NVDA", "score": 0.82},
        {"asset_id": "US_EQ:AMZN", "score": 0.77},
        {"asset_id": "US_EQ:META", "score": 0.69},
    ]
    prod = api(
        "post",
        "/api/research-assets/production-signals/generate",
        json={
            "strategy_graph_id": graph["id"],
            "decision_date": "2025-01-02",
            "alpha_frame": alpha,
            "current_weights": {"US_EQ:AAPL": 0.05},
            "approved_by": "api-smoke",
        },
    )
    session = api(
        "post",
        "/api/research-assets/paper-sessions",
        json={
            "strategy_graph_id": graph["id"],
            "start_date": "2025-01-02",
            "initial_capital": 500_000,
        },
    )
    advanced = api(
        "post",
        f"/api/research-assets/paper-sessions/{session['id']}/advance",
        json={"decision_date": "2025-01-02", "alpha_frame": alpha},
    )
    bundle = api(
        "post",
        "/api/research-assets/reproducibility-bundles",
        json={"source_type": "strategy_graph", "source_id": graph["id"], "name": "M10 API bundle"},
    )
    fetched_prod = api("get", f"/api/research-assets/production-signals/{prod['production_signal_run']['id']}")
    fetched_session = api("get", f"/api/research-assets/paper-sessions/{session['id']}")
    fetched_bundle = api("get", f"/api/research-assets/reproducibility-bundles/{bundle['id']}")

    assert prod["production_signal_run"]["status"] == "completed"
    assert prod["strategy_signal"]["status"] == "completed"
    assert advanced["paper_daily"]["production_signal_run_id"] == advanced["production_signal_run"]["id"]
    assert fetched_prod["id"] == prod["production_signal_run"]["id"]
    assert fetched_session["strategy_graph_id"] == graph["id"]
    assert fetched_bundle["source_id"] == graph["id"]
    print(
        {
            "strategy_graph_id": graph["id"],
            "production_signal_run_id": prod["production_signal_run"]["id"],
            "paper_session_id": session["id"],
            "bundle_id": bundle["id"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
