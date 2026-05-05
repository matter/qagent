#!/usr/bin/env python3
"""HTTP smoke test for the QAgent 3.0 Market/Data Foundation API."""

from __future__ import annotations

import requests


BASE = "http://127.0.0.1:8000"


def api(method: str, path: str, **kwargs):
    response = getattr(requests, method)(f"{BASE}{path}", timeout=10, **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> int:
    health = api("get", "/api/health")
    assert health["status"] == "ok"

    profiles = api("get", "/api/market-data/profiles")
    profile_ids = {item["id"] for item in profiles}
    assert {"US_EQ", "CN_A"}.issubset(profile_ids)

    cn = api("get", "/api/market-data/profiles/CN_A")
    assert cn["trading_rule_set"]["limit_up_down"] is True

    context = api("get", "/api/market-data/projects/bootstrap_us/context")
    assert context["market_profile"]["id"] == "US_EQ"

    status = api("get", "/api/market-data/projects/bootstrap_us/status")
    assert status["project_id"] == "bootstrap_us"
    assert "coverage" in status

    assets = api(
        "get",
        "/api/market-data/assets/search",
        params={"project_id": "bootstrap_us", "q": "A", "limit": 5},
    )
    for asset in assets:
        assert asset["asset_id"].startswith("US_EQ:")

    bars = api(
        "post",
        "/api/market-data/bars/query",
        json={
            "project_id": "bootstrap_us",
            "asset_ids": [item["asset_id"] for item in assets[:2]],
            "start": "2024-01-01",
            "end": "2024-01-31",
        },
    )
    assert bars["market_profile_id"] == "US_EQ"
    assert "bars" in bars

    print(
        {
            "profiles": sorted(profile_ids),
            "asset_count": status["coverage"]["asset_count"],
            "bar_rows": len(bars["bars"]),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
