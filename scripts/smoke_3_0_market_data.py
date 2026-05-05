#!/usr/bin/env python3
"""Smoke test for the QAgent 3.0 Market/Data Foundation.

The M2 contract is intentionally profile-first:
- US_EQ and CN_A are real market profiles with split policies.
- Bootstrap project can resolve its market/data policy context.
- Data status is project-scoped and structured even when local data is empty.
- Assets expose stable asset_id values mapped from legacy stocks.
- Bar queries use asset_id internally while preserving legacy daily_bars storage.
"""

from __future__ import annotations

from datetime import date

from backend.db import init_db
from backend.services.market_data_foundation_service import MarketDataFoundationService


def main() -> int:
    init_db()
    svc = MarketDataFoundationService()

    profiles = svc.list_market_profiles()
    profile_ids = {item["id"] for item in profiles}
    assert {"US_EQ", "CN_A"}.issubset(profile_ids)

    us = svc.get_market_profile("US_EQ")
    cn = svc.get_market_profile("CN_A")

    assert us["data_policy"]["provider"] == "yfinance"
    assert us["trading_rule_set"]["calendar"] == "NYSE"
    assert us["trading_rule_set"]["decision_to_execution"] == "T+1_OPEN"
    assert "SPY" in us["benchmark_policy"]["benchmarks"]

    assert cn["data_policy"]["provider"] == "baostock"
    assert cn["trading_rule_set"]["calendar"] == "XSHG"
    assert cn["trading_rule_set"]["limit_up_down"] is True
    assert "st_status" in cn["trading_rule_set"]["tradability_fields"]

    project_context = svc.get_project_market_context("bootstrap_us")
    assert project_context["project"]["id"] == "bootstrap_us"
    assert project_context["market_profile"]["id"] == "US_EQ"

    status = svc.get_project_data_status("bootstrap_us")
    assert status["project_id"] == "bootstrap_us"
    assert status["market_profile_id"] == "US_EQ"
    assert "coverage" in status
    assert "latest_trading_day" in status

    assets = svc.search_assets(project_id="bootstrap_us", query="A", limit=5)
    assert isinstance(assets, list)
    for asset in assets:
        assert asset["asset_id"].startswith("US_EQ:")
        assert asset["symbol"]
        assert asset["market_profile_id"] == "US_EQ"

    bars = svc.query_bars(
        project_id="bootstrap_us",
        asset_ids=[asset["asset_id"] for asset in assets[:2]],
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )
    assert bars["project_id"] == "bootstrap_us"
    assert bars["market_profile_id"] == "US_EQ"
    assert "bars" in bars
    for row in bars["bars"]:
        assert row["asset_id"].startswith("US_EQ:")
        assert row["date"]

    print(
        {
            "profiles": sorted(profile_ids),
            "project_id": status["project_id"],
            "stock_count": status["coverage"]["asset_count"],
            "bar_rows": len(bars["bars"]),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
