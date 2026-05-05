#!/usr/bin/env python3
"""Smoke test for the QAgent 3.0 Migration Layer."""

from __future__ import annotations

from backend.db import init_db
from backend.services.factor_service import FactorService
from backend.services.migration_service import MigrationService
from backend.services.strategy_service import StrategyService


FACTOR_SOURCE = '''
from backend.factors.base import FactorBase
import pandas as pd

class MigrationSmokeFactor(FactorBase):
    name = "MigrationSmokeFactor"
    description = "close/open spread for M3 smoke"

    def compute(self, data):
        close = data["close"].astype(float)
        open_ = data["open"].astype(float)
        return (close - open_) / open_.replace(0, pd.NA)
'''


STRATEGY_SOURCE = '''
from backend.strategies.base import StrategyBase
import pandas as pd

class MigrationSmokeStrategy(StrategyBase):
    name = "MigrationSmokeStrategy"
    description = "equal-weight first five tickers for M3 smoke"

    def required_factors(self):
        return []

    def required_models(self):
        return []

    def generate_signals(self, context):
        tickers = list(context.prices.columns.get_level_values(1).unique())
        if not tickers:
            return pd.DataFrame(columns=["signal", "weight", "strength"])
        selected = tickers[: min(5, len(tickers))]
        weight = 1.0 / len(selected)
        rows = []
        for ticker in tickers:
            rows.append({
                "signal": 1 if ticker in selected else 0,
                "weight": weight if ticker in selected else 0.0,
                "strength": 1.0 if ticker in selected else 0.0,
            })
        return pd.DataFrame(rows, index=tickers)
'''


def main() -> int:
    init_db()
    migration = MigrationService()

    report = migration.build_report()
    assert "source_tables" in report
    assert "stocks" in report["source_tables"]
    assert "legacy_signatures" in report

    factor_id = _ensure_factor()
    preview = migration.preview_legacy_factor(
        factor_id=factor_id,
        universe_group_id="test20",
        start_date="2025-01-02",
        end_date="2025-02-28",
        market="US",
    )
    assert preview["preview"]["shape"]["rows"] > 0
    assert preview["artifact"]["artifact_type"] == "legacy_factor_preview"

    universe = migration.materialize_legacy_universe(
        universe_group_id="test20",
        market="US",
    )
    assert universe["materialization"]["member_count"] >= 1
    assert universe["artifact"]["artifact_type"] == "legacy_universe_materialization"

    strategy_id = _ensure_strategy()
    backtest = migration.run_legacy_strategy_backtest(
        strategy_id=strategy_id,
        universe_group_id="test20",
        market="US",
        config={
            "initial_capital": 100000,
            "start_date": "2025-01-02",
            "end_date": "2025-03-31",
            "benchmark": "SPY",
            "commission_rate": 0.001,
            "slippage_rate": 0.001,
            "max_positions": 5,
            "rebalance_freq": "weekly",
        },
    )
    assert backtest["backtest"]["backtest_id"]
    assert backtest["artifact"]["artifact_type"] == "legacy_strategy_backtest_report"

    applied = migration.apply_migration()
    assert applied["run"]["run_type"] == "migration_apply"
    assert applied["artifact"]["artifact_type"] == "migration_report"

    print(
        {
            "report_tables": len(report["source_tables"]),
            "factor_preview_run": preview["run"]["id"],
            "universe_run": universe["run"]["id"],
            "backtest_id": backtest["backtest"]["backtest_id"],
            "migration_run": applied["run"]["id"],
        }
    )
    return 0


def _ensure_factor() -> str:
    svc = FactorService()
    existing = [item for item in svc.list_factors(market="US") if item["name"] == "MigrationSmokeFactor"]
    if existing:
        return existing[0]["id"]
    return svc.create_factor(
        name="MigrationSmokeFactor",
        source_code=FACTOR_SOURCE,
        description="M3 smoke factor",
        category="custom",
        market="US",
    )["id"]


def _ensure_strategy() -> str:
    svc = StrategyService()
    existing = [item for item in svc.list_strategies(market="US") if item["name"] == "MigrationSmokeStrategy"]
    if existing:
        return existing[0]["id"]
    return svc.create_strategy(
        name="MigrationSmokeStrategy",
        source_code=STRATEGY_SOURCE,
        description="M3 smoke strategy",
        position_sizing="equal_weight",
        market="US",
    )["id"]


if __name__ == "__main__":
    raise SystemExit(main())
