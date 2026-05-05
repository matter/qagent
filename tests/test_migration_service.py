import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from backend.db import close_db, init_db
from backend.services.migration_service import MigrationService


class MigrationServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "migration.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        self.svc = MigrationService()

    def test_build_report_includes_source_targets_and_hashes(self):
        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute(
                """
                INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
                VALUES
                    ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', current_timestamp),
                    ('CN', 'sh.600000', '浦发银行', 'SH', '', 'active', current_timestamp)
                """
            )
            report = self.svc.build_report(self.db_path)
        finally:
            conn.close()

        self.assertEqual(report["mode"], "dry-run")
        self.assertIn("stocks", report["source_tables"])
        self.assertIn("assets", report["target_estimates"])
        self.assertIn("warnings", report)
        self.assertTrue(report["source_tables"]["stocks"]["exists"])
        self.assertEqual(report["source_tables"]["stocks"]["row_count"], 2)
        self.assertTrue(report["source_tables"]["stocks"]["table_hash"])
        self.assertIn("factors", report["legacy_signatures"])

    def test_preview_legacy_factor_creates_run_and_artifact(self):
        self._seed_us_smoke_data()
        factor_id = self._create_demo_factor()

        preview = self.svc.preview_legacy_factor(
            factor_id=factor_id,
            universe_group_id="test20",
            start_date="2025-01-02",
            end_date="2025-02-28",
            market="US",
        )

        self.assertEqual(preview["run"]["run_type"], "legacy_factor_preview")
        self.assertEqual(preview["artifact"]["artifact_type"], "legacy_factor_preview")
        self.assertGreater(preview["preview"]["shape"]["rows"], 0)
        self.assertGreater(preview["preview"]["shape"]["columns"], 0)

    def test_materialize_legacy_universe_creates_asset_payload(self):
        self._seed_us_smoke_data()

        materialized = self.svc.materialize_legacy_universe(
            universe_group_id="test20",
            market="US",
        )

        self.assertEqual(materialized["run"]["run_type"], "legacy_universe_materialize")
        self.assertEqual(materialized["artifact"]["artifact_type"], "legacy_universe_materialization")
        self.assertEqual(materialized["materialization"]["member_count"], 20)
        self.assertEqual(materialized["materialization"]["asset_count"], 20)

    def test_legacy_strategy_backtest_uses_existing_engine_and_records_artifact(self):
        self._seed_us_smoke_data()
        strategy_id = self._create_demo_strategy()

        result = self.svc.run_legacy_strategy_backtest(
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

        self.assertEqual(result["run"]["run_type"], "legacy_strategy_backtest")
        self.assertEqual(result["artifact"]["artifact_type"], "legacy_strategy_backtest_report")
        self.assertIn("backtest_id", result["backtest"])
        self.assertIn("total_return", result["backtest"])

    def _seed_us_smoke_data(self) -> None:
        from scripts.seed_3_0_us_smoke_data import main as seed_main

        seed_main()

    def _create_demo_strategy(self) -> str:
        source_code = '''
from backend.strategies.base import StrategyBase
import pandas as pd

class DemoLegacyStrategy(StrategyBase):
    name = "DemoLegacyStrategy"

    def required_factors(self):
        return []

    def required_models(self):
        return []

    def generate_signals(self, context):
        tickers = list(context.prices.columns.get_level_values(1).unique())
        if not tickers:
            return pd.DataFrame(columns=["signal", "weight", "strength"])
        top = tickers[: min(5, len(tickers))]
        rows = []
        weight = 1.0 / len(top)
        for ticker in tickers:
            rows.append(
                {
                    "signal": 1 if ticker in top else 0,
                    "weight": weight if ticker in top else 0.0,
                    "strength": float(len(top) - top.index(ticker)) if ticker in top else 0.0,
                }
            )
        return pd.DataFrame(rows, index=tickers)
'''
        from backend.services.strategy_service import StrategyService

        svc = StrategyService()
        existing = [item for item in svc.list_strategies(market="US") if item["name"] == "DemoLegacyStrategy"]
        if existing:
            return existing[0]["id"]
        return svc.create_strategy(
            name="DemoLegacyStrategy",
            source_code=source_code,
            description="migration smoke legacy strategy",
            position_sizing="equal_weight",
            market="US",
        )["id"]

    def _create_demo_factor(self) -> str:
        source_code = '''
from backend.factors.base import FactorBase
import pandas as pd

class DemoLegacyFactor(FactorBase):
    name = "DemoLegacyFactor"
    description = "simple close/open spread factor"

    def compute(self, data):
        close = data["close"].astype(float)
        open_ = data["open"].astype(float)
        return (close - open_) / open_.replace(0, pd.NA)
'''
        from backend.services.factor_service import FactorService

        svc = FactorService()
        existing = [item for item in svc.list_factors(market="US") if item["name"] == "DemoLegacyFactor"]
        if existing:
            return existing[0]["id"]
        return svc.create_factor(
            name="DemoLegacyFactor",
            source_code=source_code,
            description="migration smoke legacy factor",
            category="custom",
            market="US",
        )["id"]


if __name__ == "__main__":
    unittest.main()
