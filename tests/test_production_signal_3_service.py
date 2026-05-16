import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.portfolio_assets_3_service import PortfolioAssets3Service
from backend.services.production_signal_3_service import ProductionSignal3Service
from backend.services.strategy_graph_3_service import StrategyGraph3Service


class ProductionSignal3ServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "production_signal3.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        portfolio_service = PortfolioAssets3Service()
        portfolio = portfolio_service.create_portfolio_construction_spec(
            name="M10 equal weight",
            method="equal_weight",
            params={"top_n": 5},
        )
        risk = portfolio_service.create_risk_control_spec(
            name="M10 risk",
            rules=[
                {"rule": "max_positions", "max_positions": 3},
                {"rule": "max_single_weight", "max_weight": 0.40},
            ],
        )
        execution = portfolio_service.create_execution_policy_spec(
            name="M10 next open",
            policy_type="next_open",
            params={"price_field": "open"},
        )
        graph_service = StrategyGraph3Service(portfolio_service=portfolio_service)
        self.graph = graph_service.create_builtin_alpha_graph(
            name="M10 validated alpha graph",
            selection_policy={"top_n": 4, "score_column": "score"},
            portfolio_construction_spec_id=portfolio["id"],
            risk_control_spec_id=risk["id"],
            execution_policy_spec_id=execution["id"],
            lifecycle_stage="validated",
            status="active",
        )
        self.draft_graph = graph_service.create_builtin_alpha_graph(
            name="M10 draft alpha graph",
            selection_policy={"top_n": 4, "score_column": "score"},
            portfolio_construction_spec_id=portfolio["id"],
            risk_control_spec_id=risk["id"],
            execution_policy_spec_id=execution["id"],
        )
        self.alpha = [
            {"asset_id": "US_EQ:AAA", "score": 0.90},
            {"asset_id": "US_EQ:BBB", "score": 0.80},
            {"asset_id": "US_EQ:CCC", "score": 0.70},
            {"asset_id": "US_EQ:DDD", "score": 0.60},
            {"asset_id": "US_EQ:EEE", "score": 0.50},
        ]

    def test_production_signal_requires_validated_or_published_graph(self):
        service = ProductionSignal3Service()
        with self.assertRaisesRegex(ValueError, "validated or published"):
            service.generate_production_signal(
                strategy_graph_id=self.draft_graph["id"],
                decision_date="2024-01-02",
                alpha_frame=self.alpha,
            )

    def test_production_signal_uses_strategy_graph_runtime(self):
        service = ProductionSignal3Service()
        result = service.generate_production_signal(
            strategy_graph_id=self.graph["id"],
            decision_date="2024-01-02",
            alpha_frame=self.alpha,
            current_weights={"US_EQ:AAA": 0.10},
            approved_by="unit-test",
        )
        signal = result["production_signal_run"]

        self.assertEqual(signal["status"], "completed")
        self.assertEqual(signal["strategy_graph_id"], self.graph["id"])
        self.assertEqual(signal["strategy_signal_id"], result["strategy_signal"]["id"])
        self.assertEqual(signal["lifecycle_stage"], "published")
        self.assertTrue(result["target_portfolio"])
        self.assertTrue(result["order_intents"])
        self.assertIn("portfolio", result["stage_explain"]["stages"])

        counts = get_connection().execute(
            """SELECT
                    (SELECT COUNT(*) FROM production_signal_runs),
                    (SELECT COUNT(*) FROM strategy_signals),
                    (SELECT COUNT(*) FROM portfolio_runs)
            """
        ).fetchone()
        self.assertEqual(counts, (1, 1, 1))

    def test_production_and_paper_reject_legacy_signal_frames(self):
        service = ProductionSignal3Service()
        with self.assertRaisesRegex(ValueError, "legacy_signal_frame is disabled"):
            service.generate_production_signal(
                strategy_graph_id=self.graph["id"],
                decision_date="2024-01-02",
                alpha_frame=self.alpha,
                legacy_signal_frame=[{"ticker": "AAA", "signal": 1}],
            )

        session = service.create_paper_session(
            strategy_graph_id=self.graph["id"],
            name="M10 no legacy paper",
            start_date="2024-01-02",
            initial_capital=500_000,
        )
        with self.assertRaisesRegex(ValueError, "legacy_signal_frame is disabled"):
            service.advance_paper_session(
                session["id"],
                decision_date="2024-01-02",
                alpha_frame=self.alpha,
                legacy_signal_frame=[{"ticker": "AAA", "signal": 1}],
            )

    def test_paper_session_advances_with_same_runtime_and_exports_bundle(self):
        service = ProductionSignal3Service()
        session = service.create_paper_session(
            strategy_graph_id=self.graph["id"],
            name="M10 paper session",
            start_date="2024-01-02",
            initial_capital=500_000,
        )
        advanced = service.advance_paper_session(
            session["id"],
            decision_date="2024-01-02",
            alpha_frame=self.alpha,
        )
        bundle = service.export_reproducibility_bundle(
            source_type="strategy_graph",
            source_id=self.graph["id"],
            name="M10 graph bundle",
        )

        self.assertEqual(session["status"], "active")
        self.assertEqual(advanced["days_processed"], 1)
        self.assertEqual(advanced["paper_daily"]["production_signal_run_id"], advanced["production_signal_run"]["id"])
        self.assertGreater(advanced["paper_daily"]["nav"], 0)
        self.assertEqual(bundle["source_id"], self.graph["id"])
        self.assertIn("strategy_graph", bundle["bundle_payload"])
        self.assertIn("dependency_refs", bundle["bundle_payload"])

        counts = get_connection().execute(
            """SELECT
                    (SELECT COUNT(*) FROM paper_sessions),
                    (SELECT COUNT(*) FROM paper_daily),
                    (SELECT COUNT(*) FROM reproducibility_bundles)
            """
        ).fetchone()
        self.assertEqual(counts, (1, 1, 1))

    def test_paper_session_revalues_existing_weights_before_next_signal(self):
        conn = get_connection()
        conn.executemany(
            """INSERT INTO stocks
               (market, ticker, name, exchange, sector, status, updated_at)
               VALUES ('US', ?, ?, 'NYSE', 'Test', 'active', current_timestamp)""",
            [("AAA", "AAA Inc"), ("BBB", "BBB Inc")],
        )
        conn.executemany(
            """INSERT INTO daily_bars
               (market, ticker, date, open, high, low, close, volume, adj_factor)
               VALUES ('US', ?, ?, ?, ?, ?, ?, 1000, 1.0)""",
            [
                ("AAA", "2024-01-02", 100.0, 101.0, 99.0, 100.0),
                ("AAA", "2024-01-03", 110.0, 111.0, 109.0, 110.0),
                ("BBB", "2024-01-02", 50.0, 51.0, 49.0, 50.0),
                ("BBB", "2024-01-03", 50.0, 51.0, 49.0, 50.0),
            ],
        )
        service = ProductionSignal3Service()
        session = service.create_paper_session(
            strategy_graph_id=self.graph["id"],
            name="M10 revalue paper",
            start_date="2024-01-02",
            initial_capital=1_000_000,
        )
        first = service.advance_paper_session(
            session["id"],
            decision_date="2024-01-02",
            alpha_frame=[{"asset_id": "US_EQ:AAA", "score": 1.0}],
        )
        second = service.advance_paper_session(
            session["id"],
            decision_date="2024-01-03",
            alpha_frame=[{"asset_id": "US_EQ:AAA", "score": 1.0}],
        )

        self.assertAlmostEqual(first["paper_daily"]["nav"], 1_000_000.0, places=2)
        self.assertAlmostEqual(second["paper_daily"]["nav"], 1_100_000.0, places=2)
        self.assertEqual(second["paper_daily"]["diagnostics"]["valuation"]["status"], "valued")
        self.assertEqual(second["paper_daily"]["diagnostics"]["valuation"]["from_date"], "2024-01-02")
        self.assertEqual(second["paper_daily"]["diagnostics"]["valuation"]["to_date"], "2024-01-03")


if __name__ == "__main__":
    unittest.main()
