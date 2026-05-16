import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.migration_3_2_service import Migration32Service


class Migration32ServiceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "migration32.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        self.service = Migration32Service()

    def test_dry_run_manifest_classifies_assets_without_writing(self):
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', current_timestamp)
            """
        )
        conn.execute(
            """
            INSERT INTO daily_bars (market, ticker, date, open, high, low, close, volume)
            VALUES ('US', 'AAPL', DATE '2024-01-02', 10, 11, 9, 10.5, 1000)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_groups (id, market, name, description)
            VALUES ('core', 'US', 'Core', 'Core universe')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_group_members (group_id, market, ticker)
            VALUES ('core', 'US', 'AAPL')
            """
        )
        conn.execute(
            """
            INSERT INTO factors (id, market, name, source_code, status)
            VALUES ('f1', 'US', 'DemoFactor', 'class Demo: pass', 'active')
            """
        )
        conn.execute(
            """
            INSERT INTO feature_sets (id, market, name, factor_refs, preprocessing, status)
            VALUES ('fs1', 'US', 'DemoFeatures', '[{"factor_id":"f1"}]', '{}', 'active')
            """
        )
        conn.execute(
            """
            INSERT INTO models (id, market, name, feature_set_id, label_id, model_type, status)
            VALUES ('m1', 'US', 'DemoModel', 'fs1', 'label1', 'lightgbm', 'active')
            """
        )
        conn.execute(
            """
            INSERT INTO strategies
                (id, market, name, source_code, required_factors, required_models, status)
            VALUES
                ('s1', 'US', 'DemoStrategy', 'class S: pass', '[]', '["m1"]', 'active')
            """
        )
        conn.execute(
            """
            INSERT INTO paper_trading_sessions
                (id, market, name, strategy_id, universe_group_id, start_date, status)
            VALUES
                ('p1', 'US', 'DemoPaper', 's1', 'core', DATE '2024-01-02', 'active')
            """
        )
        before_runs = conn.execute("SELECT COUNT(*) FROM research_runs").fetchone()[0]

        manifest = self.service.build_dry_run_manifest(db_path=self.db_path)
        after_runs = conn.execute("SELECT COUNT(*) FROM research_runs").fetchone()[0]

        self.assertEqual(manifest["mode"], "dry-run")
        self.assertFalse(manifest["would_write"])
        self.assertEqual(before_runs, after_runs)
        self.assertEqual(manifest["summary"]["source_row_count"], 9)
        self.assertEqual(manifest["assets"]["factors"]["action"], "re_enter")
        self.assertEqual(manifest["assets"]["stocks"]["action"], "import")
        self.assertEqual(manifest["assets"]["daily_bars"]["action"], "import")
        self.assertEqual(manifest["assets"]["feature_sets"]["action"], "rebuild")
        self.assertEqual(manifest["assets"]["models"]["action"], "rebuild")
        self.assertEqual(manifest["assets"]["strategies"]["action"], "rebuild")
        self.assertEqual(manifest["assets"]["paper_trading_sessions"]["action"], "rebuild")
        self.assertIn("factors:f1", manifest["asset_map"])
        self.assertEqual(manifest["asset_map"]["factors:f1"]["new_table"], "factor_specs")
        self.assertEqual(manifest["asset_map"]["paper_trading_sessions:p1"]["dependencies"]["strategy_id"], "s1")
        self.assertGreater(manifest["assets"]["factors"]["content_hash"], "")

    def test_write_manifest_files_uses_v3_2_names(self):
        manifest = self.service.build_dry_run_manifest(db_path=self.db_path)
        out_dir = Path(self._tmp.name) / "reports"

        json_path, md_path = self.service.write_manifest_files(manifest, out_dir=out_dir)

        self.assertTrue(json_path.exists())
        self.assertTrue(md_path.exists())
        self.assertIn("3.2-migration-dry-run", json_path.name)
        self.assertIn("V3.2 Migration Dry-Run Manifest", md_path.read_text())

    def test_apply_basic_assets_reenters_imports_and_rebuilds_idempotently(self):
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', current_timestamp)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_groups (id, market, name, description)
            VALUES ('core', 'US', 'Core', 'Core universe')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_group_members (group_id, market, ticker)
            VALUES ('core', 'US', 'AAPL')
            """
        )
        conn.execute(
            """
            INSERT INTO factors (id, market, name, source_code, status, description, category)
            VALUES ('f1', 'US', 'DemoFactor', 'class Demo: pass', 'active', 'demo factor', 'custom')
            """
        )

        first = self.service.apply_basic_assets(db_path=self.db_path)
        second = self.service.apply_basic_assets(db_path=self.db_path)

        self.assertEqual(first["mode"], "apply_basic_assets")
        self.assertEqual(first["assets"]["inserted"], 1)
        self.assertEqual(first["factor_specs"]["inserted"], 1)
        self.assertEqual(first["universes"]["inserted"], 1)
        self.assertEqual(second["assets"]["inserted"], 0)
        self.assertEqual(second["factor_specs"]["inserted"], 0)
        self.assertEqual(second["universes"]["inserted"], 0)

        asset = conn.execute(
            "SELECT asset_id, market_profile_id, symbol FROM assets WHERE symbol = 'AAPL'"
        ).fetchone()
        factor = conn.execute(
            "SELECT id, market_profile_id, name, source_type, source_ref FROM factor_specs WHERE name = 'DemoFactor'"
        ).fetchone()
        universe = conn.execute(
            "SELECT id, market_profile_id, name, source_ref FROM universes WHERE name = 'Core'"
        ).fetchone()

        self.assertEqual(asset, ("US_EQ:AAPL", "US_EQ", "AAPL"))
        self.assertEqual(factor[1:4], ("US_EQ", "DemoFactor", "v3_2_reentered_factor"))
        self.assertIn('"source_table": "factors"', factor[4])
        self.assertEqual(universe[1:3], ("US_EQ", "Core"))
        self.assertIn('"tickers": ["AAPL"]', universe[3])

    def test_apply_market_data_snapshots_registers_daily_bars_without_rewriting_source(self):
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', current_timestamp),
                ('CN', '600000', 'Pufa Bank', 'SSE', 'Financials', 'active', current_timestamp)
            """
        )
        conn.execute(
            """
            INSERT INTO daily_bars (market, ticker, date, open, high, low, close, volume, adj_factor)
            VALUES
                ('US', 'AAPL', DATE '2024-01-02', 10, 11, 9, 10.5, 1000, 1),
                ('US', 'MSFT', DATE '2024-01-02', 20, 21, 19, 20.5, 2000, 1),
                ('CN', '600000', DATE '2024-01-02', 8, 9, 7, 8.5, 3000, 1)
            """
        )
        before_bars = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]

        first = self.service.apply_market_data_snapshots(db_path=self.db_path)
        second = self.service.apply_market_data_snapshots(db_path=self.db_path)
        after_bars = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]

        self.assertEqual(first["mode"], "apply_market_data_snapshots")
        self.assertEqual(first["snapshots"]["inserted"], 2)
        self.assertEqual(second["snapshots"]["inserted"], 0)
        self.assertEqual(before_bars, after_bars)
        self.assertEqual(first["markets"]["US"]["row_count"], 2)
        self.assertEqual(first["markets"]["US"]["mapped_row_count"], 1)
        self.assertEqual(first["markets"]["US"]["unmapped_ticker_count"], 1)
        self.assertEqual(first["markets"]["US"]["missing_asset_tickers_sample"], ["MSFT"])
        self.assertEqual(first["markets"]["CN"]["market_profile_id"], "CN_A")
        self.assertEqual(first["markets"]["CN"]["mapped_row_count"], 1)

        snapshots = conn.execute(
            """SELECT market_profile_id, coverage_summary, quality_summary
                 FROM market_data_snapshots
                ORDER BY market_profile_id"""
        ).fetchall()
        self.assertEqual(len(snapshots), 2)
        us_snapshot = next(row for row in snapshots if row[0] == "US_EQ")
        us_coverage = json.loads(us_snapshot[1])
        us_quality = json.loads(us_snapshot[2])
        self.assertEqual(us_coverage["row_count"], 2)
        self.assertEqual(us_quality["status"], "needs_asset_mapping")

    def test_apply_dependency_assets_rebuilds_model_strategy_and_paper_chain_idempotently(self):
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES ('US', 'AAPL', 'Apple', 'NASDAQ', 'Technology', 'active', current_timestamp)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_groups (id, market, name, description)
            VALUES ('core', 'US', 'Core', 'Core universe')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_group_members (group_id, market, ticker)
            VALUES ('core', 'US', 'AAPL')
            """
        )
        conn.execute(
            """
            INSERT INTO factors (id, market, name, source_code, status, description, category)
            VALUES ('f1', 'US', 'DemoFactor', 'class DemoFactor: pass', 'active', 'demo factor', 'custom')
            """
        )
        conn.execute(
            """
            INSERT INTO label_definitions
                (id, market, name, target_type, horizon, config, status)
            VALUES
                ('l1', 'US', 'Forward 1d', 'return', 1, '{"target":"close"}', 'active')
            """
        )
        conn.execute(
            """
            INSERT INTO feature_sets
                (id, market, name, factor_refs, preprocessing, status)
            VALUES
                ('fs1', 'US', 'Demo Features',
                 '[{"factor_id":"f1","factor_name":"DemoFactor"}]',
                 '{"missing":"forward_fill"}', 'active')
            """
        )
        conn.execute(
            """
            INSERT INTO models
                (id, market, name, feature_set_id, label_id, model_type,
                 model_params, train_config, eval_metrics, status)
            VALUES
                ('m1', 'US', 'Demo Model', 'fs1', 'l1', 'lightgbm',
                 '{"n_estimators":8}', '{"train_start":"2024-01-02"}',
                 '{"test_rmse":0.1,"task_type":"regression"}', 'trained')
            """
        )
        conn.execute(
            """
            INSERT INTO strategies
                (id, market, name, source_code, required_factors,
                 required_models, status, description)
            VALUES
                ('s1', 'US', 'Demo Strategy',
                 'class DemoStrategy: pass',
                 '["DemoFactor"]', '["m1"]', 'active', 'strategy source')
            """
        )
        conn.execute(
            """
            INSERT INTO paper_trading_sessions
                (id, market, name, strategy_id, universe_group_id, config,
                 status, start_date, current_date, initial_capital, current_nav)
            VALUES
                ('p1', 'US', 'Demo Paper', 's1', 'core',
                 '{"rebalance":"weekly"}', 'active',
                 DATE '2024-01-02', DATE '2024-01-05', 1000000, 1010000)
            """
        )

        first = self.service.apply_dependency_assets(db_path=self.db_path)
        second = self.service.apply_dependency_assets(db_path=self.db_path)

        self.assertEqual(first["mode"], "apply_dependency_assets")
        self.assertEqual(first["feature_pipelines"]["inserted"], 1)
        self.assertEqual(first["label_specs"]["inserted"], 1)
        self.assertEqual(first["model_specs"]["inserted"], 1)
        self.assertEqual(first["model_packages"]["inserted"], 1)
        self.assertEqual(first["strategy_graphs"]["inserted"], 1)
        self.assertEqual(first["paper_sessions"]["inserted"], 1)
        self.assertEqual(second["feature_pipelines"]["inserted"], 0)
        self.assertEqual(second["label_specs"]["inserted"], 0)
        self.assertEqual(second["model_specs"]["inserted"], 0)
        self.assertEqual(second["model_packages"]["inserted"], 0)
        self.assertEqual(second["strategy_graphs"]["inserted"], 0)
        self.assertEqual(second["paper_sessions"]["inserted"], 0)

        pipeline = conn.execute(
            """SELECT id, source_type, source_ref
                 FROM feature_pipelines
                WHERE name = 'Demo Features'"""
        ).fetchone()
        label = conn.execute(
            """SELECT id, source_type, source_ref
                 FROM label_specs
                WHERE name = 'Forward 1d'"""
        ).fetchone()
        package = conn.execute(
            """SELECT id, source_experiment_id, model_artifact_id, status, metadata
                 FROM model_packages
                WHERE name = 'Demo Model'"""
        ).fetchone()
        graph = conn.execute(
            """SELECT id, graph_type, dependency_refs, status
                 FROM strategy_graphs
                WHERE name = 'Demo Strategy'"""
        ).fetchone()
        paper = conn.execute(
            """SELECT strategy_graph_id, status, config, initial_capital, current_nav
                 FROM paper_sessions
                WHERE name = 'Demo Paper'"""
        ).fetchone()
        artifact = conn.execute(
            "SELECT artifact_type FROM artifacts WHERE id = ?",
            [package[2]],
        ).fetchone()

        self.assertEqual(pipeline[1], "v3_2_rebuilt_feature_set")
        self.assertEqual(json.loads(pipeline[2])["source_id"], "fs1")
        self.assertEqual(label[1], "v3_2_reentered_label")
        self.assertEqual(json.loads(label[2])["source_id"], "l1")
        self.assertTrue(package[1].startswith("v32_retrain_experiment_"))
        self.assertEqual(package[3], "requires_retrain")
        self.assertFalse(json.loads(package[4])["executable"])
        self.assertEqual(artifact[0], "v3_2_legacy_model_manifest")
        self.assertEqual(graph[1], "v3_2_reimplemented_strategy_source")
        self.assertEqual(graph[3], "requires_reimplementation")
        graph_deps = json.loads(graph[2])
        self.assertTrue(any(item["type"] == "model_package" for item in graph_deps))
        self.assertEqual(paper[0], graph[0])
        self.assertEqual(paper[1], "migration_pending")
        self.assertEqual(json.loads(paper[2])["legacy_session_id"], "p1")
        self.assertEqual(paper[3], 1000000)
        self.assertEqual(paper[4], 1010000)


if __name__ == "__main__":
    unittest.main()
