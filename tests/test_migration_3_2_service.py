import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
