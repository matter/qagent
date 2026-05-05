import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.factor_engine_3_service import FactorEngine3Service
from backend.services.factor_service import FactorService
from backend.services.label_service import LabelService
from backend.services.universe_service import UniverseService


_FACTOR_SOURCE = """\
import pandas as pd
from backend.factors.base import FactorBase


class Factor3Momentum(FactorBase):
    name = "Factor3Momentum"
    description = "one day close momentum"
    params = {"window": 1}
    category = "momentum"

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return data["close"].pct_change(1)
"""


class FactorEngine3ServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "factor3.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()
        self._seed_legacy_data()

    def test_import_preview_and_materialize_legacy_factor_spec(self):
        legacy_factor = FactorService().create_factor(
            name="Factor3 legacy momentum",
            source_code=_FACTOR_SOURCE,
            market="US",
        )
        universe = UniverseService().create_static_universe(
            name="Factor3 universe",
            tickers=["AAA", "BBB"],
            project_id="bootstrap_us",
            market_profile_id="US_EQ",
        )

        service = FactorEngine3Service()
        spec = service.create_spec_from_legacy_factor(
            legacy_factor_id=legacy_factor["id"],
            project_id="bootstrap_us",
            market="US",
            name="Factor3 momentum spec",
        )
        preview = service.preview_factor(
            factor_spec_id=spec["id"],
            universe_id=universe["id"],
            start_date="2024-01-03",
            end_date="2024-01-05",
        )
        materialized = service.materialize_factor(
            factor_spec_id=spec["id"],
            universe_id=universe["id"],
            start_date="2024-01-03",
            end_date="2024-01-05",
        )

        self.assertEqual(spec["source_type"], "legacy_factor")
        self.assertEqual(spec["compute_mode"], "time_series")
        self.assertEqual(preview["run"]["lifecycle_stage"], "scratch")
        self.assertEqual(preview["artifact"]["artifact_type"], "factor_preview")
        self.assertEqual(preview["factor_run"]["mode"], "preview")
        self.assertGreater(preview["profile"]["coverage"]["row_count"], 0)

        self.assertEqual(materialized["artifact"]["artifact_type"], "factor_values")
        self.assertEqual(materialized["factor_run"]["status"], "completed")
        self.assertEqual(materialized["factor_run"]["mode"], "materialize")
        self.assertEqual(materialized["factor_run"]["factor_spec_id"], spec["id"])
        self.assertGreater(materialized["profile"]["coverage"]["row_count"], 0)

        rows = get_connection().execute(
            "SELECT COUNT(*), COUNT(DISTINCT asset_id) FROM factor_values WHERE factor_spec_id = ?",
            [spec["id"]],
        ).fetchone()
        self.assertGreater(rows[0], 0)
        self.assertEqual(rows[1], 2)

    def test_evaluate_factor_run_writes_metrics_artifact_and_lineage(self):
        legacy_factor = FactorService().create_factor(
            name="Factor3 eval momentum",
            source_code=_FACTOR_SOURCE,
            market="US",
        )
        label = LabelService().create_label(
            name="Factor3 eval fwd 1d",
            target_type="return",
            horizon=1,
            market="US",
        )
        universe = UniverseService().create_static_universe(
            name="Factor3 eval universe",
            tickers=["AAA", "BBB"],
            project_id="bootstrap_us",
            market_profile_id="US_EQ",
        )
        service = FactorEngine3Service()
        spec = service.create_spec_from_legacy_factor(
            legacy_factor_id=legacy_factor["id"],
            project_id="bootstrap_us",
            market="US",
        )
        materialized = service.materialize_factor(
            factor_spec_id=spec["id"],
            universe_id=universe["id"],
            start_date="2024-01-03",
            end_date="2024-01-05",
        )

        evaluated = service.evaluate_factor_run(
            factor_run_id=materialized["factor_run"]["id"],
            label_id=label["id"],
        )

        self.assertEqual(evaluated["evaluation_artifact"]["artifact_type"], "factor_evaluation")
        self.assertEqual(evaluated["factor_run"]["mode"], "evaluate")
        self.assertIn("ic_mean", evaluated["metrics"])
        self.assertIn("coverage", evaluated["metrics"])
        self.assertFalse(evaluated["qa"]["blocking"])
        lineage = service.kernel.get_lineage(evaluated["run"]["id"])
        self.assertTrue(any(edge["relation"] == "evaluated" for edge in lineage["edges"]))

    def test_cross_sectional_spec_is_explicitly_not_implemented_yet(self):
        service = FactorEngine3Service()
        with self.assertRaisesRegex(ValueError, "cross_sectional"):
            service.create_python_spec(
                name="Cross sectional placeholder",
                source_code=_FACTOR_SOURCE,
                compute_mode="cross_sectional",
                project_id="bootstrap_us",
                market_profile_id="US_EQ",
            )

    def _seed_legacy_data(self) -> None:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO stocks (market, ticker, name, exchange, sector, status, updated_at)
            VALUES
                ('US', 'AAA', 'AAA Corp', 'NYSE', 'Technology', 'active', current_timestamp),
                ('US', 'BBB', 'BBB Corp', 'NASDAQ', 'Healthcare', 'active', current_timestamp)
            """
        )
        conn.execute(
            """
            INSERT INTO daily_bars
                (market, ticker, date, open, high, low, close, volume, adj_factor)
            VALUES
                ('US', 'AAA', DATE '2023-12-29', 9, 10, 8, 9, 1000, 1),
                ('US', 'AAA', DATE '2024-01-02', 10, 11, 9, 10, 1000, 1),
                ('US', 'AAA', DATE '2024-01-03', 10, 12, 9, 11, 1000, 1),
                ('US', 'AAA', DATE '2024-01-04', 11, 13, 10, 12, 1000, 1),
                ('US', 'AAA', DATE '2024-01-05', 12, 14, 11, 13, 1000, 1),
                ('US', 'AAA', DATE '2024-01-08', 13, 15, 12, 14, 1000, 1),
                ('US', 'BBB', DATE '2023-12-29', 21, 22, 20, 21, 1000, 1),
                ('US', 'BBB', DATE '2024-01-02', 20, 21, 19, 20, 1000, 1),
                ('US', 'BBB', DATE '2024-01-03', 20, 22, 19, 19, 1000, 1),
                ('US', 'BBB', DATE '2024-01-04', 19, 20, 18, 18, 1000, 1),
                ('US', 'BBB', DATE '2024-01-05', 18, 19, 17, 17, 1000, 1),
                ('US', 'BBB', DATE '2024-01-08', 17, 18, 16, 16, 1000, 1)
            """
        )


if __name__ == "__main__":
    unittest.main()
