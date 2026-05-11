import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.macro_data_service import MacroDataService


class MacroDataServiceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "macro.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()

    def test_fred_observations_are_materialized_with_pit_fields(self):
        client = _FakeFredClient(
            observations=[
                {
                    "realtime_start": "2024-01-01",
                    "realtime_end": "2024-01-31",
                    "date": "2024-01-05",
                    "value": "3.14",
                },
                {
                    "realtime_start": "2024-02-01",
                    "realtime_end": "9999-12-31",
                    "date": "2024-02-05",
                    "value": ".",
                },
            ],
            metadata={
                "id": "DGS10",
                "title": "10-Year Treasury Constant Maturity Rate",
                "frequency_short": "D",
                "units_short": "%",
            },
        )
        service = MacroDataService(client=client)

        result = service.update_fred_series(
            series_ids=["DGS10"],
            start_date="2024-01-01",
            end_date="2024-02-29",
        )
        rows = service.query_series(
            series_ids=["DGS10"],
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        self.assertEqual(result["provider"], "fred")
        self.assertEqual(result["series_count"], 1)
        self.assertEqual(result["observation_count"], 2)
        self.assertEqual(rows[0]["series_id"], "DGS10")
        self.assertEqual(rows[0]["value"], 3.14)
        self.assertEqual(rows[0]["realtime_start"], "2024-01-01")
        self.assertEqual(rows[0]["available_at"], "2024-01-01 00:00:00")
        self.assertIsNone(rows[1]["value"])

        stored_meta = get_connection().execute(
            "SELECT title, frequency, units FROM macro_series WHERE provider = 'fred' AND series_id = 'DGS10'"
        ).fetchone()
        self.assertEqual(
            stored_meta,
            ("10-Year Treasury Constant Maturity Rate", "D", "%"),
        )

    def test_update_rejects_empty_series_list(self):
        service = MacroDataService(client=_FakeFredClient())
        with self.assertRaisesRegex(ValueError, "series_ids"):
            service.update_fred_series(series_ids=[], start_date="2024-01-01")

    def test_query_series_as_of_returns_latest_observation_version_per_date(self):
        conn = get_connection()
        conn.execute(
            """INSERT INTO macro_series
               (provider, series_id, title, frequency, units, metadata)
               VALUES ('fred', 'CPIAUCSL', 'CPI', 'M', 'Index', '{}')"""
        )
        conn.executemany(
            """INSERT INTO macro_observations
               (provider, series_id, date, realtime_start, realtime_end, available_at, value, source_metadata)
               VALUES ('fred', 'CPIAUCSL', DATE '2024-01-01', ?, ?, ?, ?, '{}')""",
            [
                ("2024-02-01", "2024-02-29", "2024-02-01 00:00:00", 100.0),
                ("2024-03-01", "9999-12-31", "2024-03-01 00:00:00", 101.0),
            ],
        )
        service = MacroDataService(client=_FakeFredClient())

        early = service.query_series_as_of(
            series_ids=["CPIAUCSL"],
            start_date="2024-01-01",
            end_date="2024-01-01",
            decision_time="2024-02-15 00:00:00",
        )
        revised = service.query_series_as_of(
            series_ids=["CPIAUCSL"],
            start_date="2024-01-01",
            end_date="2024-01-01",
            decision_time="2024-03-15 00:00:00",
        )

        self.assertEqual(len(early), 1)
        self.assertEqual(early[0]["value"], 100.0)
        self.assertEqual(early[0]["pit_query"]["decision_time"], "2024-02-15 00:00:00")
        self.assertEqual(revised[0]["value"], 101.0)

    def test_update_fred_series_can_request_historical_realtime_window(self):
        client = _FakeFredClient(
            observations=[
                {
                    "realtime_start": "2014-01-01",
                    "realtime_end": "2014-01-31",
                    "date": "2014-01-15",
                    "value": "1.5",
                }
            ],
            metadata={"id": "DGS10", "title": "10Y"},
        )
        service = MacroDataService(client=client)

        result = service.update_fred_series(
            series_ids=["DGS10"],
            start_date="2014-01-01",
            end_date="2014-01-31",
            realtime_start="2014-01-01",
            realtime_end="2014-01-31",
        )

        self.assertEqual(result["realtime_start"], "2014-01-01")
        self.assertEqual(result["realtime_end"], "2014-01-31")
        self.assertEqual(
            client.observation_calls,
            [("DGS10", "2014-01-01", "2014-01-31", "2014-01-01", "2014-01-31")],
        )


class _FakeFredClient:
    def __init__(self, observations=None, metadata=None):
        self.observations = observations or []
        self.metadata = metadata or {}
        self.observation_calls = []
        self.metadata_calls = []

    def get_series_metadata(self, series_id):
        self.metadata_calls.append(series_id)
        return dict(self.metadata)

    def get_series_observations(
        self,
        series_id,
        start_date=None,
        end_date=None,
        realtime_start=None,
        realtime_end=None,
    ):
        self.observation_calls.append((series_id, start_date, end_date, realtime_start, realtime_end))
        return [dict(row) for row in self.observations]


if __name__ == "__main__":
    unittest.main()
