import asyncio
import unittest
from unittest.mock import patch

from backend.api import macro_data as macro_api


class MacroDataApiTests(unittest.TestCase):
    def test_update_endpoint_submits_fred_update_task(self):
        fake_service = _FakeMacroDataService()
        fake_executor = _FakeExecutor()
        with (
            patch.object(macro_api, "_service", fake_service),
            patch.object(macro_api, "_get_executor", return_value=fake_executor),
        ):
            result = _run_async(
                macro_api.update_fred_series(
                    macro_api.FredUpdateRequest(
                        series_ids=["DGS10", "FEDFUNDS"],
                        start_date="2024-01-01",
                        end_date="2024-02-01",
                        realtime_start="2024-01-01",
                        realtime_end="2024-01-31",
                    )
                )
            )

        self.assertEqual(result["task_id"], "task_macro_update")
        self.assertEqual(result["provider"], "fred")
        self.assertEqual(fake_executor.task_type, "macro_data_update")
        self.assertEqual(fake_executor.params["series_ids"], ["DGS10", "FEDFUNDS"])
        self.assertEqual(fake_executor.params["start_date"], "2024-01-01")
        self.assertEqual(fake_executor.params["realtime_start"], "2024-01-01")
        self.assertEqual(fake_executor.params["realtime_end"], "2024-01-31")
        self.assertIs(fake_executor.fn.__self__, fake_service)
        self.assertIs(fake_executor.fn.__func__, fake_service.update_fred_series.__func__)

    def test_query_endpoint_forwards_series_and_dates(self):
        fake_service = _FakeMacroDataService()
        with patch.object(macro_api, "_service", fake_service):
            result = _run_async(
                macro_api.query_macro_series(
                    series_ids="DGS10,FEDFUNDS",
                    start_date="2024-01-01",
                    end_date="2024-02-01",
                    as_of=None,
                    strict_pit=False,
                )
            )

        self.assertEqual(result["provider"], "fred")
        self.assertEqual(result["series_ids"], ["DGS10", "FEDFUNDS"])
        self.assertEqual(fake_service.query_args["series_ids"], ["DGS10", "FEDFUNDS"])

    def test_query_endpoint_forwards_strict_pit_as_of_request(self):
        fake_service = _FakeMacroDataService()
        with patch.object(macro_api, "_service", fake_service):
            result = _run_async(
                macro_api.query_macro_series(
                    series_ids="DGS10",
                    start_date="2024-01-01",
                    end_date="2024-02-01",
                    as_of="2024-02-15 00:00:00",
                    strict_pit=True,
                )
            )

        self.assertTrue(result["strict_pit"])
        self.assertEqual(fake_service.query_as_of_args["decision_time"], "2024-02-15 00:00:00")


class _FakeMacroDataService:
    def __init__(self):
        self.query_args = None
        self.query_as_of_args = None

    def update_fred_series(self, **kwargs):
        return {"updated": kwargs}

    def query_series(self, **kwargs):
        self.query_args = kwargs
        return [{"series_id": "DGS10", "date": "2024-01-01", "value": 4.0}]

    def query_series_as_of(self, **kwargs):
        self.query_as_of_args = kwargs
        return [{"series_id": "DGS10", "date": "2024-01-01", "value": 4.0}]


class _FakeExecutor:
    def __init__(self):
        self.task_type = None
        self.fn = None
        self.params = None

    def submit(self, task_type, fn, params, timeout, source):
        self.task_type = task_type
        self.fn = fn
        self.params = params
        return "task_macro_update"


def _run_async(coro):
    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
