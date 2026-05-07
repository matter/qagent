import unittest
from unittest.mock import patch

import backend.mcp_server as mcp_server


class MacroDataMcpTests(unittest.TestCase):
    def test_update_fred_series_tool_submits_agent_task(self):
        fake_service = _FakeMacroDataService()
        fake_executor = _FakeExecutor()
        with (
            patch.object(mcp_server, "_macro_data_service", return_value=fake_service),
            patch.object(mcp_server, "_task_executor", return_value=fake_executor),
        ):
            result = mcp_server.update_fred_series(
                series_ids=["DGS10"],
                start_date="2024-01-01",
                end_date="2024-01-31",
            )

        self.assertEqual(result["task_id"], "task_macro_mcp")
        self.assertEqual(result["provider"], "fred")
        self.assertEqual(fake_executor.task_type, "macro_data_update")
        self.assertEqual(fake_executor.params["series_ids"], ["DGS10"])


class _FakeMacroDataService:
    def update_fred_series(self, **kwargs):
        return kwargs


class _FakeExecutor:
    def __init__(self):
        self.task_type = None
        self.params = None

    def submit(self, task_type, fn, params, timeout, source):
        self.task_type = task_type
        self.params = params
        return "task_macro_mcp"


if __name__ == "__main__":
    unittest.main()
