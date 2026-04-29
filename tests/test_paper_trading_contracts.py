import unittest
from unittest.mock import patch

from backend.api.paper_trading import AdvanceRequest, advance_session
from backend.services.paper_trading_service import PaperTradingService


class PaperTradingApiContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_advance_session_returns_queued_async_contract(self):
        class FakeExecutor:
            def submit(self, **kwargs):
                self.kwargs = kwargs
                return "task-123"

        class FakeService:
            def advance(self, session_id, target_date=None, steps=0):
                return {}

        fake_executor = FakeExecutor()

        with (
            patch("backend.api.paper_trading._get_executor", return_value=fake_executor),
            patch("backend.api.paper_trading._get_svc", return_value=FakeService()),
        ):
            result = await advance_session(
                "session-1",
                AdvanceRequest(target_date="2026-04-24", steps=0),
            )

        self.assertEqual(result["task_id"], "task-123")
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["task_type"], "paper_trading_advance")
        self.assertTrue(result["async"])
        self.assertEqual(result["poll_url"], "/api/tasks/task-123")


class PaperTradingServiceContractTests(unittest.TestCase):
    def test_new_session_target_before_start_is_not_reported_up_to_date(self):
        svc = PaperTradingService.__new__(PaperTradingService)
        session = {
            "id": "session-1",
            "current_date": None,
            "start_date": "2026-04-24",
        }

        with self.assertRaisesRegex(ValueError, "before session start_date"):
            svc._resolve_advance_trading_days(
                session=session,
                target_date="2026-04-23",
                steps=0,
            )


if __name__ == "__main__":
    unittest.main()
