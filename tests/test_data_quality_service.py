import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, init_db
from backend.api import market_data as market_data_api
from backend.services.data_quality_service import DataQualityService


class DataQualityServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "data_quality.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()

    def test_seeded_provider_capabilities_are_queryable(self):
        service = DataQualityService()
        capabilities = service.list_provider_capabilities()
        by_key = {(item["provider"], item["dataset"]) for item in capabilities}

        self.assertIn(("yfinance", "daily_bars"), by_key)
        self.assertIn(("baostock", "daily_bars"), by_key)
        self.assertIn(("fred", "macro_observations"), by_key)
        fred = service.list_provider_capabilities(provider="fred")[0]
        self.assertEqual(fred["quality_level"], "research_grade")
        self.assertFalse(fred["pit_supported"])
        self.assertEqual(fred["license_scope"], "free_api_key_required")

    def test_market_data_api_exposes_provider_capability_contract(self):
        fake_service = _FakeDataQualityService()
        with patch.object(market_data_api, "_quality_svc", return_value=fake_service):
            capabilities = _run_async(
                market_data_api.list_provider_capabilities(
                    provider="fred",
                    market_profile_id=None,
                    dataset=None,
                )
            )
            contract = _run_async(
                market_data_api.get_data_quality_contract(market_profile_id="US_EQ")
            )

        self.assertEqual(capabilities[0]["provider"], "fred")
        self.assertEqual(fake_service.calls[0], ("list", "fred", None, None))
        self.assertEqual(contract["market_profile_id"], "US_EQ")
        self.assertEqual(fake_service.calls[1], ("contract", "US_EQ"))


class _FakeDataQualityService:
    def __init__(self):
        self.calls = []

    def list_provider_capabilities(self, provider=None, market_profile_id=None, dataset=None):
        self.calls.append(("list", provider, market_profile_id, dataset))
        return [{"provider": provider or "fred"}]

    def get_data_quality_contract(self, market_profile_id=None):
        self.calls.append(("contract", market_profile_id))
        return {"market_profile_id": market_profile_id, "capabilities": []}


def _run_async(coro):
    import asyncio

    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
