import asyncio
import unittest
from unittest.mock import patch

from backend.api import research_cache as cache_api


class ResearchCacheApiTests(unittest.TestCase):
    def test_inventory_endpoint_forwards_filters(self):
        fake_service = _FakeCacheService()
        with patch.object(cache_api, "_service", fake_service):
            result = _run_async(cache_api.cache_inventory(market="CN", object_type="feature_matrix", limit=5))

        self.assertEqual(result["summary"]["market"], "CN")
        self.assertEqual(fake_service.calls[0], ("summary", "CN"))
        self.assertEqual(fake_service.calls[1], ("list", "CN", "feature_matrix", 5, False))

    def test_inventory_endpoint_can_include_metadata_on_demand(self):
        fake_service = _FakeCacheService()
        with patch.object(cache_api, "_service", fake_service):
            result = _run_async(
                cache_api.cache_inventory(
                    market="CN",
                    object_type="feature_matrix",
                    limit=5,
                    include_metadata=True,
                )
            )

        self.assertEqual(result["entries"][0]["metadata"], {"large": "payload"})
        self.assertEqual(fake_service.calls[1], ("list", "CN", "feature_matrix", 5, True))

    def test_warmup_endpoint_submits_background_task(self):
        fake_feature_service = _FakeFeatureService()
        fake_group_service = _FakeGroupService()
        fake_executor = _FakeExecutor()
        with (
            patch.object(cache_api, "_feature_service", fake_feature_service),
            patch.object(cache_api, "_group_service", fake_group_service),
            patch.object(cache_api, "_get_executor", return_value=fake_executor),
        ):
            result = _run_async(
                cache_api.warmup_feature_matrix(
                    cache_api.WarmupFeatureMatrixRequest(
                        market="CN",
                        feature_set_id="fs_cn",
                        universe_group_id="group_cn",
                        start_date="2024-01-02",
                        end_date="2024-01-03",
                    )
                )
            )

        self.assertEqual(result["task_id"], "task_cache_warmup")
        self.assertEqual(fake_executor.task_type, "cache_feature_matrix_warmup")
        self.assertEqual(fake_executor.params["market"], "CN")

    def test_cleanup_apply_endpoint_uses_cache_service(self):
        fake_service = _FakeCacheService()
        with patch.object(cache_api, "_service", fake_service):
            preview = _run_async(cache_api.preview_factor_cache_cleanup(market="US", limit=10))
            applied = _run_async(cache_api.apply_factor_cache_cleanup(market="US", limit=10))

        self.assertEqual(preview["mode"], "preview")
        self.assertEqual(applied["mode"], "apply")
        self.assertEqual(fake_service.calls[-2:], [("preview_cleanup", "US", 10), ("apply_cleanup", "US", 10)])

    def test_orphan_file_cleanup_endpoints_use_cache_service(self):
        fake_service = _FakeCacheService()
        with patch.object(cache_api, "_service", fake_service):
            preview = _run_async(cache_api.preview_orphan_file_cleanup(limit=7, min_age_seconds=60))
            applied = _run_async(cache_api.apply_orphan_file_cleanup(limit=7, min_age_seconds=60))

        self.assertEqual(preview["mode"], "orphan_preview")
        self.assertEqual(applied["mode"], "orphan_apply")
        self.assertEqual(fake_service.calls[-2:], [("preview_orphan", 7, 60), ("apply_orphan", 7, 60)])


class _FakeCacheService:
    def __init__(self):
        self.calls = []

    def inventory_summary(self, market=None):
        self.calls.append(("summary", market))
        return {"market": market, "items": []}

    def list_cache_entries(self, market=None, object_type=None, limit=100, include_metadata=False):
        self.calls.append(("list", market, object_type, limit, include_metadata))
        entry = {"cache_key": "cache1"}
        if include_metadata:
            entry["metadata"] = {"large": "payload"}
        return [entry]

    def preview_factor_cache_cleanup(self, market=None, include_recent_days=0, limit=100):
        self.calls.append(("preview_cleanup", market, limit))
        return {"mode": "preview"}

    def apply_factor_cache_cleanup(self, market=None, include_recent_days=0, limit=100):
        self.calls.append(("apply_cleanup", market, limit))
        return {"mode": "apply"}

    def preview_orphan_file_cleanup(self, limit=100, min_age_seconds=3600):
        self.calls.append(("preview_orphan", limit, min_age_seconds))
        return {"mode": "orphan_preview"}

    def apply_orphan_file_cleanup(self, limit=100, min_age_seconds=3600):
        self.calls.append(("apply_orphan", limit, min_age_seconds))
        return {"mode": "orphan_apply"}


class _FakeFeatureService:
    def compute_features_from_cache(self, **kwargs):
        return {"close": object()}


class _FakeGroupService:
    def get_group_tickers(self, group_id, market=None):
        return ["sh.600000"]


class _FakeExecutor:
    def __init__(self):
        self.task_type = None
        self.params = None

    def submit(self, task_type, fn, params, timeout, source):
        self.task_type = task_type
        self.params = params
        return "task_cache_warmup"


def _run_async(coro):
    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
