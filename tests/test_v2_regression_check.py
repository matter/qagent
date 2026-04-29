from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "v2_regression_check.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("v2_regression_check", SCRIPT)
    if spec is None or spec.loader is None:
        raise AssertionError("Unable to load v2_regression_check.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self.text = str(payload)

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self):
        self.urls: list[str] = []

    def get(self, url: str, timeout: int = 10):
        self.urls.append(url)
        if url.endswith("/api/health"):
            return _FakeResponse({"status": "ok"})
        if url.endswith("/api/data/status"):
            return _FakeResponse({"market": "US", "stock_count": 1})
        if url.endswith("/api/stocks/search?q=AAPL&limit=1"):
            return _FakeResponse([{"market": "US", "ticker": "AAPL"}])
        return _FakeResponse({"detail": "not found"}, status_code=404)


class V2RegressionCheckTests(unittest.TestCase):
    def test_summary_allows_skipped_optional_checks(self):
        module = load_script_module()
        results = [
            module.CheckResult("required", "passed", "ok"),
            module.CheckResult("optional", "skipped", "not configured"),
        ]

        summary = module.summarize_results(results)

        self.assertEqual(summary["overall_status"], "passed")
        self.assertEqual(summary["passed"], 1)
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(module.exit_code_for_results(results), 0)

    def test_summary_fails_when_required_check_fails(self):
        module = load_script_module()
        results = [
            module.CheckResult("required", "failed", "bad default"),
            module.CheckResult("optional", "skipped", "not configured"),
        ]

        summary = module.summarize_results(results)

        self.assertEqual(summary["overall_status"], "failed")
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(module.exit_code_for_results(results), 1)

    def test_api_default_market_check_uses_old_no_market_shape(self):
        module = load_script_module()
        session = _FakeSession()

        result = module.check_api_us_defaults("http://example.test", session=session)

        self.assertEqual(result.status, "passed")
        self.assertIn("http://example.test/api/data/status", session.urls)
        self.assertIn("http://example.test/api/stocks/search?q=AAPL&limit=1", session.urls)
        self.assertTrue(all("market=" not in url for url in session.urls))

    def test_cn_provider_failure_check_verifies_us_through_api(self):
        module = load_script_module()
        session = _FakeSession()

        result = module.check_cn_provider_failure_does_not_block_us("http://example.test", session=session)

        self.assertEqual(result.status, "passed")
        self.assertIn("http://example.test/api/data/status", session.urls)


if __name__ == "__main__":
    unittest.main()
