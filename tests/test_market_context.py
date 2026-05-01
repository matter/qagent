import tempfile
import textwrap
import unittest
from pathlib import Path

from backend.config import load_settings
from backend.services.market_context import (
    get_default_benchmark,
    get_default_calendar,
    get_default_group,
    get_default_provider,
    normalize_market,
)


class MarketContextContractTests(unittest.TestCase):
    def test_normalize_market_defaults_and_rejects_unknown_values(self):
        self.assertEqual(normalize_market(None), "US")
        self.assertEqual(normalize_market("us"), "US")
        self.assertEqual(normalize_market("cn"), "CN")

        with self.assertRaisesRegex(ValueError, "Unsupported market"):
            normalize_market("HK")

    def test_legacy_config_still_builds_us_and_cn_market_defaults(self):
        settings = self._load_temp_settings(
            """
            data:
              provider: yfinance
            backtest:
              default_benchmark: SPY
            market:
              calendar: NYSE
            """
        )

        self.assertEqual(get_default_provider("US", settings), "yfinance")
        self.assertEqual(get_default_calendar("US", settings), "NYSE")
        self.assertEqual(get_default_benchmark("US", settings), "SPY")
        self.assertEqual(get_default_group("US", settings), "us_all_market")
        self.assertEqual(get_default_provider("CN", settings), "baostock")
        self.assertEqual(get_default_calendar("CN", settings), "XSHG")
        self.assertEqual(get_default_benchmark("CN", settings), "sh.000300")
        self.assertEqual(get_default_group("CN", settings), "cn_a_core_indices_union")

    def test_explicit_market_config_overrides_defaults(self):
        settings = self._load_temp_settings(
            """
            markets:
              US:
                provider: custom_us
                calendar: NYSE
                benchmark: QQQ
                default_group: us_custom
              CN:
                provider: custom_cn
                calendar: XSHG
                benchmark: sh.000905
                default_group: cn_custom
            """
        )

        self.assertEqual(get_default_provider("US", settings), "custom_us")
        self.assertEqual(get_default_benchmark("US", settings), "QQQ")
        self.assertEqual(get_default_group("US", settings), "us_custom")
        self.assertEqual(get_default_provider("CN", settings), "custom_cn")
        self.assertEqual(get_default_benchmark("CN", settings), "sh.000905")
        self.assertEqual(get_default_group("CN", settings), "cn_custom")

    def _load_temp_settings(self, content: str):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yaml"
            path.write_text(textwrap.dedent(content), encoding="utf-8")
            return load_settings(path)


if __name__ == "__main__":
    unittest.main()
