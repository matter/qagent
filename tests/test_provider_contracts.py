import unittest
from datetime import date

from backend.providers.base import DataProvider
from backend.providers.registry import available_providers, get_provider, register_provider
from backend.providers.baostock_provider import BaoStockProvider
from backend.providers.yfinance_provider import YFinanceProvider


class _FakeBaoResult:
    def __init__(self, fields, rows, error_code="0", error_msg=""):
        self.fields = fields
        self._rows = rows
        self.error_code = error_code
        self.error_msg = error_msg
        self._index = -1

    def next(self):
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self):
        return self._rows[self._index]


class _FakeBaoClient:
    def __init__(self):
        self.logged_out = False

    def login(self):
        return _FakeBaoResult([], [])

    def logout(self):
        self.logged_out = True

    def query_all_stock(self, day=None):
        return _FakeBaoResult(
            ["code", "tradeStatus", "code_name"],
            [
                ["sh.600000", "1", "浦发银行"],
                ["sz.000001", "1", "平安银行"],
                ["sh.000001", "1", "上证指数"],
            ],
        )

    def query_history_k_data_plus(self, code, fields, start_date, end_date, frequency, adjustflag):
        self.last_history_request = {
            "code": code,
            "fields": fields,
            "start_date": start_date,
            "end_date": end_date,
            "frequency": frequency,
            "adjustflag": adjustflag,
        }
        return _FakeBaoResult(
            fields.split(","),
            [
                [
                    "2024-01-02",
                    code,
                    "10.1",
                    "10.5",
                    "10.0",
                    "10.4",
                    "10.0",
                    "12345",
                    "123456.7",
                    "2",
                    "1.2",
                    "1",
                    "2.3",
                    "0",
                ]
            ],
        )


class ProviderContractTests(unittest.TestCase):
    def test_registry_resolves_market_provider(self):
        self.assertIsInstance(get_provider("US"), YFinanceProvider)
        self.assertIsInstance(get_provider("CN"), BaoStockProvider)

    def test_registry_supports_provider_registration_without_branching(self):
        register_provider("US", "fake_provider", _FakeProvider)

        provider = get_provider("US", "fake_provider")

        self.assertIsInstance(provider, _FakeProvider)
        self.assertIn("fake_provider", available_providers("US"))

    def test_provider_capability_contract_marks_free_sources_as_research_grade(self):
        yfinance = YFinanceProvider()
        baostock = BaoStockProvider(client=_FakeBaoClient())

        yf_caps = yfinance.capabilities()
        bao_caps = baostock.capabilities()

        self.assertEqual(yf_caps["provider"], "yfinance")
        self.assertEqual(yf_caps["market"], "US")
        self.assertEqual(yf_caps["cost"], "free")
        self.assertEqual(yf_caps["quality_level"], "exploratory")
        self.assertFalse(yf_caps["pit_supported"])
        self.assertIn("daily_bars", yf_caps["datasets"])
        self.assertEqual(bao_caps["provider"], "baostock")
        self.assertEqual(bao_caps["market"], "CN")
        self.assertFalse(bao_caps["pit_supported"])
        self.assertIn("trade_status", bao_caps["datasets"])

    def test_baostock_stock_list_is_market_scoped_and_filters_indices(self):
        provider = BaoStockProvider(client=_FakeBaoClient())

        df = provider.get_stock_list()

        self.assertEqual(df["market"].unique().tolist(), ["CN"])
        self.assertEqual(df["ticker"].tolist(), ["sh.600000", "sz.000001"])
        self.assertEqual(df["exchange"].tolist(), ["SH", "SZ"])
        self.assertEqual(df["status"].tolist(), ["active", "active"])

    def test_baostock_daily_bars_are_normalized(self):
        client = _FakeBaoClient()
        provider = BaoStockProvider(client=client)

        df = provider.get_daily_bars(["sh.600000"], date(2024, 1, 2), date(2024, 1, 3))

        self.assertEqual(client.last_history_request["adjustflag"], "2")
        self.assertEqual(df.loc[0, "market"], "CN")
        self.assertEqual(df.loc[0, "ticker"], "sh.600000")
        self.assertEqual(float(df.loc[0, "open"]), 10.1)
        self.assertEqual(int(df.loc[0, "volume"]), 12345)
        self.assertEqual(float(df.loc[0, "adj_factor"]), 1.0)


class _FakeProvider(DataProvider):
    def get_stock_list(self):
        raise NotImplementedError

    def get_daily_bars(self, tickers, start, end):
        raise NotImplementedError

    def get_index_data(self, symbol, start, end):
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
