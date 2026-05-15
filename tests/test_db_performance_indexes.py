import unittest

from backend import db


class DbPerformanceIndexContractTests(unittest.TestCase):
    def test_hot_legacy_tables_have_list_and_lookup_indexes(self):
        ddl_blob = "\n".join(db._PERFORMANCE_INDEX_DDLS)

        required_fragments = [
            "backtest_results(market, created_at)",
            "backtest_results(market, strategy_id, created_at)",
            "models(market, created_at)",
            "factor_eval_results(market, factor_id, created_at)",
            "signal_runs(market, created_at)",
            "daily_bars(market, ticker, date)",
        ]
        for fragment in required_fragments:
            self.assertIn(fragment, ddl_blob)

    def test_large_factor_cache_index_is_not_created_on_startup(self):
        ddl_blob = "\n".join(db._PERFORMANCE_INDEX_DDLS)

        self.assertNotIn("factor_values_cache(market, factor_id, date)", ddl_blob)


if __name__ == "__main__":
    unittest.main()
