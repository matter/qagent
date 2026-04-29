from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
FRONTEND_SRC = ROOT / "frontend" / "src"


class FrontendMarketScopeContractsTests(unittest.TestCase):
    def test_frontend_exposes_shared_market_scope_contract(self):
        api_index = (FRONTEND_SRC / "api" / "index.ts").read_text()
        client = (FRONTEND_SRC / "api" / "client.ts").read_text()
        app = (FRONTEND_SRC / "App.tsx").read_text()
        selector_path = FRONTEND_SRC / "components" / "MarketScopeSelector.tsx"

        self.assertIn('export type Market = "US" | "CN";', api_index)
        self.assertIn("getActiveMarket", api_index)
        self.assertIn("setActiveMarket", api_index)

        self.assertTrue(selector_path.exists())
        selector = selector_path.read_text()
        self.assertIn("Segmented", selector)
        self.assertIn("localStorage", selector)
        self.assertIn("US", selector)
        self.assertIn("CN", selector)

        self.assertIn("client.interceptors.request.use", client)
        self.assertIn("MARKET_SCOPED_PATHS", client)
        self.assertIn("market", client)

        self.assertIn("MarketScopeSelector", app)

    def test_market_aware_pages_expose_human_validation_cues(self):
        app = (FRONTEND_SRC / "App.tsx").read_text()
        market_page = (FRONTEND_SRC / "pages" / "MarketPage.tsx").read_text()
        data_page = (FRONTEND_SRC / "pages" / "DataManagePage.tsx").read_text()
        train_panel = (FRONTEND_SRC / "components" / "model" / "TrainConfigPanel.tsx").read_text()
        model_list = (FRONTEND_SRC / "components" / "model" / "ModelList.tsx").read_text()

        self.assertIn("subscribeActiveMarket", app)
        self.assertIn("key={marketScope}", app)

        self.assertIn("DEFAULT_TICKER_BY_MARKET", market_page)
        self.assertIn("getActiveMarket", market_page)
        self.assertIn("sh.600000", market_page)

        self.assertIn("status.market", data_page)
        self.assertIn("latest_trading_day", data_page)
        self.assertIn('dataIndex: "market"', data_page)

        self.assertIn("objectiveType", train_panel)
        for objective in ("regression", "classification", "ranking", "pairwise", "listwise"):
            self.assertIn(objective, train_panel)
        self.assertIn("ranking_config", train_panel)
        self.assertIn("objective_type", train_panel)

        for metric in ("ndcg@", "rank_ic", "top_k"):
            self.assertIn(metric, model_list)
        self.assertIn('dataIndex: "market"', model_list)


if __name__ == "__main__":
    unittest.main()
