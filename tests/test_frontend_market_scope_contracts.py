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


if __name__ == "__main__":
    unittest.main()
