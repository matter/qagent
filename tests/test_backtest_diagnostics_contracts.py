import unittest
from unittest.mock import patch

import pandas as pd

from backend.services.backtest_engine import BacktestResult
from backend.services.backtest_service import BacktestService


class BacktestDiagnosticsContractTests(unittest.TestCase):
    def test_rebalance_delta_reports_position_changes_and_turnover(self):
        diag = BacktestService._build_rebalance_diagnostics(
            date_key="2026-04-10",
            positions_before={"AAPL": 0.5, "MSFT": 0.5},
            positions_after={"MSFT": 0.4, "NVDA": 0.6},
            strategy_diagnostics={"candidate_pool": ["MSFT", "NVDA"]},
        )

        self.assertEqual(diag["date"], "2026-04-10")
        self.assertEqual(diag["positions_before"], {"AAPL": 0.5, "MSFT": 0.5})
        self.assertEqual(diag["positions_after"], {"MSFT": 0.4, "NVDA": 0.6})
        self.assertEqual(diag["added"], ["NVDA"])
        self.assertEqual(diag["removed"], ["AAPL"])
        self.assertEqual(diag["increased"], [])
        self.assertEqual(diag["decreased"], ["MSFT"])
        self.assertAlmostEqual(diag["turnover"], 1.2)
        self.assertEqual(diag["candidate_pool"], ["MSFT", "NVDA"])

    def test_list_summary_strips_heavy_diagnostics(self):
        summary = {
            "total_return": 0.12,
            "sharpe_ratio": 2.3,
            "rebalance_diagnostics": [{"date": "2026-04-10"}],
            "leakage_warnings": [{"model_id": "m1"}],
            "trade_diagnostics": {"by_reason": {}},
        }

        lightweight = BacktestService._list_summary(summary)

        self.assertEqual(lightweight["total_return"], 0.12)
        self.assertEqual(lightweight["sharpe_ratio"], 2.3)
        self.assertNotIn("rebalance_diagnostics", lightweight)
        self.assertNotIn("leakage_warnings", lightweight)
        self.assertIn("has_rebalance_diagnostics", lightweight)
        self.assertTrue(lightweight["has_rebalance_diagnostics"])

    def test_get_backtest_promotes_rebalance_diagnostics_to_top_level(self):
        svc = BacktestService()
        conn = _BacktestDetailConnection(
            summary={
                "total_return": 0.12,
                "rebalance_diagnostics": [
                    {
                        "date": "2026-04-10",
                        "lane_counts": {"core": 10},
                        "market_state": "risk_on",
                    }
                ],
            }
        )

        with patch("backend.services.backtest_service.get_connection", return_value=conn):
            detail = svc.get_backtest("bt_diag", market="CN")

        self.assertEqual(detail["rebalance_diagnostics"][0]["date"], "2026-04-10")
        self.assertEqual(detail["rebalance_diagnostics"][0]["lane_counts"], {"core": 10})

    def test_get_rebalance_diagnostics_returns_paginated_payload(self):
        svc = BacktestService()
        conn = _BacktestDiagnosticsConnection(
            diagnostics=[
                {"date": "2026-04-10", "lane_counts": {"core": 10}},
                {"date": "2026-04-13", "lane_counts": {"core": 8}},
                {"date": "2026-04-14", "lane_counts": {"core": 6}},
            ]
        )

        with patch("backend.services.backtest_service.get_connection", return_value=conn):
            payload = svc.get_rebalance_diagnostics("bt_diag", market="CN", offset=1, limit=1)

        self.assertEqual(payload["backtest_id"], "bt_diag")
        self.assertEqual(payload["market"], "CN")
        self.assertEqual(payload["total"], 3)
        self.assertEqual(payload["offset"], 1)
        self.assertEqual(payload["limit"], 1)
        self.assertEqual(payload["items"], [{"date": "2026-04-13", "lane_counts": {"core": 8}}])

    def test_batch_predict_reuses_feature_matrix_for_models_sharing_feature_set(self):
        svc = BacktestService()
        svc._model_service = _SharedFeatureModelService()

        result = svc._batch_predict_all_dates(
            ["model_a", "model_b"],
            tickers=["sh.600000", "sh.600001"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            rebalance_days=["2024-01-03"],
            market="CN",
        )

        self.assertEqual(svc._model_service.feature_compute_calls, 1)
        self.assertEqual(svc._model_service.load_model_calls, ["model_a", "model_b"])
        self.assertIn("model_a", result["2024-01-03"])
        self.assertIn("model_b", result["2024-01-03"])

    def test_save_result_persists_reproducibility_fingerprint(self):
        svc = BacktestService()
        result = BacktestResult(
            config={"market": "CN"},
            dates=["2024-01-02", "2024-01-03"],
            nav=[1000.0, 1010.0],
            benchmark_nav=[1000.0, 1005.0],
            drawdown=[0.0, 0.0],
            total_return=0.01,
            annual_return=0.1,
            annual_volatility=0.2,
            max_drawdown=0.0,
            sharpe_ratio=1.2,
            calmar_ratio=0.0,
            sortino_ratio=1.3,
            win_rate=1.0,
            profit_loss_ratio=1.0,
            total_trades=1,
            annual_turnover=2.0,
            total_cost=1.0,
            monthly_returns=[],
            trades=[],
            trade_diagnostics={},
        )
        conn = _BacktestSaveConnection()

        with (
            patch("backend.services.backtest_service.get_connection", return_value=conn),
            patch.object(
                svc,
                "_build_reproducibility_fingerprint",
                return_value={
                    "hash": "fp_hash",
                    "strategy": {"source_hash": "source_hash"},
                },
            ),
        ):
            svc._save_result(
                bt_id="bt1",
                market="CN",
                strategy_id="strategy_cn",
                config={"market": "CN", "universe_group_id": "cn_group"},
                result=result,
                result_level="exploratory",
            )

        summary = conn.insert_params[4]
        self.assertEqual(summary["reproducibility_fingerprint"]["hash"], "fp_hash")
        self.assertEqual(
            BacktestService._list_summary(summary)["reproducibility_hash"],
            "fp_hash",
        )

    def test_combine_portfolio_legs_builds_weighted_nav_and_leg_summary(self):
        svc = BacktestService()
        base = BacktestResult(
            config={"market": "CN"},
            dates=["2024-01-02", "2024-01-03"],
            nav=[1000.0, 1100.0],
            benchmark_nav=[1000.0, 1000.0],
            drawdown=[0.0, 0.0],
            total_return=0.1,
            annual_return=0.0,
            annual_volatility=0.0,
            max_drawdown=0.0,
            sharpe_ratio=0.0,
            calmar_ratio=0.0,
            sortino_ratio=0.0,
            win_rate=0.0,
            profit_loss_ratio=0.0,
            total_trades=0,
            annual_turnover=0.0,
            total_cost=0.0,
            monthly_returns=[],
            trades=[],
            trade_diagnostics={},
        )
        overlay = BacktestResult(
            config={"market": "CN"},
            dates=["2024-01-02", "2024-01-03"],
            nav=[1000.0, 900.0],
            benchmark_nav=[1000.0, 1000.0],
            drawdown=[0.0, -0.1],
            total_return=-0.1,
            annual_return=0.0,
            annual_volatility=0.0,
            max_drawdown=-0.1,
            sharpe_ratio=0.0,
            calmar_ratio=0.0,
            sortino_ratio=0.0,
            win_rate=0.0,
            profit_loss_ratio=0.0,
            total_trades=2,
            annual_turnover=1.0,
            total_cost=2.0,
            monthly_returns=[],
            trades=[{"date": "2024-01-03", "ticker": "sh.600000"}],
            trade_diagnostics={"by_reason": {}},
        )

        combined = svc._combine_portfolio_legs(
            base_result=base,
            overlay_result=overlay,
            base_weight=0.65,
            overlay_weight=0.35,
            portfolio_config={"mode": "base_plus_overlay"},
        )

        self.assertEqual(combined.nav, [1000.0, 1030.0])
        self.assertEqual(combined.total_return, 0.03)
        self.assertEqual(combined.total_trades, 2)
        self.assertEqual(combined.trade_diagnostics["portfolio_legs"]["base"]["weight"], 0.65)
        self.assertEqual(
            combined.trade_diagnostics["portfolio_legs"]["overlay"]["contribution_return"],
            -0.035,
        )


class _BacktestSaveConnection:
    def __init__(self):
        self.insert_params = None

    def execute(self, sql, params=None):
        if str(sql).lstrip().upper().startswith("INSERT INTO BACKTEST_RESULTS"):
            import json

            parsed = list(params)
            parsed[3] = json.loads(parsed[3])
            parsed[4] = json.loads(parsed[4])
            self.insert_params = parsed
        return self


class _BacktestDetailConnection:
    def __init__(self, summary):
        self.summary = summary

    def execute(self, sql, params=None):
        import json

        self.row = (
            "bt_diag",
            "CN",
            "strategy_cn",
            json.dumps({"start_date": "2026-04-01", "end_date": "2026-04-30"}),
            json.dumps(self.summary),
            json.dumps({"2026-04-10": 1.0}),
            json.dumps({}),
            json.dumps({}),
            json.dumps([]),
            0,
            "exploratory",
            "2026-05-02 12:00:00",
            json.dumps([]),
        )
        return self

    def fetchone(self):
        return self.row


class _BacktestDiagnosticsConnection:
    def __init__(self, diagnostics):
        self.diagnostics = diagnostics

    def execute(self, sql, params=None):
        import json

        self.row = ("CN", json.dumps({"rebalance_diagnostics": self.diagnostics}))
        return self

    def fetchone(self):
        return self.row


class _SharedFeatureModelService:
    def __init__(self):
        self.feature_compute_calls = 0
        self.load_model_calls = []
        self._feature_service = self

    def get_model(self, model_id, market=None):
        return {"id": model_id, "feature_set_id": "shared_fs"}

    def load_model(self, model_id, market=None):
        self.load_model_calls.append(model_id)
        return _LinearPredictModel(model_id)

    def compute_features_from_cache(self, fs_id, tickers, start_date, end_date, market=None):
        self.feature_compute_calls += 1
        index = pd.to_datetime(["2024-01-02", "2024-01-03"])
        return {
            "close": pd.DataFrame(
                {
                    "sh.600000": [10.0, 11.0],
                    "sh.600001": [20.0, 21.0],
                },
                index=index,
            )
        }

    def _break_prediction_ties(self, preds):
        return preds


class _LinearPredictModel:
    def __init__(self, model_id):
        self.model_id = model_id

    def predict(self, X):
        base = 1.0 if self.model_id == "model_a" else 2.0
        return pd.Series(base + X["close"].astype(float), index=X.index)


if __name__ == "__main__":
    unittest.main()
