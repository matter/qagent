"""Backtest orchestration service.

Orchestrates the full pipeline:
  strategy -> factors -> model predictions -> signals -> position sizing -> BacktestEngine
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.backtest_engine import BacktestConfig, BacktestEngine, BacktestResult
from backend.services.factor_engine import FactorEngine
from backend.services.group_service import GroupService
from backend.services.model_service import ModelService
from backend.services.strategy_service import StrategyService
from backend.strategies.base import StrategyContext
from backend.strategies.loader import load_strategy_from_code

log = get_logger(__name__)


class BacktestService:
    """Run, persist, and query backtests."""

    def __init__(self) -> None:
        self._strategy_service = StrategyService()
        self._factor_engine = FactorEngine()
        self._model_service = ModelService()
        self._group_service = GroupService()
        self._backtest_engine = BacktestEngine()

    # ------------------------------------------------------------------
    # Run backtest
    # ------------------------------------------------------------------

    def run_backtest(
        self,
        strategy_id: str,
        config_dict: dict,
        universe_group_id: str,
    ) -> dict:
        """Full backtest pipeline orchestrator.

        Steps
        -----
        1.  Load strategy source code and instantiate via loader.
        2.  Resolve universe tickers from group.
        3.  Build BacktestConfig.
        4.  Load OHLCV price data for date range.
        5.  Compute required factors via FactorEngine.
        6.  For each rebalance date, compute model predictions + build
            StrategyContext + call generate_signals + apply position sizing.
        7.  Run BacktestEngine.run(weights, config).
        8.  Determine result_level.
        9.  Save to backtest_results table.
        10. Return result dict.
        """
        # ---- 1. Load strategy ----
        strategy_def = self._strategy_service.get_strategy(strategy_id)
        strategy_instance = load_strategy_from_code(strategy_def["source_code"])

        log.info(
            "backtest_service.start",
            strategy=strategy_def["name"],
            version=strategy_def["version"],
        )

        # ---- 2. Resolve universe tickers ----
        tickers = self._group_service.get_group_tickers(universe_group_id)
        if not tickers:
            raise ValueError(f"Universe group '{universe_group_id}' has no members")

        # ---- 3. Build config ----
        bt_config = BacktestConfig(
            initial_capital=config_dict.get("initial_capital", 1_000_000),
            start_date=config_dict.get("start_date", "2020-01-01"),
            end_date=config_dict.get("end_date", "2024-12-31"),
            benchmark=config_dict.get("benchmark", "SPY"),
            commission_rate=config_dict.get("commission_rate", 0.001),
            slippage_rate=config_dict.get("slippage_rate", 0.001),
            max_positions=config_dict.get("max_positions", 50),
            rebalance_freq=config_dict.get("rebalance_freq", "monthly"),
        )

        position_sizing = strategy_def.get("position_sizing", "equal_weight")
        max_position_pct = config_dict.get("max_position_pct", 0.10)

        start_str = str(bt_config.start_date)
        end_str = str(bt_config.end_date)

        # ---- 4. Load OHLCV price data ----
        prices_close, prices_open = self._backtest_engine._load_prices(
            tickers, start_str, end_str
        )
        if prices_close.empty:
            raise ValueError("No price data available for the given tickers and date range")

        # ---- 5. Compute required factors ----
        required_factors = strategy_def.get("required_factors", [])
        factor_data: dict[str, pd.DataFrame] = {}

        if required_factors:
            # We need to resolve factor IDs from names
            factor_id_map = self._resolve_factor_ids(required_factors)
            for factor_name, factor_id in factor_id_map.items():
                try:
                    df = self._factor_engine.compute_factor(
                        factor_id, tickers, start_str, end_str
                    )
                    if not df.empty:
                        factor_data[factor_name] = df
                except Exception as exc:
                    log.warning(
                        "backtest_service.factor_failed",
                        factor_name=factor_name,
                        error=str(exc),
                    )

        # ---- 6. Build signals for each trading day ----
        required_models = strategy_def.get("required_models", [])
        all_trading_days = sorted(prices_close.index)
        rebalance_dates = self._backtest_engine._get_rebalance_dates(
            all_trading_days, bt_config.rebalance_freq
        )

        # Build a prices DataFrame with MultiIndex columns (field, ticker)
        # for the StrategyContext
        prices_multi = self._build_prices_multi(prices_close, prices_open, tickers)

        # Create weight signals: DataFrame(index=dates, columns=tickers)
        all_weights = pd.DataFrame(0.0, index=prices_close.index, columns=tickers)

        log.info(
            "backtest_service.signals",
            trading_days=len(all_trading_days),
            rebalance_dates=len(rebalance_dates),
            factors=len(factor_data),
            models=len(required_models),
        )

        for trade_date in all_trading_days:
            trade_ts = pd.Timestamp(trade_date)
            if trade_ts not in rebalance_dates:
                # Carry forward previous weights
                idx = all_trading_days.index(trade_date)
                if idx > 0:
                    prev_date = all_trading_days[idx - 1]
                    all_weights.loc[trade_ts] = all_weights.loc[pd.Timestamp(prev_date)]
                continue

            # Build model predictions for this date
            model_predictions: dict[str, pd.Series] = {}
            for model_id in required_models:
                try:
                    preds = self._model_service.predict(
                        model_id=model_id,
                        tickers=tickers,
                        date=str(trade_ts.date()),
                    )
                    if not preds.empty:
                        model_predictions[model_id] = preds
                except Exception as exc:
                    log.warning(
                        "backtest_service.model_predict_failed",
                        model_id=model_id,
                        date=str(trade_ts.date()),
                        error=str(exc),
                    )

            # Build context
            context = StrategyContext(
                prices=prices_multi.loc[:trade_ts],
                factor_values=factor_data,
                model_predictions=model_predictions,
                current_date=trade_ts,
            )

            # Generate signals
            try:
                raw_signals = strategy_instance.generate_signals(context)
            except Exception as exc:
                log.warning(
                    "backtest_service.signal_failed",
                    date=str(trade_ts.date()),
                    error=str(exc),
                )
                # Carry forward previous weights
                idx = all_trading_days.index(trade_date)
                if idx > 0:
                    prev_date = all_trading_days[idx - 1]
                    all_weights.loc[trade_ts] = all_weights.loc[pd.Timestamp(prev_date)]
                continue

            if raw_signals.empty:
                # No signals -- go to cash (all zeros)
                all_weights.loc[trade_ts] = 0.0
                continue

            # Apply position sizing
            weights = self._apply_position_sizing(
                raw_signals,
                position_sizing,
                bt_config.max_positions,
                max_position_pct,
            )

            # Write weights for this date
            for ticker, w in weights.items():
                if ticker in all_weights.columns:
                    all_weights.loc[trade_ts, ticker] = w

        # ---- 7. Run BacktestEngine ----
        result = self._backtest_engine.run(all_weights, bt_config)

        # ---- 8. Determine result_level ----
        # For now, always 'exploratory' since we use yfinance
        result_level = "exploratory"

        # ---- 9. Save to backtest_results table ----
        bt_id = uuid.uuid4().hex[:12]
        self._save_result(
            bt_id=bt_id,
            strategy_id=strategy_id,
            config=bt_config.to_dict(),
            result=result,
            result_level=result_level,
        )

        # ---- 10. Return result ----
        result_dict = result.to_dict()
        result_dict["backtest_id"] = bt_id
        result_dict["strategy_id"] = strategy_id
        result_dict["strategy_name"] = strategy_def["name"]
        result_dict["result_level"] = result_level
        result_dict["universe_group_id"] = universe_group_id

        log.info(
            "backtest_service.done",
            backtest_id=bt_id,
            total_return=result.total_return,
            sharpe=result.sharpe_ratio,
        )
        return result_dict

    # ------------------------------------------------------------------
    # CRUD for backtest results
    # ------------------------------------------------------------------

    def list_backtests(self, strategy_id: str | None = None) -> list[dict]:
        """List backtest results (summary only, no heavy series)."""
        conn = get_connection()
        if strategy_id:
            rows = conn.execute(
                """SELECT id, strategy_id, config, summary, trade_count,
                          result_level, created_at
                   FROM backtest_results
                   WHERE strategy_id = ?
                   ORDER BY created_at DESC""",
                [strategy_id],
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, strategy_id, config, summary, trade_count,
                          result_level, created_at
                   FROM backtest_results
                   ORDER BY created_at DESC"""
            ).fetchall()

        results = []
        for r in rows:
            results.append({
                "id": r[0],
                "strategy_id": r[1],
                "config": _parse_json(r[2]),
                "summary": _parse_json(r[3]),
                "trade_count": r[4],
                "result_level": r[5],
                "created_at": str(r[6]) if r[6] else None,
            })
        return results

    def get_backtest(self, backtest_id: str) -> dict:
        """Return full backtest result including all series data."""
        conn = get_connection()
        row = conn.execute(
            """SELECT id, strategy_id, config, summary,
                      nav_series, benchmark_nav, drawdown_series,
                      monthly_returns, trade_count, result_level, created_at
               FROM backtest_results
               WHERE id = ?""",
            [backtest_id],
        ).fetchone()

        if row is None:
            raise ValueError(f"Backtest {backtest_id} not found")

        return {
            "id": row[0],
            "strategy_id": row[1],
            "config": _parse_json(row[2]),
            "summary": _parse_json(row[3]),
            "nav_series": _parse_json(row[4]),
            "benchmark_nav": _parse_json(row[5]),
            "drawdown_series": _parse_json(row[6]),
            "monthly_returns": _parse_json(row[7]),
            "trade_count": row[8],
            "result_level": row[9],
            "created_at": str(row[10]) if row[10] else None,
        }

    def delete_backtest(self, backtest_id: str) -> None:
        """Delete a backtest result."""
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM backtest_results WHERE id = ?", [backtest_id]
        ).fetchone()
        if row is None:
            raise ValueError(f"Backtest {backtest_id} not found")

        conn.execute("DELETE FROM backtest_results WHERE id = ?", [backtest_id])
        log.info("backtest_service.deleted", backtest_id=backtest_id)

    def compare_strategies(self, backtest_ids: list[str]) -> dict:
        """Compare multiple backtest results.

        Returns aligned NAV curves and a metric comparison table.
        """
        if not backtest_ids or len(backtest_ids) < 2:
            raise ValueError("At least 2 backtest IDs are required for comparison")

        results = []
        for bt_id in backtest_ids:
            results.append(self.get_backtest(bt_id))

        # Build comparison metrics
        metrics_table = []
        for r in results:
            summary = r["summary"]
            metrics_table.append({
                "backtest_id": r["id"],
                "strategy_id": r["strategy_id"],
                "total_return": summary.get("total_return"),
                "annual_return": summary.get("annual_return"),
                "annual_volatility": summary.get("annual_volatility"),
                "max_drawdown": summary.get("max_drawdown"),
                "sharpe_ratio": summary.get("sharpe_ratio"),
                "calmar_ratio": summary.get("calmar_ratio"),
                "sortino_ratio": summary.get("sortino_ratio"),
                "win_rate": summary.get("win_rate"),
                "profit_loss_ratio": summary.get("profit_loss_ratio"),
                "total_trades": summary.get("total_trades"),
                "annual_turnover": summary.get("annual_turnover"),
                "total_cost": summary.get("total_cost"),
                "result_level": r["result_level"],
            })

        # Build aligned NAV curves
        nav_curves: dict[str, dict] = {}
        for r in results:
            nav_data = r.get("nav_series", {})
            nav_curves[r["id"]] = nav_data

        return {
            "metrics": metrics_table,
            "nav_curves": nav_curves,
        }

    # ------------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_position_sizing(
        raw_signals: pd.DataFrame,
        method: str,
        max_positions: int,
        max_position_pct: float = 0.10,
    ) -> dict[str, float]:
        """Apply position sizing to raw signals and return ticker->weight dict.

        Args:
            raw_signals: DataFrame with index=ticker, columns=[signal, weight, strength].
            method: One of 'equal_weight', 'signal_weight', 'max_position'.
            max_positions: Maximum number of positions.
            max_position_pct: Maximum weight per position (for 'max_position' method).

        Returns:
            Dict mapping ticker -> target weight (summing to ~1.0).
        """
        # Filter to buy signals only
        buy_mask = raw_signals["signal"] == 1
        buys = raw_signals[buy_mask].copy()

        if buys.empty:
            return {}

        # Enforce max_positions by taking top N by strength
        if len(buys) > max_positions:
            buys = buys.nlargest(max_positions, "strength")

        if method == "equal_weight":
            n = len(buys)
            weights = {ticker: 1.0 / n for ticker in buys.index}

        elif method == "signal_weight":
            # Weight proportional to signal strength
            strengths = buys["strength"].astype(float)
            total = strengths.sum()
            if total > 0:
                weights = {ticker: float(s / total) for ticker, s in strengths.items()}
            else:
                n = len(buys)
                weights = {ticker: 1.0 / n for ticker in buys.index}

        elif method == "max_position":
            # Start with signal-weighted, then cap at max_position_pct
            strengths = buys["strength"].astype(float)
            total = strengths.sum()
            if total > 0:
                raw_weights = {ticker: float(s / total) for ticker, s in strengths.items()}
            else:
                n = len(buys)
                raw_weights = {ticker: 1.0 / n for ticker in buys.index}

            # Iterative capping: cap each weight at max_position_pct and
            # redistribute excess to uncapped positions.
            weights = _cap_weights(raw_weights, max_position_pct)

        else:
            # Default to equal weight
            n = len(buys)
            weights = {ticker: 1.0 / n for ticker in buys.index}

        return weights

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_factor_ids(self, factor_names: list[str]) -> dict[str, str]:
        """Resolve factor names to factor IDs.

        Returns dict mapping factor_name -> factor_id.
        Looks up the latest version of each factor by name.
        """
        conn = get_connection()
        result: dict[str, str] = {}

        for name in factor_names:
            row = conn.execute(
                """SELECT id FROM factors
                   WHERE name = ?
                   ORDER BY version DESC
                   LIMIT 1""",
                [name],
            ).fetchone()
            if row:
                result[name] = row[0]
            else:
                log.warning("backtest_service.factor_not_found", name=name)

        return result

    @staticmethod
    def _build_prices_multi(
        prices_close: pd.DataFrame,
        prices_open: pd.DataFrame,
        tickers: list[str],
    ) -> pd.DataFrame:
        """Build a MultiIndex-column DataFrame with (field, ticker) columns.

        Fields: close, open.
        """
        frames = {}
        for ticker in tickers:
            if ticker in prices_close.columns:
                frames[("close", ticker)] = prices_close[ticker]
            if ticker in prices_open.columns:
                frames[("open", ticker)] = prices_open[ticker]

        if not frames:
            return pd.DataFrame()

        result = pd.DataFrame(frames)
        result.columns = pd.MultiIndex.from_tuples(result.columns, names=["field", "ticker"])
        return result

    def _save_result(
        self,
        bt_id: str,
        strategy_id: str,
        config: dict,
        result: BacktestResult,
        result_level: str,
    ) -> None:
        """Persist backtest result to DuckDB."""
        conn = get_connection()
        now = datetime.utcnow()

        summary = {
            "total_return": result.total_return,
            "annual_return": result.annual_return,
            "annual_volatility": result.annual_volatility,
            "max_drawdown": result.max_drawdown,
            "sharpe_ratio": result.sharpe_ratio,
            "calmar_ratio": result.calmar_ratio,
            "sortino_ratio": result.sortino_ratio,
            "win_rate": result.win_rate,
            "profit_loss_ratio": result.profit_loss_ratio,
            "total_trades": result.total_trades,
            "annual_turnover": result.annual_turnover,
            "total_cost": result.total_cost,
        }

        # Build nav_series as {dates: [...], values: [...]}
        nav_series_data = {
            "dates": result.dates,
            "values": result.nav,
        }
        benchmark_nav_data = {
            "dates": result.dates,
            "values": result.benchmark_nav,
        }
        drawdown_data = {
            "dates": result.dates,
            "values": result.drawdown,
        }

        conn.execute(
            """INSERT INTO backtest_results
               (id, strategy_id, config, summary,
                nav_series, benchmark_nav, drawdown_series,
                monthly_returns, trade_count, result_level, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                bt_id,
                strategy_id,
                json.dumps(config, default=str),
                json.dumps(summary, default=str),
                json.dumps(nav_series_data, default=str),
                json.dumps(benchmark_nav_data, default=str),
                json.dumps(drawdown_data, default=str),
                json.dumps(result.monthly_returns, default=str),
                result.total_trades,
                result_level,
                now,
            ],
        )
        log.info("backtest_service.saved", backtest_id=bt_id)


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _parse_json(raw) -> dict | list:
    """Safely parse a JSON column value."""
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return raw if raw else {}


def _cap_weights(
    weights: dict[str, float],
    max_weight: float,
    iterations: int = 10,
) -> dict[str, float]:
    """Iteratively cap individual weights and redistribute excess.

    After capping, excess weight is distributed proportionally among
    un-capped positions.  Repeats up to *iterations* times to handle
    cascading caps.
    """
    w = dict(weights)
    for _ in range(iterations):
        excess = 0.0
        uncapped_tickers = []
        uncapped_total = 0.0

        for ticker, wt in w.items():
            if wt > max_weight:
                excess += wt - max_weight
                w[ticker] = max_weight
            else:
                uncapped_tickers.append(ticker)
                uncapped_total += wt

        if excess <= 1e-10 or not uncapped_tickers:
            break

        # Redistribute excess proportionally among uncapped
        for ticker in uncapped_tickers:
            share = w[ticker] / uncapped_total if uncapped_total > 0 else 1.0 / len(uncapped_tickers)
            w[ticker] += excess * share

    # Normalize to sum to 1
    total = sum(w.values())
    if total > 0:
        w = {t: v / total for t, v in w.items()}

    return w
