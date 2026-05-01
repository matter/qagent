"""Backtest orchestration service.

Orchestrates the full pipeline:
  strategy -> factors -> model predictions -> signals -> position sizing -> BacktestEngine
"""

from __future__ import annotations

import json
import hashlib
import subprocess
import uuid
from datetime import datetime

import numpy as np
import pandas as pd

from backend.config import settings
from backend.db import get_connection
from backend.logger import get_logger
from backend.services.backtest_engine import BacktestConfig, BacktestEngine, BacktestResult
from backend.services import backtest_engine as backtest_engine_module
from backend.services.factor_engine import FactorEngine
from backend.services.group_service import GroupService
from backend.services.market_context import (
    get_default_benchmark,
    infer_ticker_market,
    normalize_market,
    normalize_ticker,
)
from backend.services.model_service import ModelService
from backend.services.strategy_service import StrategyService
from backend.time_utils import utc_now_naive
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
        market: str | None = None,
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
        resolved_market = normalize_market(market)

        # ---- 1. Load strategy ----
        strategy_def = self._strategy_service.get_strategy(strategy_id, market=resolved_market)
        strategy_instance = load_strategy_from_code(strategy_def["source_code"])
        required_models = StrategyService.resolve_required_models(strategy_def)
        StrategyService._validate_dependencies(
            strategy_def.get("required_factors", []),
            required_models,
            resolved_market,
        )

        log.info(
            "backtest_service.start",
            market=resolved_market,
            strategy=strategy_def["name"],
            version=strategy_def["version"],
        )

        # ---- 2. Resolve universe tickers ----
        tickers = self._group_service.get_group_tickers(
            universe_group_id, market=resolved_market
        )
        if not tickers:
            raise ValueError(f"Universe group '{universe_group_id}' has no members")
        tickers = [normalize_ticker(t, resolved_market) for t in tickers]

        # ---- 3. Build config ----
        benchmark = config_dict.get("benchmark") or get_default_benchmark(resolved_market)
        self._validate_benchmark_market(benchmark, resolved_market)
        bt_config = BacktestConfig(
            initial_capital=config_dict.get("initial_capital", 1_000_000),
            start_date=config_dict.get("start_date", "2020-01-01"),
            end_date=config_dict.get("end_date", "2024-12-31"),
            market=resolved_market,
            benchmark=benchmark,
            commission_rate=config_dict.get("commission_rate", 0.001),
            slippage_rate=config_dict.get("slippage_rate", 0.001),
            max_positions=config_dict.get("max_positions", 50),
            rebalance_freq=config_dict.get("rebalance_freq") or config_dict.get("rebalance_frequency", "monthly"),
            rebalance_buffer=config_dict.get("rebalance_buffer", 0.0),
            rebalance_buffer_add=config_dict.get("rebalance_buffer_add"),
            rebalance_buffer_reduce=config_dict.get("rebalance_buffer_reduce"),
            rebalance_buffer_mode=config_dict.get("rebalance_buffer_mode", "all"),
            rebalance_buffer_reference=config_dict.get("rebalance_buffer_reference", "target"),
            min_holding_days=config_dict.get("min_holding_days", 0),
            reentry_cooldown_days=config_dict.get("reentry_cooldown_days", 0),
        )

        position_sizing = strategy_def.get("position_sizing", "equal_weight")
        max_position_pct = config_dict.get("max_position_pct", 0.10)

        # Warn if strategy has custom weight logic under equal_weight sizing
        weight_warnings = StrategyService._validate_weight_effectiveness(
            strategy_def.get("source_code", ""), position_sizing
        )
        for w in weight_warnings:
            log.warning("backtest_service.weight_ineffective", detail=w)

        start_str = str(bt_config.start_date)
        end_str = str(bt_config.end_date)

        # ---- 4. Load OHLCV price data ----
        prices_close, prices_open, prices_high, prices_low, prices_volume = (
            self._backtest_engine._load_prices(
                tickers, start_str, end_str, market=resolved_market
            )
        )
        if prices_close.empty:
            raise ValueError("No price data available for the given tickers and date range")

        # ---- 5. Bulk-load required factors ----
        required_factors = strategy_def.get("required_factors", [])
        factor_data: dict[str, pd.DataFrame] = {}

        if required_factors:
            factor_id_map = self._resolve_factor_ids(
                required_factors, market=resolved_market
            )
            all_factor_ids = list(factor_id_map.values())
            cached_by_id = self._factor_engine.load_cached_factors_bulk(
                all_factor_ids, tickers, start_str, end_str, market=resolved_market
            )
            for factor_name, factor_id in factor_id_map.items():
                if factor_id in cached_by_id and not cached_by_id[factor_id].empty:
                    factor_data[factor_name] = cached_by_id[factor_id]
                else:
                    try:
                        df = self._factor_engine.compute_factor(
                            factor_id, tickers, start_str, end_str, market=resolved_market
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
        all_trading_days = sorted(prices_close.index)
        rebalance_dates = self._backtest_engine._get_rebalance_dates(
            all_trading_days, bt_config.rebalance_freq
        )
        rebalance_dates_set = set(rebalance_dates)

        # Build a prices DataFrame with MultiIndex columns (field, ticker)
        # for the StrategyContext — computed once, strategies slice via current_date
        prices_multi = self._build_prices_multi(
            prices_close, prices_open, prices_high, prices_low, prices_volume, tickers,
        )

        # Create weight signals: DataFrame(index=dates, columns=tickers)
        all_weights = pd.DataFrame(0.0, index=prices_close.index, columns=tickers)

        # ---- 6a. Pre-compute model predictions for ALL rebalance dates at once ----
        # This avoids per-date DB lookups, model loading, feature computation
        model_preds_by_date: dict[str, dict[str, pd.Series]] = {}
        if required_models:
            model_preds_by_date = self._batch_predict_all_dates(
                required_models, tickers, start_str, end_str,
                [d for d in all_trading_days if d in rebalance_dates_set],
                market=resolved_market,
            )

        # -- Runtime check: warn if required models produced no predictions --
        if required_models and not model_preds_by_date:
            missing_all_msg = (
                f"策略声明了 required_models={required_models} 但回测区间内没有任何模型产出预测。"
                f"这通常意味着模型未训练、特征集不兼容、或预测加载失败。"
                f"回测可能退化为 0 trades。"
            )
            log.error("backtest_service.no_model_predictions", detail=missing_all_msg)
            raise ValueError(missing_all_msg)
        elif required_models:
            # Check if any model has zero predictions across all dates
            all_model_ids_in_preds = set()
            for date_preds in model_preds_by_date.values():
                all_model_ids_in_preds.update(date_preds.keys())
            missing_models = [m for m in required_models if m not in all_model_ids_in_preds]
            if missing_models:
                missing_msg = (
                    f"missing_model_predictions={missing_models}; "
                    f"declared_models={required_models}; "
                    f"injected_models={sorted(all_model_ids_in_preds)}. "
                    "回测已阻断，避免策略静默退化为 no-op。"
                )
                log.error("backtest_service.partial_model_predictions", detail=missing_msg)
                raise ValueError(missing_msg)

        log.info(
            "backtest_service.signals",
            trading_days=len(all_trading_days),
            rebalance_dates=len(rebalance_dates),
            factors=len(factor_data),
            models=len(required_models),
        )

        prev_weights = None
        rebalance_diagnostics: list[dict] = []
        signal_errors: list[dict] = []
        # Portfolio state tracking for StrategyContext
        port_weights: dict[str, float] = {}     # current weight per ticker
        port_holding_days: dict[str, int] = {}   # consecutive days held
        port_entry_price: dict[str, float] = {}  # avg entry price per ticker

        for trade_date in all_trading_days:
            trade_ts = pd.Timestamp(trade_date)

            # Update holding_days for all held tickers
            for t in list(port_holding_days):
                port_holding_days[t] += 1

            # Compute unrealized P&L from entry prices
            port_unrealized: dict[str, float] = {}
            if port_entry_price:
                for t, entry in port_entry_price.items():
                    if (
                        trade_ts in prices_close.index
                        and t in prices_close.columns
                    ):
                        cur_price = prices_close.loc[trade_ts, t]
                        if pd.notna(cur_price) and entry > 0:
                            port_unrealized[t] = cur_price / entry - 1.0

            if trade_ts not in rebalance_dates_set:
                # Carry forward previous weights
                if prev_weights is not None:
                    all_weights.loc[trade_ts] = prev_weights
                continue

            # Look up pre-computed model predictions for this date
            date_key = str(trade_ts.date()) if hasattr(trade_ts, "date") else str(trade_ts)[:10]
            model_predictions = model_preds_by_date.get(date_key, {})

            positions_before = dict(port_weights)

            # Build context with portfolio state
            context = StrategyContext(
                prices=prices_multi,
                factor_values=factor_data,
                model_predictions=model_predictions,
                current_date=trade_ts,
                current_weights=dict(port_weights),
                holding_days=dict(port_holding_days),
                avg_entry_price=dict(port_entry_price),
                unrealized_pnl=dict(port_unrealized),
            )

            # Generate signals
            try:
                raw_signals = strategy_instance.generate_signals(context)
            except Exception as exc:
                log.warning(
                    "backtest_service.signal_failed",
                    date=date_key,
                    error=str(exc),
                )
                signal_errors.append({
                    "date": date_key,
                    "error": str(exc)[:200],
                })
                # Carry forward previous weights
                if prev_weights is not None:
                    all_weights.loc[trade_ts] = prev_weights
                continue

            if raw_signals.empty:
                # No signals -- go to cash (all zeros)
                all_weights.loc[trade_ts] = 0.0
                prev_weights = all_weights.loc[trade_ts].values
                diag_entry = self._build_rebalance_diagnostics(
                    date_key=date_key,
                    positions_before=positions_before,
                    positions_after={},
                    strategy_diagnostics=context.diagnostics,
                )
                rebalance_diagnostics.append(diag_entry)
                port_weights.clear()
                port_holding_days.clear()
                port_entry_price.clear()
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
            prev_weights = all_weights.loc[trade_ts].values

            # Update portfolio state tracking
            new_held = {t for t, w in weights.items() if w > 1e-8}
            old_held = set(port_weights.keys())

            # Exited tickers
            for t in old_held - new_held:
                port_weights.pop(t, None)
                port_holding_days.pop(t, None)
                port_entry_price.pop(t, None)

            # New entries — record entry price from current close
            for t in new_held - old_held:
                port_holding_days[t] = 0
                if (
                    trade_ts in prices_close.index
                    and t in prices_close.columns
                ):
                    p = prices_close.loc[trade_ts, t]
                    port_entry_price[t] = float(p) if pd.notna(p) else 0.0
                else:
                    port_entry_price[t] = 0.0

            # Update weights for all held tickers
            port_weights = {t: w for t, w in weights.items() if w > 1e-8}

            diag_entry = self._build_rebalance_diagnostics(
                date_key=date_key,
                positions_before=positions_before,
                positions_after=port_weights,
                strategy_diagnostics=context.diagnostics,
            )
            rebalance_diagnostics.append(diag_entry)

        # ---- 7. Check for pervasive signal errors ----
        num_rebalance = len(rebalance_dates)
        if signal_errors and num_rebalance > 0:
            error_ratio = len(signal_errors) / num_rebalance
            if error_ratio > 0.5:
                raise ValueError(
                    f"Signal generation failed on {len(signal_errors)}/{num_rebalance} "
                    f"rebalance days ({error_ratio:.0%}). "
                    f"First error ({signal_errors[0]['date']}): {signal_errors[0]['error']}"
                )

        # ---- 8. Run BacktestEngine ----
        overlay_result = self._backtest_engine.run(all_weights, bt_config)
        portfolio_config = config_dict.get("portfolio_overlay")
        if portfolio_config:
            base_result = self._run_base_portfolio_leg(
                tickers=tickers,
                prices_index=prices_close.index,
                bt_config=bt_config,
                portfolio_config=portfolio_config,
            )
            result = self._combine_portfolio_legs(
                base_result=base_result,
                overlay_result=overlay_result,
                base_weight=float(portfolio_config.get("base_weight", 0.65)),
                overlay_weight=float(portfolio_config.get("overlay_weight", 0.35)),
                portfolio_config=portfolio_config,
            )
        else:
            result = overlay_result

        # ---- 8. Determine result_level ----
        # For now, always 'exploratory' since we use yfinance
        result_level = "exploratory"

        # ---- 9. Save to backtest_results table ----
        bt_id = uuid.uuid4().hex[:12]
        config_to_save = bt_config.to_dict()
        config_to_save["universe_group_id"] = universe_group_id
        if portfolio_config:
            config_to_save["portfolio_overlay"] = result.config.get(
                "portfolio_overlay",
                portfolio_config,
            )
        self._save_result(
            bt_id=bt_id,
            market=resolved_market,
            strategy_id=strategy_id,
            config=config_to_save,
            result=result,
            result_level=result_level,
        )

        # ---- 10. Return result ----
        result_dict = result.to_dict()
        result_dict["backtest_id"] = bt_id
        result_dict["market"] = resolved_market
        result_dict["strategy_id"] = strategy_id
        result_dict["strategy_name"] = strategy_def["name"]
        result_dict["result_level"] = result_level
        result_dict["universe_group_id"] = universe_group_id
        if signal_errors:
            result_dict["signal_error_count"] = len(signal_errors)
            result_dict["signal_error_samples"] = signal_errors[:5]
        if rebalance_diagnostics:
            result_dict["rebalance_diagnostics"] = rebalance_diagnostics
            # Persist diagnostics into stored summary
            conn = get_connection()
            row = conn.execute(
                "SELECT summary FROM backtest_results WHERE id = ? AND market = ?",
                [bt_id, resolved_market],
            ).fetchone()
            if row:
                summary_data = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
                summary_data["rebalance_diagnostics"] = rebalance_diagnostics
                conn.execute(
                    "UPDATE backtest_results SET summary = ? WHERE id = ? AND market = ?",
                    [json.dumps(summary_data, default=str), bt_id, resolved_market],
                )

        # ---- 11. Check for data leakage ----
        leakage_warnings = self._check_data_leakage(
            required_models, bt_config, tickers, universe_group_id, market=resolved_market,
        )
        if leakage_warnings:
            result_dict["leakage_warnings"] = leakage_warnings
            # Persist warnings into the stored summary
            conn = get_connection()
            row = conn.execute(
                "SELECT summary FROM backtest_results WHERE id = ? AND market = ?",
                [bt_id, resolved_market],
            ).fetchone()
            if row:
                summary_data = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
                summary_data["leakage_warnings"] = leakage_warnings
                conn.execute(
                    "UPDATE backtest_results SET summary = ? WHERE id = ? AND market = ?",
                    [json.dumps(summary_data, default=str), bt_id, resolved_market],
                )
            log.warning(
                "backtest_service.leakage_detected",
                backtest_id=bt_id,
                warnings=len(leakage_warnings),
            )

        log.info(
            "backtest_service.done",
            backtest_id=bt_id,
            total_return=result.total_return,
            sharpe=result.sharpe_ratio,
        )
        return result_dict

    def _run_base_portfolio_leg(
        self,
        *,
        tickers: list[str],
        prices_index,
        bt_config: BacktestConfig,
        portfolio_config: dict,
    ) -> BacktestResult:
        mode = portfolio_config.get("base_leg", "equal_weight")
        if mode not in {"equal_weight", "core_union_equal_weight"}:
            raise ValueError(
                "portfolio_overlay.base_leg must be 'equal_weight' or "
                "'core_union_equal_weight'"
            )
        if not tickers:
            raise ValueError("portfolio base leg requires non-empty tickers")

        base_weights = pd.DataFrame(0.0, index=prices_index, columns=tickers)
        weight = 1.0 / len(tickers)
        base_weights.loc[:, tickers] = weight
        return self._backtest_engine.run(base_weights, bt_config)

    def _combine_portfolio_legs(
        self,
        *,
        base_result: BacktestResult,
        overlay_result: BacktestResult,
        base_weight: float,
        overlay_weight: float,
        portfolio_config: dict,
    ) -> BacktestResult:
        if base_weight < 0 or overlay_weight < 0:
            raise ValueError("portfolio leg weights must be non-negative")
        total_weight = base_weight + overlay_weight
        if total_weight <= 0:
            raise ValueError("portfolio leg weights must sum to a positive value")
        base_weight = base_weight / total_weight
        overlay_weight = overlay_weight / total_weight

        common_dates = [date for date in base_result.dates if date in set(overlay_result.dates)]
        if not common_dates:
            raise ValueError("portfolio legs have no overlapping NAV dates")

        base_nav_by_date = dict(zip(base_result.dates, base_result.nav, strict=False))
        overlay_nav_by_date = dict(zip(overlay_result.dates, overlay_result.nav, strict=False))
        base_initial = base_result.nav[0] if base_result.nav else 1.0
        overlay_initial = overlay_result.nav[0] if overlay_result.nav else 1.0
        initial_capital = float(base_result.config.get("initial_capital") or base_initial)

        combined_nav = []
        for date_key in common_dates:
            base_ratio = base_nav_by_date[date_key] / base_initial if base_initial else 1.0
            overlay_ratio = overlay_nav_by_date[date_key] / overlay_initial if overlay_initial else 1.0
            combined_nav.append(
                round(initial_capital * (base_weight * base_ratio + overlay_weight * overlay_ratio), 2)
            )

        benchmark_by_date = dict(zip(base_result.dates, base_result.benchmark_nav, strict=False))
        benchmark_nav = [benchmark_by_date.get(date_key, initial_capital) for date_key in common_dates]

        nav_arr = np.array(combined_nav, dtype=float)
        daily_returns = np.diff(nav_arr) / nav_arr[:-1] if len(nav_arr) > 1 else np.array([])
        daily_returns = np.where(np.isfinite(daily_returns), daily_returns, 0.0)
        total_return = (nav_arr[-1] / initial_capital - 1.0) if len(nav_arr) > 0 else 0.0
        years = len(nav_arr) / 252.0 if len(nav_arr) > 0 else 1.0
        annual_return = backtest_engine_module._calc_cagr(
            initial_capital,
            nav_arr[-1] if len(nav_arr) > 0 else initial_capital,
            years,
        )
        annual_volatility = backtest_engine_module._calc_annual_volatility(daily_returns)
        drawdown_series = backtest_engine_module._calc_drawdown_series(nav_arr)
        max_dd = float(np.min(drawdown_series)) if len(drawdown_series) > 0 else 0.0
        sharpe = backtest_engine_module._calc_sharpe(annual_return, annual_volatility)
        calmar = backtest_engine_module._calc_calmar(annual_return, max_dd)
        sortino = backtest_engine_module._calc_sortino(daily_returns, annual_return)
        monthly_returns = backtest_engine_module._calc_monthly_returns(common_dates, combined_nav)

        base_contribution = base_weight * base_result.total_return
        overlay_contribution = overlay_weight * overlay_result.total_return
        trade_diagnostics = dict(overlay_result.trade_diagnostics or {})
        trade_diagnostics["portfolio_legs"] = {
            "base": {
                "type": portfolio_config.get("base_leg", "equal_weight"),
                "weight": round(base_weight, 6),
                "total_return": base_result.total_return,
                "contribution_return": round(base_contribution, 6),
                "total_trades": base_result.total_trades,
            },
            "overlay": {
                "strategy": portfolio_config.get("overlay_leg", "strategy"),
                "weight": round(overlay_weight, 6),
                "total_return": overlay_result.total_return,
                "contribution_return": round(overlay_contribution, 6),
                "total_trades": overlay_result.total_trades,
            },
        }

        config = dict(overlay_result.config)
        config["portfolio_overlay"] = {
            **portfolio_config,
            "base_weight": round(base_weight, 6),
            "overlay_weight": round(overlay_weight, 6),
        }

        return BacktestResult(
            config=config,
            dates=common_dates,
            nav=combined_nav,
            benchmark_nav=benchmark_nav,
            drawdown=[round(d, 6) for d in drawdown_series.tolist()] if len(drawdown_series) > 0 else [],
            total_return=round(float(total_return), 6),
            annual_return=round(float(annual_return), 6),
            annual_volatility=round(float(annual_volatility), 6),
            max_drawdown=round(max_dd, 6),
            sharpe_ratio=round(float(sharpe), 4),
            calmar_ratio=round(float(calmar), 4),
            sortino_ratio=round(float(sortino), 4),
            win_rate=overlay_result.win_rate,
            profit_loss_ratio=overlay_result.profit_loss_ratio,
            total_trades=overlay_result.total_trades,
            annual_turnover=round(float(overlay_result.annual_turnover * overlay_weight), 4),
            total_cost=round(float(overlay_result.total_cost * overlay_weight), 2),
            monthly_returns=monthly_returns,
            trades=overlay_result.trades,
            trade_diagnostics=trade_diagnostics,
        )

    # ------------------------------------------------------------------
    # CRUD for backtest results
    # ------------------------------------------------------------------

    def list_backtests(
        self,
        strategy_id: str | None = None,
        market: str | None = None,
    ) -> list[dict]:
        """List backtest results (summary only, no heavy series)."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        if strategy_id:
            rows = conn.execute(
                """SELECT id, market, strategy_id, config, summary, trade_count,
                          result_level, created_at
                   FROM backtest_results
                   WHERE market = ? AND strategy_id = ?
                   ORDER BY created_at DESC""",
                [resolved_market, strategy_id],
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, market, strategy_id, config, summary, trade_count,
                          result_level, created_at
                   FROM backtest_results
                   WHERE market = ?
                   ORDER BY created_at DESC""",
                [resolved_market],
            ).fetchall()

        results = []
        for r in rows:
            results.append({
                "id": r[0],
                "market": r[1],
                "strategy_id": r[2],
                "config": _parse_json(r[3]),
                "summary": self._list_summary(_parse_json(r[4])),
                "trade_count": r[5],
                "result_level": r[6],
                "created_at": str(r[7]) if r[7] else None,
            })
        return results

    def get_backtest(self, backtest_id: str, market: str | None = None) -> dict:
        """Return full backtest result including all series data."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        row = conn.execute(
            """SELECT id, market, strategy_id, config, summary,
                      nav_series, benchmark_nav, drawdown_series,
                      monthly_returns, trade_count, result_level, created_at,
                      trades
               FROM backtest_results
               WHERE id = ? AND market = ?""",
            [backtest_id, resolved_market],
        ).fetchone()

        if row is None:
            raise ValueError(f"Backtest {backtest_id} not found")

        trades = _parse_json(row[12]) if row[12] else []

        return {
            "id": row[0],
            "market": row[1],
            "strategy_id": row[2],
            "config": _parse_json(row[3]),
            "summary": _parse_json(row[4]),
            "nav_series": _parse_json(row[5]),
            "benchmark_nav": _parse_json(row[6]),
            "drawdown_series": _parse_json(row[7]),
            "monthly_returns": _parse_json(row[8]),
            "trade_count": row[9],
            "result_level": row[10],
            "created_at": str(row[11]) if row[11] else None,
            "trades": trades,
            "stock_pnl": _compute_stock_pnl(trades if isinstance(trades, list) else []),
        }

    def delete_backtest(self, backtest_id: str, market: str | None = None) -> None:
        """Delete a backtest result."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM backtest_results WHERE id = ? AND market = ?",
            [backtest_id, resolved_market],
        ).fetchone()
        if row is None:
            raise ValueError(f"Backtest {backtest_id} not found")

        conn.execute(
            "DELETE FROM backtest_results WHERE id = ? AND market = ?",
            [backtest_id, resolved_market],
        )
        log.info("backtest_service.deleted", backtest_id=backtest_id, market=resolved_market)

    def get_stock_chart_data(
        self,
        backtest_id: str,
        ticker: str,
        market: str | None = None,
    ) -> dict:
        """Return daily bars and trade markers for a single stock within a backtest.

        Used to render a K-line chart with buy/sell markers.
        """
        conn = get_connection()
        resolved_market = normalize_market(market)
        row = conn.execute(
            "SELECT config, trades, market FROM backtest_results WHERE id = ? AND market = ?",
            [backtest_id, resolved_market],
        ).fetchone()

        if row is None:
            raise ValueError(f"Backtest {backtest_id} not found")

        config = _parse_json(row[0])
        all_trades = _parse_json(row[1]) if row[1] else []
        resolved_market = row[2]
        normalized_ticker = normalize_ticker(ticker, resolved_market)

        start_date = config.get("start_date", "2020-01-01")
        end_date = config.get("end_date", "2024-12-31")

        # Fetch daily bars for this ticker in the backtest range
        bars = conn.execute(
            """SELECT date, open, high, low, close, volume
               FROM daily_bars
               WHERE market = ?
                 AND ticker = ?
                 AND date >= ? AND date <= ?
               ORDER BY date""",
            [resolved_market, normalized_ticker, start_date, end_date],
        ).fetchall()

        daily_bars = [
            {
                "date": str(r[0]),
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5],
            }
            for r in bars
        ]

        # Filter trades for this ticker
        ticker_trades = [
            {
                "date": t["date"],
                "action": t["action"],
                "shares": t["shares"],
                "price": t["price"],
                "cost": t.get("cost", 0),
            }
            for t in all_trades
            if isinstance(t, dict) and t.get("ticker") == normalized_ticker
        ]

        return {
            "market": resolved_market,
            "ticker": normalized_ticker,
            "daily_bars": daily_bars,
            "trades": ticker_trades,
        }

    def compare_strategies(
        self,
        backtest_ids: list[str],
        market: str | None = None,
    ) -> dict:
        """Compare multiple backtest results.

        Returns aligned NAV curves and a metric comparison table.
        """
        if not backtest_ids or len(backtest_ids) < 2:
            raise ValueError("At least 2 backtest IDs are required for comparison")

        results = []
        for bt_id in backtest_ids:
            results.append(self.get_backtest(bt_id, market=market))

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
    def _list_summary(summary: dict) -> dict:
        """Return a summary safe for list views.

        Full diagnostics can be large and should be fetched via get_backtest().
        """
        if not isinstance(summary, dict):
            return {}
        heavy_keys = {
            "rebalance_diagnostics",
            "leakage_warnings",
            "reproducibility_fingerprint",
        }
        lightweight = {
            key: value
            for key, value in summary.items()
            if key not in heavy_keys
        }
        lightweight["has_rebalance_diagnostics"] = bool(
            summary.get("rebalance_diagnostics")
        )
        lightweight["has_leakage_warnings"] = bool(
            summary.get("leakage_warnings")
        )
        fingerprint = summary.get("reproducibility_fingerprint")
        if isinstance(fingerprint, dict):
            lightweight["reproducibility_hash"] = fingerprint.get("hash")
            lightweight["has_reproducibility_fingerprint"] = True
        else:
            lightweight["has_reproducibility_fingerprint"] = False
        return lightweight

    @staticmethod
    def _round_weight_map(weights: dict[str, float]) -> dict[str, float]:
        return {
            str(ticker): round(float(weight), 6)
            for ticker, weight in sorted(weights.items())
            if abs(float(weight)) > 1e-8
        }

    @classmethod
    def _build_rebalance_diagnostics(
        cls,
        *,
        date_key: str,
        positions_before: dict[str, float],
        positions_after: dict[str, float],
        strategy_diagnostics: dict | None = None,
    ) -> dict:
        """Build a structured per-rebalance position delta snapshot."""
        before = {
            str(t): float(w)
            for t, w in positions_before.items()
            if abs(float(w)) > 1e-8
        }
        after = {
            str(t): float(w)
            for t, w in positions_after.items()
            if abs(float(w)) > 1e-8
        }

        before_tickers = set(before)
        after_tickers = set(after)
        shared = before_tickers & after_tickers

        added = sorted(after_tickers - before_tickers)
        removed = sorted(before_tickers - after_tickers)
        increased = sorted(
            ticker for ticker in shared
            if after[ticker] - before[ticker] > 1e-8
        )
        decreased = sorted(
            ticker for ticker in shared
            if before[ticker] - after[ticker] > 1e-8
        )
        turnover = sum(
            abs(after.get(ticker, 0.0) - before.get(ticker, 0.0))
            for ticker in before_tickers | after_tickers
        )

        diag = {
            "date": date_key,
            "positions_before": cls._round_weight_map(before),
            "positions_after": cls._round_weight_map(after),
            "added": added,
            "removed": removed,
            "increased": increased,
            "decreased": decreased,
            "turnover": round(float(turnover), 6),
        }
        if strategy_diagnostics:
            diag.update(strategy_diagnostics)
        return diag

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

    def _batch_predict_all_dates(
        self,
        model_ids: list[str],
        tickers: list[str],
        start_date: str,
        end_date: str,
        rebalance_days: list,
        market: str | None = None,
    ) -> dict[str, dict[str, pd.Series]]:
        """Pre-compute model predictions for ALL rebalance dates at once.

        Instead of calling predict() per-date (which reloads model + features
        each time), this method:
          1. Loads each model from disk ONCE.
          2. Computes features for the FULL date range ONCE.
          3. Builds the full X matrix ONCE.
          4. Runs model.predict() on the entire X matrix.
          5. Slices predictions by date for lookup.

        Returns:
            dict[date_str -> dict[model_id -> Series(ticker -> prediction)]]
        """
        from backend.services.model_service import ModelService

        resolved_market = normalize_market(market)
        result: dict[str, dict[str, pd.Series]] = {}
        failed_models: list[tuple[str, str]] = []

        for model_id in model_ids:
            try:
                record = self._model_service.get_model(model_id, market=resolved_market)
                model_instance = self._model_service.load_model(
                    model_id, market=resolved_market
                )
                fs_id = record["feature_set_id"]

                # Compute features for full date range (use bulk cache path)
                feature_data = self._model_service._feature_service.compute_features_from_cache(
                    fs_id, tickers, start_date, end_date, market=resolved_market
                )

                # Build full X matrix: (date, ticker) x factors
                X, _ = ModelService._build_Xy(
                    feature_data,
                    # Dummy label_df: we only need X, not y
                    pd.DataFrame(columns=["ticker", "date", "label_value"]),
                )

                # _build_Xy returns empty if no labels overlap.
                # Instead, build X directly from feature_data.
                if X.empty:
                    X = self._build_full_X(feature_data)

                if X.empty:
                    log.warning("backtest_service.batch_predict.empty_X", model_id=model_id)
                    continue

                # Run prediction on entire X at once
                all_preds = model_instance.predict(X)
                all_preds.index = X.index

                # Slice by date and store
                dates_in_X = all_preds.index.get_level_values("date").unique()
                for rebal_day in rebalance_days:
                    rebal_ts = pd.Timestamp(rebal_day)
                    date_key = str(rebal_ts.date()) if hasattr(rebal_ts, "date") else str(rebal_ts)[:10]

                    # Find the closest date <= rebal_ts in the predictions
                    available = dates_in_X[dates_in_X <= rebal_ts]
                    if len(available) == 0:
                        continue
                    closest = available[-1]

                    try:
                        day_preds = all_preds.xs(closest, level="date")
                        if not day_preds.empty:
                            day_preds = self._model_service._break_prediction_ties(day_preds)
                            day_preds.name = "prediction"
                            if date_key not in result:
                                result[date_key] = {}
                            result[date_key][model_id] = day_preds
                    except KeyError:
                        continue

                log.info(
                    "backtest_service.batch_predict.done",
                    model_id=model_id,
                    total_predictions=len(all_preds),
                    dates_covered=len([d for d in result if model_id in result.get(d, {})]),
                )

            except Exception as exc:
                log.warning(
                    "backtest_service.batch_predict.failed",
                    model_id=model_id,
                    error=str(exc),
                )
                failed_models.append((model_id, str(exc)))

        if failed_models:
            details = "; ".join(f"{mid}: {err}" for mid, err in failed_models)
            raise ValueError(
                f"Model prediction failed for {len(failed_models)} model(s): {details}"
            )

        return result

    @staticmethod
    def _build_full_X(feature_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build a full X matrix from feature data without requiring labels.

        Returns DataFrame with MultiIndex (date, ticker), columns = factor names.
        """
        long_frames: list[pd.Series] = []
        factor_names = sorted(feature_data.keys())

        for factor_name in factor_names:
            df = feature_data[factor_name]
            stacked = df.stack()
            stacked.name = factor_name
            stacked.index.names = ["date", "ticker"]
            long_frames.append(stacked)

        if not long_frames:
            return pd.DataFrame()

        X = pd.concat(long_frames, axis=1)
        X.index.names = ["date", "ticker"]
        X = X.dropna()
        return X

    def _resolve_factor_ids(
        self,
        factor_names: list[str],
        market: str | None = None,
    ) -> dict[str, str]:
        """Resolve factor names to factor IDs (latest version) in a single query."""
        if not factor_names:
            return {}
        resolved_market = normalize_market(market)
        conn = get_connection()
        placeholders = ",".join("?" for _ in factor_names)
        rows = conn.execute(
            f"""SELECT name, id, version FROM factors
                WHERE market = ?
                  AND name IN ({placeholders})
                ORDER BY version DESC""",
            [resolved_market, *factor_names],
        ).fetchall()
        result: dict[str, str] = {}
        for name, fid, _version in rows:
            if name not in result:
                result[name] = fid
        for name in factor_names:
            if name not in result:
                log.warning(
                    "backtest_service.factor_not_found",
                    name=name,
                    market=resolved_market,
                )
        return result

    @staticmethod
    def _validate_benchmark_market(symbol: str, market: str) -> None:
        """Reject unambiguous benchmark/market mismatches."""
        resolved_market = normalize_market(market)
        inferred = infer_ticker_market(symbol)
        if resolved_market == "CN" and inferred != "CN":
            raise ValueError(
                f"benchmark '{symbol}' is not a CN benchmark. "
                "Use a BaoStock-style symbol such as sh.000300 for market CN."
            )
        if inferred is not None and inferred != resolved_market:
            raise ValueError(
                f"benchmark '{symbol}' belongs to market {inferred}, "
                f"not market {resolved_market}"
            )

    @staticmethod
    def _build_prices_multi(
        prices_close: pd.DataFrame,
        prices_open: pd.DataFrame,
        prices_high: pd.DataFrame,
        prices_low: pd.DataFrame,
        prices_volume: pd.DataFrame,
        tickers: list[str],
    ) -> pd.DataFrame:
        """Build a MultiIndex-column DataFrame with (field, ticker) columns.

        Fields: close, open, high, low, volume.
        """
        frames = {}
        field_dfs = [
            ("close", prices_close),
            ("open", prices_open),
            ("high", prices_high),
            ("low", prices_low),
            ("volume", prices_volume),
        ]
        for field_name, df in field_dfs:
            for ticker in tickers:
                if ticker in df.columns:
                    frames[(field_name, ticker)] = df[ticker]

        if not frames:
            return pd.DataFrame()

        result = pd.DataFrame(frames)
        result.columns = pd.MultiIndex.from_tuples(result.columns, names=["field", "ticker"])
        return result

    def _check_data_leakage(
        self,
        model_ids: list[str],
        bt_config: BacktestConfig,
        backtest_tickers: list[str],
        backtest_group_id: str,
        market: str | None = None,
    ) -> list[dict]:
        """Detect overlap between model training data and backtest window.

        Returns a list of warning dicts, one per model with detected leakage.
        Each warning includes:
          - model_id, model_name
          - time_overlap: bool (backtest window intersects train/valid window)
          - ticker_overlap: bool (backtest tickers intersect training tickers)
          - overlap_level: 'fully_unseen' | 'time_unseen' | 'ticker_unseen' | 'contaminated'
          - details: human-readable description
        """
        if not model_ids:
            return []

        warnings: list[dict] = []
        resolved_market = normalize_market(market)
        bt_start = bt_config.start_date
        bt_end = bt_config.end_date
        backtest_ticker_set = set(backtest_tickers)

        for model_id in model_ids:
            try:
                record = self._model_service.get_model(model_id, market=resolved_market)
            except Exception:
                continue

            tc = record.get("train_config") or {}
            model_name = record.get("name", model_id)

            # Parse training date boundaries
            train_end_str = tc.get("train_end") or tc.get("valid_end") or tc.get("test_end")
            train_start_str = tc.get("train_start")
            if not train_end_str:
                continue

            from datetime import date as _date
            try:
                model_train_end = _date.fromisoformat(str(train_end_str))
                model_train_start = _date.fromisoformat(str(train_start_str)) if train_start_str else None
            except (ValueError, TypeError):
                continue

            # Include valid/test window as "seen" data
            for key in ("valid_end", "test_end"):
                val = tc.get(key)
                if val:
                    try:
                        d = _date.fromisoformat(str(val))
                        if d > model_train_end:
                            model_train_end = d
                    except (ValueError, TypeError):
                        pass

            # Check time overlap: does the backtest window start before
            # the model's last seen date?
            time_overlap = bt_start <= model_train_end

            # Check ticker overlap: read training universe from metadata.json
            ticker_overlap = False
            model_group_id = None
            try:
                import json as _json
                from backend.config import settings as _settings
                meta_path = _settings.models_dir / model_id / "metadata.json"
                if meta_path.exists():
                    meta = _json.loads(meta_path.read_text())
                    model_group_id = meta.get("universe_group_id")
            except Exception:
                pass

            if model_group_id:
                if model_group_id == backtest_group_id:
                    ticker_overlap = True
                else:
                    try:
                        model_tickers = set(
                            self._group_service.get_group_tickers(
                                model_group_id, market=resolved_market
                            )
                        )
                        if backtest_ticker_set & model_tickers:
                            ticker_overlap = True
                    except Exception:
                        pass

            # Classify overlap level
            if time_overlap and ticker_overlap:
                level = "contaminated"
            elif time_overlap:
                level = "ticker_unseen"
            elif ticker_overlap:
                level = "time_unseen"
            else:
                level = "fully_unseen"

            if level == "fully_unseen":
                continue  # no warning needed

            # Build detail message
            detail_parts = []
            if time_overlap:
                detail_parts.append(
                    f"回测起始 {bt_start} 在模型训练数据截止日 {model_train_end} 之前，"
                    f"存在 {(model_train_end - bt_start).days + 1} 天时间重叠"
                )
            if ticker_overlap:
                detail_parts.append("回测股票池与模型训练股票池存在重叠")

            warnings.append({
                "model_id": model_id,
                "model_name": model_name,
                "time_overlap": time_overlap,
                "ticker_overlap": ticker_overlap,
                "overlap_level": level,
                "model_data_end": str(model_train_end),
                "backtest_start": str(bt_start),
                "details": "；".join(detail_parts),
            })

        return warnings

    def _save_result(
        self,
        bt_id: str,
        market: str,
        strategy_id: str,
        config: dict,
        result: BacktestResult,
        result_level: str,
    ) -> None:
        """Persist backtest result to DuckDB."""
        conn = get_connection()
        now = utc_now_naive()

        resolved_market = normalize_market(market)
        summary = {
            "market": resolved_market,
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
            "trade_diagnostics": result.trade_diagnostics,
        }
        summary["reproducibility_fingerprint"] = self._build_reproducibility_fingerprint(
            strategy_id=strategy_id,
            market=resolved_market,
            config=config,
            result=result,
        )

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

        # Cap trades at 10000 to avoid huge JSON blobs
        trades_to_save = result.trades[:10000] if len(result.trades) > 10000 else result.trades

        conn.execute(
            """INSERT INTO backtest_results
               (id, market, strategy_id, config, summary,
                nav_series, benchmark_nav, drawdown_series,
                monthly_returns, trade_count, trades, result_level, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                bt_id,
                resolved_market,
                strategy_id,
                json.dumps(config, default=str),
                json.dumps(summary, default=str),
                json.dumps(nav_series_data, default=str),
                json.dumps(benchmark_nav_data, default=str),
                json.dumps(drawdown_data, default=str),
                json.dumps(result.monthly_returns, default=str),
                result.total_trades,
                json.dumps(trades_to_save, default=str),
                result_level,
                now,
            ],
        )
        log.info("backtest_service.saved", backtest_id=bt_id, market=resolved_market)

    def _build_reproducibility_fingerprint(
        self,
        *,
        strategy_id: str,
        market: str,
        config: dict,
        result: BacktestResult,
    ) -> dict:
        resolved_market = normalize_market(market)
        conn = get_connection()
        strategy = self._strategy_service.get_strategy(strategy_id, market=resolved_market)
        required_models = StrategyService.resolve_required_models(strategy)
        required_factors = strategy.get("required_factors") or []
        factor_ids = self._resolve_factor_ids(required_factors, market=resolved_market)

        model_refs = []
        for model_id in required_models:
            try:
                model = self._model_service.get_model(model_id, market=resolved_market)
            except Exception:
                model_refs.append({"id": model_id, "status": "missing"})
                continue
            metadata_hash = None
            metadata_path = settings.models_dir / model_id / "metadata.json"
            if metadata_path.exists():
                metadata_hash = _sha256_text(metadata_path.read_text())
            model_refs.append({
                "id": model_id,
                "market": model.get("market"),
                "name": model.get("name"),
                "feature_set_id": model.get("feature_set_id"),
                "label_id": model.get("label_id"),
                "model_type": model.get("model_type"),
                "task_type": model.get("task_type"),
                "train_config": model.get("train_config"),
                "metadata_hash": metadata_hash,
            })

        factor_refs = []
        for factor_name, factor_id in sorted(factor_ids.items()):
            row = conn.execute(
                """SELECT id, market, name, version, status, source_code, updated_at
                   FROM factors
                   WHERE id = ? AND market = ?""",
                [factor_id, resolved_market],
            ).fetchone()
            if row:
                factor_refs.append({
                    "name": factor_name,
                    "id": row[0],
                    "market": row[1],
                    "version": row[3],
                    "status": row[4],
                    "source_hash": _sha256_text(row[5] or ""),
                    "updated_at": str(row[6]) if row[6] else None,
                })
            else:
                factor_refs.append({
                    "name": factor_name,
                    "id": factor_id,
                    "status": "missing",
                })

        data_watermark = conn.execute(
            """SELECT MIN(date), MAX(date), COUNT(*), COUNT(DISTINCT ticker)
               FROM daily_bars
               WHERE market = ?
                 AND date >= ?
                 AND date <= ?""",
            [
                resolved_market,
                config.get("start_date"),
                config.get("end_date"),
            ],
        ).fetchone()
        benchmark_watermark = conn.execute(
            """SELECT MIN(date), MAX(date), COUNT(*)
               FROM index_bars
               WHERE market = ?
                 AND symbol = ?
                 AND date >= ?
                 AND date <= ?""",
            [
                resolved_market,
                normalize_ticker(config.get("benchmark"), resolved_market),
                config.get("start_date"),
                config.get("end_date"),
            ],
        ).fetchone()

        payload = {
            "schema_version": 1,
            "service_version": _service_version(),
            "git_commit": _git_commit_hash(),
            "market": resolved_market,
            "strategy": {
                "id": strategy.get("id"),
                "market": strategy.get("market"),
                "name": strategy.get("name"),
                "version": strategy.get("version"),
                "position_sizing": strategy.get("position_sizing"),
                "source_hash": _sha256_text(strategy.get("source_code", "")),
                "required_factors": required_factors,
                "required_models": required_models,
            },
            "dependencies": {
                "factors": factor_refs,
                "models": model_refs,
            },
            "config": _stable_json_value(config),
            "data_watermark": {
                "daily_bars": {
                    "min_date": str(data_watermark[0]) if data_watermark and data_watermark[0] else None,
                    "max_date": str(data_watermark[1]) if data_watermark and data_watermark[1] else None,
                    "rows": int(data_watermark[2] or 0) if data_watermark else 0,
                    "tickers": int(data_watermark[3] or 0) if data_watermark else 0,
                },
                "benchmark": {
                    "symbol": normalize_ticker(config.get("benchmark"), resolved_market),
                    "min_date": str(benchmark_watermark[0]) if benchmark_watermark and benchmark_watermark[0] else None,
                    "max_date": str(benchmark_watermark[1]) if benchmark_watermark and benchmark_watermark[1] else None,
                    "rows": int(benchmark_watermark[2] or 0) if benchmark_watermark else 0,
                },
            },
            "result_shape": {
                "dates": len(result.dates),
                "trades": result.total_trades,
            },
        }
        payload["hash"] = _fingerprint_hash(payload)
        return payload


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


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _fingerprint_hash(payload: dict) -> str:
    clean = {k: v for k, v in payload.items() if k != "hash"}
    return hashlib.sha256(
        json.dumps(_stable_json_value(clean), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _stable_json_value(value):
    if isinstance(value, dict):
        return {str(k): _stable_json_value(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_stable_json_value(v) for v in value]
    if isinstance(value, tuple):
        return [_stable_json_value(v) for v in value]
    return value


def _service_version() -> str:
    pyproject = settings.project_root / "pyproject.toml"
    try:
        for line in pyproject.read_text().splitlines():
            if line.strip().startswith("version"):
                return line.split("=", 1)[1].strip().strip('"')
    except Exception:
        pass
    return "unknown"


def _git_commit_hash() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=settings.project_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def _compute_stock_pnl(trades: list[dict]) -> list[dict]:
    """Compute per-stock P&L statistics from a trade log.

    Groups trades by ticker and computes realized P&L using FIFO matching
    of buy/sell pairs with support for partial fills.

    Returns a list sorted by realized_pnl descending.
    """
    if not trades:
        return []

    from collections import defaultdict
    from datetime import date as _date, datetime as _dt

    # Group trades by ticker
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_ticker[t["ticker"]].append(t)

    results: list[dict] = []

    for ticker, ticker_trades in by_ticker.items():
        buy_count = 0
        sell_count = 0
        total_buy_value = 0.0
        total_sell_value = 0.0
        win_count = 0
        loss_count = 0
        realized_pnl = 0.0

        # FIFO queue for open buy positions: [(shares_remaining, price, cost)]
        open_buys: list[dict] = []

        for trade in ticker_trades:
            shares = trade["shares"]
            price = trade["price"]
            cost = trade.get("cost", 0)
            value = shares * price

            if trade["action"] == "buy":
                buy_count += 1
                total_buy_value += value
                # Add to open positions with full shares
                open_buys.append({
                    "shares": shares,
                    "price": price,
                    "cost": cost,
                })
            elif trade["action"] == "sell":
                sell_count += 1
                total_sell_value += value

                # Match against open buys using FIFO with partial fill support
                shares_to_sell = shares
                sell_cost_allocated = 0.0

                while shares_to_sell > 0 and open_buys:
                    buy_position = open_buys[0]
                    buy_shares = buy_position["shares"]
                    buy_price = buy_position["price"]
                    buy_cost = buy_position["cost"]

                    matched_shares = min(shares_to_sell, buy_shares)

                    # Prorate costs: sell cost by fraction of sell, buy cost by fraction of buy lot
                    sell_cost_for_match = (matched_shares / shares) * cost if shares > 0 else 0
                    buy_cost_for_match = (matched_shares / buy_shares) * buy_cost if buy_shares > 0 else 0

                    pnl = (price - buy_price) * matched_shares
                    pnl -= (sell_cost_for_match + buy_cost_for_match)

                    realized_pnl += pnl
                    sell_cost_allocated += sell_cost_for_match

                    if pnl > 0:
                        win_count += 1
                    elif pnl < 0:
                        loss_count += 1

                    # Update remaining shares and cost
                    shares_to_sell -= matched_shares
                    buy_position["shares"] -= matched_shares
                    buy_position["cost"] -= buy_cost_for_match

                    # Remove buy position if fully consumed
                    if buy_position["shares"] <= 1e-8:
                        open_buys.pop(0)

        pnl_pct = (realized_pnl / total_buy_value * 100) if total_buy_value > 0 else 0.0

        results.append({
            "ticker": ticker,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "total_buy_value": round(total_buy_value, 2),
            "total_sell_value": round(total_sell_value, 2),
            "realized_pnl": round(realized_pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "win_count": win_count,
            "loss_count": loss_count,
        })

    # Sort by realized_pnl descending
    results.sort(key=lambda x: x["realized_pnl"], reverse=True)
    return results


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
