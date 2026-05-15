"""Backtest orchestration service.

Orchestrates the full pipeline:
  strategy -> factors -> model predictions -> signals -> position sizing -> BacktestEngine
"""

from __future__ import annotations

import json
import hashlib
import shutil
import subprocess
import threading
import time
import uuid
import copy
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from backend.config import settings
from backend.db import get_connection
from backend.logger import get_logger
from backend.services.backtest_engine import BacktestConfig, BacktestEngine, BacktestResult
from backend.services import backtest_engine as backtest_engine_module
from backend.services.calendar_service import offset_trading_days
from backend.services.execution_model_service import _positive_float
from backend.services.execution_model_service import evaluate_planned_price_fill
from backend.services.execution_model_service import normalize_execution_model
from backend.services.execution_model_service import normalize_planned_price_buffer_bps
from backend.services.execution_model_service import normalize_planned_price_fallback
from backend.services.factor_engine import FactorEngine
from backend.services.group_service import GroupService
from backend.services.market_context import (
    get_default_benchmark,
    infer_ticker_market,
    normalize_market,
    normalize_ticker,
)
from backend.services.model_service import ModelService
from backend.services.strategy_service import SUPPORTED_POSITION_SIZING, StrategyService
from backend.time_utils import utc_now_iso, utc_now_naive
from backend.strategies.base import StrategyContext
from backend.strategies.loader import load_strategy_from_code

log = get_logger(__name__)


_INITIAL_ENTRY_POLICIES = {
    "wait_for_anchor",
    "open_immediately",
    "bootstrap_from_history",
    "require_warmup_state",
}
_PREDICTION_CACHE_LOCKS_GUARD = threading.Lock()
_PREDICTION_CACHE_LOCKS: dict[tuple, threading.Lock] = {}
_RUN_CONFIG_DEFAULTS: dict[str, Any] = {
    "initial_capital": 1_000_000,
    "commission_rate": 0.001,
    "slippage_rate": 0.001,
    "max_positions": 50,
    "rebalance_freq": "monthly",
    "rebalance_buffer": 0.0,
    "rebalance_buffer_add": None,
    "rebalance_buffer_reduce": None,
    "rebalance_buffer_mode": "all",
    "rebalance_buffer_reference": "target",
    "min_holding_days": 0,
    "reentry_cooldown_days": 0,
    "normalize_target_weights": True,
    "max_position_pct": 0.10,
    "execution_model": "next_open",
    "planned_price_buffer_bps": 50,
    "planned_price_fallback": "cancel",
}
_PER_ORDER_INTENT_COLUMNS = {
    "execution_model",
    "planned_price",
    "planned_price_buffer_bps",
    "planned_price_fallback",
    "price_field",
    "time_in_force",
    "order_reason",
}


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
        stage_domain_write: Any | None = None,
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
        run_started = time.perf_counter()
        setup_started = run_started
        runtime_profile: dict[str, Any] = {"market": resolved_market}

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
        runtime_profile["ticker_count"] = len(tickers)

        strategy_default_config = self._strategy_default_config_from_instance(
            strategy_instance,
            strategy_def,
            kind="default_backtest_config",
        )
        effective_config, config_provenance = self._merge_strategy_run_config(
            strategy_default_config,
            config_dict,
            fallback_position_sizing=strategy_def.get("position_sizing", "equal_weight"),
        )

        position_sizing = effective_config.get("position_sizing", "equal_weight")
        position_sizing = StrategyService._validate_position_sizing(position_sizing)

        # ---- 3. Build config ----
        benchmark = effective_config.get("benchmark") or get_default_benchmark(resolved_market)
        self._validate_benchmark_market(benchmark, resolved_market)
        constraint_config = self._resolve_run_constraint_config(
            strategy_config=strategy_default_config.get("constraint_config")
            or strategy_def.get("constraint_config"),
            config_dict=effective_config,
        )
        warmup_start_date = effective_config.get("warmup_start_date")
        evaluation_start_date = effective_config.get("evaluation_start_date")
        initial_entry_policy = effective_config.get("initial_entry_policy", "wait_for_anchor")
        if initial_entry_policy not in _INITIAL_ENTRY_POLICIES:
            raise ValueError(
                "initial_entry_policy must be one of "
                f"{sorted(_INITIAL_ENTRY_POLICIES)}"
        )
        simulation_start_date = warmup_start_date or effective_config.get("start_date", "2020-01-01")
        normalize_target_weights = effective_config.get("normalize_target_weights", True)
        if (
            position_sizing == "raw_weight"
            and config_provenance.get("normalize_target_weights") == "system_default"
        ):
            normalize_target_weights = False
            effective_config["normalize_target_weights"] = False
        execution_rebalance_buffer = self._resolve_rebalance_buffer(
            effective_config,
            constraint_config,
        )
        execution_min_holding_days = self._resolve_min_holding_days(
            effective_config,
            constraint_config,
        )
        bt_config = BacktestConfig(
            initial_capital=effective_config.get("initial_capital", 1_000_000),
            start_date=simulation_start_date,
            end_date=effective_config.get("end_date", "2024-12-31"),
            market=resolved_market,
            benchmark=benchmark,
            commission_rate=effective_config.get("commission_rate", 0.001),
            slippage_rate=effective_config.get("slippage_rate", 0.001),
            max_positions=effective_config.get("max_positions", 50),
            rebalance_freq=effective_config.get("rebalance_freq") or effective_config.get("rebalance_frequency", "monthly"),
            rebalance_buffer=execution_rebalance_buffer,
            rebalance_buffer_add=effective_config.get("rebalance_buffer_add"),
            rebalance_buffer_reduce=effective_config.get("rebalance_buffer_reduce"),
            rebalance_buffer_mode=effective_config.get("rebalance_buffer_mode", "all"),
            rebalance_buffer_reference=self._resolve_rebalance_buffer_reference(
                effective_config,
                constraint_config,
            ),
            min_holding_days=execution_min_holding_days,
            reentry_cooldown_days=effective_config.get("reentry_cooldown_days", 0),
            normalize_target_weights=normalize_target_weights,
            max_single_name_weight=constraint_config.get("max_single_name_weight"),
            max_holding_days=self._resolve_max_holding_days(constraint_config),
            execution_model=effective_config.get("execution_model", "next_open"),
            planned_price_buffer_bps=effective_config.get("planned_price_buffer_bps", 50),
            planned_price_fallback=effective_config.get("planned_price_fallback", "cancel"),
        )

        max_position_pct = effective_config.get("max_position_pct", 0.10)
        if constraint_config.get("max_single_name_weight") is not None:
            max_position_pct = float(constraint_config["max_single_name_weight"])

        # Warn if strategy has custom weight logic under equal_weight sizing
        weight_warnings = StrategyService._validate_weight_effectiveness(
            strategy_def.get("source_code", ""), position_sizing
        )
        for w in weight_warnings:
            log.warning("backtest_service.weight_ineffective", detail=w)

        start_str = str(bt_config.start_date)
        end_str = str(bt_config.end_date)
        runtime_profile["setup_seconds"] = round(time.perf_counter() - setup_started, 6)

        # ---- 4. Load OHLCV price data ----
        price_started = time.perf_counter()
        prices_close, prices_open, prices_high, prices_low, prices_volume = (
            self._backtest_engine._load_prices(
                tickers, start_str, end_str, market=resolved_market
            )
        )
        runtime_profile["price_load_seconds"] = round(time.perf_counter() - price_started, 6)
        if prices_close.empty:
            raise ValueError("No price data available for the given tickers and date range")

        # ---- 5. Bulk-load required factors ----
        factor_started = time.perf_counter()
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
        runtime_profile["factor_load_seconds"] = round(time.perf_counter() - factor_started, 6)
        runtime_profile["factor_count"] = len(required_factors)
        runtime_profile["computed_factor_count"] = len(factor_data)

        # ---- 6. Build signals for each trading day ----
        debug_state = self._init_debug_replay_state(config_dict, market=resolved_market)
        all_trading_days = sorted(prices_close.index)
        rebalance_dates = self._backtest_engine._get_rebalance_dates(
            all_trading_days, bt_config.rebalance_freq
        )
        rebalance_dates_set = set(rebalance_dates)
        runtime_profile["trading_days"] = len(all_trading_days)
        runtime_profile["rebalance_dates"] = len(rebalance_dates)
        runtime_profile["model_count"] = len(required_models)

        # Build a prices DataFrame with MultiIndex columns (field, ticker)
        # for the StrategyContext — computed once, strategies slice via current_date
        prices_multi = self._build_prices_multi(
            prices_close, prices_open, prices_high, prices_low, prices_volume, tickers,
        )

        # Create weight signals: DataFrame(index=dates, columns=tickers)
        all_weights = pd.DataFrame(0.0, index=prices_close.index, columns=tickers)
        planned_prices = pd.DataFrame(np.nan, index=prices_close.index, columns=tickers)
        planned_price_diagnostics: dict[str, Any] | None = {
            "fallback_count": 0,
            "invalid_count": 0,
            "samples": [],
        }
        execution_overrides = pd.DataFrame(index=prices_close.index, columns=tickers, dtype=object)
        has_execution_overrides = False
        has_planned_price_inputs = bt_config.execution_model == "planned_price"

        # ---- 6a. Pre-compute model predictions for ALL rebalance dates at once ----
        # This avoids per-date DB lookups, model loading, feature computation
        model_preds_by_date: dict[str, dict[str, pd.Series]] = {}
        model_predict_started = time.perf_counter()
        if required_models:
            model_preds_by_date = self._batch_predict_all_dates(
                required_models, tickers, start_str, end_str,
                [d for d in all_trading_days if d in rebalance_dates_set],
                market=resolved_market,
            )
        runtime_profile["model_predict_seconds"] = round(
            time.perf_counter() - model_predict_started,
            6,
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
        port_exit_idx: dict[str, int] = {}
        pending_context_state: dict | None = None

        signal_started = time.perf_counter()
        for day_idx, trade_date in enumerate(all_trading_days):
            trade_ts = pd.Timestamp(trade_date)

            # Update holding_days for all held tickers
            for t in list(port_holding_days):
                port_holding_days[t] += 1
            if pending_context_state is not None:
                self._apply_pending_context_execution_state(
                    pending_context_state,
                    trade_ts=trade_ts,
                    day_idx=day_idx,
                    port_weights=port_weights,
                    port_holding_days=port_holding_days,
                    port_entry_price=port_entry_price,
                    port_exit_idx=port_exit_idx,
                    prices_close=prices_close,
                    prices_open=prices_open,
                    prices_high=prices_high,
                    prices_low=prices_low,
                    planned_prices=planned_prices,
                    execution_overrides=execution_overrides if has_execution_overrides else None,
                    bt_config=bt_config,
                )
                pending_context_state = None

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
                raw_signals = self._normalize_strategy_signals(
                    strategy_instance.generate_signals(context)
                )
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
                if bt_config.execution_model == "planned_price" and planned_price_diagnostics is not None:
                    self._write_planned_prices_for_date(
                        planned_prices=planned_prices,
                        raw_signals=raw_signals,
                        selected_weights={},
                        current_weights=positions_before,
                        prices_close=prices_close,
                        trade_ts=trade_ts,
                        diagnostics=planned_price_diagnostics,
                    )
                prev_weights = all_weights.loc[trade_ts].values
                diag_entry = self._build_rebalance_diagnostics(
                    date_key=date_key,
                    positions_before=positions_before,
                    positions_after={},
                    target_positions_after={},
                    target_layer="strategy_sized",
                    executed_layer="post_constraints",
                    strategy_diagnostics=context.diagnostics,
                )
                diag_entry["phase"] = self._diagnostic_phase(
                    date_key,
                    evaluation_start_date,
                )
                if debug_state:
                    self._record_debug_rebalance(
                        debug_state,
                        date_key=date_key,
                        model_predictions=model_predictions,
                        factor_data=factor_data,
                        raw_signals=raw_signals,
                        target_weights={},
                        adjusted_weights={},
                        context_diagnostics=context.diagnostics,
                        positions_before=positions_before,
                        positions_after={},
                    )
                rebalance_diagnostics.append(diag_entry)
                pending_context_state = {
                    "target_weights": {},
                    "decision_date": trade_ts,
                    "positions_before": positions_before,
                }
                continue

            # Apply position sizing
            weights = self._apply_position_sizing(
                raw_signals,
                position_sizing,
                bt_config.max_positions,
                max_position_pct,
            )
            target_weights = dict(weights)
            weights, constraint_actions = self._apply_weight_constraints(
                weights,
                constraint_config,
            )
            wrote_overrides, wrote_planned_inputs = self._write_execution_overrides_for_date(
                execution_overrides=execution_overrides,
                planned_prices=planned_prices,
                raw_signals=raw_signals,
                selected_weights=weights,
                current_weights=positions_before,
                prices_close=prices_close,
                trade_ts=trade_ts,
                bt_config=bt_config,
                diagnostics=planned_price_diagnostics,
            )
            has_execution_overrides = has_execution_overrides or wrote_overrides
            has_planned_price_inputs = has_planned_price_inputs or wrote_planned_inputs
            if debug_state:
                self._record_debug_rebalance(
                    debug_state,
                    date_key=date_key,
                    model_predictions=model_predictions,
                    factor_data=factor_data,
                    raw_signals=raw_signals,
                    target_weights=target_weights,
                    adjusted_weights=weights,
                    context_diagnostics=context.diagnostics,
                    positions_before=positions_before,
                    positions_after=weights,
                )

            # Write weights for this date
            for ticker, w in weights.items():
                if ticker in all_weights.columns:
                    all_weights.loc[trade_ts, ticker] = w
            prev_weights = all_weights.loc[trade_ts].values

            pending_context_state = {
                "target_weights": weights,
                "decision_date": trade_ts,
                "positions_before": positions_before,
            }

            diag_entry = self._build_rebalance_diagnostics(
                date_key=date_key,
                positions_before=positions_before,
                positions_after=port_weights,
                target_positions_after=target_weights,
                target_layer="strategy_sized",
                executed_layer="post_constraints",
                strategy_diagnostics=context.diagnostics,
            )
            diag_entry["phase"] = self._diagnostic_phase(
                date_key,
                evaluation_start_date,
            )
            if constraint_actions:
                diag_entry["constraint_actions"] = constraint_actions
            rebalance_diagnostics.append(diag_entry)
        runtime_profile["signal_loop_seconds"] = round(time.perf_counter() - signal_started, 6)

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
        engine_started = time.perf_counter()
        run_kwargs: dict[str, Any] = {}
        if has_planned_price_inputs:
            run_kwargs["planned_prices"] = planned_prices
        if has_execution_overrides:
            run_kwargs["execution_overrides"] = execution_overrides
        if run_kwargs:
            overlay_result = self._backtest_engine.run(
                all_weights,
                bt_config,
                **run_kwargs,
            )
        else:
            overlay_result = self._backtest_engine.run(all_weights, bt_config)
        if has_planned_price_inputs and planned_price_diagnostics is not None:
            overlay_result.trade_diagnostics.setdefault("planned_price_inputs", {}).update(
                planned_price_diagnostics
            )
        portfolio_config = effective_config.get("portfolio_overlay")
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
        runtime_profile["engine_seconds"] = round(time.perf_counter() - engine_started, 6)
        rebalance_diagnostics = self._merge_engine_rebalance_diagnostics(
            rebalance_diagnostics,
            (result.trade_diagnostics or {}).get("rebalance_execution_diagnostics"),
        )
        startup_state_report = self._build_startup_state_report(
            rebalance_diagnostics=rebalance_diagnostics,
            warmup_start_date=str(warmup_start_date) if warmup_start_date else None,
            evaluation_start_date=str(evaluation_start_date) if evaluation_start_date else None,
            initial_entry_policy=initial_entry_policy,
        )
        if evaluation_start_date:
            result = self._slice_result_to_evaluation(
                result,
                evaluation_start_date=str(evaluation_start_date),
                evaluation_end_date=str(effective_config.get("end_date", bt_config.end_date)),
                initial_capital=float(bt_config.initial_capital),
            )

        # ---- 8. Determine result_level ----
        # For now, always 'exploratory' since we use yfinance
        result_level = "exploratory"

        # ---- 9. Save to backtest_results table ----
        bt_id = uuid.uuid4().hex[:12]
        config_to_save = bt_config.to_dict()
        config_to_save["universe_group_id"] = universe_group_id
        config_to_save["position_sizing"] = position_sizing
        config_to_save["max_position_pct"] = max_position_pct
        if constraint_config:
            config_to_save["constraint_config"] = constraint_config
        config_to_save["initial_entry_policy"] = initial_entry_policy
        if warmup_start_date:
            config_to_save["warmup_start_date"] = str(warmup_start_date)
            config_to_save["simulation_start_date"] = str(bt_config.start_date)
        if evaluation_start_date:
            config_to_save["evaluation_start_date"] = str(evaluation_start_date)
        requested_start = effective_config.get("start_date", "2020-01-01")
        requested_end = effective_config.get("end_date", "2024-12-31")
        config_to_save["requested_start_date"] = str(requested_start)
        config_to_save["requested_end_date"] = str(requested_end)
        config_to_save["effective_start_date"] = str(bt_config.start_date)
        config_to_save["effective_end_date"] = str(bt_config.end_date)
        if str(requested_start) != str(bt_config.start_date) or str(requested_end) != str(bt_config.end_date):
            config_to_save["date_adjustment"] = {
                "requested_start_date": str(requested_start),
                "effective_start_date": str(bt_config.start_date),
                "requested_end_date": str(requested_end),
                "effective_end_date": str(bt_config.end_date),
                "reason": "calendar_or_data_trading_day_snap",
            }
        if portfolio_config:
            config_to_save["portfolio_overlay"] = result.config.get(
                "portfolio_overlay",
                portfolio_config,
            )
        effective_config_to_save = dict(effective_config)
        effective_config_to_save.update(bt_config.to_dict())
        effective_config_to_save["universe_group_id"] = universe_group_id
        if constraint_config:
            effective_config_to_save["constraint_config"] = constraint_config
        effective_config_to_save["position_sizing"] = position_sizing
        effective_config_to_save["max_position_pct"] = max_position_pct
        effective_config_to_save["initial_entry_policy"] = initial_entry_policy
        if warmup_start_date:
            effective_config_to_save["warmup_start_date"] = str(warmup_start_date)
            effective_config_to_save["simulation_start_date"] = str(bt_config.start_date)
        if evaluation_start_date:
            effective_config_to_save["evaluation_start_date"] = str(evaluation_start_date)
        effective_config_to_save["requested_start_date"] = str(requested_start)
        effective_config_to_save["requested_end_date"] = str(requested_end)
        effective_config_to_save["effective_start_date"] = str(bt_config.start_date)
        effective_config_to_save["effective_end_date"] = str(bt_config.end_date)
        config_to_save["strategy_default_config"] = strategy_default_config
        config_to_save["effective_config"] = effective_config_to_save
        config_to_save["config_provenance"] = config_provenance
        debug_artifact = None
        if debug_state:
            debug_artifact = self._write_debug_replay_bundle(
                backtest_id=bt_id,
                market=resolved_market,
                strategy_id=strategy_id,
                config=config_to_save,
                result=result,
                rebalance_diagnostics=rebalance_diagnostics,
                debug_state=debug_state,
            )
            config_to_save["debug_mode"] = True
            config_to_save["debug_artifact_id"] = debug_artifact["id"]
        save_payload = {
            "bt_id": bt_id,
            "market": resolved_market,
            "strategy_id": strategy_id,
            "config": copy.deepcopy(config_to_save),
            "result": result,
            "result_level": result_level,
        }
        persistence_started = time.perf_counter()
        if callable(stage_domain_write):
            stage_domain_write(
                "backtest_results",
                {
                    "id": bt_id,
                    "market": resolved_market,
                    "strategy_id": strategy_id,
                },
                commit=lambda conn=None, payload=save_payload: self._save_result(
                    **payload,
                    conn=conn,
                ),
            )
        else:
            self._save_result(**save_payload)
        runtime_profile["persistence_seconds"] = round(
            time.perf_counter() - persistence_started,
            6,
        )

        # ---- 10. Return result ----
        postprocess_started = time.perf_counter()
        result_dict = result.to_dict()
        result_dict["backtest_id"] = bt_id
        result_dict["market"] = resolved_market
        result_dict["strategy_id"] = strategy_id
        result_dict["strategy_name"] = strategy_def["name"]
        result_dict["result_level"] = result_level
        result_dict["universe_group_id"] = universe_group_id
        result_dict["config"] = config_to_save
        if debug_artifact:
            result_dict["debug_artifact_id"] = debug_artifact["id"]
            result_dict["debug_artifact_path"] = debug_artifact["path"]
        for key in (
            "requested_start_date",
            "requested_end_date",
            "effective_start_date",
            "effective_end_date",
            "date_adjustment",
            "evaluation_start_date",
            "warmup_start_date",
            "simulation_start_date",
        ):
            if key in config_to_save:
                result_dict[key] = config_to_save[key]
        if signal_errors:
            result_dict["signal_error_count"] = len(signal_errors)
            result_dict["signal_error_samples"] = signal_errors[:5]
        constraint_report = self._build_constraint_report(
            constraint_config=constraint_config,
            rebalance_diagnostics=rebalance_diagnostics,
            trades=result.trades,
            startup_state_report=startup_state_report,
        )
        result_dict["constraint_report"] = constraint_report
        result_dict["constraint_pass"] = constraint_report["constraint_pass"]
        result_dict["failed_constraints"] = constraint_report["failed_constraints"]
        if startup_state_report:
            result_dict["startup_state_report"] = startup_state_report
        if evaluation_start_date:
            result_dict["evaluation_start_date"] = str(evaluation_start_date)
        if warmup_start_date:
            result_dict["warmup_start_date"] = str(warmup_start_date)
        if rebalance_diagnostics:
            result_dict["rebalance_diagnostics"] = rebalance_diagnostics
            portfolio_compliance = self._build_portfolio_compliance_metrics(
                rebalance_diagnostics=rebalance_diagnostics,
                trades=result.trades,
                config=config_to_save,
            )
            result_dict["portfolio_compliance"] = portfolio_compliance
            # Persist diagnostics into stored summary
            if callable(stage_domain_write):
                self._update_staged_result_summary(
                    save_payload,
                    {
                        "rebalance_diagnostics": rebalance_diagnostics,
                        "portfolio_compliance": portfolio_compliance,
                        "constraint_report": constraint_report,
                        "constraint_pass": constraint_report["constraint_pass"],
                        "failed_constraints": constraint_report["failed_constraints"],
                        **({"startup_state_report": startup_state_report} if startup_state_report else {}),
                    },
                )
            else:
                conn = get_connection()
                row = conn.execute(
                    "SELECT summary FROM backtest_results WHERE id = ? AND market = ?",
                    [bt_id, resolved_market],
                ).fetchone()
                if row:
                    summary_data = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
                    summary_data["rebalance_diagnostics"] = rebalance_diagnostics
                    summary_data["portfolio_compliance"] = portfolio_compliance
                    summary_data["constraint_report"] = constraint_report
                    summary_data["constraint_pass"] = constraint_report["constraint_pass"]
                    summary_data["failed_constraints"] = constraint_report["failed_constraints"]
                    if startup_state_report:
                        summary_data["startup_state_report"] = startup_state_report
                    conn.execute(
                        "UPDATE backtest_results SET summary = ? WHERE id = ? AND market = ?",
                        [json.dumps(summary_data, default=str), bt_id, resolved_market],
                    )
        else:
            updates = {
                "constraint_report": constraint_report,
                "constraint_pass": constraint_report["constraint_pass"],
                "failed_constraints": constraint_report["failed_constraints"],
                **({"startup_state_report": startup_state_report} if startup_state_report else {}),
            }
            if callable(stage_domain_write):
                self._update_staged_result_summary(save_payload, updates)
            else:
                self._update_result_summary(
                    bt_id=bt_id,
                    market=resolved_market,
                    updates=updates,
                )

        # ---- 11. Check for data leakage ----
        leakage_warnings = self._check_data_leakage(
            required_models, bt_config, tickers, universe_group_id, market=resolved_market,
        )
        if leakage_warnings:
            result_dict["leakage_warnings"] = leakage_warnings
            # Persist warnings into the stored summary
            if callable(stage_domain_write):
                self._update_staged_result_summary(
                    save_payload,
                    {"leakage_warnings": leakage_warnings},
                )
            else:
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

        runtime_profile["postprocess_seconds"] = round(
            time.perf_counter() - postprocess_started,
            6,
        )
        runtime_profile["total_seconds"] = round(time.perf_counter() - run_started, 6)
        result_dict["runtime_profile"] = dict(runtime_profile)
        if callable(stage_domain_write):
            self._update_staged_result_summary(
                save_payload,
                {"runtime_profile": dict(runtime_profile)},
            )
        else:
            self._update_result_summary(
                bt_id=bt_id,
                market=resolved_market,
                updates={"runtime_profile": dict(runtime_profile)},
            )

        log.info(
            "backtest_service.done",
            backtest_id=bt_id,
            total_return=result.total_return,
            sharpe=result.sharpe_ratio,
            runtime_profile=runtime_profile,
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

    @staticmethod
    def _strategy_default_config_from_instance(
        strategy_instance: Any,
        strategy_def: dict,
        *,
        kind: str,
    ) -> dict:
        raw = getattr(strategy_instance, kind, None)
        if _is_mock_value(raw):
            raw = None
        if raw is None and isinstance(strategy_def, dict):
            raw = strategy_def.get(kind)
        return StrategyService._normalize_strategy_default_config(
            raw or {},
            kind=kind,
        )

    @staticmethod
    def _merge_strategy_run_config(
        strategy_defaults: dict | None,
        run_config: dict | None,
        *,
        fallback_position_sizing: str,
    ) -> tuple[dict, dict]:
        defaults = dict(strategy_defaults or {})
        run = dict(run_config or {})
        if "rebalance_frequency" in defaults and "rebalance_freq" not in defaults:
            defaults["rebalance_freq"] = defaults.get("rebalance_frequency")
        if "rebalance_frequency" in run and "rebalance_freq" not in run:
            run["rebalance_freq"] = run.get("rebalance_frequency")

        merged = dict(_RUN_CONFIG_DEFAULTS)
        provenance = {key: "system_default" for key in merged}
        merged["position_sizing"] = StrategyService._validate_position_sizing(
            fallback_position_sizing
        )
        provenance["position_sizing"] = "strategy_record"

        for key, value in defaults.items():
            if key == "constraint_config":
                continue
            if value is not None:
                merged[key] = value
                provenance[key] = "strategy_default"

        run_constraint_config = run.get("constraint_config")
        for key, value in run.items():
            if key == "constraint_config":
                continue
            if value is not None:
                merged[key] = value
                provenance[key] = "run_override"

        strategy_constraint_config = defaults.get("constraint_config")
        if strategy_constraint_config or run_constraint_config:
            merged["constraint_config"] = BacktestService._merge_constraint_config(
                strategy_constraint_config,
                run_constraint_config,
            )
            provenance["constraint_config"] = (
                "run_override" if run_constraint_config is not None else "strategy_default"
            )

        merged["position_sizing"] = StrategyService._validate_position_sizing(
            merged.get("position_sizing")
        )
        if "execution_model" in merged:
            merged["execution_model"] = normalize_execution_model(merged.get("execution_model"))
        if "planned_price_buffer_bps" in merged:
            merged["planned_price_buffer_bps"] = normalize_planned_price_buffer_bps(
                merged.get("planned_price_buffer_bps")
            )
        if "planned_price_fallback" in merged:
            merged["planned_price_fallback"] = normalize_planned_price_fallback(
                merged.get("planned_price_fallback")
            )
        return merged, provenance

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

    @staticmethod
    def _apply_pending_context_execution_state(
        pending_state: dict,
        *,
        trade_ts: pd.Timestamp,
        day_idx: int,
        port_weights: dict[str, float],
        port_holding_days: dict[str, int],
        port_entry_price: dict[str, float],
        port_exit_idx: dict[str, int],
        prices_open: pd.DataFrame,
        prices_close: pd.DataFrame,
        prices_high: pd.DataFrame,
        prices_low: pd.DataFrame,
        planned_prices: pd.DataFrame | None,
        execution_overrides: pd.DataFrame | None,
        bt_config: BacktestConfig,
    ) -> None:
        """Advance StrategyContext holdings through the same low-turnover layer."""
        target_weights = {
            str(t): float(w)
            for t, w in (pending_state.get("target_weights") or {}).items()
            if float(w) > 1e-8
        }
        effective_targets = dict(target_weights)
        actual_open_weights = BacktestService._context_actual_open_weights(
            port_weights,
            port_entry_price,
            prices_open,
            trade_ts,
        )

        if (
            bt_config.rebalance_buffer > 0
            or bt_config.min_holding_days > 0
            or bt_config.reentry_cooldown_days > 0
            or bt_config.max_holding_days is not None
        ):
            all_tickers = set(port_weights) | set(target_weights)
            for ticker in all_tickers:
                old_w = float(port_weights.get(ticker, 0.0))
                reference_w = old_w
                if bt_config.rebalance_buffer_reference == "actual_open":
                    reference_w = actual_open_weights.get(ticker, 0.0)
                new_w = float(target_weights.get(ticker, 0.0))

                buffer_applies = True
                if bt_config.rebalance_buffer_mode == "hold_overlap_only":
                    buffer_applies = reference_w > 0 and new_w > 0
                direction_buffer = bt_config.rebalance_buffer
                if new_w > reference_w and bt_config.rebalance_buffer_add is not None:
                    direction_buffer = bt_config.rebalance_buffer_add
                elif new_w < reference_w and bt_config.rebalance_buffer_reduce is not None:
                    direction_buffer = bt_config.rebalance_buffer_reduce
                if (
                    direction_buffer > 0
                    and buffer_applies
                    and abs(new_w - reference_w) < direction_buffer
                ):
                    effective_targets[ticker] = reference_w
                    continue

                if (
                    bt_config.min_holding_days > 0
                    and reference_w > 0
                    and new_w < reference_w
                    and port_holding_days.get(ticker, 0) < bt_config.min_holding_days
                ):
                    effective_targets[ticker] = reference_w
                    continue

                if (
                    bt_config.reentry_cooldown_days > 0
                    and reference_w == 0
                    and new_w > 0
                ):
                    exit_idx = port_exit_idx.get(ticker)
                    if (
                        exit_idx is not None
                        and (day_idx - exit_idx) < bt_config.reentry_cooldown_days
                    ):
                        effective_targets.pop(ticker, None)
                        continue

                if bt_config.max_holding_days is not None and reference_w > 0:
                    days_held = port_holding_days.get(ticker, 0)
                    if days_held >= int(bt_config.max_holding_days):
                        effective_targets.pop(ticker, None)
                        continue

            eff_sum = sum(w for w in effective_targets.values() if w > 0)
            if (
                eff_sum > 0
                and bt_config.normalize_target_weights
                and bt_config.rebalance_buffer_mode != "hold_overlap_only"
            ):
                effective_targets = {
                    t: float(w) / eff_sum
                    for t, w in effective_targets.items()
                    if float(w) > 0
                }

        if bt_config.max_single_name_weight is not None:
            effective_targets = backtest_engine_module._cap_absolute_weights(
                effective_targets,
                float(bt_config.max_single_name_weight),
            )

        new_weights = {
            str(t): float(w)
            for t, w in effective_targets.items()
            if float(w) > 1e-8
        }
        new_weights = BacktestService._filter_context_executable_targets(
            current_weights=port_weights,
            target_weights=new_weights,
            decision_date=pending_state.get("decision_date"),
            trade_ts=trade_ts,
            prices_close=prices_close,
            prices_open=prices_open,
            prices_high=prices_high,
            prices_low=prices_low,
            planned_prices=planned_prices,
            execution_overrides=execution_overrides,
            bt_config=bt_config,
        )
        old_tickers = set(port_weights)
        new_tickers = set(new_weights)
        for ticker in old_tickers - new_tickers:
            port_weights.pop(ticker, None)
            port_holding_days.pop(ticker, None)
            port_entry_price.pop(ticker, None)
            port_exit_idx[ticker] = day_idx
        for ticker in new_tickers - old_tickers:
            port_holding_days[ticker] = 0
            port_entry_price[ticker] = (
                BacktestService._context_execution_price(
                    ticker=ticker,
                    decision_ts=pd.Timestamp(pending_state.get("decision_date")),
                    trade_ts=trade_ts,
                    prices_close=prices_close,
                    prices_open=prices_open,
                    prices_high=prices_high,
                    prices_low=prices_low,
                    planned_prices=planned_prices,
                    execution_overrides=execution_overrides,
                    bt_config=bt_config,
                )
                or 0.0
            )
        port_weights.clear()
        port_weights.update(new_weights)

    @staticmethod
    def _filter_context_executable_targets(
        *,
        current_weights: dict[str, float],
        target_weights: dict[str, float],
        decision_date: Any,
        trade_ts: pd.Timestamp,
        prices_close: pd.DataFrame,
        prices_open: pd.DataFrame,
        prices_high: pd.DataFrame,
        prices_low: pd.DataFrame,
        planned_prices: pd.DataFrame | None,
        execution_overrides: pd.DataFrame | None,
        bt_config: BacktestConfig,
    ) -> dict[str, float]:
        executable = dict(current_weights)
        all_tickers = set(current_weights) | set(target_weights)
        decision_ts = pd.Timestamp(decision_date) if decision_date is not None else trade_ts
        for ticker in all_tickers:
            old_w = float(current_weights.get(ticker, 0.0))
            new_w = float(target_weights.get(ticker, 0.0))
            if abs(new_w - old_w) < 1e-8:
                continue
            if BacktestService._context_execution_price(
                ticker=ticker,
                decision_ts=decision_ts,
                trade_ts=trade_ts,
                prices_close=prices_close,
                prices_open=prices_open,
                prices_high=prices_high,
                prices_low=prices_low,
                planned_prices=planned_prices,
                execution_overrides=execution_overrides,
                bt_config=bt_config,
            ) is None:
                continue
            if new_w > 1e-8:
                executable[ticker] = new_w
            else:
                executable.pop(ticker, None)
        return {
            str(ticker): float(weight)
            for ticker, weight in executable.items()
            if float(weight) > 1e-8
        }

    @staticmethod
    def _context_execution_price(
        *,
        ticker: str,
        decision_ts: pd.Timestamp,
        trade_ts: pd.Timestamp,
        prices_close: pd.DataFrame,
        prices_open: pd.DataFrame,
        prices_high: pd.DataFrame,
        prices_low: pd.DataFrame,
        planned_prices: pd.DataFrame | None,
        execution_overrides: pd.DataFrame | None,
        bt_config: BacktestConfig,
    ) -> float | None:
        override = _lookup_execution_override(execution_overrides, decision_ts, ticker)
        execution_model = normalize_execution_model(
            str(override.get("execution_model") or bt_config.execution_model)
        )
        if execution_model == "planned_price":
            planned_price = BacktestService._context_lookup_price(
                planned_prices,
                decision_ts,
                ticker,
            )
            high_price = BacktestService._context_lookup_price(
                prices_high,
                trade_ts,
                ticker,
            )
            low_price = BacktestService._context_lookup_price(
                prices_low,
                trade_ts,
                ticker,
            )
            fill_decision = evaluate_planned_price_fill(
                planned_price=planned_price,
                high=high_price,
                low=low_price,
                buffer_bps=override.get(
                    "planned_price_buffer_bps",
                    bt_config.planned_price_buffer_bps,
                ),
            )
            if fill_decision.filled:
                return fill_decision.fill_price
            if (
                override.get(
                    "planned_price_fallback",
                    bt_config.planned_price_fallback,
                ) == "next_close"
                and fill_decision.reason == "planned_price_outside_buffered_range"
            ):
                return BacktestService._context_lookup_price(
                    prices_close,
                    trade_ts,
                    ticker,
                )
            return None
        if execution_model == "next_close":
            return BacktestService._context_lookup_price(
                prices_close,
                trade_ts,
                ticker,
            )
        return BacktestService._context_lookup_price(
            prices_open,
            trade_ts,
            ticker,
        )

    @staticmethod
    def _context_lookup_price(
        prices: pd.DataFrame | None,
        trade_ts: pd.Timestamp,
        ticker: str,
    ) -> float | None:
        if prices is None or trade_ts not in prices.index or ticker not in prices.columns:
            return None
        value = prices.loc[trade_ts, ticker]
        if pd.isna(value) or value <= 0:
            return None
        return float(value)

    @staticmethod
    def _context_actual_open_weights(
        port_weights: dict[str, float],
        port_entry_price: dict[str, float],
        prices_open: pd.DataFrame,
        trade_ts: pd.Timestamp,
    ) -> dict[str, float]:
        if not port_weights:
            return {}
        raw_values: dict[str, float] = {}
        for ticker, weight in port_weights.items():
            if trade_ts not in prices_open.index or ticker not in prices_open.columns:
                raw_values[ticker] = float(weight)
                continue
            price = prices_open.loc[trade_ts, ticker]
            if pd.notna(price) and price > 0:
                entry_price = float(port_entry_price.get(ticker) or 0.0)
                price_ratio = float(price) / entry_price if entry_price > 0 else 1.0
                raw_values[ticker] = float(weight) * price_ratio
            else:
                raw_values[ticker] = float(weight)
        cash_weight = max(0.0, 1.0 - sum(float(w) for w in port_weights.values() if w > 0))
        total = cash_weight + sum(value for value in raw_values.values() if value > 0)
        if total <= 0:
            return dict(port_weights)
        return {
            ticker: value / total
            for ticker, value in raw_values.items()
            if value > 0
        }

    @staticmethod
    def _context_entry_price(
        ticker: str,
        prices_open: pd.DataFrame,
        trade_ts: pd.Timestamp,
    ) -> float:
        if trade_ts in prices_open.index and ticker in prices_open.columns:
            price = prices_open.loc[trade_ts, ticker]
            if pd.notna(price) and price > 0:
                return float(price)
        return 0.0

    # ------------------------------------------------------------------
    # CRUD for backtest results
    # ------------------------------------------------------------------

    def list_backtests(
        self,
        strategy_id: str | None = None,
        market: str | None = None,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict]:
        """List backtest results (summary only, no heavy series)."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        safe_limit = max(1, min(int(limit), 1000)) if limit is not None else None
        safe_offset = max(0, int(offset or 0))
        if strategy_id:
            params: list[Any] = [resolved_market, strategy_id]
            query = """SELECT id, market, strategy_id, config, summary, trade_count,
                              result_level, created_at
                       FROM backtest_results
                       WHERE market = ? AND strategy_id = ?
                       ORDER BY created_at DESC"""
            if safe_limit is not None:
                query += " LIMIT ? OFFSET ?"
                params.extend([safe_limit, safe_offset])
            rows = conn.execute(
                query,
                params,
            ).fetchall()
        else:
            params = [resolved_market]
            query = """SELECT id, market, strategy_id, config, summary, trade_count,
                              result_level, created_at
                       FROM backtest_results
                       WHERE market = ?
                       ORDER BY created_at DESC"""
            if safe_limit is not None:
                query += " LIMIT ? OFFSET ?"
                params.extend([safe_limit, safe_offset])
            rows = conn.execute(
                query,
                params,
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
        summary = _parse_json(row[4])
        rebalance_diagnostics = []
        if isinstance(summary, dict):
            raw_diagnostics = summary.get("rebalance_diagnostics")
            if isinstance(raw_diagnostics, list):
                rebalance_diagnostics = raw_diagnostics

        detail = {
            "id": row[0],
            "market": row[1],
            "strategy_id": row[2],
            "config": _parse_json(row[3]),
            "summary": summary,
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
        if rebalance_diagnostics:
            detail["rebalance_diagnostics"] = rebalance_diagnostics
            detail["rebalance_diagnostics_count"] = len(rebalance_diagnostics)
        return detail

    def get_rebalance_diagnostics(
        self,
        backtest_id: str,
        market: str | None = None,
        *,
        offset: int = 0,
        limit: int = 200,
    ) -> dict:
        """Return paginated per-rebalance diagnostics for a backtest."""
        resolved_market = normalize_market(market)
        safe_offset = max(0, int(offset))
        safe_limit = min(max(1, int(limit)), 1000)
        conn = get_connection()
        row = conn.execute(
            """SELECT market, summary
               FROM backtest_results
               WHERE id = ? AND market = ?""",
            [backtest_id, resolved_market],
        ).fetchone()
        if row is None:
            raise ValueError(f"Backtest {backtest_id} not found")

        summary = _parse_json(row[1])
        diagnostics = []
        if isinstance(summary, dict) and isinstance(summary.get("rebalance_diagnostics"), list):
            diagnostics = summary["rebalance_diagnostics"]
        total = len(diagnostics)
        items = diagnostics[safe_offset:safe_offset + safe_limit]
        return {
            "backtest_id": backtest_id,
            "market": row[0],
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
            "items": items,
        }

    def get_research_summary(
        self,
        *,
        baseline_backtest_id: str,
        trial_backtest_id: str,
        market: str | None = None,
        changed_variable: dict | None = None,
        conclusion: str | None = None,
        reason: str | None = None,
        max_rebalance_items: int = 20,
    ) -> dict:
        """Return a compact, bounded comparison artifact for agent research."""
        resolved_market = normalize_market(market)
        baseline = self.get_backtest(baseline_backtest_id, market=resolved_market)
        trial = self.get_backtest(trial_backtest_id, market=resolved_market)
        baseline_summary = baseline.get("summary") or {}
        trial_summary = trial.get("summary") or {}
        metric_keys = [
            "total_return",
            "annual_return",
            "sharpe_ratio",
            "max_drawdown",
            "annual_turnover",
            "total_trades",
        ]
        baseline_metrics = {
            key: baseline_summary.get(key)
            for key in metric_keys
            if baseline_summary.get(key) is not None
        }
        trial_metrics = {
            key: trial_summary.get(key)
            for key in metric_keys
            if trial_summary.get(key) is not None
        }
        metric_delta = {}
        for key in metric_keys:
            if baseline_metrics.get(key) is None or trial_metrics.get(key) is None:
                continue
            try:
                metric_delta[key] = round(
                    float(trial_metrics[key]) - float(baseline_metrics[key]),
                    6,
                )
            except (TypeError, ValueError):
                continue

        trial_rebalances = trial_summary.get("rebalance_diagnostics") or []
        if not isinstance(trial_rebalances, list):
            trial_rebalances = []
        safe_limit = max(0, min(int(max_rebalance_items), 200))
        digest_items = [
            self._compact_rebalance_item(item)
            for item in trial_rebalances[:safe_limit]
            if isinstance(item, dict)
        ]
        decision = self._research_decision_from_metrics(
            metric_delta,
            conclusion=conclusion,
            reason=reason,
        )
        return {
            "market": resolved_market,
            "baseline_id": baseline_backtest_id,
            "trial_id": trial_backtest_id,
            "baseline_strategy_id": baseline.get("strategy_id"),
            "trial_strategy_id": trial.get("strategy_id"),
            "changed_variable": changed_variable or {},
            "metrics": {
                "baseline": baseline_metrics,
                "trial": trial_metrics,
            },
            "metric_delta": metric_delta,
            "rebalance_digest": {
                "total": len(trial_rebalances),
                "shown": len(digest_items),
                "items": digest_items,
            },
            "trade_digest": {
                "baseline_trade_count": len(baseline.get("trades") or []),
                "trial_trade_count": len(trial.get("trades") or []),
            },
            "reproducibility_diagnostics": self._compare_reproducibility_fingerprints(
                baseline_summary.get("reproducibility_fingerprint"),
                trial_summary.get("reproducibility_fingerprint"),
            ),
            "decision": decision,
            "size_policy": {
                "max_rebalance_items": safe_limit,
                "heavy_payloads_omitted": True,
            },
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

    def get_debug_replay(
        self,
        backtest_id: str,
        market: str | None = None,
        *,
        date: str | None = None,
        ticker: str | None = None,
    ) -> dict:
        """Load an optional temporary backtest debug replay bundle."""
        resolved_market = normalize_market(market)
        bundle_dir = self._debug_bundle_dir(backtest_id)
        manifest_path = bundle_dir / "manifest.json"
        events_path = bundle_dir / "rebalance.jsonl"
        if not manifest_path.exists() or not events_path.exists():
            raise ValueError(f"Debug replay bundle for backtest {backtest_id} not found")

        manifest = json.loads(manifest_path.read_text())
        if normalize_market(manifest.get("market")) != resolved_market:
            raise ValueError(f"Debug replay bundle for backtest {backtest_id} not found")

        items = []
        for line in events_path.read_text().splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            if date and str(item.get("date"))[:10] != str(date)[:10]:
                continue
            if ticker:
                item = self._filter_debug_item_by_ticker(
                    item,
                    normalize_ticker(ticker, resolved_market),
                )
            items.append(item)
        return {
            "backtest_id": backtest_id,
            "market": resolved_market,
            "manifest": manifest,
            "items": items,
        }

    def cleanup_debug_replay(self, ttl_hours: int = 24) -> dict:
        """Delete expired temporary debug replay bundles."""
        root = self._debug_root()
        if not root.exists():
            return {"deleted": 0, "paths": []}
        cutoff = utc_now_naive() - timedelta(hours=max(0, int(ttl_hours)))
        deleted_paths = []
        for bundle_dir in root.iterdir():
            if not bundle_dir.is_dir():
                continue
            manifest_path = bundle_dir / "manifest.json"
            if not manifest_path.exists():
                continue
            try:
                manifest = json.loads(manifest_path.read_text())
                created_at = datetime.fromisoformat(
                    str(manifest.get("created_at", "")).replace("Z", "")
                )
            except Exception:
                created_at = datetime.fromtimestamp(manifest_path.stat().st_mtime)
            if created_at <= cutoff:
                shutil.rmtree(bundle_dir, ignore_errors=True)
                deleted_paths.append(str(bundle_dir))
        return {"deleted": len(deleted_paths), "paths": deleted_paths}

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
        scalar_keys = {
            "total_return",
            "annual_return",
            "annual_volatility",
            "max_drawdown",
            "sharpe",
            "sharpe_ratio",
            "calmar_ratio",
            "sortino_ratio",
            "win_rate",
            "profit_loss_ratio",
            "total_trades",
            "annual_turnover",
            "total_cost",
            "constraint_pass",
            "start_date",
            "end_date",
            "requested_start_date",
            "requested_end_date",
            "effective_start_date",
            "effective_end_date",
            "evaluation_start_date",
            "warmup_start_date",
            "debug_artifact_id",
        }
        lightweight = {key: summary.get(key) for key in scalar_keys if key in summary}
        compliance = summary.get("portfolio_compliance")
        if isinstance(compliance, dict):
            lightweight["portfolio_compliance"] = {
                key: compliance.get(key)
                for key in (
                    "compliance_pass",
                    "min_position_count",
                    "max_target_weight",
                    "max_trade_holding_days",
                )
                if key in compliance
            }
        runtime_profile = summary.get("runtime_profile")
        if isinstance(runtime_profile, dict):
            runtime_keys = {
                "total_seconds",
                "setup_seconds",
                "price_load_seconds",
                "factor_load_seconds",
                "model_predict_seconds",
                "signal_loop_seconds",
                "engine_seconds",
                "persistence_seconds",
                "postprocess_seconds",
                "ticker_count",
                "trading_days",
                "rebalance_dates",
                "factor_count",
                "computed_factor_count",
                "model_count",
            }
            lightweight["runtime_profile"] = {
                key: runtime_profile.get(key)
                for key in runtime_keys
                if key in runtime_profile
            }
        lightweight["has_rebalance_diagnostics"] = bool(
            summary.get("rebalance_diagnostics")
        )
        lightweight["has_leakage_warnings"] = bool(
            summary.get("leakage_warnings")
        )
        lightweight["has_trade_diagnostics"] = bool(summary.get("trade_diagnostics"))
        lightweight["has_constraint_report"] = bool(summary.get("constraint_report"))
        lightweight["has_startup_state_report"] = bool(summary.get("startup_state_report"))
        lightweight["has_planned_price_execution"] = bool(summary.get("planned_price_execution"))
        lightweight["has_planned_price_inputs"] = bool(summary.get("planned_price_inputs"))
        lightweight["has_fill_diagnostics"] = bool(summary.get("fill_diagnostics"))
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
        target_positions_after: dict[str, float] | None = None,
        target_layer: str = "strategy_target",
        executed_layer: str = "executed",
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
        target_after = {
            str(t): float(w)
            for t, w in (target_positions_after or positions_after).items()
            if abs(float(w)) > 1e-8
        }

        before_tickers = set(before)
        after_tickers = set(after)
        target_tickers = set(target_after)
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
        target_turnover = sum(
            abs(target_after.get(ticker, 0.0) - before.get(ticker, 0.0))
            for ticker in before_tickers | target_tickers
        )

        diag = {
            "date": date_key,
            "positions_before": cls._round_weight_map(before),
            "positions_after": cls._round_weight_map(after),
            "executed_positions_after": cls._round_weight_map(after),
            "target_positions_after": cls._round_weight_map(target_after),
            "added": added,
            "removed": removed,
            "increased": increased,
            "decreased": decreased,
            "turnover": round(float(turnover), 6),
            "target_turnover": round(float(target_turnover), 6),
            "diagnostic_layers": {
                "positions_after": executed_layer,
                "executed_positions_after": executed_layer,
                "target_positions_after": target_layer,
            },
        }
        if strategy_diagnostics:
            diag.update(strategy_diagnostics)
        return diag

    @staticmethod
    def _init_debug_replay_state(config: dict | None, market: str | None = None) -> dict | None:
        config = config or {}
        if not bool(config.get("debug_mode")):
            return None
        resolved_market = normalize_market(market)
        level = str(config.get("debug_level") or "signals").lower()
        if level not in {"summary", "signals", "full"}:
            raise ValueError("debug_level must be one of: summary, signals, full")
        tickers = config.get("debug_tickers")
        dates = config.get("debug_dates")
        return {
            "debug_mode": True,
            "debug_level": level,
            "debug_tickers": (
                {normalize_ticker(t, resolved_market) for t in tickers}
                if isinstance(tickers, list)
                else None
            ),
            "debug_dates": {str(d)[:10] for d in dates} if isinstance(dates, list) else None,
            "rebalance": [],
            "captured_items": 0,
            "skipped_items": 0,
            "skipped_by_date": 0,
        }

    @classmethod
    def _record_debug_rebalance(
        cls,
        debug_state: dict | None,
        *,
        date_key: str,
        model_predictions: dict[str, pd.Series],
        factor_data: dict[str, pd.DataFrame],
        raw_signals: pd.DataFrame,
        target_weights: dict[str, float],
        adjusted_weights: dict[str, float],
        context_diagnostics: dict | None,
        positions_before: dict[str, float],
        positions_after: dict[str, float],
    ) -> None:
        if not debug_state:
            return
        debug_dates = debug_state.get("debug_dates")
        if debug_dates and str(date_key)[:10] not in debug_dates:
            debug_state["skipped_items"] = int(debug_state.get("skipped_items") or 0) + 1
            debug_state["skipped_by_date"] = int(debug_state.get("skipped_by_date") or 0) + 1
            return

        tickers = debug_state.get("debug_tickers")
        level = str(debug_state.get("debug_level") or "signals")
        item = {
            "date": str(date_key)[:10],
            "target_weights": cls._weight_map_for_debug(target_weights, tickers),
            "adjusted_weights": cls._weight_map_for_debug(adjusted_weights, tickers),
            "positions_before": cls._weight_map_for_debug(positions_before, tickers),
            "positions_after": cls._weight_map_for_debug(positions_after, tickers),
            "strategy_diagnostics": _stable_json_value(context_diagnostics or {}),
        }
        if level in {"signals", "full"}:
            item["model_predictions"] = {
                str(model_id): cls._series_to_debug_map(preds, tickers)
                for model_id, preds in (model_predictions or {}).items()
            }
            item["raw_signals"] = cls._frame_to_debug_map(raw_signals, tickers)
        if level == "full":
            item["factor_snapshots"] = cls._factor_snapshots_for_date(
                factor_data,
                date_key,
                tickers,
            )
        debug_state.setdefault("rebalance", []).append(item)
        debug_state["captured_items"] = int(debug_state.get("captured_items") or 0) + 1

    @classmethod
    def _write_debug_replay_bundle(
        cls,
        *,
        backtest_id: str,
        market: str,
        strategy_id: str,
        config: dict,
        result: BacktestResult,
        rebalance_diagnostics: list[dict],
        debug_state: dict | None,
    ) -> dict | None:
        if not debug_state:
            return None
        bundle_dir = cls._debug_bundle_dir(backtest_id)
        bundle_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "id": backtest_id,
            "backtest_id": backtest_id,
            "market": normalize_market(market),
            "strategy_id": strategy_id,
            "created_at": utc_now_iso(),
            "ttl_hours": int(config.get("debug_ttl_hours", 24) or 24),
            "debug_level": debug_state.get("debug_level"),
            "debug_tickers": sorted(debug_state.get("debug_tickers") or []),
            "debug_dates": sorted(debug_state.get("debug_dates") or []),
            "summary": {
                "total_return": result.total_return,
                "sharpe_ratio": result.sharpe_ratio,
                "max_drawdown": result.max_drawdown,
                "total_trades": result.total_trades,
                "dates": len(result.dates),
                "rebalance_diagnostics": len(rebalance_diagnostics or []),
                "captured_items": int(debug_state.get("captured_items") or 0),
                "skipped_items": int(debug_state.get("skipped_items") or 0),
                "skipped_by_date": int(debug_state.get("skipped_by_date") or 0),
            },
            "files": {"rebalance": "rebalance.jsonl"},
        }
        (bundle_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, default=str)
        )
        with open(bundle_dir / "rebalance.jsonl", "w") as f:
            for item in debug_state.get("rebalance", []):
                f.write(json.dumps(_stable_json_value(item), default=str) + "\n")
        return {
            "id": backtest_id,
            "path": str(bundle_dir),
            "manifest_path": str(bundle_dir / "manifest.json"),
        }

    @staticmethod
    def _debug_root() -> Path:
        return settings.project_root / "data" / "backtest_debug"

    @classmethod
    def _debug_bundle_dir(cls, backtest_id: str) -> Path:
        return cls._debug_root() / str(backtest_id)

    @staticmethod
    def _series_to_debug_map(series: pd.Series | dict | None, tickers: set[str] | None) -> dict:
        if series is None:
            return {}
        if isinstance(series, dict):
            items = series.items()
        else:
            items = series.items()
        result = {}
        for ticker, value in items:
            key = str(ticker)
            if tickers and key not in tickers:
                continue
            if value is None or pd.isna(value):
                continue
            result[key] = round(float(value), 6)
        return result

    @staticmethod
    def _frame_to_debug_map(frame: pd.DataFrame | None, tickers: set[str] | None) -> dict:
        if frame is None or frame.empty:
            return {}
        result = {}
        for ticker, row in frame.iterrows():
            key = str(ticker)
            if tickers and key not in tickers:
                continue
            row_data = {}
            for col, value in row.items():
                if value is None or pd.isna(value):
                    continue
                if isinstance(value, (int, float, np.number)):
                    row_data[str(col)] = round(float(value), 6)
                else:
                    row_data[str(col)] = str(value)
            result[key] = row_data
        return result

    @classmethod
    def _factor_snapshots_for_date(
        cls,
        factor_data: dict[str, pd.DataFrame],
        date_key: str,
        tickers: set[str] | None,
    ) -> dict:
        snapshots = {}
        target_date = pd.Timestamp(date_key)
        for factor_name, frame in (factor_data or {}).items():
            if frame is None or frame.empty:
                continue
            available = frame.index[frame.index <= target_date]
            if len(available) == 0:
                continue
            row = frame.loc[available[-1]]
            snapshots[str(factor_name)] = cls._series_to_debug_map(row, tickers)
        return snapshots

    @staticmethod
    def _weight_map_for_debug(weights: dict[str, float] | None, tickers: set[str] | None) -> dict:
        result = {}
        for ticker, value in (weights or {}).items():
            key = str(ticker)
            if tickers and key not in tickers:
                continue
            if value is None or pd.isna(value):
                continue
            result[key] = round(float(value), 6)
        return result

    @staticmethod
    def _filter_debug_item_by_ticker(item: dict, ticker: str) -> dict:
        filtered = dict(item)
        for key in ("raw_signals", "target_weights", "adjusted_weights", "positions_before", "positions_after"):
            value = filtered.get(key)
            if isinstance(value, dict):
                filtered[key] = {ticker: value[ticker]} if ticker in value else {}
        model_predictions = filtered.get("model_predictions")
        if isinstance(model_predictions, dict):
            filtered["model_predictions"] = {
                model_id: ({ticker: values[ticker]} if isinstance(values, dict) and ticker in values else {})
                for model_id, values in model_predictions.items()
            }
        factor_snapshots = filtered.get("factor_snapshots")
        if isinstance(factor_snapshots, dict):
            filtered["factor_snapshots"] = {
                factor: ({ticker: values[ticker]} if isinstance(values, dict) and ticker in values else {})
                for factor, values in factor_snapshots.items()
            }
        return filtered

    @staticmethod
    def _build_portfolio_compliance_metrics(
        *,
        rebalance_diagnostics: list[dict],
        trades: list[dict],
        config: dict | None = None,
    ) -> dict:
        """Summarize portfolio concentration and holding-period compliance."""
        position_counts: list[int] = []
        target_sums: list[float] = []
        max_target_weight = 0.0
        max_trade_holding_days = 0

        for diag in rebalance_diagnostics or []:
            positions = diag.get("positions_after") or {}
            if not isinstance(positions, dict):
                continue
            weights = [
                abs(float(weight))
                for weight in positions.values()
                if weight is not None and abs(float(weight)) > 1e-8
            ]
            position_counts.append(len(weights))
            target_sum = float(sum(weights))
            target_sums.append(target_sum)
            if weights:
                max_target_weight = max(max_target_weight, max(weights))

        for trade in trades or []:
            if str(trade.get("action", "")).lower() != "sell":
                continue
            holding_days = trade.get("holding_days")
            if holding_days is None:
                continue
            try:
                max_trade_holding_days = max(max_trade_holding_days, int(holding_days))
            except (TypeError, ValueError):
                continue

        config = config or {}
        constraint_config = config.get("constraint_config")
        if not isinstance(constraint_config, dict):
            constraint_config = {}
        holding_constraint_active = (
            "compliance_max_holding_days" in config
            or isinstance(constraint_config.get("holding_period"), dict)
            and constraint_config["holding_period"].get("max_days") is not None
        )
        thresholds = {
            "min_position_count": int(config.get("compliance_min_positions", 5)),
            "max_target_weight": float(config.get("compliance_max_target_weight", 0.20)),
            "max_trade_holding_days": int(config.get("compliance_max_holding_days", 21)),
            "max_target_sum": float(config.get("compliance_max_target_sum", 1.000001)),
        }

        min_position_count = min(position_counts) if position_counts else 0
        avg_position_count = (
            sum(position_counts) / len(position_counts) if position_counts else 0.0
        )
        max_target_sum = max(target_sums) if target_sums else 0.0
        avg_target_sum = sum(target_sums) / len(target_sums) if target_sums else 0.0

        violations: dict[str, dict[str, float | int]] = {}
        heuristic_violations: dict[str, dict[str, float | int]] = {}
        if position_counts and min_position_count < thresholds["min_position_count"]:
            violations["min_position_count"] = {
                "actual": min_position_count,
                "required": thresholds["min_position_count"],
            }
        if max_target_weight > thresholds["max_target_weight"] + 1e-8:
            violations["max_target_weight"] = {
                "actual": round(max_target_weight, 6),
                "limit": thresholds["max_target_weight"],
            }
        if max_trade_holding_days > thresholds["max_trade_holding_days"]:
            holding_violation = {
                "actual": max_trade_holding_days,
                "limit": thresholds["max_trade_holding_days"],
            }
            if holding_constraint_active:
                violations["max_trade_holding_days"] = holding_violation
            else:
                heuristic_violations["max_trade_holding_days"] = holding_violation
        if max_target_sum > thresholds["max_target_sum"] + 1e-8:
            violations["max_target_sum"] = {
                "actual": round(max_target_sum, 6),
                "limit": thresholds["max_target_sum"],
            }

        return {
            "min_position_count": min_position_count,
            "avg_position_count": round(float(avg_position_count), 3),
            "max_target_weight": round(float(max_target_weight), 6),
            "max_trade_holding_days": max_trade_holding_days,
            "max_target_sum": round(float(max_target_sum), 6),
            "avg_target_sum": round(float(avg_target_sum), 6),
            "thresholds": thresholds,
            "violations": violations,
            "heuristic_violations": heuristic_violations,
            "compliance_pass": not violations,
        }

    @staticmethod
    def _merge_engine_rebalance_diagnostics(
        service_diagnostics: list[dict],
        engine_diagnostics: Any,
    ) -> list[dict]:
        if not isinstance(engine_diagnostics, list) or not engine_diagnostics:
            return service_diagnostics
        by_date = {
            str(item.get("date"))[:10]: item
            for item in engine_diagnostics
            if isinstance(item, dict) and item.get("date")
        }
        if not by_date:
            return service_diagnostics

        merged: list[dict] = []
        seen_dates: set[str] = set()
        for diag in service_diagnostics or []:
            date_key = str(diag.get("date"))[:10]
            engine_diag = by_date.get(date_key)
            if not engine_diag:
                merged.append(diag)
                continue
            updated = dict(diag)
            updated.update(engine_diag)
            if "phase" in diag and "phase" not in engine_diag:
                updated["phase"] = diag["phase"]
            if "constraint_actions" in diag and "constraint_actions" not in engine_diag:
                updated["constraint_actions"] = diag["constraint_actions"]
            merged.append(updated)
            seen_dates.add(date_key)

        for date_key, engine_diag in by_date.items():
            if date_key not in seen_dates:
                merged.append(engine_diag)
        return merged

    @staticmethod
    def _write_planned_prices_for_date(
        *,
        planned_prices: pd.DataFrame,
        raw_signals: pd.DataFrame,
        selected_weights: dict[str, float],
        current_weights: dict[str, float] | None = None,
        prices_close: pd.DataFrame,
        trade_ts: pd.Timestamp,
        diagnostics: dict[str, Any],
    ) -> None:
        involved_tickers = set(selected_weights) | set(current_weights or {})
        for ticker in sorted(involved_tickers):
            if ticker not in planned_prices.columns:
                continue
            source = "strategy_output"
            planned_price = None
            if "planned_price" in raw_signals.columns and ticker in raw_signals.index:
                planned_price = _positive_float(raw_signals.loc[ticker, "planned_price"])
            if planned_price is None:
                close_price = None
                if trade_ts in prices_close.index and ticker in prices_close.columns:
                    close_price = _positive_float(prices_close.loc[trade_ts, ticker])
                if close_price is None:
                    diagnostics["invalid_count"] = int(diagnostics.get("invalid_count") or 0) + 1
                    continue
                source = "decision_close"
                planned_price = close_price
                diagnostics["fallback_count"] = int(diagnostics.get("fallback_count") or 0) + 1
                raw_value = (
                    raw_signals.loc[ticker, "planned_price"]
                    if "planned_price" in raw_signals.columns and ticker in raw_signals.index
                    else None
                )
                if raw_value is not None and pd.notna(raw_value):
                    diagnostics["invalid_count"] = int(diagnostics.get("invalid_count") or 0) + 1
            planned_prices.loc[trade_ts, ticker] = planned_price
            samples = diagnostics.setdefault("samples", [])
            if len(samples) < 20:
                samples.append(
                    {
                        "date": str(trade_ts.date()),
                        "ticker": str(ticker),
                        "planned_price": round(float(planned_price), 6),
                        "planned_price_source": source,
                    }
                )

    @staticmethod
    def _write_execution_overrides_for_date(
        *,
        execution_overrides: pd.DataFrame,
        planned_prices: pd.DataFrame,
        raw_signals: pd.DataFrame,
        selected_weights: dict[str, float],
        current_weights: dict[str, float] | None,
        prices_close: pd.DataFrame,
        trade_ts: pd.Timestamp,
        bt_config: BacktestConfig,
        diagnostics: dict[str, Any] | None,
    ) -> tuple[bool, bool]:
        involved_tickers = set(selected_weights) | set(current_weights or {})
        if not involved_tickers:
            return False, False

        has_override = False
        has_planned_input = False
        intent_columns = set(raw_signals.columns) & _PER_ORDER_INTENT_COLUMNS
        global_planned = bt_config.execution_model == "planned_price"
        diag = diagnostics if diagnostics is not None else {}

        for ticker in sorted(involved_tickers):
            if ticker not in execution_overrides.columns:
                continue
            has_row = ticker in raw_signals.index
            row = raw_signals.loc[ticker] if has_row else None
            override: dict[str, Any] = {}
            row_model = _string_value(row.get("execution_model")) if row is not None else None
            if row_model:
                override["execution_model"] = normalize_execution_model(row_model)
            row_buffer = row.get("planned_price_buffer_bps") if row is not None else None
            if row_buffer is not None and pd.notna(row_buffer):
                override["planned_price_buffer_bps"] = normalize_planned_price_buffer_bps(
                    row_buffer
                )
            row_fallback = (
                _string_value(row.get("planned_price_fallback"))
                if row is not None
                else None
            )
            if row_fallback:
                override["planned_price_fallback"] = normalize_planned_price_fallback(
                    row_fallback
                )
            for optional_key in ("price_field", "time_in_force", "order_reason"):
                value = _string_value(row.get(optional_key)) if row is not None else None
                if value:
                    override[optional_key] = value

            effective_model = override.get("execution_model", bt_config.execution_model)
            needs_planned_price = effective_model == "planned_price" or global_planned
            if needs_planned_price:
                planned_price, source, invalid = BacktestService._resolve_planned_price_for_signal(
                    raw_signals=raw_signals,
                    ticker=ticker,
                    prices_close=prices_close,
                    trade_ts=trade_ts,
                )
                if planned_price is not None and ticker in planned_prices.columns:
                    planned_prices.at[trade_ts, ticker] = planned_price
                    has_planned_input = True
                    if source == "decision_close":
                        diag["fallback_count"] = int(diag.get("fallback_count") or 0) + 1
                    if invalid:
                        diag["invalid_count"] = int(diag.get("invalid_count") or 0) + 1
                    samples = diag.setdefault("samples", [])
                    if len(samples) < 20:
                        samples.append(
                            {
                                "date": str(trade_ts.date()),
                                "ticker": str(ticker),
                                "planned_price": round(float(planned_price), 6),
                                "planned_price_source": source,
                            }
                        )
                elif needs_planned_price:
                    diag["invalid_count"] = int(diag.get("invalid_count") or 0) + 1
            if intent_columns:
                if override:
                    execution_overrides.at[trade_ts, ticker] = override
                    has_override = True
                elif row_model:
                    has_override = True

        return has_override, has_planned_input

    @staticmethod
    def _resolve_planned_price_for_signal(
        *,
        raw_signals: pd.DataFrame,
        ticker: str,
        prices_close: pd.DataFrame,
        trade_ts: pd.Timestamp,
    ) -> tuple[float | None, str, bool]:
        planned_price = None
        invalid = False
        if "planned_price" in raw_signals.columns and ticker in raw_signals.index:
            raw_value = raw_signals.loc[ticker, "planned_price"]
            planned_price = _positive_float(raw_value)
            invalid = raw_value is not None and pd.notna(raw_value) and planned_price is None
        if planned_price is not None:
            return planned_price, "strategy_output", invalid

        close_price = None
        if trade_ts in prices_close.index and ticker in prices_close.columns:
            close_price = _positive_float(prices_close.loc[trade_ts, ticker])
        if close_price is None:
            return None, "decision_close", invalid
        return close_price, "decision_close", invalid

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
            method: One of 'equal_weight', 'signal_weight', 'max_position', 'raw_weight'.
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

        elif method == "raw_weight":
            weights = {
                ticker: max(0.0, float(weight))
                for ticker, weight in buys["weight"].astype(float).items()
                if pd.notna(weight) and float(weight) > 0
            }

        else:
            raise ValueError(
                "Unsupported position_sizing "
                f"'{method}'. Supported values: {sorted(SUPPORTED_POSITION_SIZING)}"
            )

        return weights

    @staticmethod
    def _normalize_strategy_signals(raw_signals: Any) -> pd.DataFrame:
        """Normalize strategy outputs into the canonical signal DataFrame."""
        if raw_signals is None:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        if isinstance(raw_signals, pd.DataFrame):
            signals = raw_signals.copy()
        elif isinstance(raw_signals, dict):
            if not raw_signals:
                signals = pd.DataFrame(columns=["signal", "weight", "strength"])
            else:
                signals = pd.DataFrame.from_dict(raw_signals, orient="index")
        elif isinstance(raw_signals, list):
            signals = pd.DataFrame(raw_signals)
            if "ticker" in signals.columns:
                signals = signals.set_index("ticker")
        else:
            raise ValueError(
                "Strategy generate_signals must return a DataFrame, dict, or list of signal rows"
            )

        for column, default in (("signal", 0), ("weight", 0.0), ("strength", 0.0)):
            if column not in signals.columns:
                signals[column] = default
        ordered = ["signal", "weight", "strength"] + [
            col for col in signals.columns if col not in {"signal", "weight", "strength"}
        ]
        signals = signals[ordered].copy()
        if signals.index.name is None:
            signals.index.name = "ticker"
        signals["signal"] = pd.to_numeric(signals["signal"], errors="coerce").fillna(0).astype(int)
        signals["weight"] = pd.to_numeric(signals["weight"], errors="coerce").fillna(0.0)
        signals["strength"] = pd.to_numeric(signals["strength"], errors="coerce").fillna(0.0)
        return signals

    @staticmethod
    def _merge_constraint_config(
        strategy_config: dict | None,
        run_config: dict | None,
    ) -> dict:
        """Merge strategy default constraints with per-run overrides."""
        def _clean(raw: dict | None) -> dict:
            return dict(raw) if isinstance(raw, dict) else {}

        def _deep_merge(base: dict, override: dict) -> dict:
            merged = dict(base)
            for key, value in override.items():
                if value is None:
                    continue
                if isinstance(value, dict) and isinstance(merged.get(key), dict):
                    merged[key] = _deep_merge(merged[key], value)
                else:
                    merged[key] = value
            return merged

        merged = _deep_merge(_clean(strategy_config), _clean(run_config))
        normalized: dict = {}
        for key, value in merged.items():
            if value is None:
                continue
            if key in {
                "max_single_name_weight",
                "weekly_turnover_floor",
                "rebalance_drift_buffer",
            }:
                normalized[key] = float(value)
            elif key == "holding_period" and isinstance(value, dict):
                holding: dict = {}
                for hp_key, hp_value in value.items():
                    if hp_value is None:
                        continue
                    if hp_key in {"min_days", "max_days"}:
                        holding[hp_key] = int(hp_value)
                    else:
                        holding[hp_key] = hp_value
                if holding:
                    normalized[key] = holding
            else:
                normalized[key] = value
        return normalized

    @staticmethod
    def _resolve_run_constraint_config(
        *,
        strategy_config: dict | None,
        config_dict: dict | None,
    ) -> dict:
        """Merge strategy constraints, nested run constraints, and legacy top-level run fields."""
        run = config_dict if isinstance(config_dict, dict) else {}
        top_level: dict[str, Any] = {}
        for key in {
            "max_single_name_weight",
            "weekly_turnover_floor",
            "weekly_turnover_exclude_initial",
            "rebalance_drift_buffer",
        }:
            if key in run:
                top_level[key] = run[key]
        if "holding_period" in run:
            top_level["holding_period"] = run["holding_period"]
        merged_run = BacktestService._merge_constraint_config(
            run.get("constraint_config"),
            top_level,
        )
        return BacktestService._merge_constraint_config(strategy_config, merged_run)

    @staticmethod
    def _resolve_rebalance_buffer(config_dict: dict, constraint_config: dict) -> float:
        if "rebalance_buffer" in config_dict:
            return float(config_dict.get("rebalance_buffer") or 0.0)
        if "rebalance_drift_buffer" in constraint_config:
            return float(constraint_config.get("rebalance_drift_buffer") or 0.0)
        return 0.0

    @staticmethod
    def _resolve_rebalance_buffer_reference(
        config_dict: dict,
        constraint_config: dict,
    ) -> str:
        if "rebalance_buffer_reference" in config_dict:
            return str(config_dict.get("rebalance_buffer_reference") or "target")
        if "rebalance_drift_buffer" in constraint_config:
            return "actual_open"
        return "target"

    @staticmethod
    def _resolve_min_holding_days(config_dict: dict, constraint_config: dict) -> int:
        if "min_holding_days" in config_dict:
            return int(config_dict.get("min_holding_days") or 0)
        holding = constraint_config.get("holding_period")
        if isinstance(holding, dict):
            return int(holding.get("min_days") or 0)
        return 0

    @staticmethod
    def _resolve_max_holding_days(constraint_config: dict) -> int | None:
        holding = constraint_config.get("holding_period")
        if isinstance(holding, dict) and holding.get("max_days") is not None:
            return int(holding["max_days"])
        return None

    @staticmethod
    def _apply_weight_constraints(
        weights: dict[str, float],
        constraint_config: dict | None,
    ) -> tuple[dict[str, float], dict]:
        """Apply target-level hard constraints and return audit actions."""
        if not weights:
            return {}, {}
        config = constraint_config if isinstance(constraint_config, dict) else {}
        max_weight = config.get("max_single_name_weight")
        raw_target_sum = sum(max(0.0, float(weight)) for weight in weights.values())
        if max_weight is None:
            actions = {}
            if raw_target_sum > 1.000001:
                actions["raw_target_sum"] = round(float(raw_target_sum), 6)
                actions["constrained_target_sum"] = round(float(raw_target_sum), 6)
                actions["target_sum_limit"] = 1.0
            return dict(weights), actions

        limit = float(max_weight)
        clipped: dict[str, dict[str, float]] = {}
        constrained: dict[str, float] = {}
        for ticker, weight in weights.items():
            raw = max(0.0, float(weight))
            capped = min(raw, limit)
            if raw > limit + 1e-10:
                clipped[str(ticker)] = {
                    "raw": round(raw, 6),
                    "clipped": round(capped, 6),
                }
            if capped > 1e-8:
                constrained[str(ticker)] = capped

        constrained_target_sum = sum(constrained.values())
        actions = {}
        if clipped:
            actions.update({
                "max_single_name_weight": limit,
                "clipped": clipped,
            })
        if raw_target_sum > 1.000001 or constrained_target_sum > 1.000001:
            actions["raw_target_sum"] = round(float(raw_target_sum), 6)
            actions["constrained_target_sum"] = round(float(constrained_target_sum), 6)
            actions["target_sum_limit"] = 1.0
        return constrained, actions

    @staticmethod
    def _diagnostic_phase(date_key: str, evaluation_start_date: str | None) -> str:
        if not evaluation_start_date:
            return "evaluation"
        try:
            return "warmup" if date.fromisoformat(str(date_key)[:10]) < date.fromisoformat(str(evaluation_start_date)[:10]) else "evaluation"
        except ValueError:
            return "evaluation"

    @staticmethod
    def _build_constraint_report(
        *,
        constraint_config: dict | None,
        rebalance_diagnostics: list[dict],
        trades: list[dict],
        startup_state_report: dict | None,
    ) -> dict:
        """Build a compact hard-constraint report for backtest summaries."""
        config = constraint_config if isinstance(constraint_config, dict) else {}
        failed: list[str] = []

        max_observed_weight = 0.0
        clipped_events = 0
        max_raw_target_sum = 0.0
        max_constrained_target_sum = 0.0
        target_sum_dates: list[dict[str, Any]] = []
        for diag in rebalance_diagnostics or []:
            positions = diag.get("positions_after") or {}
            if isinstance(positions, dict):
                for weight in positions.values():
                    try:
                        max_observed_weight = max(max_observed_weight, abs(float(weight)))
                    except (TypeError, ValueError):
                        pass
            actions = diag.get("constraint_actions") or {}
            if isinstance(actions, dict):
                clipped = actions.get("clipped") or {}
                if isinstance(clipped, dict):
                    clipped_events += len(clipped)
                try:
                    raw_sum = float(actions.get("raw_target_sum") or 0.0)
                    constrained_sum = float(actions.get("constrained_target_sum") or 0.0)
                except (TypeError, ValueError):
                    raw_sum = 0.0
                    constrained_sum = 0.0
                max_raw_target_sum = max(max_raw_target_sum, raw_sum)
                max_constrained_target_sum = max(max_constrained_target_sum, constrained_sum)
                if raw_sum > 1.000001 or constrained_sum > 1.000001:
                    target_sum_dates.append({
                        "date": str(diag.get("date"))[:10],
                        "raw_target_sum": round(float(raw_sum), 6),
                        "constrained_target_sum": round(float(constrained_sum), 6),
                    })

        max_single_report = None
        if config.get("max_single_name_weight") is not None:
            limit = float(config["max_single_name_weight"])
            max_single_report = {
                "limit": limit,
                "max_observed": round(float(max_observed_weight), 6),
                "clipped_events": clipped_events,
                "pass": max_observed_weight <= limit + 1e-8,
            }
            if not max_single_report["pass"]:
                failed.append("max_single_name_weight")

        weekly_report = None
        if config.get("weekly_turnover_floor") is not None:
            floor = float(config["weekly_turnover_floor"])
            exclude_initial = bool(config.get("weekly_turnover_exclude_initial", True))
            weekly: dict[tuple[int, int], float] = {}
            for diag in rebalance_diagnostics or []:
                if diag.get("phase") == "warmup":
                    continue
                try:
                    d = date.fromisoformat(str(diag.get("date"))[:10])
                except ValueError:
                    continue
                iso = d.isocalendar()
                key = (iso.year, iso.week)
                weekly[key] = weekly.get(key, 0.0) + float(diag.get("turnover") or 0.0)
            weeks = []
            for idx, key in enumerate(sorted(weekly)):
                excluded = exclude_initial and idx == 0
                turnover = weekly[key]
                passed = excluded or turnover >= floor - 1e-8
                weeks.append({
                    "year": key[0],
                    "week": key[1],
                    "turnover": round(float(turnover), 6),
                    "excluded": excluded,
                    "pass": passed,
                })
            weekly_report = {
                "floor": floor,
                "exclude_initial": exclude_initial,
                "weeks": weeks,
                "pass": all(item["pass"] for item in weeks),
            }
            if not weekly_report["pass"]:
                failed.append("weekly_turnover_floor")

        holding_report = None
        holding_cfg = config.get("holding_period")
        if isinstance(holding_cfg, dict):
            sell_holding_days: list[int] = []
            for trade in trades or []:
                if str(trade.get("action", "")).lower() != "sell":
                    continue
                try:
                    sell_holding_days.append(int(trade.get("holding_days") or 0))
                except (TypeError, ValueError):
                    pass
            min_days = holding_cfg.get("min_days")
            max_days = holding_cfg.get("max_days")
            min_violation = (
                min_days is not None
                and any(days < int(min_days) for days in sell_holding_days)
            )
            max_violation = (
                max_days is not None
                and any(days > int(max_days) for days in sell_holding_days)
            )
            holding_report = {
                "min_days": min_days,
                "max_days": max_days,
                "target_bucket": holding_cfg.get("target_bucket"),
                "position_level": {
                    "sell_count": len(sell_holding_days),
                    "min_observed": min(sell_holding_days) if sell_holding_days else None,
                    "max_observed": max(sell_holding_days) if sell_holding_days else None,
                },
                "lot_level": {
                    "status": "approximated_from_engine_trade_log",
                },
                "pass": not (min_violation or max_violation),
            }
            if not holding_report["pass"]:
                failed.append("holding_period")

        startup_report = startup_state_report if isinstance(startup_state_report, dict) else None
        if startup_report and startup_report.get("startup_silence_violation"):
            failed.append("startup_silence")

        budget_report = None
        if max_raw_target_sum > 0.0 or max_constrained_target_sum > 0.0:
            budget_report = {
                "limit": 1.0,
                "max_raw_target_sum": round(float(max_raw_target_sum), 6),
                "max_constrained_target_sum": round(float(max_constrained_target_sum), 6),
                "violation_dates": target_sum_dates[:20],
                "pass": max_raw_target_sum <= 1.000001 and max_constrained_target_sum <= 1.000001,
            }
            if not budget_report["pass"]:
                failed.append("target_weight_budget")

        return {
            "constraint_pass": not failed,
            "failed_constraints": failed,
            "max_single_name_weight": max_single_report,
            "weekly_turnover": weekly_report,
            "holding_period": holding_report,
            "target_weight_budget": budget_report,
            "rebalance_drift_buffer": config.get("rebalance_drift_buffer"),
            "startup_state_report": startup_report,
        }

    @staticmethod
    def _build_startup_state_report(
        *,
        rebalance_diagnostics: list[dict],
        warmup_start_date: str | None,
        evaluation_start_date: str | None,
        initial_entry_policy: str,
    ) -> dict | None:
        if not evaluation_start_date:
            return None

        eval_start = date.fromisoformat(str(evaluation_start_date)[:10])
        last_before_eval: dict | None = None
        first_eval: dict | None = None
        empty_wait_count = 0
        anchor_blocked_count = 0

        for diag in rebalance_diagnostics or []:
            try:
                diag_date = date.fromisoformat(str(diag.get("date"))[:10])
            except ValueError:
                continue
            if diag_date < eval_start:
                last_before_eval = diag
                continue
            if first_eval is None:
                first_eval = diag
            before = diag.get("positions_before") or {}
            after = diag.get("positions_after") or {}
            if not before and not after:
                empty_wait_count += 1
            if diag.get("wait_for_anchor") or diag.get("reason") == "wait_for_anchor":
                anchor_blocked_count += 1

        start_positions = (
            last_before_eval.get("positions_after")
            if isinstance(last_before_eval, dict)
            else {}
        ) or {}
        first_before = (
            first_eval.get("positions_before")
            if isinstance(first_eval, dict)
            else {}
        ) or {}
        first_after = (
            first_eval.get("positions_after")
            if isinstance(first_eval, dict)
            else {}
        ) or {}

        first_before_count = len(first_before) if isinstance(first_before, dict) else 0
        startup_silence_violation = False
        if initial_entry_policy == "require_warmup_state":
            startup_silence_violation = first_before_count == 0
        elif empty_wait_count > 0 and initial_entry_policy in {"bootstrap_from_history", "open_immediately"}:
            startup_silence_violation = True

        return {
            "warmup_start_date": warmup_start_date,
            "evaluation_start_date": evaluation_start_date,
            "initial_entry_policy": initial_entry_policy,
            "evaluation_start_position_count": len(start_positions) if isinstance(start_positions, dict) else 0,
            "first_evaluation_rebalance_date": first_eval.get("date") if isinstance(first_eval, dict) else None,
            "first_evaluation_positions_before_count": first_before_count,
            "first_evaluation_positions_after_count": len(first_after) if isinstance(first_after, dict) else 0,
            "first_evaluation_turnover": first_eval.get("turnover") if isinstance(first_eval, dict) else None,
            "empty_wait_rebalance_count": empty_wait_count,
            "anchor_blocked_count": anchor_blocked_count,
            "startup_silence_violation": startup_silence_violation,
        }

    @staticmethod
    def _slice_result_to_evaluation(
        result: BacktestResult,
        *,
        evaluation_start_date: str,
        evaluation_end_date: str | None,
        initial_capital: float,
    ) -> BacktestResult:
        eval_start = date.fromisoformat(str(evaluation_start_date)[:10])
        eval_end = date.fromisoformat(str(evaluation_end_date)[:10]) if evaluation_end_date else None
        keep_indices: list[int] = []
        for idx, date_key in enumerate(result.dates):
            try:
                d = date.fromisoformat(str(date_key)[:10])
            except ValueError:
                continue
            if d < eval_start:
                continue
            if eval_end and d > eval_end:
                continue
            keep_indices.append(idx)

        if not keep_indices:
            return result

        dates = [result.dates[idx] for idx in keep_indices]
        base_nav = float(result.nav[keep_indices[0]]) if result.nav else initial_capital
        base_benchmark = (
            float(result.benchmark_nav[keep_indices[0]])
            if result.benchmark_nav and len(result.benchmark_nav) > keep_indices[0]
            else initial_capital
        )
        nav = [
            round(float(result.nav[idx]) / base_nav * initial_capital, 2)
            if base_nav
            else initial_capital
            for idx in keep_indices
        ]
        benchmark_nav = [
            round(float(result.benchmark_nav[idx]) / base_benchmark * initial_capital, 2)
            if base_benchmark and result.benchmark_nav and len(result.benchmark_nav) > idx
            else initial_capital
            for idx in keep_indices
        ]

        nav_arr = np.array(nav, dtype=float)
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

        eval_trades = []
        warmup_trades_excluded = False
        for trade in result.trades or []:
            try:
                trade_date = date.fromisoformat(str(trade.get("date"))[:10])
            except ValueError:
                continue
            if trade_date < eval_start:
                warmup_trades_excluded = True
                continue
            if eval_end and trade_date > eval_end:
                continue
            eval_trades.append(trade)

        win_rate, pl_ratio = backtest_engine_module._calc_trade_stats(eval_trades)
        total_cost = sum(float(t.get("cost") or 0.0) for t in eval_trades)
        trade_value = sum(
            abs(float(t.get("shares") or 0.0) * float(t.get("price") or 0.0))
            for t in eval_trades
        )
        annual_turnover = (trade_value / initial_capital / years) if years > 0 and initial_capital > 0 else 0.0
        trade_diagnostics = backtest_engine_module._calc_trade_diagnostics(eval_trades)
        trade_diagnostics["evaluation_slice"] = {
            "warmup_start_date": result.config.get("start_date"),
            "evaluation_start_date": evaluation_start_date,
            "evaluation_end_date": str(evaluation_end_date) if evaluation_end_date else None,
            "warmup_trades_excluded": warmup_trades_excluded,
        }
        if isinstance(result.trade_diagnostics, dict) and "target_weight_policy" in result.trade_diagnostics:
            trade_diagnostics["target_weight_policy"] = result.trade_diagnostics["target_weight_policy"]

        config = dict(result.config)
        config["evaluation_start_date"] = evaluation_start_date
        if evaluation_end_date:
            config["evaluation_end_date"] = str(evaluation_end_date)
        config["evaluation_slice_applied"] = True

        return BacktestResult(
            config=config,
            dates=dates,
            nav=nav,
            benchmark_nav=benchmark_nav,
            drawdown=[round(d, 6) for d in drawdown_series.tolist()] if len(drawdown_series) > 0 else [],
            total_return=round(float(total_return), 6),
            annual_return=round(float(annual_return), 6),
            annual_volatility=round(float(annual_volatility), 6),
            max_drawdown=round(max_dd, 6),
            sharpe_ratio=round(float(sharpe), 4),
            calmar_ratio=round(float(calmar), 4),
            sortino_ratio=round(float(sortino), 4),
            win_rate=round(float(win_rate), 4),
            profit_loss_ratio=round(float(pl_ratio), 4),
            total_trades=len(eval_trades),
            annual_turnover=round(float(annual_turnover), 4),
            total_cost=round(float(total_cost), 2),
            monthly_returns=backtest_engine_module._calc_monthly_returns(dates, nav),
            trades=eval_trades,
            trade_diagnostics=trade_diagnostics,
        )

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
        feature_matrix_cache: dict[str, pd.DataFrame] = {}

        for model_id in model_ids:
            try:
                record = self._model_service.get_model(model_id, market=resolved_market)
                model_instance = self._model_service.load_model(
                    model_id, market=resolved_market
                )
                fs_id = record["feature_set_id"]

                if fs_id in feature_matrix_cache:
                    X = feature_matrix_cache[fs_id]
                else:
                    lock_key = self._prediction_feature_lock_key(
                        market=resolved_market,
                        feature_set_id=fs_id,
                        tickers=tickers,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    with _prediction_cache_lock(lock_key):
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
                        feature_matrix_cache[fs_id] = X

                if X.empty:
                    log.warning("backtest_service.batch_predict.empty_X", model_id=model_id)
                    continue

                load_frozen = getattr(self._model_service, "_load_frozen_features", None)
                frozen = load_frozen(model_id) if callable(load_frozen) else None
                if frozen:
                    align_frozen = getattr(self._model_service, "_align_features_to_frozen", None)
                    if not callable(align_frozen):
                        raise ValueError(
                            f"Model {model_id} has frozen features but model service cannot align them"
                        )
                    X_model = align_frozen(
                        X,
                        frozen,
                        model_id,
                    )
                else:
                    X_model = X

                # Run prediction on entire X at once
                all_preds = model_instance.predict(X_model)
                all_preds.index = X_model.index

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
    def _prediction_feature_lock_key(
        *,
        market: str,
        feature_set_id: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> tuple:
        return (
            market,
            feature_set_id,
            tuple(sorted(str(ticker) for ticker in tickers)),
            str(start_date),
            str(end_date),
        )

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

            model_window_end = model_train_end
            label_horizon = self._extract_model_label_horizon(record, model_id)
            if label_horizon > 0:
                try:
                    model_train_end = offset_trading_days(
                        model_window_end,
                        label_horizon,
                        market=resolved_market,
                    )
                except Exception:
                    model_train_end = model_window_end

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
                if label_horizon > 0 and model_train_end != model_window_end:
                    detail_parts.append(
                        f"模型标签 horizon={label_horizon}，窗口截止 {model_window_end} "
                        f"实际需要看到 {model_train_end} 的价格"
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
                "model_window_end": str(model_window_end),
                "label_horizon": label_horizon,
                "backtest_start": str(bt_start),
                "details": "；".join(detail_parts),
            })

        return warnings

    @staticmethod
    def _extract_model_label_horizon(record: dict, model_id: str | None = None) -> int:
        candidates: list[Any] = []
        for container in (
            record,
            record.get("eval_metrics") if isinstance(record.get("eval_metrics"), dict) else None,
            record.get("train_config") if isinstance(record.get("train_config"), dict) else None,
        ):
            if not isinstance(container, dict):
                continue
            candidates.extend([
                container.get("effective_label_horizon"),
                container.get("label_horizon"),
                container.get("horizon"),
            ])
            label_summary = container.get("label_summary")
            if isinstance(label_summary, dict):
                candidates.extend([
                    label_summary.get("effective_horizon"),
                    label_summary.get("effective_label_horizon"),
                    label_summary.get("horizon"),
                    label_summary.get("label_horizon"),
                ])

        try:
            meta_candidates = BacktestService._read_model_metadata_label_horizon(model_id)
            candidates.extend(meta_candidates)
        except Exception:
            pass

        max_horizon = 0
        for value in candidates:
            if value is None:
                continue
            try:
                max_horizon = max(max_horizon, int(value))
            except (TypeError, ValueError):
                continue
        return max(0, max_horizon)

    @staticmethod
    def _read_model_metadata_label_horizon(model_id: str | None) -> list[Any]:
        if not model_id:
            return []
        meta_path = settings.models_dir / model_id / "metadata.json"
        if not meta_path.exists():
            return []
        meta = json.loads(meta_path.read_text())
        candidates = [
            meta.get("effective_label_horizon"),
            meta.get("label_horizon"),
            meta.get("horizon"),
        ]
        label_summary = meta.get("label_summary")
        if isinstance(label_summary, dict):
            candidates.extend([
                label_summary.get("effective_horizon"),
                label_summary.get("effective_label_horizon"),
                label_summary.get("horizon"),
                label_summary.get("label_horizon"),
            ])
        eval_metrics = meta.get("eval_metrics")
        if isinstance(eval_metrics, dict):
            candidates.extend([
                eval_metrics.get("effective_label_horizon"),
                eval_metrics.get("label_horizon"),
                eval_metrics.get("horizon"),
            ])
            metric_label_summary = eval_metrics.get("label_summary")
            if isinstance(metric_label_summary, dict):
                candidates.extend([
                    metric_label_summary.get("effective_horizon"),
                    metric_label_summary.get("effective_label_horizon"),
                    metric_label_summary.get("horizon"),
                    metric_label_summary.get("label_horizon"),
                ])
        return candidates

    def _save_result(
        self,
        bt_id: str,
        market: str,
        strategy_id: str,
        config: dict,
        result: BacktestResult,
        result_level: str,
        conn: Any | None = None,
    ) -> None:
        """Persist backtest result to DuckDB."""
        conn = conn or get_connection()
        now = utc_now_naive()

        resolved_market = normalize_market(market)
        trade_diagnostics = dict(result.trade_diagnostics or {})
        staged_summary_updates = trade_diagnostics.pop("staged_summary_updates", None)
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
            "trade_diagnostics": trade_diagnostics,
        }
        summary["reproducibility_fingerprint"] = self._build_reproducibility_fingerprint(
            strategy_id=strategy_id,
            market=resolved_market,
            config=config,
            result=result,
        )
        if isinstance(staged_summary_updates, dict):
            summary.update(staged_summary_updates)

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
        runtime_state = _git_runtime_state()
        payload["runtime"] = runtime_state
        comparability_warnings = []
        if runtime_state.get("dirty"):
            comparability_warnings.append("dirty_worktree")
        payload["comparability"] = {
            "clean_runtime": not bool(runtime_state.get("dirty")),
            "warnings": comparability_warnings,
        }
        payload["hash"] = _fingerprint_hash(payload)
        return payload

    @classmethod
    def _compare_reproducibility_fingerprints(
        cls,
        baseline: dict | None,
        trial: dict | None,
    ) -> dict:
        if not isinstance(baseline, dict) or not isinstance(trial, dict):
            return {
                "available": False,
                "strictly_comparable": False,
                "compatibility_flag": "missing_fingerprint",
                "difference_sources": ["missing_fingerprint"],
                "field_diffs": {},
                "result_shape_delta": {},
            }

        field_diffs: dict[str, Any] = {}
        difference_sources: list[str] = []

        def mark(source: str) -> None:
            if source not in difference_sources:
                difference_sources.append(source)

        scalar_fields = {
            "schema_version": "fingerprint_schema",
            "service_version": "service_version",
            "git_commit": "backend_commit",
            "market": "market",
        }
        for field, source in scalar_fields.items():
            if baseline.get(field) != trial.get(field):
                field_diffs[field] = {
                    "baseline": baseline.get(field),
                    "trial": trial.get(field),
                }
                mark(source)

        section_sources = {
            "strategy": "strategy_source",
            "dependencies": "dependency_hash",
            "config": "config",
            "data_watermark": "data_watermark",
            "runtime": "runtime",
            "result_shape": "result_shape",
        }
        for section, source in section_sources.items():
            base_value = _stable_json_value(baseline.get(section))
            trial_value = _stable_json_value(trial.get(section))
            if base_value != trial_value:
                field_diffs[section] = {
                    "baseline": baseline.get(section),
                    "trial": trial.get(section),
                }
                mark(source)

        result_shape_delta = cls._result_shape_delta(
            baseline.get("result_shape"),
            trial.get("result_shape"),
        )
        hash_equal = bool(baseline.get("hash")) and baseline.get("hash") == trial.get("hash")
        strictly_comparable = bool(hash_equal and not difference_sources)
        compatibility_flag = cls._fingerprint_compatibility_flag(difference_sources)
        return {
            "available": True,
            "strictly_comparable": strictly_comparable,
            "compatibility_flag": compatibility_flag,
            "difference_sources": difference_sources,
            "field_diffs": field_diffs,
            "result_shape_delta": result_shape_delta,
            "hashes": {
                "baseline": baseline.get("hash"),
                "trial": trial.get("hash"),
            },
        }

    @staticmethod
    def _result_shape_delta(
        baseline_shape: Any,
        trial_shape: Any,
    ) -> dict[str, int | float]:
        if not isinstance(baseline_shape, dict) or not isinstance(trial_shape, dict):
            return {}
        result = {}
        for key in sorted(set(baseline_shape) | set(trial_shape)):
            try:
                result[str(key)] = (
                    float(trial_shape.get(key) or 0)
                    - float(baseline_shape.get(key) or 0)
                )
            except (TypeError, ValueError):
                continue
            if float(result[str(key)]).is_integer():
                result[str(key)] = int(result[str(key)])
        return result

    @staticmethod
    def _fingerprint_compatibility_flag(difference_sources: list[str]) -> str:
        sources = set(difference_sources)
        input_sources = {
            "market",
            "strategy_source",
            "dependency_hash",
            "config",
            "data_watermark",
            "runtime",
        }
        if not sources:
            return "same_fingerprint"
        if sources & input_sources:
            return "input_or_runtime_changed"
        if "backend_commit" in sources or "service_version" in sources:
            if sources <= {"backend_commit", "service_version", "result_shape"}:
                return "backend_change_same_inputs"
            return "backend_or_service_changed"
        if "result_shape" in sources:
            return "result_shape_changed_same_fingerprint_inputs"
        return "unknown_difference"

    @staticmethod
    def _compact_rebalance_item(item: dict) -> dict:
        positions_after = item.get("positions_after") or {}
        target_after = item.get("target_positions_after") or {}
        if not isinstance(positions_after, dict):
            positions_after = {}
        if not isinstance(target_after, dict):
            target_after = {}
        return {
            "date": item.get("date"),
            "phase": item.get("phase"),
            "added": list(item.get("added") or [])[:20],
            "removed": list(item.get("removed") or [])[:20],
            "increased": list(item.get("increased") or [])[:20],
            "decreased": list(item.get("decreased") or [])[:20],
            "turnover": item.get("turnover"),
            "target_turnover": item.get("target_turnover"),
            "position_count": len(positions_after),
            "target_position_count": len(target_after),
            "top_positions_after": BacktestService._top_weight_items(positions_after, limit=10),
        }

    @staticmethod
    def _top_weight_items(weights: dict, *, limit: int) -> list[dict]:
        items = []
        for ticker, weight in weights.items():
            try:
                items.append({"ticker": str(ticker), "weight": round(float(weight), 6)})
            except (TypeError, ValueError):
                continue
        return sorted(items, key=lambda item: abs(item["weight"]), reverse=True)[:limit]

    @staticmethod
    def _research_decision_from_metrics(
        metric_delta: dict,
        *,
        conclusion: str | None,
        reason: str | None,
    ) -> dict:
        normalized = (conclusion or "").strip().lower()
        if normalized not in {"promote", "continue", "stop"}:
            sharpe_delta = metric_delta.get("sharpe_ratio")
            return_delta = metric_delta.get("total_return")
            drawdown_delta = metric_delta.get("max_drawdown")
            if (
                sharpe_delta is not None
                and return_delta is not None
                and float(sharpe_delta) > 0
                and float(return_delta) > 0
                and (drawdown_delta is None or float(drawdown_delta) >= 0)
            ):
                normalized = "promote"
            elif (
                sharpe_delta is not None
                and return_delta is not None
                and float(sharpe_delta) < 0
                and float(return_delta) <= 0
            ):
                normalized = "stop"
            else:
                normalized = "continue"
        return {
            "conclusion": normalized,
            "reason": reason or "derived_from_metric_delta",
        }

    @staticmethod
    def _update_result_summary(
        *,
        bt_id: str,
        market: str,
        updates: dict,
    ) -> None:
        conn = get_connection()
        row = conn.execute(
            "SELECT summary FROM backtest_results WHERE id = ? AND market = ?",
            [bt_id, market],
        ).fetchone()
        if not row:
            return
        summary_data = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
        summary_data.update(updates)
        conn.execute(
            "UPDATE backtest_results SET summary = ? WHERE id = ? AND market = ?",
            [json.dumps(summary_data, default=str), bt_id, market],
        )

    @staticmethod
    def _update_staged_result_summary(
        save_payload: dict,
        updates: dict,
    ) -> None:
        result = save_payload.get("result")
        if not isinstance(result, BacktestResult):
            return
        trade_diagnostics = dict(result.trade_diagnostics or {})
        staged_summary_updates = trade_diagnostics.setdefault(
            "staged_summary_updates",
            {},
        )
        staged_summary_updates.update(updates)
        result.trade_diagnostics = trade_diagnostics


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


def _string_value(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _lookup_execution_override(
    execution_overrides: pd.DataFrame | None,
    decision_date: pd.Timestamp,
    ticker: str,
) -> dict[str, Any]:
    if execution_overrides is None:
        return {}
    if decision_date not in execution_overrides.index or ticker not in execution_overrides.columns:
        return {}
    value = execution_overrides.loc[decision_date, ticker]
    return dict(value) if isinstance(value, dict) else {}


def _is_mock_value(value: Any) -> bool:
    return value is not None and value.__class__.__module__.startswith("unittest.mock")


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


def _git_runtime_state() -> dict:
    try:
        diff_result = subprocess.run(
            [
                "git",
                "diff",
                "--",
                "backend/services",
                "backend/factors",
                "backend/models",
                "backend/strategies",
                "backend/tasks",
            ],
            cwd=settings.project_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        status_result = subprocess.run(
            [
                "git",
                "status",
                "--short",
                "--",
                "backend/services",
                "backend/factors",
                "backend/models",
                "backend/strategies",
                "backend/tasks",
            ],
            cwd=settings.project_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        diff_text = diff_result.stdout or ""
        status_lines = [
            line.strip()
            for line in (status_result.stdout or "").splitlines()
            if line.strip()
        ]
        return {
            "dirty": bool(diff_text.strip() or status_lines),
            "dirty_paths": [line[3:] if len(line) > 3 else line for line in status_lines],
            "patch_hash": _sha256_text(diff_text) if diff_text.strip() else None,
        }
    except Exception as exc:
        return {
            "dirty": None,
            "dirty_paths": [],
            "patch_hash": None,
            "error": str(exc),
        }


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


def _prediction_cache_lock(key: tuple) -> threading.Lock:
    with _PREDICTION_CACHE_LOCKS_GUARD:
        lock = _PREDICTION_CACHE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PREDICTION_CACHE_LOCKS[key] = lock
        return lock


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
