"""Simplified vectorized backtest engine for strategy validation.

Key correctness requirements:
- T+1 open execution: signals generated on day T execute at T+1 open price
- Commission + slippage cost model applied to trade value
- Multi-stock position management with configurable limits
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.calendar_service import snap_to_trading_day
from backend.services.execution_model_service import (
    DEFAULT_PLANNED_PRICE_BUFFER_BPS,
    DEFAULT_PLANNED_PRICE_FALLBACK,
    evaluate_planned_price_fill,
    normalize_execution_model,
    normalize_planned_price_buffer_bps,
    normalize_planned_price_fallback,
)
from backend.services.market_context import normalize_market, normalize_ticker
from backend.services.sql_filters import registered_values_table

log = get_logger(__name__)

# Risk-free rate assumption for Sharpe/Sortino calculation
_RISK_FREE_RATE = 0.04


@dataclass
class BacktestConfig:
    """Configuration for a backtest run."""

    initial_capital: float = 1_000_000.0
    start_date: date | str = "2020-01-01"
    end_date: date | str = "2024-12-31"
    market: str = "US"
    benchmark: str = "SPY"
    commission_rate: float = 0.001   # 0.1% per trade
    slippage_rate: float = 0.001     # 0.1% slippage
    max_positions: int = 50
    rebalance_freq: str = "daily"    # daily / weekly / monthly

    # Low-turnover controls
    rebalance_buffer: float = 0.0       # ignore weight change below this threshold
    rebalance_buffer_add: float | None = None
    rebalance_buffer_reduce: float | None = None
    rebalance_buffer_mode: str = "all"  # all / hold_overlap_only
    rebalance_buffer_reference: str = "target"  # target / actual_open
    min_holding_days: int = 0           # don't sell a position before N trading days
    reentry_cooldown_days: int = 0      # after selling, wait N days before re-buying
    normalize_target_weights: bool = True  # legacy default: invest non-empty targets fully
    max_single_name_weight: float | None = None  # absolute per-name target/order cap
    max_holding_days: int | None = None           # force exit after N holding days
    execution_model: str = "next_open"
    planned_price_buffer_bps: float = DEFAULT_PLANNED_PRICE_BUFFER_BPS
    planned_price_fallback: str = DEFAULT_PLANNED_PRICE_FALLBACK

    def __post_init__(self) -> None:
        self.market = normalize_market(self.market)
        self.execution_model = normalize_execution_model(self.execution_model)
        self.planned_price_buffer_bps = normalize_planned_price_buffer_bps(
            self.planned_price_buffer_bps
        )
        self.planned_price_fallback = normalize_planned_price_fallback(
            self.planned_price_fallback
        )
        if isinstance(self.start_date, str):
            self.start_date = date.fromisoformat(self.start_date)
        if isinstance(self.end_date, str):
            self.end_date = date.fromisoformat(self.end_date)
        # Snap to valid trading days so weekends/holidays don't cause empty ranges
        self.start_date = snap_to_trading_day(
            self.start_date, direction="forward", market=self.market
        )
        self.end_date = snap_to_trading_day(
            self.end_date, direction="backward", market=self.market
        )

    def to_dict(self) -> dict:
        return {
            "initial_capital": self.initial_capital,
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "market": self.market,
            "benchmark": self.benchmark,
            "commission_rate": self.commission_rate,
            "slippage_rate": self.slippage_rate,
            "max_positions": self.max_positions,
            "rebalance_freq": self.rebalance_freq,
            "rebalance_buffer": self.rebalance_buffer,
            "rebalance_buffer_add": self.rebalance_buffer_add,
            "rebalance_buffer_reduce": self.rebalance_buffer_reduce,
            "rebalance_buffer_mode": self.rebalance_buffer_mode,
            "rebalance_buffer_reference": self.rebalance_buffer_reference,
            "min_holding_days": self.min_holding_days,
            "reentry_cooldown_days": self.reentry_cooldown_days,
            "normalize_target_weights": self.normalize_target_weights,
            "max_single_name_weight": self.max_single_name_weight,
            "max_holding_days": self.max_holding_days,
            "execution_model": self.execution_model,
            "planned_price_buffer_bps": self.planned_price_buffer_bps,
            "planned_price_fallback": self.planned_price_fallback,
        }


@dataclass
class BacktestResult:
    """Full result of a backtest run."""

    # Config
    config: dict

    # Time series
    dates: list[str]
    nav: list[float]               # portfolio net asset value
    benchmark_nav: list[float]     # benchmark nav
    drawdown: list[float]          # drawdown series

    # Summary metrics
    total_return: float
    annual_return: float
    annual_volatility: float
    max_drawdown: float
    sharpe_ratio: float
    calmar_ratio: float
    sortino_ratio: float
    win_rate: float
    profit_loss_ratio: float
    total_trades: int
    annual_turnover: float
    total_cost: float

    # Monthly returns (for heatmap)
    monthly_returns: list[dict]    # [{year, month, return}]

    # Trade log
    trades: list[dict]             # [{date, ticker, action, shares, price, cost, trade_reason, position_state, holding_days}]

    # Trade diagnostics
    trade_diagnostics: dict        # aggregate stats by trade_reason and position_state

    def to_dict(self) -> dict:
        """Serialize to a plain dict."""
        return {
            "config": self.config,
            "dates": self.dates,
            "nav": self.nav,
            "benchmark_nav": self.benchmark_nav,
            "drawdown": self.drawdown,
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "annual_volatility": self.annual_volatility,
            "max_drawdown": self.max_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "calmar_ratio": self.calmar_ratio,
            "sortino_ratio": self.sortino_ratio,
            "win_rate": self.win_rate,
            "profit_loss_ratio": self.profit_loss_ratio,
            "total_trades": self.total_trades,
            "annual_turnover": self.annual_turnover,
            "total_cost": self.total_cost,
            "monthly_returns": self.monthly_returns,
            "trades": self.trades,
            "trade_diagnostics": self.trade_diagnostics,
        }


class BacktestEngine:
    """Simplified vectorized backtest engine.

    Execution logic:
    - Signal on day T (using T's close data) -> execute at T+1 open
    - Cost = |weight_change| * (commission_rate + slippage_rate) applied to capital
    - Track: daily portfolio value, positions, trades
    """

    def run(
        self,
        signals: pd.DataFrame,
        config: BacktestConfig,
        planned_prices: pd.DataFrame | None = None,
        execution_overrides: pd.DataFrame | None = None,
    ) -> BacktestResult:
        """Run a backtest.

        Args:
            signals: DataFrame with columns=[ticker], index=[date],
                     values=target_weight (0~1). Only tickers with
                     weight > 0 are held.
            config: Backtest configuration.
            planned_prices: Optional DataFrame with columns=[ticker],
                            index=[decision_date], values=planned execution price.
            execution_overrides: Optional DataFrame with columns=[ticker],
                                 index=[decision_date], values=dict(order intent).

        Returns:
            BacktestResult with all metrics and data.
        """
        log.info(
            "backtest.start",
            market=config.market,
            start=str(config.start_date),
            end=str(config.end_date),
            tickers=len(signals.columns),
        )

        # 1. Load price data
        tickers = list(signals.columns)
        prices_close, prices_open, prices_high, prices_low, _vol = self._load_prices(
            tickers, str(config.start_date), str(config.end_date), market=config.market
        )
        benchmark_close = self._load_benchmark(
            config.benchmark, str(config.start_date), str(config.end_date), market=config.market
        )

        if prices_close.empty:
            raise ValueError("No price data available for the given tickers and date range")

        # 2. Align signals and prices to common trading days
        all_trading_days = sorted(prices_close.index)
        signals.index = pd.to_datetime(signals.index)
        if planned_prices is not None:
            planned_prices = planned_prices.copy()
            planned_prices.index = pd.to_datetime(planned_prices.index)
        if execution_overrides is not None:
            execution_overrides = execution_overrides.copy()
            execution_overrides.index = pd.to_datetime(execution_overrides.index)

        # 3. Determine rebalance dates
        rebalance_dates = self._get_rebalance_dates(all_trading_days, config.rebalance_freq)

        # 4. Run the simulation
        cash = config.initial_capital
        cost_rate = config.commission_rate + config.slippage_rate

        # Current holdings: {ticker: num_shares}
        holdings: dict[str, float] = {}
        current_weights: dict[str, float] = {}

        # Low-turnover state tracking
        ticker_holding_days: dict[str, int] = {}   # days held per ticker
        ticker_exit_day: dict[str, int] = {}        # day_idx when last sold

        nav_series: list[float] = []
        date_series: list[str] = []
        trade_log: list[dict] = []
        total_cost = 0.0
        total_weight_turnover = 0.0
        num_rebalance_periods = 0
        last_close_by_ticker: dict[str, float] = {}
        missing_price_valuations: list[dict[str, Any]] = []
        last_cash_weight: float | None = None
        rebalance_execution_diagnostics: list[dict[str, Any]] = []
        planned_execution_diagnostics: dict[str, Any] = {
            "execution_model": config.execution_model,
            "planned_price_buffer_bps": config.planned_price_buffer_bps,
            "planned_price_fallback": config.planned_price_fallback,
            "filled_order_count": 0,
            "planned_fill_count": 0,
            "fallback_close_count": 0,
            "blocked_order_count": 0,
            "filled": [],
            "blocked": [],
        }
        order_intent_records: list[dict[str, Any]] = []
        execution_model_counts: dict[str, int] = {}
        saw_planned_price_order = config.execution_model == "planned_price"

        for day_idx, trade_date in enumerate(all_trading_days):
            trade_date_ts = pd.Timestamp(trade_date)

            # Increment holding days for all held tickers
            for t in ticker_holding_days:
                ticker_holding_days[t] += 1

            # --- Check if previous day was a signal/rebalance day ---
            # We look for signals from the PREVIOUS trading day to execute today
            if day_idx > 0:
                prev_date = all_trading_days[day_idx - 1]
                prev_date_ts = pd.Timestamp(prev_date)

                is_rebalance = prev_date_ts in rebalance_dates

                if is_rebalance and prev_date_ts in signals.index:
                    # Get target weights from the previous day's signal
                    target_weights_raw = signals.loc[prev_date_ts]
                    target_weights = {}
                    for t in tickers:
                        w = target_weights_raw.get(t, 0.0)
                        if pd.notna(w) and w > 0:
                            target_weights[t] = float(w)

                    # Enforce max_positions: keep top N by weight
                    if len(target_weights) > config.max_positions:
                        sorted_tw = sorted(
                            target_weights.items(), key=lambda x: x[1], reverse=True
                        )
                        target_weights = dict(sorted_tw[: config.max_positions])

                    # Normalize weights to sum to 1 unless the strategy is
                    # explicitly allowed to hold cash.
                    weight_sum = sum(target_weights.values())
                    if weight_sum > 0 and config.normalize_target_weights:
                        target_weights = {
                            t: w / weight_sum for t, w in target_weights.items()
                        }
                    if config.max_single_name_weight is not None:
                        target_weights = _cap_absolute_weights(
                            target_weights,
                            float(config.max_single_name_weight),
                        )
                    last_cash_weight = max(0.0, 1.0 - sum(target_weights.values()))

                    # Calculate portfolio value BEFORE rebalance (at today's open)
                    portfolio_value = cash + self._calc_portfolio_value_at_open(
                        holdings, prices_open, trade_date_ts
                    )
                    if portfolio_value <= 0:
                        portfolio_value = cash
                    actual_open_weights: dict[str, float] = {}
                    if portfolio_value > 0:
                        for ticker, shares in holdings.items():
                            if (
                                trade_date_ts in prices_open.index
                                and ticker in prices_open.columns
                            ):
                                open_price = prices_open.loc[trade_date_ts, ticker]
                                if pd.notna(open_price) and open_price > 0:
                                    actual_open_weights[ticker] = (
                                        shares * float(open_price) / portfolio_value
                                    )

                    # Compute trades: current weights vs target weights
                    # Apply low-turnover constraints before executing trades
                    effective_targets = dict(target_weights)
                    positions_before = dict(current_weights)

                    if config.rebalance_buffer > 0 or config.min_holding_days > 0 or config.reentry_cooldown_days > 0:
                        all_involved_tickers = set(current_weights.keys()) | set(
                            target_weights.keys()
                        )
                        for ticker in all_involved_tickers:
                            old_w = current_weights.get(ticker, 0.0)
                            reference_w = old_w
                            if config.rebalance_buffer_reference == "actual_open":
                                reference_w = actual_open_weights.get(ticker, 0.0)
                            new_w = target_weights.get(ticker, 0.0)

                            # Buffer: skip small weight-only add/reduce trades.
                            buffer_applies = True
                            if config.rebalance_buffer_mode == "hold_overlap_only":
                                buffer_applies = reference_w > 0 and new_w > 0
                            direction_buffer = config.rebalance_buffer
                            if new_w > reference_w and config.rebalance_buffer_add is not None:
                                direction_buffer = config.rebalance_buffer_add
                            elif new_w < reference_w and config.rebalance_buffer_reduce is not None:
                                direction_buffer = config.rebalance_buffer_reduce
                            if (
                                direction_buffer > 0
                                and buffer_applies
                                and abs(new_w - reference_w) < direction_buffer
                            ):
                                effective_targets[ticker] = reference_w
                                continue

                            # Min holding days: prevent selling before N days
                            if config.min_holding_days > 0 and reference_w > 0 and new_w < reference_w:
                                days_held = ticker_holding_days.get(ticker, 0)
                                if days_held < config.min_holding_days:
                                    effective_targets[ticker] = reference_w
                                    continue

                            # Re-entry cooldown: prevent buying back after recent sell
                            if config.reentry_cooldown_days > 0 and reference_w == 0 and new_w > 0:
                                exit_idx = ticker_exit_day.get(ticker)
                                if exit_idx is not None and (day_idx - exit_idx) < config.reentry_cooldown_days:
                                    effective_targets.pop(ticker, None)
                                    continue

                            # Max holding days: force an exit once the position
                            # has aged past the configured hard cap.
                            if config.max_holding_days is not None and reference_w > 0:
                                days_held = ticker_holding_days.get(ticker, 0)
                                if days_held >= int(config.max_holding_days):
                                    effective_targets.pop(ticker, None)
                                    continue

                        # Re-normalize if constraints altered the weights. In
                        # hold-overlap mode the buffer is specifically meant to
                        # preserve existing position sizes, so normalizing would
                        # reintroduce the same add/reduce trades it just blocked.
                        eff_sum = sum(w for w in effective_targets.values() if w > 0)
                        if (
                            eff_sum > 0
                            and config.normalize_target_weights
                            and config.rebalance_buffer_mode != "hold_overlap_only"
                        ):
                            effective_targets = {
                                t: w / eff_sum for t, w in effective_targets.items() if w > 0
                            }
                        if not config.normalize_target_weights:
                            last_cash_weight = max(
                                0.0,
                                1.0 - sum(w for w in effective_targets.values() if w > 0),
                            )

                    if config.max_single_name_weight is not None:
                        effective_targets = _cap_absolute_weights(
                            effective_targets,
                            float(config.max_single_name_weight),
                        )
                        last_cash_weight = max(
                            0.0,
                            1.0 - sum(w for w in effective_targets.values() if w > 0),
                        )

                    all_involved_tickers = set(current_weights.keys()) | set(
                        effective_targets.keys()
                    )

                    day_turnover = 0.0
                    executed_targets = dict(current_weights)
                    for ticker in all_involved_tickers:
                        old_w = current_weights.get(ticker, 0.0)
                        new_w = effective_targets.get(ticker, 0.0)
                        weight_change = abs(new_w - old_w)

                        if weight_change < 1e-8:
                            continue

                        # Get execution price.
                        exec_price: float | None = None
                        fill_type: str | None = None
                        fallback_reason: str | None = None
                        order_override = _lookup_execution_override(
                            execution_overrides,
                            prev_date_ts,
                            ticker,
                        )
                        order_execution_model = normalize_execution_model(
                            str(order_override.get("execution_model") or config.execution_model)
                        )
                        order_buffer_bps = normalize_planned_price_buffer_bps(
                            order_override.get(
                                "planned_price_buffer_bps",
                                config.planned_price_buffer_bps,
                            )
                        )
                        order_fallback = normalize_planned_price_fallback(
                            order_override.get(
                                "planned_price_fallback",
                                config.planned_price_fallback,
                            )
                        )
                        order_reason = order_override.get("order_reason")
                        order_record: dict[str, Any] = {
                            "date": str(trade_date_ts.date()),
                            "decision_date": str(prev_date_ts.date()),
                            "execution_date": str(trade_date_ts.date()),
                            "ticker": ticker,
                            "target_weight": round(float(new_w), 6),
                            "previous_weight": round(float(old_w), 6),
                            "execution_model": order_execution_model,
                            "price_field": order_override.get("price_field"),
                            "time_in_force": order_override.get("time_in_force"),
                            "order_reason": order_reason,
                        }
                        execution_model_counts[order_execution_model] = (
                            execution_model_counts.get(order_execution_model, 0) + 1
                        )
                        if order_execution_model == "planned_price":
                            saw_planned_price_order = True
                            planned_price = _lookup_planned_price(
                                planned_prices=planned_prices,
                                decision_date=prev_date_ts,
                                ticker=ticker,
                            )
                            order_record["planned_price"] = planned_price
                            order_record["planned_price_buffer_bps"] = order_buffer_bps
                            order_record["planned_price_fallback"] = order_fallback
                            high_price = _lookup_price(prices_high, trade_date_ts, ticker)
                            low_price = _lookup_price(prices_low, trade_date_ts, ticker)
                            fill_decision = evaluate_planned_price_fill(
                                planned_price=planned_price,
                                high=high_price,
                                low=low_price,
                                buffer_bps=order_buffer_bps,
                            )
                            if not fill_decision.filled:
                                can_fallback_to_close = (
                                    order_fallback == "next_close"
                                    and fill_decision.reason == "planned_price_outside_buffered_range"
                                )
                                close_price = (
                                    _lookup_price(prices_close, trade_date_ts, ticker)
                                    if can_fallback_to_close
                                    else None
                                )
                                if close_price is None:
                                    planned_execution_diagnostics["blocked_order_count"] += 1
                                    planned_execution_diagnostics["blocked"].append(
                                        {
                                            "date": str(trade_date_ts.date()),
                                            "decision_date": str(prev_date_ts.date()),
                                            "ticker": ticker,
                                            "reason": (
                                                "missing_fallback_close"
                                                if can_fallback_to_close
                                                else fill_decision.reason
                                            ),
                                            "planned_price": planned_price,
                                            "high": high_price,
                                            "low": low_price,
                                            "close": close_price,
                                            "lower_bound": fill_decision.lower_bound,
                                            "upper_bound": fill_decision.upper_bound,
                                            "planned_price_fallback": order_fallback,
                                            "planned_price_reject_reason": fill_decision.reason,
                                            "fill_type": "blocked",
                                        }
                                    )
                                    order_record.update(
                                        {
                                            "fill_status": "blocked",
                                            "fill_type": "blocked",
                                            "fill_price": None,
                                            "blocked_reason": (
                                                "missing_fallback_close"
                                                if can_fallback_to_close
                                                else fill_decision.reason
                                            ),
                                            "planned_price_reject_reason": fill_decision.reason,
                                            "high": high_price,
                                            "low": low_price,
                                            "close": close_price,
                                            "lower_bound": fill_decision.lower_bound,
                                            "upper_bound": fill_decision.upper_bound,
                                        }
                                    )
                                    order_intent_records.append(order_record)
                                    continue
                                exec_price = close_price
                                fill_type = "fallback_close"
                                fallback_reason = fill_decision.reason
                            else:
                                exec_price = fill_decision.fill_price
                                fill_type = "planned_price"
                        elif order_execution_model == "next_close":
                            exec_price = _lookup_price(prices_close, trade_date_ts, ticker)
                            fill_type = "next_close"
                            if exec_price is None:
                                order_record.update(
                                    {
                                        "fill_status": "blocked",
                                        "fill_type": "blocked",
                                        "fill_price": None,
                                        "blocked_reason": "missing_next_close_price",
                                    }
                                )
                                order_intent_records.append(order_record)
                                continue
                        else:
                            exec_price = _lookup_price(prices_open, trade_date_ts, ticker)
                            if exec_price is None:
                                order_record.update(
                                    {
                                        "fill_status": "blocked",
                                        "fill_type": "blocked",
                                        "fill_price": None,
                                        "blocked_reason": "missing_next_open_price",
                                    }
                                )
                                order_intent_records.append(order_record)
                                continue
                            fill_type = "next_open"

                        # Calculate target dollar amount and shares
                        target_dollar = new_w * portfolio_value
                        target_shares = target_dollar / exec_price

                        old_shares = holdings.get(ticker, 0.0)
                        share_change = target_shares - old_shares
                        if abs(share_change) < 1e-8:
                            continue

                        # Cost on the traded dollar amount
                        trade_value = abs(share_change * exec_price)
                        day_turnover += trade_value / portfolio_value if portfolio_value > 0 else weight_change
                        trade_cost = trade_value * cost_rate
                        total_cost += trade_cost
                        if share_change > 0:
                            cash -= trade_value + trade_cost
                        else:
                            cash += trade_value - trade_cost

                        # Determine trade reason and position state
                        was_held = old_shares > 1e-8
                        will_hold = target_shares > 1e-8
                        if share_change > 0:
                            if not was_held:
                                # Check if this is a re-entry
                                if ticker in ticker_exit_day:
                                    trade_reason = "reentry"
                                else:
                                    trade_reason = "new_entry"
                            else:
                                trade_reason = "add"
                        else:
                            if will_hold:
                                trade_reason = "reduce"
                            else:
                                trade_reason = "exit"

                        days_held = ticker_holding_days.get(ticker, 0)
                        position_state = "core" if days_held > 5 else "tactical"

                        # Update holdings and track exits
                        if target_shares > 1e-8:
                            holdings[ticker] = target_shares
                            executed_targets[ticker] = new_w
                            # Track new entry for holding days
                            if ticker not in ticker_holding_days:
                                ticker_holding_days[ticker] = 0
                        else:
                            if ticker in holdings:
                                del holdings[ticker]
                            executed_targets.pop(ticker, None)
                            # Record exit day for cooldown
                            ticker_exit_day[ticker] = day_idx
                            ticker_holding_days.pop(ticker, None)

                        # Log the trade
                        action = "buy" if share_change > 0 else "sell"
                        order_record.update(
                            {
                                "side": action,
                                "fill_status": "filled",
                                "fill_type": fill_type or order_execution_model,
                                "fill_price": round(float(exec_price), 6),
                                "shares": round(abs(share_change), 4),
                                "fallback_reason": fallback_reason,
                            }
                        )
                        order_intent_records.append(order_record)
                        trade_log.append(
                            {
                                "date": str(trade_date_ts.date()),
                                "ticker": ticker,
                                "action": action,
                                "shares": round(abs(share_change), 4),
                                "price": round(exec_price, 4),
                                "cost": round(trade_cost, 4),
                                "trade_reason": trade_reason,
                                "position_state": position_state,
                                "holding_days": days_held,
                                "execution_model": order_execution_model,
                                "fill_type": fill_type or order_execution_model,
                                "order_reason": order_reason,
                            }
                        )
                        if order_execution_model == "planned_price":
                            planned_execution_diagnostics["filled_order_count"] += 1
                            if fill_type == "fallback_close":
                                planned_execution_diagnostics["fallback_close_count"] += 1
                            else:
                                planned_execution_diagnostics["planned_fill_count"] += 1
                            planned_execution_diagnostics["filled"].append(
                                {
                                    "date": str(trade_date_ts.date()),
                                    "decision_date": str(prev_date_ts.date()),
                                    "ticker": ticker,
                                    "price": round(float(exec_price), 6),
                                    "planned_price": (
                                        round(float(planned_price), 6)
                                        if planned_price is not None
                                        else None
                                    ),
                                    "high": high_price,
                                    "low": low_price,
                                    "close": (
                                        _lookup_price(prices_close, trade_date_ts, ticker)
                                        if fill_type == "fallback_close"
                                        else None
                                    ),
                                    "fill_type": fill_type or "planned_price",
                                    "fallback_reason": fallback_reason,
                                    "shares": round(abs(share_change), 4),
                                    "action": action,
                                }
                            )

                    total_weight_turnover += day_turnover
                    num_rebalance_periods += 1
                    rebalance_execution_diagnostics.append(
                        _build_execution_rebalance_diagnostic(
                            date_key=str(trade_date_ts.date()),
                            positions_before=positions_before,
                            target_positions_after=target_weights,
                            executed_positions_after=executed_targets,
                            executed_turnover=day_turnover,
                        )
                    )
                    current_weights = dict(executed_targets)

            # --- Value portfolio at today's close ---
            portfolio_value = cash
            valued_positions = 0
            for ticker, shares in holdings.items():
                used_price: float | None = None
                valuation_method = "close"
                if (
                    trade_date_ts in prices_close.index
                    and ticker in prices_close.columns
                ):
                    close_price = prices_close.loc[trade_date_ts, ticker]
                    if pd.notna(close_price) and close_price > 0:
                        used_price = float(close_price)
                        last_close_by_ticker[ticker] = used_price
                if used_price is None:
                    used_price = last_close_by_ticker.get(ticker)
                    valuation_method = "last_close_carry_forward"
                if used_price is not None and used_price > 0:
                    portfolio_value += shares * used_price
                    valued_positions += 1
                    if valuation_method != "close":
                        missing_price_valuations.append({
                            "date": str(trade_date_ts.date()),
                            "ticker": ticker,
                            "shares": round(float(shares), 8),
                            "price": round(float(used_price), 6),
                            "valuation_method": valuation_method,
                        })

            if holdings and valued_positions == 0:
                # If we can't value, carry forward the last known NAV
                portfolio_value = nav_series[-1] if nav_series else config.initial_capital

            nav_series.append(round(portfolio_value, 2))
            date_series.append(
                str(trade_date_ts.date())
                if hasattr(trade_date_ts, "date")
                else str(trade_date_ts)
            )

        # 5. Compute benchmark NAV
        benchmark_nav = self._compute_benchmark_nav(
            benchmark_close, all_trading_days, config.initial_capital
        )

        # 6. Calculate metrics
        nav_arr = np.array(nav_series, dtype=float)
        daily_returns = np.diff(nav_arr) / nav_arr[:-1] if len(nav_arr) > 1 else np.array([])

        # Replace any inf/nan in returns
        daily_returns = np.where(np.isfinite(daily_returns), daily_returns, 0.0)

        total_return = (nav_arr[-1] / config.initial_capital - 1.0) if len(nav_arr) > 0 else 0.0

        n_days = len(nav_arr)
        years = n_days / 252.0 if n_days > 0 else 1.0

        annual_return = _calc_cagr(config.initial_capital, nav_arr[-1] if len(nav_arr) > 0 else config.initial_capital, years)
        annual_volatility = _calc_annual_volatility(daily_returns)
        drawdown_series = _calc_drawdown_series(nav_arr)
        max_dd = float(np.min(drawdown_series)) if len(drawdown_series) > 0 else 0.0
        sharpe = _calc_sharpe(annual_return, annual_volatility)
        calmar = _calc_calmar(annual_return, max_dd)
        sortino = _calc_sortino(daily_returns, annual_return)
        win_rate, pl_ratio = _calc_trade_stats(trade_log)
        monthly_rets = _calc_monthly_returns(date_series, nav_series)

        annual_turnover = (total_weight_turnover / years) if years > 0 else 0.0

        # Compute trade diagnostics
        trade_diagnostics = _calc_trade_diagnostics(trade_log)
        trade_diagnostics["missing_price_valuations"] = missing_price_valuations[:1000]
        trade_diagnostics["missing_price_valuation_count"] = len(missing_price_valuations)
        trade_diagnostics["target_weight_policy"] = {
            "normalized": bool(config.normalize_target_weights),
            "last_cash_weight": round(float(last_cash_weight or 0.0), 6),
        }
        trade_diagnostics["rebalance_execution_diagnostics"] = rebalance_execution_diagnostics
        if order_intent_records:
            trade_diagnostics["order_intents"] = {
                "execution_model_counts": execution_model_counts,
                "orders": order_intent_records[:10000],
            }
        if saw_planned_price_order:
            filled = int(planned_execution_diagnostics["filled_order_count"])
            blocked = int(planned_execution_diagnostics["blocked_order_count"])
            total = filled + blocked
            planned_execution_diagnostics["fill_rate"] = (
                round(filled / total, 6) if total else None
            )
            planned_execution_diagnostics["planned_fill_rate"] = (
                round(int(planned_execution_diagnostics["planned_fill_count"]) / total, 6)
                if total else None
            )
            planned_execution_diagnostics["fallback_close_rate"] = (
                round(int(planned_execution_diagnostics["fallback_close_count"]) / total, 6)
                if total else None
            )
            planned_execution_diagnostics["blocked_rate"] = (
                round(blocked / total, 6) if total else None
            )
            trade_diagnostics["planned_price_execution"] = planned_execution_diagnostics

        result = BacktestResult(
            config=config.to_dict(),
            dates=date_series,
            nav=nav_series,
            benchmark_nav=benchmark_nav,
            drawdown=[round(d, 6) for d in drawdown_series.tolist()] if len(drawdown_series) > 0 else [],
            total_return=round(total_return, 6),
            annual_return=round(annual_return, 6),
            annual_volatility=round(annual_volatility, 6),
            max_drawdown=round(max_dd, 6),
            sharpe_ratio=round(sharpe, 4),
            calmar_ratio=round(calmar, 4),
            sortino_ratio=round(sortino, 4),
            win_rate=round(win_rate, 4),
            profit_loss_ratio=round(pl_ratio, 4),
            total_trades=len(trade_log),
            annual_turnover=round(annual_turnover, 4),
            total_cost=round(total_cost, 2),
            monthly_returns=monthly_rets,
            trades=trade_log,
            trade_diagnostics=trade_diagnostics,
        )

        log.info(
            "backtest.done",
            total_return=result.total_return,
            sharpe=result.sharpe_ratio,
            max_dd=result.max_drawdown,
            trades=result.total_trades,
        )
        return result

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_prices(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
        market: str | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load OHLCV prices from daily_bars.

        Returns:
            (close_df, open_df, high_df, low_df, volume_df) each with
            DatetimeIndex and ticker columns.
        """
        resolved_market = normalize_market(market)
        normalized_tickers = [
            normalize_ticker(t, resolved_market)
            for t in tickers
            if str(t).strip()
        ]
        if not normalized_tickers:
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty

        conn = get_connection()
        with registered_values_table(conn, "ticker", normalized_tickers, table_prefix="_qagent_tickers") as ticker_table:
            query = f"""
                SELECT b.ticker, b.date, b.open, b.high, b.low, b.close, b.volume
                FROM daily_bars b
                JOIN {ticker_table} t ON b.ticker = t.ticker
                WHERE b.market = ?
                  AND b.date >= ?
                  AND b.date <= ?
                ORDER BY b.date, b.ticker
            """
            rows = conn.execute(
                query,
                [resolved_market, start_date, end_date],
            ).fetchdf()

        if rows.empty:
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty

        close_df = rows.pivot(index="date", columns="ticker", values="close")
        open_df = rows.pivot(index="date", columns="ticker", values="open")
        high_df = rows.pivot(index="date", columns="ticker", values="high")
        low_df = rows.pivot(index="date", columns="ticker", values="low")
        volume_df = rows.pivot(index="date", columns="ticker", values="volume")
        for df in (close_df, open_df, high_df, low_df, volume_df):
            df.index = pd.to_datetime(df.index)

        return close_df, open_df, high_df, low_df, volume_df

    def _load_benchmark(
        self, symbol: str, start_date: str, end_date: str, market: str | None = None
    ) -> pd.Series:
        """Load benchmark close prices from index_bars."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        rows = conn.execute(
            """SELECT date, close
               FROM index_bars
               WHERE market = ?
                 AND symbol = ?
                 AND date >= ?
                 AND date <= ?
               ORDER BY date""",
            [resolved_market, normalize_ticker(symbol, resolved_market), start_date, end_date],
        ).fetchdf()

        if rows.empty:
            return pd.Series(dtype=float)

        series = rows.set_index("date")["close"]
        series.index = pd.to_datetime(series.index)
        return series

    @staticmethod
    def _calc_portfolio_value_at_open(
        holdings: dict[str, float],
        prices_open: pd.DataFrame,
        trade_date: pd.Timestamp,
    ) -> float:
        """Calculate portfolio value using open prices."""
        value = 0.0
        for ticker, shares in holdings.items():
            if trade_date in prices_open.index and ticker in prices_open.columns:
                price = prices_open.loc[trade_date, ticker]
                if pd.notna(price) and price > 0:
                    value += shares * price
        return value

    @staticmethod
    def _get_rebalance_dates(
        trading_days: list, freq: str
    ) -> set[pd.Timestamp]:
        """Determine which trading days are rebalance days."""
        if not trading_days:
            return set()

        dates = pd.DatetimeIndex(trading_days)

        if freq == "daily":
            return set(dates)

        elif freq == "weekly":
            # Rebalance on every Friday (or last trading day of the week)
            rebalance = set()
            for i, d in enumerate(dates):
                # Check if next trading day is in a different week
                if i == len(dates) - 1:
                    rebalance.add(d)
                elif dates[i + 1].isocalendar()[1] != d.isocalendar()[1]:
                    rebalance.add(d)
            return rebalance

        elif freq == "monthly":
            # Rebalance on last trading day of each month
            rebalance = set()
            for i, d in enumerate(dates):
                if i == len(dates) - 1:
                    rebalance.add(d)
                elif dates[i + 1].month != d.month:
                    rebalance.add(d)
            return rebalance

        else:
            # Default to daily
            return set(dates)

    @staticmethod
    def _compute_benchmark_nav(
        benchmark_close: pd.Series,
        trading_days: list,
        initial_capital: float,
    ) -> list[float]:
        """Compute benchmark NAV series aligned to trading days."""
        if benchmark_close.empty:
            return [initial_capital] * len(trading_days)

        nav = []
        first_price = None
        for d in trading_days:
            ts = pd.Timestamp(d)
            if ts in benchmark_close.index:
                price = benchmark_close.loc[ts]
                if pd.notna(price) and price > 0:
                    if first_price is None:
                        first_price = price
                    nav.append(round(initial_capital * price / first_price, 2))
                    continue
            # Carry forward
            nav.append(nav[-1] if nav else initial_capital)

        return nav


# ------------------------------------------------------------------
# Metrics calculation helpers
# ------------------------------------------------------------------


def _cap_absolute_weights(weights: dict[str, float], max_weight: float) -> dict[str, float]:
    """Apply an absolute per-name cap without redistributing clipped cash."""
    if max_weight <= 0:
        return {}
    return {
        ticker: min(float(weight), float(max_weight))
        for ticker, weight in weights.items()
        if float(weight) > 1e-8
    }


def _calc_cagr(initial: float, final: float, years: float) -> float:
    """Compound Annual Growth Rate."""
    if years <= 0 or initial <= 0:
        return 0.0
    if final <= 0:
        return -1.0
    return float((final / initial) ** (1.0 / years) - 1.0)


def _calc_annual_volatility(daily_returns: np.ndarray) -> float:
    """Annualized volatility from daily returns."""
    if len(daily_returns) < 2:
        return 0.0
    return float(np.std(daily_returns, ddof=1) * np.sqrt(252))


def _calc_sharpe(annual_return: float, annual_vol: float) -> float:
    """Sharpe ratio with risk-free rate assumption."""
    if annual_vol <= 0:
        return 0.0
    return float((annual_return - _RISK_FREE_RATE) / annual_vol)


def _calc_calmar(annual_return: float, max_drawdown: float) -> float:
    """Calmar ratio = annual return / |max drawdown|."""
    abs_dd = abs(max_drawdown)
    if abs_dd <= 0:
        return 0.0
    return float(annual_return / abs_dd)


def _calc_sortino(daily_returns: np.ndarray, annual_return: float) -> float:
    """Sortino ratio using downside deviation only."""
    if len(daily_returns) < 2:
        return 0.0
    # Downside returns (negative only)
    downside = daily_returns[daily_returns < 0]
    if len(downside) < 1:
        return 0.0
    downside_std = float(np.std(downside, ddof=1) * np.sqrt(252))
    if downside_std <= 0:
        return 0.0
    return float((annual_return - _RISK_FREE_RATE) / downside_std)


def _calc_drawdown_series(nav: np.ndarray) -> np.ndarray:
    """Compute drawdown series from NAV array."""
    if len(nav) == 0:
        return np.array([])
    peak = np.maximum.accumulate(nav)
    # Avoid division by zero
    peak_safe = np.where(peak > 0, peak, 1.0)
    dd = (nav - peak) / peak_safe
    return dd


def _calc_trade_stats(trade_log: list[dict]) -> tuple[float, float]:
    """Calculate win rate and profit/loss ratio from trade log.

    Returns:
        (win_rate, profit_loss_ratio)
    """
    if not trade_log:
        return 0.0, 0.0

    # Group trades by ticker to compute per-round-trip PnL
    # Simple approach: group sequential buy-sell pairs
    # For simplicity, track PnL per trade pair
    profits: list[float] = []
    losses: list[float] = []

    # Track open positions per ticker
    open_positions: dict[str, list[dict]] = {}

    for trade in trade_log:
        ticker = trade["ticker"]
        if trade["action"] == "buy":
            if ticker not in open_positions:
                open_positions[ticker] = []
            open_positions[ticker].append(trade)
        elif trade["action"] == "sell":
            if ticker in open_positions and open_positions[ticker]:
                buy_trade = open_positions[ticker].pop(0)
                pnl = (trade["price"] - buy_trade["price"]) * trade["shares"]
                pnl -= trade["cost"] + buy_trade["cost"]
                if pnl > 0:
                    profits.append(pnl)
                else:
                    losses.append(abs(pnl))

    total_round_trips = len(profits) + len(losses)
    if total_round_trips == 0:
        return 0.0, 0.0

    win_rate = len(profits) / total_round_trips
    avg_profit = np.mean(profits) if profits else 0.0
    avg_loss = np.mean(losses) if losses else 1.0
    pl_ratio = float(avg_profit / avg_loss) if avg_loss > 0 else 0.0

    return float(win_rate), pl_ratio


def _calc_trade_diagnostics(trade_log: list[dict]) -> dict:
    """Compute aggregate trade diagnostics by reason and position state.

    Tactical P&L is computed via round-trip matching: each buy is paired
    with its corresponding sell for the same ticker (FIFO order).  A
    round-trip held <= 5 trading days is classified as tactical.
    """
    by_reason: dict[str, dict] = {}
    by_state: dict[str, dict] = {}

    for t in trade_log:
        reason = t.get("trade_reason", "unknown")
        state = t.get("position_state", "unknown")
        value = t.get("shares", 0) * t.get("price", 0)
        cost = t.get("cost", 0)

        # Aggregate by reason
        if reason not in by_reason:
            by_reason[reason] = {"count": 0, "total_value": 0.0, "total_cost": 0.0}
        by_reason[reason]["count"] += 1
        by_reason[reason]["total_value"] += value
        by_reason[reason]["total_cost"] += cost

        # Aggregate by position state
        if state not in by_state:
            by_state[state] = {"count": 0, "total_value": 0.0, "total_cost": 0.0}
        by_state[state]["count"] += 1
        by_state[state]["total_value"] += value
        by_state[state]["total_cost"] += cost

    # Round values
    for d in list(by_reason.values()) + list(by_state.values()):
        d["total_value"] = round(d["total_value"], 2)
        d["total_cost"] = round(d["total_cost"], 2)

    # ---- Round-trip P&L computation ----
    # Track open buy lots per ticker as FIFO queue: [(shares, price, date, cost)]
    open_lots: dict[str, list[list]] = {}
    tactical_pnl = 0.0
    tactical_cost = 0.0
    tactical_trips = 0
    core_pnl = 0.0
    core_cost = 0.0
    core_trips = 0

    for t in trade_log:
        ticker = t["ticker"]
        shares = t.get("shares", 0.0)
        price = t.get("price", 0.0)
        cost = t.get("cost", 0.0)
        trade_date = t.get("date", "")

        if t["action"] == "buy":
            if ticker not in open_lots:
                open_lots[ticker] = []
            open_lots[ticker].append([shares, price, trade_date, cost])
        elif t["action"] == "sell" and ticker in open_lots:
            remaining = shares
            buy_cost_total = 0.0
            buy_trade_cost = 0.0
            earliest_buy_date = trade_date

            # Match against open lots FIFO
            while remaining > 1e-8 and open_lots[ticker]:
                lot = open_lots[ticker][0]
                lot_shares, lot_price, lot_date, lot_cost = lot
                matched = min(remaining, lot_shares)
                frac = matched / lot_shares if lot_shares > 1e-8 else 0

                buy_cost_total += matched * lot_price
                buy_trade_cost += lot_cost * frac
                earliest_buy_date = lot_date
                remaining -= matched
                lot[0] -= matched
                lot[3] -= lot_cost * frac

                if lot[0] < 1e-8:
                    open_lots[ticker].pop(0)

            if not open_lots[ticker]:
                del open_lots[ticker]

            # Only count the matched portion of the sell
            matched_shares = shares - remaining
            if matched_shares < 1e-8:
                continue
            sell_revenue = matched_shares * price
            sell_frac = matched_shares / shares if shares > 1e-8 else 0
            sell_cost = cost * sell_frac

            # Compute P&L for this round-trip
            trip_pnl = sell_revenue - buy_cost_total
            trip_cost = sell_cost + buy_trade_cost

            # Classify by holding duration
            holding = t.get("holding_days", 0)
            if holding <= 5:
                tactical_pnl += trip_pnl
                tactical_cost += trip_cost
                tactical_trips += 1
            else:
                core_pnl += trip_pnl
                core_cost += trip_cost
                core_trips += 1

    return {
        "by_reason": by_reason,
        "by_position_state": by_state,
        "tactical_pnl": round(tactical_pnl, 2),
        "tactical_cost": round(tactical_cost, 2),
        "tactical_net_pnl": round(tactical_pnl - tactical_cost, 2),
        "tactical_trips": tactical_trips,
        "core_pnl": round(core_pnl, 2),
        "core_cost": round(core_cost, 2),
        "core_net_pnl": round(core_pnl - core_cost, 2),
        "core_trips": core_trips,
    }


def _build_execution_rebalance_diagnostic(
    *,
    date_key: str,
    positions_before: dict[str, float],
    target_positions_after: dict[str, float],
    executed_positions_after: dict[str, float],
    executed_turnover: float,
) -> dict[str, Any]:
    before = _nonzero_weight_map(positions_before)
    target = _nonzero_weight_map(target_positions_after)
    executed = _nonzero_weight_map(executed_positions_after)
    target_turnover = sum(
        abs(target.get(ticker, 0.0) - before.get(ticker, 0.0))
        for ticker in set(before) | set(target)
    )
    return {
        "date": date_key,
        "positions_before": before,
        "positions_after": executed,
        "executed_positions_after": executed,
        "target_positions_after": target,
        "turnover": round(float(executed_turnover), 6),
        "target_turnover": round(float(target_turnover), 6),
        "diagnostic_layers": {
            "positions_after": "post_buffer",
            "executed_positions_after": "post_buffer",
            "target_positions_after": "pre_buffer",
        },
    }


def _nonzero_weight_map(weights: dict[str, float]) -> dict[str, float]:
    return {
        str(ticker): round(float(weight), 6)
        for ticker, weight in weights.items()
        if abs(float(weight)) > 1e-8
    }


def _lookup_price(
    prices: pd.DataFrame,
    trade_date: pd.Timestamp,
    ticker: str,
) -> float | None:
    if trade_date not in prices.index or ticker not in prices.columns:
        return None
    value = prices.loc[trade_date, ticker]
    if pd.isna(value) or value <= 0:
        return None
    return float(value)


def _lookup_planned_price(
    *,
    planned_prices: pd.DataFrame | None,
    decision_date: pd.Timestamp,
    ticker: str,
) -> float | None:
    if planned_prices is None:
        return None
    if decision_date not in planned_prices.index or ticker not in planned_prices.columns:
        return None
    value = planned_prices.loc[decision_date, ticker]
    if pd.isna(value) or value <= 0:
        return None
    return float(value)


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


def _calc_monthly_returns(
    dates: list[str], nav: list[float]
) -> list[dict]:
    """Calculate monthly returns for heatmap display."""
    if len(dates) < 2 or len(nav) < 2:
        return []

    df = pd.DataFrame({"date": pd.to_datetime(dates), "nav": nav})
    df.set_index("date", inplace=True)

    # Resample to month-end and compute returns
    monthly_nav = df["nav"].resample("ME").last()

    results: list[dict] = []
    prev_nav = None
    for dt, value in monthly_nav.items():
        if pd.isna(value):
            continue
        if prev_nav is not None and prev_nav > 0:
            ret = (value - prev_nav) / prev_nav
            results.append(
                {
                    "year": int(dt.year),
                    "month": int(dt.month),
                    "return": round(float(ret), 6),
                }
            )
        prev_nav = value

    return results
