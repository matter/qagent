"""Shared daily-bar execution simulator for 3.0 order intents."""

from __future__ import annotations

import json
import math
import uuid
from typing import Any

from backend.db import get_connection
from backend.services.execution_model_service import (
    evaluate_planned_price_fill,
    normalize_planned_price_buffer_bps,
    normalize_planned_price_fallback,
)
from backend.services.market_context import normalize_market
from backend.services.market_data_foundation_service import MarketDataFoundationService
from backend.time_utils import utc_now_naive


SUPPORTED_ORDER_EXECUTION_MODELS = {
    "next_open",
    "next_close",
    "planned_price",
    "limit",
    "stop",
    "stop_limit",
}


class ExecutionSimulatorService:
    """Evaluate low-frequency order intents against daily bars."""

    def __init__(self, *, market_data_service: MarketDataFoundationService | None = None) -> None:
        self.market_data = market_data_service or MarketDataFoundationService()

    def execute_orders(
        self,
        *,
        backtest_run_id: str,
        order_intents: list[dict[str, Any]],
        market_profile_id: str,
        nav: float,
    ) -> dict[str, Any]:
        now = utc_now_naive()
        rows = []
        diagnostics = {
            "status": "no_orders" if not order_intents else "evaluated",
            "filled": [],
            "blocked": [],
            "missing_price": [],
            "path_assumption_warnings": [],
            "path_assumption_warning_count": 0,
            "execution_model_counts": {},
            "cost_model": {},
            "trading_rules": {},
        }
        if not order_intents:
            return {
                "filled_order_count": 0,
                "blocked_order_count": 0,
                "total_cost": 0.0,
                "diagnostics": diagnostics,
            }

        profile = self.market_data.get_market_profile(market_profile_id)
        market = normalize_market(profile["market_code"])
        cost_model = profile.get("cost_model") or {}
        trading_rules = profile.get("trading_rule_set") or {}
        diagnostics["cost_model"] = {
            "commission_rate": float(cost_model.get("commission_rate") or 0.0),
            "slippage_rate": float(cost_model.get("slippage_rate") or 0.0),
            "stamp_tax_rate": float(cost_model.get("stamp_tax_rate") or 0.0),
            "min_commission": float(cost_model.get("min_commission") or 0.0),
        }
        diagnostics["trading_rules"] = {
            "lot_size": int(trading_rules.get("lot_size") or 1),
            "limit_up_down": bool(trading_rules.get("limit_up_down")),
        }

        normalized_orders = [self._normalize_order(order) for order in order_intents]
        for order in normalized_orders:
            model = str(order["execution_model"])
            diagnostics["execution_model_counts"][model] = diagnostics["execution_model_counts"].get(model, 0) + 1

        asset_ids = sorted({str(order["asset_id"]) for order in normalized_orders})
        execution_dates = sorted(
            {
                str(order.get("execution_date"))
                for order in normalized_orders
                if order.get("execution_date")
            }
            | {
                str(order.get("decision_date"))
                for order in normalized_orders
                if order["execution_model"] == "planned_price" and order.get("decision_date")
            }
        )
        prices = self._load_execution_prices(
            market_profile_id=market_profile_id,
            asset_ids=asset_ids,
            execution_dates=execution_dates,
            price_fields=["open", "high", "low", "close"],
        )
        statuses = self._load_trade_status(
            market_profile_id=market_profile_id,
            asset_ids=asset_ids,
            execution_dates=execution_dates,
        )

        filled_count = 0
        blocked_count = 0
        total_cost = 0.0
        for order in normalized_orders:
            execution_date = str(order.get("execution_date"))
            asset_id = str(order["asset_id"])
            side = str(order["side"])
            estimated_value = abs(float(order.get("estimated_value") or 0.0))
            fill = self._evaluate_order_fill(order, prices)
            if fill.get("block_reason"):
                block_reason = str(fill["block_reason"])
            else:
                block_reason = self._execution_block_reason(
                    order=order,
                    status=statuses.get((asset_id, execution_date)),
                    price=fill["price"],
                    market=market,
                    trading_rules=trading_rules,
                )

            quantity = None
            value = estimated_value
            cost = None
            metadata = {**order, **fill["metadata"]}
            warnings = list(fill.get("warnings") or [])
            if warnings:
                diagnostics["path_assumption_warnings"].append(
                    {"asset_id": asset_id, "execution_date": execution_date, "warnings": warnings}
                )
            if block_reason:
                blocked_count += 1
                metadata["fill_status"] = "blocked"
                metadata["block_reason"] = block_reason
                diagnostics["blocked"].append(
                    {
                        "asset_id": asset_id,
                        "execution_date": execution_date,
                        "side": side,
                        "reason": block_reason,
                        "fill_type": fill.get("fill_type", "blocked"),
                        "warnings": warnings,
                        **fill.get("diagnostics", {}),
                    }
                )
                if block_reason == "missing_execution_price":
                    diagnostics["missing_price"].append(
                        {
                            "asset_id": asset_id,
                            "execution_date": execution_date,
                            "price_field": fill.get("price_field"),
                        }
                    )
            else:
                price = float(fill["price"])
                quantity = self._order_quantity(
                    side=side,
                    estimated_value=estimated_value,
                    price=price,
                    trading_rules=trading_rules,
                )
                if quantity <= 0:
                    blocked_count += 1
                    metadata["fill_status"] = "blocked"
                    metadata["block_reason"] = "quantity_rounds_to_zero"
                    diagnostics["blocked"].append(
                        {
                            "asset_id": asset_id,
                            "execution_date": execution_date,
                            "side": side,
                            "reason": "quantity_rounds_to_zero",
                            "fill_type": fill.get("fill_type", "blocked"),
                            "warnings": warnings,
                        }
                    )
                    quantity = None
                else:
                    value = abs(float(quantity) * price)
                    cost = self._execution_cost(side=side, trade_value=value, cost_model=cost_model)
                    total_cost += cost
                    filled_count += 1
                    metadata["fill_status"] = "filled"
                    metadata["execution_price"] = price
                    metadata["quantity"] = quantity
                    metadata["cost"] = cost
                    diagnostics["filled"].append(
                        {
                            "asset_id": asset_id,
                            "execution_date": execution_date,
                            "side": side,
                            "target_weight": float(order.get("target_weight") or 0.0),
                            "quantity": quantity,
                            "price": price,
                            "value": round(value, 6),
                            "cost": round(cost, 6),
                            "fill_type": fill.get("fill_type"),
                            "warnings": warnings,
                            **fill.get("diagnostics", {}),
                        }
                    )
            rows.append(
                [
                    uuid.uuid4().hex[:12],
                    backtest_run_id,
                    order.get("decision_date"),
                    execution_date,
                    asset_id,
                    side,
                    quantity,
                    fill["price"] if quantity is not None else None,
                    value,
                    cost,
                    json.dumps(metadata, default=str),
                    now,
                ]
            )

        if rows:
            get_connection().executemany(
                """INSERT INTO backtest_trades
                   (id, backtest_run_id, decision_date, execution_date, asset_id,
                    side, quantity, price, value, cost, metadata, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
        diagnostics["status"] = "filled" if blocked_count == 0 else ("partial" if filled_count else "blocked")
        diagnostics["filled_order_count"] = filled_count
        diagnostics["blocked_order_count"] = blocked_count
        diagnostics["total_cost"] = round(total_cost, 6)
        diagnostics["nav_after_cost"] = round(float(nav) - total_cost, 6)
        diagnostics["path_assumption_warning_count"] = len(diagnostics["path_assumption_warnings"])
        diagnostics["execution_model"] = (
            "mixed"
            if len(diagnostics["execution_model_counts"]) > 1
            else next(iter(diagnostics["execution_model_counts"]), "next_open")
        )
        return {
            "filled_order_count": filled_count,
            "blocked_order_count": blocked_count,
            "total_cost": total_cost,
            "diagnostics": diagnostics,
        }

    def _normalize_order(self, order: dict[str, Any]) -> dict[str, Any]:
        model = str(order.get("execution_model") or "next_open")
        if model not in SUPPORTED_ORDER_EXECUTION_MODELS:
            raise ValueError(f"Unsupported execution_model {model!r}")
        normalized = dict(order)
        normalized["execution_model"] = model
        if model == "next_close":
            normalized["price_field"] = "close"
        elif model == "next_open":
            normalized["price_field"] = str(normalized.get("price_field") or "open")
        elif model in {"limit", "stop", "stop_limit"}:
            normalized["price_field"] = str(normalized.get("price_field") or "close")
        return normalized

    def _evaluate_order_fill(
        self,
        order: dict[str, Any],
        prices: dict[tuple[str, str, str], float],
    ) -> dict[str, Any]:
        model = str(order["execution_model"])
        asset_id = str(order["asset_id"])
        execution_date = str(order.get("execution_date"))
        decision_date = str(order.get("decision_date"))
        side = str(order.get("side"))
        if model in {"next_open", "next_close"}:
            price_field = str(order.get("price_field") or ("close" if model == "next_close" else "open"))
            price = prices.get((asset_id, execution_date, price_field))
            return {
                "price": price,
                "price_field": price_field,
                "fill_type": model,
                "block_reason": None if price else "missing_execution_price",
                "warnings": [],
                "metadata": {"execution_model": model, "price_field": price_field, "fill_type": model},
                "diagnostics": {},
            }
        if model == "planned_price":
            planned_price = _positive_float(order.get("planned_price"))
            planned_price_source = str(order.get("planned_price_source") or "strategy_output")
            if planned_price is None and planned_price_source == "decision_close":
                planned_price = prices.get((asset_id, decision_date, "close"))
            high_price = prices.get((asset_id, execution_date, "high"))
            low_price = prices.get((asset_id, execution_date, "low"))
            close_price = prices.get((asset_id, execution_date, "close"))
            fill_fallback = normalize_planned_price_fallback(order.get("fill_fallback"))
            fill_decision = evaluate_planned_price_fill(
                planned_price=planned_price,
                high=high_price,
                low=low_price,
                buffer_bps=order.get("planned_price_buffer_bps"),
            )
            price = fill_decision.fill_price
            block_reason = fill_decision.reason
            fill_type = "planned_price" if fill_decision.filled else "blocked"
            if (
                not fill_decision.filled
                and fill_fallback == "next_close"
                and block_reason == "planned_price_outside_buffered_range"
                and close_price is not None
                and close_price > 0
            ):
                price = float(close_price)
                block_reason = None
                fill_type = "fallback_close"
            if block_reason == "missing_high_low":
                block_reason = "missing_execution_price"
            metadata = {
                "execution_model": model,
                "price_field": "planned_price",
                "planned_price": planned_price,
                "planned_price_source": planned_price_source,
                "planned_price_buffer_bps": normalize_planned_price_buffer_bps(order.get("planned_price_buffer_bps")),
                "fill_fallback": fill_fallback,
                "fill_type": fill_type,
                "execution_high": high_price,
                "execution_low": low_price,
                "execution_close": close_price,
                "planned_price_bounds": {
                    "lower": fill_decision.lower_bound,
                    "upper": fill_decision.upper_bound,
                },
            }
            return {
                "price": price,
                "price_field": "planned_price",
                "fill_type": fill_type,
                "block_reason": block_reason,
                "warnings": [],
                "metadata": metadata,
                "diagnostics": {
                    "planned_price": planned_price,
                    "planned_price_source": planned_price_source,
                    "fill_fallback": fill_fallback,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                },
            }
        if model == "limit":
            return self._evaluate_limit_order(order, prices)
        if model == "stop":
            return self._evaluate_stop_order(order, prices)
        if model == "stop_limit":
            return self._evaluate_stop_limit_order(order, prices)
        raise ValueError(f"Unsupported execution_model {model!r}")

    def _evaluate_limit_order(
        self,
        order: dict[str, Any],
        prices: dict[tuple[str, str, str], float],
    ) -> dict[str, Any]:
        asset_id = str(order["asset_id"])
        execution_date = str(order.get("execution_date"))
        side = str(order["side"])
        limit_price = _positive_float(order.get("limit_price"))
        high = prices.get((asset_id, execution_date, "high"))
        low = prices.get((asset_id, execution_date, "low"))
        warnings = ["daily_bar_no_intraday_path"]
        block_reason = None
        price = None
        if limit_price is None:
            block_reason = "invalid_limit_price"
        elif high is None or low is None:
            block_reason = "missing_execution_price"
        elif side == "buy" and float(low) <= limit_price:
            price = limit_price
        elif side == "sell" and float(high) >= limit_price:
            price = limit_price
        else:
            block_reason = "limit_not_reached"
        return self._conditional_result(
            order=order,
            price=price,
            fill_type="limit" if price is not None else "blocked",
            block_reason=block_reason,
            warnings=warnings,
            diagnostics={"limit_price": limit_price, "high": high, "low": low},
        )

    def _evaluate_stop_order(
        self,
        order: dict[str, Any],
        prices: dict[tuple[str, str, str], float],
    ) -> dict[str, Any]:
        asset_id = str(order["asset_id"])
        execution_date = str(order.get("execution_date"))
        side = str(order["side"])
        stop_price = _positive_float(order.get("stop_price"))
        high = prices.get((asset_id, execution_date, "high"))
        low = prices.get((asset_id, execution_date, "low"))
        close = prices.get((asset_id, execution_date, "close"))
        warnings = ["daily_bar_no_intraday_path"]
        block_reason = None
        price = None
        if stop_price is None:
            block_reason = "invalid_stop_price"
        elif high is None or low is None or close is None:
            block_reason = "missing_execution_price"
        elif side == "buy" and float(high) >= stop_price:
            price = float(close)
        elif side == "sell" and float(low) <= stop_price:
            price = float(close)
        else:
            block_reason = "stop_not_triggered"
        return self._conditional_result(
            order=order,
            price=price,
            fill_type="stop" if price is not None else "blocked",
            block_reason=block_reason,
            warnings=warnings,
            diagnostics={"stop_price": stop_price, "high": high, "low": low, "close": close},
        )

    def _evaluate_stop_limit_order(
        self,
        order: dict[str, Any],
        prices: dict[tuple[str, str, str], float],
    ) -> dict[str, Any]:
        asset_id = str(order["asset_id"])
        execution_date = str(order.get("execution_date"))
        side = str(order["side"])
        stop_price = _positive_float(order.get("stop_price"))
        limit_price = _positive_float(order.get("limit_price"))
        high = prices.get((asset_id, execution_date, "high"))
        low = prices.get((asset_id, execution_date, "low"))
        warnings = ["daily_bar_no_intraday_path", "stop_limit_path_order_unknown"]
        block_reason = None
        price = None
        if stop_price is None:
            block_reason = "invalid_stop_price"
        elif limit_price is None:
            block_reason = "invalid_limit_price"
        elif high is None or low is None:
            block_reason = "missing_execution_price"
        else:
            triggered = float(high) >= stop_price if side == "buy" else float(low) <= stop_price
            limit_reached = float(low) <= limit_price if side == "buy" else float(high) >= limit_price
            if triggered and limit_reached:
                price = limit_price
            elif not triggered:
                block_reason = "stop_not_triggered"
            else:
                block_reason = "stop_limit_not_reached"
        return self._conditional_result(
            order=order,
            price=price,
            fill_type="stop_limit" if price is not None else "blocked",
            block_reason=block_reason,
            warnings=warnings,
            diagnostics={"stop_price": stop_price, "limit_price": limit_price, "high": high, "low": low},
        )

    @staticmethod
    def _conditional_result(
        *,
        order: dict[str, Any],
        price: float | None,
        fill_type: str,
        block_reason: str | None,
        warnings: list[str],
        diagnostics: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "price": price,
            "price_field": str(order.get("price_field") or "close"),
            "fill_type": fill_type,
            "block_reason": block_reason,
            "warnings": warnings,
            "metadata": {
                "execution_model": order["execution_model"],
                "price_field": str(order.get("price_field") or "close"),
                "fill_type": fill_type,
                "path_assumption": "daily_bar_no_intraday_path",
                "warnings": warnings,
                **diagnostics,
            },
            "diagnostics": diagnostics,
        }

    def _load_execution_prices(
        self,
        *,
        market_profile_id: str,
        asset_ids: list[str],
        execution_dates: list[str],
        price_fields: list[str],
    ) -> dict[tuple[str, str, str], float]:
        if not asset_ids or not execution_dates:
            return {}
        allowed_fields = {"open", "high", "low", "close"}
        fields = [field for field in price_fields if field in allowed_fields] or ["open"]
        bars = self.market_data.query_bars(
            market_profile_id=market_profile_id,
            asset_ids=asset_ids,
            start=min(execution_dates),
            end=max(execution_dates),
            limit=max(len(asset_ids) * len(execution_dates) * 2, 100),
        )["bars"]
        result: dict[tuple[str, str, str], float] = {}
        execution_date_set = set(execution_dates)
        for row in bars:
            row_date = str(row["date"])
            if row_date not in execution_date_set:
                continue
            for field in fields:
                value = row.get(field)
                if value is None:
                    continue
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    continue
                if numeric > 0:
                    result[(str(row["asset_id"]), row_date, field)] = numeric
        return result

    @staticmethod
    def _load_trade_status(
        *,
        market_profile_id: str,
        asset_ids: list[str],
        execution_dates: list[str],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        if not asset_ids or not execution_dates:
            return {}
        asset_placeholders = ",".join("?" for _ in asset_ids)
        date_placeholders = ",".join("?" for _ in execution_dates)
        rows = get_connection().execute(
            f"""SELECT asset_id, date, is_trading, is_suspended, is_st,
                       limit_up, limit_down, metadata
                FROM trade_status
                WHERE market_profile_id = ?
                  AND asset_id IN ({asset_placeholders})
                  AND date IN ({date_placeholders})""",
            [market_profile_id, *asset_ids, *execution_dates],
        ).fetchall()
        return {
            (str(row[0]), str(row[1])): {
                "is_trading": bool(row[2]),
                "is_suspended": bool(row[3]),
                "is_st": bool(row[4]),
                "limit_up": row[5],
                "limit_down": row[6],
                "metadata": _json(row[7], {}),
            }
            for row in rows
        }

    @staticmethod
    def _execution_block_reason(
        *,
        order: dict[str, Any],
        status: dict[str, Any] | None,
        price: float | None,
        market: str,
        trading_rules: dict[str, Any],
    ) -> str | None:
        if price is None or float(price) <= 0:
            return "missing_execution_price"
        if not status:
            return None
        if status.get("is_suspended") or status.get("is_trading") is False:
            return "suspended"
        if market == "CN" and status.get("is_st") and order.get("side") == "buy":
            return "st_buy_blocked"
        if bool(trading_rules.get("limit_up_down")):
            side = str(order.get("side"))
            limit_up = status.get("limit_up")
            limit_down = status.get("limit_down")
            if side == "buy" and limit_up is not None and math.isclose(float(price), float(limit_up), rel_tol=0, abs_tol=1e-9):
                return "limit_up_buy_blocked"
            if side == "sell" and limit_down is not None and math.isclose(float(price), float(limit_down), rel_tol=0, abs_tol=1e-9):
                return "limit_down_sell_blocked"
        return None

    @staticmethod
    def _order_quantity(
        *,
        side: str,
        estimated_value: float,
        price: float,
        trading_rules: dict[str, Any],
    ) -> float:
        if price <= 0:
            return 0.0
        raw = estimated_value / price
        lot_size = int(trading_rules.get("lot_size") or 1)
        if side == "buy" and lot_size > 1:
            return float(math.floor(raw / lot_size) * lot_size)
        return float(raw)

    @staticmethod
    def _execution_cost(*, side: str, trade_value: float, cost_model: dict[str, Any]) -> float:
        commission_rate = float(cost_model.get("commission_rate") or 0.0)
        slippage_rate = float(cost_model.get("slippage_rate") or 0.0)
        stamp_tax_rate = float(cost_model.get("stamp_tax_rate") or 0.0)
        min_commission = float(cost_model.get("min_commission") or 0.0)
        commission = max(trade_value * commission_rate, min_commission) if trade_value > 0 else 0.0
        slippage = trade_value * slippage_rate
        stamp_tax = trade_value * stamp_tax_rate if side == "sell" else 0.0
        return round(commission + slippage + stamp_tax, 6)


def _positive_float(value: float | int | str | None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0:
        return None
    return numeric


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default
