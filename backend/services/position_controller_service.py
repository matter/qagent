"""Position controller for 3.0 portfolio order intents.

The controller filters target-delta orders before execution simulation. It
keeps strategy intent auditable while avoiding daily micro rebalances.
"""

from __future__ import annotations

from typing import Any


class PositionControllerService:
    """Apply low-turnover position controls to order intents."""

    def apply(
        self,
        *,
        orders: list[dict[str, Any]],
        params: dict[str, Any] | None = None,
        portfolio_value: float = 1_000_000,
    ) -> dict[str, Any]:
        params = params or {}
        portfolio_value = float(portfolio_value or 0.0)
        rebalance_band = _optional_float(params.get("rebalance_band", params.get("band")))
        min_weight_delta = _optional_float(params.get("min_weight_delta"))
        min_trade_value = _optional_float(params.get("min_trade_value"))
        turnover_budget = _optional_float(params.get("turnover_budget"))

        kept = []
        skipped = []
        turnover_before = sum(abs(float(order.get("delta_weight") or 0.0)) for order in orders)
        turnover_used = 0.0
        forced_exit_count = 0

        for order in sorted(orders, key=self._priority_key):
            delta = float(order.get("delta_weight") or 0.0)
            trade_value = abs(float(order.get("estimated_value") or 0.0))
            if trade_value <= 0 and portfolio_value > 0:
                trade_value = abs(delta) * portfolio_value
            force_trade = self._is_forced_trade(order)
            if force_trade:
                forced_exit_count += 1

            reasons = []
            if not force_trade:
                if rebalance_band is not None and abs(delta) < rebalance_band:
                    reasons.append("below_rebalance_band")
                if min_weight_delta is not None and abs(delta) < min_weight_delta:
                    reasons.append("below_min_weight_delta")
                if min_trade_value is not None and trade_value < min_trade_value:
                    reasons.append("below_min_trade_value")
                if (
                    turnover_budget is not None
                    and turnover_used + abs(delta) > turnover_budget + 1e-12
                ):
                    reasons.append("turnover_budget_exceeded")

            if reasons:
                skipped.append(self._skipped_order(order, reasons=reasons, trade_value=trade_value))
                continue

            enriched = dict(order)
            enriched["position_controller_status"] = "kept"
            if force_trade:
                enriched["force_trade"] = True
            kept.append(enriched)
            turnover_used += abs(delta)

        turnover_after = sum(abs(float(order.get("delta_weight") or 0.0)) for order in kept)
        diagnostics = {
            "status": "controlled",
            "input_order_count": len(orders),
            "output_order_count": len(kept),
            "skipped_rebalance_count": len(skipped),
            "skipped_rebalance": skipped,
            "drift": skipped,
            "turnover_before": round(turnover_before, 12),
            "turnover_after": round(turnover_after, 12),
            "turnover_saved": round(turnover_before - turnover_after, 12),
            "forced_exit_count": forced_exit_count,
            "params": {
                key: value
                for key, value in {
                    "rebalance_band": rebalance_band,
                    "min_weight_delta": min_weight_delta,
                    "min_trade_value": min_trade_value,
                    "turnover_budget": turnover_budget,
                }.items()
                if value is not None
            },
        }
        return {"orders": kept, "diagnostics": diagnostics}

    @staticmethod
    def _priority_key(order: dict[str, Any]) -> tuple[int, float, str]:
        force_rank = 0 if PositionControllerService._is_forced_trade(order) else 1
        priority = -float(order.get("priority") or 0.0)
        return (force_rank, priority, str(order.get("asset_id") or ""))

    @staticmethod
    def _is_forced_trade(order: dict[str, Any]) -> bool:
        if bool(order.get("force_trade")):
            return True
        reason = str(order.get("order_reason") or order.get("reason") or "")
        return "forced_exit" in reason or "risk_exit" in reason

    @staticmethod
    def _skipped_order(
        order: dict[str, Any],
        *,
        reasons: list[str],
        trade_value: float,
    ) -> dict[str, Any]:
        return {
            "asset_id": order.get("asset_id"),
            "side": order.get("side"),
            "current_weight": order.get("current_weight"),
            "target_weight": order.get("target_weight"),
            "delta_weight": round(float(order.get("delta_weight") or 0.0), 12),
            "estimated_value": round(trade_value, 6),
            "reasons": reasons,
        }


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric > 0 else None
