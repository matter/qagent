"""Portfolio valuation helpers shared by backtest and paper trading."""

from __future__ import annotations

from typing import Any

from backend.services.market_data_foundation_service import MarketDataFoundationService


class PortfolioValuationService:
    """Revalue weight portfolios from stored daily bars."""

    def __init__(self, *, market_data_service: MarketDataFoundationService | None = None) -> None:
        self.market_data = market_data_service or MarketDataFoundationService()

    def revalue_weights(
        self,
        *,
        market_profile_id: str,
        from_date: str | None,
        to_date: str,
        nav: float,
        weights: dict[str, float] | None,
        price_field: str = "close",
    ) -> dict[str, Any]:
        current_weights = {
            str(asset_id): float(weight)
            for asset_id, weight in (weights or {}).items()
            if abs(float(weight)) > 1e-12
        }
        if not from_date or from_date == to_date or not current_weights:
            return {
                "nav": float(nav),
                "weights": current_weights,
                "diagnostics": {
                    "status": "flat",
                    "from_date": from_date,
                    "to_date": to_date,
                    "price_field": price_field,
                    "reason": "no prior valuation interval or no holdings",
                },
            }

        bars = self.market_data.query_bars(
            market_profile_id=market_profile_id,
            asset_ids=list(current_weights),
            start=from_date,
            end=to_date,
            limit=max(len(current_weights) * 4, 100),
        )["bars"]
        price_by_asset_date: dict[tuple[str, str], float] = {}
        for row in bars:
            value = row.get(price_field)
            if value is None or float(value) <= 0:
                continue
            price_by_asset_date[(row["asset_id"], row["date"])] = float(value)

        missing: list[dict[str, str]] = []
        relatives: dict[str, float] = {}
        for asset_id in current_weights:
            start_price = price_by_asset_date.get((asset_id, from_date))
            end_price = price_by_asset_date.get((asset_id, to_date))
            if start_price is None or end_price is None:
                missing.append(
                    {
                        "asset_id": asset_id,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
                continue
            relatives[asset_id] = end_price / start_price

        if missing:
            return {
                "nav": float(nav),
                "weights": current_weights,
                "diagnostics": {
                    "status": "missing_prices",
                    "from_date": from_date,
                    "to_date": to_date,
                    "price_field": price_field,
                    "missing": missing,
                    "reason": "valuation skipped because one or more held assets lack endpoint prices",
                },
            }

        invested = sum(abs(weight) for weight in current_weights.values())
        cash_weight = max(1.0 - invested, 0.0)
        valuation_factor = cash_weight + sum(
            float(current_weights[asset_id]) * relatives[asset_id]
            for asset_id in current_weights
        )
        if valuation_factor <= 0:
            return {
                "nav": float(nav),
                "weights": current_weights,
                "diagnostics": {
                    "status": "invalid_valuation_factor",
                    "from_date": from_date,
                    "to_date": to_date,
                    "price_field": price_field,
                    "valuation_factor": valuation_factor,
                },
            }

        drifted = {
            asset_id: weight * relatives[asset_id] / valuation_factor
            for asset_id, weight in current_weights.items()
            if abs(weight * relatives[asset_id] / valuation_factor) > 1e-12
        }
        return {
            "nav": float(nav) * valuation_factor,
            "weights": drifted,
            "diagnostics": {
                "status": "valued",
                "from_date": from_date,
                "to_date": to_date,
                "price_field": price_field,
                "valuation_factor": round(valuation_factor, 12),
                "asset_count": len(current_weights),
                "cash_weight": round(cash_weight, 12),
            },
        }
