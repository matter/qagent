"""Shared execution-model helpers for backtest and paper trading runtimes."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal


ExecutionModel = Literal["next_open", "planned_price"]
PlannedPriceFallback = Literal["cancel", "next_close"]

DEFAULT_EXECUTION_MODEL: ExecutionModel = "next_open"
DEFAULT_PLANNED_PRICE_FALLBACK: PlannedPriceFallback = "cancel"
DEFAULT_PLANNED_PRICE_BUFFER_BPS = 50.0
SUPPORTED_EXECUTION_MODELS = {"next_open", "planned_price"}
SUPPORTED_PLANNED_PRICE_FALLBACKS = {"cancel", "next_close"}


@dataclass(frozen=True)
class FillDecision:
    filled: bool
    fill_price: float | None
    reason: str | None
    lower_bound: float | None
    upper_bound: float | None
    metadata: dict


def normalize_execution_model(value: str | None) -> ExecutionModel:
    model = (value or DEFAULT_EXECUTION_MODEL).strip()
    if model not in SUPPORTED_EXECUTION_MODELS:
        raise ValueError(f"Unsupported execution_model {value!r}")
    return model  # type: ignore[return-value]


def normalize_planned_price_fallback(value: str | None) -> PlannedPriceFallback:
    fallback = (value or DEFAULT_PLANNED_PRICE_FALLBACK).strip()
    if fallback not in SUPPORTED_PLANNED_PRICE_FALLBACKS:
        raise ValueError(
            "Unsupported planned_price_fallback "
            f"{value!r}; supported values: {sorted(SUPPORTED_PLANNED_PRICE_FALLBACKS)}"
        )
    return fallback  # type: ignore[return-value]


def normalize_planned_price_buffer_bps(value: int | float | None) -> float:
    if value is None:
        return DEFAULT_PLANNED_PRICE_BUFFER_BPS
    try:
        buffer_bps = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid planned_price_buffer_bps {value!r}") from exc
    if not math.isfinite(buffer_bps) or buffer_bps < 0:
        raise ValueError(f"Invalid planned_price_buffer_bps {value!r}")
    if buffer_bps >= 5000:
        raise ValueError("planned_price_buffer_bps must be below 5000")
    return buffer_bps


def evaluate_planned_price_fill(
    *,
    planned_price: float | None,
    high: float | None,
    low: float | None,
    buffer_bps: int | float | None = None,
) -> FillDecision:
    buffer_value = normalize_planned_price_buffer_bps(buffer_bps)
    price = _positive_float(planned_price)
    high_value = _positive_float(high)
    low_value = _positive_float(low)
    metadata = {
        "execution_model": "planned_price",
        "planned_price": planned_price,
        "high": high,
        "low": low,
        "planned_price_buffer_bps": buffer_value,
    }

    if price is None:
        return FillDecision(False, None, "invalid_planned_price", None, None, metadata)
    if high_value is None or low_value is None:
        return FillDecision(False, None, "missing_high_low", None, None, metadata)
    if low_value > high_value:
        return FillDecision(False, None, "invalid_high_low_range", None, None, metadata)

    buffer = buffer_value / 10000.0
    lower_bound = low_value * (1.0 + buffer)
    upper_bound = high_value * (1.0 - buffer)
    metadata["lower_bound"] = lower_bound
    metadata["upper_bound"] = upper_bound
    if lower_bound <= price <= upper_bound:
        return FillDecision(True, price, None, lower_bound, upper_bound, metadata)
    return FillDecision(
        False,
        None,
        "planned_price_outside_buffered_range",
        lower_bound,
        upper_bound,
        metadata,
    )


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
