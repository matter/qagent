"""Built-in strategy templates as source-code strings.

Each template is a complete, runnable Python snippet that:
- Inherits from ``StrategyBase``
- Implements ``generate_signals``
- Declares ``required_factors`` / ``required_models`` as applicable

These are stored as plain strings so users can view, modify, and use them
as starting points for custom strategies.
"""

from __future__ import annotations

TEMPLATES: dict[str, str] = {}

# ------------------------------------------------------------------
# 1. Momentum factor strategy
# ------------------------------------------------------------------
TEMPLATES["动量因子策略"] = '''\
"""动量因子策略：买入动量因子排名前50的股票。

Uses Momentum_20 factor values to rank stocks each rebalance date
and goes long the top 50 by momentum.
"""

import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class MomentumStrategy(StrategyBase):
    name = "动量因子策略"
    description = "买入20日动量因子排名前50的股票，等权配置"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        # Get momentum factor values for current date
        if "Momentum_20" not in context.factor_values:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        factor_df = context.factor_values["Momentum_20"]
        current_date = pd.Timestamp(context.current_date)

        # Find the latest available date <= current_date
        available = factor_df.index[factor_df.index <= current_date]
        if len(available) == 0:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        latest = available[-1]
        scores = factor_df.loc[latest].dropna()

        if len(scores) == 0:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        # Rank and select top 50
        top_n = 50
        ranked = scores.nlargest(min(top_n, len(scores)))

        # Build signals DataFrame
        signals = pd.DataFrame(index=ranked.index, columns=["signal", "weight", "strength"])
        signals["signal"] = 1
        signals["weight"] = 1.0 / len(ranked)
        signals["strength"] = ranked.values

        return signals

    def required_factors(self) -> list[str]:
        return ["Momentum_20"]

    def required_models(self) -> list[str]:
        return []
'''

# ------------------------------------------------------------------
# 2. Model prediction strategy
# ------------------------------------------------------------------
TEMPLATES["模型预测策略"] = '''\
"""模型预测策略：基于ML模型预测分数买入排名前50的股票。

Uses a trained model\'s prediction scores to rank stocks each
rebalance date and goes long the top 50 by predicted return.
"""

import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class ModelPredictionStrategy(StrategyBase):
    name = "模型预测策略"
    description = "基于机器学习模型预测分数，买入预测排名前50的股票"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        if not context.model_predictions:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        # Use the first available model\'s predictions
        model_id = list(context.model_predictions.keys())[0]
        preds = context.model_predictions[model_id]

        if preds.empty:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        scores = preds.dropna()
        if len(scores) == 0:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        # Select top 50 by prediction score
        top_n = 50
        ranked = scores.nlargest(min(top_n, len(scores)))

        signals = pd.DataFrame(index=ranked.index, columns=["signal", "weight", "strength"])
        signals["signal"] = 1
        signals["weight"] = 1.0 / len(ranked)
        signals["strength"] = ranked.values

        return signals

    def required_factors(self) -> list[str]:
        return []

    def required_models(self) -> list[str]:
        # The model ID should be configured when creating the strategy
        return []
'''

# ------------------------------------------------------------------
# 3. Multi-factor composite strategy
# ------------------------------------------------------------------
TEMPLATES["多因子综合策略"] = '''\
"""多因子综合策略：综合多个因子加权打分，买入排名前50的股票。

Combines Momentum_20, RSI_14, and Volatility_20 factors with
configurable weights to produce a composite score, then goes
long the top 50 stocks.
"""

import pandas as pd
import numpy as np
from backend.strategies.base import StrategyBase, StrategyContext


class MultiFactorStrategy(StrategyBase):
    name = "多因子综合策略"
    description = "综合动量、RSI、波动率等多因子加权打分，买入排名前50的股票"

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        current_date = pd.Timestamp(context.current_date)

        # Define factor weights (positive = prefer higher, negative = prefer lower)
        factor_weights = {
            "Momentum_20": 0.4,    # Higher momentum is better
            "RSI_14": -0.3,        # Lower RSI (oversold) is better
            "Volatility_20": -0.3, # Lower volatility is better
        }

        score_components = []
        for factor_name, weight in factor_weights.items():
            if factor_name not in context.factor_values:
                continue

            factor_df = context.factor_values[factor_name]
            available = factor_df.index[factor_df.index <= current_date]
            if len(available) == 0:
                continue

            latest = available[-1]
            values = factor_df.loc[latest].dropna()
            if len(values) == 0:
                continue

            # Cross-sectional rank (percentile)
            ranked = values.rank(pct=True)
            score_components.append(ranked * weight)

        if not score_components:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        # Combine scores
        combined = score_components[0]
        for sc in score_components[1:]:
            combined = combined.add(sc, fill_value=0)
        combined = combined.dropna()

        if len(combined) == 0:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        # Select top 50
        top_n = 50
        ranked = combined.nlargest(min(top_n, len(combined)))

        signals = pd.DataFrame(index=ranked.index, columns=["signal", "weight", "strength"])
        signals["signal"] = 1
        signals["weight"] = 1.0 / len(ranked)
        signals["strength"] = ranked.values

        return signals

    def required_factors(self) -> list[str]:
        return ["Momentum_20", "RSI_14", "Volatility_20"]

    def required_models(self) -> list[str]:
        return []
'''

# ------------------------------------------------------------------
# 4. Planned execution intent strategy
# ------------------------------------------------------------------
TEMPLATES["计划交易策略模板"] = '''\
"""计划交易策略模板：在策略层声明默认参数，并为每只股票输出成交意图。

策略输出的 execution_model / planned_price 等列只描述意图；
真实成交、失败、fallback、成本和持仓变化由共享回测/模拟交易引擎判定。
"""

import pandas as pd
from backend.strategies.base import StrategyBase, StrategyContext


class PlannedExecutionStrategy(StrategyBase):
    name = "计划交易策略模板"
    description = "展示 default_backtest_config、default_paper_config 和逐订单成交意图"

    default_backtest_config = {
        "position_sizing": "raw_weight",
        "max_positions": 20,
        "rebalance_freq": "daily",
        "normalize_target_weights": False,
        "execution_model": "planned_price",
        "planned_price_buffer_bps": 50,
        "planned_price_fallback": "next_close",
        "constraint_config": {"max_single_name_weight": 0.08},
    }
    default_paper_config = {
        "position_sizing": "raw_weight",
        "max_positions": 20,
        "rebalance_freq": "daily",
        "normalize_target_weights": False,
        "execution_model": "planned_price",
        "planned_price_buffer_bps": 50,
        "planned_price_fallback": "next_close",
        "constraint_config": {"max_single_name_weight": 0.08},
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        prices = context.prices
        current_date = pd.Timestamp(context.current_date)
        if ("close" not in prices.columns.get_level_values(0)):
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        close = prices["close"]
        available = close.index[close.index <= current_date]
        if len(available) < 20:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        latest = available[-1]
        momentum = close.loc[latest] / close.loc[available[-20]] - 1.0
        ranked = momentum.dropna().nlargest(10)
        if ranked.empty:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        rows = []
        weight = min(0.08, 0.8 / len(ranked))
        latest_close = close.loc[latest]
        for ticker, score in ranked.items():
            planned_price = float(latest_close[ticker]) * 1.002
            rows.append(
                {
                    "ticker": ticker,
                    "signal": 1,
                    "weight": weight,
                    "strength": float(score),
                    "execution_model": "planned_price",
                    "planned_price": planned_price,
                    "planned_price_buffer_bps": 50,
                    "planned_price_fallback": "next_close",
                    "time_in_force": "day",
                    "order_reason": "20d momentum planned entry",
                }
            )
        return pd.DataFrame(rows).set_index("ticker")

    def required_factors(self) -> list[str]:
        return []

    def required_models(self) -> list[str]:
        return []
'''


def get_template_names() -> list[str]:
    """Return sorted list of all available strategy template names."""
    return sorted(TEMPLATES.keys())


def get_template_source(name: str) -> str | None:
    """Return source code for the named template, or None if not found."""
    return TEMPLATES.get(name)
