# [2026-05-09] P2 StrategyGraph execution-grade backtest fill model

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：US, CN 3.0 StrategyGraph backtest.
- **入口**：`StrategyGraph3Service.backtest_graph`
- **现象**：3.0 StrategyGraph 回测能产出组合目标和 NAV，但成交记录、价格、数量、费用、停牌/ST/涨跌停/缺价诊断不够执行级。

## 修复记录

- 回测订单意图现在转换为 `backtest_trades` 成交/未成交记录。
- 成交记录包含执行价、数量、成交金额、费用、`fill_status`、阻断原因和执行模型 metadata。
- 使用 market profile 的交易规则和成本模型。
- CN 买入按 lot size 向下取整。
- 缺执行价、停牌/不可交易、CN ST 买入、涨停买入、跌停卖出会阻断并进入 diagnostics。
- NAV 扣减总交易成本，summary 输出 `total_cost`、`fill_diagnostics`、`valuation_warnings`。

## 验证

- `uv run python -m unittest tests.test_strategy_graph_3_service.StrategyGraph3ServiceContractTests -v`

## 残余风险

- 目前是低频研究级成交模型，不覆盖盘口撮合、部分成交、分钟级冲击成本或真实券商订单状态。
