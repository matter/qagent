# [2026-05-03] P2 研究验收：回测摘要缺少分散持仓和最长持仓合规指标

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

高 Sharpe 回测需要额外解析 `rebalance_diagnostics` 和 `trades` 才能判断是否满足最少持仓数、最大单票权重和最长持仓天数等约束。Human 验收和 agent 自动筛选缺少可直接使用的组合合规指标。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_backtest_diagnostics_contracts -v`
- **复验结论**：通过。`BacktestService` 在保存回测时写入 `summary.portfolio_compliance`，包含 `min_position_count`、`avg_position_count`、`max_target_weight`、`max_trade_holding_days`、`max_target_sum`、`avg_target_sum`、`thresholds`、`violations` 和 `compliance_pass`。回测结果卡片和历史列表展示合规状态，并可按“通过/违规”筛选。
