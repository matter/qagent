# [2026-05-03] P2 研究限制：非空目标权重会被回测引擎归一化为满仓

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

回测引擎旧默认会将非空目标权重归一化到满仓，离线风险覆盖实验不能在正式回测中验证“保留 50%/70% 仓位、剩余现金”的方案。

## 修复记录

- **commit**：`4e383b5 fix: improve backtest task reliability`；本次提交补 UI 开关
- **验证命令**：`uv run python -m unittest tests.test_backtest_engine_contracts tests.test_strategy_backtest_market_scope tests.test_paper_trading_contracts -v`；`cd frontend && pnpm build`
- **复验结论**：通过。`BacktestConfig.normalize_target_weights` 已支持关闭归一化并在 `trade_diagnostics.target_weight_policy` 记录现金权重。本次在策略回测面板增加“目标权重归一化”开关，human 可在 UI 中显式选择“满仓/保留现金”。
