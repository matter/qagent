# [2026-05-03] P3 体验：CN 回测起始日自动调整但提交响应不提示

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

CN 回测请求日期如果不是交易日，实际回测会从下一交易日开始，但提交响应和任务结果缺少 `requested_start_date -> effective_start_date` 映射。

## 修复记录

- **commit**：`4e383b5 fix: improve backtest task reliability`
- **验证命令**：`uv run python -m unittest tests.test_strategy_backtest_market_scope`；`cd frontend && pnpm build`
- **复验结论**：通过。回测保存配置和任务摘要包含 `requested_start_date`、`effective_start_date`、`requested_end_date`、`effective_end_date` 和 `date_adjustment`。任务列表展示请求起始日到实际起始日的映射。
