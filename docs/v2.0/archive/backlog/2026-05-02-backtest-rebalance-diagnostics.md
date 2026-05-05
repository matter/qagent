# [2026-05-02] P2 可诊断性：新生成的策略回测详情缺少 rebalance_diagnostics

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent A 股多召回精排策略研究发现
- **影响范围**：`GET /api/strategies/backtests/{backtest_id}`、策略诊断、回测历史复盘
- **复现入口**：
  - 策略：`43302b490f0f` / `CN0502_DEV_MULTIRECALL_RERANK_PRE2025_V1`
  - 回测：`5b5485324baa`、`3e3132ac8996`、`cbd81d780c6d`

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_backtest_diagnostics_contracts`；`cd frontend && pnpm build`
- **复验结论**：通过。`BacktestService.get_backtest()` 会把 summary 内的 `rebalance_diagnostics` 提升到详情顶层，并返回 `rebalance_diagnostics_count`。新增分页接口 `GET /api/strategies/backtests/{backtest_id}/rebalance-diagnostics`。回测历史详情和新回测结果 UI 增加“调仓诊断”表，展示 `date`、`market_state`、`lane_counts`、涨停阻断、跌停保留等关键字段。
