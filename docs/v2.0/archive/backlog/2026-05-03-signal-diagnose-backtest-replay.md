# [2026-05-03] P1 信号诊断 API 与回测路径同策略同日输出不一致

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

同一策略、同一日期调用 `/api/signals/diagnose` 与已保存回测的 `rebalance_diagnostics.positions_after` 不一致，导致 agent 不能用诊断 API 解释回测真实持仓。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_signal_contracts -v`
- **复验结论**：通过。`diagnose_signals(backtest_id=...)` 会读取已保存回测 summary 的 `rebalance_diagnostics.positions_after`，并将其作为 replay 模式下的权威 `signals` 返回；同时保留 `backtest_replay.generated_weights`、`ticker_match`、`missing_from_generated`、`extra_in_generated` 和 `max_weight_diff`，用于解释当前重新生成信号与已保存回测持仓的差异。
