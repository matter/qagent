# [2026-05-09] P2 Signal diagnose backtest replay context filtering

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：US legacy signal diagnosis.
- **入口**：`POST /api/signals/diagnose` 携带 `backtest_id`。
- **现象**：回测 replay 状态会带上 `replay_positions_after`，随后通过 `**portfolio_state` 传给 `StrategyContext`，触发 `StrategyContext.__init__() got an unexpected keyword argument 'replay_positions_after'`。

## 修复记录

- 新增 `SignalService._strategy_context_portfolio_kwargs()`，只允许 `StrategyContext` dataclass 定义的字段进入 context 构造。
- `replay_positions_after` 继续保留在 `portfolio_state`，供 replay overlay 和诊断输出使用，但不再作为 `StrategyContext` 参数传入。

## 验证

- `uv run python -m unittest tests.test_signal_contracts.SignalServiceContractTests.test_replay_only_state_fields_are_filtered_before_strategy_context -v`
- `uv run python -m unittest tests.test_signal_contracts.SignalServiceContractTests.test_backtest_replay_overlay_returns_saved_positions_as_canonical_signals -v`

## 残余风险

- replay 仍依赖已保存 backtest 的 `rebalance_diagnostics.positions_after` 质量；如果历史结果没有该字段，会回退到交易日志重建路径。
