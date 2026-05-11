# [2026-05-08] P2 Legacy rebalance diagnostics post-buffer execution layer

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-08

## 原始问题

- **范围**：legacy `BacktestService`、`BacktestEngine`、`GET /api/strategies/backtests/{backtest_id}/rebalance-diagnostics`
- **现象**：提高 `rebalance_buffer` 后 trade count 和 cost 已明显变化，但 paginated rebalance diagnostics 仍展示相同 position-count/turnover 聚合，容易误导 agent 分析。
- **根因**：服务层 diagnostics 记录的是策略目标/约束后目标层；真正的 buffer/min-hold/cooldown effective target 在 `BacktestEngine` 内部生成，没有回流到 stored diagnostics。

## 修复记录

- `BacktestEngine` 在每个执行调仓日写出 `rebalance_execution_diagnostics`：
  - `positions_before`
  - `target_positions_after`
  - `executed_positions_after`
  - `positions_after`，兼容旧字段，语义为 post-buffer executed
  - `turnover`，语义为 post-buffer executed turnover
  - `target_turnover`
  - `diagnostic_layers`
- `BacktestService` 按日期合并 engine execution diagnostics，保存和分页接口使用 post-buffer 执行层。
- `_build_rebalance_diagnostics()` 保留旧字段，同时新增 target/executed 分层字段。

## 验证

- `uv run python -m unittest tests.test_backtest_engine_contracts.BacktestEngineContractTests.test_hold_overlap_buffer_skips_small_add_reduce_without_renormalizing -v`
- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_rebalance_diagnostics_merge_engine_post_buffer_execution_layer tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_rebalance_diagnostics_distinguish_target_and_executed_weights -v`
- `uv run python -m unittest tests.test_agent_research_3_service tests.test_research_cache_service tests.test_backtest_engine_contracts tests.test_backtest_diagnostics_contracts -v`

## 残余风险

- 历史已保存 backtest 不会自动回填新的 execution diagnostics；新跑的 backtest 会持久化新字段。
