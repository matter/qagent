# [2026-05-08] P2 Agent research empty-plan trial index failure

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-08

## 原始问题

- **范围**：US agent research trial recording
- **入口**：`POST /api/research/agent/plans/{plan_id}/trials/batch`
- **现象**：空 QRP2 plan 在记录第一批 trial 时触发 DuckDB vector error。
- **根因**：`_next_trial_index()` 使用 `SELECT COALESCE(MAX(trial_index), 0) + 1 ...` 聚合路径，在特定空表/plan 状态下触发 DuckDB 内部异常。

## 修复记录

- `_next_trial_index()` 改为按 `trial_index DESC LIMIT 1` 查询最后一条 trial。
- 空 plan 返回 `trial_index=1`；非空 plan 继续单调递增。
- 新增回归测试确保代码不再依赖 `MAX()` / `COALESCE` 聚合路径。

## 验证

- `uv run python -m unittest tests.test_agent_research_3_service.AgentResearch3ServiceContractTests.test_next_trial_index_avoids_empty_plan_aggregate_path tests.test_agent_research_3_service.AgentResearch3ServiceContractTests.test_batch_trial_recording_dedupes_and_returns_plan_performance -v`
- `uv run python -m unittest tests.test_agent_research_3_service tests.test_research_cache_service tests.test_backtest_engine_contracts tests.test_backtest_diagnostics_contracts -v`

## 残余风险

- 本修复只覆盖 trial index 生成路径；DuckDB 单写者架构风险仍由 P0 backlog 项单独跟踪。
