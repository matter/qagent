# [2026-05-13] Backlog operability fixes

- **归档状态**：Done / Mitigated
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-13

## 已修复问题

### Backtest request `max_single_name_weight` ignored

- **原问题**：当 strategy `constraint_config` 没有 `max_single_name_weight` 时，backtest request 顶层 `config.max_single_name_weight=0.2` 没有进入 `constraint_report`，用户会误以为单票 20% 约束已执行。
- **修复**：
  - 新增 `BacktestService._resolve_run_constraint_config()`，统一合并 strategy 约束、嵌套 `config.constraint_config` 和兼容顶层 run config 约束。
  - `BacktestConfig.max_single_name_weight`、position sizing 上限、constraint report 都使用同一份约束配置。
- **验证**：
  - `tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_top_level_backtest_constraint_config_is_preserved`

### Unsupported `position_sizing` silently changes semantics

- **原问题**：strategy 可以保存 `position_sizing="custom"`，legacy backtest 遇到未知值会按等权兜底，导致研究对比不可归因。
- **修复**：
  - `StrategyService` 增加支持集合：`equal_weight`、`signal_weight`、`max_position`、`raw_weight`。
  - create/update strategy 时拒绝非法值。
  - `BacktestService._apply_position_sizing()` 遇到非法值直接抛错，不再等权兜底。
- **验证**：
  - `tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_create_strategy_rejects_unsupported_position_sizing`
  - `tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_apply_position_sizing_rejects_unsupported_mode`

### Legacy predict-batch small diagnostic requests can time out

- **原问题**：小批量 `POST /api/models/{model_id}/predict-batch` 仍可能被 feature 加载拖慢，agent 调试模型语义时容易卡在同步 HTTP 超时。
- **修复**：
  - `ModelService.predict_batch()` 改为多日期一次构建矩阵、一次模型预测。
  - 增加 48 小时进程内短 TTL 结果缓存，同模型、feature set、ticker/date 集合重复请求直接复用结果。
  - API 增加 `async_mode=true`，返回 `model_predict_batch` task id，长诊断可走 `/api/tasks/{task_id}` 轮询。
  - 同步返回增加 `runtime_seconds`。
- **验证**：
  - `tests.test_model_market_scope.ModelMarketScopeTests.test_predict_batch_runs_model_once_for_all_dates`
  - `tests.test_model_market_scope.ModelMarketScopeTests.test_predict_batch_reuses_short_lived_result_cache`
  - `tests.test_model_market_scope.ModelMarketScopeTests.test_model_predict_batch_api_can_submit_async_task`

### Legacy model training lacks task progress during expensive feature phase

- **原问题**：模型训练长时间停在 feature cache loading 时，`/api/tasks/{task_id}` 没有阶段信息，agent 容易误判为后端卡死。
- **修复**：
  - `TaskExecutor` 支持向显式接受 `progress` 的任务函数注入进度回调。
  - running task 的 `result_summary.progress` 和 `progress_history` 会记录阶段、时间、percent 和说明，并在最终 completed summary 中保留。
  - `ModelService.train_model()` 上报 `resolve_universe`、`feature_load`、`label_load`、`build_xy`、`split`、`fit_prep`、`fit`、`predict_eval`、`metrics`、`persist`、`completed`。
- **验证**：
  - `tests.test_task_executor_contracts.TaskExecutorContractTests.test_task_progress_updates_running_result_summary`
  - `tests.test_model_market_scope.ModelMarketScopeTests.test_train_model_reports_coarse_progress_phases`

## 已缓解问题

### Filtered full debug replay overhead

- **缓解**：
  - debug replay state 现在记录 `captured_items`、`skipped_items` 和 `skipped_by_date`。
  - manifest summary 输出捕获/跳过计数，agent 可以确认 `debug_dates` 过滤是否先于重型 snapshot 写入生效。
- **剩余风险**：尚未做真实 15 ticker x 30 date 性能基准，若仍超出可接受 overhead，需要继续做异步 bundle 写入或更细粒度 factor snapshot 压缩。
- **验证**：
  - `tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_filtered_full_debug_replay_reports_skipped_counts`

### Parallel legacy backtest contention

- **缓解**：
  - `TaskExecutor._serial_key()` 对 legacy `strategy_backtest` 增加市场级串行键，例如 `US:legacy-backtest`。
  - 这会把同市场 legacy backtest 从隐式资源争抢变成可预测排队；CN 仍走已有 `CN:heavy-research` 键。
- **剩余风险**：这不是吞吐优化，而是稳定性优先的调度策略。若需要批量实验吞吐，应另建 batch runner 或多进程/多库隔离方案。
- **验证**：
  - `tests.test_task_executor_contracts.TaskExecutorContractTests.test_cn_model_and_backtest_share_heavy_serial_key`

## 继续保留在 backlog 的原因

- `Domain writes are not transactionally staged by task acceptance`：仍需逐个迁移 write-heavy workflow 到 stage/promote，工作量高。
- `Main DuckDB has single-writer operational fragility`：这是单机 DuckDB 架构边界，不应在当前需求中替换数据库。
- `Free equity data is not PIT or survivorship-safe`：需要更高质量数据源或 curated dated facts。
- `Legacy strategy context portfolio state can drift from engine holdings`：需要重构 legacy backtest 为服务/engine 同一逐日状态机。
- `Legacy and 3.0 engines coexist`：属于迁移项目，不能在 backlog 小修中安全删除 legacy surface。
