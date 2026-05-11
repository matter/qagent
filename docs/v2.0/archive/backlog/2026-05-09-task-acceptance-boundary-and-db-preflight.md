# [2026-05-09] Task acceptance boundary and DuckDB operation guardrails

- **归档状态**：Mitigated
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：US, CN long-running tasks and local DuckDB operations.
- **入口**：`TaskExecutor`, `DbPreflightService`
- **现象**：任务超时/取消后，返回 payload 可被隔离，但服务内部若已写入 domain rows，仍可能留下部分结果；DuckDB 主库被占用时直接连接会抛原始 lock 错误。

## 修复记录

- `TaskExecutor.submit()` 支持 `on_accept(result, task_record)`。
- 只有任务仍处于 RUNNING 的接受边界才执行 accept callback。
- 超时/取消后的 late result 不执行 accept callback，并继续进入 quarantine。
- DB preflight 对 `locked` / `in_use` 返回可操作 payload，包含可走运行中 API 的诊断 routes 和需要维护窗口的操作列表。

## 验证

- `uv run python -m unittest tests.test_task_executor_contracts.TaskExecutorContractTests.test_commit_callback_runs_only_for_accepted_completed_task tests.test_task_executor_contracts.TaskExecutorContractTests.test_commit_callback_is_not_run_for_late_timed_out_result tests.test_db_preflight.DbPreflightServiceContractTests -v`

## 残余风险

- 这是基础能力和操作防护，不等于全域事务化写入迁移完成。`docs/backlog.md` 继续保留 domain writes staging 的 P0 剩余项。
- DuckDB 单写者架构未改变；`docs/backlog.md` 继续保留主库单写者运维脆弱性。
