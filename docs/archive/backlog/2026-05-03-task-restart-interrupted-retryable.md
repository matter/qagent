# [2026-05-03] P2 任务可靠性：服务重启会把运行中的长任务标记失败且不提示可重跑

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

服务重启后，旧的 queued/running 长任务被标记 failed，但错误信息只说明 server restarted，任务页不能区分代码异常失败和服务中断，agent 也缺少可重跑提示。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_task_executor_contracts.TaskExecutorContractTests.test_stale_running_tasks_are_marked_retryable_interrupted -v`
- **复验结论**：通过。`TaskStore.mark_stale_running()` 会将 stale queued/running 任务标记为 failed，并写入 `result_summary.interrupted=true`、`retryable=true`、`reason=server_restarted`，错误信息包含 `retryable=true` 和重跑建议。REST 任务状态和任务管理页均展示“服务重启中断，可用相同参数重跑”。
