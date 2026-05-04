# [2026-05-03] P2 缺陷：running 任务取消语义不稳定且不可解释

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

任务列表显示 running/queued，但取消接口可能返回 not cancellable；即使取消成功，Python 已运行线程也无法被强杀，任务页没有说明后台计算可能继续。

## 修复记录

- **commit**：`4e383b5 fix: improve backtest task reliability`；本次提交补 `cancel_requested / compute_may_continue`
- **验证命令**：`uv run python -m unittest tests.test_task_executor_contracts.TaskExecutorContractTests.test_cancelled_running_task_warns_compute_may_continue tests.test_task_executor_contracts.TaskExecutorContractTests.test_cancelled_queued_task_does_not_warn_compute_may_continue -v`
- **复验结论**：通过。取消 running 任务时 REST 返回任务状态可见 `cancel_requested=true` 和 `compute_may_continue=true`，任务页提示“已请求取消，后台计算可能仍在收尾”。排队任务若在 worker 启动前取消成功，则 `compute_may_continue=false`。
