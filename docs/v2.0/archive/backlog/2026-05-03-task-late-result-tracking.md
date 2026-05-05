# [2026-05-03] P2 任务可靠性：取消或超时后后台 late result 不可追踪

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

并发回测或长任务取消后，任务状态显示 `failed / Cancelled by user` 且 `result=null`，但后台实际已保存 backtest 或模型资产。Agent 需要反查历史才能恢复结果，容易重复提交任务。

## 修复记录

- **commit**：`4e383b5 fix: improve backtest task reliability`；本次提交保留取消说明并继续合并 late result
- **验证命令**：`uv run python -m unittest tests.test_task_executor_contracts -v`
- **复验结论**：通过。取消后或超时后如果内部任务返回结果，`TaskExecutor` 在 `result.late_result`、REST/MCP `late_result_id` 和错误信息中暴露已保存资产；任务页显示 late result。取消请求的 `cancel_requested` 和后续 `late_result` 会合并保存，不互相覆盖。
