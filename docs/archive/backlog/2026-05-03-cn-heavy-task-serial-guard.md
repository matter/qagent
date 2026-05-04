# [2026-05-03] P2 稳定性：Heavy CN 并发任务会争用资源并导致任务全部 stale

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

CN 大特征模型训练与 CN 核心股票池回测并发运行时资源争用严重，backend 退出后所有 active 任务都被 stale cleanup 标记失败。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_task_executor_contracts.TaskExecutorContractTests.test_cn_model_and_backtest_share_heavy_serial_key -v`
- **复验结论**：通过。`TaskExecutor` 对 CN `strategy_backtest` 和 `model_train` 使用同一个 `"CN:heavy-research"` 串行锁，避免重型 CN 研究任务在进程内并发执行。超时任务的串行锁释放交给 watcher，避免后台仍运行时下一个 CN 重任务提前开始。该项修复的是 qagent 进程内资源隔离，硬件资源不足仍应通过任务暂停规则和窄范围验证控制。
