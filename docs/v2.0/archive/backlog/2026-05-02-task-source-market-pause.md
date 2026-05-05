# [2026-05-02] P2 缺陷：旧研究脚本持续提交任务污染研究队列

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent A 股策略回测执行发现
- **影响范围**：任务队列、`/api/tasks`、A 股回测执行稳定性、长任务研究效率
- **复现入口**：
  - 进程：`/Users/m/dev/atlas/tmp/research_0502_pathq_model_probe.py`
  - 父进程：`/Applications/Codex.app/Contents/Resources/codex app-server`
  - 污染任务示例：US `strategy_backtest`，`universe_group_id="sp500"`

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_task_executor_contracts`
- **复验结论**：通过。任务列表支持按 `source`、`market`、`task_type` 过滤；新增批量取消接口和 UI 操作；新增持久化 `task_pause_rules`，可按任务类型、来源、market 暂停未来匹配任务提交。命中暂停规则时 `TaskExecutor` 在入库前拒绝任务，FastAPI 统一返回 HTTP 409。旧任务缺少 `params.market` 时按 US 兼容处理。

## 说明

qagent 不能控制 Codex app-server 是否重新拉起外部脚本进程；本修复在 qagent 边界内阻断匹配任务继续入队。若外部脚本绕过 qagent API 或用错误 source 标识提交，需要在任务管理页按实际 `source/market/type` 建立暂停规则。
