# [2026-05-03] P2 使用可靠性：agent shell 中直接运行 scripts/start.sh 会阻塞并随会话退出

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

`scripts/start.sh` 是前台开发脚本，在一次性 agent shell 中执行会进入 `wait`，工具会话结束时 backend/frontend 也可能被终止，后续 REST 调用失败。

## 修复记录

- **commit**：本次提交
- **验证命令**：`bash -n scripts/start_detached.sh scripts/status.sh`
- **复验结论**：通过。新增 `scripts/start_detached.sh` 和 `scripts/status.sh`。前者用 `nohup` 分离 backend/frontend 并写入 PID/log；后者显示 backend/frontend 进程状态并执行 `/api/health`。`docs/agent-guide.md` 明确 agent 后台研究服务应使用 detached 脚本，停止仍使用 `scripts/stop.sh`。
