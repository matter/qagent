# [2026-05-03] P3 稳定性：API 健康检查短时 30 秒超时但重试恢复

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

本地研究时观察到 `/api/health` 短时 30 秒超时，稍后重试恢复。没有持久不可用证据，符合 backend reload、本机资源占用或长任务争用下的瞬时可用性问题。

## 处理记录

- **commit**：本次提交
- **验证命令**：`cd frontend && pnpm build`
- **复验结论**：该项按运行体验问题处理，不作为服务层缺陷继续保留。新增 `scripts/start_detached.sh`、`scripts/status.sh`，并在 `docs/agent-guide.md` 明确 health check 短时超时按 30 秒退避重试；连续失败时检查 `logs/backend-detached.log`、`logs/qagent.log` 和 `scripts/status.sh`。后续若出现稳定可复现的 health 阻塞，需要以新问题记录具体长任务、日志和响应时间。
