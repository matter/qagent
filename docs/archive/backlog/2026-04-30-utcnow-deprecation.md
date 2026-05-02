# [2026-04-30] P3 技术债：Python 3.14 `datetime.utcnow()` 废弃警告

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent 发现
- **影响范围**：backend services、tasks、部分测试 fixture
- **复现入口**：
  - UI：无
  - API / MCP：无直接入口
  - 资产 ID：无

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_time_utils tests.test_data_group_market_scope`
- **复验结论**：通过。新增 `backend/time_utils.py`，统一提供 `utc_now_naive()` 和 `utc_now_iso()`；服务层和 task 时间戳写入保持 DuckDB 既有 naive UTC 存储契约，后端源码不再直接调用废弃的 `datetime.utcnow()`。新增测试防止 backend 重新引入 `.utcnow(`。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
