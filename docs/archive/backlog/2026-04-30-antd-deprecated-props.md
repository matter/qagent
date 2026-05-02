# [2026-04-30] P3 UI：Ant Design 废弃属性警告

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：UI 验收
- **影响范围**：`frontend/src/pages/MarketPage.tsx` 及复用旧 AntD 写法的前端页面/组件
- **复现入口**：
  - UI：`http://127.0.0.1:5173/market`、`/data`、`/models`
  - API / MCP：无
  - 资产 ID：无

## 修复记录

- **commit**：待提交于 Task 14
- **验证命令**：`cd frontend && pnpm build`；Playwright console capture
- **复验结论**：通过，Task 14 提交后闭环。Task 14 页面可正常渲染，Playwright console 捕获到 `0` 条 Ant Design warning。已移除 `bodyStyle`、`Spin tip`、`Space direction`、`Modal destroyOnClose`、`Statistic valueStyle`、`Input addonBefore` 废弃用法。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
