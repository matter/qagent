# QAgent 需求与问题看板

本文件是 QAgent 项目的统一需求看板。Agent 在开发、研究、验收过程中发现的未闭环问题、改进需求和验收缺口都记录在这里。Human 通过 UI 和量化指标验收，agent 通过本文件维持跨会话上下文。

## 使用规则

- 新问题先放到 `Inbox`，复现清楚后移动到 `Open`。
- 开始修复前移动到 `In Progress`，写明负责人、分支或会话。
- 修复完成但未验收放到 `Verify`。
- 验收通过后移动到 `Done`，保留复验证据和 commit。
- 暂不处理但仍有价值的问题放到 `Deferred`，写明重新评估条件。
- 不记录纯猜测。没有复现步骤的问题必须标记为 `Needs Repro`。
- V2.0 期间凡是发现 REST、MCP、UI 对 `market`、ranking/listwise 指标、任务状态或资产 ID 的展示不一致，统一记录到本文件。
- agent 记录问题时必须写清楚 market、入口、资产 ID、请求参数和可复验命令；human UI 验收问题还要补充页面路径和截图/指标位置。

## 记录模板

```md
### [YYYY-MM-DD] P0/P1/P2/P3 类型：一句话标题

- **状态**：Inbox / Open / In Progress / Verify / Done / Deferred / Needs Repro
- **来源**：human 反馈 / agent 发现 / live API 复验 / UI 验收
- **影响范围**：页面、API、服务、数据资产或研究链路
- **复现入口**：
  - UI：
  - API / MCP：
  - 资产 ID：
- **当前证据**：
  - 实际结果：
  - 日志 / 错误：
  - 相关指标：
- **期望行为**：
- **验收标准**：
  - 可量化指标：
  - UI 验收点：
  - 命令 / API 复验：
- **修复记录**：
  - commit：
  - 验证命令：
  - 复验结论：
```

## Inbox

暂无。

## Open

暂无。

## In Progress

暂无。

## Verify

暂无。

## Deferred

暂无。

## Done

### [2026-04-30] P3 UI：Ant Design 废弃属性警告

- **状态**：Done
- **来源**：UI 验收
- **影响范围**：`frontend/src/pages/MarketPage.tsx` 及复用旧 AntD 写法的前端页面/组件
- **复现入口**：
  - UI：`http://127.0.0.1:5173/market`、`/data`、`/models`
  - API / MCP：无
  - 资产 ID：无
- **当前证据**：
  - 实际结果：Task 14 页面可正常渲染，Playwright console 捕获到 `0` 条 Ant Design warning。
  - 日志 / 错误：已移除 `bodyStyle`、`Spin tip`、`Space direction`、`Modal destroyOnClose`、`Statistic valueStyle`、`Input addonBefore` 废弃用法。
  - 相关指标：`rg -n "valueStyle|addonBefore|direction=|destroyOnClose|bodyStyle|tip=" frontend/src` 无命中。
- **期望行为**：页面不使用 AntD 已废弃 props，dev console 保持低噪音。
- **验收标准**：
  - 可量化指标：打开 `/market`、切换 CN、进入 `/data` 和 `/models` 不再出现 AntD warning。
  - UI 验收点：行情页加载、图表 loading、卡片样式、数据概览、模型训练目标选择和模型指标展示保持一致。
  - 命令 / API 复验：`cd frontend && pnpm build`；Playwright 浏览器 smoke。
- **修复记录**：
  - commit：待提交于 Task 14
  - 验证命令：`cd frontend && pnpm build`；Playwright console capture。
  - 复验结论：通过，Task 14 提交后闭环。
