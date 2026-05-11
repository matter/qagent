# [2026-05-09] P2 Workbench 3.0 backtest and data-quality UI workflow

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：US, CN 3.0 Research Workbench.
- **入口**：`frontend/src/pages/ResearchWorkbench3.tsx`, `frontend/src/api/index.ts`
- **现象**：REST/MCP/frontend API client 已有 3.0 StrategyGraph 回测与 provider capability 能力，但 UI 没有可配置入口。

## 修复记录

- Workbench Assets 页新增 StrategyGraph 表格与回测 action。
- StrategyGraph drawer 支持配置 `start_date`、`end_date`、`initial_capital`、`price_field` 并提交回测任务。
- UI 显示 queued task id、历史回测列表、收益、Final NAV、成本、成交诊断、估值告警。
- Data Quality 页展示 `publication_gates`、publication grade、PIT capability 统计和 provider capability 表。
- 前端类型新增 `PublicationGate3` 与 `DataQualityContract3.publication_gates`。

## 验证

- `cd frontend && pnpm build`

## 残余风险

- 未新增独立审批流；该需求已按用户要求删除，不作为待修问题保留。
