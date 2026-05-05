# [2026-05-01] P2 功能缺失：CN 缺少核心指数成分并集股票池

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：human 反馈
- **影响范围**：A 股研究股票池构建、`/api/groups`、MCP group tools、`GroupService`、`DataService`、后续模型训练与回测链路
- **复现入口**：
  - UI：数据管理页 / 股票分组区域点击“刷新指数成分”
  - API / MCP：`GET /api/groups?market=CN`；`POST /api/groups/refresh-indices?market=CN`；MCP `refresh_index_groups(market="CN")`
  - 资产 ID：`cn_sz50`、`cn_hs300`、`cn_zz500`、`cn_chinext`、`cn_a_core_indices_union`

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_data_group_market_scope`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`；`GET /api/groups/{group_id}?market=CN`
- **复验结论**：通过；新增真实成分种子 `backend/seeds/cn_core_indices_constituents.json`，`ensure_builtins("CN")` 首次创建时会填充核心指数与并集，`refresh_index_groups("CN")` 在外部源为空且本地无成员时会用种子兜底。后端全量 `96` 个 unittest 通过，前端构建通过，diff whitespace 检查通过；当前运行库 API 返回 `cn_sz50=50`、`cn_hs300=300`、`cn_zz500=500`、`cn_chinext=100`、`cn_a_core_indices_union=806`。BaoStock provider 下载性能/可靠性逻辑未改动，`backend/providers/baostock_provider.py` 无差异。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
