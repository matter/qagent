# [2026-05-01] P2 缺陷：REST 创建 CN 策略返回 500 且无可读错误

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent 研究发现
- **影响范围**：A 股策略研究、`POST /api/strategies`、`StrategyService.create_strategy`、前端策略创建页、后续回测链路
- **复现入口**：
  - API：`POST /api/strategies`
  - 成功对照：相同极简策略使用 `market="US"` 返回 HTTP 200 并生成策略 `56634ff0fe0d`
  - 失败请求：相同极简策略使用 `market="CN"`，或 `CN_CORE_MOM_RANK_BASELINE_V1` 策略使用 `market="CN"`

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_schema_migrations tests.test_strategy_backtest_market_scope`；live API CN 策略创建 smoke
- **复验结论**：通过。根因是 legacy 策略表约束没有 market 维度；迁移会重建为 `UNIQUE(market, name, version)`，并保留行数校验。CN 策略创建失败时 API 会写入 `api.strategy.create_failed` 结构化日志并返回可读 detail。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
