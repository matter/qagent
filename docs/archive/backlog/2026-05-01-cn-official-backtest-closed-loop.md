# [2026-05-01] P1 研究链路阻断：A 股研究结果无法沉淀为 qagent 官方回测历史

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：human 反馈 / agent live API 复验
- **影响范围**：A 股因子研究、模型研究、策略创建、`POST /api/strategies`、`/api/strategies/backtests`、回测历史展示
- **复现入口**：
  - UI：策略创建页 / 策略回测页选择 CN 市场后无法完成“创建策略 -> 运行正式回测 -> 回测历史可见”的研究交付链路
  - API / MCP：`POST /api/strategies` with `market="CN"`；后续预期使用 `cn_a_core_indices_union` 作为训练与回测股票池
  - 资产 ID：股票池 `cn_a_core_indices_union`，临时研究脚本 `/Users/m/dev/atlas/tmp/cn_core_regime_factor_experiment.py` 仅作诊断，不满足正式交付形态

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_schema_migrations tests.test_strategy_backtest_market_scope`；live API CN 策略创建 / 回测 / 删除 smoke
- **复验结论**：通过。新增策略表 market 唯一约束迁移，修复 legacy `UNIQUE(name, version)` 在 CN 查询/写入路径触发的内部错误；API 500 路径补充结构化日志和可读 detail。CN 极简策略可创建，`cn_a_core_indices_union` 官方回测可完成并写入 `backtest_results`，临时 smoke 资产已删除。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
