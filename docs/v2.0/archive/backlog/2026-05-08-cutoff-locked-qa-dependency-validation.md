# [2026-05-08] P1 Cutoff-locked QA dependency validation

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-08

## 原始问题

- **范围**：US cutoff replay、agent research QA、legacy strategy backtest reproducibility
- **现象**：回测数据区间可以限制在 cutoff 前，但 QA 无法证明依赖策略、因子、模型、feature set、label 没有在 cutoff 后创建或修改。
- **风险**：研究过程可能在看过 cutoff 后表现后才调参，即使代码层 PIT 检查没有读取未来 bar，也会污染 holdout simulation。

## 修复记录

- promotion-like QA 的 evidence gate 增加 `cutoff_validation` 校验。
- `mode=strict|locked|cutoff_locked` 时要求有效 `cutoff_date` 和依赖资产时间戳。
- 检查依赖项的 `created_at`、`updated_at`、`frozen_at`/`snapshot_at`：
  - cutoff 后创建或更新且无 override：`cutoff_locked_dependency` fail，阻断 QA。
  - 有 reviewer override 和 reason：降为 warning，保留 retrospective 分类证据。
  - strict 模式缺少依赖时间戳：阻断 QA。
- 支持从 `cutoff_validation.dependencies` 或 `dependency_snapshot.dependencies` 读取依赖列表。

## 验证

- `uv run python -m unittest tests.test_agent_research_3_service.AgentResearch3ServiceContractTests.test_cutoff_locked_qa_rejects_post_cutoff_dependencies_without_override tests.test_agent_research_3_service.AgentResearch3ServiceContractTests.test_cutoff_locked_qa_warns_when_post_cutoff_dependencies_have_override -v`
- `uv run python -m unittest tests.test_agent_research_3_service tests.test_research_cache_service tests.test_backtest_engine_contracts tests.test_backtest_diagnostics_contracts -v`

## 残余风险

- 当前校验依赖 QA evidence 中提交的 dependency snapshot；自动从所有 legacy asset 表反查依赖时间戳仍需后续把 snapshot 生成链路进一步结构化。
