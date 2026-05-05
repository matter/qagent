# [2026-05-01] P1 可靠性：CN 并发模型训练会并发写同一因子缓存并触发主键冲突

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent live API 复验
- **影响范围**：`/api/models/train`、`FeatureService.compute_features`、`FactorEngine.compute_factor`、`factor_values_cache`、CN 多模型研究链路
- **复现入口**：
  - UI：模型训练页同时提交两个使用同一 CN feature set 的训练任务
  - API / MCP：`POST /api/models/train`，任务 `fff2674c0c6f45d9898ea684299f9910`；`POST /api/models/train`，任务 `a9014886d61d49b8a5d2e199d772eee5`
  - 资产 ID：feature set `05821b6c142f`，group `cn_a_core_indices_union`

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_factor_feature_market_scope`
- **复验结论**：通过。`FactorEngine` 对 `(market, factor_id)` 的缓存写入加进程内 keyed lock，缓存写入使用唯一临时 relation 名并通过 `conn.register()` 批量写入，避免并发任务互相覆盖固定 `_tmp_fv` 或抢写同一主键。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
