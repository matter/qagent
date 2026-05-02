# [2026-05-01] P2 缺陷：CN listwise 排序训练使用 rank 标签触发 LightGBM label mapping 错误

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent 研究发现
- **影响范围**：A 股排序模型训练、`/api/models/train`、`ModelService.train_model`、LightGBM rank/listwise 目标、后续模型驱动策略研究
- **复现入口**：
  - API：`POST /api/models/train`
  - 参数：`market="CN"`、`feature_set_id="05821b6c142f"`、`label_id="cn_preset_fwd_rank_20d"`、`universe_group_id="cn_a_core_indices_union"`、`objective_type="listwise"`、`ranking_config={"query_group":"date","eval_at":[5,10,20],"min_group_size":20}`
  - 任务 ID：`b174e4c04e9444e0b03acc77760e69b7`

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_ranking_dataset tests.test_model_market_scope`
- **复验结论**：通过。ranking/listwise 默认 `label_gain="ordinal"`，rank 原始标签进入 LightGBM 前会按同日分组转换为 dense non-negative relevance label；`identity` 仅接受已经 dense 的非负整数标签。模型 metadata 记录 `ranking_config.label_gain`，避免 rank 值如 `170` 直接触发 LightGBM label mapping 错误。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
