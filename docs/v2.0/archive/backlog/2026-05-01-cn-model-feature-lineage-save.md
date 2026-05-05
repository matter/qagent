# [2026-05-01] P2 缺陷：CN 模型训练完成后因 feature_id / factor_name 校验错配导致无法落库

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent 研究发现
- **影响范围**：A 股模型训练、`FeatureService.compute_features`、`ModelService.train_model`、模型资产保存、模型驱动策略回测
- **复现入口**：
  - API：`POST /api/models/train`
  - 参数：`market="CN"`、`feature_set_id="05821b6c142f"`、`label_id="cn_preset_path_return_20d"`、`universe_group_id="cn_a_core_indices_union"`、`model_type="lightgbm"`
  - 任务 ID：`a9014886d61d49b8a5d2e199d772eee5`

## 修复记录

- **commit**：原 backlog 记录为“本次提交”
- **验证命令**：`uv run python -m unittest tests.test_model_market_scope`
- **复验结论**：通过。模型落库前校验改为基于训练列的 `factor_name` 命名空间，同时 metadata 新增 `feature_lineage` 记录 `factor_id`、`factor_name`、训练列、缺失声明因子和未声明训练列。未声明训练列仍会阻断保存；声明但训练窗口无有效覆盖的因子会进入 metrics/metadata，而不再误判全部 factor_id 缺失。

## 本轮归档复验

- **复验时间**：2026-05-02
- **复验命令**：`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：后端全量 unittest `118` 个通过；前端构建通过，仅保留既有动态导入和 chunk size warning；diff whitespace 检查通过。
