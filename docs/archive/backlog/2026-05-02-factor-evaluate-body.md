# [2026-05-02] P2 使用可靠性：中文 factor_id 作为 REST 路径参数时客户端编码失败

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-02

## 原始问题

- **来源**：agent A 股因子质量评估发现
- **影响范围**：`POST /api/factors/{factor_id}/evaluate`、中文内置统计因子、agent 自动化因子评估
- **复现入口**：`POST /api/factors/cn_builtin_统计_roc_std_60/evaluate`

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_factor_feature_market_scope.FactorFeatureMarketScopeTests.test_factor_api_forwards_market_scope`；`cd frontend && pnpm build`
- **复验结论**：通过。新增 `POST /api/factors/evaluate`，把 `factor_id` 放在 request body，避免 Python `urllib` 等客户端直接拼中文路径导致 ASCII 编码失败。旧路径接口保留兼容。前端因子评价改用 body 版接口，中文统计因子可从 UI 正常触发评价任务。
