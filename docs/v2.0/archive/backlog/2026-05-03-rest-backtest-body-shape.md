# [2026-05-03] P2 API 易用性：REST backtest body shape 容易误用

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

调用 `POST /api/strategies/{strategy_id}/backtest` 时将 backtest 字段平铺在 body 顶层，接口曾接受请求但 `params.config` 为空，造成长任务使用默认配置运行。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_strategy_api_rejects_flattened_backtest_config_fields -v`
- **复验结论**：通过。`RunBacktestRequest` 使用 `ConfigDict(extra="forbid")`，未知顶层字段直接返回 422，避免错误配置静默进入任务队列。正确 body 仍是 `{"market":"CN","universe_group_id":"...","config":{...}}`。
