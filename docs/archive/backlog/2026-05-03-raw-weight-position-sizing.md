# [2026-05-03] P2 研究限制：普通策略回测路径会在 position sizing 层满仓化

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

策略源码输出的低于满仓的权重会先被 `BacktestService._apply_position_sizing()` 标准化，导致动态现金、风险预算、vol target、cooldown 等策略无法通过普通回测路径正式验证。

## 修复记录

- **commit**：本次提交
- **验证命令**：`uv run python -m unittest tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_raw_weight_position_sizing_preserves_strategy_cash_budget tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_raw_weight_backtest_config_disables_target_normalization_by_default tests.test_paper_trading_contracts.PaperTradingServiceContractTests.test_paper_raw_weight_position_sizing_preserves_cash_budget -v`
- **复验结论**：通过。新增 `position_sizing="raw_weight"`，普通回测路径和 paper trading 均保留策略输出权重；未显式传 `normalize_target_weights` 时自动设为 `false`，未分配权重作为现金保留。策略编辑、策略列表、MCP 文档和 agent 文档均补充该模式。
