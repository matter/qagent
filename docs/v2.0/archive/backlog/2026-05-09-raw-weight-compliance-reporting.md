# [2026-05-09] P2 Raw-weight compliance and target-budget reporting

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **问题 1**：`position_sizing="raw_weight"` 且只配置 `max_single_name_weight` 时，`portfolio_compliance` 会因为默认 21 天持仓阈值失败，即使用户没有配置持仓期硬约束。
- **问题 2**：raw weights 在单票上限裁剪前总和超过 100% 时，`constraint_report.constraint_pass` 仍可能为 true，容易误读为可推广组合。

## 修复记录

- `portfolio_compliance` 将未配置的默认持仓期检查降级为 `heuristic_violations`。只有显式配置 `compliance_max_holding_days` 或 `constraint_config.holding_period.max_days` 时，才将持仓天数超限计入硬失败。
- `_apply_weight_constraints()` 记录 `raw_target_sum`、`constrained_target_sum` 和 `target_sum_limit`。
- `_build_constraint_report()` 增加 `target_weight_budget` 报告。raw 或裁剪后目标权重总和超过 100% 时，`failed_constraints` 包含 `target_weight_budget`，`constraint_pass=false`。

## 验证

- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_portfolio_compliance_metrics_does_not_fail_long_holds_without_hard_constraint -v`
- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_portfolio_compliance_metrics_enforces_configured_holding_limit -v`
- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_constraint_report_fails_raw_target_budget_before_single_name_cap -v`

## 残余风险

- `portfolio_compliance` 仍保留默认分散度、单票权重、目标总和阈值，用作研究质量门槛；它不是交易撮合层约束。
