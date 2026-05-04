# [2026-05-03] P1 回测可靠性：持仓当日缺失行情时 NAV 将持仓按 0 估值

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-03

## 原始问题

US 回测窗口末端存在持仓 ticker 当日缺 daily bar，旧估值循环跳过该持仓，相当于按 0 估值，导致 NAV 异常断崖。

## 修复记录

- **commit**：`4e383b5 fix: improve backtest task reliability`
- **验证命令**：`uv run python -m unittest tests.test_backtest_engine_contracts`；`uv run python -m unittest discover tests`；`cd frontend && pnpm build`；`git diff --check`
- **复验结论**：通过。`BacktestEngine` 对单只持仓缺少当日 close 的场景使用该 ticker 最近可用 close carry-forward 估值，并在 `trade_diagnostics.missing_price_valuations` 记录日期、ticker、持仓数、carry-forward 价格和估值方式，不再静默按 0 估值。
