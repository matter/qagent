# ANCHOR_DAY 开局静默与 warm-up/evaluation split

- **原始问题**：stateful cohort 策略从主评估窗口第一天直接起跑时，策略源码中的锚点等待会导致开局空仓静默；简单把锚点改成第一天会放大 churn 并劣化路径。
- **修复日期**：2026-05-04
- **影响范围**：策略回测配置、回测统计切片、调仓诊断、回测 summary、前端回测配置与结果展示、agent 使用文档。

## 修复内容

- 回测配置新增：
  - `warmup_start_date`：从更早日期开始模拟，用历史窗口形成状态；
  - `evaluation_start_date`：只从该日期开始统计主窗口绩效；
  - `initial_entry_policy`：`wait_for_anchor`、`open_immediately`、`bootstrap_from_history`、`require_warmup_state`。
- 回测仍从 `warmup_start_date` 或原 `start_date` 运行完整 T+1/open-price 仿真，不修改策略源码中的锚点常量。
- 当配置 `evaluation_start_date` 时，系统会对 `BacktestResult` 做主窗口切片：
  - NAV 与 benchmark NAV 以评估起点重新归一到初始资金；
  - Sharpe、收益、回撤、月度收益等指标只用评估期数据重算；
  - warm-up 交易不计入评估期 `total_trades`、`win_rate`、`annual_turnover`、`total_cost`。
- 调仓诊断新增 `phase=warmup/evaluation`。
- summary 新增 `startup_state_report`，包括评估首个 rebalance 的前后持仓数、换手、空仓等待次数、锚点阻断次数和 `startup_silence_violation`。
- 前端回测配置页增加 warm-up 起点、统计起点和开局策略选择；回测摘要显示开局状态。

## 验证

- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_evaluation_slice_rebases_nav_and_excludes_warmup_trades`
- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_startup_state_report_flags_missing_warmup_state_for_evaluation`
- `uv run python -m unittest tests.test_schema_migrations tests.test_strategy_backtest_market_scope tests.test_signal_contracts tests.test_paper_trading_contracts`

## 复验结论

已修复。系统支持通过 warm-up 形成持仓状态，再从 `evaluation_start_date` 统计主窗口，不需要把策略源码中的 `ANCHOR_DAY` 改成 1。
