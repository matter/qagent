# 策略/回测统一硬限制 constraint_config

- **原始问题**：策略创建、策略更新、回测、信号生成和模拟交易缺少统一可复用的硬限制配置，单票上限、周换手、调仓偏离缓冲和持仓期要求散落在策略源码或临时脚本中。
- **修复日期**：2026-05-04
- **影响范围**：`StrategyService`、策略 REST/MCP、`BacktestService`、`BacktestEngine`、`SignalService`、`PaperTradingService`、策略编辑 UI、回测配置与结果 UI、agent 使用文档。

## 修复内容

- `strategies` 表新增可空 `constraint_config` JSON 字段，并加入轻量迁移，旧策略为空时行为不变。
- 策略创建/更新支持保存默认 `constraint_config`，回测配置中的 `constraint_config` 可覆盖策略默认值。
- 回测执行支持：
  - `max_single_name_weight`：策略目标层裁剪，并在 T+1 执行目标层再次保护；
  - `rebalance_drift_buffer`：映射到执行层 rebalance buffer，默认使用 `actual_open` 权重参考；
  - `holding_period.min_days`：映射到执行层最小持仓；
  - `holding_period.max_days`：执行层强制超期退出；
  - `weekly_turnover_floor`：按评估期 ISO week 输出 pass/fail。
- 回测 summary 输出 `constraint_report`、`constraint_pass`、`failed_constraints`，调仓诊断记录 `constraint_actions`。
- 信号生成会按同一 position sizing 与单票上限输出裁剪后的 `target_weight`，并在 dependency snapshot 中记录约束报告。
- paper trading 创建 session 时合并策略默认和 session 配置，推进时复用同一单票上限、调仓缓冲和持仓期口径。
- 前端策略编辑页支持硬限制 JSON；回测配置页支持常用硬限制字段；回测摘要和调仓诊断显示硬限制状态。

## 验证

- `uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_constraint_config_caps_weights_and_reports_weekly_failures`
- `uv run python -m unittest tests.test_signal_contracts.SignalServiceContractTests.test_signal_constraints_apply_position_sizing_and_report_clipped_orders`
- `uv run python -m unittest tests.test_schema_migrations tests.test_strategy_backtest_market_scope tests.test_signal_contracts tests.test_paper_trading_contracts`

## 复验结论

已修复。旧调用不传 `constraint_config` 时保持兼容；新配置在策略、回测、信号和模拟交易链路中有统一服务层口径，并可在 UI / API summary 中验收。
