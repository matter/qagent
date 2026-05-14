# [2026-05-12] Model/backtest hardening fixes

- **归档状态**：Done / Partially mitigated
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-12

## 已修复问题

### Composite label effective horizon

- **原问题**：composite label 只暴露外层 `horizon`，模型元数据和回测泄漏审计会低估内层 forward label 需要看到的未来数据。
- **修复**：
  - `LabelService.resolve_effective_horizon()` 递归解析 composite 组件最大 horizon。
  - `get_label()` / `list_labels()` 返回 `effective_horizon`。
  - `ModelService.train_model()` 将 `effective_horizon` 写入 `label_summary`，并把 `label_horizon` / `effective_label_horizon` 设为有效 horizon。
  - `BacktestService` 泄漏审计从模型 record 和 metadata 中取所有 horizon 候选的最大值。
- **验证**：
  - `tests.test_label_market_scope.LabelMarketScopeTests.test_composite_label_reports_recursive_effective_horizon`
  - `tests.test_model_market_scope.ModelMarketScopeTests.test_train_model_persists_effective_composite_label_horizon`
  - `tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_leakage_audit_prefers_effective_label_horizon`

### Legacy batch backtest prediction frozen schema

- **原问题**：legacy `_batch_predict_all_dates()` 直接用当前 feature set 矩阵预测，不对齐模型训练时冻结的 `feature_names`。
- **修复**：每个模型预测前调用 `_load_frozen_features()` / `_align_features_to_frozen()`，允许当前 feature set 多列但会过滤和重排；缺少 frozen 列时提前抛出清晰错误。
- **验证**：
  - `tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_batch_predict_aligns_features_to_frozen_model_schema`
  - `tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_batch_predict_missing_frozen_feature_raises_clear_error`

### Planned-price sell/reduce coverage

- **原问题**：planned-price 矩阵只给新 target 中的股票写计划价，被移除持仓的卖单可能因缺少 planned price 被阻断。
- **修复**：`_write_planned_prices_for_date()` 现在为 `selected_weights ∪ current_weights` 写入计划价；没有策略输出计划价时回退 decision close，并保留诊断样本。
- **验证**：
  - `tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_planned_price_matrix_includes_positions_removed_from_target`
  - 既有 planned-price engine tests 通过。

### Concurrent isolated strategy metadata load

- **原问题**：同一大策略源码的多个并发 backtest 会并发启动 isolated metadata 进程，容易在 3 秒加载超时上抖动失败。
- **修复**：`backend.strategies.loader.load_strategy_from_code()` 增加源码 SHA-256 级 metadata cache 和 per-source lock；安全校验仍每次执行，生成信号仍在 isolated process 中运行。
- **验证**：
  - `tests.test_custom_code_safety.CustomCodeSafetyContractTests.test_strategy_loader_serializes_and_caches_same_source_metadata`
  - `tests.test_custom_code_safety`

### V2 regression check default CN group drift

- **原问题**：`scripts/v2_regression_check.py` 仍把 CN 默认股票池期望为旧的 `cn_all_a`，与当前产品配置 `cn_a_core_indices_union` 不一致，导致隔离回归验证误报。
- **修复**：回归脚本期望值更新为 `cn_a_core_indices_union`，并补充脚本级测试锁定当前默认组。
- **验证**：
  - `tests.test_v2_regression_check.V2RegressionCheckTests.test_market_context_defaults_expect_current_cn_core_union`
  - 临时配置、临时 DuckDB、临时后端端口执行 `scripts/v2_regression_check.py --skip-us-e2e`，结果 `passed=3, failed=0, skipped=3`。

## 已有修复确认

### Rebalance diagnostics layer naming

`2026-05-08-legacy-rebalance-diagnostics-post-buffer.md` 已归档过诊断层修复。本轮复核确认新 backtest 的 diagnostics 已区分：

- `target_positions_after`
- `executed_positions_after`
- `positions_after`，兼容旧字段，语义为 post-buffer executed
- `diagnostic_layers`

## 未完全解决并保留

### Strategy context state drift under engine buffer

legacy `BacktestService` 仍是先生成全窗口信号，再交给 `BacktestEngine` 执行。`StrategyContext.current_weights / holding_days / avg_entry_price / unrealized_pnl` 不能在同一个循环内读取引擎 post-buffer 实际持仓。完全修复需要把 engine 执行状态回流到服务日循环，或把 legacy 服务和 engine 合并为逐日执行运行时，工作量中高，已保留在 backlog。
