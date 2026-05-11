# [2026-05-09] P2 Isolated strategy diagnostics propagation

- **归档状态**：Done
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-09

## 原始问题

- **范围**：US, CN legacy custom strategy execution.
- **入口**：`IsolatedStrategyProxy.generate_signals`
- **现象**：策略在子进程里修改 `context.diagnostics` 后，隔离 runner 只返回 signal DataFrame，父进程 context 仍为空，导致 backtest `rebalance_diagnostics` 丢失 `replacement_trace`、`stage_trace` 等自定义诊断。

## 修复记录

- strategy `generate_signals` 子进程返回 `{signals, diagnostics}` 包装结果。
- `IsolatedStrategyProxy.generate_signals()` 在父进程将 sanitised diagnostics 合并回父侧 `StrategyContext.diagnostics`，对外仍返回原 signal DataFrame。
- 子进程回传前对 diagnostics 做轻量清洗：基础 JSON 类型原样保留，set 转排序列表，复杂 pandas/numpy 对象转字符串，避免跨进程序列化失败。

## 验证

- `uv run python -m unittest tests.test_custom_code_safety.CustomCodeSafetyContractTests.test_isolated_strategy_propagates_context_diagnostics -v`
- `uv run python -m unittest tests.test_custom_code_safety.CustomCodeSafetyContractTests.test_isolated_strategy_sanitizes_non_json_diagnostics -v`

## 残余风险

- 诊断清洗会把复杂对象转为字符串，适合可视化审计，不适合作为下游计算输入。
