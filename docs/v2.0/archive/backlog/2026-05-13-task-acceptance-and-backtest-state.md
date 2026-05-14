# [2026-05-13] Task acceptance and legacy backtest state fixes

- **归档状态**：Done / Partially migrated
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-13

## 已修复问题

### Legacy strategy context portfolio state drift under low-turnover controls

- **原问题**：`BacktestService.run_backtest()` 在信号生成日直接用策略目标权重更新 `StrategyContext` 的 `current_weights`、`holding_days`、`avg_entry_price`。实际成交层在 `BacktestEngine.run()` 中还会应用 `rebalance_buffer`、`min_holding_days`、`reentry_cooldown_days` 等低换手控制，因此下一次策略评估可能看到目标持仓，而不是 engine 实际保留的持仓。
- **修复**：
  - legacy backtest 增加 service 侧的 T+1 执行状态镜像。
  - 信号生成日只记录 pending target；下一交易日先按与 engine 一致的低换手约束推进 service 侧持仓状态，再构建新的 `StrategyContext`。
  - 覆盖 `rebalance_buffer`、方向阈值、`hold_overlap_only`、`actual_open` 参考、`min_holding_days`、`reentry_cooldown_days`、`max_holding_days` 和单票上限。
  - 对 `planned_price` 未成交、缺失下一日开盘价等未实际成交场景，不提前把目标持仓写入下一次 strategy context。
  - `actual_open` 参考纳入现金权重，避免 raw-weight/保留现金策略被误归一成满仓。
- **验证**：
  - `tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_strategy_context_tracks_engine_holdings_after_rebalance_buffer`
  - `tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_strategy_context_does_not_assume_blocked_planned_price_fill`
  - `tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_strategy_context_actual_open_buffer_preserves_cash_weight`
  - `tests.test_backtest_engine_contracts`
  - `tests.test_backtest_diagnostics_contracts`

### Main final-asset task writes can publish rows before task acceptance

- **原问题**：`TaskExecutor` 已能 quarantine late result，但主要长任务服务方法仍可能在 callable 返回前直接写最终领域表。任务超时/取消后，late result 不会进入 task summary，但最终领域行可能已经写入。
- **修复**：
  - `TaskExecutor.submit()` 增加 staged domain write 注入机制。任务函数显式接受 `stage_domain_write` 时，最终写入先登记为 staged write，只有任务仍处于 accepted completion boundary 时才执行 commit callback。
  - 带 commit callback 的 staged writes 在 accepted completion boundary 内使用同一个 DuckDB 事务提交；任意 commit 失败会回滚本批 staged final rows。
  - late timeout/cancel result 会保留 quarantine 语义，不执行 staged write commit。
  - REST 与 MCP 的同名入口透传同一服务层行为。
  - 已迁移主要最终资产任务：
    - legacy strategy backtest：`backtest_results`
    - legacy model train：`models`
    - factor evaluate：`factor_eval_results`
    - signal generate：`signal_runs` / `signal_details`
- **验证**：
  - `tests.test_task_executor_contracts.TaskExecutorContractTests.test_staged_task_does_not_publish_domain_rows_before_acceptance`
  - `tests.test_task_executor_contracts.TaskExecutorContractTests.test_staged_task_discards_late_domain_writes_after_timeout`
  - `tests.test_task_executor_contracts.TaskExecutorContractTests.test_staged_domain_writes_roll_back_if_acceptance_commit_fails`
  - `tests.test_factor_feature_market_scope.FactorFeatureMarketScopeTests.test_factor_eval_can_stage_final_result_until_task_acceptance`
  - `tests.test_signal_contracts.SignalServiceContractTests.test_signal_service_can_stage_final_run_until_task_acceptance`
  - `uv run python -m unittest discover -s tests -v`

## 仍需保留的剩余风险

### Stateful/multi-table long-running workflows still need domain-specific staging

- **原因**：paper trading advance、data refresh、3.0 graph/model/universe/research workflows 往往是多表状态机或可重建缓存/原始数据更新，不适合用单个 final-row commit callback 机械包裹。它们需要逐项定义 staging、resume、idempotency 和 rollback 语义。模型训练当前已事务化暂存 DB 最终行，但模型文件 artifact 仍在训练完成前写入模型目录；超时/取消不会发布模型 DB 记录，但可能留下可清理的孤儿文件。
- **处理**：已在 `docs/backlog.md` 保留更窄的剩余项，不再沿用原来的“所有 domain writes 未 staged”的泛化问题。

## 验证记录

- `uv run python -m unittest tests.test_task_executor_contracts -v`
- `uv run python -m unittest tests.test_strategy_backtest_market_scope tests.test_backtest_engine_contracts tests.test_backtest_diagnostics_contracts -v`
- `uv run python -m unittest tests.test_factor_feature_market_scope tests.test_signal_contracts tests.test_model_market_scope -v`
- `uv run python -m unittest discover -s tests -v`：298 tests OK
- `git diff --check`：通过
