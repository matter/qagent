# 2026-05-14 Research Diagnostics And Stateful Staging Fixes

Archived from `docs/backlog.md`. These items are no longer open.

## Fixed Items

### Research diagnostics compact summaries

- Added `BacktestService.get_research_summary()`.
- Added REST endpoint `POST /api/strategies/backtests/research-summary`.
- Added MCP tool `get_backtest_research_summary`.
- The response returns baseline/trial metric snapshots, metric deltas, bounded rebalance digest, trade counts, changed variable, and an explicit `promote` / `continue` / `stop` decision.
- Full rebalance/debug payloads remain available through paginated diagnostics and debug replay.

### Structured agent research trial matrix

- Added `AgentResearch3Service.get_trial_matrix()`.
- Added REST endpoint `GET /api/research/agent/plans/{plan_id}/trial-matrix`.
- Added MCP tool `get_agent_research_trial_matrix_3_0`.
- The matrix surfaces baseline ids, changed module, changed variable, hypothesis, config hash, metrics, result refs, decision, and stop reason.
- Trial rows are sorted by primary metric and grouped by changed module for fast agent review.

### Legacy model audit metadata and leakage horizon visibility

- `ModelService.get_model()` now exposes top-level audit fields derived from `train_config` and `eval_metrics`.
- Returned records include split dates, `purge_gap`, `metrics`, `label_horizon`, `effective_label_horizon`, and `metadata.label_data_end`.
- The audit metadata explicitly documents `label_data_end < backtest_start` as the cutoff rule.
- Existing forward-label leakage checks continue to use effective label horizon in backtest warnings.

### Legacy backtest progress and reproducibility fingerprint

- `TaskExecutor` now writes `serial_wait` and `serial_acquired` progress events with `serial_key`.
- Agents can distinguish queued-on-lock from active compute through `/api/tasks/{task_id}` without reading logs.
- Backtest reproducibility fingerprints now include dirty runtime state for backend execution modules:
  - `runtime.dirty`
  - `runtime.dirty_paths`
  - `runtime.patch_hash`
  - `comparability.clean_runtime`
  - `comparability.warnings`
- Dirty worktrees are marked with `dirty_worktree`, so reruns with uncommitted semantic changes are not silently treated as strictly comparable.

### Paper trading advance staged acceptance

- `PaperTradingService.advance()` accepts `stage_domain_write`.
- When run through `TaskExecutor`, paper daily snapshots and session current-state updates are staged under `paper_trading_advance`.
- Staged writes are promoted only at accepted task completion inside the existing task transaction boundary.
- Timeout, cancellation, and late results do not publish final paper-trading rows.
- Direct synchronous service calls still preserve the previous immediate-write behavior for tests and maintenance use.

## Validation

- `uv run python -m unittest tests.test_model_market_scope tests.test_strategy_backtest_market_scope tests.test_backtest_diagnostics_contracts -v`
- `uv run python -m unittest tests.test_task_executor_contracts tests.test_agent_research_3_service tests.test_signal_paper_market_scope tests.test_paper_trading_contracts -v`

