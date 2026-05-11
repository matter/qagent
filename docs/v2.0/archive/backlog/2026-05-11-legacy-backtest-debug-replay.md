# Legacy Backtest Debug Replay Snapshots

## Archived Status

- **Status**: Fixed
- **Original priority**: P2
- **Market**: US, CN
- **Entry**: `POST /api/strategies/{strategy_id}/backtest`, `GET /api/strategies/backtests/{backtest_id}/debug-replay`, `BacktestService`

## Original Problem

Diagnosing strategy behavior required repeated prediction and backtest inspection to recover model predictions, factor values, raw signals, weights, constraints, trades, and daily state. These intermediate values existed during backtest execution but were not retained in a replayable form.

## Fix

- Backtest config now supports opt-in `debug_mode`, `debug_level`, `debug_tickers`, and `debug_dates`.
- Debug mode records per-rebalance snapshots for model predictions, factor snapshots, raw signals, target weights, adjusted weights, positions before/after, and strategy diagnostics.
- Debug bundles are written under ignored runtime data path `data/backtest_debug/{backtest_id}` as `manifest.json` plus `rebalance.jsonl`.
- Backtest task summaries expose `debug_artifact_id` only when debug mode is enabled.
- Added REST and MCP loaders plus TTL cleanup:
  - `GET /api/strategies/backtests/{backtest_id}/debug-replay`
  - `DELETE /api/strategies/backtests/debug-replay/expired`
  - `get_backtest_debug_replay`
  - `cleanup_backtest_debug_replay`
- Updated frontend API types and agent documentation.

## Validation

```bash
uv run python -m unittest tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_strategy_api_backtest_task_summary_includes_debug_artifact_id tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_strategy_api_debug_replay_routes_to_backtest_service tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_debug_backtest_writes_readable_replay_bundle_and_cleanup -v
```

