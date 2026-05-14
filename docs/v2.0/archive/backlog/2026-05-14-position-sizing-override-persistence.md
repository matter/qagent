# [2026-05-14] Legacy backtest position_sizing override persistence

## Status

Fixed and archived.

## Original Issue

Legacy backtest requests could submit `config.position_sizing`, but earlier persisted backtest details did not expose the effective sizing mode at top-level `config.position_sizing`. This made parameter-only research ambiguous because agents could not tell whether a no-op result came from strategy economics or an override that did not reach the execution path.

## Fix

- `BacktestService.run_backtest()` now persists the resolved `position_sizing` and `max_position_pct` in top-level saved backtest `config`.
- The existing V3.1 config audit remains intact:
  - `config.effective_config.position_sizing`
  - `config.config_provenance.position_sizing`
  - `config.strategy_default_config`
- Added a regression that runs an equal-weight strategy with request `position_sizing="signal_weight"` and verifies:
  - execution weights follow signal strength;
  - top-level returned/saved `config.position_sizing` is `"signal_weight"`;
  - `effective_config.position_sizing` is `"signal_weight"`;
  - `config_provenance.position_sizing` is `"run_override"`.

## Validation

```bash
uv run python -m unittest tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_backtest_request_position_sizing_override_is_applied_and_persisted -v
uv run python -m unittest tests.test_strategy_backtest_market_scope tests.test_strategy_contracts tests.test_backtest_diagnostics_contracts -v
```

Result: 62 tests passed.

## Residual Risk

None for the documented issue. Full REST/MCP behavior uses the same `BacktestService.run_backtest()` path; the focused test validates the service contract that both entry points depend on.
