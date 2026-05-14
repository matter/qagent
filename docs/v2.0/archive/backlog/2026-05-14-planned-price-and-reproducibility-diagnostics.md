# 2026-05-14 Planned Price And Reproducibility Diagnostics

## Archived Items

### P1 Planned-price legacy backtest misses decision-close fallback on empty-signal exit days

- **Status**: Fixed.
- **Change**: `BacktestService.run_backtest()` now writes planned prices in the `raw_signals.empty` branch for current holdings that are being exited. The fallback uses the decision-date close, matching the documented planned-price semantics.
- **Impact**: Strategies may continue using an empty signal frame to mean target cash. Existing holdings no longer become `invalid_planned_price` exits solely because the strategy emitted no rows.

### P1 Legacy backtest reproducibility changes across backend commits despite same asset/config/data fingerprints

- **Status**: Fixed.
- **Change**: `/api/strategies/backtests/research-summary` now includes `reproducibility_diagnostics` built from baseline/trial `summary.reproducibility_fingerprint`.
- **Impact**: Agent research can distinguish input/runtime drift from backend/service version drift before interpreting pre/post update performance changes.
- **New fields**:
  - `available`
  - `strictly_comparable`
  - `compatibility_flag`
  - `difference_sources`
  - `field_diffs`
  - `result_shape_delta`
  - `hashes`

### P1 Planned-price fallback request field is not persisted in legacy backtests

- **Status**: Verified stale / covered.
- **Evidence**: Current request flow passes `config` through REST/MCP into `BacktestService`, `BacktestConfig.to_dict()` includes `planned_price_fallback`, and `_save_result()` persists the normalized config. A focused regression test now covers `planned_price_fallback="next_close"` persistence and planned execution diagnostics.
- **Impact**: Old result rows that omitted this field should still be treated as ambiguous, but new runs persist the accepted fallback setting.

## Validation

Commands run during the fix:

```bash
uv run python -m unittest tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_planned_price_empty_signal_exit_uses_decision_close_fallback -v
uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_research_summary_explains_backend_commit_fingerprint_drift -v
uv run python -m unittest tests.test_backtest_diagnostics_contracts.BacktestDiagnosticsContractTests.test_save_result_persists_planned_price_fallback_config -v
uv run python -m unittest tests.test_strategy_backtest_market_scope tests.test_backtest_diagnostics_contracts -v
uv run python -m unittest discover -s tests -v
```

## Residual Risk

- `reproducibility_diagnostics` explains drift categories and result-shape deltas. It does not reconstruct an exact semantic code diff between commits.
- Old persisted backtest rows remain historical records. If their config lacks `planned_price_fallback`, agents should not infer the originally requested fallback mode.
