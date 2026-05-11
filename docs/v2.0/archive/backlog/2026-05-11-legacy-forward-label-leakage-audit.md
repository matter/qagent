# Legacy Forward-Label Leakage Audit

## Archived Status

- **Status**: Fixed
- **Original priority**: P1
- **Market**: US
- **Entry**: Legacy model metadata, label service, backtest leakage warnings.

## Original Problem

Legacy backtest leakage warnings compared `model_data_end` directly to `backtest_start`. Forward labels can use prices up to `label_date + horizon`, so a model with `test_end=2025-12-31` and a 20-trading-day label horizon could consume January 2026 prices while still reporting `time_overlap=false` for a `2026-01-02` backtest.

## Fix

- Model training now persists `label_summary` and `label_horizon` into model metadata/eval metrics.
- Legacy leakage audit derives the effective model data end by shifting the train/valid/test window end forward by `label_horizon` trading days.
- Leakage warnings now include:
  - `model_window_end`
  - `model_data_end`
  - `label_horizon`

## Validation

```bash
uv run python -m unittest tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_leakage_audit_uses_forward_label_horizon_effective_data_end tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_leakage_audit_allows_safe_forward_label_horizon_cutoff -v
uv run python -m unittest tests.test_strategy_backtest_market_scope -v
```
