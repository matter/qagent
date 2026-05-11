# Legacy Signal Weight Backtest And Task JSON

## Archived Status

- **Status**: Fixed
- **Original priority**: P1
- **Market**: US
- **Entry**: `POST /api/strategies/{strategy_id}/backtest`, `GET /api/tasks/{task_id}`, `backend/services/backtest_service.py`

## Original Problem

A legacy strategy using `position_sizing="signal_weight"` could fail when strategy output arrived as a dict instead of the canonical signal DataFrame, causing `AttributeError: 'dict' object has no attribute 'empty'`. Failed task status payloads could also preserve raw traceback control characters that broke strict JSON consumers such as `jq`.

## Fix

- Legacy backtest now normalizes strategy outputs before the empty-signal check and position sizing.
- Accepted signal output forms:
  - canonical DataFrame indexed by ticker
  - dict keyed by ticker
  - list of rows with optional `ticker`
- Missing `signal`, `weight`, and `strength` columns are filled with safe defaults.
- Task executor, task store, and task API now sanitize task params, result summaries, and error strings for JSON-safe control characters.

## Validation

```bash
uv run python -m unittest tests.test_strategy_backtest_market_scope.StrategyBacktestMarketScopeTests.test_signal_weight_position_sizing_accepts_dict_strategy_output -v
uv run python -m unittest tests.test_task_executor_contracts.TaskExecutorContractTests.test_failed_task_error_message_is_strict_json_safe -v
uv run python -m unittest tests.test_strategy_backtest_market_scope tests.test_task_executor_contracts -v
```
