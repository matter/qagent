# DB-Backed Task Resource Leases

## Archived Backlog Item

- **Original priority**: P0
- **Original title**: DuckDB-sensitive task scheduling remains process-local
- **Archived on**: 2026-05-14
- **Market scope**: US, CN

## Fix Summary

QAgent now coordinates protected long-running tasks through DuckDB-backed resource leases instead of relying only on process-local serial locks.

Implemented changes:

- Added `task_resource_leases` with active/released/expired status, TTL, heartbeat, release reason, and metadata.
- Added `TaskStore` methods for atomic lease acquire, heartbeat, release, stale expiry, and listing.
- Updated `TaskExecutor` to derive stable resource keys, wait with `serial_wait`, acquire with `serial_acquired`, heartbeat leases during work, and release after accepted terminal states.
- Preserved legacy in-memory serial-lock fallback for test stores that do not implement lease methods.
- Preserved late result quarantine and staged-domain-write semantics for timeout/cancel paths.
- Added REST diagnostics at `GET /api/tasks/resource-leases`.
- Added MCP diagnostics through `list_task_resource_leases`.
- Migrated multi-market data updates to `task_type="data_update_markets"` so they use the global data-update resource key.
- Updated `docs/AGENT_GUIDE.md` with resource lease diagnostics and operational interpretation.

Protected resource key coverage:

- `strategy_backtest`
- `model_train`
- `model_distillation_train`
- `factor_compute`
- `factor_materialize_3_0`
- `model_train_experiment_3_0`
- `strategy_graph_backtest`
- `data_update`
- `data_update_markets`
- `cache_feature_matrix_warmup`

## Validation

Fresh validation after implementation:

```bash
uv run python -m unittest tests.test_task_executor_contracts -v
```

Result: 32 tests passed.

```bash
uv run python -m unittest discover -s tests -v
```

Result: 324 tests passed.

The regression suite covers:

- Two independent `TaskExecutor` instances sharing one DuckDB serialize the same protected resource.
- Completed protected tasks release leases with `release_reason="completed"`.
- Active stale leases expire and can be reacquired.
- REST resource lease listing returns the store payload.
- Existing cancellation, timeout, late-result quarantine, pause-rule, staged-write, and legacy serial progress contracts remain intact.

## Residual Risks

- DuckDB is still a single-file local database. Resource leases coordinate QAgent task submission paths, but they do not make arbitrary direct scripts safe.
- Workflow-specific staging/idempotency remains separate backlog work for stateful multi-table workflows.
- Research cache file writes still need dedicated atomic writer leases and cleanup fencing; task-level cache warmup is now coordinated, but file-level cache integrity is tracked separately.
