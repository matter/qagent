# DB-Backed Task Resource Leases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace process-local task serial lanes with DuckDB-backed resource leases so DuckDB-sensitive tasks serialize across backend processes and expose lease ownership/status to agents.

**Architecture:** Keep `TaskExecutor` as the only submission/execution boundary, but move the serial-lane decision from in-memory `threading.Lock` to persisted task resource leases. A task derives one or more resource keys before work starts, acquires active leases with TTL/heartbeat semantics, releases them on accepted terminal states, and reports wait/acquire/release progress through existing `/api/tasks` diagnostics.

**Tech Stack:** Python 3.14, FastAPI, DuckDB, existing `backend/tasks` executor/store models, `unittest`.

---

## Scope

This plan fixes the backlog item "P0 DuckDB-sensitive task scheduling remains process-local" by adding DB-backed resource coordination. It does not migrate QAgent away from DuckDB, and it does not solve workflow-specific staging/idempotency. Those remain separate backlog items.

## File Map

- Modify: `backend/db.py`
  - Add `task_resource_leases` DDL and indexes.
  - Add lightweight migration for existing databases.
- Modify: `backend/tasks/store.py`
  - Add lease acquisition, heartbeat, release, stale lease expiry, and listing methods.
- Modify: `backend/tasks/executor.py`
  - Replace `_serial_locks` execution gating with DB-backed resource leases.
  - Preserve progress phases `serial_wait` and `serial_acquired`, adding lease metadata.
  - Release leases on completion, failure, timeout, cancellation, and pre-work cancellation.
- Modify: `backend/api/tasks.py`
  - Add read-only endpoint to list active/recent leases.
  - Optionally add admin endpoint to expire stale leases after preflight.
- Modify: `backend/mcp_server.py`
  - Add MCP read tool for task resource leases if existing task MCP patterns are nearby and low-risk.
- Modify: `tests/test_task_executor_contracts.py`
  - Add unit contracts for resource-key derivation, cross-executor serialization, stale lease expiration, release on terminal status, and API payload exposure.
- Modify: `docs/AGENT_GUIDE.md`
  - Document resource leases, diagnostics, and operational rules.
- Modify: `docs/backlog.md`
  - Keep item until implementation and validation are complete; archive afterward.

## Resource Key Policy

Initial protected resources should be conservative:

- `strategy_backtest`: `market:{market}:legacy-backtest`
- `model_train`: `market:{market}:model-train:{feature_set_id}:{universe_group_id}` when both IDs exist; for CN use `market:CN:heavy-research` to preserve the existing shared lane.
- `model_train_distillation`: `market:{market}:model-train:{student_feature_set_id}:{universe_group_id}`
- `factor_compute`: `market:{market}:factor:{factor_id}`
- `factor_materialize_3_0`: `market_profile:{market_profile_id}:factor_spec:{factor_spec_id}:universe:{universe_id}`
- `model_train_experiment_3_0`: `dataset:{dataset_id}:model-train-experiment`
- `strategy_graph_backtest`: `strategy_graph:{strategy_graph_id}:backtest`
- `data_update`: `market:{market}:data-update`
- `data_update_markets`: `global:data-update`
- `research_cache_warmup` or cache write tasks if present: `cache:{cache_key}` or `market:{market}:research-cache`

Keep this mapping in one function, for example `TaskExecutor._resource_keys(task_type, params)`. The old `_serial_key()` can call the new function for compatibility until tests are updated.

## Lease Semantics

Create table:

```sql
CREATE TABLE IF NOT EXISTS task_resource_leases (
    resource_key VARCHAR PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_type VARCHAR NOT NULL,
    market VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'active',
    acquired_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    heartbeat_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    expires_at TIMESTAMP NOT NULL,
    released_at TIMESTAMP,
    release_reason VARCHAR,
    metadata JSON
);
```

Acquire rule:

- A task may acquire a lease if no active unexpired row exists for the key.
- If an active row exists but `expires_at < now`, mark it `expired` and allow acquisition.
- Acquisition must happen in a DB transaction.
- If multiple resource keys are needed, sort keys and acquire in deterministic order.

Heartbeat rule:

- Worker heartbeat should update `heartbeat_at` and `expires_at` periodically while waiting/working.
- Start with lease TTL `max(120 seconds, min(timeout_seconds, 900 seconds))`.
- Heartbeat every `min(30 seconds, ttl / 3)` while the task is active.

Release rule:

- On normal accepted completion, failed execution, timeout terminalization, or cancellation, update lease `status='released'`, `released_at`, `release_reason`.
- If process dies, `mark_stale()` should expire leases owned by queued/running tasks that are marked interrupted.

## Task 1: Add Schema And Store Methods

**Files:**
- Modify: `backend/db.py`
- Modify: `backend/tasks/store.py`
- Test: `tests/test_task_executor_contracts.py`

- [ ] **Step 1: Write failing store tests**

Add tests:

```python
def test_task_store_acquires_and_rejects_active_resource_lease(self):
    # Use temp DuckDB through backend.db.get_connection patch pattern already in this file.
    # acquire_resource_leases(task_id="t1", keys=["market:US:legacy-backtest"]) returns acquired.
    # acquire_resource_leases(task_id="t2", same key) returns blocked with owner t1.
```

```python
def test_task_store_expires_stale_resource_lease_before_acquiring(self):
    # Insert active lease with expires_at in the past.
    # acquire by t2 marks old row expired/replaced and returns acquired.
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
uv run python -m unittest tests.test_task_executor_contracts.TaskExecutorContractTests.test_task_store_acquires_and_rejects_active_resource_lease tests.test_task_executor_contracts.TaskExecutorContractTests.test_task_store_expires_stale_resource_lease_before_acquiring -v
```

Expected: fail because store methods/table do not exist.

- [ ] **Step 3: Add schema**

In `backend/db.py`:

- Add `_TASK_RESOURCE_LEASES_DDL`.
- Add it to the initialization table list immediately after `task_pause_rules`.
- Add indexes:
  - `idx_task_resource_leases_task`
  - `idx_task_resource_leases_status_expires`

- [ ] **Step 4: Implement store methods**

In `TaskStore` add:

- `acquire_resource_leases(task_id, task_type, resource_keys, market, ttl_seconds, metadata=None) -> dict`
- `heartbeat_resource_leases(task_id, resource_keys, ttl_seconds) -> None`
- `release_resource_leases(task_id, resource_keys=None, reason="completed") -> int`
- `expire_stale_resource_leases(now=None) -> int`
- `list_resource_leases(active_only=True, limit=100) -> list[dict]`

Return blocked acquisition payload like:

```python
{
    "acquired": False,
    "blocked": [
        {"resource_key": "...", "task_id": "owner", "expires_at": "..."}
    ],
}
```

- [ ] **Step 5: Run store tests GREEN**

Run the same command from Step 2. Expected: PASS.

## Task 2: Replace Process-Local Serial Locks In TaskExecutor

**Files:**
- Modify: `backend/tasks/executor.py`
- Test: `tests/test_task_executor_contracts.py`

- [ ] **Step 1: Write failing executor tests**

Add tests:

```python
def test_two_executors_serialize_same_resource_through_store_lease(self):
    # Create two TaskExecutor instances with stores pointing at same temp DuckDB.
    # First task holds lease until event release.
    # Second task reports serial_wait and does not start work until first releases.
```

```python
def test_task_releases_resource_lease_after_completion(self):
    # Submit one protected task.
    # After completion, list leases and assert status released/release_reason completed.
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
uv run python -m unittest tests.test_task_executor_contracts.TaskExecutorContractTests.test_two_executors_serialize_same_resource_through_store_lease tests.test_task_executor_contracts.TaskExecutorContractTests.test_task_releases_resource_lease_after_completion -v
```

Expected: fail because executor still uses local locks.

- [ ] **Step 3: Implement resource key derivation**

In `TaskExecutor`:

- Add `_resource_keys(task_type, params) -> list[str]`.
- Keep `_serial_key()` as compatibility wrapper returning the first key or `None` for old unit tests during transition.
- Include all resource keys from the "Resource Key Policy" section.

- [ ] **Step 4: Implement wait/acquire loop**

In `_run()` before user function execution:

- Derive sorted resource keys.
- Try `store.acquire_resource_leases(...)`.
- If blocked, write progress phase `serial_wait` with `resource_keys`, `blocked_by`, and retry after a small interval.
- Use short polling interval initially, e.g. `0.25s`, capped at `2s`.
- Respect cancellation/timeout while waiting.
- On acquire, write progress phase `serial_acquired` with lease info.

- [ ] **Step 5: Implement release**

Release leases in all terminal paths:

- normal accepted completion;
- callable exception;
- timeout terminalization;
- cancellation before work;
- cancellation while work may continue should not publish result, but should release lease only when worker exits or when lease expires. The cancellation response should explain this.

- [ ] **Step 6: Run executor tests GREEN**

Run the command from Step 2. Expected: PASS.

## Task 3: Expose Lease Diagnostics Through REST And MCP

**Files:**
- Modify: `backend/api/tasks.py`
- Modify: `backend/mcp_server.py`
- Test: `tests/test_task_executor_contracts.py` or create `tests/test_tasks_api_contracts.py`

- [ ] **Step 1: Write failing API test**

Test `GET /api/tasks/resource-leases` via direct endpoint call with patched store:

```python
def test_task_api_lists_resource_leases(self):
    # Patch _get_store().list_resource_leases and assert payload is returned.
```

- [ ] **Step 2: Add endpoint**

Add:

```python
@router.get("/resource-leases")
async def list_resource_leases(active_only: bool = Query(True), limit: int = Query(100, ge=1, le=500)) -> list[dict]:
    return _get_store().list_resource_leases(active_only=active_only, limit=limit)
```

- [ ] **Step 3: Add MCP read tool**

Add `list_task_resource_leases(active_only: bool = True, limit: int = 100)` near task MCP tools, calling the same `TaskStore` method.

- [ ] **Step 4: Run API/MCP-focused tests**

Run:

```bash
uv run python -m unittest tests.test_task_executor_contracts -v
```

Expected: PASS.

## Task 4: Migrate Protected Task Submissions And Preserve Existing Behavior

**Files:**
- Modify: `backend/tasks/executor.py`
- Read-only verification: API modules that submit tasks
  - `backend/api/data.py`
  - `backend/api/factors.py`
  - `backend/api/models.py`
  - `backend/api/strategies.py`
  - `backend/api/strategy_graph_3.py`
  - `backend/api/model_experiment_3.py`
  - `backend/api/factor_engine_3.py`
  - `backend/api/research_cache.py`

- [ ] **Step 1: Add tests for task type key coverage**

Add assertions for each protected task type:

```python
self.assertIn("market:US:data-update", TaskExecutor._resource_keys("data_update", {"market": "US"}))
self.assertIn("strategy_graph:graph_1:backtest", TaskExecutor._resource_keys("strategy_graph_backtest", {"strategy_graph_id": "graph_1"}))
```

- [ ] **Step 2: Run coverage tests RED/GREEN**

Run:

```bash
uv run python -m unittest tests.test_task_executor_contracts -v
```

- [ ] **Step 3: Audit task submitters**

Use:

```bash
rg -n "_executor\\(\\)\\.submit|get_task_executor\\(\\)\\.submit|executor\\.submit" backend/api backend/mcp_server.py backend/services -g '*.py'
```

For any protected task missing required params for resource key derivation, pass the needed IDs in `params`.

- [ ] **Step 4: Verify existing task contracts**

Run:

```bash
uv run python -m unittest tests.test_task_executor_contracts -v
```

Expected: all task executor tests pass.

## Task 5: Documentation, Backlog, And Validation

**Files:**
- Modify: `docs/AGENT_GUIDE.md`
- Modify: `docs/backlog.md`
- Create: `docs/v2.0/archive/backlog/YYYY-MM-DD-db-backed-task-resource-leases.md`

- [ ] **Step 1: Update agent guide**

Document:

- `serial_wait` now represents DB-backed resource lease waiting.
- New `/api/tasks/resource-leases` endpoint and MCP tool.
- How to interpret `resource_key`, `task_id`, `expires_at`, `heartbeat_at`, `released_at`, and stale lease expiry.

- [ ] **Step 2: Archive backlog item only after validation**

Move the P0 backlog item out of `docs/backlog.md` only after all tests below pass and manual diagnostics are confirmed.

- [ ] **Step 3: Run focused validation**

Run:

```bash
uv run python -m unittest tests.test_task_executor_contracts -v
uv run python -m unittest discover -s tests -v
git diff --check
```

Expected:

- Task executor tests pass.
- Full test suite passes or any unrelated environmental failures are documented with exact failures.
- No whitespace errors.

## Rollback Plan

If DB-backed leases create deadlock or regressions:

1. Keep schema in place; do not drop data.
2. Add a guarded fallback config/env flag only if needed: `tasks.use_resource_leases=false`.
3. Re-enable old process-local `_serial_locks` under the flag.
4. Keep `/api/tasks/resource-leases` read-only so stale rows can be diagnosed.

## Acceptance Criteria

- Two independent `TaskExecutor` instances sharing one DuckDB serialize a protected resource.
- Stale leases expire and can be reacquired deterministically.
- Active leases are visible through REST and, if implemented, MCP.
- Existing task pause rules and cancellation semantics still work.
- Late/cancelled task results remain quarantined and do not publish staged domain writes.
- `docs/backlog.md` no longer contains the P0 item only after the archive document records validation evidence.
