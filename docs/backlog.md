# QAgent Backlog

This file only tracks unresolved or deferred work. Fixed and mitigated items are archived under `docs/v2.0/archive/backlog/`.

## System Defect Priority

| Priority | Defect | Necessity | Workload |
| --- | --- | --- | --- |
| P1 | Remaining stateful/multi-table long-running workflows need domain-specific staging | Medium. 3.0 graph/model/universe/research workflows, data refresh, and cache/materialization pipelines are state machines or source-data updates that need explicit resume/idempotency/rollback semantics before old runtime paths can be removed. | High |
| P2 | Coordinator-friendly research metadata is lightweight/manual | Medium. The coordinator can dispatch top-model Codex agents outside QAgent today; optional metadata/artifact conveniences would reduce bookkeeping without turning QAgent into an agent control plane. | Low-Medium |
| P1 | Research cache file and metadata writes need atomic writer leases | Medium-High. Current cache locks are process-local; concurrent agents through one backend are mostly protected, but second processes or cleanup during writes can corrupt cache state. | Medium |
| P2 | Multi-agent research observability is sparse | Medium. The coordinator can maintain an external dispatch board, but a compact QAgent view of tasks, budgets, evidence, and quarantined results would reduce manual joins. | Medium |
| P0 | Main DuckDB has single-writer operational fragility | Medium. Preflight/API diagnostics make locks actionable, but DuckDB remains a single-file local database. For this single-user system, full replacement is not urgent unless concurrent agent writers become common. | Medium-High |
| P3 | Old and 3.0 engines coexist with overlapping concepts | High. V3.2 should not keep old runtime compatibility. Valuable old assets and capabilities must be re-entered, imported, or reimplemented in 3.0, then old services/routes/tables/UI paths should be removed. | High |

## Open

### [2026-05-15] P1 Cancelled old backtest lease can block queued research tasks until restart

- **Market**: US
- **Entry**: old backtest `TaskExecutor` resource leases for `US:legacy-backtest`, `/api/strategies/{strategy_id}/backtest`, `/api/tasks`.
- **Current mitigation**:
  - Agent can inspect `/api/tasks` progress and see `serial_wait`, `blocked_by`, `serial_key`, and terminal `server_restarted` / `cancelled_running_thread` results.
  - Agent should submit old backtests one at a time when a stale or cancelled lane owner is visible.
- **Remaining issue**: On 2026-05-15, three current-system replay backtests for high-performing old strategies were submitted and all waited behind task `522b3734912f4b759f77ffaa9b67e94b` on `US:legacy-backtest`. The blocking task was already terminal with `reason=cancelled_running_thread`, but queued tasks continued to report `serial_wait` until the backend restarted. The queued tasks then failed with `reason=server_restarted` and no `backtest_id`.
- **Expected behavior**: Short term, terminal/cancelled tasks should release or expire resource leases promptly. V3.2 end state should eliminate this lane by reimplementing retained backtest capability in the 3.0 StrategyGraph runtime and deleting the old backtest runtime path.
- **Validation standard**:
  - Start a long old backtest, request cancellation, and verify the `US:legacy-backtest` lease is released or deterministically expired before the old path is removed.
  - Submit a second old backtest after cancellation and confirm it does not wait indefinitely behind a terminal task.
  - `/api/tasks` should distinguish active running owners from stale terminal owners in `blocked_by`.
  - V3.2 final validation should run the equivalent 3.0 StrategyGraph backtest path and confirm the old lane is disabled or removed.
- **Fix necessity**: Medium-High for autonomous research because stale leases can waste trial budget and force manual restarts during backtest sweeps.
- **Estimated workload**: Medium.

### [2026-05-14] P2 Coordinator-friendly research metadata is lightweight/manual

- **Market**: US, CN
- **Entry**: `docs/agent-collaboration-protocol.md`, `docs/agent-workspace/`, `backend/services/agent_research_3_service.py`, `backend/api/agent_research_3.py`, REST agent research endpoints, and research artifact APIs.
- **Current mitigation**:
  - A manual coordinator protocol defines the main coordinator, top-model specialist agents, task packets, result packets, coordinator reports, and a compact work record.
  - Existing 3.0 agent research plans can record hypothesis, search space, budget, stop conditions, trials, trial matrix, QA, and promotion decisions.
  - The coordinator can dispatch independent Codex threads outside QAgent and use QAgent only for execution/evidence records.
- **Remaining issue**: Useful coordinator metadata is still mostly free-form. Plan/trial/artifact records can hold role, round, model, and result status in JSON, but local REST/CLI examples and result schemas are still manual. This increases bookkeeping, but it does not block the coordinator from operating externally.
- **Expected behavior**: QAgent offers lightweight support for coordinator-managed work: plan metadata, standard JSON result artifacts, optional fields or filters for `round`, `agent_role`, `model`, and `result_status`, and convenient REST/local CLI examples to write those records. QAgent should not be required to own Codex thread lifecycle, agent leases, or model dispatch authority.
- **Validation standard**:
  - REST and local CLI examples can write/read plan metadata and JSON result artifacts.
  - A documented result schema can capture baseline refs, role, model, changed module, frozen modules, trials, metrics, risks, and recommendation.
  - Trial matrix or artifact listing can be filtered or grouped by round and role without requiring a full control-plane migration.
- **Fix necessity**: Medium. It reduces manual coordination cost, but the coordinator can already operate from the Codex layer.
- **Estimated workload**: Low-Medium.

### [2026-05-14] P1 Research cache file and metadata writes need atomic writer leases

- **Market**: US, CN
- **Entry**: `backend/services/research_cache_service.py`, `backend/services/factor_engine.py`, cache cleanup APIs, model/dataset/backtest consumers.
- **Current mitigation**:
  - Feature/label hot cache uses process-local locks keyed by cache key.
  - Factor cache writes use process-local locks keyed by market and factor id.
  - Cleanup APIs exist and agents are told not to hand-delete cache files.
- **Remaining issue**: Cache locks are not cross-process. File writes and DuckDB metadata updates are not a single atomic protocol, and cleanup does not have writer leases to fence active writers. A second process or cleanup during a write can leave orphan files, stale metadata, or corrupt partial cache entries.
- **Expected behavior**: Cache writes use temp paths, atomic rename, DB metadata transaction, writer lease, and cleanup fencing. Active writer leases should be visible to coordinator/task diagnostics.
- **Validation standard**: Simulated concurrent writes and cleanup leave either one valid cache entry or a clean miss; no partial file is advertised as valid metadata; stale writer lease cleanup is deterministic.
- **Fix necessity**: Medium-High for multi-agent repeated research. Low-Medium for strictly single-process manual use.
- **Estimated workload**: Medium.

#### Evidence: feature matrix duplicate metadata write can terminate backend

- **Observed at**: 2026-05-15 during current-system replay of `M0427_S262_S204_RANK20_AUG_R1` (`strategy_id=e86479701d15`) over `2026-01-02` to `2026-04-03`.
- **Task**: `c6ff3af0d64e416e9e2432dc03dc0206`, old backtest, `US:legacy-backtest`.
- **Crash point**: `FeatureService.compute_features_from_cache()` writing research-cache metadata for feature set `c52c3ec5572a` after loading 15 factors and starting a 221-factor feature matrix.
- **Fatal error**: DuckDB raised an uncaught fatal internal exception while appending `research_cache_entries`: duplicate primary key `feature_matrix:0c6ebff714c7fba0fadf85f1ea84c65bfb406bc6e0f77def500c3680557b1f58`.
- **Impact**: The backend process terminated; the task was later marked `server_restarted`. Repeating similar current-system replay tasks can crash the backend instead of returning a retryable cache-write conflict.
- **Expected behavior addition**: Cache metadata writes should be idempotent for an already-materialized cache key. A duplicate key should become a valid cache hit or a controlled retryable conflict, never a process-fatal DuckDB append path.

#### Evidence: feature matrix cache path can block API health during backtest

- **Observed at**: 2026-05-15 during `M0515_STRICT_S262_ADAPTIVE_WAVE_CAP20_R1` (`strategy_id=1c9a330600fc`) backtest task `aa561366548240009b448756eb5a2433`.
- **Task config**: `US/sp500`, `2026-01-02` to `2026-04-03`, strategy defaults inherited, `debug_mode=true`, `debug_level=signals`.
- **Last backend progress**: `factor_engine.bulk_cache.loaded` loaded 17 requested factors, then `feature_set.compute_from_cache.start` for `fs_id=c52c3ec5572a`, 221 factors, 503 tickers.
- **Impact**: `/api/health`, `/api/tasks/{task_id}`, and `/api/tasks/resource-leases` timed out after three 30-second wait/retry cycles. Manual backend restart was required, and the task was marked `server_restarted`.
- **Expected behavior addition**: Long feature-matrix materialization should not starve health/task APIs. The task should emit progress heartbeat, reuse an existing active feature-matrix entry when present, or fail with a retryable cache diagnostic instead of making the backend appear unavailable.

### [2026-05-14] P2 Multi-agent research observability is sparse

- **Market**: US, CN
- **Entry**: `/api/tasks`, agent research plan/trial APIs, artifact APIs, React research workbench/task management pages.
- **Current mitigation**:
  - `/api/tasks` lists tasks and exposes task status, late-result diagnostics, pause rules, source, and market.
  - Manual coordinator protocol includes an external dispatch board.
- **Remaining issue**: There is no compact operational view joining QAgent tasks, research plans, trials, artifacts, budgets, quarantined results, and evidence gaps. A human or coordinator can still operate externally, but must manually join multiple APIs and docs.
- **Expected behavior**: A lightweight dashboard/API returns task queue, running work, budget usage, recent trials, result artifacts, pending coordinator decisions, quarantined outputs, and unsafe direct-maintenance warnings.
- **Validation standard**: One endpoint or UI view can answer "what is running, what evidence exists, what is blocked, and what requires human decision?" for the current research round.
- **Fix necessity**: Medium. It improves operability without moving agent orchestration into QAgent.
- **Estimated workload**: Medium.

### [2026-05-13] P1 Remaining stateful/multi-table long-running workflows need domain-specific staging

- **Market**: US, CN
- **Entry**: data refresh tasks, 3.0 strategy graph/model experiment/universe/research workflows, and cache/materialization tasks.
- **Current mitigation**:
  - `TaskExecutor.submit()` now injects `stage_domain_write` for task functions that explicitly accept it.
  - Staged commit callbacks run only at accepted completion boundary and execute inside one DuckDB transaction.
  - Late timeout/cancel results remain quarantined and do not run staged commits.
  - Migrated final-asset tasks: old backtest (`backtest_results`), model train (`models` DB row), factor evaluation (`factor_eval_results`), signal generation (`signal_runs` / `signal_details`), and old paper trading advance (`paper_trading_daily` / `paper_trading_sessions`).
- **Remaining issue**: Remaining stateful workflows still write intermediate or state-transition rows during execution. They cannot be safely migrated by a single final-row commit callback because their correctness depends on resume points, idempotent day advancement, source-data semantics, cache invalidation, and rollback policy.
- **Expected behavior**: Each stateful workflow defines its own staging/resume/idempotency contract. Accepted completion should promote final state atomically; timeout/cancel should either leave a clearly resumable staging state or roll back final domain state.
- **Validation standard**: Synthetic timeout/cancel cases for each migrated workflow leave no partial final state, or leave only documented resumable staging rows with a recovery command.
- **Fix necessity**: Medium-High for autonomous batch agents. Lower for manually supervised single-user runs where paper/data updates are intentionally stateful.
- **Estimated workload**: High. Requires workflow-by-workflow design rather than generic wrapping.

### [2026-05-08] P0 Main DuckDB has single-writer operational fragility

- **Market**: US, CN
- **Entry**: `backend/db.py`, backup/restore, diagnostics, tests, local agent tools
- **Current mitigation**: `/api/diagnostics/db-preflight`, `DbPreflightService`, backup preflight, and diagnostic route hints report `locked` / `in_use` states with actionable API routes and maintenance-mode guidance.
- **Remaining issue**: Separate agent/test/diagnostic processes can still fail when another process holds the DuckDB file lock.
- **Expected behavior**: Operational tools should either route through the running API, use explicit maintenance mode, or move to a server database if multi-process writers become required.
- **Validation standard**: With backend running, direct-maintenance commands fail gracefully and agent diagnostics can be completed through API routes.
- **Fix necessity**: Medium under the current single-user local design. Do not replace DuckDB unless concurrent writers become a product requirement.
- **Estimated workload**: Medium-High for a full server-DB migration; Low-Medium for continued guardrail improvements.

### [2026-05-08] P3 Old and 3.0 engines coexist with overlapping concepts

- **Market**: US, CN
- **Entry**: old services under `backend/services/*_service.py`, 3.0 services under research assets / StrategyGraph / production signal / paper paths.
- **Current mitigation**: 3.0 introduces market-aware assets, StrategyGraph runtime, execution diagnostics, QA evidence, and Workbench visibility, but old US runtime paths still exist.
- **Remaining issue**: Backtest, signal, paper trading, model, and strategy concepts still exist in both old and 3.0 forms.
- **Expected behavior**: One audited 3.0 service path owns each semantic contract. Any old asset worth keeping is re-entered, imported, or reimplemented in 3.0; old entry points are deleted or disabled instead of delegated.
- **Validation standard**: 3.0 flows pass for all retained capabilities; migration reports prove retained old assets were re-entered/imported/reimplemented; docs/API no longer advertise old runtime entry points.
- **Fix necessity**: High. Keeping compatibility increases maintenance cost and blocks the goal of fully separating from the old architecture.
- **Estimated workload**: High. This is the main V3.2 migration project.
