# QAgent Backlog

This file only tracks unresolved or deferred work. Fixed and mitigated items are archived under `docs/v2.0/archive/backlog/`.

## System Defect Priority

| Priority | Defect | Necessity | Workload |
| --- | --- | --- | --- |
| P1 | Remaining stateful/multi-table long-running workflows need domain-specific staging | Medium. Main final-asset tasks and legacy paper trading advance now stage/promote at task acceptance, but data refresh and 3.0 graph/model/universe/research workflows are state machines, source-data updates, or cache/materialization pipelines that still need explicit resume/idempotency/rollback semantics. | High |
| P2 | Coordinator-friendly research metadata is lightweight/manual | Medium. The coordinator can dispatch top-model Codex agents outside QAgent today; optional metadata/artifact conveniences would reduce bookkeeping without turning QAgent into an agent control plane. | Low-Medium |
| P1 | Research cache file and metadata writes need atomic writer leases | Medium-High. Current cache locks are process-local; concurrent agents through one backend are mostly protected, but second processes or cleanup during writes can corrupt cache state. | Medium |
| P2 | Multi-agent research observability is sparse | Medium. The coordinator can maintain an external dispatch board, but a compact QAgent view of tasks, budgets, evidence, and quarantined results would reduce manual joins. | Medium |
| P0 | Main DuckDB has single-writer operational fragility | Medium. Preflight/API diagnostics make locks actionable, but DuckDB remains a single-file local database. For this single-user system, full replacement is not urgent unless concurrent agent writers become common. | Medium-High |
| P1 | Free equity data is not PIT or survivorship-safe | High for publication-grade research, medium for local exploration. Free yfinance/BaoStock data cannot prove delisted assets, historical membership, symbol changes, and full corporate-action history. Current code blocks publication gates instead of pretending the data is clean. | High |
| P3 | Legacy and 3.0 engines coexist with overlapping concepts | Medium. Migration overlap increases maintenance/audit cost, but it protects existing US workflows. Fix only after 3.0 backtest/signal/paper semantics cover the legacy surface. | High |

## Open

### [2026-05-14] P2 Coordinator-friendly research metadata is lightweight/manual

- **Market**: US, CN
- **Entry**: `docs/agent-collaboration-protocol.md`, `backend/services/agent_research_3_service.py`, `backend/api/agent_research_3.py`, MCP agent research tools, and research artifact APIs.
- **Current mitigation**:
  - A manual coordinator protocol defines the main coordinator, top-model specialist agents, task packets, result packets, coordinator reports, and a compact work record.
  - Existing 3.0 agent research plans can record hypothesis, search space, budget, stop conditions, trials, trial matrix, QA, and promotion decisions.
  - The coordinator can dispatch independent Codex threads outside QAgent and use QAgent only for execution/evidence records.
- **Remaining issue**: Useful coordinator metadata is still mostly free-form. Plan/trial/artifact records can hold role, round, model, and result status in JSON, but MCP/REST convenience is uneven, and there is no standard result artifact schema. This increases bookkeeping, but it does not block the coordinator from operating externally.
- **Expected behavior**: QAgent offers lightweight support for coordinator-managed work: plan metadata, standard JSON result artifacts, optional fields or filters for `round`, `agent_role`, `model`, and `result_status`, and convenient MCP/REST calls to write those records. QAgent should not be required to own Codex thread lifecycle, agent leases, or model dispatch authority.
- **Validation standard**:
  - MCP and REST can write/read plan metadata and JSON result artifacts.
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
  - Migrated final-asset tasks: legacy backtest (`backtest_results`), model train (`models` DB row), factor evaluation (`factor_eval_results`), signal generation (`signal_runs` / `signal_details`), and legacy paper trading advance (`paper_trading_daily` / `paper_trading_sessions`).
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

### [2026-05-07] P1 Corporate actions and survivorship-safe equity universe

- **Market**: US, CN
- **Entry**: `MarketDataFoundationService`, universe/materialization, backtest valuation
- **Current mitigation**: Provider capability metadata and `DataQualityService` publication gates explicitly block `pit_data`, `survivorship_safe_universe`, and `corporate_actions` for free equity sources.
- **Remaining issue**: Free providers expose current/free universe snapshots and daily bars; they do not provide complete dated delistings, historical index membership, symbol changes, split/dividend facts, or fully auditable adjusted-price semantics.
- **Expected behavior**: Delistings, symbol changes, corporate actions, and historical membership are modeled as dated facts and enforced during universe materialization.
- **Validation standard**: Backtests over historical periods include delisted assets when eligible, exclude assets before listing, and apply dated corporate actions consistently.
- **Fix necessity**: High for publication-grade long-horizon alpha claims; medium for exploratory personal research.
- **Estimated workload**: High. Requires a better data source or curated local dated facts plus materialization/backtest enforcement.

### [2026-05-08] P3 Legacy and 3.0 engines coexist with overlapping concepts

- **Market**: US, CN
- **Entry**: legacy services under `backend/services/*_service.py`, 3.0 services under research assets / StrategyGraph / production signal / paper paths.
- **Current mitigation**: 3.0 introduces market-aware assets, StrategyGraph runtime, execution diagnostics, QA evidence, and Workbench visibility while preserving legacy US compatibility.
- **Remaining issue**: Backtest, signal, paper trading, model, and strategy concepts still exist in both legacy and 3.0 forms.
- **Expected behavior**: One audited service path owns each semantic contract, with legacy entry points delegating or migrating without behavior regressions.
- **Validation standard**: Existing US legacy flows pass, 3.0 flows pass, and docs/API clearly mark canonical entry points.
- **Fix necessity**: Medium. It reduces maintenance cost, but premature removal risks breaking validated legacy workflows.
- **Estimated workload**: High. Should be scheduled as a migration project after 3.0 is feature-complete enough to replace legacy flows.
