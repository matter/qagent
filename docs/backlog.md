# QAgent Backlog

This file only tracks unresolved or deferred work. Fixed and mitigated items are archived under `docs/v2.0/archive/backlog/`.

## System Defect Priority

| Priority | Defect | Necessity | Workload |
| --- | --- | --- | --- |
| P0 | Domain writes are not transactionally staged by task acceptance | High. Late task return payloads are quarantined and accepted callbacks now exist, but long-running domain services still write directly during computation. Cancel/timeout can still leave partial final rows until each write-heavy workflow adopts stage-then-promote. | High |
| P0 | Main DuckDB has single-writer operational fragility | Medium. Preflight/API diagnostics make locks actionable, but DuckDB remains a single-file local database. For this single-user system, full replacement is not urgent unless concurrent agent writers become common. | Medium-High |
| P1 | Free equity data is not PIT or survivorship-safe | High for publication-grade research, medium for local exploration. Free yfinance/BaoStock data cannot prove delisted assets, historical membership, symbol changes, and full corporate-action history. Current code blocks publication gates instead of pretending the data is clean. | High |
| P3 | Legacy and 3.0 engines coexist with overlapping concepts | Medium. Migration overlap increases maintenance/audit cost, but it protects existing US workflows. Fix only after 3.0 backtest/signal/paper semantics cover the legacy surface. | High |

## Open

### [2026-05-08] P0 Domain writes are not transactionally staged by task acceptance

- **Market**: US, CN
- **Entry**: Long-running service methods that write before task completion.
- **Current mitigation**: `TaskExecutor.submit(..., on_accept=...)` can run commit callbacks only at the accepted completion boundary, and timeout/cancel late results are quarantined.
- **Remaining issue**: Existing service methods still insert domain rows during computation before returning. Those paths have not all been migrated to staging tables/artifacts plus accepted promotion callbacks.
- **Expected behavior**: Long task outputs should write to staging artifacts/runs and be promoted only if the task is still accepted.
- **Validation standard**: A synthetic long-running write task cancelled mid-run leaves no final domain rows unless explicitly resumed or accepted.
- **Fix necessity**: High for autonomous agent workflows that create research assets in batches; lower for manually supervised single short tasks.
- **Estimated workload**: High. Requires inventorying write-heavy workflows, adding staging/promote contracts, and migrating backtest/model/factor/signal/paper task entry points incrementally.

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
