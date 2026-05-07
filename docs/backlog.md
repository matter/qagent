# QAgent Backlog

## System Defect Priority

This table tracks defects that block QAgent from becoming a stable professional quant system for agent-led exploration and human-led review.

| Priority | Defect | Impact | Workload |
| --- | --- | --- | --- |
| P0 | User factor/strategy code still executes in backend process | Static safety checks now reject obvious unsafe constructs, but a bad or hostile factor/strategy can still block workers or consume memory until full process isolation exists. | High |
| P0 | Domain writes are not transactionally staged by task acceptance | Task timeout/cancel now marks terminal state authoritative and quarantines late return payloads, but arbitrary domain code can still write before returning unless write paths are staged/accepted. | High |
| P0 | Main DuckDB has single-writer operational fragility | DB preflight and backup fail-fast now give actionable lock diagnostics, but the architecture still depends on DuckDB single-writer discipline. | Medium |
| P1 | Free equity data is not PIT or survivorship-safe | Long-horizon research can use current universe snapshots, missing delisted assets, and incomplete corporate-action history. Reported alpha is not publication-grade. | High |
| P1 | Macro data lacks strict historical realtime replay | FRED data is useful for research, but the current implementation cannot prove each decision date only saw values available then. | High |
| P1 | Human review UI still lacks approval workflow depth | Workbench now exposes data quality, PIT warnings, macro source status, QA evidence blocks, runs, artifacts, and promotions, but explicit approve/reject workflow is still thin. | Medium |
| P2 | 3.0 StrategyGraph backtest fill model is incomplete | Strategy comparison is possible, but fills, costs, lot sizes, suspend/limit checks, and NAV/trade reconciliation are not execution-grade. | Medium-High |
| P2 | Research cache versioning can fall back to `latest` | Cache reuse can become ambiguous when data changes and callers omit a stable snapshot/version. This is efficiency-positive but reproducibility-risky. | Medium |
| P2 | Agent research trial recording has a table-state-specific failure | One QRP2 plan hit a DuckDB vector error while another empty plan succeeded; this needs isolation before trusting batch trial persistence. | Low-Medium |
| P2 | REST/MCP/UI parity is incomplete | Backend service layer mostly exists, but UI screens do not expose all 3.0 REST/MCP capabilities and review diagnostics. | Medium |
| P3 | Legacy and 3.0 engines coexist with overlapping concepts | Useful during migration, but duplicated backtest/signal/paper semantics increase maintenance and audit cost. | High |

## Completed / Mitigated

### [2026-05-08] Task timeout/cancel late-result quarantine

- **Change**: `TaskExecutor` marks cancellation/timeout terminal state as authoritative, preserves late worker payload only under `late_result_diagnostics`, and exposes `late_result_quarantined=true` through REST/MCP/UI.
- **Validation**: `uv run python -m unittest tests.test_task_executor_contracts -v`
- **Residual risk**: This prevents consumers from accepting late return payloads, but does not yet transactionally prevent domain writes performed inside the callable before return.

### [2026-05-08] QA evidence package gate

- **Change**: Promotion-like QA now requires lineage artifacts and complete `metrics.evidence` with data-quality contract, PIT status, split policy, dependency snapshot, valuation diagnostics, artifact hashes, and reviewer decision.
- **Validation**: `uv run python -m unittest tests.test_agent_research_3_service -v`
- **Residual risk**: Evidence content is structurally required, but deep semantic verification of PIT replay and valuation reconciliation remains separate work.

### [2026-05-08] Secret/config governance

- **Change**: Committed `config.yaml` no longer stores the FRED API key. Runtime can use `FRED_API_KEY` or ignored `config.local.yaml`.
- **Validation**: `uv run python -m unittest tests.test_macro_data_config -v`
- **Residual risk**: Any key already committed in old git history should still be considered exposed and rotated outside this repo change.

### [2026-05-08] DB lock preflight

- **Change**: Added `/api/diagnostics/db-preflight`, `DbPreflightService`, and backup script fail-fast handling for locked/unavailable DuckDB.
- **Validation**: `uv run python -m unittest tests.test_db_preflight tests.test_diagnostics_api_contracts -v`
- **Residual risk**: This improves diagnostics; it does not remove DuckDB single-writer constraints.

### [2026-05-08] Custom code static risk checks

- **Change**: Factor/strategy loaders reject obvious unsafe constructs such as `while True`, file IO calls, dunder access, and blocked system/network modules before `exec`.
- **Validation**: `uv run python -m unittest tests.test_custom_code_safety -v`
- **Residual risk**: Static checks are not a sandbox. Full fix requires isolated worker processes with CPU/time/memory/filesystem/network limits.

### [2026-05-08] Human review workbench visibility

- **Change**: 3.0 Research Workbench now shows data-quality/provider capabilities, PIT support counts, FRED macro series, and QA evidence blocking summary.
- **Validation**: `cd frontend && pnpm build`
- **Residual risk**: Humans can inspect more context, but explicit approve/reject queues and reviewer assignment are not complete.

## Open

### [2026-05-08] P0 Domain writes are not transactionally staged by task acceptance

- **Market**: US, CN
- **Entry**: Long-running service methods that write before task completion.
- **Evidence**: Task return payloads are now quarantined after cancel/timeout, but service code can still insert domain rows during computation before returning.
- **Actual result**: A timed-out or cancelled agent task can still leave partial domain rows if write paths are not staged.
- **Expected behavior**: Long task outputs should write to staging artifacts/runs and be promoted only if the task is still accepted.
- **Validation standard**: A synthetic long-running write task cancelled mid-run leaves no final domain rows unless explicitly resumed or accepted.
- **Research impact**: Agent-led exploration can pollute research assets and make human review state unreliable.

### [2026-05-08] P0 User factor/strategy code executes in backend process

- **Market**: US, CN
- **Entry**: `backend/factors/loader.py`, `backend/strategies/loader.py`
- **Evidence**: Custom source is executed with `exec(compile(...))` in a restricted namespace. Imports are whitelisted, but execution still runs inside the API worker process.
- **Actual result**: Obvious unsafe source is now statically rejected, but memory-heavy code, expensive pandas operations, and non-obvious abuse can still degrade the backend and task executor.
- **Expected behavior**: User code runs in an isolated worker process with CPU/time/memory/file/network limits and a typed input/output contract.
- **Validation standard**: A deliberately looping or memory-heavy factor is killed without taking down the backend or leaving partial writes.
- **Research impact**: Strategy exploration is powerful but not operationally safe enough for autonomous agents.

### [2026-05-08] P0 Main DuckDB has single-writer operational fragility

- **Market**: US, CN
- **Entry**: `backend/db.py`, backup/restore, diagnostics, tests, local agent tools
- **Evidence**: `init_db()` against the main DB previously failed with `DuckDB IOException: Could not set lock on file ... Conflicting lock is held ... PID 27714`; preflight now reports this as `status=locked`.
- **Actual result**: Separate agent/test/diagnostic processes can still fail when the app or another Python process holds the DB lock, but diagnostics are now actionable.
- **Expected behavior**: Operational tools should detect active locks, route read-only diagnostics through the running API, or use explicit maintenance mode.
- **Validation standard**: With the backend running, read diagnostics, backup preflight, and test setup fail gracefully with actionable messages instead of raw DuckDB lock errors.
- **Research impact**: Agent operations are brittle; unattended development or monitoring can fail for environmental reasons unrelated to research logic.

### [2026-05-08] P1 Human review approval workflow is incomplete

- **Market**: US, CN
- **Entry**: `frontend/src/pages/ResearchWorkbench3.tsx`, `DataManagePage.tsx`, `SystemSettings.tsx`
- **Evidence**: Workbench now exposes data-quality contract, PIT warnings, macro source status, QA evidence blocks, runs, artifacts, and promotions; approval/rejection remains a direct promotion call without reviewer queue/state.
- **Actual result**: Humans can inspect more evidence, but review workflow is still not strong enough for controlled approval operations.
- **Expected behavior**: Human reviewers can approve/reject a StrategyGraph promotion from the UI after seeing source quality, lineage, metrics, diagnostics, and artifact hashes, with reviewer decision persisted as first-class evidence.
- **Validation standard**: A reviewer can approve/reject a StrategyGraph promotion from the UI and the decision appears in QA/promotion evidence.
- **Research impact**: The target operating model, agent explores and human audits, is only partially supported.

### [2026-05-07] P2 Agent research trial recording fails for empty QRP2 plan

- **Market**: US
- **Entry**: REST `POST /api/research/agent/plans/{plan_id}/trials/batch`
- **Affected plan**: `51cf7a803839` (`reclaim_event_alpha`)
- **Request shape**: batch trial recording for completed strategy backtests under QRP2.
- **Actual result**: API returns HTTP 500 before inserting any trial. `GET /api/research/agent/plans/51cf7a803839/trials?limit=20` returns `[]`.
- **Observed log**: `DuckDB InternalException: Attempted to access index 0 within vector of size 0` at `backend/services/agent_research_3_service.py::_next_trial_index`, query `SELECT COALESCE(MAX(trial_index), 0) + 1 FROM agent_research_trials WHERE plan_id = ?`.
- **Follow-up observation**: Empty-plan batch recording succeeded for QRP3 plan `e97e4c4f3d51` on 2026-05-07, inserting five trials. The failure may be plan/table-state specific rather than a universal empty-plan path failure.
- **Expected behavior**: First trial for an empty research plan should record successfully with `trial_index = 1`; batch and single-trial recording should share the same behavior.
- **Validation standard**: Empty-plan single and batch trial recording both return 200, insert records, and existing non-empty plan trial indices remain monotonic.
- **Research impact**: QRP2 backtests can be evaluated, but audit records cannot currently be persisted through the official agent research trial endpoint for this plan.

## Deferred

### [2026-05-08] P2 Research cache versioning can fall back to `latest`

- **Market**: US, CN
- **Entry**: `backend/services/research_cache_service.py`
- **Current result**: `default_data_version()` returns `<market>:latest` when callers omit an as-of date.
- **Expected behavior**: Reusable research caches should be keyed by a stable data snapshot or explicit as-of/version identifier.
- **Validation standard**: Recomputing after a data refresh cannot reuse a stale feature matrix unless the data snapshot hash matches.
- **Research impact**: Cache speedups can silently trade away reproducibility.

### [2026-05-07] P1 Strict PIT macro replay and validated external data

- **Market**: Global auxiliary data
- **Entry**: `MacroDataService`, `DataQualityService`
- **Current result**: FRED is persisted and marked `research_grade`, but observations are from the current realtime window unless caller explicitly queries available data. `provider_capabilities.pit_supported=false`.
- **Expected behavior**: Historical realtime windows can be replayed by decision date, with release calendar, revision handling, and explicit availability timestamps.
- **Validation standard**: A backtest using macro features can prove that each decision date only sees observations available before that date.
- **Research impact**: Macro factors remain usable for exploratory research, but not for strict publication-grade PIT validation.

### [2026-05-07] P1 Corporate actions and survivorship-safe equity universe

- **Market**: US, CN
- **Entry**: `MarketDataFoundationService`, universe/materialization, backtest valuation
- **Current result**: Free providers expose current/free universe snapshots and daily bars. Capability metadata marks these as exploratory and not PIT.
- **Expected behavior**: Delistings, symbol changes, corporate actions, and historical membership are modeled as dated facts and enforced during universe materialization.
- **Validation standard**: Backtests over historical periods include delisted assets when eligible and exclude assets before listing.
- **Research impact**: Current long-horizon equity backtests can still contain survivorship bias.

### [2026-05-07] P2 Execution-grade backtest fill model

- **Market**: US, CN
- **Entry**: `StrategyGraph3Service.backtest_graph`, portfolio execution policy
- **Current result**: 3.0 StrategyGraph backtest reuses portfolio/order intent logic and close-to-close NAV valuation, but trade quantity, fill price, limit/suspend checks, and cost attribution are still minimal.
- **Expected behavior**: Execution policy converts order intents to fills using next-open rules, costs, lot size, suspend/ST/limit checks, and missing-price handling.
- **Validation standard**: Backtest trades reconcile to daily NAV and diagnostics explain unfilled or partially filled orders.
- **Research impact**: Strategy comparison is now possible, but production-grade execution diagnostics remain incomplete.

### [2026-05-07] P2 UI workflow for 3.0 backtest and data quality

- **Market**: US, CN
- **Entry**: React pages under `frontend/src/pages`
- **Current result**: REST/MCP/frontend API client expose provider capabilities and StrategyGraph backtest, but there is no dedicated UI workflow for configuring these screens.
- **Expected behavior**: UI can launch StrategyGraph backtests, inspect valuation warnings, and show provider quality/PIT warnings before promotion.
- **Validation standard**: `pnpm build` plus browser check for the new workflow.
- **Research impact**: Agents can use the feature immediately; human workflow still has friction.
