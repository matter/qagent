# QAgent Backlog

## Open

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
