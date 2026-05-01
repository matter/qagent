# QAgent V2.0 Acceptance Log

This file records milestone evidence for the V2.0 A-share and ranking upgrade.

## Final Acceptance Summary

- Branch: `V2.0`
- Current V2 scope: A-share support with BaoStock, explicit `market` isolation, ranking/listwise model objectives, and agent/human market-scope workflows.
- Backward compatibility rule: old REST/MCP/UI calls without `market` default to `US`.
- Pairwise scope: V2.0 exposes `pairwise` as a same-day candidate competition objective backed by LightGBM LambdaRank; model metadata records `pairwise_mode="lambdarank"`. This is not a separate pair-sampling learner.
- Final verification commands:
  - `uv run python scripts/v2_regression_check.py`
  - `uv run python scripts/e2e_demo.py`
  - `uv run python -m unittest discover tests`
  - `cd frontend && pnpm build`
  - Playwright browser smoke on `/market`, `/data`, and `/models`

## Final Acceptance Matrix

| Scenario | Status | Evidence |
|---|---|---|
| Existing US no-market API compatibility | Accepted | `scripts/v2_regression_check.py` checks `/api/data/status` and `/api/stocks/search?q=AAPL&limit=1` without `market`; both resolve to `US`. |
| Existing US full research loop | Accepted | `uv run python scripts/e2e_demo.py` passes factor evaluation, feature set reuse, model training, strategy backtest, and signal generation without explicit `market`. |
| Lossless DB upgrade safety | Accepted | P1 migration added `market='US'`, retained backup path, and validated row counts/null markets/duplicate target keys; Task 15 validates copied DBs through `--migration-copy`. |
| CN data update and bars | Accepted | BaoStock narrow update for `sh.600000` completed successfully; CN daily bars and `sh.000300` benchmark rows were stored with `market='CN'`. |
| CN factor evaluation | Accepted | CN factor compute/evaluation reads CN bars/cache only; cache evidence for `factor_values_cache` shows `market='CN'`. |
| CN feature set isolation | Accepted | CN feature set creation with US factors is rejected; CN factor references create CN feature sets. |
| CN ranking/listwise model | Accepted | Ranking dataset and LightGBM ranker tests pass; ranking model metadata includes `task="ranking"`, `objective_type`, `ndcg@k`, `rank_ic`, and top-k metrics. |
| CN backtest safety | Accepted | CN backtests reject US benchmark `SPY`; backtest price/benchmark paths filter by market. |
| CN signal generation | Accepted | CN signal generation queues and completes with CN strategy/group, persisting `signal_runs` and `signal_details` with `market='CN'`. |
| CN paper trading | Accepted | CN paper sessions persist `market='CN'`, reject US strategies, and support scoped advancement paths. |
| Agent workflow parity | Accepted | MCP tools expose `market`, default to `US`, return task polling metadata, and reject invalid markets with actionable errors. |
| Human UI validation | Accepted | Global market selector persists `US`/`CN`; pages remount on market switch; browser smoke confirms CN data/model pages and ranking metrics with `0` Ant Design warnings. |
| Refactor safety | Accepted | Provider registry and DuckDB values-table filters pass V2 regression, full unit discovery, and old US e2e. |

## Residual Non-Blocking Items

- Vite still reports the existing dynamic-import and large-chunk warnings during `pnpm build`.
- Python 3.14 UTC timestamp cleanup is accepted: backend code uses `backend.time_utils` instead of deprecated `datetime.utcnow()` calls, with a regression test preventing reintroduction.
- Default `scripts/v2_regression_check.py` skips optional `--migration-copy` and `--cn-provider-smoke` unless those flags are explicitly provided; skipped checks are reported clearly and do not hide required US compatibility failures.

## Final Task 17 Verification

- `uv run python scripts/v2_regression_check.py` passed with `4` passed, `0` failed, `2` skipped.
- `uv run python scripts/e2e_demo.py` passed through the full old US flow.
- `uv run python -m unittest discover tests` passed: `84` tests.
- `cd frontend && pnpm build` passed. Vite still reports the known dynamic-import and large-chunk warnings.
- `git diff --check` passed.
- Browser smoke with backend and Vite dev server:
  - `/market` first requested `/api/stocks/SPY/daily?...market=US`.
  - Switching to `CN` requested `/api/stocks/sh.600000/daily?...market=CN`.
  - `/data` requested CN data status and displayed `最近交易日`.
  - `/models` exposed the `listwise 列表排序` objective and showed `NDCG@5` plus `Rank IC`.
  - Console capture found `0` Ant Design warnings.
  - Screenshots saved at `logs/v2-task17-market-cn.png` and `logs/v2-task17-models-cn.png`.

## P0 Baseline Safety

- Branch: `V2.0`
- Baseline commit before V2 implementation: `712daaf`
- Data backup: `data/backup/20260429_225326`
- Schema snapshot: `data/backup/20260429_225326/schema_snapshot.json`
- Database backup contents:
  - `855,600,549` table rows exported to parquet
  - `225` model directories copied
- Baseline issue found before V2 schema/code changes:
  - `scripts/e2e_demo.py` failed at signal generation with `UnboundLocalError` in `SignalService._validate_dependency_chain`.
  - Root cause: local `from backend.services.strategy_service import StrategyService` inside the function shadowed the module-level import.
  - Fix: removed the local import and added a signal contract regression test.
- Verification after fix:
  - `uv run python -m unittest tests.test_signal_contracts.SignalServiceContractTests.test_dependency_validation_uses_imported_strategy_service` passed.
  - `uv run python -m unittest tests.test_signal_contracts` passed.
  - `uv run python scripts/e2e_demo.py` passed through factor evaluation, feature set creation, model training, backtest, and signal generation.

## P1 Market Foundation And DB Upgrade

- Market context:
  - Added `backend/services/market_context.py`.
  - Added `markets.US` and `markets.CN` config blocks while preserving legacy `data.provider`, `market.calendar`, and `backtest.default_benchmark` fallbacks.
  - Verification:
    - `uv run python -m unittest tests.test_market_context` passed.
    - Market context smoke printed `market context ok`.
- Schema migration:
  - Added `schema_migrations` audit table.
  - Added market-aware DDL for new databases.
  - Existing DB migration uses conservative additive `market='US'` backfill and validation before later service-level market filtering work.
  - Copy migration report: `data/backup/20260429_225326/migration_copy_report.json`
  - Working DB migration report: `data/backup/20260429_225326/migration_working_report.json`
  - Working DB migration status: `applied`
  - Sample validation after working DB migration:
    - `stocks`: `12,056` rows, `market` present, `0` null market values, `0` duplicate target key groups.
    - `daily_bars`: `17,752,013` rows, `market` present, `0` null market values, `0` duplicate target key groups.
    - `factor_values_cache`: `837,838,253` rows, `market` present, `0` null market values, `0` duplicate target key groups.
    - `models`: `215` rows, `market` present, `0` null market values, `0` duplicate target key groups.
    - `signal_runs`: `73` rows, `market` present, `0` null market values, `0` duplicate target key groups.
- Old system regression after migration:
  - `uv run python -m unittest tests.test_schema_migrations tests.test_market_context tests.test_signal_contracts` passed.
  - `GET /api/health` returned `{"status": "ok"}`.
  - `GET /api/stocks/search?q=AAPL&limit=3` returned AAPL results through the old no-market API path.
  - `uv run python scripts/e2e_demo.py` passed through the full US demo flow.
  - `cd frontend && pnpm build` passed.
  - Direct schema check after stopping backend printed `market schema ok`.

## P2 Provider, Calendar, Data, And Groups

- Provider and calendar foundation:
  - Added provider registry with `US -> yfinance` and `CN -> baostock`.
  - Added `BaoStockProvider` for A-share stock list, daily bars, and index bars.
  - Made trading calendar helpers market-aware while preserving old US signatures.
  - Verification:
    - `uv run python -m unittest tests.test_provider_contracts tests.test_calendar_contracts` passed.
    - Real BaoStock smoke for `sh.600000` returned daily bars with `market='CN'`.
- Data and group market scope:
  - Data status, updates, ticker search, daily bars, and quality checks accept `market`, defaulting to `US`.
  - Data upserts now persist `market` for `stocks`, `daily_bars`, and `index_bars`.
  - Group APIs and service methods accept `market`; group membership rows include market.
  - Existing US group IDs keep working; added `us_all_market`, `us_sp500`, `us_nasdaq100`, `cn_all_a`, `cn_sz50`, `cn_hs300`, `cn_zz500`, `cn_chinext`, and `cn_a_core_indices_union`.
  - CN default group is `cn_a_core_indices_union`, the de-duplicated union of 上证50、沪深300、中证500、创业板指 constituents.
  - CN market-level data update and stock-list refresh use `cn_a_core_indices_union` as the default universe; BaoStock full A list is not used as the default download scope.
  - Manual group validation rejects unambiguous cross-market tickers, while CN tickers keep BaoStock-native form such as `sh.600000`.
  - Added regression coverage in `tests/test_data_group_market_scope.py`.
- CN narrow-path evidence:
  - `POST /api/data/update/tickers` with `{"market":"CN","tickers":["sh.600000"]}` completed task `8759e76878bb45c3a566c285094cb087` with result `total=1`, `success=1`, `failed=0`.
  - `GET /api/stocks/sh.600000/daily?market=CN&start=2024-01-02&end=2024-01-03` returned two CN daily bars.
  - Direct BaoStock benchmark load wrote `7` `index_bars` rows for `market='CN'`, `symbol='sh.000300'`.
  - `cn_all_a` refresh returned `member_count=1` with `sh.600000`.
  - `GET /api/stocks/search?q=600000&market=CN&limit=3` returned `sh.600000`.
- Old system regression after P2:
  - `GET /api/health` returned `{"status":"ok"}`.
  - `GET /api/stocks/search?q=AAPL&limit=3` returned US AAPL results through the old no-market path.
  - `uv run python scripts/e2e_demo.py` passed after CN rows were added.
  - `uv run python -m unittest discover tests` passed: `41` tests.
  - `cd frontend && pnpm build` passed. Vite reported the existing large-chunk and dynamic-import warnings.

## P3 Research Asset Isolation

- Label scope completed:
  - Label definitions now carry `market`; old requests default to `US`.
  - Label list/detail/update/delete and create APIs accept `market`.
  - Label computation reads `daily_bars` and `index_bars` within the label market only.
  - CN excess-return labels reject US benchmarks such as `SPY`.
  - CN presets use `cn_` id/name prefixes to avoid legacy single-column `name` unique constraints in upgraded databases.
  - Verification:
    - `uv run python -m unittest tests.test_label_market_scope` passed.
    - `GET /api/labels?market=CN` returned `26` CN presets.
    - `POST /api/labels` with `market=CN`, `target_type=excess_return`, `benchmark=SPY` returned HTTP `400`.
    - `uv run python scripts/e2e_demo.py` passed through the old US label-dependent factor evaluation and model-training flow.
    - `uv run python -m unittest discover tests` passed: `46` tests.
    - `cd frontend && pnpm build` passed with the existing Vite warnings.
- Factor and feature scope completed:
  - Factor definitions, factor cache rows, factor evaluation results, and feature sets now carry `market`; old no-market calls default to `US`.
  - Factor compute and bulk-cache reads filter `daily_bars` and `factor_values_cache` by market.
  - Feature sets validate that all referenced factors belong to the same market.
  - Factor evaluation resolves groups, factors, labels, and persisted results within one market.
  - REST factor/feature endpoints accept `market`; factor MCP list/create/evaluate tools accept `market` for the same service-layer path.
  - Frontend API types and clients expose the new factor/feature market fields without changing existing UI defaults.
  - Regression coverage added in `tests/test_factor_feature_market_scope.py`.
- P3 verification:
  - `uv run python -m unittest tests.test_data_group_market_scope tests.test_label_market_scope tests.test_factor_feature_market_scope` passed: `17` tests.
  - `uv run python -m unittest discover tests` passed: `53` tests.
  - Real API smoke with backend on `127.0.0.1:8000`:
    - `GET /api/health` returned `{"status":"ok"}`.
    - `GET /api/factors?market=US` returned `377` US factors.
    - `GET /api/factors?market=CN` returned `119` CN factors.
    - CN feature set creation with a US factor returned HTTP `400`.
    - CN feature set creation with a CN factor returned `market="CN"` and was deleted successfully.
    - CN custom factor compute for `cn_all_a`, `2024-01-02` to `2024-01-10`, completed task `fd7401d00d2f49d68fad202bf516c2e5` with shape `[7, 1]`.
    - Direct cache check for factor `f400ffff09bb` returned `[("CN", 7)]` from `factor_values_cache`.
  - Old system regression:
    - `uv run python scripts/e2e_demo.py` passed through factor evaluation, feature set creation, model training, backtest, and signal generation.
    - `cd frontend && pnpm build` passed. Vite reported the existing large-chunk and dynamic-import warnings.

## P4 Model Foundation And Ranking Objectives

- Model market scope completed:
  - Model records and metadata now carry `market`; old train/list/detail requests default to `US`.
  - Training resolves universe groups, feature sets, labels, factor features, and label values in one market.
  - Training date snapping uses the selected market calendar.
  - Prediction paths use the model record market and market-scoped feature computation.
  - Frontend model API types include market-aware train/list/detail/delete fields.
  - Regression coverage added in `tests/test_model_market_scope.py`.
- Task 8 verification:
  - `uv run python -m unittest tests.test_model_market_scope` passed: `2` tests.
  - `uv run python -m unittest discover tests` passed: `55` tests.
  - `GET /api/models?market=US` returned `222` US models.
  - `GET /api/models/ae5e994c4195?market=US` returned `market="US"`.
  - `uv run python scripts/e2e_demo.py` passed through old US model training, backtest, and signal generation.
  - `cd frontend && pnpm build` passed with the existing Vite warnings.
- Ranking objectives completed:
  - Added date-grouped ranking dataset builder in `backend/services/ranking_dataset.py`.
  - `LightGBMModel(task="ranking")` now uses `LGBMRanker` with query group sizes.
  - `objective_type="ranking"` and `objective_type="listwise"` use native LambdaRank ranking.
  - `objective_type="pairwise"` is accepted as a V2.0 user-facing objective and records `pairwise_mode="lambdarank"`.
  - Ranking metrics include `ndcg@k`, `top_k_mean_label`, `rank_ic_mean`, and `pairwise_accuracy_sampled`.
  - Prediction output remains per-ticker scores for existing strategy sorting.
- P4 verification:
  - `uv run python -m unittest tests.test_ranking_dataset tests.test_model_market_scope` passed: `6` tests.
  - `uv run python -m unittest discover tests` passed: `59` tests.
  - `uv run python -m py_compile backend/services/ranking_dataset.py backend/services/model_service.py backend/models/lightgbm_model.py backend/api/models.py` passed.
  - Real API ranking smoke completed task `aa78ba2d983e4a3e8b4fac6ea4486d8a`.
  - Ranking smoke model `dfb45466926f` returned `market="US"`, `task="ranking"`, `objective_type="ranking"`, `test_ndcg@5=0.64652`, `test_rank_ic_mean=0.124884`, and `test_pairwise_accuracy_sampled=0.5245`.
  - `uv run python scripts/e2e_demo.py` passed through old US model training, backtest, and signal generation after ranking support was added.
  - `cd frontend && pnpm build` passed with the existing Vite warnings.

## P5 Full Research Loop Safety

- Task 10 strategy and backtest scope completed:
  - Strategy records now carry `market`; old strategy CRUD calls default to `US`.
  - Strategy versioning is scoped by `(market, name)`.
  - Strategy dependency validation rejects concrete model IDs from another market and rejects factor names that only exist in another market.
  - Backtest config, persisted result rows, list/detail responses, and result summaries now include `market`.
  - Backtest price, benchmark, factor-cache, factor-compute, model-prediction, and leakage-check paths now pass the same market through the shared service layer.
  - Backtest engine price and benchmark queries filter by `market` and use parameterized ticker lists.
  - CN backtests with US-style benchmarks such as `SPY` are rejected before a background task is queued.
  - Frontend strategy/backtest API types accept optional `market` while preserving current US defaults.
- Task 10 verification:
  - `uv run python -m unittest tests.test_strategy_backtest_market_scope` passed: `7` tests.
  - `uv run python -m unittest discover tests` passed: `66` tests.
  - `uv run python -m py_compile backend/services/strategy_service.py backend/services/backtest_service.py backend/services/backtest_engine.py backend/api/strategies.py` passed.
  - API smoke with backend on `127.0.0.1:8000`:
    - `GET /api/strategies` returned HTTP `200` through the old no-market US path.
    - Temporary CN strategy and CN group creation returned `market="CN"`.
    - `POST /api/strategies/{cn_strategy_id}/backtest` with `market="CN"` and `benchmark="SPY"` returned HTTP `400` with a benchmark-market validation message.
    - Temporary CN smoke assets were deleted after the check.
  - Old system regression:
    - `uv run python scripts/e2e_demo.py` passed through US factor evaluation, feature set reuse, model training, strategy backtest, and signal generation.

- Task 14 frontend page workflows completed:
  - Human validation pages now remount on global market switch, so CN/US page state does not leak across the selected market scope.
  - Market Browser defaults to `SPY` for `US` and `sh.600000` for `CN`, passes `market` through search, daily-bar, and update calls, and labels the selected market in the UI.
  - Data Management shows market badges, latest trading day, scoped data status, scoped group actions, and CN ticker placeholders.
  - Factor, feature, model, strategy, backtest, signal, and paper-trading lists show market badges and call detail/delete/export APIs with the row's market.
  - Model training exposes `regression`, `classification`, `ranking`, `pairwise`, and `listwise` objectives; ranking objectives send `ranking_config` with date query groups, `eval_at`, and minimum group size.
  - Model list/detail views expose `NDCG@5`, `Rank IC`, `ndcg@*`, `rank_ic`, `top_*_mean_label`, and `pairwise_accuracy` metrics for ranking/listwise acceptance.
  - Removed Ant Design deprecated `bodyStyle`, `Spin tip`, `Space direction`, `Modal destroyOnClose`, `Statistic valueStyle`, and `Input addonBefore` usage from frontend source to keep browser validation logs actionable.
- Task 14 verification:
  - `uv run python -m unittest tests.test_frontend_market_scope_contracts` passed: `2` tests.
  - `cd frontend && pnpm build` passed. Vite still reports the existing dynamic-import and large-chunk warnings.
  - `uv run python -m unittest discover tests` passed: `78` tests.
  - `uv run python scripts/e2e_demo.py` passed through the old US flow while backend/frontend servers were running.
  - Browser smoke with backend and Vite dev server:
    - First load of `/market` requested `/api/stocks/SPY/daily?...market=US`.
    - Switching to `CN` requested `/api/stocks/sh.600000/daily?...market=CN`.
    - `/data` requested `/api/data/status?market=CN` and displayed `数据概览` plus `最近交易日`.
    - `/models` displayed `学习目标`, `regression 回归`, `NDCG@5`, and `Rank IC`.
    - Playwright console capture found `0` Ant Design warnings.
    - Screenshots saved at `logs/v2-task14-market-cn.png`, `logs/v2-task14-data-cn.png`, and `logs/v2-task14-models-cn.png`.
  - Old system regression:
    - `GET /api/health` remained available through the old server path.
    - `uv run python scripts/e2e_demo.py` passed with no explicit `market` arguments, confirming `US` default compatibility.

## P7 Regression, Refactor, And Final Acceptance

- Task 15 V2 regression check script completed:
  - Added `scripts/v2_regression_check.py` as the repeatable phase gate for V2.0.
  - The script reports `passed`, `failed`, and `skipped` checks with a non-zero exit code only when required checks fail.
  - Default checks cover:
    - Market context defaults and aliases, including missing market -> `US`.
    - Old REST API calls without `market`: `/api/data/status` and `/api/stocks/search?q=AAPL&limit=1`.
    - Simulated BaoStock/CN provider failure isolation followed by a live US API status check.
    - Old US e2e flow through `scripts/e2e_demo.py`.
  - Optional checks cover:
    - `--migration-copy PATH` validation on a copied DuckDB file.
    - `--cn-provider-smoke` BaoStock network smoke for `sh.600000`.
  - CN optional checks report `skipped` with a clear reason when not requested or unavailable.
- Task 15 verification:
  - Red/green contract check:
    - `uv run python -m unittest tests.test_v2_regression_check` failed before the script existed.
    - The same command passed after implementation: `4` tests.
  - `uv run python -m py_compile scripts/v2_regression_check.py` passed.
  - Lightweight script smoke:
    - `uv run python scripts/v2_regression_check.py --skip-us-e2e --json` passed with `3` passed, `0` failed, `3` skipped.
  - Migration-copy branch smoke on a synthetic DuckDB file:
    - `uv run python scripts/v2_regression_check.py --source-db logs/v2-task15-migration-source.duckdb --migration-copy logs/v2-task15-migration-copy.duckdb --skip-us-e2e --json` passed.
    - Migration status was `applied`; `2` tables were checked.
  - Default regression command:
    - `uv run python scripts/v2_regression_check.py` passed with `4` passed, `0` failed, `2` skipped.
    - The skipped checks were the optional migration-copy and BaoStock network checks.
  - Submission gate:
    - `uv run python -m unittest discover tests` passed: `82` tests.
    - `cd frontend && pnpm build` passed. Vite still reports the existing dynamic-import and large-chunk warnings.
    - `git diff --check` passed.

- Task 16 bounded refactoring/performance pass completed:
  - Provider resolution now uses a registry factory map with `register_provider()` and `available_providers()`, so adding future data providers does not require branching inside `get_provider()`.
  - Added `backend/services/sql_filters.py` with `registered_values_table()` for large DuckDB value filters.
  - Replaced large dynamic ticker/factor `IN (...)` filters with registered values-table joins in:
    - Factor cache bulk loads.
    - Factor cache coverage checks.
    - Factor cached-value loads.
    - Factor compute warm-up price loads.
    - Backtest engine price loads.
    - Signal service price loads.
    - Paper-trading batch price loads and preload price cache.
  - Left small current-position lookup paths on parameterized `IN` because tests use a lightweight fake connection and those lists are bounded by current positions rather than full universes.
- Task 16 verification:
  - Red/green contract checks:
    - `uv run python -m unittest tests.test_sql_filters tests.test_provider_contracts` failed before `registered_values_table()`, `register_provider()`, and `available_providers()` existed.
    - The same command passed after implementation.
  - Focused regression:
    - `uv run python -m unittest tests.test_signal_paper_market_scope tests.test_paper_trading_contracts tests.test_signal_contracts tests.test_backtest_engine_contracts tests.test_strategy_backtest_market_scope tests.test_factor_feature_market_scope` passed: `36` tests.
    - `uv run python -m py_compile backend/services/signal_service.py backend/services/paper_trading_service.py backend/services/factor_engine.py backend/services/backtest_engine.py backend/services/sql_filters.py` passed.
  - V2 regression:
    - `uv run python scripts/v2_regression_check.py` passed with `4` passed, `0` failed, `2` skipped.
  - Submission gate:
    - `uv run python -m unittest discover tests` passed: `84` tests.
    - `cd frontend && pnpm build` passed. Vite still reports the existing dynamic-import and large-chunk warnings.
    - `git diff --check` passed.
  - Follow-up recorded:
    - Python 3.14 UTC timestamp cleanup is tracked in `docs/backlog.md` as completed technical debt; backend code no longer calls deprecated `datetime.utcnow()` directly.

- Task 13 frontend API market scope and global selector completed:
  - Added shared frontend `Market = "US" | "CN"` type and market helper exports.
  - Added API client market state with `qagent.market` localStorage persistence, first-load default `US`, and request interceptor injection for market-scoped REST paths.
  - Added `MarketScopeSelector` to the global header using the existing Ant Design dark layout.
  - Preserved explicit per-call market override support for data, group, label, factor, feature, model, strategy, signal, and paper-trading API helpers.
  - Logged existing AntD deprecation warnings from `/market` to `docs/backlog.md` for Task 14 cleanup.
- Task 13 verification:
  - Red/green contract check:
    - `uv run python -m unittest tests.test_frontend_market_scope_contracts` failed before implementation because the shared frontend market contract was missing.
    - The same command passed after implementation: `1` test.
  - `cd frontend && pnpm build` passed. Vite reported the existing large-chunk and dynamic-import warnings.
  - `uv run python -m unittest discover tests` passed: `77` tests.
  - API smoke with backend on `127.0.0.1:8000`:
    - `GET /api/health` returned `{"status":"ok"}`.
    - `GET /api/data/status` returned `market="US"` through the old no-market path.
    - `GET /api/stocks/search?q=AAPL&limit=1` returned AAPL with `market="US"`.
  - Browser smoke with backend and Vite dev server:
    - Opened `http://127.0.0.1:5173/market`.
    - Header selector displayed `Market`, `US`, and `CN`.
    - First load used `US` default and the initial chart request included `market=US`.
    - Switching to `CN` saved `qagent.market="CN"` in localStorage and showed the `A股` tag.
    - Searching `600000` after switching sent `/api/stocks/search` with `market=CN`.
    - Screenshot saved at `logs/v2-task13-market-selector.png`.
  - Old system regression:
    - `uv run python scripts/e2e_demo.py` passed through US factor evaluation, feature set reuse, model training, strategy backtest, and signal generation.
    - `cd frontend && pnpm build` passed. Vite reported the existing large-chunk and dynamic-import warnings.

- Task 11 signal and paper-trading scope completed:
  - Signal generation and diagnosis now accept `market`, default to `US`, and resolve strategies, groups, factors, feature sets, models, price data, backtest replay state, signal runs, and signal details within one market.
  - Signal run/detail persistence stores `market`, and list/detail/export queries are market-scoped.
  - Paper trading sessions, daily snapshots, signal cache, price preload, latest-signal preview, stock chart, positions, trades, summary, and backtest comparison paths are market-scoped.
  - Paper session creation rejects strategies/groups from another market through the shared service layer.
  - Paper-trading API request/response contracts and frontend API client types expose optional `market` while preserving old no-market `US` defaults.
  - Existing paper-trading contract tests were updated to assert the new `US` default market scope.
- Task 11 verification:
  - `uv run python -m unittest tests.test_signal_paper_market_scope` passed: `5` tests.
  - `uv run python -m unittest tests.test_paper_trading_contracts tests.test_signal_paper_market_scope` passed: `15` tests.
  - `uv run python -m unittest discover tests` passed: `71` tests.
  - `uv run python -m py_compile backend/services/paper_trading_service.py backend/api/paper_trading.py backend/services/signal_service.py backend/api/signals.py` passed.
  - `cd frontend && pnpm build` passed. Vite reported the existing large-chunk and dynamic-import warnings.
  - API smoke with backend on `127.0.0.1:8000`:
    - `GET /api/signals` returned HTTP `200` through the old no-market US path.
    - `GET /api/paper-trading/sessions` returned HTTP `200` through the old no-market US path.
    - Temporary CN strategy creation returned `market="CN"`.
    - `POST /api/signals/generate` with `market="CN"`, `strategy_id=<cn_strategy>`, and `universe_group_id="cn_all_a"` queued task `f7eae9b2837f47c5a6e5cca03389c053`; task completed successfully.
    - Temporary CN paper session creation returned `market="CN"`.
    - Temporary CN paper session and strategy were deleted after the check.
  - Old system regression:
    - `uv run python scripts/e2e_demo.py` passed through US factor evaluation, feature set reuse, model training, strategy backtest, and signal generation.

## P6 Agent And Human Workflows

- Task 12 MCP and agent-facing contracts completed:
  - MCP data, group, factor, label, feature-set, model, strategy, backtest, signal, and paper-trading tools now accept `market`, default missing values to `US`, and use the same service-layer market scope as REST routes.
  - Long-running MCP tools return `task_id`, `task_type`, `market`, `asset_scope`, and `poll_url` so agents can poll `/api/tasks/{task_id}` without inferring state from logs.
  - MCP model training exposes `objective_type` and `ranking_config` for `ranking`, `pairwise`, and `listwise` objectives.
  - Invalid MCP market input is normalized into an actionable `Invalid MCP request` error with allowed values `US, CN`.
  - Added MCP tools for market-scoped labels, feature sets, strategies, and paper-trading sessions.
  - Updated `AGENTS.md`, `docs/agent-guide.md`, and `docs/backlog.md` with V2 market-scope rules, ranking/listwise notes, CN examples, and issue-board workflow.
- Task 12 verification:
  - `uv run python -m unittest tests.test_mcp_market_contracts` passed: `5` tests.
  - `uv run python -m py_compile backend/mcp_server.py` passed.
  - `uv run python -m backend.mcp_server --help` exited `0`.
  - `uv run python -m unittest discover tests` passed: `76` tests.
  - `cd frontend && pnpm build` passed. Vite reported the existing large-chunk and dynamic-import warnings.
  - API smoke with backend on `127.0.0.1:8000`:
    - `GET /api/health` returned `{"status":"ok"}`.
    - `GET /api/signals` returned HTTP `200` and US signal rows through the old no-market path.
    - `GET /api/paper-trading/sessions` returned HTTP `200` and US sessions through the old no-market path.
  - MCP schema smoke after stopping the backend:
    - `mcp.list_tools()` returned `24` tools.
    - Market-scoped MCP tools had no missing `market` input schema fields.
    - `search_stocks(query="AAPL", limit=1)` returned `market="US"` through the old no-market call shape.
    - `search_stocks(query="AAPL", market="HK")` raised `Invalid MCP request: market must be one of US, CN...`.
  - Old system regression:
    - `uv run python scripts/e2e_demo.py` passed through US factor evaluation, feature set reuse, model training, strategy backtest, and signal generation.
