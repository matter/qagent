# QAgent V2.0 Acceptance Log

This file records milestone evidence for the V2.0 A-share and ranking upgrade.

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
  - Existing US group IDs keep working; added `us_all_market`, `us_sp500`, `us_nasdaq100`, `cn_all_a`, and `cn_hs300`.
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
- Remaining P4 work:
  - Add ranking, pairwise, and listwise training objectives.
