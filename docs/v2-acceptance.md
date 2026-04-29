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
