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

