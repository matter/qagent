# [2026-05-08] Previously mitigated backlog items preserved from docs/backlog.md

- **归档状态**：Preserved
- **归档来源**：`docs/backlog.md`
- **归档日期**：2026-05-08

This file preserves items that were already listed under `Completed / Mitigated` in `docs/backlog.md` before this repair pass. They were moved out so `docs/backlog.md` only tracks unresolved or deferred work.

## Preserved Items

### Task timeout/cancel late-result quarantine

- **Change**: `TaskExecutor` marks cancellation/timeout terminal state as authoritative, preserves late worker payload only under `late_result_diagnostics`, and exposes `late_result_quarantined=true` through REST/MCP/UI.
- **Recorded validation**: `uv run python -m unittest tests.test_task_executor_contracts -v`
- **Residual risk**: This prevents consumers from accepting late return payloads, but does not yet transactionally prevent domain writes performed inside the callable before return.

### QA evidence package gate

- **Change**: Promotion-like QA requires lineage artifacts and complete `metrics.evidence` with data-quality contract, PIT status, split policy, dependency snapshot, valuation diagnostics, artifact hashes, and reviewer decision.
- **Recorded validation**: `uv run python -m unittest tests.test_agent_research_3_service -v`
- **Residual risk**: Evidence content is structurally required, but deep semantic verification of PIT replay and valuation reconciliation remains separate work.

### Secret/config governance

- **Change**: Committed `config.yaml` no longer stores the FRED API key. Runtime can use `FRED_API_KEY` or ignored `config.local.yaml`.
- **Recorded validation**: `uv run python -m unittest tests.test_macro_data_config -v`
- **Residual risk**: Any key already committed in old git history should still be considered exposed and rotated outside this repo change.

### DB lock preflight

- **Change**: Added `/api/diagnostics/db-preflight`, `DbPreflightService`, and backup script fail-fast handling for locked/unavailable DuckDB.
- **Recorded validation**: `uv run python -m unittest tests.test_db_preflight tests.test_diagnostics_api_contracts -v`
- **Residual risk**: This improves diagnostics; it does not remove DuckDB single-writer constraints.

### Custom code static risk checks

- **Change**: Factor/strategy loaders reject obvious unsafe constructs such as `while True`, file IO calls, dunder access, and blocked system/network modules before `exec`.
- **Recorded validation**: `uv run python -m unittest tests.test_custom_code_safety -v`
- **Residual risk**: Static checks are not a sandbox. Full fix requires isolated worker processes with CPU/time/memory/filesystem/network limits.

### Human review workbench visibility

- **Change**: 3.0 Research Workbench shows data-quality/provider capabilities, PIT support counts, FRED macro series, and QA evidence blocking summary.
- **Recorded validation**: `cd frontend && pnpm build`
- **Residual risk**: Humans can inspect more context, but explicit approve/reject queues and reviewer assignment are not complete.
