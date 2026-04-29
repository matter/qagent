# QAgent V2.0 A-Share Market Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add A-share support with BaoStock while keeping the existing US-equity research loop working, and add same-day candidate competition model objectives (`ranking`, `pairwise`, `listwise`) for cross-sectional stock selection.

**Architecture:** Use option B: one shared backend service layer and one shared DuckDB schema family with an explicit `market` dimension. REST, MCP, and React UI must all call the same market-aware services. Market isolation is enforced by service validation and database keys, not by duplicating the whole codebase.

**Tech Stack:** FastAPI, DuckDB, Pandas, LightGBM, BaoStock, yfinance, exchange_calendars, React, TypeScript, Ant Design, ECharts, MCP server.

---

## Product Positioning

QAgent remains a local-first, single-user, low-frequency quantitative research system. It is built for one human working with agents: agents operate through REST/MCP, humans validate through visual and quantitative UI. V2.0 should improve that workflow, not turn the system into a multi-user enterprise platform or broker execution system.

## Core Decisions

- Market values: use `US` and `CN`.
- Existing rows without market are migrated to `US`.
- A-share ticker storage: store BaoStock-native codes such as `sh.600000` and `sz.000001`; use `market` to avoid ambiguity. UI can display exchange/name aliases.
- A-share data source: BaoStock first, with adapter boundaries that allow another provider later.
- A-share default benchmark: `sh.000300` (CSI 300) unless the user changes it later.
- A-share first stage: full research loop, not data browsing only.
- Ranking first stage: implement LightGBM listwise/ranking support using query groups by trade date. Pairwise is exposed as a training objective alias to the same ranking engine first, then extended with explicit pair sampling only if needed.
- Refactoring scope: only refactor modules touched by market support, ranking support, or UI/agent workflow improvements. Avoid broad rewrites that do not unblock V2.0.

## Pairwise Scope For V2.0

V2.0 treats `pairwise` as a user-facing objective for same-day candidate competition, backed by LightGBM LambdaRank in the first implementation. This is acceptable for V2.0 if responses and model metadata clearly report `pairwise_mode="lambdarank"`.

Do not claim V2.0 has a separate true pair-sampling learner unless an explicit pairwise dataset builder, pair sampling policy, estimator/loss choice, and red-green verification are added. If strict pairwise learning becomes required, split it into a V2.1 task after listwise ranking is accepted.

## Non-Goals

- Do not add live broker trading.
- Do not add multi-user auth or permissions.
- Do not add minute/tick data.
- Do not duplicate all A-share logic into separate services if a market-aware shared service can handle it.
- Do not rewrite all frontend pages for visual polish before the underlying data contracts are stable.

## Required Isolation Rules

- Every data, asset, and result lookup that can cross markets must include `market`.
- A model cannot train on a feature set or label from another market.
- A strategy cannot depend on factors/models from another market.
- A backtest, signal run, or paper-trading session cannot mix a CN universe with a US benchmark or US model.
- Default API behavior remains `market=US` for backward compatibility.
- MCP tools must expose the same market fields as REST endpoints.

## Backward Compatibility Contract

These rules are hard gates for the whole V2.0 branch:

- Existing REST requests that do not pass `market` must continue to behave as `market=US`.
- Existing MCP calls must either keep their old shape with default `US`, or expose a compatible wrapper that fills `market=US`.
- Existing model, factor, feature, strategy, backtest, signal, and paper-trading records must remain readable after migration.
- Existing model files under `data/models` must not be rewritten by schema migration. Metadata can be lazily backfilled only when the model is loaded or retrained.
- Existing US groups keep working. If group IDs need to change, create aliases or migration rows so old API references still resolve.
- US yfinance update, US factor evaluation, US model training, US backtest, US signal generation, and US paper trading are the regression suite. A task is not complete if any of these regress.
- CN provider or network failures must not block startup, US data updates, or US research workflows.
- UI defaults to `US` on first load so existing human workflows do not change unless the user switches markets.

## Database Lossless Upgrade Contract

The database migration must be treated as a data-preservation task, not a schema-only edit.

- Before any schema rewrite, create a timestamped backup with `scripts/backup_data.sh` or an equivalent DuckDB file copy while the backend is stopped.
- Add a `schema_migrations` table with migration ID, applied time, code version, preflight summary, and validation summary.
- Support `dry_run=True` or a standalone migration validation command that runs against a copied DuckDB file.
- Never drop or overwrite an original table until the replacement table has passed row-count, primary-key, non-null, and checksum validation.
- For primary-key changes, use shadow-table migration:
  1. Create `table__v2` with the new schema.
  2. Insert all old rows with `market='US'`.
  3. Validate row count equals the original.
  4. Validate key uniqueness under the new key.
  5. Validate selected column checksums or aggregate fingerprints.
  6. Rename the old table to `table__backup_<migration_id>`.
  7. Rename `table__v2` to the original table name.
  8. Keep backup tables until final V2 acceptance passes.
- If uniqueness collisions appear after adding `market`, abort migration and write a report. Do not silently deduplicate.
- If any migration step fails, the app must still be able to open the pre-migration backup.
- Migration validation must include both empty/new DB initialization and upgrade from an existing V1/V1.5 DB.

Suggested validation aggregates per table:

```sql
SELECT COUNT(*) FROM <table>;
SELECT COUNT(*) FROM <table> WHERE market IS NULL;
SELECT COUNT(DISTINCT <new_primary_key_columns>) FROM <table>;
SELECT SUM(HASH(<stable_business_columns>)) FROM <table>;
```

Use DuckDB-compatible expressions when implementing the actual checks.

## Phase Milestone Gates

Each phase must finish with a visible, testable milestone. Do not start the next phase if the gate fails.

| Phase | Tasks | Milestone | Required evidence |
|---|---:|---|---|
| P0 Baseline safety | 0 | Branch, data, and US baseline are known before V2 changes | `git status --short --branch`; DB backup path; pre-change `uv run python scripts/e2e_demo.py` result or documented reason it cannot run locally |
| P1 Market foundation + DB upgrade | 1-2 | Existing DB upgrades losslessly and new DB initializes cleanly | Migration dry-run on copied DB; row-count/checksum report; `market='US'` on all old rows; old no-market API calls still work |
| P2 Provider, calendar, data, groups | 3-5 | US data path remains unchanged; CN has a narrow BaoStock data path | US data status/update smoke; CN one-ticker daily bars; CN benchmark bars; group membership scoped by market |
| P3 Research asset isolation | 6-7 | Labels, factors, and features are market-scoped | US factor/feature e2e; CN factor cache rows include `market='CN'`; cross-market dependency attempt is rejected |
| P4 Model foundation + ranking objectives | 8-9 | Models are market-scoped before ranking is added | one old regression/classification model training still passes; ranking dataset unit check; one small ranking model saves `task_type=ranking` |
| P5 Full research loop safety | 10-11 | Backtest, signals, and paper trading are market-safe | US backtest/signal/paper smoke; CN workflow smoke where data exists; CN workflow with US benchmark/model rejected |
| P6 Agent and human workflows | 12-14 | REST, MCP, and UI expose the same market scope | MCP schema check; `cd frontend && pnpm build`; manual browser check for market switching and ranking metrics |
| P7 Regression, refactor, and final acceptance | 15-17 | Refactors did not change behavior and V2 is acceptably documented | `uv run python scripts/v2_regression_check.py`; `uv run python scripts/e2e_demo.py`; `cd frontend && pnpm build`; `docs/v2-acceptance.md` completed |

The milestone reports should be appended to `docs/v2-acceptance.md` as phases complete.

## Planned File Map

### Backend Foundation

- Modify: `backend/db.py`
  - Add schema migration helpers and `market` columns/keys.
  - Preserve existing data by backfilling `US`.
- Create: `backend/services/market_context.py`
  - Normalize market values, default market, benchmark defaults, and validation helpers.
- Modify: `backend/config.py`
  - Add per-market provider, calendar, benchmark, and default group settings.
- Modify: `config.yaml`
  - Add `markets.US` and `markets.CN` blocks.
- Create: `backend/providers/registry.py`
  - Resolve provider by market and provider name.
- Modify: `backend/providers/base.py`
  - Add provider metadata and optional market capabilities while keeping the current provider protocol simple.

### BaoStock And Calendar

- Create: `backend/providers/baostock_provider.py`
  - Implement login/logout lifecycle, stock list, daily bars, and index data.
- Modify: `backend/providers/yfinance_provider.py`
  - Make market metadata explicit, no behavior change for US.
- Modify: `backend/services/calendar_service.py`
  - Make all calendar functions market-aware.
  - US uses NYSE semantics; CN uses XSHG/XSHZ or BaoStock trade dates.

### Market-Aware Services

- Modify: `backend/services/data_service.py`
- Modify: `backend/services/group_service.py`
- Modify: `backend/services/label_service.py`
- Modify: `backend/services/factor_service.py`
- Modify: `backend/services/factor_engine.py`
- Modify: `backend/services/factor_eval_service.py`
- Modify: `backend/services/feature_service.py`
- Modify: `backend/services/model_service.py`
- Modify: `backend/services/strategy_service.py`
- Modify: `backend/services/backtest_service.py`
- Modify: `backend/services/backtest_engine.py`
- Modify: `backend/services/signal_service.py`
- Modify: `backend/services/paper_trading_service.py`
- Modify: `backend/mcp_server.py`

### Ranking Objectives

- Modify: `backend/api/models.py`
  - Add `objective_type`, `ranking_config`, and metric fields to train requests/responses.
- Modify: `backend/models/base.py`
  - Keep prediction interface stable; add optional fit kwargs for ranking groups.
- Modify: `backend/models/lightgbm_model.py`
  - Add `ranking` task using `lightgbm.LGBMRanker`.
- Create: `backend/services/ranking_dataset.py`
  - Build date-based query groups, validate labels, and prepare ranking metrics inputs.
- Modify: `backend/services/model_service.py`
  - Route regression/classification/ranking training through clearer dataset builders.

### REST API And Frontend

- Modify: `backend/api/data.py`
- Modify: `backend/api/groups.py`
- Modify: `backend/api/labels.py`
- Modify: `backend/api/factors.py`
- Modify: `backend/api/features.py`
- Modify: `backend/api/models.py`
- Modify: `backend/api/strategies.py`
- Modify: `backend/api/signals.py`
- Modify: `backend/api/paper_trading.py`
- Modify: `frontend/src/api/index.ts`
- Modify: `frontend/src/api/client.ts`
- Create: `frontend/src/components/MarketScopeSelector.tsx`
- Modify: `frontend/src/App.tsx`
- Modify: pages under `frontend/src/pages/`

### Documentation And Acceptance

- Modify: `AGENTS.md`
- Modify or append: `docs/agent-guide.md`
- Modify or append: `docs/backlog.md`
- Create: `docs/v2-acceptance.md`
- Modify: `scripts/e2e_demo.py`
  - Keep existing US e2e coverage and add focused CN smoke hooks when BaoStock is available.
- Create: `scripts/v2_regression_check.py`
  - Provide a repeatable per-phase regression gate for US compatibility, migration validation, and optional CN smoke checks.

## Task 0: Preserve Current Branch State

**Files:**
- No code changes.
- Create later in Task 17: `docs/v2-acceptance.md`

- [ ] Run `git status --short --branch`.
- [ ] Confirm branch is `V2.0`.
- [ ] Note pre-existing unrelated changes before staging anything.
- [ ] Do not stage existing unrelated files unless the user asks.
- [ ] Stop backend before taking any DB backup or schema snapshot.
- [ ] Create a pre-V2 backup using `scripts/backup_data.sh` or a direct copy of the DuckDB file if the script is not usable.
- [ ] Record the backup path and current commit hash in the implementation notes.
- [ ] Capture a schema snapshot with table names, columns, row counts, and primary-key-like uniqueness checks.
- [ ] Run the existing US e2e baseline before schema changes. If local data/network makes this impossible, record the exact blocker and run the narrowest available US smoke checks instead.
- [ ] Commit only self-contained V2.0 changes as implementation progresses.

**Verification:**

```bash
git status --short --branch
uv run python scripts/e2e_demo.py
```

Expected: branch is `V2.0`; unrelated existing docs remain visible if not part of the current task; US baseline result is known before any migration.

## Task 1: Add Market Context Foundation

**Files:**
- Create: `backend/services/market_context.py`
- Modify: `backend/config.py`
- Modify: `config.yaml`

- [ ] Add a `Market` literal/enum accepting `US` and `CN`.
- [ ] Add `normalize_market(value: str | None) -> str`, defaulting to `US`.
- [ ] Add `get_market_config(market)`, `get_default_benchmark(market)`, and `get_default_calendar(market)`.
- [ ] Add configuration shape:

```yaml
markets:
  US:
    provider: yfinance
    calendar: NYSE
    benchmark: SPY
    default_group: us_all_market
  CN:
    provider: baostock
    calendar: XSHG
    benchmark: sh.000300
    default_group: cn_all_a
```

- [ ] Keep old `data.provider`, `market.calendar`, and `backtest.default_benchmark` readable as fallback for compatibility.
- [ ] Add focused tests or a small validation script for normalization and default resolution.
- [ ] Commit:

```bash
git add backend/services/market_context.py backend/config.py config.yaml
git commit -m "feat: add market context configuration"
```

**Verification:**

```bash
uv run python - <<'PY'
from backend.services.market_context import normalize_market, get_default_benchmark
assert normalize_market(None) == "US"
assert normalize_market("cn") == "CN"
assert get_default_benchmark("CN") == "sh.000300"
print("market context ok")
PY
```

Expected: prints `market context ok`.

## Task 2: Migrate DuckDB Schema To Market-Aware Keys

**Files:**
- Modify: `backend/db.py`
- Create if needed: `backend/services/schema_migrations.py`
- Create if useful: `scripts/validate_schema_migration.py`

- [ ] Add migration helpers that inspect existing columns before altering tables.
- [ ] Add a migration version table so migrations are idempotent and auditable.
- [ ] Implement a dry-run path against a copied DuckDB database.
- [ ] Generate a preflight report before changing any table: row count, null count for key columns, duplicate-key risk under the new market-aware key, and stable aggregate fingerprints.
- [ ] Add `market VARCHAR NOT NULL DEFAULT 'US'` to market-sensitive tables.
- [ ] Rebuild primary keys where DuckDB cannot alter them in place using shadow tables.
- [ ] Keep original tables as `table__backup_<migration_id>` after successful replacement; do not drop backup tables during the V2 implementation.
- [ ] Migrate these tables first: `stocks`, `daily_bars`, `index_bars`, `stock_groups`, `stock_group_members`.
- [ ] Migrate research assets next: `label_definitions`, `factors`, `factor_values_cache`, `factor_eval_results`, `feature_sets`, `models`, `strategies`, `backtest_results`, `signal_runs`, `signal_details`, `paper_trading_sessions`, `paper_trading_daily`, `paper_trading_signal_cache`.
- [ ] Add compatibility views or query helpers only if direct schema migration breaks too much at once.
- [ ] Ensure all old rows become `US`.
- [ ] Validate every migrated table: old row count equals new row count, `market` is non-null, new keys are unique, and stable aggregate fingerprints match.
- [ ] Validate new empty database initialization still works.
- [ ] Validate upgrade from a copied existing database works before touching the working database.
- [ ] Confirm old no-market service/API calls still resolve to US after migration.
- [ ] Commit:

```bash
git add backend/db.py backend/services/schema_migrations.py scripts/validate_schema_migration.py
git commit -m "feat: add market-aware database schema"
```

**Verification:**

```bash
uv run python scripts/validate_schema_migration.py --database-copy data/qagent-v2-migration-test.duckdb
uv run python - <<'PY'
from backend.db import get_connection
conn = get_connection()
for table in ["stocks", "daily_bars", "models", "signal_runs"]:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]
    assert "market" in cols, table
print("market schema ok")
PY
uv run python scripts/e2e_demo.py
```

Expected: migration validation report shows no row loss, no duplicate-key risk, and all old rows marked `US`; prints `market schema ok`; existing US e2e still works or any blocker is unrelated to migration and documented.

## Task 3: Add Provider Registry And BaoStock Provider

**Files:**
- Create: `backend/providers/registry.py`
- Create: `backend/providers/baostock_provider.py`
- Modify: `backend/providers/base.py`
- Modify: `backend/providers/yfinance_provider.py`
- Modify: `pyproject.toml` or dependency lock files if BaoStock is not already installed.

- [ ] Add provider registry keyed by `(market, provider_name)`.
- [ ] Keep yfinance behavior unchanged for `US`.
- [ ] Add BaoStock login/logout wrapper with clear error messages.
- [ ] Implement `get_stock_list()` returning `ticker`, `name`, `exchange`, `sector`, `status`, and `market`.
- [ ] Implement `get_daily_bars(tickers, start, end)` using BaoStock `query_history_k_data_plus`.
- [ ] Store front-adjusted daily bars by default, using BaoStock `adjustflag="2"`.
- [ ] Implement `get_index_data(symbol, start, end)` for `sh.000300` and other index symbols.
- [ ] Normalize all BaoStock output dates and numeric fields before returning DataFrames.
- [ ] Commit:

```bash
git add backend/providers/registry.py backend/providers/baostock_provider.py backend/providers/base.py backend/providers/yfinance_provider.py pyproject.toml uv.lock
git commit -m "feat: add baostock data provider"
```

**Verification:**

```bash
uv run python - <<'PY'
from datetime import date
from backend.providers.registry import get_provider
p = get_provider("CN")
bars = p.get_daily_bars(["sh.600000"], date(2024, 1, 2), date(2024, 1, 10))
assert {"market", "ticker", "date", "open", "close", "volume"}.issubset(bars.columns)
print(bars.tail(1).to_dict("records"))
PY
```

Expected: returns at least one BaoStock daily bar when network/provider is available. If BaoStock is unavailable, the failure message must identify login/network/provider status.

## Task 4: Make Calendars Market-Aware

**Files:**
- Modify: `backend/services/calendar_service.py`
- Modify callers in `backend/services/*.py`

- [ ] Change calendar functions to accept `market: str | None = None`.
- [ ] Keep old call sites working by defaulting to `US`.
- [ ] Use NYSE calendar and US close-time semantics for `US`.
- [ ] Use CN exchange calendar or BaoStock trade-date fallback for `CN`.
- [ ] Ensure `snap_to_trading_day`, `offset_trading_days`, and `get_latest_trading_day` never mix market calendars.
- [ ] Commit:

```bash
git add backend/services/calendar_service.py backend/services
git commit -m "feat: make trading calendars market aware"
```

**Verification:**

```bash
uv run python - <<'PY'
from backend.services.calendar_service import get_trading_days
assert get_trading_days("US", "2024-01-02", "2024-01-05")
assert get_trading_days("CN", "2024-01-02", "2024-01-05")
print("calendar ok")
PY
```

Expected: both markets return trading-day lists and holiday differences are possible.

## Task 5: Update Data And Group Services For Market Scope

**Files:**
- Modify: `backend/services/data_service.py`
- Modify: `backend/services/group_service.py`
- Modify: `backend/api/data.py`
- Modify: `backend/api/groups.py`

- [ ] Add `market` to data update requests, data status, ticker search, and daily bar query.
- [ ] Scope stale-data checks by market.
- [ ] Scope incremental start-date queries by `(market, ticker)`.
- [ ] Add built-in groups: `us_all_market`, `us_sp500`, `us_nasdaq100`, `cn_all_a`, `cn_hs300`.
- [ ] Ensure group membership always resolves inside one market.
- [ ] Reject manual group members whose ticker market does not match the group market.
- [ ] Commit:

```bash
git add backend/services/data_service.py backend/services/group_service.py backend/api/data.py backend/api/groups.py
git commit -m "feat: scope data and groups by market"
```

**Verification:**

```bash
uv run python scripts/e2e_demo.py
```

Expected: existing US demo still works. Then run a narrow CN data update through API or service and confirm rows have `market='CN'`.

## Task 6: Make Labels Market-Scoped

**Files:**
- Modify: `backend/services/label_service.py`
- Modify: `backend/api/labels.py`

- [ ] Add `market` to label definitions and label list/detail responses.
- [ ] Keep old label requests valid by defaulting to `market=US`.
- [ ] Scope label computation by market-aware daily bars and market-aware benchmark data.
- [ ] Validate label benchmark market before computing excess-return labels.
- [ ] Add a small service/API check that a CN label cannot use a US benchmark.
- [ ] Commit:

```bash
git add backend/services/label_service.py backend/api/labels.py
git commit -m "feat: scope labels by market"
```

**Verification:**

```bash
uv run python - <<'PY'
from backend.services.market_context import normalize_market
assert normalize_market(None) == "US"
print("label market default check ok")
PY
uv run python scripts/e2e_demo.py
```

Expected: existing US label-dependent workflows still pass; CN label validation blocks a US benchmark.

## Task 7: Make Factors And Feature Sets Market-Scoped

**Files:**
- Modify: `backend/services/factor_service.py`
- Modify: `backend/services/factor_engine.py`
- Modify: `backend/services/factor_eval_service.py`
- Modify: `backend/services/feature_service.py`
- Modify: `backend/api/factors.py`
- Modify: `backend/api/features.py`

- [ ] Add `market` to factor records, factor cache rows, and factor evaluation records.
- [ ] Add `market` to feature sets and feature dependency snapshots.
- [ ] Scope factor compute/evaluation queries by `(market, ticker, date)`.
- [ ] Validate factor, label, feature set, and universe group markets before evaluation or feature computation.
- [ ] Keep old factor and feature requests valid by defaulting to `market=US`.
- [ ] Commit:

```bash
git add backend/services/factor_service.py backend/services/factor_engine.py backend/services/factor_eval_service.py backend/services/feature_service.py backend/api/factors.py backend/api/features.py
git commit -m "feat: scope factors and features by market"
```

**Verification:**

```bash
uv run python scripts/e2e_demo.py
```

Expected: US factor and feature workflows continue to pass; a CN feature set cannot reference a US factor.

## Task 8: Make Model Records Market-Aware Before Ranking

**Files:**
- Modify: `backend/services/model_service.py`
- Modify: `backend/api/models.py`

- [ ] Add `market` to model records, model metadata files, model list/detail responses, and training result summaries.
- [ ] Validate model feature set, label, universe group, and benchmark market before training.
- [ ] Keep old train-model requests valid by defaulting `market=US`.
- [ ] Keep regression/classification task inference unchanged for existing labels.
- [ ] Ensure existing model files under `data/models` load without rewriting files during migration.
- [ ] Add lazy metadata backfill only when a model is loaded or retrained.
- [ ] Commit:

```bash
git add backend/services/model_service.py backend/api/models.py
git commit -m "feat: make model records market aware"
```

**Verification:**

```bash
uv run python scripts/e2e_demo.py
```

Expected: existing US regression/classification training and prediction still work; model records include `market='US'` when no market is supplied.

## Task 9: Add Ranking, Pairwise, And Listwise Training Objectives

**Files:**
- Create: `backend/services/ranking_dataset.py`
- Modify: `backend/models/base.py`
- Modify: `backend/models/lightgbm_model.py`
- Modify: `backend/services/model_service.py`
- Modify: `backend/api/models.py`

- [ ] Add train request fields:

```json
{
  "objective_type": "regression | classification | ranking | pairwise | listwise",
  "ranking_config": {
    "query_group": "date",
    "eval_at": [5, 10, 20],
    "min_group_size": 5,
    "label_gain": "identity"
  }
}
```

- [ ] Build ranking datasets from the existing market-aware `(date, ticker)` aligned index.
- [ ] Sort training samples by date and pass LightGBM group sizes for train/valid/test.
- [ ] Add `LightGBMModel(task="ranking")` backed by `lightgbm.LGBMRanker`.
- [ ] Map `ranking` and `listwise` to native LightGBM ranking.
- [ ] Map `pairwise` to the same LambdaRank-backed implementation in V2.0, with metadata marking `pairwise_mode="lambdarank"`.
- [ ] Add ranking metrics: `ndcg@k`, `top_k_mean_label`, `rank_ic_mean`, and `pairwise_accuracy_sampled`.
- [ ] Preserve prediction output as per-ticker scores so existing strategies can sort candidates.
- [ ] Commit:

```bash
git add backend/services/ranking_dataset.py backend/models/base.py backend/models/lightgbm_model.py backend/services/model_service.py backend/api/models.py
git commit -m "feat: add cross-sectional ranking objectives"
```

**Verification:**

```bash
uv run python - <<'PY'
from backend.services.ranking_dataset import build_date_groups
import pandas as pd
idx = pd.MultiIndex.from_tuples(
    [("2024-01-02", "A"), ("2024-01-02", "B"), ("2024-01-03", "A"), ("2024-01-03", "B")],
    names=["date", "ticker"],
)
X = pd.DataFrame({"x": [1, 2, 3, 4]}, index=idx)
y = pd.Series([0.1, 0.2, -0.1, 0.3], index=idx)
groups = build_date_groups(X, y, min_group_size=2)
assert groups.group_sizes == [2, 2]
print("ranking dataset ok")
PY
uv run python scripts/e2e_demo.py
```

Expected: ranking dataset check passes; old model training still passes; a small ranking model saves `task_type=ranking` and ranking metrics.

## Task 10: Make Strategies And Backtests Market-Safe

**Files:**
- Modify: `backend/services/strategy_service.py`
- Modify: `backend/services/backtest_service.py`
- Modify: `backend/services/backtest_engine.py`
- Modify: `backend/api/strategies.py`

- [ ] Add `market` to strategy records and strategy dependency snapshots.
- [ ] Validate required factors/models are in the same market as the strategy.
- [ ] Add market to backtest config, benchmark loading, result records, and result summaries.
- [ ] Reject a CN backtest using a US benchmark, US strategy, or US model.
- [ ] Keep T+1/open-price execution semantics unchanged.
- [ ] Commit:

```bash
git add backend/services/strategy_service.py backend/services/backtest_service.py backend/services/backtest_engine.py backend/api/strategies.py
git commit -m "feat: make strategies and backtests market safe"
```

**Verification:**

```bash
uv run python scripts/e2e_demo.py
```

Expected: existing US strategy/backtest smoke passes; cross-market backtest attempts return clear validation errors.

## Task 11: Make Signals And Paper Trading Market-Safe

**Files:**
- Modify: `backend/services/signal_service.py`
- Modify: `backend/services/paper_trading_service.py`
- Modify: `backend/api/signals.py`
- Modify: `backend/api/paper_trading.py`

- [ ] Add market to signal runs and signal details.
- [ ] Add market to paper-trading sessions, paper daily records, and signal cache rows.
- [ ] Validate strategy, universe group, model/factor dependencies, and target date calendar market before signal generation.
- [ ] Validate paper-trading session advancement uses the session market calendar and market-specific prices.
- [ ] Keep old no-market signal and paper-trading requests defaulting to `US`.
- [ ] Commit:

```bash
git add backend/services/signal_service.py backend/services/paper_trading_service.py backend/api/signals.py backend/api/paper_trading.py
git commit -m "feat: make signals and paper trading market safe"
```

**Verification:**

```bash
uv run python scripts/e2e_demo.py
```

Expected: existing US signal and paper-trading workflows still pass; a CN paper session cannot use a US strategy or US prices.

## Task 12: Update MCP And Agent-Facing Contracts

**Files:**
- Modify: `backend/mcp_server.py`
- Modify: `docs/agent-guide.md`
- Modify: `AGENTS.md`
- Modify: `docs/backlog.md`

- [ ] Add `market` argument to MCP tools that touch data, groups, factors, labels, features, models, strategies, backtests, signals, or paper trading.
- [ ] Keep old agent calls compatible by defaulting missing market to `US`.
- [ ] Return `market`, `asset_scope`, `task_id`, and `poll_url` where useful.
- [ ] Standardize validation errors so agents can repair requests without reading server logs.
- [ ] Add agent best-practice examples for CN update, CN factor evaluation, ranking model training, and CN backtest.
- [ ] Keep `docs/backlog.md` as the requirement/issue board for agent-visible future work.
- [ ] Commit:

```bash
git add backend/mcp_server.py docs/agent-guide.md AGENTS.md docs/backlog.md
git commit -m "docs: document v2 agent workflows"
```

**Verification:**

```bash
uv run python -m backend.mcp_server --help
```

Expected: MCP server still starts or shows help, and tool schemas include `market` where applicable.

## Task 13: Add Frontend API Market Scope And Global Selector

**Files:**
- Modify: `frontend/src/api/index.ts`
- Modify: `frontend/src/api/client.ts`
- Create: `frontend/src/components/MarketScopeSelector.tsx`
- Modify: `frontend/src/App.tsx`

- [ ] Add shared `Market = "US" | "CN"` frontend type.
- [ ] Add a global market selector with `US` and `CN`.
- [ ] Persist selected market in local storage.
- [ ] Add a shared API helper that injects the selected market into scoped API calls.
- [ ] Keep the first-load default as `US`.
- [ ] Commit:

```bash
git add frontend/src/api/index.ts frontend/src/api/client.ts frontend/src/components/MarketScopeSelector.tsx frontend/src/App.tsx
git commit -m "feat: add frontend market scope selector"
```

**Verification:**

```bash
cd frontend && pnpm build
```

Expected: TypeScript build passes and existing pages still render with the default US scope.

## Task 14: Update Frontend Pages For Human Validation

**Files:**
- Modify: `frontend/src/pages/DataManagePage.tsx`
- Modify: `frontend/src/pages/MarketPage.tsx`
- Modify: `frontend/src/pages/FactorResearch.tsx`
- Modify: `frontend/src/pages/FeatureEngineering.tsx`
- Modify: `frontend/src/pages/ModelTraining.tsx`
- Modify: `frontend/src/pages/StrategyBacktest.tsx`
- Modify: `frontend/src/pages/SignalGeneration.tsx`
- Modify: `frontend/src/pages/PaperTrading.tsx`

- [ ] Pass market through all page-level API calls.
- [ ] Add market badges to assets and result rows.
- [ ] Add model training objective selector: regression/classification/ranking/pairwise/listwise.
- [ ] Add ranking metric visualization: NDCG@k, Rank IC, top-k label chart.
- [ ] Improve empty states: show whether the selected market has data, groups, factors, feature sets, or models.
- [ ] Improve data status cards for human acceptance: coverage range, ticker count, latest trade day, benchmark freshness.
- [ ] Keep Ant Design dark layout and avoid decorative layout rewrites.
- [ ] Commit:

```bash
git add frontend/src/pages
git commit -m "feat: add market-aware page workflows"
```

**Verification:**

```bash
cd frontend && pnpm build
```

Expected: TypeScript build passes. Manual browser check verifies market switching does not leak US assets into CN views.

## Task 15: Add V2 Regression Check Script

**Files:**
- Create: `scripts/v2_regression_check.py`
- Modify if needed: `scripts/e2e_demo.py`

- [ ] Add a single command that can be run after each phase.
- [ ] Check old no-market service/API calls default to `US`.
- [ ] Check migration validation output when a copied DB path is supplied.
- [ ] Check US data/factor/model/backtest smoke paths.
- [ ] Check CN provider unavailable or network failure does not block US flows.
- [ ] Make CN BaoStock checks optional and report `skipped` with a clear reason when unavailable.
- [ ] Commit:

```bash
git add scripts/v2_regression_check.py scripts/e2e_demo.py
git commit -m "test: add v2 regression check"
```

**Verification:**

```bash
uv run python scripts/v2_regression_check.py
```

Expected: US compatibility checks pass; optional CN checks pass or are explicitly skipped.

## Task 16: Bounded Refactoring And Performance Pass

**Files:**
- Modify only files already touched by Tasks 1-15 unless a bottleneck requires a focused helper.

- [ ] Replace hardcoded provider selection with registry calls.
- [ ] Remove duplicated market/default benchmark logic from services.
- [ ] Keep ranking dataset construction out of `model_service.py`.
- [ ] Keep API routes thin; move validation into services or `market_context.py`.
- [ ] Replace repeated ticker `IN (...)` string construction with parameterized or DataFrame joins where practical.
- [ ] Batch DuckDB writes for high-row paths: daily bars, factor cache, signal details.
- [ ] Add structured warnings to API responses instead of forcing agents to inspect logs.
- [ ] Remove dead compatibility branches only after the US e2e demo and V2 regression check pass.
- [ ] Commit one focused refactor at a time, for example:

```bash
git add backend/providers backend/services/data_service.py
git commit -m "refactor: centralize provider resolution"
```

**Verification:**

```bash
uv run python scripts/v2_regression_check.py
uv run python scripts/e2e_demo.py
cd frontend && pnpm build
```

Expected: no user-visible regression; data update and model training logs are clearer and less duplicated.

## Task 17: Create V2 Acceptance Document And Run Final Checks

**Files:**
- Create: `docs/v2-acceptance.md`
- Modify: `scripts/e2e_demo.py`

- [ ] Document acceptance scenarios for US backward compatibility.
- [ ] Document acceptance scenarios for CN data update, CN factor evaluation, CN feature set, CN ranking model, CN backtest, CN signal generation, and CN paper-trading advancement.
- [ ] Add phase milestone evidence from P0-P7.
- [ ] Record whether `pairwise` is LambdaRank-backed or true pair-sampling-backed for this release.
- [ ] Run V2 regression check.
- [ ] Run backend e2e.
- [ ] Run frontend build.
- [ ] Run at least one manual browser validation for market switching and ranking training UI.
- [ ] Commit:

```bash
git add docs/v2-acceptance.md scripts/e2e_demo.py
git commit -m "docs: add v2 acceptance checklist"
```

**Verification:**

```bash
uv run python scripts/v2_regression_check.py
uv run python scripts/e2e_demo.py
cd frontend && pnpm build
```

Expected: US flow passes. CN smoke checks are either passing or clearly marked skipped when BaoStock/network is unavailable. `docs/v2-acceptance.md` contains evidence for every phase gate.

## Suggested Implementation Order

1. Task 0: preserve branch state.
2. Task 1: market context.
3. Task 2: schema migration.
4. Task 3: BaoStock provider.
5. Task 4: calendar service.
6. Task 5: data/groups.
7. Task 6: labels.
8. Task 7: factors and features.
9. Task 8: model records and old model compatibility.
10. Task 9: ranking objectives.
11. Task 10: strategies and backtests.
12. Task 11: signals and paper trading.
13. Task 12: MCP and docs.
14. Task 13: frontend API and market selector.
15. Task 14: frontend page workflows.
16. Task 15: V2 regression check.
17. Task 16: bounded refactoring/performance.
18. Task 17: acceptance.

## Commit Strategy

- Keep each task in its own commit when possible.
- Do not mix unrelated pre-existing docs with V2 implementation commits.
- Use short conventional commit subjects: `feat: ...`, `fix: ...`, `refactor: ...`, `docs: ...`, `test: ...`.
- If a task becomes too large, split by service boundary rather than by mechanical file type.
- Do not start a later phase until the previous phase gate evidence is recorded in `docs/v2-acceptance.md` or the blocker is explicitly documented.

## Acceptance Criteria

- Existing US workflows continue to run with no required API changes.
- Existing DB files upgrade without row loss, key collision, or silent deduplication.
- Existing model files remain loadable after migration.
- CN market can update stock list, daily bars, benchmark data, and built-in groups from BaoStock.
- CN factor, label, feature, model, strategy, backtest, signal, and paper-trading workflows are isolated from US assets.
- Cross-market mistakes are blocked with clear messages.
- Ranking/listwise training produces date-grouped ranking metrics and per-ticker prediction scores.
- Pairwise objective is available as a documented LambdaRank-backed V2.0 objective, or true pairwise support is explicitly implemented and verified.
- The React UI makes current market scope visible and gives humans enough charts/tables to validate results.
- MCP/REST responses include enough structured fields for agents to operate without scraping UI state.
- Final checks include `uv run python scripts/v2_regression_check.py`, `uv run python scripts/e2e_demo.py`, and `cd frontend && pnpm build`.

## Open Decisions

- Whether to keep `sh.600000` as the long-term canonical CN ticker or later add display aliases like `600000.SH`.
- Whether CN default benchmark should remain `sh.000300` or be user-configurable per backtest as the primary path.
- Whether explicit pair sampling deserves a V2.1 task after the first ranking implementation is validated.
- Whether BaoStock trading calendar should be cached into DuckDB for reproducible historical holiday behavior.
