# Repository Guidelines

## Product Context
QAgent is a local-first, single-user low-frequency quantitative research system. The legacy/default scope is US equities, and the V2.0 branch adds explicit `market` isolation for `US` and China A-shares (`CN`). It supports the full research loop: market data management, factor research, feature engineering, model training, strategy backtesting, signal generation, and paper trading. It is a research platform, not a broker integration or high-frequency trading system.

The system is agent-native: humans use the React UI, agents call the REST API or the mounted MCP server, and both paths must share the same backend service layer. Preserve this invariant when adding features.

All V2 market-aware REST and MCP calls must default missing `market` to `US` for backward compatibility. Do not mix assets across markets: groups, factors, labels, feature sets, models, strategies, backtests, signal runs, and paper-trading sessions must resolve dependencies within one market.

## Project Structure & Module Organization
`backend/` contains the FastAPI server. Keep route handlers in `backend/api/` thin and move domain logic into `backend/services/`. Reusable protocols and primitives live in `backend/factors/`, `backend/models/`, `backend/strategies/`, and `backend/tasks/`. Data providers live in `backend/providers/`, and shared calendar logic lives in `backend/services/calendar_service.py`.

`frontend/src/` contains the Vite + React UI. Screens live in `pages/`, API clients and shared types in `api/`, and reusable UI pieces in `components/`. The app uses React Router, Ant Design, ECharts, Monaco Editor, and an axios client whose base URL is `/api`.

Runtime configuration lives in `config.yaml` and is loaded via `backend/config.py`. The DuckDB schema is initialized in `backend/db.py`. Generated data, caches, trained model files, custom factor/strategy assets, logs, PID files, and frontend builds live under ignored paths such as `data/`, `logs/`, `.backend.pid`, `.frontend.pid`, and `frontend/dist/`.

## Domain Rules
Time-series correctness is more important than feature breadth. Do not introduce look-ahead bias. Signals should be based on data available at the decision date, and execution semantics should respect the existing T+1/open-price conventions in backtesting, signal, and paper-trading code.

Do not use random K-Fold style validation for market time series. Prefer the existing time split, rolling, expanding, purge-gap, and calendar-aware patterns already used by the model and backtest services.

Persist research assets with enough metadata to be reproducible. Factors, feature sets, models, strategies, backtests, signal runs, and paper-trading sessions should keep source/config/dependency snapshots rather than only final metrics.

When a workflow is long-running or user/agent initiated, run it through `TaskExecutor` and return a `task_id` for polling through `/api/tasks`. Existing examples include data updates, factor compute/evaluation, model training, backtests, signal generation/diagnosis, and paper-trading advancement.

## Build, Test, and Development Commands
Install backend dependencies with `uv sync`. Install frontend dependencies with `cd frontend && pnpm install`.

Use `scripts/start.sh` to launch both services in development and `scripts/stop.sh` to stop them. Run the API only with:

```bash
uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload
```

Run the UI only with:

```bash
cd frontend && pnpm dev
```

Build the frontend with:

```bash
cd frontend && pnpm build
```

The frontend dev server runs on `localhost:5173` and proxies `/api` to `127.0.0.1:8000`. FastAPI serves the built SPA from `frontend/dist/` when that directory exists.

Use `scripts/backup_data.sh` and `scripts/restore_data.sh` for DuckDB/model data backup and restore. Stop the backend before restore; the script refuses to restore while the DB is in use.

## Coding Style & Naming Conventions
Match the existing code style rather than introducing a new one. Use 4-space indentation in Python and 2-space indentation in TypeScript/TSX. Prefer `snake_case` for Python modules, functions, and variables; use `PascalCase` for React page and component filenames such as `StrategyBacktest.tsx`.

Backend API modules should validate/shape request data and delegate to services. Services may access DuckDB via `backend.db.get_connection()`, but keep SQL parameterized and prefer batch DataFrame/DuckDB operations for factor, signal, model, and backtest paths.

Custom factor code should subclass `FactorBase` and implement `compute(data: pd.DataFrame) -> pd.Series`. Custom strategy code should subclass `StrategyBase` and return a DataFrame indexed by ticker with `signal`, `weight`, and `strength` columns. Preserve `required_factors()` and `required_models()` dependency declarations when strategy logic depends on upstream assets.

Frontend code is strict TypeScript. `frontend/tsconfig.json` enables `strict`, `noUnusedLocals`, and `noUnusedParameters`, so keep types explicit enough for `pnpm build`. Follow the existing Ant Design dark layout, React Router route structure, and shared API types in `frontend/src/api/index.ts`.

## Testing Guidelines
This repo does not currently include a checked-in pytest, Vitest, or coverage workflow. For backend-affecting changes, start the local server and validate the end-to-end flow with:

```bash
uv run python scripts/e2e_demo.py
```

For frontend changes, run:

```bash
cd frontend && pnpm build
```

Also perform a manual browser check for UI changes, especially pages under `frontend/src/pages/` and reusable components under `frontend/src/components/`. If you add automated tests, place them near the affected feature and keep naming explicit, for example `test_backtest_service.py`.

For data-sensitive changes, verify against realistic local data when available and avoid committing generated artifacts from `data/`, `logs/`, or `frontend/dist/`.

## Agent Workflow Notes
Before editing, check `git status --short` and avoid overwriting unrelated user changes. This repository often has runtime data and local experiments in progress.

Use `docs/agent-guide.md` as the operating manual for agent-driven research and development. Record all unresolved problems, requirements, and validation gaps in `docs/backlog.md` using its template; do not leave issue context only in chat history.

When changing behavior that is visible through multiple entry points, update the service layer first and keep REST, MCP, and UI behavior consistent. The MCP server in `backend/mcp_server.py` should call the same service methods as REST routes.

When adding or changing API response fields, update `frontend/src/api/index.ts` and all affected components together. Long-running endpoints should expose task status consistently through `/api/tasks`.

Prefer targeted validation over broad data refreshes. Full data updates can be slow and network-dependent; use narrow ticker/group/date ranges when a focused check is enough.

## Commit & Pull Request Guidelines
Recent history follows short conventional subjects like `feat: ...`, `fix: ...`, and `merge: ...`. Keep commit messages imperative and limited to one change set.

Pull requests should explain user-visible impact, note any updates to `config.yaml` or generated data expectations, list the commands used for verification, and include screenshots for UI changes under `frontend/src/pages/`.
