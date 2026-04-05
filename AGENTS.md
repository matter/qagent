# Repository Guidelines

## Project Structure & Module Organization
`backend/` contains the FastAPI server. Keep route handlers in `backend/api/`, core business logic in `backend/services/`, reusable research primitives in `backend/factors/`, `backend/models/`, and `backend/strategies/`, and async task plumbing in `backend/tasks/`. `frontend/src/` contains the Vite + React UI, with screens in `pages/`, API clients in `api/`, and shared UI pieces in `components/`. Runtime configuration lives in `config.yaml`. Generated data, caches, models, logs, and PID files are written under ignored paths such as `data/`, `logs/`, `.backend.pid`, and `.frontend.pid`.

## Build, Test, and Development Commands
Install backend dependencies with `uv sync`. Install frontend dependencies with `cd frontend && pnpm install`. Use `scripts/start.sh` to launch both services in development, or `scripts/stop.sh` to stop them. Run the API only with `uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload`. Run the UI only with `cd frontend && pnpm dev`. Build the frontend with `cd frontend && pnpm build`; this also runs TypeScript checks before emitting `frontend/dist/`.

## Coding Style & Naming Conventions
Match the existing code style rather than introducing a new one. Use 4-space indentation in Python and 2-space indentation in TypeScript/TSX. Prefer `snake_case` for Python modules, functions, and variables; use `PascalCase` for React page and component filenames such as `StrategyBacktest.tsx`. Keep FastAPI route modules thin and move domain logic into `backend/services/`. Follow the current strict TypeScript setup in `frontend/tsconfig.json` and avoid unused locals or parameters.

## Testing Guidelines
This repo does not currently include a checked-in `pytest`, Vitest, or coverage workflow. Validate backend changes with `uv run python scripts/e2e_demo.py` against a running local server, and validate frontend changes with `cd frontend && pnpm build` plus manual UI checks in the browser. If you add automated tests, place them near the affected feature and keep naming explicit, for example `test_backtest_service.py`.

## Commit & Pull Request Guidelines
Recent history follows short conventional subjects like `feat: ...`, `fix: ...`, and `merge: ...`. Keep commit messages imperative and limited to one change set. Pull requests should explain user-visible impact, note any updates to `config.yaml` or generated data expectations, list the commands used for verification, and include screenshots for UI changes under `frontend/src/pages/`.
