# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- **Install backend**: `uv sync`
- **Install frontend**: `cd frontend && pnpm install`
- **Run both services**: `scripts/start.sh` (background, writes .pid files)
- **Stop services**: `scripts/stop.sh`
- **Run API only**: `uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload`
- **Run frontend dev**: `cd frontend && pnpm dev`
- **Build frontend**: `cd frontend && pnpm build` (runs TypeScript check + Vite build)
- **Run demo test**: `uv run python scripts/e2e_demo.py` (requires running server)

## Architecture

**Stack**: FastAPI backend, DuckDB database, React + Ant Design + ECharts frontend, yfinance data provider.

**Backend structure**:
- `backend/api/` – FastAPI route handlers (thin, delegate to services)
- `backend/services/` – Core business logic (backtest, factor, signal, paper trading, data, etc.)
- `backend/factors/`, `backend/models/`, `backend/strategies/` – Research primitives with base classes and loaders
- `backend/tasks/` – Async task executor for long-running operations (data updates, backtests)
- `backend/providers/` – Market data abstraction (yfinance implementation)
- `db.py` – DuckDB connection helper, schema initialization

**Frontend structure**:
- `pages/` – Route-level components (MarketPage, PaperTrading, DataManage, StrategyBacktest, etc.)
- `api/` – Axios client + typed API functions
- `components/` – Reusable UI pieces (factor library, eval history, etc.)

**Key data flows**:
- Signal generation: `SignalService.generate_signals()` → loads strategy, factors, models, prices → returns target weights per ticker
- Paper trading: `PaperTradingService.advance()` → batch signal generation (optimized for multi-day) → trade execution → daily snapshots
- Factor computation: `FactorEngine.compute_factor()` → checks cache (metadata-only coverage), computes missing values in batches (500 tickers), writes via vectorized `df.stack()`
- Data updates: `DataService.update_tickers()` for single/group updates; async tasks via TaskExecutor with polling

**Performance notes**:
- Factor engine uses metadata-only cache check (`_find_uncovered_tickers()`) before loading cached values – avoids full data load for large universes
- Paper trading advance now uses `_generate_signals_batch()` to load strategy/factors/models/prices once for multiple days, then loops per day
- `get_latest_signals()` uses `_generate_signals_lightweight()` skipping validation and DB persistence
- Price cache (`_preload_prices()`) is reused across days in advance

**Configuration**: `config.yaml` – data paths, server port, backtest defaults, market calendar (NYSE).

## Code style

- Python: 4-space indent, `snake_case` for modules/functions/variables
- TypeScript: 2-space indent, `PascalCase` for component files, strict TypeScript enabled
- Route modules stay thin; domain logic goes in services
- Avoid unused imports/variables (TS compiler enforces)