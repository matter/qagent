"""FastAPI application entry-point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.api.health import router as health_router
from backend.api.data import router as data_router
from backend.api.groups import router as groups_router
from backend.api.tasks import router as tasks_router
from backend.api.labels import router as labels_router
from backend.api.factors import router as factors_router
from backend.api.features import router as features_router
from backend.api.models import router as models_router
from backend.api.strategies import router as strategies_router
from backend.api.signals import router as signals_router
from backend.api.paper_trading import router as paper_trading_router
from backend.api.diagnostics import router as diagnostics_router
from backend.api.research import router as research_router
from backend.api.market_data import router as market_data_router
from backend.api.macro_data import router as macro_data_router
from backend.api.migration import router as migration_router
from backend.api.research_cache import router as research_cache_router
from backend.api.universe_dataset import router as universe_dataset_router
from backend.api.factor_engine_3 import router as factor_engine_3_router
from backend.api.model_experiment_3 import router as model_experiment_3_router
from backend.api.portfolio_assets_3 import router as portfolio_assets_3_router
from backend.api.strategy_graph_3 import router as strategy_graph_3_router
from backend.api.agent_research_3 import router as agent_research_3_router
from backend.api.production_signal_3 import router as production_signal_3_router
from backend.config import settings
from backend.db import close_db, init_db
from backend.logger import get_logger, setup_logging
from backend.services.startup_maintenance_service import StartupMaintenanceService
from backend.tasks.executor import TaskSubmissionPaused
from backend.tasks.store import TaskStore

log = get_logger(__name__)

_FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    # --- startup ---
    setup_logging(log_dir=settings.project_root / "logs")
    log.info("app.startup", host=settings.server.host, port=settings.server.port)

    # Ensure data directories exist
    for d in (
        settings.db_path.parent,
        settings.models_dir,
        settings.factors_dir,
        settings.strategies_dir,
    ):
        d.mkdir(parents=True, exist_ok=True)

    maintenance = StartupMaintenanceService()
    try:
        temp_cleanup = maintenance.cleanup_stale_duckdb_temp_files()
        if temp_cleanup.get("deleted_files") or temp_cleanup.get("errors"):
            log.info("app.startup.temp_cleanup", **temp_cleanup)
    except Exception as exc:
        log.warning("app.startup.temp_cleanup_failed", error=str(exc))

    init_db()

    try:
        maintenance.run_after_db_init()
    except Exception as exc:
        log.warning("app.startup.cache_maintenance_failed", error=str(exc))

    # Mark any tasks left running/queued from a previous server run as failed
    TaskStore().mark_stale_running()
    from backend.services.data_service import DataService
    DataService().mark_stale_running_updates()

    # Seed preset label definitions
    from backend.services.label_service import LabelService
    LabelService().ensure_presets()

    # Register built-in factor templates
    from backend.services.factor_service import FactorService
    FactorService().ensure_builtin_templates()

    # Register built-in agent research playbooks
    from backend.services.agent_research_3_service import AgentResearch3Service
    AgentResearch3Service().ensure_builtin_playbooks()

    yield

    # --- shutdown ---
    close_db()
    log.info("app.shutdown")


app = FastAPI(title="QAgent", version="0.1.0", lifespan=lifespan)


@app.exception_handler(TaskSubmissionPaused)
async def task_submission_paused_handler(
    request: Request,
    exc: TaskSubmissionPaused,
) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})

# CORS – allow the Vite dev server during development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(health_router)
app.include_router(data_router)
app.include_router(groups_router)
app.include_router(tasks_router)
app.include_router(labels_router)
app.include_router(factors_router)
app.include_router(features_router)
app.include_router(models_router)
app.include_router(strategies_router)
app.include_router(signals_router)
app.include_router(paper_trading_router)
app.include_router(diagnostics_router)
app.include_router(research_router)
app.include_router(market_data_router)
app.include_router(macro_data_router)
app.include_router(migration_router)
app.include_router(research_cache_router)
app.include_router(universe_dataset_router)
app.include_router(factor_engine_3_router)
app.include_router(model_experiment_3_router)
app.include_router(portfolio_assets_3_router)
app.include_router(strategy_graph_3_router)
app.include_router(agent_research_3_router)
app.include_router(production_signal_3_router)

# MCP Server -- mount at /mcp
from backend.mcp_server import mcp as _mcp_server
app.mount("/mcp", _mcp_server.streamable_http_app())

# Serve frontend build if it exists
if _FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(_FRONTEND_DIST), html=True), name="spa")
