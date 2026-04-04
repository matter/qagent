"""FastAPI application entry-point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
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
from backend.config import settings
from backend.db import close_db, init_db
from backend.logger import get_logger, setup_logging
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

    init_db()

    # Mark any tasks left running/queued from a previous server run as failed
    TaskStore().mark_stale_running()

    # Seed preset label definitions
    from backend.services.label_service import LabelService
    LabelService().ensure_presets()

    # Register built-in factor templates
    from backend.services.factor_service import FactorService
    FactorService().ensure_builtin_templates()

    yield

    # --- shutdown ---
    close_db()
    log.info("app.shutdown")


app = FastAPI(title="QAgent", version="0.1.0", lifespan=lifespan)

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

# MCP Server -- mount at /mcp
from backend.mcp_server import mcp as _mcp_server
app.mount("/mcp", _mcp_server.streamable_http_app())

# Serve frontend build if it exists
if _FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(_FRONTEND_DIST), html=True), name="spa")
