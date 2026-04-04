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

# Serve frontend build if it exists
if _FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(_FRONTEND_DIST), html=True), name="spa")
