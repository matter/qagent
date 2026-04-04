"""Health-check and system info endpoints."""

from __future__ import annotations

import sys
from dataclasses import asdict

from fastapi import APIRouter

from backend.config import settings

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/system/info")
async def system_info() -> dict:
    """Return system information for the settings page."""
    return {
        "version": "0.1.0",
        "python_version": sys.version,
        "db_path": str(settings.db_path),
        "data_dir": str(settings.db_path.parent),
        "models_dir": str(settings.models_dir),
        "factors_dir": str(settings.factors_dir),
        "strategies_dir": str(settings.strategies_dir),
        "data_provider": settings.data.provider,
        "server_host": settings.server.host,
        "server_port": settings.server.port,
        "market_calendar": settings.market.calendar,
        "config": {
            "data": asdict(settings.data),
            "server": asdict(settings.server),
            "backtest": asdict(settings.backtest),
            "market": asdict(settings.market),
        },
    }
