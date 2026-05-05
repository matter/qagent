"""QAgent 3.0 migration API endpoints."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.services.migration_service import MigrationService

router = APIRouter(prefix="/api/migration", tags=["migration"])


class MigrationReportRequest(BaseModel):
    db_path: str | None = None


class MigrationApplyRequest(BaseModel):
    db_path: str | None = None


def _svc() -> MigrationService:
    return MigrationService()


@router.post("/report")
async def migration_report(body: MigrationReportRequest) -> dict:
    try:
        path = Path(body.db_path) if body.db_path else None
        report = _svc().build_report(path)
        return report
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/apply")
async def migration_apply(body: MigrationApplyRequest) -> dict:
    try:
        path = Path(body.db_path) if body.db_path else None
        return _svc().apply_migration(path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
