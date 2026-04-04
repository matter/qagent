"""Stock group CRUD API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.group_service import GroupService

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["groups"])

_service: GroupService | None = None


def _get_service() -> GroupService:
    global _service
    if _service is None:
        _service = GroupService()
        _service.ensure_builtins()
    return _service


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class CreateGroupRequest(BaseModel):
    name: str
    description: Optional[str] = None
    group_type: str = "manual"
    tickers: Optional[list[str]] = None
    filter_expr: Optional[str] = None


class UpdateGroupRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tickers: Optional[list[str]] = None
    filter_expr: Optional[str] = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/groups/refresh-indices")
async def refresh_index_groups() -> list[dict]:
    """Re-fetch S&P 500, NASDAQ 100, and Russell 3000 constituents."""
    svc = _get_service()
    try:
        return svc.refresh_index_groups()
    except Exception as e:
        log.error("api.groups.refresh_indices_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to refresh index groups: {e}")


@router.post("/groups")
async def create_group(body: CreateGroupRequest) -> dict:
    """Create a new stock group."""
    svc = _get_service()
    try:
        return svc.create_group(
            name=body.name,
            description=body.description,
            group_type=body.group_type,
            tickers=body.tickers,
            filter_expr=body.filter_expr,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/groups")
async def list_groups() -> list[dict]:
    """List all stock groups with member counts."""
    svc = _get_service()
    return svc.list_groups()


@router.get("/groups/{group_id}")
async def get_group(group_id: str) -> dict:
    """Get group detail including member tickers."""
    svc = _get_service()
    try:
        return svc.get_group(group_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/groups/{group_id}")
async def update_group(group_id: str, body: UpdateGroupRequest) -> dict:
    """Update a stock group."""
    svc = _get_service()
    try:
        return svc.update_group(
            group_id=group_id,
            name=body.name,
            description=body.description,
            tickers=body.tickers,
            filter_expr=body.filter_expr,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/groups/{group_id}")
async def delete_group(group_id: str) -> dict:
    """Delete a stock group."""
    svc = _get_service()
    try:
        svc.delete_group(group_id)
        return {"status": "deleted", "id": group_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/groups/{group_id}/refresh")
async def refresh_group(group_id: str) -> dict:
    """Re-evaluate filter expression for a filter group."""
    svc = _get_service()
    try:
        return svc.refresh_filter(group_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
