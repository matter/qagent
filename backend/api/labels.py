"""Label definition CRUD API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.label_service import LabelService

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["labels"])

_service: LabelService | None = None


def _get_service() -> LabelService:
    global _service
    if _service is None:
        _service = LabelService()
    return _service


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class CreateLabelRequest(BaseModel):
    name: str
    description: Optional[str] = None
    target_type: str = "return"
    horizon: int = 5
    benchmark: Optional[str] = None


class UpdateLabelRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    target_type: Optional[str] = None
    horizon: Optional[int] = None
    benchmark: Optional[str] = None
    status: Optional[str] = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/labels")
async def create_label(body: CreateLabelRequest) -> dict:
    """Create a new label definition."""
    svc = _get_service()
    try:
        return svc.create_label(
            name=body.name,
            description=body.description,
            target_type=body.target_type,
            horizon=body.horizon,
            benchmark=body.benchmark,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/labels")
async def list_labels() -> list[dict]:
    """List all label definitions."""
    svc = _get_service()
    return svc.list_labels()


@router.get("/labels/{label_id}")
async def get_label(label_id: str) -> dict:
    """Get label definition detail."""
    svc = _get_service()
    try:
        return svc.get_label(label_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/labels/{label_id}")
async def update_label(label_id: str, body: UpdateLabelRequest) -> dict:
    """Update a label definition."""
    svc = _get_service()
    try:
        return svc.update_label(
            label_id=label_id,
            name=body.name,
            description=body.description,
            target_type=body.target_type,
            horizon=body.horizon,
            benchmark=body.benchmark,
            status=body.status,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/labels/{label_id}")
async def delete_label(label_id: str) -> dict:
    """Delete a label definition."""
    svc = _get_service()
    try:
        svc.delete_label(label_id)
        return {"status": "deleted", "id": label_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
