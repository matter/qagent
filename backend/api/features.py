"""Feature set CRUD + correlation API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.feature_service import FeatureService
from backend.services.group_service import GroupService

log = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["feature-sets"])

_service: FeatureService | None = None
_group_service: GroupService | None = None


def _get_service() -> FeatureService:
    global _service
    if _service is None:
        _service = FeatureService()
    return _service


def _get_group_service() -> GroupService:
    global _group_service
    if _group_service is None:
        _group_service = GroupService()
    return _group_service


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class CreateFeatureSetRequest(BaseModel):
    market: Optional[str] = None
    name: str
    description: Optional[str] = None
    factor_refs: list[dict]         # [{factor_id, factor_name, version}]
    preprocessing: Optional[dict] = None


class UpdateFeatureSetRequest(BaseModel):
    market: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    factor_refs: Optional[list[dict]] = None
    preprocessing: Optional[dict] = None
    status: Optional[str] = None


class CorrelationRequest(BaseModel):
    market: Optional[str] = None
    universe_group_id: str
    start_date: str
    end_date: str


# ------------------------------------------------------------------
# CRUD Endpoints
# ------------------------------------------------------------------


@router.post("/feature-sets")
async def create_feature_set(body: CreateFeatureSetRequest) -> dict:
    """Create a new feature set."""
    svc = _get_service()
    try:
        return svc.create_feature_set(
            name=body.name,
            description=body.description,
            factor_refs=body.factor_refs,
            preprocessing=body.preprocessing,
            market=body.market,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/feature-sets")
async def list_feature_sets(market: Optional[str] = Query(None)) -> list[dict]:
    """List all feature sets."""
    svc = _get_service()
    try:
        return svc.list_feature_sets(market=market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/feature-sets/{fs_id}")
async def get_feature_set(fs_id: str, market: Optional[str] = Query(None)) -> dict:
    """Get feature set detail."""
    svc = _get_service()
    try:
        return svc.get_feature_set(fs_id, market=market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/feature-sets/{fs_id}")
async def update_feature_set(fs_id: str, body: UpdateFeatureSetRequest) -> dict:
    """Update a feature set."""
    svc = _get_service()
    try:
        return svc.update_feature_set(
            fs_id=fs_id,
            name=body.name,
            description=body.description,
            factor_refs=body.factor_refs,
            preprocessing=body.preprocessing,
            status=body.status,
            market=body.market,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/feature-sets/{fs_id}")
async def delete_feature_set(fs_id: str, market: Optional[str] = Query(None)) -> dict:
    """Delete a feature set."""
    svc = _get_service()
    try:
        svc.delete_feature_set(fs_id, market=market)
        return {"status": "deleted", "id": fs_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------------------------------------------------
# Correlation Endpoint
# ------------------------------------------------------------------


@router.post("/feature-sets/{fs_id}/correlation")
async def compute_correlation(fs_id: str, body: CorrelationRequest) -> dict:
    """Compute correlation matrix between factors in the feature set.

    Synchronous for small sets; returns the full matrix.
    """
    svc = _get_service()
    gsvc = _get_group_service()

    try:
        svc.get_feature_set(fs_id, market=body.market)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    try:
        tickers = gsvc.get_group_tickers(body.universe_group_id, market=body.market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not tickers:
        raise HTTPException(status_code=400, detail="Universe group has no members")

    try:
        return svc.compute_correlation_matrix(
            fs_id=fs_id,
            tickers=tickers,
            start_date=body.start_date,
            end_date=body.end_date,
            market=body.market,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error("api.feature_set.correlation_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Correlation computation failed: {e}")
