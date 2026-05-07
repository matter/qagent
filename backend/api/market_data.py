"""QAgent 3.0 market/data foundation endpoints."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.data_quality_service import DataQualityService
from backend.services.market_data_foundation_service import MarketDataFoundationService

router = APIRouter(prefix="/api/market-data", tags=["market-data"])


class QueryBarsRequest(BaseModel):
    project_id: str | None = None
    market_profile_id: str | None = None
    asset_ids: list[str] = Field(default_factory=list)
    start: date
    end: date
    limit: int = Field(default=10000, ge=1, le=100000)


def _svc() -> MarketDataFoundationService:
    return MarketDataFoundationService()


def _quality_svc() -> DataQualityService:
    return DataQualityService()


@router.get("/profiles")
async def list_market_profiles() -> list[dict]:
    return _svc().list_market_profiles()


@router.get("/provider-capabilities")
async def list_provider_capabilities(
    provider: str | None = Query(None),
    market_profile_id: str | None = Query(None),
    dataset: str | None = Query(None),
) -> list[dict]:
    return _quality_svc().list_provider_capabilities(
        provider=provider,
        market_profile_id=market_profile_id,
        dataset=dataset,
    )


@router.get("/quality-contract")
async def get_data_quality_contract(
    market_profile_id: str | None = Query(None),
) -> dict:
    return _quality_svc().get_data_quality_contract(market_profile_id=market_profile_id)


@router.get("/profiles/{profile_id}")
async def get_market_profile(profile_id: str) -> dict:
    try:
        return _svc().get_market_profile(profile_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/projects/{project_id}/context")
async def get_project_market_context(project_id: str) -> dict:
    try:
        return _svc().get_project_market_context(project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/projects/{project_id}/status")
async def get_project_data_status(project_id: str) -> dict:
    try:
        return _svc().get_project_data_status(project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/assets/search")
async def search_assets(
    q: str = Query("", description="Symbol or name query"),
    project_id: str | None = Query(None),
    market_profile_id: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
) -> list[dict]:
    try:
        return _svc().search_assets(
            project_id=project_id,
            market_profile_id=market_profile_id,
            query=q,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/bars/query")
async def query_bars(body: QueryBarsRequest) -> dict:
    try:
        return _svc().query_bars(
            project_id=body.project_id,
            market_profile_id=body.market_profile_id,
            asset_ids=body.asset_ids,
            start=body.start,
            end=body.end,
            limit=body.limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
