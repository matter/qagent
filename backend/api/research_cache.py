"""Research cache inventory, warmup, and cleanup endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.logger import get_logger
from backend.services.feature_service import FeatureService
from backend.services.group_service import GroupService
from backend.services.market_context import normalize_market
from backend.services.research_cache_service import ResearchCacheService
from backend.tasks.executor import TaskExecutor, get_task_executor
from backend.tasks.models import TaskSource

log = get_logger(__name__)

router = APIRouter(prefix="/api/research-cache", tags=["research-cache"])

_service: ResearchCacheService | None = None
_feature_service: FeatureService | None = None
_group_service: GroupService | None = None


def _get_service() -> ResearchCacheService:
    global _service
    if _service is None:
        _service = ResearchCacheService()
    return _service


def _get_feature_service() -> FeatureService:
    global _feature_service
    if _feature_service is None:
        _feature_service = FeatureService()
    return _feature_service


def _get_group_service() -> GroupService:
    global _group_service
    if _group_service is None:
        _group_service = GroupService()
    return _group_service


def _get_executor() -> TaskExecutor:
    return get_task_executor()


class WarmupFeatureMatrixRequest(BaseModel):
    market: Optional[str] = None
    feature_set_id: str
    universe_group_id: str
    start_date: str
    end_date: str
    timeout: int = 3600


@router.get("/inventory")
async def cache_inventory(
    market: Optional[str] = Query(None),
    object_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
) -> dict:
    svc = _get_service()
    try:
        return {
            "summary": svc.inventory_summary(market=market),
            "entries": svc.list_cache_entries(
                market=market,
                object_type=object_type,
                limit=limit,
            ),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/feature-matrix/warmup")
async def warmup_feature_matrix(body: WarmupFeatureMatrixRequest) -> dict:
    feature_service = _get_feature_service()
    group_service = _get_group_service()
    executor = _get_executor()
    try:
        resolved_market = normalize_market(body.market)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    def _do_warmup(
        market: str,
        feature_set_id: str,
        universe_group_id: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        tickers = group_service.get_group_tickers(universe_group_id, market=market)
        if not tickers:
            raise ValueError("Universe group has no members")
        feature_data = feature_service.compute_features_from_cache(
            fs_id=feature_set_id,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            market=market,
        )
        return {
            "market": market,
            "feature_set_id": feature_set_id,
            "universe_group_id": universe_group_id,
            "start_date": start_date,
            "end_date": end_date,
            "ticker_count": len(tickers),
            "feature_count": len(feature_data),
            "status": "warmed",
        }

    task_id = executor.submit(
        task_type="cache_feature_matrix_warmup",
        fn=_do_warmup,
        params={
            "market": resolved_market,
            "feature_set_id": body.feature_set_id,
            "universe_group_id": body.universe_group_id,
            "start_date": body.start_date,
            "end_date": body.end_date,
        },
        timeout=body.timeout,
        source=TaskSource.UI,
    )
    return {
        "task_id": task_id,
        "status": "queued",
        "market": resolved_market,
        "feature_set_id": body.feature_set_id,
        "universe_group_id": body.universe_group_id,
    }


@router.get("/factor-cache/cleanup-preview")
async def preview_factor_cache_cleanup(
    market: Optional[str] = Query(None),
    include_recent_days: int = Query(0, ge=0, le=3650),
    limit: int = Query(100, ge=1, le=1000),
) -> dict:
    try:
        return _get_service().preview_factor_cache_cleanup(
            market=market,
            include_recent_days=include_recent_days,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/factor-cache/cleanup-apply")
async def apply_factor_cache_cleanup(
    market: Optional[str] = Query(None),
    include_recent_days: int = Query(0, ge=0, le=3650),
    limit: int = Query(100, ge=1, le=1000),
) -> dict:
    try:
        return _get_service().apply_factor_cache_cleanup(
            market=market,
            include_recent_days=include_recent_days,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/expired/cleanup-apply")
async def apply_expired_cache_cleanup(
    market: Optional[str] = Query(None),
    object_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
) -> dict:
    try:
        return _get_service().apply_expired_cache_cleanup(
            market=market,
            object_type=object_type,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
