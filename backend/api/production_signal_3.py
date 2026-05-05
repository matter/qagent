"""QAgent 3.0 production signal, paper, and reproducibility API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.services.production_signal_3_service import ProductionSignal3Service

router = APIRouter(prefix="/api/research-assets", tags=["research-assets"])


class GenerateProductionSignalRequest(BaseModel):
    strategy_graph_id: str
    decision_date: str
    alpha_frame: list[dict] | None = None
    legacy_signal_frame: list[dict] | None = None
    current_weights: dict[str, float] | None = None
    portfolio_value: float = 1_000_000
    qa_report_id: str | None = None
    approved_by: str = "api"


class CreatePaperSessionRequest(BaseModel):
    strategy_graph_id: str
    start_date: str
    name: str | None = None
    initial_capital: float = 1_000_000
    config: dict | None = None


class AdvancePaperSessionRequest(BaseModel):
    decision_date: str
    alpha_frame: list[dict] | None = None
    legacy_signal_frame: list[dict] | None = None


class ExportBundleRequest(BaseModel):
    source_type: str = "strategy_graph"
    source_id: str
    name: str | None = None


def _svc() -> ProductionSignal3Service:
    return ProductionSignal3Service()


@router.post("/production-signals/generate")
async def generate_production_signal(body: GenerateProductionSignalRequest) -> dict:
    try:
        return _svc().generate_production_signal(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/production-signals")
async def list_production_signals(
    strategy_graph_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    return _svc().list_production_signal_runs(
        strategy_graph_id=strategy_graph_id,
        limit=limit,
    )


@router.get("/production-signals/{signal_run_id}")
async def get_production_signal(signal_run_id: str) -> dict:
    try:
        return _svc().get_production_signal_run(signal_run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/paper-sessions")
async def create_paper_session(body: CreatePaperSessionRequest) -> dict:
    try:
        return _svc().create_paper_session(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/paper-sessions")
async def list_paper_sessions_3(
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    return _svc().list_paper_sessions(status=status, limit=limit)


@router.get("/paper-sessions/{session_id}")
async def get_paper_session(session_id: str) -> dict:
    try:
        return _svc().get_paper_session(session_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/paper-sessions/{session_id}/advance")
async def advance_paper_session(session_id: str, body: AdvancePaperSessionRequest) -> dict:
    try:
        return _svc().advance_paper_session(session_id, **body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/reproducibility-bundles")
async def export_reproducibility_bundle(body: ExportBundleRequest) -> dict:
    try:
        return _svc().export_reproducibility_bundle(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/reproducibility-bundles/{bundle_id}")
async def get_reproducibility_bundle(bundle_id: str) -> dict:
    try:
        return _svc().get_reproducibility_bundle(bundle_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
