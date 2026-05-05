"""Universe asset service for QAgent 3.0.

M4 makes universes first-class research assets.  The service owns 3.0 universe
metadata and materialized memberships, while legacy stock groups remain only an
input adapter.
"""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

import pandas as pd

from backend.db import get_connection
from backend.services.calendar_service import get_trading_days
from backend.services.market_context import normalize_market, normalize_ticker
from backend.services.market_data_foundation_service import MarketDataFoundationService
from backend.services.research_kernel_service import ResearchKernelService
from backend.time_utils import utc_now_naive


_PROFILE_BY_MARKET = {"US": "US_EQ", "CN": "CN_A"}
_MARKET_BY_PROFILE = {"US_EQ": "US", "CN_A": "CN"}


class UniverseService:
    """Create, materialize, and profile project-scoped universes."""

    def __init__(
        self,
        *,
        kernel_service: ResearchKernelService | None = None,
        market_data_service: MarketDataFoundationService | None = None,
    ) -> None:
        self._kernel = kernel_service or ResearchKernelService()
        self._market_data = market_data_service or MarketDataFoundationService()

    def create_static_universe(
        self,
        *,
        name: str,
        tickers: list[str],
        project_id: str | None = None,
        market_profile_id: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        project = self._kernel.get_project(project_id)
        profile_id = market_profile_id or project["market_profile_id"]
        market = self._market_for_profile(profile_id)
        normalized = sorted({normalize_ticker(t, market) for t in tickers if str(t).strip()})
        if not normalized:
            raise ValueError("tickers must contain at least one symbol")

        universe_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO universes
               (id, project_id, market_profile_id, name, description,
                universe_type, source_ref, lifecycle_stage, status, metadata,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'static', ?, ?, 'draft', ?, ?, ?)""",
            [
                universe_id,
                project["id"],
                profile_id,
                name.strip(),
                description,
                json.dumps({"tickers": normalized}, default=str),
                lifecycle_stage,
                json.dumps(metadata or {}, default=str),
                now,
                now,
            ],
        )
        return self.get_universe(universe_id)

    def create_from_legacy_group(
        self,
        *,
        legacy_group_id: str,
        project_id: str | None = None,
        market: str | None = None,
        name: str | None = None,
        description: str | None = None,
        lifecycle_stage: str = "experiment",
    ) -> dict:
        from backend.services.group_service import GroupService

        resolved_market = normalize_market(market)
        group = GroupService().get_group(legacy_group_id, market=resolved_market)
        if not group["tickers"]:
            raise ValueError(f"Legacy group {legacy_group_id} has no members")

        project = self._kernel.get_project(project_id)
        profile_id = _PROFILE_BY_MARKET[resolved_market]
        universe_id = uuid.uuid4().hex[:12]
        now = utc_now_naive()
        get_connection().execute(
            """INSERT INTO universes
               (id, project_id, market_profile_id, name, description,
                universe_type, source_ref, lifecycle_stage, status, metadata,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'legacy_group', ?, ?, 'draft', ?, ?, ?)""",
            [
                universe_id,
                project["id"],
                profile_id,
                name or group["name"],
                description or group.get("description"),
                json.dumps(
                    {
                        "legacy_group_id": legacy_group_id,
                        "legacy_market": resolved_market,
                        "tickers": group["tickers"],
                    },
                    default=str,
                ),
                lifecycle_stage,
                json.dumps({"legacy_group": True}, default=str),
                now,
                now,
            ],
        )
        return self.get_universe(universe_id)

    def get_universe(self, universe_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, project_id, market_profile_id, name, description,
                      universe_type, source_ref, filter_expr, lifecycle_stage,
                      status, metadata, created_at, updated_at
               FROM universes
               WHERE id = ?""",
            [universe_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Universe {universe_id} not found")
        return self._universe_row(row)

    def list_universes(
        self,
        *,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = """SELECT id, project_id, market_profile_id, name, description,
                          universe_type, source_ref, filter_expr, lifecycle_stage,
                          status, metadata, created_at, updated_at
                   FROM universes
                   WHERE 1 = 1"""
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if market_profile_id:
            query += " AND market_profile_id = ?"
            params.append(market_profile_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = get_connection().execute(query, params).fetchall()
        return [self._universe_row(row) for row in rows]

    def materialize_universe(
        self,
        universe_id: str,
        *,
        start_date: str,
        end_date: str,
        lifecycle_stage: str = "experiment",
    ) -> dict:
        universe = self.get_universe(universe_id)
        market = self._market_for_profile(universe["market_profile_id"])
        tickers = self._resolve_tickers(universe)
        if not tickers:
            raise ValueError(f"Universe {universe_id} has no members")

        self._market_data.sync_assets_from_legacy_stocks(universe["market_profile_id"])
        assets = self._asset_rows(universe["market_profile_id"], tickers)
        by_symbol = {asset["symbol"]: asset for asset in assets}
        missing_assets = [ticker for ticker in tickers if ticker not in by_symbol]
        sessions = get_trading_days(date.fromisoformat(start_date), date.fromisoformat(end_date), market=market)
        if not sessions:
            raise ValueError("No trading days resolved for universe materialization")

        run = self._kernel.create_run(
            run_type="universe_materialize",
            project_id=universe["project_id"],
            market_profile_id=universe["market_profile_id"],
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            created_by="universe_service",
            params={
                "universe_id": universe_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        )

        rows: list[dict[str, Any]] = []
        for session in sessions:
            for ticker in tickers:
                asset = by_symbol.get(ticker)
                if asset is None:
                    continue
                rows.append(
                    {
                        "date": pd.Timestamp(session),
                        "asset_id": asset["asset_id"],
                        "symbol": asset["symbol"],
                        "membership_state": "active",
                        "available_at": pd.Timestamp(session),
                    }
                )

        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError(f"Universe {universe_id} produced no materialized rows")

        conn = get_connection()
        conn.execute("DELETE FROM universe_memberships WHERE universe_id = ?", [universe_id])
        memberships = frame[["date", "asset_id", "membership_state", "available_at"]].copy()
        memberships["universe_id"] = universe_id
        memberships["run_id"] = run["id"]
        memberships["market_profile_id"] = universe["market_profile_id"]
        memberships["metadata"] = None
        memberships = memberships[
            [
                "universe_id",
                "run_id",
                "market_profile_id",
                "date",
                "asset_id",
                "membership_state",
                "available_at",
                "metadata",
            ]
        ]
        conn.register("_universe_memberships", memberships)
        try:
            conn.execute(
                """INSERT INTO universe_memberships
                   SELECT universe_id, run_id, market_profile_id, date, asset_id,
                          membership_state, available_at, metadata
                   FROM _universe_memberships"""
            )
        finally:
            conn.unregister("_universe_memberships")

        profile = self._profile_frame(
            frame,
            universe=universe,
            requested_tickers=tickers,
            missing_assets=missing_assets,
        )
        artifact = self._kernel.create_dataframe_artifact(
            run_id=run["id"],
            artifact_type="universe_materialization",
            frame=frame,
            lifecycle_stage=lifecycle_stage,
            retention_class="rebuildable",
            metadata={
                "universe_id": universe_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        self._kernel.add_lineage(
            from_type="universe",
            from_id=universe_id,
            to_type="artifact",
            to_id=artifact["id"],
            relation="materialized",
            metadata={"run_id": run["id"]},
        )
        self._kernel.update_run_status(
            run["id"],
            status="completed",
            metrics_summary={
                "artifact_id": artifact["id"],
                "member_count": len(frame),
                "asset_count": int(frame["asset_id"].nunique()),
            },
            qa_summary={"missing_assets": missing_assets},
        )
        conn.execute(
            """UPDATE universes
                  SET status = 'materialized', updated_at = ?
                WHERE id = ?""",
            [utc_now_naive(), universe_id],
        )

        materialization = {
            "universe_id": universe_id,
            "run_id": run["id"],
            "artifact_id": artifact["id"],
            "start_date": start_date,
            "end_date": end_date,
            "member_count": len(frame),
            "asset_count": int(frame["asset_id"].nunique()),
            "date_count": int(frame["date"].nunique()),
            "missing_assets": missing_assets,
        }
        return {
            "universe": self.get_universe(universe_id),
            "run": self._kernel.get_run(run["id"]),
            "artifact": artifact,
            "materialization": materialization,
            "profile": profile,
        }

    def profile_universe(
        self,
        universe_id: str,
        *,
        run_id: str | None = None,
    ) -> dict:
        universe = self.get_universe(universe_id)
        query = """SELECT date, asset_id, membership_state, available_at
                   FROM universe_memberships
                   WHERE universe_id = ?"""
        params: list[Any] = [universe_id]
        if run_id:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " ORDER BY date, asset_id"
        frame = get_connection().execute(query, params).fetchdf()
        if frame.empty:
            return {
                "universe_id": universe_id,
                "coverage": {
                    "row_count": 0,
                    "asset_count": 0,
                    "date_count": 0,
                    "date_range": {"start": None, "end": None},
                },
                "missing_assets": [],
            }
        return self._profile_frame(frame, universe=universe, requested_tickers=[], missing_assets=[])

    def latest_materialized_run_id(self, universe_id: str) -> str | None:
        row = get_connection().execute(
            """SELECT run_id
               FROM universe_memberships
               WHERE universe_id = ?
               GROUP BY run_id
               ORDER BY MAX(date) DESC, COUNT(*) DESC
               LIMIT 1""",
            [universe_id],
        ).fetchone()
        return str(row[0]) if row else None

    def _resolve_tickers(self, universe: dict) -> list[str]:
        source_ref = universe.get("source_ref") or {}
        tickers = source_ref.get("tickers") or []
        market = self._market_for_profile(universe["market_profile_id"])
        return sorted({normalize_ticker(ticker, market) for ticker in tickers if str(ticker).strip()})

    def _asset_rows(self, market_profile_id: str, tickers: list[str]) -> list[dict[str, Any]]:
        if not tickers:
            return []
        placeholders = ",".join("?" for _ in tickers)
        rows = get_connection().execute(
            f"""SELECT asset_id, symbol, display_symbol, name, exchange, sector,
                       industry, status
                FROM assets
                WHERE market_profile_id = ?
                  AND symbol IN ({placeholders})
                ORDER BY symbol""",
            [market_profile_id, *tickers],
        ).fetchall()
        return [
            {
                "asset_id": row[0],
                "symbol": row[1],
                "display_symbol": row[2],
                "name": row[3],
                "exchange": row[4],
                "sector": row[5],
                "industry": row[6],
                "status": row[7],
            }
            for row in rows
        ]

    @staticmethod
    def _profile_frame(
        frame: pd.DataFrame,
        *,
        universe: dict,
        requested_tickers: list[str],
        missing_assets: list[str],
    ) -> dict:
        dates = pd.to_datetime(frame["date"])
        per_date = frame.groupby("date")["asset_id"].nunique()
        return {
            "universe_id": universe["id"],
            "market_profile_id": universe["market_profile_id"],
            "coverage": {
                "row_count": int(len(frame)),
                "asset_count": int(frame["asset_id"].nunique()),
                "date_count": int(dates.nunique()),
                "date_range": {
                    "start": str(dates.min().date()) if len(dates) else None,
                    "end": str(dates.max().date()) if len(dates) else None,
                },
                "avg_assets_per_date": round(float(per_date.mean()), 4) if not per_date.empty else 0.0,
                "min_assets_per_date": int(per_date.min()) if not per_date.empty else 0,
                "max_assets_per_date": int(per_date.max()) if not per_date.empty else 0,
            },
            "requested_ticker_count": len(requested_tickers),
            "missing_assets": missing_assets,
        }

    @staticmethod
    def _universe_row(row: tuple) -> dict:
        return {
            "id": row[0],
            "project_id": row[1],
            "market_profile_id": row[2],
            "name": row[3],
            "description": row[4],
            "universe_type": row[5],
            "source_ref": _json(row[6], {}),
            "filter_expr": row[7],
            "lifecycle_stage": row[8],
            "status": row[9],
            "metadata": _json(row[10], {}),
            "created_at": str(row[11]) if row[11] else None,
            "updated_at": str(row[12]) if row[12] else None,
        }

    @staticmethod
    def _market_for_profile(profile_id: str) -> str:
        market = _MARKET_BY_PROFILE.get(profile_id)
        if market is None:
            raise ValueError(f"Unsupported market profile {profile_id}")
        return market


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value
