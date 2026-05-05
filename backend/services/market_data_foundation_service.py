"""Market/Data Foundation service for QAgent 3.0.

M2 introduces project-scoped market semantics while keeping the 2.0 storage
tables usable.  The service is the compatibility boundary: callers work with
market profiles and stable asset_id values; legacy stocks/daily_bars remain an
implementation detail until the full migration layer lands.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from backend.db import get_connection
from backend.services.calendar_service import get_latest_trading_day
from backend.services.market_context import normalize_market


class MarketDataFoundationService:
    """Resolve market profiles, assets, and project-scoped market data."""

    # ------------------------------------------------------------------
    # Market profiles and project context
    # ------------------------------------------------------------------

    def list_market_profiles(self) -> list[dict]:
        rows = get_connection().execute(
            """SELECT id, market_code, asset_class, name, currency, timezone,
                      symbol_format, provider_symbol_format, data_policy_id,
                      trading_rule_set_id, cost_model_id, benchmark_policy_id,
                      status, metadata, created_at, updated_at
               FROM market_profiles
               ORDER BY id"""
        ).fetchall()
        return [self._market_profile_row(row, include_policies=False) for row in rows]

    def get_market_profile(self, profile_id: str) -> dict:
        row = get_connection().execute(
            """SELECT id, market_code, asset_class, name, currency, timezone,
                      symbol_format, provider_symbol_format, data_policy_id,
                      trading_rule_set_id, cost_model_id, benchmark_policy_id,
                      status, metadata, created_at, updated_at
               FROM market_profiles
               WHERE id = ?""",
            [profile_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Market profile {profile_id} not found")
        return self._market_profile_row(row, include_policies=True)

    def get_project_market_context(self, project_id: str | None = None) -> dict:
        project = self._get_project(project_id)
        profile = self.get_market_profile(project["market_profile_id"])
        return {"project": project, "market_profile": profile}

    # ------------------------------------------------------------------
    # Data status and snapshots
    # ------------------------------------------------------------------

    def get_project_data_status(self, project_id: str | None = None) -> dict:
        context = self.get_project_market_context(project_id)
        project = context["project"]
        profile = context["market_profile"]
        market = normalize_market(profile["market_code"])
        self.sync_assets_from_legacy_stocks(profile["id"])

        conn = get_connection()
        asset_count = conn.execute(
            "SELECT COUNT(*) FROM assets WHERE market_profile_id = ?",
            [profile["id"]],
        ).fetchone()[0]
        active_asset_count = conn.execute(
            """SELECT COUNT(*) FROM assets
               WHERE market_profile_id = ? AND status = 'active'""",
            [profile["id"]],
        ).fetchone()[0]
        bar_stats = conn.execute(
            """SELECT COUNT(DISTINCT ticker), MIN(date), MAX(date), COUNT(*)
               FROM daily_bars
               WHERE market = ?""",
            [market],
        ).fetchone()
        last_update = conn.execute(
            """SELECT completed_at, status, update_type
               FROM data_update_log
               WHERE market = ?
               ORDER BY started_at DESC LIMIT 1""",
            [market],
        ).fetchone()

        latest_trading_day = get_latest_trading_day(market)
        stale_cutoff = latest_trading_day - timedelta(days=3)
        stale_assets = conn.execute(
            """SELECT COUNT(*) FROM assets a
               WHERE a.market_profile_id = ?
                 AND EXISTS (
                   SELECT 1 FROM daily_bars b
                   WHERE b.market = ? AND b.ticker = a.symbol
                   GROUP BY b.ticker
                   HAVING MAX(b.date) < ?
                 )""",
            [profile["id"], market, stale_cutoff],
        ).fetchone()[0]

        snapshot = self._latest_snapshot(profile["id"])
        return {
            "project_id": project["id"],
            "market_profile_id": profile["id"],
            "market": market,
            "provider": profile["data_policy"]["provider"],
            "latest_trading_day": str(latest_trading_day),
            "coverage": {
                "asset_count": asset_count,
                "active_asset_count": active_asset_count,
                "tickers_with_bars": bar_stats[0] if bar_stats[0] else 0,
                "total_bars": bar_stats[3] if bar_stats[3] else 0,
                "date_range": {
                    "min": str(bar_stats[1]) if bar_stats[1] else None,
                    "max": str(bar_stats[2]) if bar_stats[2] else None,
                },
                "stale_assets": stale_assets,
            },
            "last_update": {
                "completed_at": str(last_update[0]) if last_update else None,
                "status": last_update[1] if last_update else None,
                "type": last_update[2] if last_update else None,
            },
            "latest_snapshot": snapshot,
            "semantics": {
                "bar_availability": profile["data_policy"]["bar_availability"],
                "decision_to_execution": profile["trading_rule_set"]["decision_to_execution"],
                "calendar": profile["trading_rule_set"]["calendar"],
            },
        }

    # ------------------------------------------------------------------
    # Assets and bars
    # ------------------------------------------------------------------

    def sync_assets_from_legacy_stocks(self, market_profile_id: str) -> dict:
        """Populate 3.0 asset rows from legacy stocks without changing 2.0 tables."""
        profile = self.get_market_profile(market_profile_id)
        market = normalize_market(profile["market_code"])
        prefix = f"{market_profile_id}:"
        conn = get_connection()
        before = conn.execute(
            "SELECT COUNT(*) FROM assets WHERE market_profile_id = ?",
            [market_profile_id],
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO assets
               (asset_id, market_profile_id, symbol, display_symbol, name,
                exchange, sector, industry, status, metadata, created_at, updated_at)
               SELECT ? || ticker,
                      ?,
                      ticker,
                      ticker,
                      name,
                      exchange,
                      sector,
                      NULL,
                      COALESCE(status, 'active'),
                      NULL,
                      current_timestamp,
                      current_timestamp
                 FROM stocks s
                WHERE s.market = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM assets a WHERE a.asset_id = ? || s.ticker
                  )""",
            [prefix, market_profile_id, market, prefix],
        )
        conn.execute(
            """INSERT INTO asset_identifiers
               (asset_id, identifier_type, identifier_value, valid_from, valid_to, metadata)
               SELECT a.asset_id,
                      'ticker',
                      a.symbol,
                      DATE '1900-01-01',
                      NULL,
                      NULL
                 FROM assets a
                WHERE a.market_profile_id = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM asset_identifiers i
                       WHERE i.asset_id = a.asset_id
                         AND i.identifier_type = 'ticker'
                         AND i.identifier_value = a.symbol
                  )""",
            [market_profile_id],
        )
        after = conn.execute(
            "SELECT COUNT(*) FROM assets WHERE market_profile_id = ?",
            [market_profile_id],
        ).fetchone()[0]
        return {
            "market_profile_id": market_profile_id,
            "legacy_market": market,
            "before": before,
            "after": after,
            "inserted": after - before,
        }

    def search_assets(
        self,
        *,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        query: str = "",
        limit: int = 20,
    ) -> list[dict]:
        profile_id = self._resolve_profile_id(project_id, market_profile_id)
        self.sync_assets_from_legacy_stocks(profile_id)
        q = str(query or "").strip()
        like = f"%{q}%"
        symbol_prefix = f"{q.upper()}%"
        rows = get_connection().execute(
            """SELECT asset_id, market_profile_id, symbol, display_symbol, name,
                      exchange, sector, industry, status, metadata, created_at,
                      updated_at
               FROM assets
               WHERE market_profile_id = ?
                 AND (
                      ? = ''
                      OR UPPER(symbol) LIKE ?
                      OR UPPER(display_symbol) LIKE ?
                      OR UPPER(COALESCE(name, '')) LIKE UPPER(?)
                 )
               ORDER BY
                    CASE WHEN UPPER(symbol) LIKE ? THEN 0 ELSE 1 END,
                    symbol
               LIMIT ?""",
            [profile_id, q, symbol_prefix, symbol_prefix, like, symbol_prefix, limit],
        ).fetchall()
        return [self._asset_row(row) for row in rows]

    def query_bars(
        self,
        *,
        project_id: str | None = None,
        market_profile_id: str | None = None,
        asset_ids: list[str] | None = None,
        start: date | str,
        end: date | str,
        limit: int = 10000,
    ) -> dict:
        profile_id = self._resolve_profile_id(project_id, market_profile_id)
        profile = self.get_market_profile(profile_id)
        market = normalize_market(profile["market_code"])
        self.sync_assets_from_legacy_stocks(profile_id)

        ids = [item for item in (asset_ids or []) if str(item).strip()]
        if not ids:
            return {
                "project_id": self._get_project(project_id)["id"] if project_id else None,
                "market_profile_id": profile_id,
                "market": market,
                "start": str(start),
                "end": str(end),
                "bars": [],
            }

        placeholders = ",".join("?" for _ in ids)
        rows = get_connection().execute(
            f"""SELECT a.asset_id, a.symbol, b.date, b.open, b.high, b.low,
                       b.close, b.volume, b.adj_factor
                FROM assets a
                JOIN daily_bars b
                  ON b.market = ?
                 AND b.ticker = a.symbol
               WHERE a.market_profile_id = ?
                 AND a.asset_id IN ({placeholders})
                 AND b.date BETWEEN ? AND ?
               ORDER BY b.date, a.symbol
               LIMIT ?""",
            [market, profile_id, *ids, start, end, limit],
        ).fetchall()
        return {
            "project_id": self._get_project(project_id)["id"] if project_id else None,
            "market_profile_id": profile_id,
            "market": market,
            "start": str(start),
            "end": str(end),
            "bars": [
                {
                    "asset_id": row[0],
                    "symbol": row[1],
                    "date": str(row[2]),
                    "open": row[3],
                    "high": row[4],
                    "low": row[5],
                    "close": row[6],
                    "volume": row[7],
                    "adj_factor": row[8],
                }
                for row in rows
            ],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_profile_id(
        self,
        project_id: str | None = None,
        market_profile_id: str | None = None,
    ) -> str:
        if market_profile_id:
            return self.get_market_profile(market_profile_id)["id"]
        return self._get_project(project_id)["market_profile_id"]

    def _get_project(self, project_id: str | None = None) -> dict:
        resolved_id = project_id or "bootstrap_us"
        row = get_connection().execute(
            """SELECT id, name, market_profile_id, default_universe_id,
                      data_policy_id, trading_rule_set_id, cost_model_id,
                      benchmark_policy_id, artifact_policy_id, metadata,
                      created_at, updated_at
               FROM research_projects
               WHERE id = ?""",
            [resolved_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Research project {resolved_id} not found")
        return {
            "id": row[0],
            "name": row[1],
            "market_profile_id": row[2],
            "default_universe_id": row[3],
            "data_policy_id": row[4],
            "trading_rule_set_id": row[5],
            "cost_model_id": row[6],
            "benchmark_policy_id": row[7],
            "artifact_policy_id": row[8],
            "metadata": self._json(row[9], {}),
            "created_at": str(row[10]) if row[10] else None,
            "updated_at": str(row[11]) if row[11] else None,
        }

    def _latest_snapshot(self, market_profile_id: str) -> dict | None:
        row = get_connection().execute(
            """SELECT id, market_profile_id, provider, data_policy_id,
                      as_of_date, coverage_summary, quality_summary, created_at
               FROM market_data_snapshots
               WHERE market_profile_id = ?
               ORDER BY created_at DESC
               LIMIT 1""",
            [market_profile_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "market_profile_id": row[1],
            "provider": row[2],
            "data_policy_id": row[3],
            "as_of_date": str(row[4]) if row[4] else None,
            "coverage_summary": self._json(row[5], {}),
            "quality_summary": self._json(row[6], {}),
            "created_at": str(row[7]) if row[7] else None,
        }

    def _get_data_policy(self, policy_id: str | None) -> dict | None:
        if not policy_id:
            return None
        row = get_connection().execute(
            """SELECT id, market_profile_id, provider, price_adjustment,
                      bar_availability, data_quality_level, field_semantics,
                      metadata, created_at, updated_at
               FROM data_policies WHERE id = ?""",
            [policy_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "market_profile_id": row[1],
            "provider": row[2],
            "price_adjustment": row[3],
            "bar_availability": row[4],
            "data_quality_level": row[5],
            "field_semantics": self._json(row[6], {}),
            "metadata": self._json(row[7], {}),
            "created_at": str(row[8]) if row[8] else None,
            "updated_at": str(row[9]) if row[9] else None,
        }

    def _get_trading_rule_set(self, rule_set_id: str | None) -> dict | None:
        if not rule_set_id:
            return None
        row = get_connection().execute(
            """SELECT id, market_profile_id, calendar, decision_to_execution,
                      settlement_cycle, lot_size, allow_short, limit_up_down,
                      tradability_fields, rules, created_at, updated_at
               FROM trading_rule_sets WHERE id = ?""",
            [rule_set_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "market_profile_id": row[1],
            "calendar": row[2],
            "decision_to_execution": row[3],
            "settlement_cycle": row[4],
            "lot_size": row[5],
            "allow_short": bool(row[6]),
            "limit_up_down": bool(row[7]),
            "tradability_fields": self._json(row[8], []),
            "rules": self._json(row[9], {}),
            "created_at": str(row[10]) if row[10] else None,
            "updated_at": str(row[11]) if row[11] else None,
        }

    def _get_cost_model(self, cost_model_id: str | None) -> dict | None:
        if not cost_model_id:
            return None
        row = get_connection().execute(
            """SELECT id, market_profile_id, commission_rate, slippage_rate,
                      stamp_tax_rate, min_commission, currency, metadata,
                      created_at, updated_at
               FROM cost_models WHERE id = ?""",
            [cost_model_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "market_profile_id": row[1],
            "commission_rate": row[2],
            "slippage_rate": row[3],
            "stamp_tax_rate": row[4],
            "min_commission": row[5],
            "currency": row[6],
            "metadata": self._json(row[7], {}),
            "created_at": str(row[8]) if row[8] else None,
            "updated_at": str(row[9]) if row[9] else None,
        }

    def _get_benchmark_policy(self, benchmark_policy_id: str | None) -> dict | None:
        if not benchmark_policy_id:
            return None
        row = get_connection().execute(
            """SELECT id, market_profile_id, default_benchmark, benchmarks,
                      benchmark_semantics, created_at, updated_at
               FROM benchmark_policies WHERE id = ?""",
            [benchmark_policy_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "market_profile_id": row[1],
            "default_benchmark": row[2],
            "benchmarks": self._json(row[3], []),
            "benchmark_semantics": self._json(row[4], {}),
            "created_at": str(row[5]) if row[5] else None,
            "updated_at": str(row[6]) if row[6] else None,
        }

    def _market_profile_row(self, row: tuple, *, include_policies: bool) -> dict:
        profile = {
            "id": row[0],
            "market_code": row[1],
            "asset_class": row[2],
            "name": row[3],
            "currency": row[4],
            "timezone": row[5],
            "symbol_format": row[6],
            "provider_symbol_format": row[7],
            "data_policy_id": row[8],
            "trading_rule_set_id": row[9],
            "cost_model_id": row[10],
            "benchmark_policy_id": row[11],
            "status": row[12],
            "metadata": self._json(row[13], {}),
            "created_at": str(row[14]) if row[14] else None,
            "updated_at": str(row[15]) if row[15] else None,
        }
        if include_policies:
            profile["data_policy"] = self._get_data_policy(profile["data_policy_id"])
            profile["trading_rule_set"] = self._get_trading_rule_set(profile["trading_rule_set_id"])
            profile["cost_model"] = self._get_cost_model(profile["cost_model_id"])
            profile["benchmark_policy"] = self._get_benchmark_policy(profile["benchmark_policy_id"])
        return profile

    def _asset_row(self, row: tuple) -> dict:
        return {
            "asset_id": row[0],
            "market_profile_id": row[1],
            "symbol": row[2],
            "display_symbol": row[3],
            "name": row[4],
            "exchange": row[5],
            "sector": row[6],
            "industry": row[7],
            "status": row[8],
            "metadata": self._json(row[9], {}),
            "created_at": str(row[10]) if row[10] else None,
            "updated_at": str(row[11]) if row[11] else None,
        }

    @staticmethod
    def _json(value: Any, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return default
        return value
