"""Factor CRUD service – register, version, and manage factor definitions."""

from __future__ import annotations

import json
import uuid
from datetime import datetime

from backend.db import get_connection
from backend.factors.builtins import TEMPLATES, get_template_names, get_template_source
from backend.factors.loader import load_factor_from_code
from backend.logger import get_logger
from backend.services.market_context import normalize_market

log = get_logger(__name__)


class FactorService:
    """CRUD operations for factor definitions stored in DuckDB."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_builtin_templates(self, market: str | None = None) -> None:
        """Register built-in factor templates in the factors table if absent."""
        resolved_market = normalize_market(market)
        conn = get_connection()
        for tpl_name in get_template_names():
            factor_id = f"builtin_{tpl_name.lower()}" if resolved_market == "US" else f"{resolved_market.lower()}_builtin_{tpl_name.lower()}"
            factor_name = tpl_name if resolved_market == "US" else f"{resolved_market.lower()}_{tpl_name}"
            row = conn.execute(
                "SELECT id FROM factors WHERE id = ? AND market = ?",
                [factor_id, resolved_market],
            ).fetchone()
            if row is not None:
                continue

            source = get_template_source(tpl_name)
            if source is None:
                continue

            # Validate the template actually loads
            try:
                instance = load_factor_from_code(source)
                category = getattr(instance, "category", "custom")
                description = getattr(instance, "description", "")
                params = getattr(instance, "params", {})
            except Exception as exc:
                log.warning("factor.builtin_template_invalid", name=tpl_name, error=str(exc))
                continue

            now = datetime.utcnow()

            conn.execute(
                """INSERT INTO factors
                   (id, market, name, version, description, category, source_code, params, status, created_at, updated_at)
                   VALUES (?, ?, ?, 1, ?, ?, ?, ?, 'active', ?, ?)""",
                [
                    factor_id,
                    resolved_market,
                    factor_name,
                    description,
                    category,
                    source,
                    json.dumps(params),
                    now,
                    now,
                ],
            )
            log.info("factor.builtin_registered", market=resolved_market, name=factor_name)

    def create_factor(
        self,
        name: str,
        source_code: str,
        description: str | None = None,
        category: str = "custom",
        params: dict | None = None,
        market: str | None = None,
    ) -> dict:
        """Create a new factor (version 1)."""
        resolved_market = normalize_market(market)
        # Validate that source_code is loadable
        try:
            instance = load_factor_from_code(source_code)
        except Exception as exc:
            raise ValueError(f"Invalid factor source code: {exc}") from exc

        conn = get_connection()

        # Check for name conflict
        existing = conn.execute(
            "SELECT MAX(version) FROM factors WHERE market = ? AND name = ?",
            [resolved_market, name],
        ).fetchone()
        version = 1
        if existing and existing[0] is not None:
            raise ValueError(
                f"Factor '{name}' already exists. Use update to create a new version."
            )

        factor_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()

        conn.execute(
            """INSERT INTO factors
               (id, market, name, version, description, category, source_code, params, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [
                factor_id,
                resolved_market,
                name,
                version,
                description or getattr(instance, "description", ""),
                category or getattr(instance, "category", "custom"),
                source_code,
                json.dumps(params or getattr(instance, "params", {})),
                now,
                now,
            ],
        )
        log.info("factor.created", id=factor_id, market=resolved_market, name=name, version=version)
        return self.get_factor(factor_id, market=resolved_market)

    def update_factor(
        self,
        factor_id: str,
        source_code: str | None = None,
        description: str | None = None,
        category: str | None = None,
        params: dict | None = None,
        status: str | None = None,
        market: str | None = None,
    ) -> dict:
        """Update a factor – if source_code changes, create a new version."""
        conn = get_connection()
        existing = self._fetch_row(factor_id, market)
        if existing is None:
            raise ValueError(f"Factor {factor_id} not found")
        resolved_market = existing["market"]

        if source_code is not None and source_code != existing["source_code"]:
            # Validate new source code
            try:
                load_factor_from_code(source_code)
            except Exception as exc:
                raise ValueError(f"Invalid factor source code: {exc}") from exc

            # Create a new version
            max_ver = conn.execute(
                "SELECT MAX(version) FROM factors WHERE market = ? AND name = ?",
                [resolved_market, existing["name"]],
            ).fetchone()
            new_version = (max_ver[0] or 0) + 1
            new_id = uuid.uuid4().hex[:12]
            now = datetime.utcnow()

            conn.execute(
                """INSERT INTO factors
                   (id, market, name, version, description, category, source_code, params, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    new_id,
                    resolved_market,
                    existing["name"],
                    new_version,
                    description or existing["description"],
                    category or existing["category"],
                    source_code,
                    json.dumps(params or existing.get("params", {})),
                    status or "draft",
                    now,
                    now,
                ],
            )
            log.info("factor.new_version", id=new_id, market=resolved_market, name=existing["name"], version=new_version)
            return self.get_factor(new_id, market=resolved_market)

        # Simple metadata update (no version bump)
        now = datetime.utcnow()
        sets: list[str] = ["updated_at = ?"]
        vals: list = [now]

        for col, val in [
            ("description", description),
            ("category", category),
            ("status", status),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                vals.append(val)
        if params is not None:
            sets.append("params = ?")
            vals.append(json.dumps(params))

        vals.append(factor_id)
        vals.append(resolved_market)
        conn.execute(
            f"UPDATE factors SET {', '.join(sets)} WHERE id = ? AND market = ?", vals
        )
        log.info("factor.updated", id=factor_id, market=resolved_market)
        return self.get_factor(factor_id, market=resolved_market)

    def delete_factor(self, factor_id: str, market: str | None = None) -> None:
        """Delete a factor and its cached values."""
        conn = get_connection()
        existing = self._fetch_row(factor_id, market)
        if existing is None:
            raise ValueError(f"Factor {factor_id} not found")

        conn.execute(
            "DELETE FROM factor_values_cache WHERE market = ? AND factor_id = ?",
            [existing["market"], factor_id],
        )
        conn.execute("DELETE FROM factors WHERE id = ? AND market = ?", [factor_id, existing["market"]])
        log.info("factor.deleted", id=factor_id, market=existing["market"])

    def get_factor(self, factor_id: str, market: str | None = None) -> dict:
        """Return a single factor definition."""
        row = self._fetch_row(factor_id, market)
        if row is None:
            raise ValueError(f"Factor {factor_id} not found")
        return row

    def list_factors(
        self,
        category: str | None = None,
        status: str | None = None,
        market: str | None = None,
    ) -> list[dict]:
        """List factors with optional filters.

        Includes ``latest_ir`` for each factor via a single LEFT JOIN,
        avoiding N+1 per-factor evaluation queries.
        """
        resolved_market = normalize_market(market)
        conn = get_connection()
        where_parts: list[str] = ["f.market = ?"]
        params: list = [resolved_market]

        if category:
            where_parts.append("f.category = ?")
            params.append(category)
        if status:
            where_parts.append("f.status = ?")
            params.append(status)

        where_clause = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        rows = conn.execute(
            f"""SELECT f.id, f.market, f.name, f.version, f.description, f.category,
                       f.source_code, f.params, f.status, f.created_at, f.updated_at,
                       le.summary AS latest_eval_summary
                FROM factors f
                LEFT JOIN (
                    SELECT factor_id, summary,
                           ROW_NUMBER() OVER (PARTITION BY factor_id ORDER BY created_at DESC) AS rn
                    FROM factor_eval_results
                    WHERE market = ?
                ) le ON le.factor_id = f.id AND le.rn = 1
                {where_clause}
                ORDER BY f.name, f.version""",
            [resolved_market, *params],
        ).fetchall()

        results = []
        for r in rows:
            d = self._row_to_dict(r)
            # Extract latest IR from joined eval summary
            summary_raw = r[11]
            ir = None
            if summary_raw is not None:
                if isinstance(summary_raw, str):
                    try:
                        summary_parsed = json.loads(summary_raw)
                        ir = summary_parsed.get("ir")
                    except (json.JSONDecodeError, TypeError):
                        pass
                elif isinstance(summary_raw, dict):
                    ir = summary_raw.get("ir")
            d["latest_ir"] = ir
            results.append(d)
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_row(self, factor_id: str, market: str | None = None) -> dict | None:
        resolved_market = normalize_market(market)
        conn = get_connection()
        row = conn.execute(
            """SELECT id, market, name, version, description, category, source_code, params,
                      status, created_at, updated_at
               FROM factors WHERE id = ? AND market = ?""",
            [factor_id, resolved_market],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        params_raw = row[7]
        if isinstance(params_raw, str):
            try:
                params_parsed = json.loads(params_raw)
            except (json.JSONDecodeError, TypeError):
                params_parsed = {}
        else:
            params_parsed = params_raw if params_raw else {}

        return {
            "id": row[0],
            "market": row[1],
            "name": row[2],
            "version": row[3],
            "description": row[4],
            "category": row[5],
            "source_code": row[6],
            "params": params_parsed,
            "status": row[8],
            "created_at": str(row[9]) if row[9] else None,
            "updated_at": str(row[10]) if row[10] else None,
        }
