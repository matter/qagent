"""Factor CRUD service – register, version, and manage factor definitions."""

from __future__ import annotations

import json
import uuid
from datetime import datetime

from backend.db import get_connection
from backend.factors.builtins import TEMPLATES, get_template_names, get_template_source
from backend.factors.loader import load_factor_from_code
from backend.logger import get_logger

log = get_logger(__name__)


class FactorService:
    """CRUD operations for factor definitions stored in DuckDB."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_builtin_templates(self) -> None:
        """Register built-in factor templates in the factors table if absent."""
        conn = get_connection()
        for tpl_name in get_template_names():
            row = conn.execute(
                "SELECT id FROM factors WHERE name = ? AND version = 1",
                [tpl_name],
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

            factor_id = f"builtin_{tpl_name.lower()}"
            now = datetime.utcnow()

            conn.execute(
                """INSERT INTO factors
                   (id, name, version, description, category, source_code, params, status, created_at, updated_at)
                   VALUES (?, ?, 1, ?, ?, ?, ?, 'active', ?, ?)""",
                [
                    factor_id,
                    tpl_name,
                    description,
                    category,
                    source,
                    json.dumps(params),
                    now,
                    now,
                ],
            )
            log.info("factor.builtin_registered", name=tpl_name)

    def create_factor(
        self,
        name: str,
        source_code: str,
        description: str | None = None,
        category: str = "custom",
        params: dict | None = None,
    ) -> dict:
        """Create a new factor (version 1)."""
        # Validate that source_code is loadable
        try:
            instance = load_factor_from_code(source_code)
        except Exception as exc:
            raise ValueError(f"Invalid factor source code: {exc}") from exc

        conn = get_connection()

        # Check for name conflict
        existing = conn.execute(
            "SELECT MAX(version) FROM factors WHERE name = ?", [name]
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
               (id, name, version, description, category, source_code, params, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [
                factor_id,
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
        log.info("factor.created", id=factor_id, name=name, version=version)
        return self.get_factor(factor_id)

    def update_factor(
        self,
        factor_id: str,
        source_code: str | None = None,
        description: str | None = None,
        category: str | None = None,
        params: dict | None = None,
        status: str | None = None,
    ) -> dict:
        """Update a factor – if source_code changes, create a new version."""
        conn = get_connection()
        existing = self._fetch_row(factor_id)
        if existing is None:
            raise ValueError(f"Factor {factor_id} not found")

        if source_code is not None and source_code != existing["source_code"]:
            # Validate new source code
            try:
                load_factor_from_code(source_code)
            except Exception as exc:
                raise ValueError(f"Invalid factor source code: {exc}") from exc

            # Create a new version
            max_ver = conn.execute(
                "SELECT MAX(version) FROM factors WHERE name = ?",
                [existing["name"]],
            ).fetchone()
            new_version = (max_ver[0] or 0) + 1
            new_id = uuid.uuid4().hex[:12]
            now = datetime.utcnow()

            conn.execute(
                """INSERT INTO factors
                   (id, name, version, description, category, source_code, params, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    new_id,
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
            log.info("factor.new_version", id=new_id, name=existing["name"], version=new_version)
            return self.get_factor(new_id)

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
        conn.execute(
            f"UPDATE factors SET {', '.join(sets)} WHERE id = ?", vals
        )
        log.info("factor.updated", id=factor_id)
        return self.get_factor(factor_id)

    def delete_factor(self, factor_id: str) -> None:
        """Delete a factor and its cached values."""
        conn = get_connection()
        existing = self._fetch_row(factor_id)
        if existing is None:
            raise ValueError(f"Factor {factor_id} not found")

        conn.execute("DELETE FROM factor_values_cache WHERE factor_id = ?", [factor_id])
        conn.execute("DELETE FROM factors WHERE id = ?", [factor_id])
        log.info("factor.deleted", id=factor_id)

    def get_factor(self, factor_id: str) -> dict:
        """Return a single factor definition."""
        row = self._fetch_row(factor_id)
        if row is None:
            raise ValueError(f"Factor {factor_id} not found")
        return row

    def list_factors(
        self,
        category: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        """List factors with optional filters."""
        conn = get_connection()
        where_parts: list[str] = []
        params: list = []

        if category:
            where_parts.append("category = ?")
            params.append(category)
        if status:
            where_parts.append("status = ?")
            params.append(status)

        where_clause = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        rows = conn.execute(
            f"""SELECT id, name, version, description, category, source_code, params,
                       status, created_at, updated_at
                FROM factors{where_clause}
                ORDER BY name, version""",
            params,
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_row(self, factor_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, version, description, category, source_code, params,
                      status, created_at, updated_at
               FROM factors WHERE id = ?""",
            [factor_id],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        params_raw = row[6]
        if isinstance(params_raw, str):
            try:
                params_parsed = json.loads(params_raw)
            except (json.JSONDecodeError, TypeError):
                params_parsed = {}
        else:
            params_parsed = params_raw if params_raw else {}

        return {
            "id": row[0],
            "name": row[1],
            "version": row[2],
            "description": row[3],
            "category": row[4],
            "source_code": row[5],
            "params": params_parsed,
            "status": row[7],
            "created_at": str(row[8]) if row[8] else None,
            "updated_at": str(row[9]) if row[9] else None,
        }
