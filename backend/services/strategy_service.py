"""Strategy CRUD service -- register, version, and manage strategy definitions."""

from __future__ import annotations

import json
import uuid
from datetime import datetime

from backend.db import get_connection
from backend.logger import get_logger
from backend.strategies.loader import load_strategy_from_code

log = get_logger(__name__)


class StrategyService:
    """CRUD operations for strategy definitions stored in DuckDB."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_strategy(
        self,
        name: str,
        source_code: str,
        description: str | None = None,
        position_sizing: str = "equal_weight",
    ) -> dict:
        """Create a new strategy (auto-versioned).

        If a strategy with the same *name* already exists the version is
        incremented automatically.
        """
        # Validate source code is loadable
        try:
            instance = load_strategy_from_code(source_code)
        except Exception as exc:
            raise ValueError(f"Invalid strategy source code: {exc}") from exc

        conn = get_connection()

        # Auto-version: find max version for this name
        row = conn.execute(
            "SELECT MAX(version) FROM strategies WHERE name = ?", [name]
        ).fetchone()
        version = 1
        if row and row[0] is not None:
            version = row[0] + 1

        strategy_id = uuid.uuid4().hex[:12]
        now = datetime.utcnow()

        # Extract required_factors / required_models from the instance
        required_factors = instance.required_factors()
        required_models = instance.required_models()

        conn.execute(
            """INSERT INTO strategies
               (id, name, version, description, source_code,
                required_factors, required_models, position_sizing,
                status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            [
                strategy_id,
                name,
                version,
                description or getattr(instance, "description", ""),
                source_code,
                json.dumps(required_factors),
                json.dumps(required_models),
                position_sizing,
                now,
                now,
            ],
        )
        log.info(
            "strategy.created",
            id=strategy_id,
            name=name,
            version=version,
        )
        return self.get_strategy(strategy_id)

    def update_strategy(
        self,
        strategy_id: str,
        source_code: str | None = None,
        description: str | None = None,
        position_sizing: str | None = None,
        status: str | None = None,
    ) -> dict:
        """Update a strategy -- if source_code changes, create a new version."""
        conn = get_connection()
        existing = self._fetch_row(strategy_id)
        if existing is None:
            raise ValueError(f"Strategy {strategy_id} not found")

        if source_code is not None and source_code != existing["source_code"]:
            # Validate new source code
            try:
                instance = load_strategy_from_code(source_code)
            except Exception as exc:
                raise ValueError(f"Invalid strategy source code: {exc}") from exc

            # Create a new version
            max_ver = conn.execute(
                "SELECT MAX(version) FROM strategies WHERE name = ?",
                [existing["name"]],
            ).fetchone()
            new_version = (max_ver[0] or 0) + 1
            new_id = uuid.uuid4().hex[:12]
            now = datetime.utcnow()

            required_factors = instance.required_factors()
            required_models = instance.required_models()

            conn.execute(
                """INSERT INTO strategies
                   (id, name, version, description, source_code,
                    required_factors, required_models, position_sizing,
                    status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    new_id,
                    existing["name"],
                    new_version,
                    description or existing["description"],
                    source_code,
                    json.dumps(required_factors),
                    json.dumps(required_models),
                    position_sizing or existing["position_sizing"],
                    status or "draft",
                    now,
                    now,
                ],
            )
            log.info(
                "strategy.new_version",
                id=new_id,
                name=existing["name"],
                version=new_version,
            )
            return self.get_strategy(new_id)

        # Simple metadata update (no version bump)
        now = datetime.utcnow()
        sets: list[str] = ["updated_at = ?"]
        vals: list = [now]

        for col, val in [
            ("description", description),
            ("position_sizing", position_sizing),
            ("status", status),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                vals.append(val)

        vals.append(strategy_id)
        conn.execute(
            f"UPDATE strategies SET {', '.join(sets)} WHERE id = ?", vals
        )
        log.info("strategy.updated", id=strategy_id)
        return self.get_strategy(strategy_id)

    def delete_strategy(self, strategy_id: str) -> None:
        """Delete a strategy definition."""
        conn = get_connection()
        existing = self._fetch_row(strategy_id)
        if existing is None:
            raise ValueError(f"Strategy {strategy_id} not found")

        conn.execute("DELETE FROM strategies WHERE id = ?", [strategy_id])
        log.info("strategy.deleted", id=strategy_id)

    def get_strategy(self, strategy_id: str) -> dict:
        """Return a single strategy definition."""
        row = self._fetch_row(strategy_id)
        if row is None:
            raise ValueError(f"Strategy {strategy_id} not found")
        return row

    def list_strategies(self) -> list[dict]:
        """List all strategies."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT id, name, version, description, source_code,
                      required_factors, required_models, position_sizing,
                      status, created_at, updated_at
               FROM strategies
               ORDER BY name, version DESC"""
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_row(self, strategy_id: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            """SELECT id, name, version, description, source_code,
                      required_factors, required_models, position_sizing,
                      status, created_at, updated_at
               FROM strategies WHERE id = ?""",
            [strategy_id],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        def _parse_json(raw):
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    return []
            return raw if raw else []

        return {
            "id": row[0],
            "name": row[1],
            "version": row[2],
            "description": row[3],
            "source_code": row[4],
            "required_factors": _parse_json(row[5]),
            "required_models": _parse_json(row[6]),
            "position_sizing": row[7],
            "status": row[8],
            "created_at": str(row[9]) if row[9] else None,
            "updated_at": str(row[10]) if row[10] else None,
        }
