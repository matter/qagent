"""Operational preflight checks for the local DuckDB database."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from backend.config import settings


class DbPreflightService:
    """Detect common local DB operational failures before long workflows run."""

    _RUNNING_API_ROUTES = [
        "/api/diagnostics/db-preflight",
        "/api/diagnostics/daily-bars",
        "/api/diagnostics/factor-values",
        "/api/tasks",
        "/api/market-data/quality-contract",
    ]
    _MAINTENANCE_REQUIRED_FOR = [
        "backup",
        "restore",
        "schema_migration",
        "direct_duckdb_reads",
    ]

    def check_database(self, db_path: Path | str | None = None) -> dict[str, Any]:
        path = Path(db_path) if db_path is not None else settings.db_path
        result: dict[str, Any] = {
            "db_path": str(path),
            "exists": path.exists(),
        }
        if not path.exists():
            return {
                **result,
                "ok": False,
                "status": "missing",
                "message": "Database file does not exist.",
                "action": "Run the backend once to initialize the database, or restore from backup.",
            }
        try:
            conn = duckdb.connect(str(path), read_only=True)
            conn.execute("SELECT 1").fetchone()
            conn.close()
        except (duckdb.IOException, duckdb.ConnectionException) as exc:
            message = str(exc)
            if self._is_lock_error(message):
                return {
                    **result,
                    "ok": False,
                    "status": "locked",
                    "message": "Database is locked by another process.",
                    "detail": message,
                    "action": "Use the running API for diagnostics, or stop services with bash scripts/stop.sh before maintenance.",
                    "running_api_routes": self._RUNNING_API_ROUTES,
                    "maintenance_required_for": self._MAINTENANCE_REQUIRED_FOR,
                }
            if self._is_same_process_connection_conflict(message):
                return {
                    **result,
                    "ok": False,
                    "status": "in_use",
                    "message": "Database is already open by the running backend process.",
                    "detail": message,
                    "action": "Use the running API for diagnostics, or stop services with bash scripts/stop.sh before maintenance.",
                    "running_api_routes": self._RUNNING_API_ROUTES,
                    "maintenance_required_for": self._MAINTENANCE_REQUIRED_FOR,
                }
            return {
                **result,
                "ok": False,
                "status": "unavailable",
                "message": "Database cannot be opened read-only.",
                "detail": message,
                "action": "Check file permissions and DuckDB process ownership before retrying.",
            }
        return {
            **result,
            "ok": True,
            "status": "available",
            "message": "Database can be opened read-only.",
            "action": None,
        }

    @staticmethod
    def _is_lock_error(message: str) -> bool:
        text = message.lower()
        return "lock" in text or "conflicting lock" in text

    @staticmethod
    def _is_same_process_connection_conflict(message: str) -> bool:
        text = message.lower()
        return "same database file" in text and "different configuration" in text
