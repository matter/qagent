"""Guardrails for direct DuckDB maintenance scripts."""

from __future__ import annotations

from pathlib import Path

from backend.services.db_preflight_service import DbPreflightService


class MaintenanceGuardService:
    """Fail fast before scripts open the main DuckDB file directly."""

    def assert_direct_db_maintenance_allowed(
        self,
        db_path: Path | str,
        *,
        operation: str,
    ) -> dict:
        result = DbPreflightService().check_database(db_path)
        if result["ok"]:
            return result

        message = (
            f"Direct DuckDB maintenance '{operation}' is blocked: "
            f"{result['message']} Status={result['status']}. "
            f"Action={result['action']}"
        )
        raise RuntimeError(message)
