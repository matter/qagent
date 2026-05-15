"""Conservative startup maintenance for local runtime performance."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from backend.config import settings
from backend.logger import get_logger
from backend.services.research_cache_service import ResearchCacheService

log = get_logger(__name__)


class StartupMaintenanceService:
    """Bounded cleanup tasks that prevent stale local artifacts from slowing QAgent."""

    def cleanup_stale_duckdb_temp_files(
        self,
        *,
        db_path: Path | None = None,
        min_age_seconds: int = 3600,
    ) -> dict[str, Any]:
        resolved_db_path = Path(db_path or settings.db_path)
        temp_dir = resolved_db_path.parent / f"{resolved_db_path.name}.tmp"
        summary: dict[str, Any] = {
            "temp_dir": str(temp_dir),
            "deleted_files": 0,
            "deleted_bytes": 0,
            "skipped_recent_files": 0,
            "errors": [],
            "min_age_seconds": max(0, int(min_age_seconds)),
        }
        if not temp_dir.exists():
            return summary

        min_updated_at = time.time() - summary["min_age_seconds"]
        for path in sorted(temp_dir.rglob("*")):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except OSError as exc:
                summary["errors"].append({"path": str(path), "error": str(exc)})
                continue
            if stat.st_mtime > min_updated_at:
                summary["skipped_recent_files"] += 1
                continue
            try:
                path.unlink()
            except OSError as exc:
                summary["errors"].append({"path": str(path), "error": str(exc)})
                continue
            summary["deleted_files"] += 1
            summary["deleted_bytes"] += int(stat.st_size)

        self._remove_empty_dirs(temp_dir)
        return summary

    def run_after_db_init(
        self,
        *,
        expired_cache_limit: int = 1000,
        orphan_file_limit: int = 1000,
        orphan_file_min_age_seconds: int = 3600,
    ) -> dict[str, Any]:
        cache_service = ResearchCacheService()
        result = {
            "expired_cache": cache_service.apply_expired_cache_cleanup(limit=expired_cache_limit),
            "orphan_files": cache_service.apply_orphan_file_cleanup(
                limit=orphan_file_limit,
                min_age_seconds=orphan_file_min_age_seconds,
            ),
        }
        log.info(
            "startup.maintenance.completed",
            expired_cache_deleted=result["expired_cache"].get("deleted_entries"),
            orphan_files_deleted=result["orphan_files"].get("deleted_files"),
        )
        return result

    @staticmethod
    def _remove_empty_dirs(root: Path) -> None:
        for path in sorted(root.rglob("*"), reverse=True):
            if not path.is_dir():
                continue
            try:
                path.rmdir()
            except OSError:
                continue
        try:
            root.rmdir()
        except OSError:
            pass
