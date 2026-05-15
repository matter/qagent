import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.config import settings
from backend.services.startup_maintenance_service import StartupMaintenanceService


class StartupMaintenanceServiceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def test_cleanup_stale_duckdb_temp_files_removes_old_files_only(self):
        root = Path(self._tmp.name)
        db_path = root / "qagent.duckdb"
        temp_dir = root / "qagent.duckdb.tmp"
        stale = temp_dir / "old.tmp"
        fresh = temp_dir / "fresh.tmp"
        stale.parent.mkdir(parents=True)
        stale.write_bytes(b"old")
        fresh.write_bytes(b"fresh")
        old_ts = time.time() - 7200
        os.utime(stale, (old_ts, old_ts))

        result = StartupMaintenanceService().cleanup_stale_duckdb_temp_files(
            db_path=db_path,
            min_age_seconds=3600,
        )

        self.assertEqual(result["deleted_files"], 1)
        self.assertEqual(result["skipped_recent_files"], 1)
        self.assertFalse(stale.exists())
        self.assertTrue(fresh.exists())

    def test_run_startup_maintenance_invokes_cache_governance_after_db_is_ready(self):
        fake_cache = _FakeResearchCacheService()
        with (
            patch.object(settings, "data", type("Data", (), {"db_path": str(Path(self._tmp.name) / "qagent.duckdb")})()),
            patch("backend.services.startup_maintenance_service.ResearchCacheService", return_value=fake_cache),
        ):
            result = StartupMaintenanceService().run_after_db_init(
                expired_cache_limit=12,
                orphan_file_limit=5,
                orphan_file_min_age_seconds=600,
            )

        self.assertEqual(fake_cache.calls, [("expired", 12), ("orphan", 5, 600)])
        self.assertEqual(result["expired_cache"]["deleted_entries"], 0)
        self.assertEqual(result["orphan_files"]["deleted_files"], 0)


class _FakeResearchCacheService:
    def __init__(self):
        self.calls = []

    def apply_expired_cache_cleanup(self, limit=100):
        self.calls.append(("expired", limit))
        return {"deleted_entries": 0}

    def apply_orphan_file_cleanup(self, limit=100, min_age_seconds=3600):
        self.calls.append(("orphan", limit, min_age_seconds))
        return {"deleted_files": 0}


if __name__ == "__main__":
    unittest.main()
