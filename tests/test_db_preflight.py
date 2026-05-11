import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from backend.services.db_preflight_service import DbPreflightService


class DbPreflightServiceContractTests(unittest.TestCase):
    def test_preflight_reports_ok_for_readable_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "qagent.duckdb"
            conn = duckdb.connect(str(db_path))
            conn.execute("CREATE TABLE t(i INTEGER)")
            conn.close()

            result = DbPreflightService().check_database(db_path)

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "available")
        self.assertEqual(result["db_path"], str(db_path))

    def test_preflight_converts_duckdb_lock_error_to_actionable_payload(self):
        def locked_connect(*args, **kwargs):
            raise duckdb.IOException(
                "Could not set lock on file /tmp/qagent.duckdb: Conflicting lock is held"
            )

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "qagent.duckdb"
            db_path.write_bytes(b"placeholder")
            with patch("backend.services.db_preflight_service.duckdb.connect", side_effect=locked_connect):
                result = DbPreflightService().check_database(db_path)

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "locked")
        self.assertIn("scripts/stop.sh", result["action"])
        self.assertNotIn("Traceback", result["message"])

    def test_preflight_treats_backend_connection_configuration_conflict_as_in_use(self):
        def in_use_connect(*args, **kwargs):
            raise duckdb.ConnectionException(
                "Can't open a connection to same database file with a different configuration than existing connections"
            )

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "qagent.duckdb"
            db_path.write_bytes(b"placeholder")
            with patch("backend.services.db_preflight_service.duckdb.connect", side_effect=in_use_connect):
                result = DbPreflightService().check_database(db_path)

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "in_use")
        self.assertIn("running API", result["action"])

    def test_preflight_includes_running_api_diagnostic_routes_when_locked(self):
        def locked_connect(*args, **kwargs):
            raise duckdb.IOException("Could not set lock on file /tmp/qagent.duckdb")

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "qagent.duckdb"
            db_path.write_bytes(b"placeholder")
            with patch("backend.services.db_preflight_service.duckdb.connect", side_effect=locked_connect):
                result = DbPreflightService().check_database(db_path)

        self.assertEqual(result["status"], "locked")
        self.assertIn("/api/diagnostics/daily-bars", result["running_api_routes"])
        self.assertEqual(result["maintenance_required_for"], ["backup", "restore", "schema_migration", "direct_duckdb_reads"])


if __name__ == "__main__":
    unittest.main()
