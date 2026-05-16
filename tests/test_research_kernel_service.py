import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection, init_db
from backend.services.research_kernel_service import ResearchKernelService


class ResearchKernelServiceContractTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "research_kernel.duckdb"
        close_db()
        patcher = patch("backend.config.settings.data.db_path", str(self.db_path))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(close_db)
        init_db()

    def test_list_runs_and_artifacts_support_project_workbench(self):
        service = ResearchKernelService()
        run = service.create_run(
            run_type="kernel_list_smoke",
            lifecycle_stage="scratch",
            retention_class="scratch",
            created_by="unit-test",
            params={"purpose": "workbench"},
        )
        artifact = service.create_json_artifact(
            run_id=run["id"],
            artifact_type="kernel_list_payload",
            payload={"ok": True},
            lifecycle_stage="scratch",
            retention_class="scratch",
        )

        runs = service.list_runs(
            project_id="bootstrap_us",
            run_type="kernel_list_smoke",
            lifecycle_stage="scratch",
        )
        artifacts = service.list_artifacts(
            project_id="bootstrap_us",
            run_id=run["id"],
            artifact_type="kernel_list_payload",
        )

        self.assertEqual([item["id"] for item in runs], [run["id"]])
        self.assertEqual([item["id"] for item in artifacts], [artifact["id"]])
        self.assertEqual(runs[0]["output_refs"][0]["id"], artifact["id"])

    def test_preview_artifact_cleanup_marks_protected_items(self):
        service = ResearchKernelService()
        run = service.create_run(
            run_type="cleanup_preview_smoke",
            lifecycle_stage="published",
            retention_class="standard",
            created_by="unit-test",
            params={"purpose": "cleanup"},
        )
        protected = service.create_json_artifact(
            run_id=run["id"],
            artifact_type="cleanup_protected_payload",
            payload={"ok": True},
            lifecycle_stage="published",
            retention_class="standard",
            rebuildable=False,
        )
        candidate = service.create_json_artifact(
            run_id=run["id"],
            artifact_type="cleanup_candidate_payload",
            payload={"ok": True},
            lifecycle_stage="scratch",
            retention_class="scratch",
            rebuildable=True,
        )

        preview = service.preview_artifact_cleanup(
            project_id="bootstrap_us",
            run_id=run["id"],
            limit=10,
        )

        self.assertEqual(preview["summary"]["candidate_count"], 1)
        self.assertEqual(preview["summary"]["protected_count"], 1)
        self.assertEqual(preview["candidates"][0]["id"], candidate["id"])
        self.assertEqual(preview["protected"][0]["artifact"]["id"], protected["id"])
        self.assertIn("non_rebuildable", preview["protected"][0]["reasons"])
        self.assertIn("published_or_validated", preview["protected"][0]["reasons"])

    def test_archive_artifact_moves_file_and_keeps_metadata(self):
        service = ResearchKernelService()
        run = service.create_run(
            run_type="archive_artifact_smoke",
            lifecycle_stage="scratch",
            retention_class="scratch",
            created_by="unit-test",
        )
        artifact = service.create_json_artifact(
            run_id=run["id"],
            artifact_type="archive_payload",
            payload={"ok": True},
            lifecycle_stage="scratch",
            retention_class="scratch",
            rebuildable=True,
        )
        original_path = Path(artifact["uri"])

        archived = service.archive_artifact(
            artifact["id"],
            archive_reason="unit test archive",
        )

        self.assertEqual(archived["lifecycle_stage"], "archived")
        self.assertEqual(archived["retention_class"], "archived")
        self.assertFalse(original_path.exists())
        self.assertTrue(Path(archived["uri"]).exists())
        self.assertEqual(archived["metadata"]["archive_previous_uri"], artifact["uri"])
        self.assertEqual(archived["metadata"]["archive_reason"], "unit test archive")

    def test_apply_artifact_cleanup_archives_only_preview_candidates_when_confirmed(self):
        service = ResearchKernelService()
        run = service.create_run(
            run_type="cleanup_apply_smoke",
            lifecycle_stage="scratch",
            retention_class="scratch",
            created_by="unit-test",
        )
        candidate = service.create_json_artifact(
            run_id=run["id"],
            artifact_type="cleanup_apply_candidate",
            payload={"ok": True},
            lifecycle_stage="scratch",
            retention_class="scratch",
            rebuildable=True,
        )
        protected = service.create_json_artifact(
            run_id=run["id"],
            artifact_type="cleanup_apply_protected",
            payload={"ok": True},
            lifecycle_stage="published",
            retention_class="standard",
            rebuildable=False,
        )

        with self.assertRaisesRegex(ValueError, "confirm"):
            service.apply_artifact_cleanup(project_id="bootstrap_us", confirm=False)

        result = service.apply_artifact_cleanup(
            project_id="bootstrap_us",
            run_id=run["id"],
            confirm=True,
            archive_reason="unit test cleanup",
        )

        self.assertEqual(result["mode"], "archive")
        self.assertEqual(result["summary"]["archived_count"], 1)
        self.assertEqual(result["archived"][0]["id"], candidate["id"])
        self.assertEqual(service.get_artifact(candidate["id"])["lifecycle_stage"], "archived")
        self.assertEqual(service.get_artifact(protected["id"])["lifecycle_stage"], "published")

    def test_list_promotion_records_supports_workbench_filters(self):
        service = ResearchKernelService()

        get_connection().execute(
            """INSERT INTO promotion_records
               (id, project_id, source_type, source_id, target_type, target_id,
                decision, policy_snapshot, qa_summary, approved_by, rationale)
               VALUES ('promo_unit', 'bootstrap_us', 'strategy_graph',
                       'graph_unit', 'strategy_graph', 'graph_unit',
                       'promoted', '{}', '{}', 'unit-test', 'looks stable')"""
        )

        records = service.list_promotion_records(
            project_id="bootstrap_us",
            source_type="strategy_graph",
            source_id="graph_unit",
        )

        self.assertEqual([item["id"] for item in records], ["promo_unit"])
        self.assertEqual(records[0]["decision"], "promoted")
        self.assertEqual(records[0]["approved_by"], "unit-test")


if __name__ == "__main__":
    unittest.main()
