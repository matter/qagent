import threading
import time
import unittest
import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from backend.db import close_db, get_connection

from backend.tasks.executor import TaskExecutor, TaskSubmissionPaused
from backend.tasks.models import TaskRecord, TaskSource, TaskStatus
from backend.tasks.store import TaskStore


class MemoryOnlyStore:
    def __init__(self):
        self.inserted = []
        self.updates = []

    def insert(self, record):
        self.inserted.append(record)

    def update_status(self, task_id, status, **kwargs):
        self.updates.append((task_id, status, kwargs))

    def get(self, task_id):
        raise AssertionError("get_task should prefer in-memory task records")


class TaskExecutorContractTests(unittest.TestCase):
    def test_get_task_prefers_in_memory_record(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        gate = threading.Event()

        task_id = executor.submit(
            "slow_test",
            fn=lambda: gate.wait(timeout=1),
            params={},
        )

        try:
            record = executor.get_task(task_id)
            self.assertEqual(record.id, task_id)
            self.assertEqual(record.task_type, "slow_test")
            self.assertIn(record.status.value, {"queued", "running"})
        finally:
            gate.set()
            executor.shutdown(wait=True)

    def test_api_modules_share_the_same_task_executor(self):
        from backend.api import data, factors, models, paper_trading, signals, strategies, tasks

        executors = {
            data._get_executor(),
            factors._get_executor(),
            models._get_executor(),
            paper_trading._get_executor(),
            signals._get_executor(),
            strategies._get_executor(),
            tasks._get_executor(),
        }

        self.assertEqual(len(executors), 1)

    def test_cancelled_running_task_is_not_overwritten_by_late_completion(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        gate = threading.Event()

        task_id = executor.submit(
            "slow_test",
            fn=lambda: gate.wait(timeout=1),
            params={},
        )

        try:
            self.assertTrue(executor.cancel(task_id))
            gate.set()
            executor.shutdown(wait=True)
            record = executor.get_task(task_id)
        finally:
            gate.set()

        self.assertEqual(record.status.value, "failed")
        self.assertTrue(record.error_message.startswith("Cancelled by user"))

    def test_cancelled_running_task_quarantines_late_result(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        started = threading.Event()
        gate = threading.Event()

        def work():
            started.set()
            gate.wait(timeout=1)
            return {"backtest_id": "bt_late", "market": "US"}

        task_id = executor.submit("slow_test", fn=work, params={})
        try:
            self.assertTrue(started.wait(timeout=1))
            self.assertTrue(executor.cancel(task_id))
            gate.set()
            deadline = time.time() + 2
            while time.time() < deadline:
                record = executor.get_task(task_id)
                if record.result_summary and record.result_summary.get("late_result_quarantined"):
                    break
                time.sleep(0.01)
            executor.shutdown(wait=True)
            record = executor.get_task(task_id)
        finally:
            gate.set()

        self.assertEqual(record.status.value, "failed")
        self.assertEqual(record.error_message, "Cancelled by user; late result quarantined")
        self.assertTrue(record.result_summary["authoritative_terminal"])
        self.assertTrue(record.result_summary["late_result_quarantined"])
        self.assertNotIn("late_result", record.result_summary)
        self.assertEqual(record.result_summary["late_result_diagnostics"]["backtest_id"], "bt_late")

    def test_cancelled_running_task_warns_compute_may_continue_and_terminal_is_authoritative(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        started = threading.Event()
        gate = threading.Event()

        def work():
            started.set()
            gate.wait(timeout=1)
            return {"backtest_id": "bt_late", "market": "US"}

        task_id = executor.submit("slow_test", fn=work, params={})
        try:
            self.assertTrue(started.wait(timeout=1))
            self.assertTrue(executor.cancel(task_id))
            record = executor.get_task(task_id)
        finally:
            gate.set()
            executor.shutdown(wait=True)

        self.assertEqual(record.status.value, "failed")
        self.assertTrue(record.result_summary["cancel_requested"])
        self.assertTrue(record.result_summary["compute_may_continue"])
        self.assertTrue(record.result_summary["authoritative_terminal"])
        self.assertEqual(record.result_summary["reason"], "cancelled_running_thread")

    def test_cancelled_queued_task_does_not_warn_compute_may_continue(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        blocker = threading.Event()
        second_started = threading.Event()

        first_id = executor.submit("slow_test", fn=lambda: blocker.wait(timeout=1), params={})
        second_id = executor.submit(
            "queued_test",
            fn=lambda: second_started.set(),
            params={},
        )

        try:
            self.assertTrue(executor.cancel(second_id))
            record = executor.get_task(second_id)
        finally:
            blocker.set()
            executor.shutdown(wait=True)

        self.assertEqual(record.status.value, "failed")
        self.assertTrue(record.result_summary["cancel_requested"])
        self.assertFalse(record.result_summary["compute_may_continue"])
        self.assertEqual(record.result_summary["reason"], "cancelled_before_worker_started")
        self.assertFalse(second_started.is_set())
        self.assertNotEqual(first_id, second_id)

    def test_timed_out_task_quarantines_late_result(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        gate = threading.Event()

        def work():
            gate.wait(timeout=1)
            return {"model_id": "model_late", "market": "CN"}

        task_id = executor.submit("slow_test", fn=work, params={}, timeout=0.01)
        try:
            deadline = time.time() + 2
            while time.time() < deadline:
                record = executor.get_task(task_id)
                if record.status.value == "timeout":
                    break
                time.sleep(0.01)
            gate.set()
            deadline = time.time() + 2
            while time.time() < deadline:
                record = executor.get_task(task_id)
                if record.result_summary and record.result_summary.get("late_result_quarantined"):
                    break
                time.sleep(0.01)
            executor.shutdown(wait=True)
            record = executor.get_task(task_id)
        finally:
            gate.set()

        self.assertEqual(record.status.value, "timeout")
        self.assertIn("late result quarantined", record.error_message)
        self.assertTrue(record.result_summary["authoritative_terminal"])
        self.assertTrue(record.result_summary["late_result_quarantined"])
        self.assertNotIn("late_result", record.result_summary)
        self.assertEqual(record.result_summary["late_result_diagnostics"]["model_id"], "model_late")

    def test_task_store_filters_by_source_and_market_param(self):
        store = QueryRecordingStore()

        store.list_tasks(source=TaskSource.AGENT, market="CN", limit=50)

        self.assertIn("source = ?", store.sql)
        self.assertIn("COALESCE(json_extract_string(params, '$.market'), 'US') = ?", store.sql)
        self.assertEqual(store.params, ["agent", "CN", 50])

    def test_task_store_treats_missing_market_param_as_us_when_filtering(self):
        store = QueryRecordingStore()

        store.list_tasks(source=TaskSource.AGENT, market="US", limit=50)

        self.assertIn("COALESCE(json_extract_string(params, '$.market'), 'US') = ?", store.sql)
        self.assertEqual(store.params, ["agent", "US", 50])

    def test_task_store_bulk_cancel_by_source_and_market(self):
        store = QueryRecordingStore(active_records=[
            TaskRecord(
                id="task_cn",
                task_type="strategy_backtest",
                status=TaskStatus.RUNNING,
                params={"market": "CN"},
                source=TaskSource.AGENT,
            )
        ])

        cancelled = store.mark_matching_active_cancelled(
            source=TaskSource.AGENT,
            market="CN",
            task_type="strategy_backtest",
        )

        self.assertEqual(cancelled, ["task_cn"])
        self.assertIn("UPDATE task_runs", store.sql)

    def test_task_store_active_filter_uses_queued_running_status_clause(self):
        store = QueryRecordingStore()

        store.list_matching_active(source=TaskSource.AGENT, market="US")

        self.assertIn("status IN ('queued', 'running')", store.sql)

    def test_cancel_matching_treats_missing_market_param_as_us(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        gate = threading.Event()

        task_id = executor.submit(
            "strategy_backtest",
            fn=lambda: gate.wait(timeout=1),
            params={},
            source=TaskSource.AGENT,
        )

        try:
            cancelled = executor.cancel_matching(
                task_type="strategy_backtest",
                source=TaskSource.AGENT,
                market="US",
            )
        finally:
            gate.set()
            executor.shutdown(wait=True)

        self.assertEqual(cancelled, [task_id])

    def test_paused_submission_rejects_matching_source_market_type_before_insert(self):
        store = PauseAwareMemoryStore()
        executor = TaskExecutor(store=store, max_workers=1)

        with self.assertRaises(TaskSubmissionPaused):
            executor.submit(
                "strategy_backtest",
                fn=lambda: {"ok": True},
                params={"market": "CN"},
                source=TaskSource.AGENT,
            )

        self.assertEqual(store.inserted, [])
        executor.shutdown(wait=True)

    def test_paused_submission_allows_different_market(self):
        store = PauseAwareMemoryStore()
        executor = TaskExecutor(store=store, max_workers=1)

        task_id = executor.submit(
            "strategy_backtest",
            fn=lambda **_: {"ok": True},
            params={"market": "US"},
            source=TaskSource.AGENT,
        )

        try:
            self.assertEqual(store.inserted[0].id, task_id)
        finally:
            executor.shutdown(wait=True)

    def test_task_submission_pause_maps_to_http_conflict(self):
        from backend.app import task_submission_paused_handler

        response = asyncio.run(
            task_submission_paused_handler(
                request=None,
                exc=TaskSubmissionPaused("paused by test"),
            )
        )

        self.assertEqual(response.status_code, 409)
        self.assertIn(b"paused by test", response.body)

    def test_task_api_rejects_invalid_market_filter(self):
        from fastapi import HTTPException
        from backend.api.tasks import _validate_market

        with self.assertRaises(HTTPException) as ctx:
            _validate_market("EU")

        self.assertEqual(ctx.exception.status_code, 400)

    def test_stale_running_tasks_are_marked_retryable_interrupted(self):
        store = StaleRecordingStore()

        count = store.mark_stale_running()

        self.assertEqual(count, 2)
        self.assertEqual(store.status, TaskStatus.FAILED)
        self.assertEqual(store.result_summary["interrupted"], True)
        self.assertEqual(store.result_summary["retryable"], True)
        self.assertIn("retryable", store.error_message)

    def test_cn_model_and_backtest_share_heavy_serial_key(self):
        self.assertEqual(
            TaskExecutor._serial_key("strategy_backtest", {"market": "CN"}),
            "CN:heavy-research",
        )
        self.assertEqual(
            TaskExecutor._serial_key("model_train", {"market": "CN"}),
            "CN:heavy-research",
        )
        self.assertEqual(
            TaskExecutor._serial_key("strategy_backtest", {"market": "US"}),
            "US:legacy-backtest",
        )
        self.assertEqual(
            TaskExecutor._serial_key(
                "model_train",
                {
                    "market": "US",
                    "feature_set_id": "fs_alpha",
                    "universe_group_id": "sp500",
                },
            ),
            "US:model-train:fs_alpha:sp500",
        )
        self.assertEqual(
            TaskExecutor._serial_key(
                "model_train",
                {
                    "market": "US",
                    "feature_set_id": "fs_alpha",
                    "universe_group_id": "sp500",
                },
            ),
            TaskExecutor._serial_key(
                "model_train",
                {
                    "market": "US",
                    "feature_set_id": "fs_alpha",
                    "universe_group_id": "sp500",
                    "label_id": "different_label",
                },
            ),
        )

    def test_commit_callback_runs_only_for_accepted_completed_task(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        committed = []

        task_id = executor.submit(
            "staged_write",
            fn=lambda **_: {"staging_id": "stage_ok"},
            params={},
            on_accept=lambda result, task: committed.append((task.id, result["staging_id"])),
        )

        executor.shutdown(wait=True)
        record = executor.get_task(task_id)
        self.assertEqual(record.status, TaskStatus.COMPLETED)
        self.assertEqual(committed, [(task_id, "stage_ok")])
        self.assertEqual(record.result_summary["acceptance"]["status"], "accepted")

    def test_staged_task_does_not_publish_domain_rows_before_acceptance(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        final_rows = []
        staging_rows = []

        def work(stage_domain_write=None, **_):
            stage_domain_write(
                "backtest_results",
                {"id": "bt_stage", "market": "US"},
            )
            return {"backtest_id": "bt_stage"}

        task_id = executor.submit(
            "strategy_backtest",
            fn=work,
            params={"market": "US"},
            timeout=1,
            staged_domain_writes=final_rows,
            on_accept=lambda result, task: staging_rows.append((task.id, result["backtest_id"])),
        )

        executor.shutdown(wait=True)
        record = executor.get_task(task_id)

        self.assertEqual(record.status, TaskStatus.COMPLETED)
        self.assertEqual(final_rows, [{"table": "backtest_results", "payload": {"id": "bt_stage", "market": "US"}}])
        self.assertEqual(staging_rows, [(task_id, "bt_stage")])
        self.assertEqual(record.result_summary["acceptance"]["staged_write_count"], 1)

    def test_commit_callback_is_not_run_for_late_timed_out_result(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        committed = []
        gate = threading.Event()

        def work():
            gate.wait(timeout=1)
            return {"staging_id": "stage_late"}

        task_id = executor.submit(
            "staged_write",
            fn=work,
            params={},
            timeout=0.01,
            on_accept=lambda result, task: committed.append(result["staging_id"]),
        )
        try:
            deadline = time.time() + 2
            while time.time() < deadline:
                if executor.get_task(task_id).status == TaskStatus.TIMEOUT:
                    break
                time.sleep(0.01)
            gate.set()
            deadline = time.time() + 2
            while time.time() < deadline:
                record = executor.get_task(task_id)
                if record.result_summary and record.result_summary.get("late_result_quarantined"):
                    break
                time.sleep(0.01)
            executor.shutdown(wait=True)
        finally:
            gate.set()

        record = executor.get_task(task_id)
        self.assertEqual(record.status, TaskStatus.TIMEOUT)
        self.assertEqual(committed, [])
        self.assertTrue(record.result_summary["late_result_quarantined"])

    def test_staged_task_discards_late_domain_writes_after_timeout(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)
        final_rows = []
        gate = threading.Event()

        def work(stage_domain_write=None, **_):
            gate.wait(timeout=1)
            stage_domain_write(
                "models",
                {"id": "model_late", "market": "CN"},
            )
            return {"model_id": "model_late"}

        task_id = executor.submit(
            "model_train",
            fn=work,
            params={"market": "CN"},
            timeout=0.01,
            staged_domain_writes=final_rows,
        )
        try:
            deadline = time.time() + 2
            while time.time() < deadline:
                if executor.get_task(task_id).status == TaskStatus.TIMEOUT:
                    break
                time.sleep(0.01)
            gate.set()
            deadline = time.time() + 2
            while time.time() < deadline:
                record = executor.get_task(task_id)
                if record.result_summary and record.result_summary.get("late_result_quarantined"):
                    break
                time.sleep(0.01)
            executor.shutdown(wait=True)
        finally:
            gate.set()

        record = executor.get_task(task_id)
        self.assertEqual(record.status, TaskStatus.TIMEOUT)
        self.assertEqual(final_rows, [])
        self.assertTrue(record.result_summary["late_result_quarantined"])

    def test_staged_domain_writes_roll_back_if_acceptance_commit_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "task_staged_commit.duckdb"
            close_db()
            patcher = patch("backend.config.settings.data.db_path", str(db_path))
            patcher.start()
            self.addCleanup(patcher.stop)
            self.addCleanup(close_db)
            conn = get_connection()
            conn.execute("CREATE TABLE final_rows (id VARCHAR PRIMARY KEY, value VARCHAR)")
            executor = TaskExecutor(store=MemoryOnlyStore(), max_workers=1)

            def work(stage_domain_write=None, **_):
                def commit_ok(conn=None):
                    conn.execute("INSERT INTO final_rows VALUES ('ok', 'committed')")

                def commit_fail(conn=None):
                    conn.execute("INSERT INTO final_rows VALUES ('partial', 'must_rollback')")
                    raise RuntimeError("commit failed")

                stage_domain_write("final_rows", {"id": "ok"}, commit=commit_ok)
                stage_domain_write("final_rows", {"id": "partial"}, commit=commit_fail)
                return {"result": "ready"}

            task_id = executor.submit(
                "staged_write",
                fn=work,
                params={},
                timeout=1,
            )
            executor.shutdown(wait=True)
            record = executor.get_task(task_id)
            rows = conn.execute("SELECT * FROM final_rows ORDER BY id").fetchall()

        self.assertEqual(record.status, TaskStatus.FAILED)
        self.assertIn("commit failed", record.error_message)
        self.assertEqual(rows, [])

    def test_failed_task_error_message_is_strict_json_safe(self):
        from backend.api.tasks import _record_to_dict

        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)

        def fail_with_control_character():
            raise ValueError("bad payload \x00 inside")

        task_id = executor.submit("json_failure", fn=fail_with_control_character, params={})
        executor.shutdown(wait=True)
        record = executor.get_task(task_id)

        self.assertEqual(record.status, TaskStatus.FAILED)
        self.assertNotIn("\x00", record.error_message)
        encoded = json.dumps(_record_to_dict(record), ensure_ascii=False)
        self.assertNotIn("\x00", encoded)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["status"], "failed")

    def test_task_progress_updates_running_result_summary(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=1)

        def work(progress=None, **_kwargs):
            progress("feature_load", message="loading features", percent=0.25)
            return {"ok": True}

        task_id = executor.submit("model_train", fn=work, params={"market": "US"})
        executor.shutdown(wait=True)
        record = executor.get_task(task_id)

        self.assertEqual(record.status, TaskStatus.COMPLETED)
        self.assertEqual(record.result_summary["ok"], True)
        self.assertEqual(record.result_summary["progress"]["phase"], "feature_load")
        self.assertEqual(record.result_summary["progress_history"][0]["phase"], "feature_load")
        progress_updates = [
            update
            for update in store.updates
            if update[1] == TaskStatus.RUNNING
            and update[2].get("result_summary", {}).get("progress", {}).get("phase") == "feature_load"
        ]
        self.assertEqual(len(progress_updates), 1)
        self.assertEqual(progress_updates[0][2]["result_summary"]["progress"]["percent"], 0.25)

    def test_serial_wait_and_acquire_are_visible_in_task_progress(self):
        store = MemoryOnlyStore()
        executor = TaskExecutor(store=store, max_workers=2)
        first_started = threading.Event()
        release_first = threading.Event()

        def first_work(**_kwargs):
            first_started.set()
            release_first.wait(timeout=2)
            return {"first": True}

        def second_work(**_kwargs):
            return {"second": True}

        executor.submit(
            "strategy_backtest",
            fn=first_work,
            params={"market": "US"},
            timeout=5,
        )
        self.assertTrue(first_started.wait(timeout=2))
        second_id = executor.submit(
            "strategy_backtest",
            fn=second_work,
            params={"market": "US"},
            timeout=5,
        )
        try:
            deadline = time.time() + 2
            second_record = executor.get_task(second_id)
            while time.time() < deadline:
                second_record = executor.get_task(second_id)
                progress = (second_record.result_summary or {}).get("progress")
                if progress and progress.get("phase") == "serial_wait":
                    break
                time.sleep(0.01)
            self.assertEqual(second_record.result_summary["progress"]["phase"], "serial_wait")
            self.assertEqual(
                second_record.result_summary["progress"]["serial_key"],
                "US:legacy-backtest",
            )
        finally:
            release_first.set()
            executor.shutdown(wait=True)

        second_record = executor.get_task(second_id)
        phases = [
            item["phase"]
            for item in second_record.result_summary["progress_history"]
        ]
        self.assertIn("serial_acquired", phases)

class QueryRecordingStore(TaskStore):
    def __init__(self, active_records=None):
        self.sql = ""
        self.params = []
        self.active_records = active_records or []

    def _fetch_rows(self, sql, params):
        self.sql = " ".join(str(sql).split())
        self.params = list(params)
        return []

    def list_matching_active(self, **kwargs):
        if self.active_records:
            return self.active_records
        return super().list_matching_active(**kwargs)

    def update_status(self, task_id, status, **kwargs):
        self.sql = "UPDATE task_runs"


class PauseAwareMemoryStore(MemoryOnlyStore):
    def get_matching_pause_rule(self, *, task_type, source, market):
        if task_type == "strategy_backtest" and source == TaskSource.AGENT and market == "CN":
            return {
                "id": "pause_cn_agent_backtest",
                "task_type": "strategy_backtest",
                "source": "agent",
                "market": "CN",
                "reason": "protect CN queue from stale script",
            }
        return None


class StaleRecordingStore(TaskStore):
    def __init__(self):
        self.status = None
        self.result_summary = None
        self.error_message = None

    def list_matching_active(self, **kwargs):
        return [
            TaskRecord(id="task_cn", task_type="strategy_backtest", params={"market": "CN"}),
            TaskRecord(id="task_model", task_type="model_train", params={"market": "CN"}),
        ]

    def update_status(self, task_id, status, **kwargs):
        self.status = status
        self.result_summary = kwargs.get("result_summary")
        self.error_message = kwargs.get("error_message")


if __name__ == "__main__":
    unittest.main()
