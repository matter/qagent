import threading
import time
import unittest
import asyncio

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

    def test_cancelled_running_task_exposes_late_result(self):
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
                if record.result_summary:
                    break
                time.sleep(0.01)
            executor.shutdown(wait=True)
            record = executor.get_task(task_id)
        finally:
            gate.set()

        self.assertEqual(record.status.value, "failed")
        self.assertEqual(record.error_message, "Cancelled by user; late result saved")
        self.assertEqual(record.result_summary["late_result"]["backtest_id"], "bt_late")

    def test_timed_out_task_exposes_late_result(self):
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
                if record.result_summary:
                    break
                time.sleep(0.01)
            executor.shutdown(wait=True)
            record = executor.get_task(task_id)
        finally:
            gate.set()

        self.assertEqual(record.status.value, "timeout")
        self.assertIn("late result saved", record.error_message)
        self.assertEqual(record.result_summary["late_result"]["model_id"], "model_late")

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


if __name__ == "__main__":
    unittest.main()


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
