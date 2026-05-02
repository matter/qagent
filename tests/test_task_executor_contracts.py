import threading
import time
import unittest

from backend.tasks.executor import TaskExecutor


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


if __name__ == "__main__":
    unittest.main()
