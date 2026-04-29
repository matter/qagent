import threading
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
        self.assertEqual(record.error_message, "Cancelled by user")


if __name__ == "__main__":
    unittest.main()
