"""Task executor – run callables in a thread pool with timeout and status tracking."""

from __future__ import annotations

import threading
import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from datetime import datetime
from typing import Any, Callable

from backend.logger import get_logger
from backend.tasks.models import TaskRecord, TaskSource, TaskStatus
from backend.tasks.store import TaskStore

log = get_logger(__name__)

# Default timeout in seconds if none specified.
DEFAULT_TIMEOUT = 300
MAX_WORKERS = 6


class TaskExecutor:
    """Execute arbitrary callables, track them as TaskRecords in DuckDB."""

    def __init__(
        self,
        store: TaskStore | None = None,
        max_workers: int = MAX_WORKERS,
    ) -> None:
        self._store = store or TaskStore()
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: dict[str, Future] = {}
        self._records: dict[str, TaskRecord] = {}
        self._records_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(
        self,
        task_type: str,
        fn: Callable[..., Any],
        *,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
        source: TaskSource = TaskSource.SYSTEM,
        task_id: str | None = None,
    ) -> str:
        """Submit *fn* for background execution.

        Returns the task_id (UUID).
        """
        tid = task_id or uuid.uuid4().hex
        timeout = timeout or DEFAULT_TIMEOUT

        record = TaskRecord(
            id=tid,
            task_type=task_type,
            status=TaskStatus.QUEUED,
            params=params,
            timeout_seconds=timeout,
            source=source,
        )
        self._store.insert(record)
        self._remember(record)

        future = self._pool.submit(self._run, tid, fn, params or {}, timeout)
        self._futures[tid] = future
        log.info("task.submitted", task_id=tid, task_type=task_type)
        return tid

    def retry(self, task_id: str, fn: Callable[..., Any]) -> str:
        """Re-run a previously failed/timed-out task with the same params."""
        old = self._store.get(task_id)
        if old is None:
            raise ValueError(f"Task {task_id} not found")
        return self.submit(
            task_type=old.task_type,
            fn=fn,
            params=old.params,
            timeout=old.timeout_seconds,
            source=old.source,
        )

    def get_task(self, task_id: str) -> TaskRecord | None:
        with self._records_lock:
            record = self._records.get(task_id)
            if record is not None:
                return record
        return self._store.get(task_id)

    def cancel(self, task_id: str) -> bool:
        """Cancel a queued or running task. Returns True if cancelled."""
        record = self._store.get(task_id)
        if record is None:
            return False
        if record.status not in (TaskStatus.QUEUED, TaskStatus.RUNNING):
            return False

        future = self._futures.get(task_id)
        if future is not None:
            future.cancel()

        self._update_memory_status(
            task_id,
            TaskStatus.FAILED,
            completed_at=datetime.utcnow(),
            error_message="Cancelled by user",
        )
        self._store.update_status(
            task_id,
            TaskStatus.FAILED,
            completed_at=datetime.utcnow(),
            error_message="Cancelled by user",
        )
        log.info("task.cancelled", task_id=task_id)
        return True

    def has_running_task(self, task_type: str) -> str | None:
        """Return task_id if there's a running/queued task of this type, else None."""
        tasks = self._store.list_tasks(task_type=task_type, limit=1)
        for t in tasks:
            if t.status in (TaskStatus.QUEUED, TaskStatus.RUNNING):
                return t.id
        return None

    def mark_stale(self) -> None:
        """Mark stale tasks from previous server run as failed."""
        self._store.mark_stale_running()

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _remember(self, record: TaskRecord) -> None:
        with self._records_lock:
            self._records[record.id] = record

    def _update_memory_status(
        self,
        task_id: str,
        status: TaskStatus,
        *,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        result_summary: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._records_lock:
            record = self._records.get(task_id)
            if record is None:
                return
            record.status = status
            if started_at is not None:
                record.started_at = started_at
            if completed_at is not None:
                record.completed_at = completed_at
            if result_summary is not None:
                record.result_summary = result_summary
            if error_message is not None:
                record.error_message = error_message

    def _run(
        self,
        task_id: str,
        fn: Callable[..., Any],
        params: dict[str, Any],
        timeout: int,
    ) -> None:
        """Wrapper executed inside the thread pool."""
        started = datetime.utcnow()
        self._update_memory_status(
            task_id, TaskStatus.RUNNING, started_at=started
        )
        self._store.update_status(
            task_id, TaskStatus.RUNNING, started_at=started
        )
        log.info("task.running", task_id=task_id)

        # We run fn in a *nested* future so we can enforce a timeout from
        # the calling thread.  The outer thread-pool thread blocks here
        # until the inner future completes or times out.
        inner_pool = ThreadPoolExecutor(max_workers=1)
        inner_future = inner_pool.submit(fn, **params)

        try:
            result = inner_future.result(timeout=timeout)
            completed = datetime.utcnow()
            summary = result if isinstance(result, dict) else {"result": result}
            self._update_memory_status(
                task_id,
                TaskStatus.COMPLETED,
                completed_at=completed,
                result_summary=summary,
            )
            self._store.update_status(
                task_id,
                TaskStatus.COMPLETED,
                completed_at=completed,
                result_summary=summary,
            )
            log.info("task.completed", task_id=task_id)

        except TimeoutError:
            completed = datetime.utcnow()
            self._update_memory_status(
                task_id,
                TaskStatus.TIMEOUT,
                completed_at=completed,
                error_message=f"Task timed out after {timeout}s (still running in background)",
            )
            self._store.update_status(
                task_id,
                TaskStatus.TIMEOUT,
                completed_at=completed,
                error_message=f"Task timed out after {timeout}s (still running in background)",
            )
            log.warning("task.timeout", task_id=task_id, timeout=timeout)

            # The inner thread is still running (cancel() is a no-op for running threads).
            # Spawn a lightweight watcher that updates status if/when the task completes.
            def _watch_completion(fut: Future, tid: str, store: TaskStore) -> None:
                try:
                    result = fut.result()  # blocks until inner completes
                    summary = result if isinstance(result, dict) else {"result": result}
                    self._update_memory_status(
                        tid,
                        TaskStatus.COMPLETED,
                        completed_at=datetime.utcnow(),
                        result_summary=summary,
                    )
                    store.update_status(
                        tid,
                        TaskStatus.COMPLETED,
                        completed_at=datetime.utcnow(),
                        result_summary=summary,
                    )
                    log.info("task.late_completed", task_id=tid)
                except Exception:
                    tb = traceback.format_exc()
                    self._update_memory_status(
                        tid,
                        TaskStatus.FAILED,
                        completed_at=datetime.utcnow(),
                        error_message=tb,
                    )
                    store.update_status(
                        tid,
                        TaskStatus.FAILED,
                        completed_at=datetime.utcnow(),
                        error_message=tb,
                    )
                    log.error("task.late_failed", task_id=tid, error=tb)

            watcher = threading.Thread(
                target=_watch_completion,
                args=(inner_future, task_id, self._store),
                daemon=True,
            )
            watcher.start()

        except Exception:
            completed = datetime.utcnow()
            tb = traceback.format_exc()
            self._update_memory_status(
                task_id,
                TaskStatus.FAILED,
                completed_at=completed,
                error_message=tb,
            )
            self._store.update_status(
                task_id,
                TaskStatus.FAILED,
                completed_at=completed,
                error_message=tb,
            )
            log.error("task.failed", task_id=task_id, error=tb)

        finally:
            inner_pool.shutdown(wait=False)


_shared_executor: TaskExecutor | None = None
_shared_executor_lock = threading.Lock()


def get_task_executor() -> TaskExecutor:
    """Return the process-wide task executor used by API and MCP entry points."""
    global _shared_executor
    if _shared_executor is None:
        with _shared_executor_lock:
            if _shared_executor is None:
                _shared_executor = TaskExecutor()
    return _shared_executor
