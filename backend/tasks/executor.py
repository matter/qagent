"""Task executor – run callables in a thread pool with timeout and status tracking."""

from __future__ import annotations

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
MAX_WORKERS = 4


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
        return self._store.get(task_id)

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(
        self,
        task_id: str,
        fn: Callable[..., Any],
        params: dict[str, Any],
        timeout: int,
    ) -> None:
        """Wrapper executed inside the thread pool."""
        started = datetime.utcnow()
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
            self._store.update_status(
                task_id,
                TaskStatus.COMPLETED,
                completed_at=completed,
                result_summary=summary,
            )
            log.info("task.completed", task_id=task_id)

        except TimeoutError:
            inner_future.cancel()
            completed = datetime.utcnow()
            self._store.update_status(
                task_id,
                TaskStatus.TIMEOUT,
                completed_at=completed,
                error_message=f"Task timed out after {timeout}s",
            )
            log.warning("task.timeout", task_id=task_id, timeout=timeout)

        except Exception:
            completed = datetime.utcnow()
            tb = traceback.format_exc()
            self._store.update_status(
                task_id,
                TaskStatus.FAILED,
                completed_at=completed,
                error_message=tb,
            )
            log.error("task.failed", task_id=task_id, error=tb)

        finally:
            inner_pool.shutdown(wait=False)
