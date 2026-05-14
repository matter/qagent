"""Task executor – run callables in a thread pool with timeout and status tracking."""

from __future__ import annotations

import threading
import traceback
import uuid
import inspect
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from datetime import datetime
from typing import Any, Callable

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.tasks.json_safety import json_safe_string, json_safe_value
from backend.tasks.models import TaskRecord, TaskSource, TaskStatus
from backend.tasks.store import TaskStore
from backend.time_utils import utc_now_naive

log = get_logger(__name__)

# Default timeout in seconds if none specified.
DEFAULT_TIMEOUT = 300
MAX_WORKERS = 6


class TaskSubmissionPaused(ValueError):
    """Raised when a task submission matches an active pause rule."""


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
        self._serial_locks: dict[str, threading.Lock] = {}
        self._serial_locks_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(
        self,
        task_type: str,
        fn: Callable[..., Any],
        *,
        params: dict[str, Any] | None = None,
        run_id: str | None = None,
        timeout: int | None = None,
        source: TaskSource = TaskSource.SYSTEM,
        task_id: str | None = None,
        on_accept: Callable[[Any, TaskRecord], Any] | None = None,
        staged_domain_writes: list[dict[str, Any]] | None = None,
    ) -> str:
        """Submit *fn* for background execution.

        Returns the task_id (UUID).
        """
        tid = task_id or uuid.uuid4().hex
        timeout = timeout or DEFAULT_TIMEOUT
        market = self._pause_rule_market(params)
        pause_rule = self._get_matching_pause_rule(
            task_type=task_type,
            source=source,
            market=market,
        )
        if pause_rule:
            detail = (
                f"Task submission paused by rule {pause_rule['id']}: "
                f"task_type={pause_rule.get('task_type') or '*'}, "
                f"source={pause_rule.get('source') or '*'}, "
                f"market={pause_rule.get('market') or '*'}"
            )
            if pause_rule.get("reason"):
                detail = f"{detail}; reason={pause_rule['reason']}"
            raise TaskSubmissionPaused(detail)

        record = TaskRecord(
            id=tid,
            run_id=run_id,
            task_type=task_type,
            status=TaskStatus.QUEUED,
            params=json_safe_value(params),
            timeout_seconds=timeout,
            source=source,
        )
        self._store.insert(record)
        self._remember(record)

        future = self._pool.submit(
            self._run,
            tid,
            fn,
            params or {},
            timeout,
            on_accept,
            staged_domain_writes,
        )
        self._futures[tid] = future
        log.info("task.submitted", task_id=tid, task_type=task_type)
        return tid

    def _get_matching_pause_rule(
        self,
        *,
        task_type: str,
        source: TaskSource,
        market: str | None,
    ) -> dict[str, Any] | None:
        get_rule = getattr(self._store, "get_matching_pause_rule", None)
        if get_rule is None:
            return None
        return get_rule(task_type=task_type, source=source, market=market)

    @staticmethod
    def _pause_rule_market(params: dict[str, Any] | None) -> str | None:
        if params is None:
            return "US"
        try:
            return normalize_market(params.get("market"))
        except ValueError:
            return str(params.get("market")) if params.get("market") else None

    @staticmethod
    def _serial_key(task_type: str, params: dict[str, Any] | None) -> str | None:
        market = TaskExecutor._pause_rule_market(params)
        if market == "CN" and task_type in {"strategy_backtest", "model_train"}:
            return "CN:heavy-research"
        if task_type == "strategy_backtest":
            return f"{market or 'US'}:legacy-backtest"
        if task_type == "model_train":
            feature_set_id = str((params or {}).get("feature_set_id") or "")
            universe_group_id = str((params or {}).get("universe_group_id") or "")
            if feature_set_id and universe_group_id:
                return f"{market or 'US'}:model-train:{feature_set_id}:{universe_group_id}"
        return None

    @staticmethod
    def _resource_keys(task_type: str, params: dict[str, Any] | None) -> list[str]:
        params = params or {}
        market = TaskExecutor._pause_rule_market(params) or "US"
        if market == "CN" and task_type in {"strategy_backtest", "model_train"}:
            return ["market:CN:heavy-research"]
        if task_type == "strategy_backtest":
            return [f"market:{market}:legacy-backtest"]
        if task_type == "model_train":
            feature_set_id = str(params.get("feature_set_id") or "")
            universe_group_id = str(params.get("universe_group_id") or "")
            if feature_set_id and universe_group_id:
                return [f"market:{market}:model-train:{feature_set_id}:{universe_group_id}"]
            return []
        if task_type in {"model_train_distillation", "model_distillation_train"}:
            feature_set_id = str(params.get("student_feature_set_id") or "")
            universe_group_id = str(params.get("universe_group_id") or "")
            if feature_set_id and universe_group_id:
                return [f"market:{market}:model-train:{feature_set_id}:{universe_group_id}"]
            return []
        if task_type == "factor_compute":
            factor_id = str(params.get("factor_id") or "")
            return [f"market:{market}:factor:{factor_id}"] if factor_id else []
        if task_type == "factor_materialize_3_0":
            factor_spec_id = str(params.get("factor_spec_id") or "")
            universe_id = str(params.get("universe_id") or "")
            market_profile_id = str(params.get("market_profile_id") or "")
            if factor_spec_id and universe_id:
                prefix = (
                    f"market_profile:{market_profile_id}:"
                    if market_profile_id
                    else ""
                )
                return [f"{prefix}factor_spec:{factor_spec_id}:universe:{universe_id}"]
            return []
        if task_type == "model_train_experiment_3_0":
            payload = params.get("payload") if isinstance(params.get("payload"), dict) else {}
            dataset_id = str(params.get("dataset_id") or payload.get("dataset_id") or "")
            return [f"dataset:{dataset_id}:model-train-experiment"] if dataset_id else []
        if task_type == "strategy_graph_backtest":
            strategy_graph_id = str(params.get("strategy_graph_id") or "")
            return [f"strategy_graph:{strategy_graph_id}:backtest"] if strategy_graph_id else []
        if task_type == "data_update":
            return [f"market:{market}:data-update"]
        if task_type == "data_update_markets":
            return ["global:data-update"]
        if task_type in {"research_cache_warmup", "cache_feature_matrix_warmup"}:
            cache_key = str(params.get("cache_key") or "")
            if cache_key:
                return [f"cache:{cache_key}"]
            feature_set_id = str(params.get("feature_set_id") or "")
            universe_group_id = str(params.get("universe_group_id") or "")
            if feature_set_id and universe_group_id:
                return [f"market:{market}:research-cache:{feature_set_id}:{universe_group_id}"]
            return [f"market:{market}:research-cache"]
        return []

    @staticmethod
    def _lease_ttl(timeout: int | None) -> int:
        return max(120, min(int(timeout or DEFAULT_TIMEOUT), 900))

    def _get_serial_lock(self, key: str) -> threading.Lock:
        with self._serial_locks_lock:
            lock = self._serial_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._serial_locks[key] = lock
            return lock

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
        record = self.get_task(task_id)
        if record is None:
            return False
        if record.status not in (TaskStatus.QUEUED, TaskStatus.RUNNING):
            return False

        future = self._futures.get(task_id)
        future_cancelled = False
        if future is not None:
            future_cancelled = future.cancel()
        compute_may_continue = record.status == TaskStatus.RUNNING or not future_cancelled
        cancel_summary = {
            "cancel_requested": True,
            "compute_may_continue": compute_may_continue,
            "authoritative_terminal": True,
            "late_results_are_quarantined": True,
            "reason": (
                "cancelled_running_thread"
                if compute_may_continue
                else "cancelled_before_worker_started"
            ),
            "message": (
                "Cancellation was requested. Python cannot interrupt an "
                "already-running worker thread, so compute may continue "
                "until the callable returns."
                if compute_may_continue
                else "Cancellation was requested before worker execution started."
            ),
        }

        self._update_memory_status(
            task_id,
            TaskStatus.FAILED,
            completed_at=utc_now_naive(),
            result_summary=cancel_summary,
            error_message="Cancelled by user",
        )
        self._store.update_status(
            task_id,
            TaskStatus.FAILED,
            completed_at=utc_now_naive(),
            result_summary=cancel_summary,
            error_message="Cancelled by user",
        )
        log.info("task.cancelled", task_id=task_id)
        return True

    def cancel_matching(
        self,
        *,
        task_type: str | None = None,
        source: TaskSource | None = None,
        market: str | None = None,
    ) -> list[str]:
        """Cancel in-memory tasks matching optional filters."""
        cancelled: list[str] = []
        for task_id, record in list(self._records.items()):
            if record.status not in (TaskStatus.QUEUED, TaskStatus.RUNNING):
                continue
            if task_type and record.task_type != task_type:
                continue
            if source and record.source != source:
                continue
            if market and self._pause_rule_market(record.params) != normalize_market(market):
                continue
            if self.cancel(task_id):
                cancelled.append(task_id)
        return cancelled

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
                record.result_summary = json_safe_value(result_summary)
            if error_message is not None:
                record.error_message = json_safe_string(error_message)

    def _update_progress(self, task_id: str, phase: str, **payload: Any) -> None:
        existing = self.get_task(task_id)
        if existing is None or existing.status != TaskStatus.RUNNING:
            return
        summary: dict[str, Any] = {}
        if isinstance(existing.result_summary, dict):
            summary.update(existing.result_summary)
        progress = {
            "phase": json_safe_string(phase),
            "updated_at": utc_now_naive().isoformat(),
            **json_safe_value(payload),
        }
        history = list(summary.get("progress_history") or [])
        history.append(progress)
        summary["progress"] = progress
        summary["progress_history"] = history[-20:]
        self._update_memory_status(
            task_id,
            TaskStatus.RUNNING,
            result_summary=summary,
        )
        self._store.update_status(
            task_id,
            TaskStatus.RUNNING,
            result_summary=summary,
        )

    @staticmethod
    def _supports_progress(fn: Callable[..., Any]) -> bool:
        return TaskExecutor._supports_injected_param(fn, "progress")

    @staticmethod
    def _supports_stage_domain_write(fn: Callable[..., Any]) -> bool:
        return TaskExecutor._supports_injected_param(fn, "stage_domain_write")

    @staticmethod
    def _supports_injected_param(fn: Callable[..., Any], name: str) -> bool:
        try:
            signature = inspect.signature(fn)
        except (TypeError, ValueError):
            return False
        for param in signature.parameters.values():
            if param.kind == param.VAR_KEYWORD:
                return True
            if param.name == name:
                return True
        return False

    @staticmethod
    def _make_stage_domain_write(staged_writes: list[dict[str, Any]]) -> Callable[..., int]:
        def _stage_domain_write(
            table: str,
            payload: Any | None = None,
            *,
            commit: Callable[[], Any] | None = None,
        ) -> int:
            staged_writes.append({
                "table": json_safe_string(table),
                "payload": json_safe_value(payload if payload is not None else {}),
                "commit": commit,
            })
            return len(staged_writes)

        return _stage_domain_write

    @staticmethod
    def _staged_write_summary(write: dict[str, Any]) -> dict[str, Any]:
        return {
            "table": json_safe_string(write.get("table", "")),
            "payload": json_safe_value(write.get("payload", {})),
            "has_commit": callable(write.get("commit")),
        }

    @staticmethod
    def _commit_staged_domain_writes(
        staged_writes: list[dict[str, Any]],
        accepted_sink: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        committed: list[dict[str, Any]] = []
        if not staged_writes:
            return committed
        if not any(callable(write.get("commit")) for write in staged_writes):
            for write in staged_writes:
                summary = TaskExecutor._staged_write_summary(write)
                if accepted_sink is not None:
                    accepted_sink.append({
                        "table": summary["table"],
                        "payload": summary["payload"],
                    })
                committed.append(summary)
            return committed
        conn = get_connection()
        accepted_rows: list[dict[str, Any]] = []
        try:
            conn.execute("BEGIN TRANSACTION")
            for write in staged_writes:
                commit = write.get("commit")
                commit_result = (
                    TaskExecutor._call_staged_commit(commit, conn)
                    if callable(commit)
                    else None
                )
                summary = TaskExecutor._staged_write_summary(write)
                if commit_result is not None:
                    summary["commit_result"] = json_safe_value(commit_result)
                if accepted_sink is not None:
                    accepted_rows.append({
                        "table": summary["table"],
                        "payload": summary["payload"],
                    })
                committed.append(summary)
            conn.execute("COMMIT")
            if accepted_sink is not None:
                accepted_sink.extend(accepted_rows)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception as rollback_exc:
                log.warning(
                    "task.staged_domain_write.rollback_failed",
                    error=str(rollback_exc),
                )
            raise
        return committed

    @staticmethod
    def _call_staged_commit(commit: Callable[..., Any], conn: Any) -> Any:
        try:
            signature = inspect.signature(commit)
        except (TypeError, ValueError):
            return commit()
        for param in signature.parameters.values():
            if param.kind == param.VAR_KEYWORD or param.name == "conn":
                return commit(conn=conn)
        return commit()

    def _is_cancelled(self, task_id: str) -> bool:
        with self._records_lock:
            record = self._records.get(task_id)
            return (
                record is not None
                and record.status == TaskStatus.FAILED
                and bool(record.error_message)
                and record.error_message.startswith("Cancelled by user")
            )

    def _quarantine_late_result(
        self,
        task_id: str,
        result: Any,
        *,
        status: TaskStatus,
        error_message: str,
    ) -> None:
        summary = result if isinstance(result, dict) else {"result": result}
        summary = json_safe_value(summary)
        existing = self.get_task(task_id)
        late_summary: dict[str, Any] = {}
        if existing and isinstance(existing.result_summary, dict):
            late_summary.update(existing.result_summary)
        late_summary.pop("late_result", None)
        late_summary["late_result_diagnostics"] = summary
        late_summary["late_result_quarantined"] = True
        late_summary["authoritative_terminal"] = True
        completed = utc_now_naive()
        self._update_memory_status(
            task_id,
            status,
            completed_at=completed,
            result_summary=late_summary,
            error_message=error_message,
        )
        self._store.update_status(
            task_id,
            status,
            completed_at=completed,
            result_summary=late_summary,
            error_message=error_message,
        )

    def _accepted_summary(
        self,
        task_id: str,
        result: Any,
        on_accept: Callable[[Any, TaskRecord], Any] | None,
        staged_writes: list[dict[str, Any]] | None = None,
        staged_domain_writes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        summary = result if isinstance(result, dict) else {"result": result}
        summary = json_safe_value(dict(summary))
        existing = self.get_task(task_id)
        if existing and isinstance(existing.result_summary, dict):
            for key in ("progress", "progress_history"):
                if key in existing.result_summary and key not in summary:
                    summary[key] = existing.result_summary[key]
        staged_writes = staged_writes or []
        if on_accept is None and not staged_writes:
            return summary
        record = existing or self.get_task(task_id)
        if record is None or record.status != TaskStatus.RUNNING:
            summary["acceptance"] = {
                "status": "skipped",
                "reason": "task_not_running_at_acceptance_boundary",
            }
            return summary
        accept_result = on_accept(result, record) if on_accept is not None else None
        committed_writes = self._commit_staged_domain_writes(
            staged_writes,
            staged_domain_writes,
        )
        acceptance = {
            "status": "accepted",
            "result": accept_result if accept_result is not None else None,
        }
        if staged_writes:
            acceptance["staged_write_count"] = len(staged_writes)
            acceptance["staged_writes"] = committed_writes
        summary["acceptance"] = json_safe_value(acceptance)
        return summary

    def _lease_store_available(self) -> bool:
        return callable(getattr(self._store, "acquire_resource_leases", None))

    def _acquire_resource_leases_for_task(
        self,
        *,
        task_id: str,
        task_type: str,
        params: dict[str, Any],
        timeout: int,
    ) -> dict[str, Any]:
        resource_keys = self._resource_keys(task_type, params)
        if not resource_keys or not self._lease_store_available():
            return {
                "resource_keys": [],
                "leases": [],
                "ttl_seconds": self._lease_ttl(timeout),
                "timed_out": False,
                "cancelled": False,
            }
        acquire = getattr(self._store, "acquire_resource_leases")
        ttl_seconds = self._lease_ttl(timeout)
        serial_key = self._serial_key(task_type, params)
        market = self._pause_rule_market(params)
        deadline = time.monotonic() + float(timeout or DEFAULT_TIMEOUT)
        delay = 0.25
        last_progress_at = 0.0
        while True:
            if self._is_cancelled(task_id):
                return {
                    "resource_keys": resource_keys,
                    "leases": [],
                    "ttl_seconds": ttl_seconds,
                    "timed_out": False,
                    "cancelled": True,
                }
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                completed = utc_now_naive()
                self._update_memory_status(
                    task_id,
                    TaskStatus.TIMEOUT,
                    completed_at=completed,
                    result_summary={
                        "compute_may_continue": False,
                        "authoritative_terminal": True,
                        "message": "Task timed out while waiting for resource leases.",
                        "resource_keys": resource_keys,
                    },
                    error_message=f"Task timed out after {timeout}s while waiting for resource leases",
                )
                self._store.update_status(
                    task_id,
                    TaskStatus.TIMEOUT,
                    completed_at=completed,
                    result_summary={
                        "compute_may_continue": False,
                        "authoritative_terminal": True,
                        "message": "Task timed out while waiting for resource leases.",
                        "resource_keys": resource_keys,
                    },
                    error_message=f"Task timed out after {timeout}s while waiting for resource leases",
                )
                return {
                    "resource_keys": resource_keys,
                    "leases": [],
                    "ttl_seconds": ttl_seconds,
                    "timed_out": True,
                    "cancelled": False,
                }
            result = acquire(
                task_id=task_id,
                task_type=task_type,
                resource_keys=resource_keys,
                market=market,
                ttl_seconds=ttl_seconds,
                metadata={"params": params, "timeout_seconds": timeout},
            )
            if result.get("acquired"):
                self._update_progress(
                    task_id,
                    "serial_acquired",
                    serial_key=serial_key,
                    resource_keys=resource_keys,
                    leases=result.get("leases") or [],
                    message=f"Acquired resource lease {', '.join(resource_keys)}",
                )
                log.info(
                    "task.resource_leases_acquired",
                    task_id=task_id,
                    resource_keys=resource_keys,
                )
                return {
                    "resource_keys": resource_keys,
                    "leases": result.get("leases") or [],
                    "ttl_seconds": ttl_seconds,
                    "timed_out": False,
                    "cancelled": False,
                }
            now = time.monotonic()
            if last_progress_at == 0.0 or now - last_progress_at >= 1.0:
                self._update_progress(
                    task_id,
                    "serial_wait",
                    serial_key=serial_key,
                    resource_keys=resource_keys,
                    blocked_by=result.get("blocked") or [],
                    message=f"Waiting for resource lease {', '.join(resource_keys)}",
                )
                log.info(
                    "task.resource_leases_wait",
                    task_id=task_id,
                    resource_keys=resource_keys,
                    blocked_by=result.get("blocked") or [],
                )
                last_progress_at = now
            time.sleep(min(delay, max(0.01, remaining)))
            delay = min(2.0, delay * 1.5)

    def _start_resource_heartbeat(
        self,
        task_id: str,
        resource_keys: list[str],
        ttl_seconds: int,
    ) -> threading.Event | None:
        heartbeat = getattr(self._store, "heartbeat_resource_leases", None)
        if not resource_keys or not callable(heartbeat):
            return None
        stop_event = threading.Event()
        interval = max(1.0, min(30.0, ttl_seconds / 3.0))

        def _heartbeat_loop() -> None:
            while not stop_event.wait(interval):
                try:
                    heartbeat(task_id, resource_keys, ttl_seconds)
                except Exception as exc:
                    log.warning(
                        "task.resource_lease_heartbeat_failed",
                        task_id=task_id,
                        error=str(exc),
                    )

        thread = threading.Thread(target=_heartbeat_loop, daemon=True)
        thread.start()
        return stop_event

    @staticmethod
    def _stop_resource_heartbeat(stop_event: threading.Event | None) -> None:
        if stop_event is not None:
            stop_event.set()

    def _release_resource_leases(
        self,
        task_id: str,
        resource_keys: list[str],
        reason: str,
    ) -> None:
        release = getattr(self._store, "release_resource_leases", None)
        if not resource_keys or not callable(release):
            return
        try:
            release(task_id, resource_keys, reason=reason)
            log.info(
                "task.resource_leases_released",
                task_id=task_id,
                resource_keys=resource_keys,
                reason=reason,
            )
        except Exception as exc:
            log.warning(
                "task.resource_lease_release_failed",
                task_id=task_id,
                reason=reason,
                error=str(exc),
            )

    def _acquire_local_serial_lock(
        self,
        *,
        task_id: str,
        task_type: str,
        params: dict[str, Any],
    ) -> threading.Lock | None:
        serial_key = self._serial_key(task_type, params)
        serial_lock = self._get_serial_lock(serial_key) if serial_key else None
        if serial_lock is None:
            return None
        self._update_progress(
            task_id,
            "serial_wait",
            serial_key=serial_key,
            message=f"Waiting for serial lane {serial_key}",
        )
        log.info("task.serial_wait", task_id=task_id, serial_key=serial_key)
        serial_lock.acquire()
        self._update_progress(
            task_id,
            "serial_acquired",
            serial_key=serial_key,
            message=f"Acquired serial lane {serial_key}",
        )
        log.info("task.serial_acquired", task_id=task_id, serial_key=serial_key)
        return serial_lock

    def _run(
        self,
        task_id: str,
        fn: Callable[..., Any],
        params: dict[str, Any],
        timeout: int,
        on_accept: Callable[[Any, TaskRecord], Any] | None,
        staged_domain_writes: list[dict[str, Any]] | None,
    ) -> None:
        """Wrapper executed inside the thread pool."""
        started = utc_now_naive()
        self._update_memory_status(
            task_id, TaskStatus.RUNNING, started_at=started
        )
        self._store.update_status(
            task_id, TaskStatus.RUNNING, started_at=started
        )
        log.info("task.running", task_id=task_id)

        task_record = self.get_task(task_id)
        task_type = task_record.task_type if task_record else ""
        resource_keys: list[str] = []
        heartbeat_stop: threading.Event | None = None
        serial_lock: threading.Lock | None = None
        if self._lease_store_available():
            lease_state = self._acquire_resource_leases_for_task(
                task_id=task_id,
                task_type=task_type,
                params=params,
                timeout=timeout,
            )
            if lease_state.get("timed_out") or lease_state.get("cancelled"):
                return
            resource_keys = list(lease_state.get("resource_keys") or [])
            heartbeat_stop = self._start_resource_heartbeat(
                task_id,
                resource_keys,
                int(lease_state.get("ttl_seconds") or self._lease_ttl(timeout)),
            )
            if self._is_cancelled(task_id):
                log.info("task.cancelled_before_resource_work", task_id=task_id)
                self._release_resource_leases(
                    task_id,
                    resource_keys,
                    "cancelled_before_work",
                )
                self._stop_resource_heartbeat(heartbeat_stop)
                return
        else:
            serial_lock = self._acquire_local_serial_lock(
                task_id=task_id,
                task_type=task_type,
                params=params,
            )
            if serial_lock is not None and self._is_cancelled(task_id):
                log.info("task.cancelled_before_serial_work", task_id=task_id)
                serial_lock.release()
                return

        # We run fn in a *nested* future so we can enforce a timeout from
        # the calling thread.  The outer thread-pool thread blocks here
        # until the inner future completes or times out.
        inner_pool = ThreadPoolExecutor(max_workers=1)
        call_params = dict(params)
        staged_writes: list[dict[str, Any]] = []
        if self._supports_progress(fn):
            call_params["progress"] = lambda phase, **payload: self._update_progress(
                task_id,
                phase,
                **payload,
            )
        if self._supports_stage_domain_write(fn):
            call_params["stage_domain_write"] = self._make_stage_domain_write(
                staged_writes
            )
        inner_future = inner_pool.submit(fn, **call_params)
        release_resources_in_finally = True
        release_reason = "completed"

        try:
            result = inner_future.result(timeout=timeout)
            if self._is_cancelled(task_id):
                self._quarantine_late_result(
                    task_id,
                    result,
                    status=TaskStatus.FAILED,
                    error_message="Cancelled by user; late result quarantined",
                )
                log.info("task.cancelled_late_result_quarantined", task_id=task_id)
                release_reason = "cancelled_late_completed"
                return
            completed = utc_now_naive()
            summary = self._accepted_summary(
                task_id,
                result,
                on_accept,
                staged_writes,
                staged_domain_writes,
            )
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
            release_reason = "completed"

        except TimeoutError:
            completed = utc_now_naive()
            self._update_memory_status(
                task_id,
                TaskStatus.TIMEOUT,
                completed_at=completed,
                result_summary={
                    "compute_may_continue": True,
                    "authoritative_terminal": True,
                    "late_results_are_quarantined": True,
                    "message": "Task timed out; worker may finish later but outputs will be quarantined.",
                },
                error_message=f"Task timed out after {timeout}s; late result quarantined",
            )
            self._store.update_status(
                task_id,
                TaskStatus.TIMEOUT,
                completed_at=completed,
                result_summary={
                    "compute_may_continue": True,
                    "authoritative_terminal": True,
                    "late_results_are_quarantined": True,
                    "message": "Task timed out; worker may finish later but outputs will be quarantined.",
                },
                error_message=f"Task timed out after {timeout}s; late result quarantined",
            )
            log.warning("task.timeout", task_id=task_id, timeout=timeout)

            # The inner thread is still running (cancel() is a no-op for running threads).
            # Spawn a lightweight watcher that updates status if/when the task completes.
            def _watch_completion(
                fut: Future,
                tid: str,
                store: TaskStore,
                serial_release_lock: threading.Lock | None,
                leased_resource_keys: list[str],
                heartbeat_event: threading.Event | None,
            ) -> None:
                reason = "timeout_late_failed"
                try:
                    result = fut.result()  # blocks until inner completes
                    if self._is_cancelled(tid):
                        self._quarantine_late_result(
                            tid,
                            result,
                            status=TaskStatus.FAILED,
                            error_message="Cancelled by user; late result quarantined",
                        )
                        log.info("task.cancelled_late_result_quarantined", task_id=tid)
                        reason = "cancelled_late_completed"
                        return
                    self._quarantine_late_result(
                        tid,
                        result,
                        status=TaskStatus.TIMEOUT,
                        error_message=f"Task timed out after {timeout}s; late result quarantined",
                    )
                    log.info("task.late_result_quarantined", task_id=tid)
                    reason = "timeout_late_completed"
                except Exception:
                    if self._is_cancelled(tid):
                        log.info("task.cancelled_late_error_ignored", task_id=tid)
                        reason = "cancelled_late_failed"
                        return
                    tb = json_safe_string(traceback.format_exc())
                    self._update_memory_status(
                        tid,
                        TaskStatus.FAILED,
                        completed_at=utc_now_naive(),
                        error_message=tb,
                    )
                    store.update_status(
                        tid,
                        TaskStatus.FAILED,
                        completed_at=utc_now_naive(),
                        error_message=tb,
                    )
                    log.error("task.late_failed", task_id=tid, error=tb)
                    reason = "timeout_late_failed"
                finally:
                    self._release_resource_leases(tid, leased_resource_keys, reason)
                    self._stop_resource_heartbeat(heartbeat_event)
                    if serial_release_lock is not None:
                        serial_release_lock.release()

            release_resources_in_finally = False
            watcher = threading.Thread(
                target=_watch_completion,
                args=(inner_future, task_id, self._store, serial_lock, resource_keys, heartbeat_stop),
                daemon=True,
            )
            watcher.start()

        except Exception:
            completed = utc_now_naive()
            tb = json_safe_string(traceback.format_exc())
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
            release_reason = "failed"

        finally:
            if resource_keys and release_resources_in_finally:
                self._release_resource_leases(task_id, resource_keys, release_reason)
            if release_resources_in_finally:
                self._stop_resource_heartbeat(heartbeat_stop)
            if serial_lock is not None and release_resources_in_finally:
                serial_lock.release()
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
