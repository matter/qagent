"""Persist and query TaskRecords in DuckDB."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from backend.db import get_connection
from backend.logger import get_logger
from backend.tasks.models import TaskRecord, TaskSource, TaskStatus

log = get_logger(__name__)


class TaskStore:
    """CRUD operations for the task_runs table."""

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert(self, task: TaskRecord) -> None:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO task_runs
                (id, task_type, status, params, result_summary,
                 error_message, created_at, started_at, completed_at,
                 timeout_seconds, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                task.id,
                task.task_type,
                task.status.value,
                json.dumps(task.params) if task.params else None,
                json.dumps(task.result_summary) if task.result_summary else None,
                task.error_message,
                task.created_at,
                task.started_at,
                task.completed_at,
                task.timeout_seconds,
                task.source.value,
            ],
        )
        log.debug("task.inserted", task_id=task.id, task_type=task.task_type)

    def update_status(
        self,
        task_id: str,
        status: TaskStatus,
        *,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        result_summary: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        conn = get_connection()
        conn.execute(
            """
            UPDATE task_runs
               SET status = ?,
                   started_at = COALESCE(?, started_at),
                   completed_at = COALESCE(?, completed_at),
                   result_summary = COALESCE(?, result_summary),
                   error_message = COALESCE(?, error_message)
             WHERE id = ?
            """,
            [
                status.value,
                started_at,
                completed_at,
                json.dumps(result_summary) if result_summary else None,
                error_message,
                task_id,
            ],
        )
        log.debug("task.updated", task_id=task_id, status=status.value)

    def mark_stale_running(self) -> int:
        """Mark any 'queued' or 'running' tasks as 'failed' (stale from previous run).

        Returns the number of rows affected.
        """
        conn = get_connection()
        result = conn.execute(
            """UPDATE task_runs
               SET status = 'failed',
                   completed_at = CURRENT_TIMESTAMP,
                   error_message = 'Marked as failed: server restarted while task was in progress'
               WHERE status IN ('queued', 'running')"""
        )
        count = result.fetchone()[0] if result.description else 0
        # DuckDB doesn't return affected rows easily; query instead
        count = conn.execute(
            """SELECT COUNT(*) FROM task_runs
               WHERE error_message = 'Marked as failed: server restarted while task was in progress'
                 AND completed_at > CURRENT_TIMESTAMP - INTERVAL 5 SECOND"""
        ).fetchone()[0]
        if count > 0:
            log.info("task.stale_cleaned", count=count)
        return count

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, task_id: str) -> TaskRecord | None:
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM task_runs WHERE id = ?", [task_id]
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def list_tasks(
        self,
        task_type: str | None = None,
        status: TaskStatus | None = None,
        limit: int = 50,
    ) -> list[TaskRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if task_type:
            clauses.append("task_type = ?")
            params.append(task_type)
        if status:
            clauses.append("status = ?")
            params.append(status.value)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM task_runs{where} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = get_connection().execute(sql, params).fetchall()
        return [self._row_to_record(r) for r in rows]

    def find_active_by_type_and_name(
        self, task_type: str, param_name: str, param_value: str,
    ) -> TaskRecord | None:
        """Find a queued/running task matching task_type and a named param."""
        conn = get_connection()
        rows = conn.execute(
            """SELECT * FROM task_runs
               WHERE task_type = ?
                 AND status IN ('queued', 'running')
               ORDER BY created_at DESC
               LIMIT 20""",
            [task_type],
        ).fetchall()
        for row in rows:
            record = self._row_to_record(row)
            if record.params and record.params.get(param_name) == param_value:
                return record
        return None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_record(row: tuple) -> TaskRecord:
        """Map a DuckDB row tuple to a TaskRecord."""
        return TaskRecord(
            id=row[0],
            task_type=row[1],
            status=TaskStatus(row[2]),
            params=json.loads(row[3]) if row[3] else None,
            result_summary=json.loads(row[4]) if row[4] else None,
            error_message=row[5],
            created_at=row[6],
            started_at=row[7],
            completed_at=row[8],
            timeout_seconds=row[9],
            source=TaskSource(row[10]),
        )
