"""Persist and query TaskRecords in DuckDB."""

from __future__ import annotations

import json
import uuid
from typing import Any

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.time_utils import utc_now_naive
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
        """Mark queued/running tasks from a previous server run as retryable failures."""
        stale_records = self.list_matching_active(limit=5000)
        interrupted_summary = {
            "interrupted": True,
            "retryable": True,
            "reason": "server_restarted",
            "message": "Server restarted while task was in progress; rerun with the same params.",
        }
        for record in stale_records:
            self.update_status(
                record.id,
                TaskStatus.FAILED,
                completed_at=utc_now_naive(),
                result_summary=interrupted_summary,
                error_message=(
                    "Interrupted by server restart; retryable=true; "
                    "rerun with the same params."
                ),
            )
        count = len(stale_records)
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
        source: TaskSource | None = None,
        market: str | None = None,
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
        if source:
            clauses.append("source = ?")
            params.append(source.value)
        if market:
            clauses.append("COALESCE(json_extract_string(params, '$.market'), 'US') = ?")
            params.append(normalize_market(market))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM task_runs{where} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._fetch_rows(sql, params)
        return [self._row_to_record(r) for r in rows]

    def list_matching_active(
        self,
        *,
        task_type: str | None = None,
        source: TaskSource | None = None,
        market: str | None = None,
        limit: int = 500,
    ) -> list[TaskRecord]:
        """List queued/running tasks matching optional source and market filters."""
        clauses = ["status IN ('queued', 'running')"]
        params: list[Any] = []
        if task_type:
            clauses.append("task_type = ?")
            params.append(task_type)
        if source:
            clauses.append("source = ?")
            params.append(source.value)
        if market:
            clauses.append("COALESCE(json_extract_string(params, '$.market'), 'US') = ?")
            params.append(normalize_market(market))
        where = " WHERE " + " AND ".join(clauses)
        sql = f"SELECT * FROM task_runs{where} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._fetch_rows(sql, params)
        return [self._row_to_record(r) for r in rows]

    def mark_matching_active_cancelled(
        self,
        *,
        task_type: str | None = None,
        source: TaskSource | None = None,
        market: str | None = None,
    ) -> list[str]:
        """Mark matching queued/running tasks cancelled in persistent storage."""
        matching = [
            record
            for record in self.list_matching_active(
                task_type=task_type,
                source=source,
                market=market,
            )
            if record.status in (TaskStatus.QUEUED, TaskStatus.RUNNING)
        ]
        for record in matching:
            self.update_status(
                record.id,
                TaskStatus.FAILED,
                completed_at=utc_now_naive(),
                error_message="Cancelled by bulk task filter",
            )
        return [record.id for record in matching]

    def create_pause_rule(
        self,
        *,
        task_type: str | None = None,
        source: TaskSource | None = None,
        market: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Persist an active task submission pause rule."""
        rule_id = uuid.uuid4().hex[:12]
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO task_pause_rules
                (id, task_type, source, market, reason, active, created_at)
            VALUES (?, ?, ?, ?, ?, TRUE, ?)
            """,
            [
                rule_id,
                task_type,
                source.value if source else None,
                normalize_market(market) if market else None,
                reason,
                utc_now_naive(),
            ],
        )
        return {
            "id": rule_id,
            "task_type": task_type,
            "source": source.value if source else None,
            "market": normalize_market(market) if market else None,
            "reason": reason,
            "active": True,
        }

    def list_pause_rules(self, active_only: bool = True) -> list[dict[str, Any]]:
        """List task submission pause rules."""
        clauses = ["active = TRUE"] if active_only else []
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = get_connection().execute(
            f"""SELECT id, task_type, source, market, reason, active, created_at
                FROM task_pause_rules{where}
                ORDER BY created_at DESC"""
        ).fetchall()
        return [self._pause_rule_row_to_dict(row) for row in rows]

    def delete_pause_rule(self, rule_id: str) -> bool:
        """Deactivate a task submission pause rule."""
        conn = get_connection()
        existing = conn.execute(
            "SELECT id FROM task_pause_rules WHERE id = ? AND active = TRUE",
            [rule_id],
        ).fetchone()
        if existing is None:
            return False
        conn.execute(
            "UPDATE task_pause_rules SET active = FALSE WHERE id = ?",
            [rule_id],
        )
        return True

    def get_matching_pause_rule(
        self,
        *,
        task_type: str,
        source: TaskSource,
        market: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the most specific active pause rule matching a submission."""
        rows = get_connection().execute(
            """
            SELECT id, task_type, source, market, reason, active, created_at
              FROM task_pause_rules
             WHERE active = TRUE
               AND (task_type IS NULL OR task_type = ?)
               AND (source IS NULL OR source = ?)
               AND (market IS NULL OR market = ?)
             ORDER BY
               ((task_type IS NOT NULL)::INTEGER
                + (source IS NOT NULL)::INTEGER
                + (market IS NOT NULL)::INTEGER) DESC,
               created_at DESC
             LIMIT 1
            """,
            [task_type, source.value, normalize_market(market) if market else None],
        ).fetchall()
        if not rows:
            return None
        return self._pause_rule_row_to_dict(rows[0])

    @staticmethod
    def _fetch_rows(sql: str, params: list[Any]) -> list[tuple]:
        return get_connection().execute(sql, params).fetchall()

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

    @staticmethod
    def _pause_rule_row_to_dict(row: tuple) -> dict[str, Any]:
        return {
            "id": row[0],
            "task_type": row[1],
            "source": row[2],
            "market": row[3],
            "reason": row[4],
            "active": bool(row[5]),
            "created_at": str(row[6]) if row[6] else None,
        }
