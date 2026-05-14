"""Persist and query TaskRecords in DuckDB."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from backend.db import get_connection
from backend.logger import get_logger
from backend.services.market_context import normalize_market
from backend.tasks.json_safety import json_safe_string, json_safe_value
from backend.time_utils import utc_now_naive
from backend.tasks.models import TaskRecord, TaskSource, TaskStatus

log = get_logger(__name__)

TASK_RUN_SELECT_COLUMNS = (
    "id, run_id, task_type, status, params, result_summary, error_message, "
    "created_at, started_at, completed_at, timeout_seconds, source"
)


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
                (id, run_id, task_type, status, params, result_summary,
                 error_message, created_at, started_at, completed_at,
                 timeout_seconds, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                task.id,
                task.run_id,
                task.task_type,
                task.status.value,
                json.dumps(json_safe_value(task.params)) if task.params else None,
                json.dumps(json_safe_value(task.result_summary)) if task.result_summary else None,
                json_safe_string(task.error_message) if task.error_message else None,
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
                json.dumps(json_safe_value(result_summary)) if result_summary else None,
                json_safe_string(error_message) if error_message else None,
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
            try:
                self.release_resource_leases(record.id, reason="server_restarted")
            except Exception as exc:
                log.warning(
                    "task.stale_resource_lease_release_failed",
                    task_id=record.id,
                    error=str(exc),
                )
        count = len(stale_records)
        if count > 0:
            log.info("task.stale_cleaned", count=count)
        return count

    def acquire_resource_leases(
        self,
        *,
        task_id: str,
        task_type: str,
        resource_keys: list[str],
        market: str | None,
        ttl_seconds: int,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically acquire one or more task resource leases."""
        keys = self._clean_resource_keys(resource_keys)
        if not keys:
            return {"acquired": True, "leases": [], "blocked": []}
        ttl = max(1, int(ttl_seconds))
        market_value = normalize_market(market) if market else None
        metadata_json = json.dumps(json_safe_value(metadata or {}))
        conn = get_connection()
        try:
            conn.execute("BEGIN TRANSACTION")
            now = self._lease_now(conn)
            expires_at = self._lease_expires_at(conn, ttl)
            self._expire_stale_resource_leases(conn, now)
            placeholders = ", ".join(["?"] * len(keys))
            rows = conn.execute(
                f"""
                SELECT resource_key, task_id, task_type, market, status,
                       acquired_at, heartbeat_at, expires_at, released_at,
                       release_reason, metadata
                  FROM task_resource_leases
                 WHERE status = 'active'
                   AND expires_at > ?
                   AND resource_key IN ({placeholders})
                 ORDER BY resource_key
                """,
                [now, *keys],
            ).fetchall()
            if rows:
                blocked = [
                    {
                        "resource_key": row[0],
                        "task_id": row[1],
                        "task_type": row[2],
                        "market": row[3],
                        "expires_at": str(row[7]) if row[7] else None,
                    }
                    for row in rows
                ]
                conn.execute("COMMIT")
                return {"acquired": False, "leases": [], "blocked": blocked}

            leases: list[dict[str, Any]] = []
            for key in keys:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO task_resource_leases
                        (resource_key, task_id, task_type, market, status,
                         acquired_at, heartbeat_at, expires_at, released_at,
                         release_reason, metadata)
                    VALUES
                        (?, ?, ?, ?, 'active', ?, ?, ?, NULL, NULL, ?)
                    """,
                    [
                        key,
                        task_id,
                        task_type,
                        market_value,
                        now,
                        now,
                        expires_at,
                        metadata_json,
                    ],
                )
                leases.append({
                    "resource_key": key,
                    "task_id": task_id,
                    "task_type": task_type,
                    "market": market_value,
                    "status": "active",
                    "acquired_at": str(now),
                    "heartbeat_at": str(now),
                    "expires_at": str(expires_at),
                    "metadata": json_safe_value(metadata or {}),
                })
            conn.execute("COMMIT")
            return {"acquired": True, "leases": leases, "blocked": []}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def heartbeat_resource_leases(
        self,
        task_id: str,
        resource_keys: list[str],
        ttl_seconds: int,
    ) -> None:
        """Extend active leases owned by a task."""
        keys = self._clean_resource_keys(resource_keys)
        if not keys:
            return
        placeholders = ", ".join(["?"] * len(keys))
        conn = get_connection()
        now = self._lease_now(conn)
        conn.execute(
            f"""
            UPDATE task_resource_leases
               SET heartbeat_at = ?,
                   expires_at = current_timestamp + INTERVAL {int(max(1, ttl_seconds))} SECOND
             WHERE task_id = ?
               AND status = 'active'
               AND resource_key IN ({placeholders})
            """,
            [now, task_id, *keys],
        )

    def release_resource_leases(
        self,
        task_id: str,
        resource_keys: list[str] | None = None,
        reason: str = "completed",
    ) -> int:
        """Release active leases owned by a task."""
        keys = self._clean_resource_keys(resource_keys or [])
        conn = get_connection()
        now = self._lease_now(conn)
        params: list[Any] = [now, json_safe_string(reason), task_id]
        key_clause = ""
        if keys:
            placeholders = ", ".join(["?"] * len(keys))
            key_clause = f" AND resource_key IN ({placeholders})"
            params.extend(keys)
        existing = conn.execute(
            f"""
            SELECT COUNT(*)
              FROM task_resource_leases
             WHERE task_id = ?
               AND status = 'active'{key_clause}
            """,
            [task_id, *keys] if keys else [task_id],
        ).fetchone()[0]
        conn.execute(
            f"""
            UPDATE task_resource_leases
               SET status = 'released',
                   released_at = ?,
                   release_reason = ?
             WHERE task_id = ?
               AND status = 'active'{key_clause}
            """,
            params,
        )
        return int(existing)

    def expire_stale_resource_leases(self, now: datetime | None = None) -> int:
        """Expire active leases whose TTL has elapsed."""
        conn = get_connection()
        return self._expire_stale_resource_leases(conn, now or self._lease_now(conn))

    def list_resource_leases(
        self,
        active_only: bool = True,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List active or recent task resource leases."""
        clauses = ["status = 'active'"] if active_only else []
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = get_connection().execute(
            f"""
            SELECT resource_key, task_id, task_type, market, status,
                   acquired_at, heartbeat_at, expires_at, released_at,
                   release_reason, metadata
              FROM task_resource_leases{where}
             ORDER BY acquired_at DESC
             LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [self._lease_row_to_dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, task_id: str) -> TaskRecord | None:
        conn = get_connection()
        row = conn.execute(
            f"SELECT {TASK_RUN_SELECT_COLUMNS} FROM task_runs WHERE id = ?",
            [task_id],
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
        sql = (
            f"SELECT {TASK_RUN_SELECT_COLUMNS} FROM task_runs{where} "
            "ORDER BY created_at DESC LIMIT ?"
        )
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
        sql = (
            f"SELECT {TASK_RUN_SELECT_COLUMNS} FROM task_runs{where} "
            "ORDER BY created_at DESC LIMIT ?"
        )
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
            f"""SELECT {TASK_RUN_SELECT_COLUMNS} FROM task_runs
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
    def _clean_resource_keys(resource_keys: list[str]) -> list[str]:
        return sorted({str(key) for key in resource_keys if str(key or "").strip()})

    @staticmethod
    def _lease_now(conn) -> Any:
        return conn.execute("SELECT current_timestamp").fetchone()[0]

    @staticmethod
    def _lease_expires_at(conn, ttl_seconds: int) -> Any:
        return conn.execute(
            f"SELECT current_timestamp + INTERVAL {int(max(1, ttl_seconds))} SECOND"
        ).fetchone()[0]

    @staticmethod
    def _expire_stale_resource_leases(conn, now: datetime) -> int:
        existing = conn.execute(
            """
            SELECT COUNT(*)
              FROM task_resource_leases
             WHERE status = 'active'
               AND expires_at <= ?
            """,
            [now],
        ).fetchone()[0]
        conn.execute(
            """
            UPDATE task_resource_leases
               SET status = 'expired',
                   released_at = ?,
                   release_reason = 'stale_expired'
             WHERE status = 'active'
               AND expires_at <= ?
            """,
            [now, now],
        )
        return int(existing)

    @staticmethod
    def _lease_row_to_dict(row: tuple) -> dict[str, Any]:
        metadata = None
        if row[10]:
            try:
                metadata = json_safe_value(json.loads(row[10]))
            except Exception:
                metadata = json_safe_string(row[10])
        return {
            "resource_key": row[0],
            "task_id": row[1],
            "task_type": row[2],
            "market": row[3],
            "status": row[4],
            "acquired_at": str(row[5]) if row[5] else None,
            "heartbeat_at": str(row[6]) if row[6] else None,
            "expires_at": str(row[7]) if row[7] else None,
            "released_at": str(row[8]) if row[8] else None,
            "release_reason": row[9],
            "metadata": metadata,
        }

    @staticmethod
    def _row_to_record(row: tuple) -> TaskRecord:
        """Map a DuckDB row tuple to a TaskRecord."""
        return TaskRecord(
            id=row[0],
            run_id=row[1],
            task_type=row[2],
            status=TaskStatus(row[3]),
            params=json_safe_value(json.loads(row[4])) if row[4] else None,
            result_summary=json_safe_value(json.loads(row[5])) if row[5] else None,
            error_message=json_safe_string(row[6]) if row[6] else None,
            created_at=row[7],
            started_at=row[8],
            completed_at=row[9],
            timeout_seconds=row[10],
            source=TaskSource(row[11]),
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
