"""Task data models."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from backend.time_utils import utc_now_naive


class TaskStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


class TaskSource(str, enum.Enum):
    UI = "ui"
    AGENT = "agent"
    SYSTEM = "system"


@dataclass
class TaskRecord:
    id: str
    task_type: str
    status: TaskStatus = TaskStatus.QUEUED
    params: dict[str, Any] | None = None
    result_summary: dict[str, Any] | None = None
    error_message: str | None = None
    created_at: datetime = field(default_factory=utc_now_naive)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    timeout_seconds: int | None = None
    source: TaskSource = TaskSource.SYSTEM
