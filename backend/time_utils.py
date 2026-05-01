"""Shared UTC time helpers.

DuckDB timestamp columns in this project have historically stored naive UTC
datetimes. Keep that storage contract explicit while avoiding deprecated
standard-library UTC constructors on newer Python versions.
"""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now_naive() -> datetime:
    """Return the current UTC time as a naive datetime for DB compatibility."""
    return datetime.now(UTC).replace(tzinfo=None)


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 Zulu timestamp."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
