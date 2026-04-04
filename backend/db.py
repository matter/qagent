"""DuckDB singleton connection manager."""

from __future__ import annotations

import threading
from pathlib import Path

import duckdb

from backend.config import settings
from backend.logger import get_logger

log = get_logger(__name__)

_lock = threading.Lock()
_connection: duckdb.DuckDBPyConnection | None = None

_TASK_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS task_runs (
    id              VARCHAR PRIMARY KEY,
    task_type       VARCHAR NOT NULL,
    status          VARCHAR NOT NULL DEFAULT 'queued',
    params          JSON,
    result_summary  JSON,
    error_message   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    timeout_seconds INTEGER,
    source          VARCHAR NOT NULL DEFAULT 'system'
);
"""

_STOCKS_DDL = """\
CREATE TABLE IF NOT EXISTS stocks (
    ticker      VARCHAR PRIMARY KEY,
    name        VARCHAR,
    exchange    VARCHAR,
    sector      VARCHAR,
    status      VARCHAR DEFAULT 'active',
    updated_at  TIMESTAMP
);
"""

_DAILY_BARS_DDL = """\
CREATE TABLE IF NOT EXISTS daily_bars (
    ticker      VARCHAR NOT NULL,
    date        DATE NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    adj_factor  DOUBLE DEFAULT 1.0,
    PRIMARY KEY (ticker, date)
);
"""

_INDEX_BARS_DDL = """\
CREATE TABLE IF NOT EXISTS index_bars (
    symbol  VARCHAR NOT NULL,
    date    DATE NOT NULL,
    open    DOUBLE,
    high    DOUBLE,
    low     DOUBLE,
    close   DOUBLE,
    volume  BIGINT,
    PRIMARY KEY (symbol, date)
);
"""

_STOCK_GROUPS_DDL = """\
CREATE TABLE IF NOT EXISTS stock_groups (
    id              VARCHAR PRIMARY KEY,
    name            VARCHAR UNIQUE NOT NULL,
    description     TEXT,
    group_type      VARCHAR DEFAULT 'manual',
    filter_expr     TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_STOCK_GROUP_MEMBERS_DDL = """\
CREATE TABLE IF NOT EXISTS stock_group_members (
    group_id    VARCHAR NOT NULL,
    ticker      VARCHAR NOT NULL,
    PRIMARY KEY (group_id, ticker)
);
"""

_DATA_UPDATE_LOG_DDL = """\
CREATE TABLE IF NOT EXISTS data_update_log (
    id              VARCHAR PRIMARY KEY,
    update_type     VARCHAR,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    status          VARCHAR,
    total_tickers   INTEGER,
    success_count   INTEGER,
    fail_count      INTEGER,
    failed_tickers  JSON,
    message         TEXT
);
"""


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return the singleton DuckDB connection (thread-safe init)."""
    global _connection
    if _connection is not None:
        return _connection
    with _lock:
        if _connection is not None:
            return _connection
        db_path = settings.db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        log.info("db.connect", path=str(db_path))
        _connection = duckdb.connect(str(db_path))
        return _connection


def init_db() -> None:
    """Create tables if they don't exist."""
    conn = get_connection()
    for name, ddl in (
        ("task_runs", _TASK_RUNS_DDL),
        ("stocks", _STOCKS_DDL),
        ("daily_bars", _DAILY_BARS_DDL),
        ("index_bars", _INDEX_BARS_DDL),
        ("data_update_log", _DATA_UPDATE_LOG_DDL),
        ("stock_groups", _STOCK_GROUPS_DDL),
        ("stock_group_members", _STOCK_GROUP_MEMBERS_DDL),
    ):
        conn.execute(ddl)
        log.info("db.init", table=name)


def close_db() -> None:
    """Close the DuckDB connection."""
    global _connection
    with _lock:
        if _connection is not None:
            _connection.close()
            _connection = None
            log.info("db.closed")
