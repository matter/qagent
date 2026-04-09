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

_LABEL_DEFINITIONS_DDL = """\
CREATE TABLE IF NOT EXISTS label_definitions (
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR UNIQUE NOT NULL,
    description TEXT,
    target_type VARCHAR NOT NULL,
    horizon     INTEGER NOT NULL,
    benchmark   VARCHAR,
    status      VARCHAR DEFAULT 'draft',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_FACTORS_DDL = """\
CREATE TABLE IF NOT EXISTS factors (
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    description TEXT,
    category    VARCHAR DEFAULT 'custom',
    source_code TEXT NOT NULL,
    params      JSON,
    status      VARCHAR DEFAULT 'draft',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, version)
);
"""

_FACTOR_VALUES_CACHE_DDL = """\
CREATE TABLE IF NOT EXISTS factor_values_cache (
    factor_id   VARCHAR NOT NULL,
    ticker      VARCHAR NOT NULL,
    date        DATE NOT NULL,
    value       DOUBLE,
    PRIMARY KEY (factor_id, ticker, date)
);
"""

_FACTOR_EVAL_RESULTS_DDL = """\
CREATE TABLE IF NOT EXISTS factor_eval_results (
    id                  VARCHAR PRIMARY KEY,
    factor_id           VARCHAR NOT NULL,
    label_id            VARCHAR NOT NULL,
    universe_group_id   VARCHAR,
    start_date          DATE,
    end_date            DATE,
    summary             JSON,
    ic_series           JSON,
    group_returns       JSON,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_FEATURE_SETS_DDL = """\
CREATE TABLE IF NOT EXISTS feature_sets (
    id              VARCHAR PRIMARY KEY,
    name            VARCHAR UNIQUE NOT NULL,
    description     TEXT,
    factor_refs     JSON NOT NULL,
    preprocessing   JSON NOT NULL,
    status          VARCHAR DEFAULT 'draft',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_MODELS_DDL = """\
CREATE TABLE IF NOT EXISTS models (
    id              VARCHAR PRIMARY KEY,
    name            VARCHAR NOT NULL,
    feature_set_id  VARCHAR NOT NULL,
    label_id        VARCHAR NOT NULL,
    model_type      VARCHAR NOT NULL DEFAULT 'lightgbm',
    model_params    JSON,
    train_config    JSON,
    eval_metrics    JSON,
    status          VARCHAR DEFAULT 'draft',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_STRATEGIES_DDL = """\
CREATE TABLE IF NOT EXISTS strategies (
    id              VARCHAR PRIMARY KEY,
    name            VARCHAR NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    description     TEXT,
    source_code     TEXT NOT NULL,
    required_factors JSON,
    required_models  JSON,
    position_sizing VARCHAR DEFAULT 'equal_weight',
    status          VARCHAR DEFAULT 'draft',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, version)
);
"""

_BACKTEST_RESULTS_DDL = """\
CREATE TABLE IF NOT EXISTS backtest_results (
    id              VARCHAR PRIMARY KEY,
    strategy_id     VARCHAR NOT NULL,
    config          JSON NOT NULL,
    summary         JSON NOT NULL,
    nav_series      JSON,
    benchmark_nav   JSON,
    drawdown_series JSON,
    monthly_returns JSON,
    trade_count     INTEGER,
    trades          JSON,
    result_level    VARCHAR DEFAULT 'exploratory',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_SIGNAL_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS signal_runs (
    id                  VARCHAR PRIMARY KEY,
    strategy_id         VARCHAR NOT NULL,
    strategy_version    INTEGER,
    target_date         DATE NOT NULL,
    universe_group_id   VARCHAR,
    result_level        VARCHAR DEFAULT 'exploratory',
    dependency_snapshot JSON,
    signal_count        INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_SIGNAL_DETAILS_DDL = """\
CREATE TABLE IF NOT EXISTS signal_details (
    run_id          VARCHAR NOT NULL,
    ticker          VARCHAR NOT NULL,
    signal          INTEGER,
    target_weight   DOUBLE,
    strength        DOUBLE,
    PRIMARY KEY (run_id, ticker)
);
"""

_PAPER_SESSIONS_DDL = """\
CREATE TABLE IF NOT EXISTS paper_trading_sessions (
    id              VARCHAR PRIMARY KEY,
    name            VARCHAR NOT NULL,
    strategy_id     VARCHAR NOT NULL,
    universe_group_id VARCHAR NOT NULL,
    config          JSON,
    status          VARCHAR NOT NULL DEFAULT 'active',
    start_date      DATE NOT NULL,
    current_date    DATE,
    initial_capital DOUBLE NOT NULL DEFAULT 1000000,
    current_nav     DOUBLE,
    total_trades    INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
    updated_at      TIMESTAMP
);
"""

_PAPER_DAILY_DDL = """\
CREATE TABLE IF NOT EXISTS paper_trading_daily (
    session_id      VARCHAR NOT NULL,
    date            DATE NOT NULL,
    nav             DOUBLE NOT NULL,
    cash            DOUBLE NOT NULL,
    positions_json  JSON,
    trades_json     JSON,
    PRIMARY KEY (session_id, date)
);
"""


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a thread-local cursor from the singleton DuckDB connection.

    DuckDB connections are not thread-safe for concurrent operations.
    Each thread must use its own cursor via connection.cursor().
    """
    global _connection
    if _connection is None:
        with _lock:
            if _connection is None:
                db_path = settings.db_path
                db_path.parent.mkdir(parents=True, exist_ok=True)
                log.info("db.connect", path=str(db_path))
                _connection = duckdb.connect(str(db_path))
    return _connection.cursor()


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
        ("label_definitions", _LABEL_DEFINITIONS_DDL),
        ("factors", _FACTORS_DDL),
        ("factor_values_cache", _FACTOR_VALUES_CACHE_DDL),
        ("factor_eval_results", _FACTOR_EVAL_RESULTS_DDL),
        ("feature_sets", _FEATURE_SETS_DDL),
        ("models", _MODELS_DDL),
        ("strategies", _STRATEGIES_DDL),
        ("backtest_results", _BACKTEST_RESULTS_DDL),
        ("signal_runs", _SIGNAL_RUNS_DDL),
        ("signal_details", _SIGNAL_DETAILS_DDL),
        ("paper_trading_sessions", _PAPER_SESSIONS_DDL),
        ("paper_trading_daily", _PAPER_DAILY_DDL),
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
