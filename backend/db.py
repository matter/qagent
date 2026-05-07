"""DuckDB singleton connection manager."""

from __future__ import annotations

import threading
from pathlib import Path

import duckdb

from backend.config import settings
from backend.logger import get_logger
from backend.services.schema_migrations import migrate_market_schema

log = get_logger(__name__)

_lock = threading.Lock()
_connection: duckdb.DuckDBPyConnection | None = None

_TASK_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS task_runs (
    id              VARCHAR PRIMARY KEY,
    run_id          VARCHAR,
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

_RESEARCH_PROJECTS_DDL = """\
CREATE TABLE IF NOT EXISTS research_projects (
    id                    VARCHAR PRIMARY KEY,
    name                  VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL DEFAULT 'US_EQ',
    default_universe_id   VARCHAR,
    data_policy_id        VARCHAR,
    trading_rule_set_id   VARCHAR,
    cost_model_id         VARCHAR,
    benchmark_policy_id   VARCHAR,
    artifact_policy_id    VARCHAR,
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_RESEARCH_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS research_runs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL DEFAULT 'US_EQ',
    run_type           VARCHAR NOT NULL,
    status             VARCHAR NOT NULL DEFAULT 'queued',
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    retention_class    VARCHAR NOT NULL DEFAULT 'standard',
    params             JSON,
    input_refs         JSON,
    output_refs        JSON,
    metrics_summary    JSON,
    qa_summary         JSON,
    warnings           JSON,
    error_message      TEXT,
    created_by         VARCHAR NOT NULL DEFAULT 'system',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at         TIMESTAMP,
    completed_at       TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_ARTIFACTS_DDL = """\
CREATE TABLE IF NOT EXISTS artifacts (
    id                 VARCHAR PRIMARY KEY,
    run_id             VARCHAR NOT NULL,
    project_id         VARCHAR NOT NULL,
    artifact_type      VARCHAR NOT NULL,
    uri                TEXT NOT NULL,
    format             VARCHAR NOT NULL,
    schema_version     VARCHAR NOT NULL DEFAULT '1',
    byte_size          BIGINT NOT NULL DEFAULT 0,
    content_hash       VARCHAR NOT NULL,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    retention_class    VARCHAR NOT NULL DEFAULT 'standard',
    cleanup_after      TIMESTAMP,
    rebuildable        BOOLEAN NOT NULL DEFAULT TRUE,
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_LINEAGE_EDGES_DDL = """\
CREATE TABLE IF NOT EXISTS lineage_edges (
    id             VARCHAR PRIMARY KEY,
    from_type      VARCHAR NOT NULL,
    from_id        VARCHAR NOT NULL,
    to_type        VARCHAR NOT NULL,
    to_id          VARCHAR NOT NULL,
    relation       VARCHAR NOT NULL,
    metadata       JSON,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_MARKET_PROFILES_DDL = """\
CREATE TABLE IF NOT EXISTS market_profiles (
    id                       VARCHAR PRIMARY KEY,
    market_code              VARCHAR NOT NULL,
    asset_class              VARCHAR NOT NULL DEFAULT 'equity',
    name                     VARCHAR NOT NULL,
    currency                 VARCHAR NOT NULL,
    timezone                 VARCHAR NOT NULL,
    symbol_format            VARCHAR NOT NULL,
    provider_symbol_format   VARCHAR,
    data_policy_id           VARCHAR,
    trading_rule_set_id      VARCHAR,
    cost_model_id            VARCHAR,
    benchmark_policy_id      VARCHAR,
    status                   VARCHAR NOT NULL DEFAULT 'active',
    metadata                 JSON,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_DATA_POLICIES_DDL = """\
CREATE TABLE IF NOT EXISTS data_policies (
    id                    VARCHAR PRIMARY KEY,
    market_profile_id     VARCHAR NOT NULL,
    provider              VARCHAR NOT NULL,
    price_adjustment      VARCHAR NOT NULL,
    bar_availability      VARCHAR NOT NULL,
    data_quality_level    VARCHAR NOT NULL DEFAULT 'exploratory',
    field_semantics       JSON,
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_PROVIDER_CAPABILITIES_DDL = """\
CREATE TABLE IF NOT EXISTS provider_capabilities (
    provider             VARCHAR NOT NULL,
    dataset              VARCHAR NOT NULL,
    market_profile_id    VARCHAR NOT NULL DEFAULT 'GLOBAL',
    capability           VARCHAR NOT NULL,
    quality_level        VARCHAR NOT NULL DEFAULT 'exploratory',
    pit_supported        BOOLEAN NOT NULL DEFAULT FALSE,
    license_scope        VARCHAR NOT NULL DEFAULT 'unknown',
    availability         VARCHAR NOT NULL DEFAULT 'best_effort',
    as_of_date           DATE,
    available_at         TIMESTAMP,
    metadata             JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, dataset, market_profile_id, capability)
);
"""

_TRADING_RULE_SETS_DDL = """\
CREATE TABLE IF NOT EXISTS trading_rule_sets (
    id                         VARCHAR PRIMARY KEY,
    market_profile_id          VARCHAR NOT NULL,
    calendar                   VARCHAR NOT NULL,
    decision_to_execution      VARCHAR NOT NULL,
    settlement_cycle           VARCHAR NOT NULL,
    lot_size                   INTEGER NOT NULL DEFAULT 1,
    allow_short                BOOLEAN NOT NULL DEFAULT FALSE,
    limit_up_down              BOOLEAN NOT NULL DEFAULT FALSE,
    tradability_fields         JSON,
    rules                      JSON,
    created_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_COST_MODELS_DDL = """\
CREATE TABLE IF NOT EXISTS cost_models (
    id                    VARCHAR PRIMARY KEY,
    market_profile_id     VARCHAR NOT NULL,
    commission_rate       DOUBLE NOT NULL DEFAULT 0.0,
    slippage_rate         DOUBLE NOT NULL DEFAULT 0.0,
    stamp_tax_rate        DOUBLE NOT NULL DEFAULT 0.0,
    min_commission        DOUBLE NOT NULL DEFAULT 0.0,
    currency              VARCHAR NOT NULL,
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_BENCHMARK_POLICIES_DDL = """\
CREATE TABLE IF NOT EXISTS benchmark_policies (
    id                    VARCHAR PRIMARY KEY,
    market_profile_id     VARCHAR NOT NULL,
    default_benchmark     VARCHAR NOT NULL,
    benchmarks            JSON,
    benchmark_semantics   JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_ASSETS_DDL = """\
CREATE TABLE IF NOT EXISTS assets (
    asset_id             VARCHAR PRIMARY KEY,
    market_profile_id    VARCHAR NOT NULL,
    symbol               VARCHAR NOT NULL,
    display_symbol       VARCHAR NOT NULL,
    name                 VARCHAR,
    exchange             VARCHAR,
    sector               VARCHAR,
    industry             VARCHAR,
    status               VARCHAR NOT NULL DEFAULT 'active',
    metadata             JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(market_profile_id, symbol)
);
"""

_ASSET_IDENTIFIERS_DDL = """\
CREATE TABLE IF NOT EXISTS asset_identifiers (
    asset_id             VARCHAR NOT NULL,
    identifier_type      VARCHAR NOT NULL,
    identifier_value     VARCHAR NOT NULL,
    valid_from           DATE DEFAULT DATE '1900-01-01',
    valid_to             DATE,
    metadata             JSON,
    PRIMARY KEY (asset_id, identifier_type, identifier_value, valid_from)
);
"""

_ASSET_LIFECYCLE_DDL = """\
CREATE TABLE IF NOT EXISTS asset_lifecycle (
    asset_id             VARCHAR PRIMARY KEY,
    listed_date          DATE,
    delisted_date        DATE,
    status_history       JSON,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_MARKET_DATA_SNAPSHOTS_DDL = """\
CREATE TABLE IF NOT EXISTS market_data_snapshots (
    id                  VARCHAR PRIMARY KEY,
    market_profile_id   VARCHAR NOT NULL,
    provider            VARCHAR NOT NULL,
    data_policy_id      VARCHAR,
    as_of_date          DATE,
    coverage_summary    JSON,
    quality_summary     JSON,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_TRADE_STATUS_DDL = """\
CREATE TABLE IF NOT EXISTS trade_status (
    market_profile_id    VARCHAR NOT NULL,
    asset_id             VARCHAR NOT NULL,
    date                 DATE NOT NULL,
    is_trading           BOOLEAN NOT NULL DEFAULT TRUE,
    is_suspended         BOOLEAN NOT NULL DEFAULT FALSE,
    is_st                BOOLEAN NOT NULL DEFAULT FALSE,
    limit_up             DOUBLE,
    limit_down           DOUBLE,
    metadata             JSON,
    PRIMARY KEY (market_profile_id, asset_id, date)
);
"""

_CORPORATE_ACTIONS_DDL = """\
CREATE TABLE IF NOT EXISTS corporate_actions (
    id                  VARCHAR PRIMARY KEY,
    market_profile_id   VARCHAR NOT NULL,
    asset_id            VARCHAR NOT NULL,
    action_type         VARCHAR NOT NULL,
    ex_date             DATE,
    effective_date      DATE,
    value               DOUBLE,
    metadata            JSON,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_UNIVERSES_DDL = """\
CREATE TABLE IF NOT EXISTS universes (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    universe_type      VARCHAR NOT NULL,
    source_ref         JSON,
    filter_expr        TEXT,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_UNIVERSE_MEMBERSHIPS_DDL = """\
CREATE TABLE IF NOT EXISTS universe_memberships (
    universe_id        VARCHAR NOT NULL,
    run_id             VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    date               DATE NOT NULL,
    asset_id           VARCHAR NOT NULL,
    membership_state   VARCHAR NOT NULL DEFAULT 'active',
    available_at       TIMESTAMP,
    metadata           JSON,
    PRIMARY KEY (universe_id, run_id, date, asset_id)
);
"""

_FEATURE_PIPELINES_DDL = """\
CREATE TABLE IF NOT EXISTS feature_pipelines (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    source_type        VARCHAR NOT NULL,
    source_ref         JSON,
    preprocessing      JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_FEATURE_PIPELINE_NODES_DDL = """\
CREATE TABLE IF NOT EXISTS feature_pipeline_nodes (
    id                   VARCHAR PRIMARY KEY,
    feature_pipeline_id  VARCHAR NOT NULL,
    node_order           INTEGER NOT NULL,
    node_type            VARCHAR NOT NULL,
    name                 VARCHAR NOT NULL,
    input_refs           JSON,
    params               JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_LABEL_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS label_specs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    target_type        VARCHAR NOT NULL,
    horizon            INTEGER NOT NULL,
    benchmark          VARCHAR,
    source_type        VARCHAR NOT NULL,
    source_ref         JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_LABEL_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS label_runs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    label_spec_id      VARCHAR NOT NULL,
    universe_id        VARCHAR NOT NULL,
    start_date         DATE NOT NULL,
    end_date           DATE NOT NULL,
    run_id             VARCHAR,
    artifact_id        VARCHAR,
    profile            JSON,
    status             VARCHAR NOT NULL DEFAULT 'completed',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_DATASETS_DDL = """\
CREATE TABLE IF NOT EXISTS datasets (
    id                   VARCHAR PRIMARY KEY,
    project_id           VARCHAR NOT NULL,
    market_profile_id    VARCHAR NOT NULL,
    name                 VARCHAR NOT NULL,
    description          TEXT,
    universe_id          VARCHAR NOT NULL,
    feature_pipeline_id  VARCHAR NOT NULL,
    label_spec_id        VARCHAR NOT NULL,
    legacy_label_id      VARCHAR,
    start_date           DATE NOT NULL,
    end_date             DATE NOT NULL,
    split_policy         JSON,
    lifecycle_stage      VARCHAR NOT NULL DEFAULT 'experiment',
    retention_class      VARCHAR NOT NULL DEFAULT 'standard',
    status               VARCHAR NOT NULL DEFAULT 'draft',
    materialized_run_id  VARCHAR,
    dataset_artifact_id  VARCHAR,
    profile_artifact_id  VARCHAR,
    row_count            BIGINT DEFAULT 0,
    feature_count        INTEGER DEFAULT 0,
    label_count          INTEGER DEFAULT 0,
    qa_summary           JSON,
    metadata             JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_DATASET_COLUMNS_DDL = """\
CREATE TABLE IF NOT EXISTS dataset_columns (
    dataset_id    VARCHAR NOT NULL,
    column_name   VARCHAR NOT NULL,
    role          VARCHAR NOT NULL,
    dtype         VARCHAR,
    ordinal       INTEGER NOT NULL DEFAULT 0,
    source_ref    JSON,
    metadata      JSON,
    PRIMARY KEY (dataset_id, column_name, role)
);
"""

_DATASET_PROFILES_DDL = """\
CREATE TABLE IF NOT EXISTS dataset_profiles (
    dataset_id   VARCHAR NOT NULL,
    run_id       VARCHAR NOT NULL,
    profile      JSON NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, run_id)
);
"""

_FACTOR_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS factor_specs (
    id                   VARCHAR PRIMARY KEY,
    project_id           VARCHAR NOT NULL,
    market_profile_id    VARCHAR NOT NULL,
    name                 VARCHAR NOT NULL,
    description          TEXT,
    version              INTEGER NOT NULL DEFAULT 1,
    source_type          VARCHAR NOT NULL,
    source_ref           JSON,
    source_code          TEXT,
    code_hash            VARCHAR,
    params_schema        JSON,
    default_params       JSON,
    required_inputs      JSON,
    compute_mode         VARCHAR NOT NULL DEFAULT 'time_series',
    expected_warmup      INTEGER NOT NULL DEFAULT 0,
    applicable_profiles  JSON,
    semantic_tags        JSON,
    lifecycle_stage      VARCHAR NOT NULL DEFAULT 'experiment',
    status               VARCHAR NOT NULL DEFAULT 'draft',
    metadata             JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_FACTOR_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS factor_runs (
    id                   VARCHAR PRIMARY KEY,
    run_id               VARCHAR NOT NULL,
    project_id           VARCHAR NOT NULL,
    market_profile_id    VARCHAR NOT NULL,
    factor_spec_id       VARCHAR NOT NULL,
    factor_spec_version  INTEGER NOT NULL DEFAULT 1,
    universe_id          VARCHAR NOT NULL,
    start_date           DATE NOT NULL,
    end_date             DATE NOT NULL,
    mode                 VARCHAR NOT NULL,
    status               VARCHAR NOT NULL DEFAULT 'queued',
    params               JSON,
    data_snapshot_id     VARCHAR,
    data_policy          JSON,
    output_artifact_id   VARCHAR,
    profile              JSON,
    qa_summary           JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at         TIMESTAMP
);
"""

_FACTOR_VALUES_DDL = """\
CREATE TABLE IF NOT EXISTS factor_values (
    factor_run_id      VARCHAR NOT NULL,
    factor_spec_id     VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    date               DATE NOT NULL,
    asset_id           VARCHAR NOT NULL,
    value              DOUBLE,
    available_at       TIMESTAMP,
    metadata           JSON,
    PRIMARY KEY (factor_run_id, date, asset_id)
);
"""

_FACTOR_SIGNALS_DDL = """\
CREATE TABLE IF NOT EXISTS factor_signals (
    id                 VARCHAR PRIMARY KEY,
    factor_run_id      VARCHAR NOT NULL,
    factor_spec_id     VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    date               DATE NOT NULL,
    asset_id           VARCHAR NOT NULL,
    signal             DOUBLE,
    rank_pct           DOUBLE,
    quantile           INTEGER,
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_MODEL_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS model_specs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    model_type         VARCHAR NOT NULL DEFAULT 'lightgbm',
    objective          VARCHAR NOT NULL DEFAULT 'regression',
    params_schema      JSON,
    default_params     JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_MODEL_EXPERIMENTS_DDL = """\
CREATE TABLE IF NOT EXISTS model_experiments (
    id                     VARCHAR PRIMARY KEY,
    run_id                 VARCHAR NOT NULL,
    project_id             VARCHAR NOT NULL,
    market_profile_id      VARCHAR NOT NULL,
    model_spec_id          VARCHAR,
    dataset_id             VARCHAR NOT NULL,
    name                   VARCHAR NOT NULL,
    model_type             VARCHAR NOT NULL,
    objective              VARCHAR NOT NULL,
    random_seed            INTEGER,
    params                 JSON,
    split_policy           JSON,
    feature_schema         JSON,
    metrics                JSON,
    qa_summary             JSON,
    model_artifact_id      VARCHAR,
    prediction_run_id      VARCHAR,
    prediction_artifact_id VARCHAR,
    status                 VARCHAR NOT NULL DEFAULT 'queued',
    lifecycle_stage        VARCHAR NOT NULL DEFAULT 'experiment',
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at           TIMESTAMP
);
"""

_PREDICTION_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS prediction_runs (
    id                   VARCHAR PRIMARY KEY,
    run_id               VARCHAR NOT NULL,
    project_id           VARCHAR NOT NULL,
    market_profile_id    VARCHAR NOT NULL,
    model_experiment_id  VARCHAR,
    model_package_id     VARCHAR,
    dataset_id           VARCHAR NOT NULL,
    prediction_artifact_id VARCHAR,
    profile              JSON,
    status               VARCHAR NOT NULL DEFAULT 'queued',
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at         TIMESTAMP
);
"""

_MODEL_PACKAGES_DDL = """\
CREATE TABLE IF NOT EXISTS model_packages (
    id                   VARCHAR PRIMARY KEY,
    project_id           VARCHAR NOT NULL,
    market_profile_id    VARCHAR NOT NULL,
    name                 VARCHAR NOT NULL,
    source_experiment_id VARCHAR NOT NULL,
    model_artifact_id    VARCHAR NOT NULL,
    feature_schema       JSON,
    prediction_contract  JSON,
    metrics              JSON,
    qa_summary           JSON,
    lifecycle_stage      VARCHAR NOT NULL DEFAULT 'candidate',
    status               VARCHAR NOT NULL DEFAULT 'candidate',
    metadata             JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_PROMOTION_RECORDS_DDL = """\
CREATE TABLE IF NOT EXISTS promotion_records (
    id                   VARCHAR PRIMARY KEY,
    project_id           VARCHAR NOT NULL,
    source_type          VARCHAR NOT NULL,
    source_id            VARCHAR NOT NULL,
    target_type          VARCHAR NOT NULL,
    target_id            VARCHAR NOT NULL,
    decision             VARCHAR NOT NULL,
    policy_snapshot      JSON,
    qa_summary           JSON,
    approved_by          VARCHAR,
    rationale            TEXT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_MODEL_SIGNALS_DDL = """\
CREATE TABLE IF NOT EXISTS model_signals (
    id                   VARCHAR PRIMARY KEY,
    prediction_run_id    VARCHAR NOT NULL,
    model_package_id     VARCHAR,
    market_profile_id    VARCHAR NOT NULL,
    date                 DATE NOT NULL,
    asset_id             VARCHAR NOT NULL,
    prediction           DOUBLE,
    rank_pct             DOUBLE,
    metadata             JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_PORTFOLIO_CONSTRUCTION_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS portfolio_construction_specs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    method             VARCHAR NOT NULL,
    params             JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_RISK_CONTROL_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS risk_control_specs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    rules              JSON,
    params             JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_REBALANCE_POLICY_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS rebalance_policy_specs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    policy_type        VARCHAR NOT NULL,
    params             JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_EXECUTION_POLICY_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS execution_policy_specs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    policy_type        VARCHAR NOT NULL,
    params             JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_STATE_POLICY_SPECS_DDL = """\
CREATE TABLE IF NOT EXISTS state_policy_specs (
    id                 VARCHAR PRIMARY KEY,
    project_id         VARCHAR NOT NULL,
    market_profile_id  VARCHAR NOT NULL,
    name               VARCHAR NOT NULL,
    description        TEXT,
    policy_type        VARCHAR NOT NULL,
    params             JSON,
    lifecycle_stage    VARCHAR NOT NULL DEFAULT 'experiment',
    status             VARCHAR NOT NULL DEFAULT 'draft',
    metadata           JSON,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_PORTFOLIO_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS portfolio_runs (
    id                               VARCHAR PRIMARY KEY,
    run_id                           VARCHAR NOT NULL,
    project_id                       VARCHAR NOT NULL,
    market_profile_id                VARCHAR NOT NULL,
    decision_date                    DATE NOT NULL,
    portfolio_construction_spec_id   VARCHAR NOT NULL,
    risk_control_spec_id             VARCHAR,
    rebalance_policy_spec_id         VARCHAR,
    execution_policy_spec_id         VARCHAR,
    state_policy_spec_id             VARCHAR,
    input_artifact_id                VARCHAR,
    target_artifact_id               VARCHAR,
    trace_artifact_id                VARCHAR,
    order_intent_artifact_id         VARCHAR,
    profile                          JSON,
    status                           VARCHAR NOT NULL DEFAULT 'queued',
    lifecycle_stage                  VARCHAR NOT NULL DEFAULT 'experiment',
    created_at                       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at                     TIMESTAMP
);
"""

_STRATEGY_GRAPHS_DDL = """\
CREATE TABLE IF NOT EXISTS strategy_graphs (
    id                    VARCHAR PRIMARY KEY,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    name                  VARCHAR NOT NULL,
    description           TEXT,
    graph_type            VARCHAR NOT NULL,
    version               INTEGER NOT NULL DEFAULT 1,
    graph_config          JSON,
    dependency_refs       JSON,
    lifecycle_stage       VARCHAR NOT NULL DEFAULT 'experiment',
    status                VARCHAR NOT NULL DEFAULT 'draft',
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_STRATEGY_NODES_DDL = """\
CREATE TABLE IF NOT EXISTS strategy_nodes (
    id                    VARCHAR PRIMARY KEY,
    strategy_graph_id     VARCHAR NOT NULL,
    node_order            INTEGER NOT NULL,
    node_key              VARCHAR NOT NULL,
    node_type             VARCHAR NOT NULL,
    name                  VARCHAR NOT NULL,
    input_schema          JSON,
    output_schema         JSON,
    data_requirements     JSON,
    params                JSON,
    code_snapshot         TEXT,
    explain_schema        JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_STRATEGY_SIGNALS_3_DDL = """\
CREATE TABLE IF NOT EXISTS strategy_signals (
    id                    VARCHAR PRIMARY KEY,
    run_id                VARCHAR NOT NULL,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    strategy_graph_id     VARCHAR NOT NULL,
    decision_date         DATE NOT NULL,
    portfolio_run_id      VARCHAR,
    explain_artifact_id   VARCHAR,
    status                VARCHAR NOT NULL DEFAULT 'queued',
    lifecycle_stage       VARCHAR NOT NULL DEFAULT 'experiment',
    profile               JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at          TIMESTAMP
);
"""

_BACKTEST_RUNS_3_DDL = """\
CREATE TABLE IF NOT EXISTS backtest_runs (
    id                    VARCHAR PRIMARY KEY,
    run_id                VARCHAR NOT NULL,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    strategy_graph_id     VARCHAR NOT NULL,
    start_date            DATE NOT NULL,
    end_date              DATE NOT NULL,
    config                JSON,
    summary               JSON,
    status                VARCHAR NOT NULL DEFAULT 'queued',
    lifecycle_stage       VARCHAR NOT NULL DEFAULT 'experiment',
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at          TIMESTAMP
);
"""

_BACKTEST_DAILY_3_DDL = """\
CREATE TABLE IF NOT EXISTS backtest_daily (
    backtest_run_id       VARCHAR NOT NULL,
    date                  DATE NOT NULL,
    nav                   DOUBLE,
    cash                  DOUBLE,
    gross_exposure        DOUBLE,
    net_exposure          DOUBLE,
    diagnostics           JSON,
    PRIMARY KEY (backtest_run_id, date)
);
"""

_BACKTEST_TRADES_3_DDL = """\
CREATE TABLE IF NOT EXISTS backtest_trades (
    id                    VARCHAR PRIMARY KEY,
    backtest_run_id       VARCHAR NOT NULL,
    decision_date         DATE,
    execution_date        DATE,
    asset_id              VARCHAR NOT NULL,
    side                  VARCHAR NOT NULL,
    quantity              DOUBLE,
    price                 DOUBLE,
    value                 DOUBLE,
    cost                  DOUBLE,
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_PRODUCTION_SIGNAL_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS production_signal_runs (
    id                    VARCHAR PRIMARY KEY,
    run_id                VARCHAR NOT NULL,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    strategy_graph_id     VARCHAR NOT NULL,
    strategy_signal_id    VARCHAR NOT NULL,
    decision_date         DATE NOT NULL,
    portfolio_run_id      VARCHAR,
    target_artifact_id    VARCHAR,
    order_intent_artifact_id VARCHAR,
    qa_report_id          VARCHAR,
    status                VARCHAR NOT NULL DEFAULT 'queued',
    lifecycle_stage       VARCHAR NOT NULL DEFAULT 'published',
    approved_by           VARCHAR,
    profile               JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at          TIMESTAMP
);
"""

_PAPER_SESSIONS_3_DDL = """\
CREATE TABLE IF NOT EXISTS paper_sessions (
    id                    VARCHAR PRIMARY KEY,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    strategy_graph_id     VARCHAR NOT NULL,
    name                  VARCHAR NOT NULL,
    status                VARCHAR NOT NULL DEFAULT 'active',
    start_date            DATE NOT NULL,
    current_date          DATE,
    initial_capital       DOUBLE NOT NULL DEFAULT 1000000,
    current_nav           DOUBLE NOT NULL DEFAULT 1000000,
    current_weights       JSON,
    config                JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_PAPER_DAILY_3_DDL = """\
CREATE TABLE IF NOT EXISTS paper_daily (
    session_id            VARCHAR NOT NULL,
    date                  DATE NOT NULL,
    nav                   DOUBLE NOT NULL,
    cash                  DOUBLE NOT NULL DEFAULT 0,
    current_weights       JSON,
    production_signal_run_id VARCHAR,
    diagnostics           JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id, date)
);
"""

_REPRODUCIBILITY_BUNDLES_DDL = """\
CREATE TABLE IF NOT EXISTS reproducibility_bundles (
    id                    VARCHAR PRIMARY KEY,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    source_type           VARCHAR NOT NULL,
    source_id             VARCHAR NOT NULL,
    name                  VARCHAR NOT NULL,
    bundle_artifact_id    VARCHAR NOT NULL,
    bundle_payload        JSON,
    status                VARCHAR NOT NULL DEFAULT 'created',
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_AGENT_RESEARCH_PLANS_DDL = """\
CREATE TABLE IF NOT EXISTS agent_research_plans (
    id                    VARCHAR PRIMARY KEY,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    hypothesis            TEXT NOT NULL,
    playbook_id           VARCHAR,
    search_space          JSON,
    budget                JSON,
    stop_conditions       JSON,
    status                VARCHAR NOT NULL DEFAULT 'active',
    created_by            VARCHAR NOT NULL DEFAULT 'agent',
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_AGENT_RESEARCH_TRIALS_DDL = """\
CREATE TABLE IF NOT EXISTS agent_research_trials (
    id                    VARCHAR PRIMARY KEY,
    plan_id               VARCHAR NOT NULL,
    trial_index           INTEGER NOT NULL,
    trial_type            VARCHAR NOT NULL,
    params                JSON,
    result_refs           JSON,
    metrics               JSON,
    qa_report_id          VARCHAR,
    status                VARCHAR NOT NULL DEFAULT 'completed',
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_QA_GATE_RESULTS_DDL = """\
CREATE TABLE IF NOT EXISTS qa_gate_results (
    id                    VARCHAR PRIMARY KEY,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    source_type           VARCHAR NOT NULL,
    source_id             VARCHAR NOT NULL,
    status                VARCHAR NOT NULL,
    blocking              BOOLEAN NOT NULL DEFAULT FALSE,
    findings              JSON,
    metrics               JSON,
    artifact_refs         JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_PROMOTION_POLICIES_DDL = """\
CREATE TABLE IF NOT EXISTS promotion_policies (
    id                    VARCHAR PRIMARY KEY,
    project_id            VARCHAR NOT NULL,
    market_profile_id     VARCHAR NOT NULL,
    name                  VARCHAR NOT NULL,
    policy_type           VARCHAR NOT NULL DEFAULT 'default_quant',
    thresholds            JSON,
    status                VARCHAR NOT NULL DEFAULT 'active',
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_RESEARCH_PLAYBOOKS_DDL = """\
CREATE TABLE IF NOT EXISTS research_playbooks (
    id                    VARCHAR PRIMARY KEY,
    name                  VARCHAR NOT NULL,
    category              VARCHAR NOT NULL,
    description           TEXT,
    steps                 JSON,
    optimization_targets  JSON,
    required_assets       JSON,
    status                VARCHAR NOT NULL DEFAULT 'active',
    metadata              JSON,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_TASK_PAUSE_RULES_DDL = """\
CREATE TABLE IF NOT EXISTS task_pause_rules (
    id          VARCHAR PRIMARY KEY,
    task_type   VARCHAR,
    source      VARCHAR,
    market      VARCHAR,
    reason      TEXT,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP NOT NULL DEFAULT current_timestamp
);
"""

_PERFORMANCE_INDEX_DDLS = [
    "CREATE INDEX IF NOT EXISTS idx_task_runs_status_type_created ON task_runs(status, task_type, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_task_runs_source_created ON task_runs(source, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_research_runs_project_created ON research_runs(project_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_research_runs_type_status ON research_runs(run_type, status)",
    "CREATE INDEX IF NOT EXISTS idx_artifacts_project_created ON artifacts(project_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_artifacts_run_type ON artifacts(run_id, artifact_type)",
    "CREATE INDEX IF NOT EXISTS idx_lineage_from ON lineage_edges(from_type, from_id)",
    "CREATE INDEX IF NOT EXISTS idx_lineage_to ON lineage_edges(to_type, to_id)",
    "CREATE INDEX IF NOT EXISTS idx_agent_plans_project_status ON agent_research_plans(project_id, status, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_agent_trials_plan_index ON agent_research_trials(plan_id, trial_index)",
    "CREATE INDEX IF NOT EXISTS idx_qa_source_status ON qa_gate_results(source_type, source_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_promotion_source_decision ON promotion_records(source_type, source_id, decision)",
    "CREATE INDEX IF NOT EXISTS idx_research_cache_market_type ON research_cache_entries(market, object_type, status)",
    "CREATE INDEX IF NOT EXISTS idx_research_cache_accessed ON research_cache_entries(last_accessed_at, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_macro_obs_provider_series_date ON macro_observations(provider, series_id, date)",
    "CREATE INDEX IF NOT EXISTS idx_macro_obs_available_at ON macro_observations(provider, series_id, available_at)",
    "CREATE INDEX IF NOT EXISTS idx_provider_capabilities_provider ON provider_capabilities(provider, market_profile_id)",
]

_STOCKS_DDL = """\
CREATE TABLE IF NOT EXISTS stocks (
    market      VARCHAR NOT NULL DEFAULT 'US',
    ticker      VARCHAR NOT NULL,
    name        VARCHAR,
    exchange    VARCHAR,
    sector      VARCHAR,
    status      VARCHAR DEFAULT 'active',
    updated_at  TIMESTAMP,
    PRIMARY KEY (market, ticker)
);
"""

_DAILY_BARS_DDL = """\
CREATE TABLE IF NOT EXISTS daily_bars (
    market      VARCHAR NOT NULL DEFAULT 'US',
    ticker      VARCHAR NOT NULL,
    date        DATE NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    adj_factor  DOUBLE DEFAULT 1.0,
    PRIMARY KEY (market, ticker, date)
);
"""

_INDEX_BARS_DDL = """\
CREATE TABLE IF NOT EXISTS index_bars (
    market  VARCHAR NOT NULL DEFAULT 'US',
    symbol  VARCHAR NOT NULL,
    date    DATE NOT NULL,
    open    DOUBLE,
    high    DOUBLE,
    low     DOUBLE,
    close   DOUBLE,
    volume  BIGINT,
    PRIMARY KEY (market, symbol, date)
);
"""

_STOCK_GROUPS_DDL = """\
CREATE TABLE IF NOT EXISTS stock_groups (
    id              VARCHAR PRIMARY KEY,
    market          VARCHAR NOT NULL DEFAULT 'US',
    name            VARCHAR NOT NULL,
    description     TEXT,
    group_type      VARCHAR DEFAULT 'manual',
    filter_expr     TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(market, name)
);
"""

_STOCK_GROUP_MEMBERS_DDL = """\
CREATE TABLE IF NOT EXISTS stock_group_members (
    group_id    VARCHAR NOT NULL,
    market      VARCHAR NOT NULL DEFAULT 'US',
    ticker      VARCHAR NOT NULL,
    PRIMARY KEY (group_id, market, ticker)
);
"""

_DATA_UPDATE_LOG_DDL = """\
CREATE TABLE IF NOT EXISTS data_update_log (
    id              VARCHAR PRIMARY KEY,
    market          VARCHAR NOT NULL DEFAULT 'US',
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
    market      VARCHAR NOT NULL DEFAULT 'US',
    name        VARCHAR NOT NULL,
    description TEXT,
    target_type VARCHAR NOT NULL,
    horizon     INTEGER NOT NULL,
    benchmark   VARCHAR,
    config      TEXT,
    status      VARCHAR DEFAULT 'draft',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(market, name)
);
"""

_FACTORS_DDL = """\
CREATE TABLE IF NOT EXISTS factors (
    id          VARCHAR PRIMARY KEY,
    market      VARCHAR NOT NULL DEFAULT 'US',
    name        VARCHAR NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    description TEXT,
    category    VARCHAR DEFAULT 'custom',
    source_code TEXT NOT NULL,
    params      JSON,
    status      VARCHAR DEFAULT 'draft',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(market, name, version)
);
"""

_FACTOR_VALUES_CACHE_DDL = """\
CREATE TABLE IF NOT EXISTS factor_values_cache (
    market      VARCHAR NOT NULL DEFAULT 'US',
    factor_id   VARCHAR NOT NULL,
    ticker      VARCHAR NOT NULL,
    date        DATE NOT NULL,
    value       DOUBLE,
    PRIMARY KEY (market, factor_id, ticker, date)
);
"""

_FACTOR_EVAL_RESULTS_DDL = """\
CREATE TABLE IF NOT EXISTS factor_eval_results (
    id                  VARCHAR PRIMARY KEY,
    market              VARCHAR NOT NULL DEFAULT 'US',
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
    market          VARCHAR NOT NULL DEFAULT 'US',
    name            VARCHAR NOT NULL,
    description     TEXT,
    factor_refs     JSON NOT NULL,
    preprocessing   JSON NOT NULL,
    status          VARCHAR DEFAULT 'draft',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(market, name)
);
"""

_MODELS_DDL = """\
CREATE TABLE IF NOT EXISTS models (
    id              VARCHAR PRIMARY KEY,
    market          VARCHAR NOT NULL DEFAULT 'US',
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
    market          VARCHAR NOT NULL DEFAULT 'US',
    name            VARCHAR NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    description     TEXT,
    source_code     TEXT NOT NULL,
    required_factors JSON,
    required_models  JSON,
    position_sizing VARCHAR DEFAULT 'equal_weight',
    constraint_config JSON,
    status          VARCHAR DEFAULT 'draft',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(market, name, version)
);
"""

_BACKTEST_RESULTS_DDL = """\
CREATE TABLE IF NOT EXISTS backtest_results (
    id              VARCHAR PRIMARY KEY,
    market          VARCHAR NOT NULL DEFAULT 'US',
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
    market              VARCHAR NOT NULL DEFAULT 'US',
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
    market          VARCHAR NOT NULL DEFAULT 'US',
    ticker          VARCHAR NOT NULL,
    signal          INTEGER,
    target_weight   DOUBLE,
    strength        DOUBLE,
    PRIMARY KEY (run_id, market, ticker)
);
"""

_PAPER_SESSIONS_DDL = """\
CREATE TABLE IF NOT EXISTS paper_trading_sessions (
    id              VARCHAR PRIMARY KEY,
    market          VARCHAR NOT NULL DEFAULT 'US',
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
    market          VARCHAR NOT NULL DEFAULT 'US',
    date            DATE NOT NULL,
    nav             DOUBLE NOT NULL,
    cash            DOUBLE NOT NULL,
    positions_json  JSON,
    trades_json     JSON,
    PRIMARY KEY (session_id, market, date)
);
"""

_PAPER_SIGNAL_CACHE_DDL = """
CREATE TABLE IF NOT EXISTS paper_trading_signal_cache (
    session_id      VARCHAR NOT NULL,
    market          VARCHAR NOT NULL DEFAULT 'US',
    signal_date     DATE NOT NULL,
    result_json     JSON NOT NULL,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (session_id, market, signal_date)
);
"""

_MACRO_SERIES_DDL = """\
CREATE TABLE IF NOT EXISTS macro_series (
    provider        VARCHAR NOT NULL,
    series_id       VARCHAR NOT NULL,
    title           TEXT,
    frequency       VARCHAR,
    units           VARCHAR,
    seasonal_adjustment VARCHAR,
    source          TEXT,
    source_url      TEXT,
    metadata        JSON,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, series_id)
);
"""

_MACRO_OBSERVATIONS_DDL = """\
CREATE TABLE IF NOT EXISTS macro_observations (
    provider        VARCHAR NOT NULL,
    series_id       VARCHAR NOT NULL,
    date            DATE NOT NULL,
    realtime_start  DATE NOT NULL,
    realtime_end    DATE NOT NULL,
    available_at    TIMESTAMP NOT NULL,
    value           DOUBLE,
    source_metadata JSON,
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, series_id, date, realtime_start, realtime_end)
);
"""

_RESEARCH_CACHE_ENTRIES_DDL = """\
CREATE TABLE IF NOT EXISTS research_cache_entries (
    cache_key        VARCHAR PRIMARY KEY,
    object_type      VARCHAR NOT NULL,
    market           VARCHAR NOT NULL DEFAULT 'US',
    object_id        VARCHAR,
    uri              TEXT,
    format           VARCHAR NOT NULL DEFAULT 'parquet',
    schema_version   VARCHAR NOT NULL DEFAULT '1',
    byte_size        BIGINT NOT NULL DEFAULT 0,
    content_hash     VARCHAR,
    row_count        BIGINT NOT NULL DEFAULT 0,
    feature_count    INTEGER NOT NULL DEFAULT 0,
    ticker_count     INTEGER NOT NULL DEFAULT 0,
    start_date       DATE,
    end_date         DATE,
    data_version     VARCHAR,
    retention_class  VARCHAR NOT NULL DEFAULT 'standard',
    rebuildable      BOOLEAN NOT NULL DEFAULT TRUE,
    status           VARCHAR NOT NULL DEFAULT 'active',
    metadata         JSON,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at       TIMESTAMP,
    last_accessed_at TIMESTAMP,
    hit_count        BIGINT NOT NULL DEFAULT 0,
    miss_count       BIGINT NOT NULL DEFAULT 0
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
        ("research_projects", _RESEARCH_PROJECTS_DDL),
        ("research_runs", _RESEARCH_RUNS_DDL),
        ("artifacts", _ARTIFACTS_DDL),
        ("lineage_edges", _LINEAGE_EDGES_DDL),
        ("market_profiles", _MARKET_PROFILES_DDL),
        ("data_policies", _DATA_POLICIES_DDL),
        ("provider_capabilities", _PROVIDER_CAPABILITIES_DDL),
        ("trading_rule_sets", _TRADING_RULE_SETS_DDL),
        ("cost_models", _COST_MODELS_DDL),
        ("benchmark_policies", _BENCHMARK_POLICIES_DDL),
        ("assets", _ASSETS_DDL),
        ("asset_identifiers", _ASSET_IDENTIFIERS_DDL),
        ("asset_lifecycle", _ASSET_LIFECYCLE_DDL),
        ("market_data_snapshots", _MARKET_DATA_SNAPSHOTS_DDL),
        ("trade_status", _TRADE_STATUS_DDL),
        ("corporate_actions", _CORPORATE_ACTIONS_DDL),
        ("universes", _UNIVERSES_DDL),
        ("universe_memberships", _UNIVERSE_MEMBERSHIPS_DDL),
        ("feature_pipelines", _FEATURE_PIPELINES_DDL),
        ("feature_pipeline_nodes", _FEATURE_PIPELINE_NODES_DDL),
        ("label_specs", _LABEL_SPECS_DDL),
        ("label_runs", _LABEL_RUNS_DDL),
        ("datasets", _DATASETS_DDL),
        ("dataset_columns", _DATASET_COLUMNS_DDL),
        ("dataset_profiles", _DATASET_PROFILES_DDL),
        ("factor_specs", _FACTOR_SPECS_DDL),
        ("factor_runs", _FACTOR_RUNS_DDL),
        ("factor_values", _FACTOR_VALUES_DDL),
        ("factor_signals", _FACTOR_SIGNALS_DDL),
        ("model_specs", _MODEL_SPECS_DDL),
        ("model_experiments", _MODEL_EXPERIMENTS_DDL),
        ("prediction_runs", _PREDICTION_RUNS_DDL),
        ("model_packages", _MODEL_PACKAGES_DDL),
        ("promotion_records", _PROMOTION_RECORDS_DDL),
        ("model_signals", _MODEL_SIGNALS_DDL),
        ("portfolio_construction_specs", _PORTFOLIO_CONSTRUCTION_SPECS_DDL),
        ("risk_control_specs", _RISK_CONTROL_SPECS_DDL),
        ("rebalance_policy_specs", _REBALANCE_POLICY_SPECS_DDL),
        ("execution_policy_specs", _EXECUTION_POLICY_SPECS_DDL),
        ("state_policy_specs", _STATE_POLICY_SPECS_DDL),
        ("portfolio_runs", _PORTFOLIO_RUNS_DDL),
        ("strategy_graphs", _STRATEGY_GRAPHS_DDL),
        ("strategy_nodes", _STRATEGY_NODES_DDL),
        ("strategy_signals", _STRATEGY_SIGNALS_3_DDL),
        ("backtest_runs", _BACKTEST_RUNS_3_DDL),
        ("backtest_daily", _BACKTEST_DAILY_3_DDL),
        ("backtest_trades", _BACKTEST_TRADES_3_DDL),
        ("production_signal_runs", _PRODUCTION_SIGNAL_RUNS_DDL),
        ("paper_sessions", _PAPER_SESSIONS_3_DDL),
        ("paper_daily", _PAPER_DAILY_3_DDL),
        ("reproducibility_bundles", _REPRODUCIBILITY_BUNDLES_DDL),
        ("agent_research_plans", _AGENT_RESEARCH_PLANS_DDL),
        ("agent_research_trials", _AGENT_RESEARCH_TRIALS_DDL),
        ("qa_gate_results", _QA_GATE_RESULTS_DDL),
        ("promotion_policies", _PROMOTION_POLICIES_DDL),
        ("research_playbooks", _RESEARCH_PLAYBOOKS_DDL),
        ("task_pause_rules", _TASK_PAUSE_RULES_DDL),
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
        ("paper_trading_signal_cache", _PAPER_SIGNAL_CACHE_DDL),
        ("macro_series", _MACRO_SERIES_DDL),
        ("macro_observations", _MACRO_OBSERVATIONS_DDL),
        ("research_cache_entries", _RESEARCH_CACHE_ENTRIES_DDL),
    ):
        conn.execute(ddl)
        log.info("db.init", table=name)

    # ---- Lightweight migrations for existing databases ----
    _run_migrations(conn)
    _ensure_market_data_foundation(conn)
    _ensure_provider_capabilities(conn)
    _ensure_bootstrap_research_project(conn)
    _ensure_performance_indexes(conn)


def _ensure_performance_indexes(conn) -> None:
    """Create lightweight indexes used by agent-heavy 3.0 research paths."""
    for ddl in _PERFORMANCE_INDEX_DDLS:
        try:
            conn.execute(ddl)
        except Exception as exc:
            log.warning("db.index_skipped", ddl=ddl, error=str(exc))


def _run_migrations(conn) -> None:
    """Apply lightweight schema migrations for existing databases."""
    try:
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'task_runs' AND column_name = 'run_id'"
        ).fetchall()
        if not cols:
            conn.execute("ALTER TABLE task_runs ADD COLUMN run_id VARCHAR")
            log.info("db.migration", action="added task_runs.run_id column")
    except Exception:
        pass

    # Migration: add 'config' column to label_definitions
    try:
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'label_definitions' AND column_name = 'config'"
        ).fetchall()
        if not cols:
            conn.execute("ALTER TABLE label_definitions ADD COLUMN config TEXT")
            log.info("db.migration", action="added label_definitions.config column")
    except Exception:
        pass  # Table may not exist yet (handled by DDL above)

    try:
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'strategies' AND column_name = 'constraint_config'"
        ).fetchall()
        if not cols:
            conn.execute("ALTER TABLE strategies ADD COLUMN constraint_config JSON")
            log.info("db.migration", action="added strategies.constraint_config column")
    except Exception:
        pass

    try:
        report = migrate_market_schema(conn)
        log.info("db.migration.market_schema", status=report["status"])
    except Exception as exc:
        log.error("db.migration.market_schema_failed", error=str(exc))
        raise


def _ensure_bootstrap_research_project(conn) -> None:
    """Create the M1 bootstrap project required by the 3.0 runtime contract."""
    row = conn.execute(
        "SELECT id FROM research_projects WHERE id = 'bootstrap_us'"
    ).fetchone()
    if row:
        conn.execute(
            """UPDATE research_projects
                  SET market_profile_id = COALESCE(market_profile_id, 'US_EQ'),
                      data_policy_id = COALESCE(data_policy_id, 'US_EQ_DATA_YF'),
                      trading_rule_set_id = COALESCE(trading_rule_set_id, 'US_EQ_TRADING_T1_OPEN'),
                      cost_model_id = COALESCE(cost_model_id, 'US_EQ_COST_DEFAULT'),
                      benchmark_policy_id = COALESCE(benchmark_policy_id, 'US_EQ_BENCHMARK_DEFAULT'),
                      updated_at = current_timestamp
                WHERE id = 'bootstrap_us'"""
        )
        return
    conn.execute(
        """INSERT INTO research_projects
           (id, name, market_profile_id, data_policy_id, trading_rule_set_id,
            cost_model_id, benchmark_policy_id, metadata, created_at, updated_at)
           VALUES ('bootstrap_us', 'US Research', 'US_EQ', 'US_EQ_DATA_YF',
                   'US_EQ_TRADING_T1_OPEN', 'US_EQ_COST_DEFAULT',
                   'US_EQ_BENCHMARK_DEFAULT', ?, current_timestamp, current_timestamp)""",
        ['{"bootstrap": true, "phase": "M1"}'],
    )
    log.info("db.bootstrap_project.created", id="bootstrap_us")


def _ensure_market_data_foundation(conn) -> None:
    """Seed M2 market/data policy fixtures.

    These fixtures are intentionally split by concern.  US_EQ is the first
    fully usable market profile; CN_A exists from day one as a real profile so
    future A-share work attaches to policies instead of scattered if-branches.
    """
    import json

    seeds = {
        "data_policies": [
            (
                "US_EQ_DATA_YF",
                "US_EQ",
                "yfinance",
                "adj_factor_column",
                "daily_close_available_after_session",
                "exploratory",
                {
                    "open": "raw session open",
                    "close": "raw session close",
                    "adj_factor": "multiplicative adjustment factor",
                    "available_at": "T after US cash equity close",
                },
                {"source": "M2_seed"},
            ),
            (
                "CN_A_DATA_BAOSTOCK",
                "CN_A",
                "baostock",
                "explicit_qfq_hfq_none",
                "daily_close_available_after_session",
                "exploratory",
                {
                    "open": "provider adjusted according to selected adjustment policy",
                    "close": "provider adjusted according to selected adjustment policy",
                    "tradability": "requires suspend/ST/limit fields before execution use",
                    "available_at": "T after CN cash equity close",
                },
                {"source": "M2_seed", "phase": "profile_fixture"},
            ),
        ],
        "trading_rule_sets": [
            (
                "US_EQ_TRADING_T1_OPEN",
                "US_EQ",
                "NYSE",
                "T+1_OPEN",
                "T+2_CASH",
                1,
                True,
                False,
                ["missing_price", "zero_volume", "stale_asset", "delisted"],
                {
                    "decision_date": "T",
                    "execution_date": "next_trading_day",
                    "execution_price": "open",
                    "shorting": "research_allowed_if_strategy_enables",
                },
            ),
            (
                "CN_A_TRADING_T1_RULES",
                "CN_A",
                "XSHG",
                "T+1_OPEN_OR_NEXT_TRADABLE",
                "T+1_STOCK_T0_CASH",
                100,
                False,
                True,
                [
                    "is_suspended",
                    "st_status",
                    "limit_up",
                    "limit_down",
                    "one_price_limit",
                    "listed_days",
                    "delisting_risk",
                ],
                {
                    "buy": "must pass suspend/ST/listed_days/limit checks",
                    "sell": "stock T+1 restriction applies",
                    "lot_size": 100,
                    "price_limit_execution": "blocked when limit prevents fill",
                },
            ),
        ],
        "cost_models": [
            (
                "US_EQ_COST_DEFAULT",
                "US_EQ",
                0.001,
                0.001,
                0.0,
                0.0,
                "USD",
                {"source": "existing backtest defaults"},
            ),
            (
                "CN_A_COST_DEFAULT",
                "CN_A",
                0.0003,
                0.0005,
                0.0005,
                5.0,
                "CNY",
                {"phase": "profile_fixture"},
            ),
        ],
        "benchmark_policies": [
            (
                "US_EQ_BENCHMARK_DEFAULT",
                "US_EQ",
                "SPY",
                ["SPY", "QQQ", "IWM"],
                {"SPY": "US broad equity ETF", "QQQ": "US growth/tech ETF"},
            ),
            (
                "CN_A_BENCHMARK_DEFAULT",
                "CN_A",
                "sh.000300",
                ["sh.000300", "sh.000905", "sh.000852"],
                {"sh.000300": "CSI 300", "sh.000905": "CSI 500"},
            ),
        ],
        "market_profiles": [
            (
                "US_EQ",
                "US",
                "equity",
                "US Equities",
                "USD",
                "America/New_York",
                "AAPL",
                "AAPL",
                "US_EQ_DATA_YF",
                "US_EQ_TRADING_T1_OPEN",
                "US_EQ_COST_DEFAULT",
                "US_EQ_BENCHMARK_DEFAULT",
                "active",
                {"provider": "yfinance", "phase": "M2"},
            ),
            (
                "CN_A",
                "CN",
                "equity",
                "China A Shares",
                "CNY",
                "Asia/Shanghai",
                "600000.SH",
                "sh.600000",
                "CN_A_DATA_BAOSTOCK",
                "CN_A_TRADING_T1_RULES",
                "CN_A_COST_DEFAULT",
                "CN_A_BENCHMARK_DEFAULT",
                "profile_fixture",
                {"provider": "baostock", "phase": "M2_profile_fixture"},
            ),
        ],
    }

    conn.execute(
        "DELETE FROM market_profiles WHERE id IN ('US_EQ', 'CN_A')"
    )
    conn.execute(
        "DELETE FROM data_policies WHERE id IN ('US_EQ_DATA_YF', 'CN_A_DATA_BAOSTOCK')"
    )
    conn.execute(
        "DELETE FROM trading_rule_sets WHERE id IN ('US_EQ_TRADING_T1_OPEN', 'CN_A_TRADING_T1_RULES')"
    )
    conn.execute(
        "DELETE FROM cost_models WHERE id IN ('US_EQ_COST_DEFAULT', 'CN_A_COST_DEFAULT')"
    )
    conn.execute(
        "DELETE FROM benchmark_policies WHERE id IN ('US_EQ_BENCHMARK_DEFAULT', 'CN_A_BENCHMARK_DEFAULT')"
    )

    for row in seeds["data_policies"]:
        conn.execute(
            """INSERT INTO data_policies
               (id, market_profile_id, provider, price_adjustment,
                bar_availability, data_quality_level, field_semantics,
                metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)""",
            [*row[:6], json.dumps(row[6]), json.dumps(row[7])],
        )

    for row in seeds["trading_rule_sets"]:
        conn.execute(
            """INSERT INTO trading_rule_sets
               (id, market_profile_id, calendar, decision_to_execution,
                settlement_cycle, lot_size, allow_short, limit_up_down,
                tradability_fields, rules, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)""",
            [*row[:8], json.dumps(row[8]), json.dumps(row[9])],
        )

    for row in seeds["cost_models"]:
        conn.execute(
            """INSERT INTO cost_models
               (id, market_profile_id, commission_rate, slippage_rate,
                stamp_tax_rate, min_commission, currency, metadata,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)""",
            [*row[:7], json.dumps(row[7])],
        )

    for row in seeds["benchmark_policies"]:
        conn.execute(
            """INSERT INTO benchmark_policies
               (id, market_profile_id, default_benchmark, benchmarks,
                benchmark_semantics, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, current_timestamp, current_timestamp)""",
            [*row[:3], json.dumps(row[3]), json.dumps(row[4])],
        )

    for row in seeds["market_profiles"]:
        conn.execute(
            """INSERT INTO market_profiles
               (id, market_code, asset_class, name, currency, timezone,
                symbol_format, provider_symbol_format, data_policy_id,
                trading_rule_set_id, cost_model_id, benchmark_policy_id,
                status, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)""",
            [*row[:13], json.dumps(row[13])],
        )

    log.info("db.market_data_foundation.seeded", profiles=["US_EQ", "CN_A"])


def _ensure_provider_capabilities(conn) -> None:
    """Seed explicit capability and quality semantics for free data sources."""
    import json

    rows = [
        (
            "yfinance",
            "stock_list",
            "US_EQ",
            "reference_universe",
            "exploratory",
            False,
            "free_web_source",
            "best_effort_web_download",
            {"source": "seed", "restriction": "no_sla"},
        ),
        (
            "yfinance",
            "daily_bars",
            "US_EQ",
            "ohlcv_daily",
            "exploratory",
            False,
            "free_web_source",
            "best_effort_web_download",
            {"source": "seed", "adjustment": "adj_factor_column", "restriction": "no_pit"},
        ),
        (
            "yfinance",
            "index_bars",
            "US_EQ",
            "benchmark_daily",
            "exploratory",
            False,
            "free_web_source",
            "best_effort_web_download",
            {"source": "seed", "restriction": "no_sla"},
        ),
        (
            "baostock",
            "stock_list",
            "CN_A",
            "reference_universe",
            "exploratory",
            False,
            "free_web_source",
            "best_effort_web_download",
            {"source": "seed", "restriction": "no_sla"},
        ),
        (
            "baostock",
            "daily_bars",
            "CN_A",
            "ohlcv_daily",
            "exploratory",
            False,
            "free_web_source",
            "best_effort_web_download",
            {"source": "seed", "adjustment": "qfq_default", "restriction": "no_pit"},
        ),
        (
            "baostock",
            "trade_status",
            "CN_A",
            "tradability_daily",
            "exploratory",
            False,
            "free_web_source",
            "best_effort_web_download",
            {"source": "seed", "fields": ["tradestatus", "isST"]},
        ),
        (
            "fred",
            "macro_observations",
            "GLOBAL",
            "macro_time_series",
            "research_grade",
            False,
            "free_api_key_required",
            "api_key_quota_limited",
            {
                "source": "seed",
                "restriction": "current realtime window unless explicitly replayed",
                "configured_by": "external_data.fred",
            },
        ),
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO provider_capabilities
           (provider, dataset, market_profile_id, capability, quality_level,
            pit_supported, license_scope, availability, as_of_date,
            available_at, metadata, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_date, current_timestamp,
                   ?, current_timestamp, current_timestamp)""",
        [
            [provider, dataset, profile, capability, quality, pit, license_scope, availability, json.dumps(metadata)]
            for (
                provider,
                dataset,
                profile,
                capability,
                quality,
                pit,
                license_scope,
                availability,
                metadata,
            ) in rows
        ],
    )
    log.info("db.provider_capabilities.seeded", count=len(rows))


def close_db() -> None:
    """Close the DuckDB connection."""
    global _connection
    with _lock:
        if _connection is not None:
            _connection.close()
            _connection = None
            log.info("db.closed")
