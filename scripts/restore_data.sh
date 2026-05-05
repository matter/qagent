#!/usr/bin/env bash
# Restore all QAgent data from a Parquet backup into the current database.
# Safe to run on a fresh DB — creates tables if needed, then inserts data.
#
# Usage:
#   bash scripts/restore_data.sh data/backup/20260405_120000
#   bash scripts/restore_data.sh /path/to/backup/dir

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DB_PATH="$PROJECT_ROOT/data/qagent.duckdb"
BACKUP_DIR="$1"

if [ -z "$BACKUP_DIR" ]; then
    # Find latest backup
    BACKUP_BASE="$PROJECT_ROOT/data/backup"
    if [ -d "$BACKUP_BASE" ]; then
        BACKUP_DIR=$(ls -d "$BACKUP_BASE"/*/ 2>/dev/null | sort -r | head -1)
    fi
    if [ -z "$BACKUP_DIR" ]; then
        echo "Usage: bash scripts/restore_data.sh <backup_dir>"
        echo ""
        echo "Available backups:"
        ls -1d "$PROJECT_ROOT/data/backup"/*/ 2>/dev/null || echo "  (none)"
        exit 1
    fi
    echo "Using latest backup: $BACKUP_DIR"
fi

if [ ! -d "$BACKUP_DIR" ]; then
    echo "Error: Backup directory not found: $BACKUP_DIR"
    exit 1
fi

# Check if backend is running
if lsof "$DB_PATH" >/dev/null 2>&1; then
    echo "Error: Database is in use. Stop the backend first:"
    echo "  bash scripts/stop.sh"
    exit 1
fi

echo "==> Restoring all QAgent data from $BACKUP_DIR"
echo "    Target DB: $DB_PATH"
echo ""

cd "$PROJECT_ROOT"
uv run python << PYEOF
import os
from pathlib import Path

backup = Path("$BACKUP_DIR")
db_path = "$DB_PATH"

# Init DB (create tables if fresh)
from backend.db import init_db
init_db()

# Now open a direct connection for bulk loading
from backend.db import get_connection
conn = get_connection()

# Restore order matters: parent tables before children
restore_order = [
    # Market data
    "stocks",
    "daily_bars",
    "index_bars",
    "stock_groups",
    "stock_group_members",
    "data_update_log",
    # Factors
    "factors",
    "factor_values_cache",
    "factor_eval_results",
    # Feature sets
    "feature_sets",
    # Labels
    "label_definitions",
    # Models
    "models",
    # Strategies
    "strategies",
    # Backtest results
    "backtest_results",
    # Signals
    "signal_runs",
    "signal_details",
    # Tasks
    "task_runs",
    # 3.0 research kernel
    "research_projects",
    "research_runs",
    "artifacts",
    "lineage_edges",
    # 3.0 market foundation
    "market_profiles",
    "data_policies",
    "trading_rule_sets",
    "cost_models",
    "benchmark_policies",
    "assets",
    "asset_identifiers",
    "asset_lifecycle",
    "market_data_snapshots",
    "trade_status",
    "corporate_actions",
    # 3.0 universe/dataset engine
    "universes",
    "universe_memberships",
    "feature_pipelines",
    "feature_pipeline_nodes",
    "label_specs",
    "label_runs",
    "datasets",
    "dataset_columns",
    "dataset_profiles",
    # 3.0 factor engine
    "factor_specs",
    "factor_runs",
    "factor_values",
    "factor_signals",
    # 3.0 model experiment/package engine
    "model_specs",
    "model_experiments",
    "prediction_runs",
    "model_packages",
    "promotion_records",
    "model_signals",
    # 3.0 portfolio/risk/execution assets
    "portfolio_construction_specs",
    "risk_control_specs",
    "rebalance_policy_specs",
    "execution_policy_specs",
    "state_policy_specs",
    "portfolio_runs",
    # 3.0 strategy graph runtime
    "strategy_graphs",
    "strategy_nodes",
    "strategy_signals",
    "backtest_runs",
    "backtest_daily",
    "backtest_trades",
    # 3.0 production signal/paper
    "production_signal_runs",
    "paper_sessions",
    "paper_daily",
    "reproducibility_bundles",
    # 3.0 agent research QA/playbooks
    "agent_research_plans",
    "agent_research_trials",
    "qa_gate_results",
    "promotion_policies",
    "research_playbooks",
]

total_rows = 0
for table in restore_order:
    pq = backup / f"{table}.parquet"
    if not pq.exists():
        print(f"  {table}: no backup file, skipping")
        continue

    # Clear existing data in this table
    try:
        conn.execute(f"DELETE FROM {table}")
    except Exception:
        print(f"  {table}: table not found in DB, skipping")
        continue

    # Load from parquet
    conn.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{pq}')")
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = pq.stat().st_size / 1024 / 1024
    print(f"  {table}: restored {count:,} rows from {size_mb:.1f} MB parquet")
    total_rows += count

print(f"\nDB tables: {total_rows:,} total rows restored.")

# Show summary
stocks = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
bars = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
date_range = conn.execute("SELECT MIN(date), MAX(date) FROM daily_bars").fetchone()
idx = conn.execute("SELECT COUNT(*) FROM index_bars").fetchone()[0]
groups = conn.execute("SELECT COUNT(*) FROM stock_groups").fetchone()[0]
factors = conn.execute("SELECT COUNT(*) FROM factors").fetchone()[0]
models = conn.execute("SELECT COUNT(*) FROM models").fetchone()[0]
strategies = conn.execute("SELECT COUNT(*) FROM strategies").fetchone()[0]
backtests = conn.execute("SELECT COUNT(*) FROM backtest_results").fetchone()[0]
research_runs = conn.execute("SELECT COUNT(*) FROM research_runs").fetchone()[0]
artifacts = conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]

print(f"\nData summary:")
print(f"  Stocks:     {stocks:,}")
print(f"  Daily bars: {bars:,} ({date_range[0]} to {date_range[1]})")
print(f"  Index bars: {idx:,}")
print(f"  Groups:     {groups}")
print(f"  Factors:    {factors}")
print(f"  Models:     {models}")
print(f"  Strategies: {strategies}")
print(f"  Backtests:  {backtests}")
print(f"  Research runs: {research_runs}")
print(f"  Artifacts:  {artifacts}")
PYEOF

# Restore model files
MODELS_SRC="$BACKUP_DIR/models"
MODELS_DEST="$PROJECT_ROOT/data/models"
if [ -d "$MODELS_SRC" ] && [ "$(ls -A "$MODELS_SRC" 2>/dev/null)" ]; then
    echo ""
    echo "==> Restoring model files to $MODELS_DEST"
    mkdir -p "$MODELS_DEST"
    cp -r "$MODELS_SRC"/* "$MODELS_DEST/"
    MODEL_COUNT=$(find "$MODELS_DEST" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
    echo "  Restored $MODEL_COUNT model directories"
fi

echo ""
echo "==> Restore complete. You can now start the backend:"
echo "    bash scripts/start.sh"
