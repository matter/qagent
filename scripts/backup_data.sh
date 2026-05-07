#!/usr/bin/env bash
# Backup all QAgent data: market data, factors, models, strategies, and related assets.
#
# Usage:
#   bash scripts/backup_data.sh              # backup to data/backup/
#   bash scripts/backup_data.sh /path/to/dir # backup to custom dir

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DB_PATH="$PROJECT_ROOT/data/qagent.duckdb"
BACKUP_DIR="${1:-$PROJECT_ROOT/data/backup}"

if [ ! -f "$DB_PATH" ]; then
    echo "Error: Database not found at $DB_PATH"
    exit 1
fi

cd "$PROJECT_ROOT"

echo "==> Checking database maintenance preflight"
uv run python - << PYEOF
from pathlib import Path
from backend.services.db_preflight_service import DbPreflightService

result = DbPreflightService().check_database(Path("$DB_PATH"))
if not result["ok"]:
    print(f"Error: {result['message']}")
    print(f"Status: {result['status']}")
    print(f"Action: {result['action']}")
    raise SystemExit(1)
print(f"Database preflight: {result['status']}")
PYEOF

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DEST="$BACKUP_DIR/$TIMESTAMP"
mkdir -p "$DEST"

echo "==> Backing up all QAgent data to $DEST"

uv run python << PYEOF
import duckdb
import os

db = duckdb.connect("$DB_PATH", read_only=True)
dest = "$DEST"

tables = {
    # Market data
    "stocks": "SELECT * FROM stocks",
    "daily_bars": "SELECT * FROM daily_bars",
    "index_bars": "SELECT * FROM index_bars",
    "stock_groups": "SELECT * FROM stock_groups",
    "stock_group_members": "SELECT * FROM stock_group_members",
    "data_update_log": "SELECT * FROM data_update_log",
    # Factors
    "factors": "SELECT * FROM factors",
    "factor_values_cache": "SELECT * FROM factor_values_cache",
    "factor_eval_results": "SELECT * FROM factor_eval_results",
    # Feature sets
    "feature_sets": "SELECT * FROM feature_sets",
    # Labels
    "label_definitions": "SELECT * FROM label_definitions",
    # Models
    "models": "SELECT * FROM models",
    # Strategies
    "strategies": "SELECT * FROM strategies",
    # Backtest results
    "backtest_results": "SELECT * FROM backtest_results",
    # Signals
    "signal_runs": "SELECT * FROM signal_runs",
    "signal_details": "SELECT * FROM signal_details",
    # Tasks
    "task_runs": "SELECT * FROM task_runs",
    # 3.0 research kernel
    "research_projects": "SELECT * FROM research_projects",
    "research_runs": "SELECT * FROM research_runs",
    "artifacts": "SELECT * FROM artifacts",
    "lineage_edges": "SELECT * FROM lineage_edges",
    # 3.0 market foundation
    "market_profiles": "SELECT * FROM market_profiles",
    "data_policies": "SELECT * FROM data_policies",
    "trading_rule_sets": "SELECT * FROM trading_rule_sets",
    "cost_models": "SELECT * FROM cost_models",
    "benchmark_policies": "SELECT * FROM benchmark_policies",
    "assets": "SELECT * FROM assets",
    "asset_identifiers": "SELECT * FROM asset_identifiers",
    "asset_lifecycle": "SELECT * FROM asset_lifecycle",
    "market_data_snapshots": "SELECT * FROM market_data_snapshots",
    "trade_status": "SELECT * FROM trade_status",
    "corporate_actions": "SELECT * FROM corporate_actions",
    # 3.0 universe/dataset engine
    "universes": "SELECT * FROM universes",
    "universe_memberships": "SELECT * FROM universe_memberships",
    "feature_pipelines": "SELECT * FROM feature_pipelines",
    "feature_pipeline_nodes": "SELECT * FROM feature_pipeline_nodes",
    "label_specs": "SELECT * FROM label_specs",
    "label_runs": "SELECT * FROM label_runs",
    "datasets": "SELECT * FROM datasets",
    "dataset_columns": "SELECT * FROM dataset_columns",
    "dataset_profiles": "SELECT * FROM dataset_profiles",
    # 3.0 factor engine
    "factor_specs": "SELECT * FROM factor_specs",
    "factor_runs": "SELECT * FROM factor_runs",
    "factor_values": "SELECT * FROM factor_values",
    "factor_signals": "SELECT * FROM factor_signals",
    # 3.0 model experiment/package engine
    "model_specs": "SELECT * FROM model_specs",
    "model_experiments": "SELECT * FROM model_experiments",
    "prediction_runs": "SELECT * FROM prediction_runs",
    "model_packages": "SELECT * FROM model_packages",
    "promotion_records": "SELECT * FROM promotion_records",
    "model_signals": "SELECT * FROM model_signals",
    # 3.0 portfolio/risk/execution assets
    "portfolio_construction_specs": "SELECT * FROM portfolio_construction_specs",
    "risk_control_specs": "SELECT * FROM risk_control_specs",
    "rebalance_policy_specs": "SELECT * FROM rebalance_policy_specs",
    "execution_policy_specs": "SELECT * FROM execution_policy_specs",
    "state_policy_specs": "SELECT * FROM state_policy_specs",
    "portfolio_runs": "SELECT * FROM portfolio_runs",
    # 3.0 strategy graph runtime
    "strategy_graphs": "SELECT * FROM strategy_graphs",
    "strategy_nodes": "SELECT * FROM strategy_nodes",
    "strategy_signals": "SELECT * FROM strategy_signals",
    "backtest_runs": "SELECT * FROM backtest_runs",
    "backtest_daily": "SELECT * FROM backtest_daily",
    "backtest_trades": "SELECT * FROM backtest_trades",
    # 3.0 production signal/paper
    "production_signal_runs": "SELECT * FROM production_signal_runs",
    "paper_sessions": "SELECT * FROM paper_sessions",
    "paper_daily": "SELECT * FROM paper_daily",
    "reproducibility_bundles": "SELECT * FROM reproducibility_bundles",
    # 3.0 agent research QA/playbooks
    "agent_research_plans": "SELECT * FROM agent_research_plans",
    "agent_research_trials": "SELECT * FROM agent_research_trials",
    "qa_gate_results": "SELECT * FROM qa_gate_results",
    "promotion_policies": "SELECT * FROM promotion_policies",
    "research_playbooks": "SELECT * FROM research_playbooks",
}

total_rows = 0
for name, query in tables.items():
    try:
        count = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    except Exception:
        print(f"  {name}: table not found, skipping")
        continue
    if count == 0:
        print(f"  {name}: empty, skipping")
        continue
    out = f"{dest}/{name}.parquet"
    db.execute(f"COPY ({query}) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    size_mb = os.path.getsize(out) / 1024 / 1024
    print(f"  {name}: {count:,} rows -> {out} ({size_mb:.1f} MB)")
    total_rows += count

db.close()
print(f"\nDB tables: {total_rows:,} total rows backed up")
PYEOF

# Backup model files (joblib + metadata)
MODELS_DIR="$PROJECT_ROOT/data/models"
if [ -d "$MODELS_DIR" ] && [ "$(ls -A "$MODELS_DIR" 2>/dev/null)" ]; then
    echo ""
    echo "==> Backing up model files from $MODELS_DIR"
    mkdir -p "$DEST/models"
    cp -r "$MODELS_DIR"/* "$DEST/models/"
    MODEL_COUNT=$(find "$DEST/models" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
    MODEL_SIZE=$(du -sh "$DEST/models" | cut -f1)
    echo "  $MODEL_COUNT model directories ($MODEL_SIZE)"
else
    echo ""
    echo "  No model files to backup"
fi

# Write a manifest
echo "{\"timestamp\": \"$TIMESTAMP\", \"db_path\": \"$DB_PATH\", \"includes\": \"full\"}" > "$DEST/manifest.json"

echo ""
echo "==> Backup complete: $DEST"
echo "    Restore with: bash scripts/restore_data.sh $DEST"
