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

# Check if backend is running (DB would be locked)
if lsof "$DB_PATH" >/dev/null 2>&1; then
    echo "Warning: Database is in use. Stop the backend first:"
    echo "  bash scripts/stop.sh"
    echo ""
    echo "Attempting backup anyway (read-only)..."
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DEST="$BACKUP_DIR/$TIMESTAMP"
mkdir -p "$DEST"

echo "==> Backing up all QAgent data to $DEST"

cd "$PROJECT_ROOT"
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
