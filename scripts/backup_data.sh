#!/usr/bin/env bash
# Backup stock market data (stocks, daily_bars, index_bars) to Parquet files.
# This preserves price data across DB schema changes and rebuilds.
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

echo "==> Backing up market data to $DEST"

cd "$PROJECT_ROOT"
uv run python << PYEOF
import duckdb
import time

db = duckdb.connect("$DB_PATH", read_only=True)
dest = "$DEST"

tables = {
    "stocks": "SELECT * FROM stocks",
    "daily_bars": "SELECT * FROM daily_bars",
    "index_bars": "SELECT * FROM index_bars",
    "stock_groups": "SELECT * FROM stock_groups",
    "stock_group_members": "SELECT * FROM stock_group_members",
}

total_rows = 0
for name, query in tables.items():
    count = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    if count == 0:
        print(f"  {name}: empty, skipping")
        continue
    out = f"{dest}/{name}.parquet"
    db.execute(f"COPY ({query}) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    import os
    size_mb = os.path.getsize(out) / 1024 / 1024
    print(f"  {name}: {count:,} rows -> {out} ({size_mb:.1f} MB)")
    total_rows += count

db.close()
print(f"\nDone! {total_rows:,} total rows backed up to {dest}")
PYEOF

# Write a manifest
echo "{\"timestamp\": \"$TIMESTAMP\", \"db_path\": \"$DB_PATH\"}" > "$DEST/manifest.json"

echo ""
echo "==> Backup complete: $DEST"
echo "    Restore with: bash scripts/restore_data.sh $DEST"
