#!/usr/bin/env bash
# Restore stock market data from a Parquet backup into the current database.
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

echo "==> Restoring market data from $BACKUP_DIR"
echo "    Target DB: $DB_PATH"
echo ""

cd "$PROJECT_ROOT"
uv run python << PYEOF
import duckdb
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

# Restore order matters: stocks first (referenced by daily_bars), groups before members
restore_order = [
    "stocks",
    "daily_bars",
    "index_bars",
    "stock_groups",
    "stock_group_members",
]

total_rows = 0
for table in restore_order:
    pq = backup / f"{table}.parquet"
    if not pq.exists():
        print(f"  {table}: no backup file, skipping")
        continue

    # Clear existing data in this table
    conn.execute(f"DELETE FROM {table}")

    # Load from parquet
    conn.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{pq}')")
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = pq.stat().st_size / 1024 / 1024
    print(f"  {table}: restored {count:,} rows from {size_mb:.1f} MB parquet")
    total_rows += count

print(f"\nDone! {total_rows:,} total rows restored.")

# Show summary
stocks = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
bars = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
date_range = conn.execute("SELECT MIN(date), MAX(date) FROM daily_bars").fetchone()
idx = conn.execute("SELECT COUNT(*) FROM index_bars").fetchone()[0]
groups = conn.execute("SELECT COUNT(*) FROM stock_groups").fetchone()[0]

print(f"\nData summary:")
print(f"  Stocks:     {stocks:,}")
print(f"  Daily bars: {bars:,} ({date_range[0]} to {date_range[1]})")
print(f"  Index bars: {idx:,}")
print(f"  Groups:     {groups}")
PYEOF

echo ""
echo "==> Restore complete. You can now start the backend:"
echo "    bash scripts/start.sh"
