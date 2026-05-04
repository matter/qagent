#!/usr/bin/env bash
# Start QAgent backend and frontend detached from the caller shell.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

is_running() {
    local pidfile="$1"
    [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

BACKEND_PID_FILE="$PROJECT_ROOT/.backend.pid"
FRONTEND_PID_FILE="$PROJECT_ROOT/.frontend.pid"

echo "==> Starting QAgent detached"

if is_running "$BACKEND_PID_FILE"; then
    echo "  -> Backend already running (PID=$(cat "$BACKEND_PID_FILE"))"
else
    (
        cd "$PROJECT_ROOT"
        nohup uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 \
            > "$LOG_DIR/backend-detached.log" 2>&1 &
        echo $! > "$BACKEND_PID_FILE"
    )
    echo "  -> Backend PID=$(cat "$BACKEND_PID_FILE")"
fi

if is_running "$FRONTEND_PID_FILE"; then
    echo "  -> Frontend already running (PID=$(cat "$FRONTEND_PID_FILE"))"
else
    (
        cd "$PROJECT_ROOT/frontend"
        nohup pnpm dev > "$LOG_DIR/frontend-detached.log" 2>&1 &
        echo $! > "$FRONTEND_PID_FILE"
    )
    echo "  -> Frontend PID=$(cat "$FRONTEND_PID_FILE")"
fi

echo "==> QAgent detached"
echo "    Backend:  http://127.0.0.1:8000"
echo "    Frontend: http://localhost:5173"
echo "    Status:   scripts/status.sh"
echo "    Stop:     scripts/stop.sh"
