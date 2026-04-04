#!/usr/bin/env bash
# Stop QAgent backend and frontend processes.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

stop_process() {
    local name="$1"
    local pidfile="$PROJECT_ROOT/.$name.pid"
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            echo "  -> Stopping $name (PID=$pid)"
            kill "$pid" 2>/dev/null || true
        else
            echo "  -> $name (PID=$pid) already stopped"
        fi
        rm -f "$pidfile"
    else
        echo "  -> No PID file for $name"
    fi
}

echo "==> Stopping QAgent"
stop_process "backend"
stop_process "frontend"
echo "==> Done"
