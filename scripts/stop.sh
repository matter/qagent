#!/usr/bin/env bash
# Stop QAgent backend and frontend processes.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

stop_by_pid() {
    local name="$1"
    local pidfile="$PROJECT_ROOT/.$name.pid"
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            echo "  -> Stopping $name (PID=$pid)"
            kill "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
    fi
}

stop_by_port() {
    local port="$1"
    local name="$2"
    local pids
    pids=$(lsof -ti :"$port" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "  -> Killing $name on port $port (PIDs: $pids)"
        echo "$pids" | xargs kill -9 2>/dev/null || true
    fi
}

echo "==> Stopping QAgent"

# Try PID files first
stop_by_pid "backend"
stop_by_pid "frontend"

sleep 1

# Then force-kill anything still on the ports
stop_by_port 8000 "backend"
stop_by_port 5173 "frontend"

echo "==> Done"
