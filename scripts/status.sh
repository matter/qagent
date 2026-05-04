#!/usr/bin/env bash
# Show local QAgent process and health status.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

show_pid() {
    local name="$1"
    local pidfile="$PROJECT_ROOT/.$name.pid"
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "$name: running PID=$(cat "$pidfile")"
    else
        echo "$name: stopped"
    fi
}

show_pid backend
show_pid frontend

if command -v curl >/dev/null 2>&1; then
    if curl --max-time 5 -fsS http://127.0.0.1:8000/api/health >/tmp/qagent-health.$$ 2>/dev/null; then
        echo "health: $(cat /tmp/qagent-health.$$)"
    else
        echo "health: unavailable"
    fi
    rm -f /tmp/qagent-health.$$
fi
