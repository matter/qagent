#!/usr/bin/env bash
# Start QAgent backend and frontend detached from the caller shell.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LAUNCH_AGENT_DIR="$HOME/Library/LaunchAgents"
mkdir -p "$LAUNCH_AGENT_DIR"

BACKEND_LABEL="com.qagent.backend"
FRONTEND_LABEL="com.qagent.frontend"
BACKEND_PLIST="$LAUNCH_AGENT_DIR/$BACKEND_LABEL.plist"
FRONTEND_PLIST="$LAUNCH_AGENT_DIR/$FRONTEND_LABEL.plist"

is_running() {
    local pidfile="$1"
    [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

port_pid() {
    local port="$1"
    if command -v lsof >/dev/null 2>&1; then
        lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
    fi
}

bootstrap_domain() {
    echo "gui/$(id -u)"
}

write_backend_plist() {
    local uv_bin
    uv_bin="$(command -v uv)"
    cat > "$BACKEND_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$BACKEND_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$uv_bin</string>
    <string>run</string>
    <string>uvicorn</string>
    <string>backend.app:app</string>
    <string>--host</string>
    <string>127.0.0.1</string>
    <string>--port</string>
    <string>8000</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$PROJECT_ROOT</string>
  <key>StandardOutPath</key>
  <string>$LOG_DIR/backend-detached.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/backend-detached.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
</dict>
</plist>
EOF
}

write_frontend_plist() {
    local pnpm_bin
    pnpm_bin="$(command -v pnpm)"
    cat > "$FRONTEND_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$FRONTEND_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$pnpm_bin</string>
    <string>dev</string>
    <string>--host</string>
    <string>127.0.0.1</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$PROJECT_ROOT/frontend</string>
  <key>StandardOutPath</key>
  <string>$LOG_DIR/frontend-detached.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/frontend-detached.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
</dict>
</plist>
EOF
}

start_launch_agent() {
    local label="$1"
    local plist="$2"
    local domain
    domain="$(bootstrap_domain)"
    launchctl bootout "$domain" "$plist" >/dev/null 2>&1 || true
    launchctl bootstrap "$domain" "$plist"
    launchctl kickstart -k "$domain/$label" >/dev/null 2>&1 || true
}

wait_for_port() {
    local port="$1"
    local tries="${2:-30}"
    local pid=""
    for _ in $(seq 1 "$tries"); do
        pid="$(port_pid "$port")"
        if [ -n "$pid" ]; then
            echo "$pid"
            return 0
        fi
        sleep 1
    done
    return 1
}

BACKEND_PID_FILE="$PROJECT_ROOT/.backend.pid"
FRONTEND_PID_FILE="$PROJECT_ROOT/.frontend.pid"

echo "==> Starting QAgent detached"

if is_running "$BACKEND_PID_FILE"; then
    echo "  -> Backend already running (PID=$(cat "$BACKEND_PID_FILE"))"
else
    existing_pid="$(port_pid 8000)"
    if [ -n "$existing_pid" ]; then
        echo "$existing_pid" > "$BACKEND_PID_FILE"
        echo "  -> Backend already listening on :8000 (PID=$existing_pid)"
    else
        write_backend_plist
        start_launch_agent "$BACKEND_LABEL" "$BACKEND_PLIST"
        backend_pid="$(wait_for_port 8000 30)"
        echo "$backend_pid" > "$BACKEND_PID_FILE"
        echo "  -> Backend PID=$backend_pid"
    fi
fi

if is_running "$FRONTEND_PID_FILE"; then
    echo "  -> Frontend already running (PID=$(cat "$FRONTEND_PID_FILE"))"
else
    existing_pid="$(port_pid 5173)"
    if [ -n "$existing_pid" ]; then
        echo "$existing_pid" > "$FRONTEND_PID_FILE"
        echo "  -> Frontend already listening on :5173 (PID=$existing_pid)"
    else
        write_frontend_plist
        start_launch_agent "$FRONTEND_LABEL" "$FRONTEND_PLIST"
        frontend_pid="$(wait_for_port 5173 30)"
        echo "$frontend_pid" > "$FRONTEND_PID_FILE"
        echo "  -> Frontend PID=$frontend_pid"
    fi
fi

echo "==> QAgent detached"
echo "    Backend:  http://127.0.0.1:8000"
echo "    Frontend: http://localhost:5173"
echo "    Status:   scripts/status.sh"
echo "    Stop:     scripts/stop.sh"
