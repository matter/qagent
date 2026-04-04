#!/usr/bin/env bash
# Start QAgent backend and frontend in dev mode.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "==> Starting QAgent (dev mode)"

# Backend
echo "  -> Starting backend (uvicorn) on :8000 ..."
cd "$PROJECT_ROOT"
uv run uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload &
BACKEND_PID=$!
echo "     PID=$BACKEND_PID"

# Frontend
echo "  -> Starting frontend (vite) on :5173 ..."
cd "$PROJECT_ROOT/frontend"
pnpm dev &
FRONTEND_PID=$!
echo "     PID=$FRONTEND_PID"

# Write PID file so stop.sh can find them
echo "$BACKEND_PID" > "$PROJECT_ROOT/.backend.pid"
echo "$FRONTEND_PID" > "$PROJECT_ROOT/.frontend.pid"

echo ""
echo "==> QAgent running"
echo "    Backend:  http://127.0.0.1:8000"
echo "    Frontend: http://localhost:5173"
echo "    Stop with: scripts/stop.sh"
echo ""

# Wait for both – Ctrl-C kills this script and the children
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit 0" SIGINT SIGTERM
wait
