#!/usr/bin/env bash
# Shared helpers for the demo scripts. Sourced, not executed directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
RUN_DIR="$ROOT/.run"
BACKEND="$ROOT/backend"
PYTHON="$BACKEND/.venv/bin/python"

mkdir -p "$RUN_DIR"

# Load .env if present so scripts see TEMPORAL_ADDRESS / BRIDGE_PORT / etc.
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-localhost:7233}"
BRIDGE_HOST="${BRIDGE_HOST:-127.0.0.1}"
BRIDGE_PORT="${BRIDGE_PORT:-8000}"
TEMPORAL_UI_PORT="${TEMPORAL_UI_PORT:-8233}"

log()  { printf '\033[36m▸ %s\033[0m\n' "$*"; }
warn() { printf '\033[33m! %s\033[0m\n' "$*"; }
err()  { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; }

require_python() {
  if [[ ! -x "$PYTHON" ]]; then
    err "Backend venv not found at $PYTHON"
    err "Create it:  cd backend && uv venv .venv && uv pip install --python .venv/bin/python -e ."
    exit 1
  fi
}

port_in_use() { lsof -ti ":$1" >/dev/null 2>&1; }

# Wait until a TCP port accepts connections (max ~20s).
wait_for_port() {
  local port="$1" name="$2" i=0
  while ! port_in_use "$port"; do
    ((i++)); [[ $i -gt 40 ]] && { err "$name did not come up on port $port"; return 1; }
    sleep 0.5
  done
}

pid_alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1")" 2>/dev/null; }

stop_pidfile() {
  local file="$1" sig="${2:-TERM}"
  if pid_alive "$file"; then
    local pid; pid="$(cat "$file")"
    kill "-$sig" "$pid" 2>/dev/null || true
    log "sent SIG$sig to $(basename "$file") (pid $pid)"
  fi
  rm -f "$file"
}

start_worker() {
  require_python
  log "starting worker"
  ( cd "$BACKEND" && exec "$PYTHON" worker.py ) >"$RUN_DIR/worker.log" 2>&1 &
  echo $! >"$RUN_DIR/worker.pid"
  log "worker pid $(cat "$RUN_DIR/worker.pid") · logs: .run/worker.log"
}

start_bridge() {
  require_python
  log "starting FastAPI bridge on $BRIDGE_HOST:$BRIDGE_PORT"
  ( cd "$BACKEND" && exec "$PYTHON" bridge.py ) >"$RUN_DIR/bridge.log" 2>&1 &
  echo $! >"$RUN_DIR/bridge.pid"
  wait_for_port "$BRIDGE_PORT" "bridge" || true
  log "bridge pid $(cat "$RUN_DIR/bridge.pid") · logs: .run/bridge.log"
}
