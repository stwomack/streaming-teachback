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

# Recursively kill a pid and all of its descendants (npm/next fork extra
# processes, so killing just the parent would orphan them).
kill_tree() {
  local pid="$1" sig="${2:-TERM}" child
  for child in $(pgrep -P "$pid" 2>/dev/null); do
    kill_tree "$child" "$sig"
  done
  kill "-$sig" "$pid" 2>/dev/null || true
}

stop_pidfile() {
  local file="$1" sig="${2:-TERM}"
  if pid_alive "$file"; then
    local pid; pid="$(cat "$file")"
    kill_tree "$pid" "$sig"
    log "sent SIG$sig to $(basename "$file") tree (pid $pid)"
  fi
  rm -f "$file"
}

# Resolve a directory containing a Node >= 22.9 (npm 11 rejects 22.8.x). Prints
# the bin dir to prepend to PATH, or empty if the default node already qualifies
# / nothing suitable is found.
resolve_node_dir() {
  local cand ver major minor
  for cand in "$(command -v node 2>/dev/null)" /usr/local/bin/node /opt/homebrew/bin/node; do
    [[ -x "$cand" ]] || continue
    ver="$("$cand" -v 2>/dev/null | sed 's/^v//')"
    major="${ver%%.*}"; minor="${ver#*.}"; minor="${minor%%.*}"
    [[ "$major" =~ ^[0-9]+$ && "$minor" =~ ^[0-9]+$ ]] || continue
    if (( major > 22 )) || { (( major == 22 )) && (( minor >= 9 )); }; then
      dirname "$cand"; return 0
    fi
  done
  return 0
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

FRONTEND="$ROOT/frontend"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"

start_frontend() {
  local node_dir path_prefix=""
  node_dir="$(resolve_node_dir)"
  if [[ -n "$node_dir" ]]; then path_prefix="$node_dir:"; fi

  if [[ ! -d "$FRONTEND/node_modules" ]]; then
    log "installing frontend dependencies (first run)…"
    ( cd "$FRONTEND" && PATH="${path_prefix}$PATH" npm install --no-audit --no-fund ) \
      >"$RUN_DIR/frontend-install.log" 2>&1 \
      || { err "npm install failed — see .run/frontend-install.log"; return 1; }
  fi

  log "starting frontend (Next.js) on http://localhost:$FRONTEND_PORT"
  ( cd "$FRONTEND" && exec env PATH="${path_prefix}$PATH" npm run dev -- -p "$FRONTEND_PORT" ) \
    >"$RUN_DIR/frontend.log" 2>&1 &
  echo $! >"$RUN_DIR/frontend.pid"
  log "frontend pid $(cat "$RUN_DIR/frontend.pid") · logs: .run/frontend.log"
}
