#!/usr/bin/env bash
# Start the WHOLE demo in the foreground: Temporal dev server (if needed), worker,
# FastAPI bridge, and the Next.js UI. Press Ctrl-C once to tear everything down —
# no separate ./stop-demo.sh needed. (stop-demo.sh remains as a safety net for
# cleaning up a previous run's strays.)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

STARTED_TEMPORAL=0

cleanup() {
  [[ -n "${_CLEANED:-}" ]] && return
  _CLEANED=1
  echo
  log "shutting down demo…"
  local svc f
  # Politely first…
  for svc in frontend bridge worker temporal; do
    f="$RUN_DIR/$svc.pid"; [[ -f "$f" ]] || continue
    kill_tree "$(cat "$f")" TERM
  done
  sleep 1
  # …then make sure nothing (or any forked grandchild) is left behind.
  for svc in frontend bridge worker temporal; do
    f="$RUN_DIR/$svc.pid"; [[ -f "$f" ]] || continue
    kill_tree "$(cat "$f")" KILL
    rm -f "$f"
  done
  log "all demo processes stopped."
}
# EXIT covers normal/`set -e` exits; INT/TERM cover Ctrl-C and `kill`.
trap cleanup EXIT
trap 'cleanup; exit 0' INT TERM

if [[ ! -f "$ROOT/.env" ]]; then
  warn "No .env found. Copy .env.example -> .env and set ANTHROPIC_API_KEY"
  warn "(or export it in this shell). Without a valid key the activity uses its"
  warn "graceful-fallback path."
fi

# ── Pre-flight: bail early on a port already taken by something that isn't ours ─
if port_in_use "$BRIDGE_PORT"; then
  err "Port $BRIDGE_PORT (bridge) is already in use by another process."
  err "Set BRIDGE_PORT in .env (and NEXT_PUBLIC_BRIDGE_URL in frontend/.env.local) to a free port,"
  err "or stop the process holding it (e.g. a leftover run: ./scripts/stop-demo.sh)."
  exit 1
fi
if port_in_use "$FRONTEND_PORT"; then
  err "Port $FRONTEND_PORT (frontend) is already in use — likely a leftover 'npm run dev'."
  err "Stop it (./scripts/stop-demo.sh, or kill it), or set FRONTEND_PORT to a free port."
  exit 1
fi

# ── Temporal dev server ───────────────────────────────────────────────────────
TEMPORAL_PORT="${TEMPORAL_ADDRESS##*:}"
if port_in_use "$TEMPORAL_PORT"; then
  log "Temporal already listening on $TEMPORAL_ADDRESS — reusing it (won't stop it)"
else
  if ! command -v temporal >/dev/null 2>&1; then
    err "temporal CLI not found. Install it: https://docs.temporal.io/cli"
    exit 1
  fi
  log "starting temporal dev server (UI on http://localhost:$TEMPORAL_UI_PORT)"
  ( exec temporal server start-dev \
      --port "$TEMPORAL_PORT" \
      --ui-port "$TEMPORAL_UI_PORT" \
      --db-filename "$RUN_DIR/temporal.db" \
  ) >"$RUN_DIR/temporal.log" 2>&1 &
  echo $! >"$RUN_DIR/temporal.pid"
  STARTED_TEMPORAL=1
  wait_for_port "$TEMPORAL_PORT" "temporal"
  log "temporal pid $(cat "$RUN_DIR/temporal.pid") · logs: .run/temporal.log"
fi

start_worker
start_bridge
start_frontend

echo
log "Demo is up. Everything runs in THIS terminal."
echo "   UI          : http://localhost:$FRONTEND_PORT"
echo "   Temporal UI : http://localhost:$TEMPORAL_UI_PORT"
echo "   Bridge      : http://$BRIDGE_HOST:$BRIDGE_PORT  (health: /health)"
echo
echo "   Scenario 1 (LLM error) : set DEMO_FORCE_LLM_ERROR_AT_TOKEN in .env, restart"
echo "   Scenario 2 (worker crash): set DEMO_PAUSE_AT_TOKEN in .env; ./scripts/kill-and-restart.sh"
echo "   Scenario 3 (bridge crash): ./scripts/kill-bridge.sh"
echo "   Scenario 4 (network blip): click 'Arm network blip' in the UI"
echo "   Scenario 5 (refresh)     : refresh the browser tab mid-stream"
echo
log "Press Ctrl-C to stop EVERYTHING (worker, bridge, UI$([[ $STARTED_TEMPORAL == 1 ]] && echo ', temporal'))."

# Stay in the foreground until interrupted; then the trap tears it all down.
wait
