#!/usr/bin/env bash
# Start the Temporal dev server (if needed), the worker, and the FastAPI bridge.
# The frontend is started separately: cd frontend && npm run dev
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

if [[ ! -f "$ROOT/.env" ]]; then
  warn "No .env found. Copy .env.example -> .env and set ANTHROPIC_API_KEY."
  warn "Without a valid key the activity will exercise its graceful-fallback path."
fi

# ── Temporal dev server ───────────────────────────────────────────────────────
TEMPORAL_PORT="${TEMPORAL_ADDRESS##*:}"
if port_in_use "$TEMPORAL_PORT"; then
  log "Temporal already listening on $TEMPORAL_ADDRESS — reusing it"
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
  wait_for_port "$TEMPORAL_PORT" "temporal"
  log "temporal pid $(cat "$RUN_DIR/temporal.pid") · logs: .run/temporal.log"
fi

start_worker
start_bridge

echo
log "Demo backend up."
echo "   Temporal UI : http://localhost:$TEMPORAL_UI_PORT"
echo "   Bridge      : http://$BRIDGE_HOST:$BRIDGE_PORT  (health: /health)"
echo "   Frontend    : cd frontend && npm run dev   ->  http://localhost:3000"
echo
echo "   Scenario 1 (LLM error) : set DEMO_FORCE_LLM_ERROR_AT_TOKEN in .env, restart worker"
echo "   Scenario 2 (worker crash): set DEMO_PAUSE_AT_TOKEN in .env, then ./scripts/kill-and-restart.sh"
echo "   Scenario 3 (bridge crash): ./scripts/kill-bridge.sh"
echo "   Scenario 4 (network blip): click 'Arm network blip' in the UI"
echo "   Scenario 5 (refresh)     : refresh the browser tab mid-stream"
