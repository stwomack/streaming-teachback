#!/usr/bin/env bash
# Safety net: tear down any strays from a previous run — frontend, bridge, worker,
# and the Temporal dev server (only if we started it; a pre-existing server on
# 7233 is left alone). Normally you just press Ctrl-C in start-demo.sh instead.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

stop_pidfile "$RUN_DIR/frontend.pid" TERM
stop_pidfile "$RUN_DIR/bridge.pid" TERM
stop_pidfile "$RUN_DIR/worker.pid" TERM

if [[ -f "$RUN_DIR/temporal.pid" ]]; then
  stop_pidfile "$RUN_DIR/temporal.pid" TERM
else
  log "Temporal server was pre-existing (not started by us) — leaving it running."
fi

# Give processes a moment, then hard-kill any stragglers we own.
sleep 1
for f in frontend bridge worker temporal; do
  [[ -f "$RUN_DIR/$f.pid" ]] && stop_pidfile "$RUN_DIR/$f.pid" KILL
done

log "Stopped. (Persisted bridge offsets kept in .bridge-offsets/; remove to reset.)"
