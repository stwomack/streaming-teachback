#!/usr/bin/env bash
# Scenario 3: kill the FastAPI bridge mid-stream, then restart it.
#
# The bridge persists the last offset it forwarded (per workflow) under
# .bridge-offsets/. On restart it resumes subscribing from that offset — not from
# zero. Meanwhile the browser's EventSource reconnects with Last-Event-ID, which
# the bridge honors, so the tail resumes with no gaps and no duplicates.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

if pid_alive "$RUN_DIR/bridge.pid"; then
  BPID="$(cat "$RUN_DIR/bridge.pid")"
  log "SIGKILL bridge pid $BPID (simulating a crash)"
  kill -9 "$BPID" 2>/dev/null || true
  rm -f "$RUN_DIR/bridge.pid"
else
  warn "No live bridge recorded."
fi

sleep 1
start_bridge
log "Bridge restarted. It resumes from the last persisted offset in .bridge-offsets/."
