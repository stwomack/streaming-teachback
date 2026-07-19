#!/usr/bin/env bash
# Safety net: tear down any strays from a previous run — frontend, bridge, worker,
# and the Temporal dev server (only if we started it; a pre-existing server on
# 7233 is left alone). Normally you just press Ctrl-C in start-demo.sh instead.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

# temporal.pid only exists if THIS project started it; a reused server has none.
[[ -f "$RUN_DIR/temporal.pid" ]] \
  || log "Temporal server was pre-existing (not started by us) — leaving it running."

# Politely first (TERM the whole tree), then escalate to KILL for anything that
# ignored it, THEN remove the pidfiles. (stop_pidfile deletes the file on the
# first call, which would defeat a two-pass TERM-then-KILL — so do it directly.)
SERVICES=(frontend bridge worker temporal)

for svc in "${SERVICES[@]}"; do
  f="$RUN_DIR/$svc.pid"; pid_alive "$f" || continue
  kill_tree "$(cat "$f")" TERM
  log "sent SIGTERM to $svc tree (pid $(cat "$f"))"
done

sleep 1

for svc in "${SERVICES[@]}"; do
  f="$RUN_DIR/$svc.pid"
  if pid_alive "$f"; then
    kill_tree "$(cat "$f")" KILL
    log "escalated to SIGKILL for $svc tree (pid $(cat "$f"))"
  fi
  rm -f "$f"
done

log "Stopped. (Persisted bridge offsets kept in .bridge-offsets/; remove to reset.)"
