#!/usr/bin/env bash
# Scenario 2: SIGKILL the worker mid-activity, then bring it back.
#
# With DEMO_PAUSE_AT_TOKEN set, the activity parks at a known token so you can run
# this at a reliable point. Temporal detects the dead worker via heartbeat
# timeout and retries the activity as a NEW attempt with a NEW publisher_id. The
# dead attempt's partial tokens stay on the stream; the retry appends its own
# sequence; the UI shows a RETRY boundary and retracts the stale output.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

if ! pid_alive "$RUN_DIR/worker.pid"; then
  warn "No live worker recorded. Starting a fresh one."
  start_worker
  exit 0
fi

WPID="$(cat "$RUN_DIR/worker.pid")"
log "SIGKILL worker pid $WPID (simulating a hard crash)"
kill -9 "$WPID" 2>/dev/null || true
rm -f "$RUN_DIR/worker.pid"

# Restart quickly so Temporal can reschedule the retried attempt onto it. The
# heartbeat_timeout (6s) governs how fast the retry fires.
sleep 1
start_worker
log "Worker restarted. Watch the UI for the RETRY boundary as the new attempt streams."
