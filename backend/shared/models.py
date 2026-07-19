"""Shared data models for the Workflow Streams mechanics demo.

These dataclasses are plain data carried three places:

1. As Workflow / Activity input (serialized by Temporal's data converter).
2. As the payload of events published onto the Workflow Stream.
3. Forwarded verbatim (as JSON) by the FastAPI bridge over SSE to the browser.

Keeping them as dataclasses means the stock JSON payload converter handles them
in every hop without extra configuration.
"""

from __future__ import annotations

from dataclasses import dataclass

# NOTE: Public Preview API. `temporalio.contrib.workflow_streams` is a contrib
# module and its surface may change before GA.
from temporalio.contrib.workflow_streams import WorkflowStreamState

# The single topic every event flows through. Token deltas AND control events
# (retry markers, fallback, start/complete) share ONE topic + ONE publisher per
# attempt on purpose: Workflow Streams only guarantees ordering *within a single
# publisher*, and only when events share a topic. A RETRY marker is meaningless
# to the consumer unless it is strictly ordered relative to the tokens it
# retracts, so it must ride the same topic as the tokens.
EVENTS_TOPIC = "events"


# ── Event kinds (the `kind` discriminant on StreamEvent) ──────────────────────
KIND_START = "start"        # a fresh activity attempt began publishing
KIND_TOKEN = "token"        # one streamed token delta from the LLM
KIND_RETRY = "retry"        # activity-level retry boundary — consumer must reset
KIND_FALLBACK = "fallback"  # graceful degradation: LLM slow/errored, not a crash
KIND_COMPLETE = "complete"  # the response finished normally
KIND_ERROR = "error"        # a terminal, non-recoverable error was surfaced


@dataclass
class StreamEvent:
    """One event on the stream.

    A discriminated union keyed on `kind`. The consumer (bridge + browser) builds
    an idempotent reducer over these: append on `token`, reset the accumulator on
    `retry`/`start`, overwrite/annotate on terminal kinds.
    """

    kind: str
    # Token index *within the current attempt*. Resets to 0 on each new attempt,
    # which is exactly why the consumer cannot rely on it for global ordering —
    # only the stream's global `offset` (attached by the bridge) is monotonic.
    seq: int = 0
    text: str = ""
    # 1-based Temporal activity attempt number. Rises on an activity-level retry.
    attempt: int = 1
    # The WorkflowStreamClient publisher id for the attempt that produced this
    # event. A *new* attempt constructs a *new* client → new publisher_id, which
    # is precisely why cross-attempt dedup does NOT happen. Surfaced so the UI can
    # show the id changing across the retry boundary.
    publisher_id: str = ""
    detail: str = ""


@dataclass
class AskInput:
    """Input to AskWorkflow.

    `stream_state` is threaded through Continue-As-New. CRITICAL: it is typed
    `WorkflowStreamState | None`, never `Any`. Under the default data converter an
    `Any` field is silently rebuilt as a plain dict, and `WorkflowStream(
    prior_state=<dict>)` then breaks on attribute access — silent data corruption,
    not a clean exception. This demo is short-lived (a single completion) so CAN
    never actually fires, but the field is typed correctly to model the rule.
    """

    question: str
    stream_state: WorkflowStreamState | None = None


@dataclass
class AskLLMInput:
    """Input to the streaming activity."""

    question: str
    # Threaded explicitly so the activity can address the host workflow's stream.
    workflow_id: str
