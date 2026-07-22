"""FastAPI bridge: subscribes to a Workflow Stream and relays it to the browser.

Responsibilities, one per demo scenario:

* POST /ask                  — start an AskWorkflow for a plain question.
* GET  /history/{wf}         — REST: buffered events [from_offset, head). Used on
                               page load (scenario 5: refresh → rehydrate).
* GET  /stream/{wf}          — SSE tail. Honors Last-Event-ID so the browser's
                               native EventSource resumes with no gaps/dupes
                               (scenario 4: network blip). On (re)start it resumes
                               from the last persisted offset (scenario 3: bridge
                               crash), not from zero.
* GET  /offset/{wf}          — current head offset + last offset this bridge
                               persisted.
* POST /chaos/drop-next-sse  — arm a one-time SSE disconnect (scenario 4).
* GET  /source               — the real workflow/activity source, for the UI's
                               right pane.

The bridge is the ONLY subscriber. The workflow hosts the stream and never reads
it (critical rule 4).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from temporalio.client import Client

# NOTE: Public Preview API — `temporalio.contrib.workflow_streams`.
from temporalio.contrib.workflow_streams import WorkflowStreamClient

from shared import config
from shared.models import EVENTS_TOPIC, StreamEvent

# Persisted offset store. Survives a bridge crash so a restart resumes from the
# last offset it forwarded rather than replaying from zero (scenario 3).
OFFSET_DIR = Path(os.getenv("BRIDGE_OFFSET_DIR", ".bridge-offsets"))

app = FastAPI(title="streams-mechanics-bridge")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # demo only; a real deployment would scope this.
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Set once at startup.
_client: Client | None = None

# Scenario 4: when armed, the next SSE connection drops after forwarding
# CHAOS_DROP_AFTER events, simulating a single network interruption partway
# through the stream. The browser auto-reconnects.
_chaos_drop_armed = False

# How many events to forward on the armed connection before dropping it. Dropping
# a few events in (rather than on the first) makes the blip land visibly *inside*
# the answer during a demo instead of at the opening `start` event.
CHAOS_DROP_AFTER = 8


def temporal() -> Client:
    assert _client is not None, "Temporal client not initialized"
    return _client


@app.on_event("startup")
async def _startup() -> None:
    global _client
    OFFSET_DIR.mkdir(parents=True, exist_ok=True)
    _client = await Client.connect(
        config.TEMPORAL_ADDRESS,
        namespace=config.TEMPORAL_NAMESPACE,
    )


# ── offset persistence ────────────────────────────────────────────────────────
def _offset_file(workflow_id: str) -> Path:
    safe = workflow_id.replace("/", "_")
    return OFFSET_DIR / f"{safe}.offset"


def _read_persisted_offset(workflow_id: str) -> int | None:
    p = _offset_file(workflow_id)
    if not p.exists():
        return None
    try:
        return int(p.read_text().strip())
    except (ValueError, OSError):
        return None


def _persist_offset(workflow_id: str, offset: int) -> None:
    try:
        _offset_file(workflow_id).write_text(str(offset))
    except OSError:
        pass


# ── models ──────────────────────────────────────────────────────────────────
class AskBody(BaseModel):
    question: str


def _event_payload(offset: int, event: Any) -> dict:
    """Normalize a subscribed item's data into a JSON-able dict with its offset.

    The stock JSON converter decodes our dataclass back into a plain dict, so we
    accept either a StreamEvent or a dict.
    """
    data = asdict(event) if isinstance(event, StreamEvent) else dict(event)
    data["offset"] = offset
    return data


# ── endpoints ─────────────────────────────────────────────────────────────────
@app.post("/ask")
async def ask(body: AskBody) -> JSONResponse:
    workflow_id = f"ask-{uuid.uuid4().hex[:12]}"
    from workflows.ask_workflow import AskWorkflow  # local import; workflow module
    from shared.models import AskInput

    await temporal().start_workflow(
        AskWorkflow.run,
        AskInput(question=body.question),
        id=workflow_id,
        task_queue=config.TASK_QUEUE,
    )
    return JSONResponse({"workflow_id": workflow_id, "question": body.question})


@app.get("/offset/{workflow_id}")
async def offset(workflow_id: str) -> JSONResponse:
    sc = WorkflowStreamClient.create(temporal(), workflow_id=workflow_id)
    head = await sc.get_offset()
    return JSONResponse(
        {
            "workflow_id": workflow_id,
            "head": head,
            "last_persisted": _read_persisted_offset(workflow_id),
        }
    )


@app.get("/history/{workflow_id}")
async def history(workflow_id: str, from_offset: int = 0) -> JSONResponse:
    """Buffered events in [from_offset, head). Powers page-load rehydration."""
    sc = WorkflowStreamClient.create(temporal(), workflow_id=workflow_id)
    head = await sc.get_offset()  # head == count; valid item offsets are [0, head)
    events: list[dict] = []
    if from_offset < head:
        topic = sc.topic(EVENTS_TOPIC, type=StreamEvent)
        # subscribe(from_offset=...) is INCLUSIVE. Collect until we've reached the
        # last item that existed at the moment we read `head`, then stop; the live
        # SSE tail picks up anything published after.
        agen = topic.subscribe(from_offset=from_offset)
        async with contextlib.aclosing(agen):
            async for item in agen:
                events.append(_event_payload(item.offset, item.data))
                if item.offset >= head - 1:
                    break
    return JSONResponse({"workflow_id": workflow_id, "head": head, "events": events})


@app.post("/chaos/drop-next-sse")
async def arm_chaos() -> JSONResponse:
    """Scenario 4: arm a single SSE disconnect on the next connection."""
    global _chaos_drop_armed
    _chaos_drop_armed = True
    return JSONResponse({"armed": True, "detail": "next SSE connection drops once"})


@app.get("/stream/{workflow_id}")
async def stream(workflow_id: str, request: Request, from_offset: int = -1):
    """SSE tail of the workflow stream.

    Resume precedence (all offsets normalized to an INCLUSIVE start):
      1. Last-Event-ID header  -> resume at last_delivered + 1  (EventSource
         reconnect; scenarios 4 & 5).
      2. ?from_offset=N (N>=0) -> start at N                    (explicit).
      3. persisted offset + 1  -> resume after a bridge restart (scenario 3).
      4. 0                     -> from the beginning.

    `from_offset` defaults to -1 (the sentinel for "not supplied") so that an
    explicit ?from_offset=0 is honored rather than being mistaken for "unset" and
    falling through to the persisted-offset branch.
    """
    last_event_id = request.headers.get("last-event-id")
    if last_event_id is not None and last_event_id.strip().isdigit():
        start = int(last_event_id) + 1
    elif from_offset >= 0:
        start = from_offset
    else:
        persisted = _read_persisted_offset(workflow_id)
        start = (persisted + 1) if persisted is not None else 0

    sc = WorkflowStreamClient.create(temporal(), workflow_id=workflow_id)
    topic = sc.topic(EVENTS_TOPIC, type=StreamEvent)

    async def gen():
        global _chaos_drop_armed
        drop_this_connection = _chaos_drop_armed
        if drop_this_connection:
            _chaos_drop_armed = False  # one-shot: disarm immediately.

        # NOTE: do NOT call request.is_disconnected() here. sse-starlette owns the
        # ASGI receive channel to detect client disconnects and cancels this
        # generator itself; a second consumer of receive() races it and can drop
        # events spuriously. Client disconnect surfaces as GeneratorExit /
        # CancelledError, which contextlib.aclosing propagates into subscribe().
        forwarded = 0
        agen = topic.subscribe(from_offset=start)
        async with contextlib.aclosing(agen):
            async for item in agen:
                payload = _event_payload(item.offset, item.data)
                # `id:` == global offset → the browser sends it back as
                # Last-Event-ID on reconnect, and we honor it above. No dedup
                # code needed anywhere in the app.
                yield {
                    "id": str(item.offset),
                    "event": "message",
                    "data": json.dumps(payload),
                }
                _persist_offset(workflow_id, item.offset)
                forwarded += 1

                if drop_this_connection and forwarded >= CHAOS_DROP_AFTER:
                    # Scenario 4: simulate a network blip by closing a few events
                    # into the stream. EventSource reconnects automatically and
                    # resumes from Last-Event-ID — no gaps, no duplicates.
                    break

    return EventSourceResponse(gen())


@app.get("/source")
async def source() -> JSONResponse:
    """Return the real workflow + activity source for the UI's right pane."""
    here = Path(__file__).parent
    files = {
        "workflow": here / "workflows" / "ask_workflow.py",
        "activity": here / "activities" / "ask_llm.py",
    }
    out = {}
    for key, path in files.items():
        try:
            out[key] = {"path": str(path.name), "code": path.read_text()}
        except OSError:
            out[key] = {"path": str(path.name), "code": ""}
    return JSONResponse(out)


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"ok": True, "task_queue": config.TASK_QUEUE})


if __name__ == "__main__":
    uvicorn.run(
        "bridge:app",
        host=config.BRIDGE_HOST,
        port=config.BRIDGE_PORT,
        log_level="info",
    )
