# streams-mechanics-demo

A deliberately plain demo of **Temporal Workflow Streams** for a technical
audience (SAs, engineers). No business storyline. A workflow asks Claude a plain
question over a **real streaming Anthropic API call**, and the demo stages the
distinct failure/resume scenarios so the audience sees the actual mechanics — not
a narrative.

> **Public Preview.** This demo uses `temporalio.contrib.workflow_streams`, which
> is in Public Preview. Every import of it carries a comment saying so. The API
> may change before GA.

---

## What you're looking at

A split-pane UI:

- **Left pane** — the live token feed as an *external subscriber* sees it:
  offset-addressed, with a clearly visible **RETRY boundary** when an
  activity-level retry occurs (the stale partial output is retracted), and a
  clean rehydration path on page load.
- **Right pane** — the actual Python (`AskWorkflow` / `ask_llm_streaming`),
  auto-selected and line-highlighted to follow whichever step is executing.

### Data flow

```
 browser ──SSE (offset as Last-Event-ID)──▶ FastAPI bridge ──subscribe()──▶ Workflow Stream
                                                                                    ▲
                                          ask_llm_streaming activity ──publish()────┘
                                          (real Anthropic streaming call)
```

The **workflow hosts the stream but never reads it.** The activity publishes
token deltas directly; only external clients (the bridge) subscribe. The workflow
only ever sees the activity's successful return value — which is exactly why it
stays consistent even when the stream carries a dead attempt's partial output.

---

## The two kinds of "retry", and why they look different on screen

This is the crux of the demo. There are **two tiers** of retry/dedup, and
conflating them is the usual source of confusion.

### Tier 1 — batch dedup *within a single publisher* (exactly-once to the log)

A `WorkflowStreamClient` batches publishes into Signals. If the SDK/network
retries a *flush* of a batch, the batch is deduplicated by
`(publisher_id, sequence)` so it lands in the workflow's event log **at most
once**. This is the "exactly-once at the execution layer" guarantee, and it is
scoped **narrowly**: it only covers one publisher instance's own retried flushes
(e.g. a transient signal-send failure the client retries internally).

### Tier 2 — Activity-level retry is a *new publisher* (NOT deduplicated)

When Temporal retries an **Activity** (e.g. the worker crashed), the new attempt
constructs a **new `WorkflowStreamClient` with a new `publisher_id`**. Its
sequence numbers restart at 0 and belong to a different dedup namespace, so:

- The dead attempt's already-published tokens **stay on the stream**, untouched.
- The new attempt **appends its own sequence on top**.
- **Nothing is retracted automatically.** The consumer must detect the retry
  boundary itself and reset its accumulated state.

The conventional pattern (which this demo implements): the retried attempt
publishes a `RETRY` marker with `force_flush=True`; the consumer clears the
prior-attempt output when it sees one. On screen, Tier 2 is the visible **RETRY
boundary** and the strike-through/retraction of stale tokens. Tier 1 you never
see — it just means you don't get duplicate tokens from one publisher's flush
retries.

> `max_retry_duration < publisher_ttl` (5 min < 15 min here). If a publisher's
> retry window outlived the dedup retention, a late-retried batch could land
> twice; keeping it under the TTL preserves Tier-1 exactly-once.

---

## Critical implementation rules (enforced in code)

1. **`WorkflowStream()` is constructed in `@workflow.init`, never `@workflow.run`.**
   The library inspects the immediate caller's frame name and requires
   `__init__`; anywhere else raises `RuntimeError`. See
   `backend/workflows/ask_workflow.py` and the test in
   `backend/tests/test_stream_init.py`.
2. **Continue-As-New state is typed `WorkflowStreamState | None`, never `Any`.**
   Under the default converter an `Any` field is silently rebuilt as a plain
   `dict`, which breaks the next run — silent data corruption, not a clean
   exception. See `AskInput.stream_state` in `backend/shared/models.py`.
3. **Dedup is scoped to one publisher instance** (Tier 1 above). Verified by
   `backend/tests/test_retry_dedup.py`.
4. **The workflow hosts the stream but does not subscribe to it.** Publish from
   the workflow and its activities; subscribe only from external clients.
5. **`max_retry_duration` on the publisher stays below the stream's
   `publisher_ttl`.**

---

## Setup

### Prerequisites

- Python 3.11+ and [`uv`](https://docs.astral.sh/uv/)
- Node 22.9+ (for the Next.js frontend)
- [`temporal` CLI](https://docs.temporal.io/cli) (for the dev server)
- An Anthropic API key

### Backend

```bash
cd backend
uv venv .venv
uv pip install --python .venv/bin/python -e .
uv pip install --python .venv/bin/python pytest pytest-asyncio   # for tests
```

### Environment

```bash
cp .env.example .env
# edit .env and set ANTHROPIC_API_KEY (model defaults to claude-sonnet-4-6)
```

### Frontend

`start-demo.sh` runs `npm install` automatically on the first run, so this is
optional. If you want to point the UI at a non-default bridge port, set it here:

```bash
cd frontend
cp .env.local.example .env.local   # NEXT_PUBLIC_BRIDGE_URL, defaults to :8000
```

---

## Running

One command starts **everything** — Temporal dev server (if needed), worker,
bridge, **and the UI** — and stays in the foreground:

```bash
./scripts/start-demo.sh
```

**Press `Ctrl-C` once to stop everything.** The worker, bridge, and UI (and the
Temporal dev server, only if this script started it) are all torn down together —
no separate stop step, so nothing gets left orphaned. A pre-existing Temporal
server on 7233 is reused and left running.

- UI: http://localhost:3000
- Temporal UI: http://localhost:8233
- Bridge health: http://localhost:8000/health

`stop-demo.sh` remains as a **safety net** to clean up strays from a previous run
(e.g. if the terminal was closed without Ctrl-C).

### Ports

Defaults: bridge `8000`, UI `3000`, Temporal `7233`/UI `8233`. `start-demo.sh`
**fails fast** if the bridge or UI port is already taken (rather than silently
colliding). To relocate a port:

- **Bridge:** set `BRIDGE_PORT` in `.env` *and* `NEXT_PUBLIC_BRIDGE_URL` in
  `frontend/.env.local` to match (e.g. `8077` / `http://127.0.0.1:8077`).
- **UI:** set `FRONTEND_PORT` in `.env`.

> The frontend needs Node ≥ 22.9 (npm 11 rejects 22.8.x). `start-demo.sh`
> auto-selects a suitable Node on your PATH (e.g. `/usr/local/bin/node`) if the
> default `node` is too old.

---

## The scenarios (talk track)

Type a plain question (e.g. *"explain how TCP handshakes work"*) and click **Ask**.
Each scenario is individually triggerable and visibly distinct.

### 1 — LLM API fails mid-stream → graceful fallback

**Trigger:** set `DEMO_FORCE_LLM_ERROR_AT_TOKEN=30` in `.env`, restart the worker,
ask a question.

**What to say:** "The activity has a hard timeout and a graceful fallback. When
the Anthropic call errors mid-response, the activity doesn't hang or crash the
workflow — it publishes a `fallback` event to the stream and returns cleanly. A
live demo can't stall on a flaky network call." On screen: tokens stream, then a
fallback message appears, then `complete`. **One** attempt — no retry, because the
activity handled the error itself.

### 2 — Activity-level retry (worker crash) → new publisher, RETRY boundary

**Trigger:** set `DEMO_PAUSE_AT_TOKEN=40` in `.env`, restart the worker, ask a
question. When it parks at token 40, run `./scripts/kill-and-restart.sh`.

**What to say:** "I'm SIGKILLing the worker mid-activity. Temporal detects the
dead worker via **heartbeat timeout** and retries the activity as a **new
attempt** — which builds a **new `publisher_id`**. This is Tier 2: the old
attempt's partial tokens are **not** deduplicated away, they stay on the stream.
The new attempt publishes a `RETRY` marker and its own sequence. The UI detects
that boundary, **retracts** the stale partial output, and shows the new attempt.
Watch the `publisher_id` in the left pane change across the boundary." This is the
correct, narrow scope of "not deduplicated," and it's labeled as such on screen.

### 3 — FastAPI bridge crash → resume from persisted offset

**Trigger:** while streaming, run `./scripts/kill-bridge.sh`.

**What to say:** "The bridge tracks the last offset it forwarded, per workflow, in
`.bridge-offsets/`. When it comes back it re-subscribes from that offset — not
from zero. And because the browser's `EventSource` reconnects with a
`Last-Event-ID` header, the bridge honors that too. Either way we resume where we
left off." The stream continues with no gap.

### 4 — Network blip (browser ↔ bridge) → EventSource auto-reconnect

**Trigger:** click **Arm network blip** in the UI, then keep watching.

**What to say:** "The bridge drops the SSE connection once. The browser's *native*
`EventSource` auto-reconnects and sends the `Last-Event-ID` header — the offset of
the last event it received. The bridge resumes from that offset. No gaps, no
duplicates, and **no reconnection code** beyond honoring the header." The
connection badge flips to `reconnecting` then back to `live`.

### 5 — Browser/client refresh → rehydrate, don't restart

**Trigger:** refresh the browser tab mid-stream.

**What to say:** "On load, the frontend first calls the bridge's REST `/history`
endpoint to fetch buffered events up to the current offset, renders them, then
attaches the live SSE tail from that offset forward. The **task does not
restart** — the view rehydrates." The same idempotent reducer replays history and
then the live tail, so state is reconstructed exactly.

> The summary calls these "four failure/resume scenarios"; scenarios 4 and 5 are
> two faces of the same offset-driven resume (one bridge-initiated, one
> client-initiated). All five are individually triggerable here.

---

## Tests

```bash
cd backend && .venv/bin/python -m pytest -q
```

- `test_stream_init.py` — asserts `RuntimeError` when `WorkflowStream()` is
  constructed outside `@workflow.init` (critical rule 1).
- `test_retry_dedup.py` — asserts that a new-`publisher_id` (activity-retry)
  publisher's events are **not** deduplicated against the prior attempt's
  (critical rule 3 / Tier 2).
- `test_e2e_stream.py` — full workflow → activity → stream → subscriber path with
  a faked Anthropic client (no key/network needed): asserts `start` → per-token
  `token` → `complete` arrive in order over the global offset.

---

## Demo-only scaffolding

`DEMO_PAUSE_AT_TOKEN`, `DEMO_PAUSE_SECONDS`, and `DEMO_FORCE_LLM_ERROR_AT_TOKEN`
exist **only** to stage scenarios reliably in front of an audience. They are
clearly commented as such in `activities/ask_llm.py` and are **not** production
patterns. Leave them unset for a normal run.
