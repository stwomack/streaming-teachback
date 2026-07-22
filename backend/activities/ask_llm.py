"""The streaming LLM activity.

This activity makes a *real* streaming Anthropic API call and publishes each token
delta onto the host Workflow's stream as it arrives (not buffered until return).
It is the sole producer the browser actually watches.

Two demo-only pacing/chaos hooks live here (clearly marked): a pause point to give
the presenter a reliable window to kill the worker, and a forced-error point to
exercise the graceful fallback path. Neither is a production pattern.
"""

from __future__ import annotations

import asyncio
import os

import anthropic
from temporalio import activity

# NOTE: Public Preview API — `temporalio.contrib.workflow_streams`.
from temporalio.contrib.workflow_streams import WorkflowStreamClient

from shared import config
from shared.models import (
    EVENTS_TOPIC,
    KIND_COMPLETE,
    KIND_FALLBACK,
    KIND_RETRY,
    KIND_START,
    KIND_TOKEN,
    AskLLMInput,
    StreamEvent,
)

# How long we let the model stream before we stop waiting and degrade gracefully.
# This is the *soft* budget enforced inside the activity so a flaky/slow network
# call can never stall a live demo. The *hard* ceiling is the activity's
# start_to_close_timeout, set by the workflow.
LLM_SOFT_TIMEOUT_SECONDS = float(os.getenv("LLM_SOFT_TIMEOUT_SECONDS", "45"))

# 200ms is the documented sweet spot for LLM token streams: responsive to the eye
# without one-Signal-per-character RPC overhead.
BATCH_INTERVAL = __import__("datetime").timedelta(milliseconds=200)

# CRITICAL RULE 5: keep the publisher's max_retry_duration BELOW the stream's
# publisher_ttl (default 15 min). Otherwise a batch retried past the dedup
# retention window is treated as a fresh publish and can land twice. 5 min < 15.
MAX_RETRY_DURATION = __import__("datetime").timedelta(minutes=5)


class _SimulatedLLMError(RuntimeError):
    """Raised by the DEMO_FORCE_LLM_ERROR_AT_TOKEN hook. Demo-only."""


@activity.defn
async def ask_llm_streaming(input: AskLLMInput) -> str:
    """Stream a plain-question completion to the Workflow Stream, token by token.

    Returns the fully accumulated answer text (or the fallback text). The workflow
    only ever sees this return value — it never reads the stream itself.
    """
    info = activity.info()
    attempt = info.attempt  # 1-based; rises on an activity-level retry.

    # `from_within_activity` infers the Temporal client and the *host workflow id*
    # from the activity context, so events land on the workflow that scheduled us.
    # Each attempt builds a NEW client here → a NEW publisher_id. That is exactly
    # why the prior attempt's already-published tokens are NOT deduplicated away:
    # dedup is scoped to a single publisher instance's own retried flushes.
    client = WorkflowStreamClient.from_within_activity(
        batch_interval=BATCH_INTERVAL,
        max_retry_duration=MAX_RETRY_DURATION,
    )
    # `_publisher_id` is an internal detail; surfaced here purely so the UI can
    # show the id changing across the retry boundary. Not something app code
    # should normally depend on.
    publisher_id = getattr(client, "_publisher_id", "")

    accumulated: list[str] = []

    async with client:
        events = client.topic(EVENTS_TOPIC, type=StreamEvent)

        def emit(evt: StreamEvent, *, flush: bool = False) -> None:
            evt.attempt = attempt
            evt.publisher_id = publisher_id
            events.publish(evt, force_flush=flush)

        # ── Retry boundary marker ────────────────────────────────────────────
        # On any attempt after the first, announce the retry FIRST (force_flush
        # so the UI reacts immediately). The consumer treats this as a hard
        # reset: retract/annotate the stale partial output from the dead attempt.
        if attempt > 1:
            emit(
                StreamEvent(
                    kind=KIND_RETRY,
                    detail=(
                        f"Activity retry: attempt {attempt} started with a new "
                        f"publisher_id. The previous attempt's partial tokens are "
                        f"stale and are being retracted."
                    ),
                ),
                flush=True,
            )

        emit(
            StreamEvent(
                kind=KIND_START,
                detail=f"attempt {attempt} · publisher {publisher_id}",
            ),
            flush=True,
        )

        try:
            text = await _stream_completion(input.question, attempt, emit)
            accumulated.append(text)
            emit(StreamEvent(kind=KIND_COMPLETE, detail="response complete"), flush=True)
        except _SimulatedLLMError as e:
            # Scenario 1: forced mid-stream API failure. Degrade gracefully — do
            # NOT crash the workflow and do NOT trigger an activity retry.
            fallback = _fallback_text(str(e))
            accumulated.append(fallback)
            emit(StreamEvent(kind=KIND_FALLBACK, text=fallback, detail=str(e)), flush=True)
            emit(StreamEvent(kind=KIND_COMPLETE, detail="completed via fallback"), flush=True)
        except (anthropic.APIError, asyncio.TimeoutError) as e:
            # Real slow/errored network call → same graceful degradation, so a
            # live demo can never hang on a flaky Anthropic call.
            reason = f"{type(e).__name__}: {e}"
            fallback = _fallback_text(reason)
            accumulated.append(fallback)
            emit(StreamEvent(kind=KIND_FALLBACK, text=fallback, detail=reason), flush=True)
            emit(StreamEvent(kind=KIND_COMPLETE, detail="completed via fallback"), flush=True)

        # Ensure everything buffered is on the server before we return. Exiting
        # the `async with` also flushes, but an explicit barrier makes the intent
        # obvious.
        await client.flush()

    return "".join(accumulated)


async def _stream_completion(question: str, attempt: int, emit) -> str:
    """Open a streaming Anthropic response and publish each text delta.

    A stalled/slow connection degrades rather than hangs: the Anthropic client's
    per-request `timeout` raises `APITimeoutError` (an `anthropic.APIError`
    subclass), which the caller turns into a graceful fallback event.
    """
    # max_retries=0: Temporal owns retries, not the SDK client (see
    # ai-patterns.md). The per-request timeout is the soft budget that keeps a
    # flaky call from stalling the demo.
    llm = anthropic.AsyncAnthropic(
        api_key=os.getenv("ANTHROPIC_API_KEY"),
        max_retries=0,
        timeout=LLM_SOFT_TIMEOUT_SECONDS,
    )

    token_count = 0
    pieces: list[str] = []

    async with llm.messages.stream(
        model=config.ANTHROPIC_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": question}],
    ) as stream:
        async for text in stream.text_stream:
            token_count += 1
            pieces.append(text)

            emit(StreamEvent(kind=KIND_TOKEN, seq=token_count, text=text))
            # Heartbeat every token so a dead worker is detected within the
            # heartbeat_timeout window (drives the retry scenario).
            activity.heartbeat({"attempt": attempt, "tokens": token_count})

            await _maybe_force_error(token_count)
            await _maybe_pause(token_count, attempt)

        # Why did the stream end? `end_turn` = the model finished on its own;
        # `max_tokens` = it hit the `max_tokens` ceiling and was cut off. Logged
        # so a too-short answer can be diagnosed (raise max_tokens vs. push the
        # prompt) without guessing.
        final = await stream.get_final_message()
        activity.logger.info(
            "LLM stream ended: stop_reason=%s tokens=%s output_tokens=%s",
            final.stop_reason,
            token_count,
            final.usage.output_tokens,
        )

    return "".join(pieces)


async def _maybe_force_error(token_count: int) -> None:
    """DEMO-ONLY (scenario 1): raise a simulated API error at a fixed token."""
    if (
        config.DEMO_FORCE_LLM_ERROR_AT_TOKEN is not None
        and token_count == config.DEMO_FORCE_LLM_ERROR_AT_TOKEN
    ):
        raise _SimulatedLLMError(
            f"DEMO_FORCE_LLM_ERROR_AT_TOKEN={config.DEMO_FORCE_LLM_ERROR_AT_TOKEN}: "
            f"simulated Anthropic API failure mid-stream"
        )


async def _maybe_pause(token_count: int, attempt: int) -> None:
    """DEMO-ONLY (scenario 2): park on a heartbeating sleep at a fixed token.

    This gives the presenter a wide, reliable window to SIGKILL the worker at a
    known point instead of eyeballing live LLM latency. NOT a production pattern.
    Only pauses on the first attempt so the retried attempt streams through.
    """
    if (
        config.DEMO_PAUSE_AT_TOKEN is None
        or token_count != config.DEMO_PAUSE_AT_TOKEN
        or attempt > 1
    ):
        return

    activity.logger.warning(
        "DEMO PAUSE: parked at token %s for up to %ss — kill the worker now",
        token_count,
        config.DEMO_PAUSE_SECONDS,
    )
    waited = 0.0
    while waited < config.DEMO_PAUSE_SECONDS:
        # Keep heartbeating so Temporal does NOT time us out during the pause;
        # the retry must be triggered by an actual SIGKILL, not by us going quiet.
        activity.heartbeat({"attempt": attempt, "tokens": token_count, "paused": True})
        await asyncio.sleep(1.0)
        waited += 1.0


def _fallback_text(reason: str) -> str:
    return (
        "\n\n[graceful fallback] The language model was slow or returned an error, "
        "so the workflow degraded instead of stalling. "
        f"(reason: {reason})"
    )
