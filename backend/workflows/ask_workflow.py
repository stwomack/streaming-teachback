"""AskWorkflow — hosts the stream, orchestrates the streaming activity.

Deliberately plain: it takes a question string and runs one streaming completion.
No business framing. The interesting mechanics live in how the stream survives
failure, not in the workflow's control flow.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# NOTE: Public Preview API — `temporalio.contrib.workflow_streams`.
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from activities.ask_llm import ask_llm_streaming
    from shared.models import AskInput, AskLLMInput


@workflow.defn
class AskWorkflow:
    @workflow.init
    def __init__(self, input: AskInput) -> None:
        # ── CRITICAL RULE 1 ──────────────────────────────────────────────────
        # WorkflowStream() MUST be constructed here, in __init__. The library
        # inspects the immediate caller's frame and requires it to be named
        # `__init__`. Constructing it from @workflow.run, a helper, or a handler
        # raises RuntimeError — the publish/poll/offset handlers have to be
        # registered before the first publish Signal can arrive.
        #
        # `prior_state` is typed WorkflowStreamState | None on AskInput (never
        # Any) so a Continue-As-New rollover would rehydrate correctly. This
        # short-lived workflow never actually continues-as-new, but the field is
        # modeled correctly per the critical rules.
        self.stream = WorkflowStream(prior_state=input.stream_state)

    @workflow.run
    async def run(self, input: AskInput) -> str:
        # First-activation handler race guard: a publish Signal from the activity
        # can be enqueued before dynamically-registered handlers have run on the
        # very first activation. A single no-op yield lets them install first.
        # asyncio.sleep(0) adds NO history events; workflow.sleep(0) would record
        # a timer — do not substitute it.
        await asyncio.sleep(0)

        # The workflow hosts the stream but never subscribes to it (CRITICAL
        # RULE 4). The activity publishes tokens directly; only external clients
        # (the FastAPI bridge) subscribe. The workflow only ever sees the
        # activity's successful return value, which is why it stays consistent
        # even when the stream carries a dead attempt's partial tokens.
        answer = await workflow.execute_activity(
            ask_llm_streaming,
            AskLLMInput(
                question=input.question,
                workflow_id=workflow.info().workflow_id,
                simulate_failure=input.simulate_failure,
            ),
            # Hard per-activity ceiling. Generous so the DEMO_PAUSE window and a
            # real completion both fit; the graceful fallback handles slowness
            # well before this fires.
            start_to_close_timeout=timedelta(minutes=10),
            # Short heartbeat timeout so a SIGKILLed worker is detected quickly
            # and the activity is retried as a NEW attempt (scenario 2).
            heartbeat_timeout=timedelta(seconds=6),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=5),
                # >1 so the worker-crash retry actually happens. The forced-error
                # scenario does NOT consume attempts: it is caught inside the
                # activity and returns success.
                maximum_attempts=500,
            ),
        )

        # End-of-stream overlap. Workflow Streams has no close() marker; a
        # workflow that returns immediately after its last publish can lose the
        # final poll round-trip. A brief sleep keeps the stream pollable so
        # subscribers reliably drain the tail (the terminal `complete` event).
        await workflow.sleep(timedelta(seconds=30))

        return answer
