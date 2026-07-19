"""End-to-end plumbing test: AskWorkflow -> ask_llm_streaming -> stream -> subscriber.

Uses a fake Anthropic streaming client so no API key or network is needed. Proves
the real wiring: the activity publishes a `start`, one `token` per delta, then a
`complete`, and an external subscriber (the bridge's role) sees them in order via
the global offset.
"""

from __future__ import annotations

import uuid

import pytest

from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import ask_llm
from activities.ask_llm import ask_llm_streaming
from shared.models import EVENTS_TOPIC, KIND_COMPLETE, KIND_START, KIND_TOKEN, AskInput, StreamEvent
from workflows.ask_workflow import AskWorkflow

TASK_QUEUE = "test-e2e-stream"
FAKE_TOKENS = ["Hello", ", ", "TCP", " ", "handshake", "!"]


class _FakeStream:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    @property
    def text_stream(self):
        async def _gen():
            for t in FAKE_TOKENS:
                yield t

        return _gen()


class _FakeMessages:
    def stream(self, **kwargs):
        return _FakeStream()


class _FakeAsyncAnthropic:
    def __init__(self, *args, **kwargs):
        self.messages = _FakeMessages()


@pytest.mark.asyncio
async def test_workflow_streams_tokens_to_subscriber(monkeypatch):
    # Fake the real Anthropic client inside the activity module.
    monkeypatch.setattr(ask_llm, "anthropic", _patched_anthropic())

    async with await WorkflowEnvironment.start_local() as env:
        client: Client = env.client
        async with Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[AskWorkflow],
            activities=[ask_llm_streaming],
        ):
            workflow_id = f"ask-{uuid.uuid4().hex[:8]}"
            handle = await client.start_workflow(
                AskWorkflow.run,
                AskInput(question="explain how TCP handshakes work"),
                id=workflow_id,
                task_queue=TASK_QUEUE,
            )

            # Subscribe like the bridge would, using a fresh client.
            sub = WorkflowStreamClient.create(client, workflow_id=workflow_id)
            events: list[StreamEvent] = []
            topic = sub.topic(EVENTS_TOPIC, type=StreamEvent)
            async for item in topic.subscribe(from_offset=0):
                events.append(item.data)
                if item.data.kind == KIND_COMPLETE:
                    break

            kinds = [e.kind for e in events]
            assert kinds[0] == KIND_START
            assert KIND_COMPLETE in kinds
            tokens = [e for e in events if e.kind == KIND_TOKEN]
            assert [t.text for t in tokens] == FAKE_TOKENS
            # seq is per-attempt and monotonic within the attempt.
            assert [t.seq for t in tokens] == list(range(1, len(FAKE_TOKENS) + 1))

            # The response completed before the workflow's 30s end-of-stream
            # overlap; terminate rather than wait it out in the test.
            await handle.terminate()


def _patched_anthropic():
    """Build a stand-in `anthropic` module object exposing the two names the
    activity references: AsyncAnthropic (constructor) and APIError (for except)."""
    import anthropic as real

    class _Shim:
        AsyncAnthropic = _FakeAsyncAnthropic
        APIError = real.APIError

    return _Shim()
