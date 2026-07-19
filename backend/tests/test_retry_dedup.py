"""Critical rule 3 / scenario 2: an activity-level retry is a NEW publisher.

Dedup is scoped to a single publisher instance's own retried flushes, keyed on
(publisher_id, sequence). A retried Activity attempt constructs a *new*
WorkflowStreamClient with a *new* publisher_id, so its events do NOT deduplicate
against the prior attempt's — both attempts' events stay on the stream, and the
consumer must detect the boundary and reset itself.

This test models two attempts as two separate WorkflowStreamClient instances that
publish the SAME sequence numbers. If cross-attempt dedup existed, the second set
would be dropped. We assert both sets survive.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStream, WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

TASK_QUEUE = "test-streams-dedup"
TOPIC = "events"


@dataclass
class Evt:
    seq: int
    text: str


@workflow.defn
class StreamHostWorkflow:
    """Minimal host: constructs the stream in __init__ and stays alive so external
    clients (standing in for two activity attempts) can publish and subscribe."""

    @workflow.init
    def __init__(self) -> None:
        self.stream = WorkflowStream()
        self._done = False

    @workflow.signal
    def finish(self) -> None:
        self._done = True

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)


async def _collect(client: WorkflowStreamClient, workflow_id: str) -> list[Evt]:
    head = await client.get_offset()
    out: list[Evt] = []
    if head == 0:
        return out
    topic = client.topic(TOPIC, type=Evt)
    async for item in topic.subscribe(from_offset=0):
        out.append(item.data)
        if item.offset >= head - 1:
            break
    return out


@pytest.mark.asyncio
async def test_activity_retry_new_publisher_is_not_deduped():
    async with await WorkflowEnvironment.start_local() as env:
        client: Client = env.client
        async with Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[StreamHostWorkflow],
        ):
            workflow_id = f"host-{uuid.uuid4().hex[:8]}"
            handle = await client.start_workflow(
                StreamHostWorkflow.run,
                id=workflow_id,
                task_queue=TASK_QUEUE,
            )

            # ── Attempt 1: publisher A publishes sequences 0,1,2 ─────────────
            pub_a = WorkflowStreamClient.create(client, workflow_id=workflow_id)
            async with pub_a:
                topic_a = pub_a.topic(TOPIC, type=Evt)
                for i in range(3):
                    topic_a.publish(Evt(seq=i, text=f"A{i}"))
                await pub_a.flush()

            # ── Attempt 2 (the retry): a NEW client → NEW publisher_id, same
            #    sequence numbers 0,1,2 ────────────────────────────────────────
            pub_b = WorkflowStreamClient.create(client, workflow_id=workflow_id)
            async with pub_b:
                topic_b = pub_b.topic(TOPIC, type=Evt)
                for i in range(3):
                    topic_b.publish(Evt(seq=i, text=f"B{i}"))
                await pub_b.flush()

            # Two attempts => two distinct publisher_ids.
            assert pub_a._publisher_id != pub_b._publisher_id

            events = await _collect(pub_b, workflow_id)

            texts = [e.text for e in events]
            # Both attempts survive despite identical (seq) values: dedup did NOT
            # span the publisher boundary. 3 + 3 = 6 events, none dropped.
            assert len(events) == 6, texts
            assert {"A0", "A1", "A2"}.issubset(set(texts))
            assert {"B0", "B1", "B2"}.issubset(set(texts))
            # The dead attempt's events are still present, unretracted, ahead of
            # the retry's — exactly what forces the consumer to reset on RETRY.
            assert texts.index("A0") < texts.index("B0")

            await handle.signal(StreamHostWorkflow.finish)
            await handle.result()
