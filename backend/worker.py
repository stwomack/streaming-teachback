"""Worker: hosts AskWorkflow and the streaming activity.

Run with:  uv run python worker.py   (or python worker.py inside the venv)

Scenario 2 (worker crash) SIGKILLs this process mid-activity; kill-and-restart.sh
brings it back so Temporal can reschedule the retried attempt onto it.
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from activities.ask_llm import ask_llm_streaming
from shared import config
from workflows.ask_workflow import AskWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")


async def main() -> None:
    client = await Client.connect(
        config.TEMPORAL_ADDRESS,
        namespace=config.TEMPORAL_NAMESPACE,
    )
    logger.info(
        "Worker connecting: address=%s namespace=%s task_queue=%s",
        config.TEMPORAL_ADDRESS,
        config.TEMPORAL_NAMESPACE,
        config.TASK_QUEUE,
    )

    worker = Worker(
        client,
        task_queue=config.TASK_QUEUE,
        workflows=[AskWorkflow],
        # ask_llm_streaming is an async activity (AsyncAnthropic +
        # WorkflowStreamClient are asyncio-only), so it runs on the event loop
        # and needs no ThreadPoolExecutor.
        activities=[ask_llm_streaming],
    )
    logger.info("Worker started. Ctrl-C or SIGKILL to stop.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
