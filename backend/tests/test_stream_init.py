"""Critical rule 1: WorkflowStream() must be constructed from @workflow.init.

The library enforces this by inspecting the *immediate caller's frame name* and
requiring it to be exactly ``__init__``. That check fires before any workflow
context is touched, so we can assert it directly and fast — no worker needed.
"""

from __future__ import annotations

import pytest

# NOTE: Public Preview API — temporalio.contrib.workflow_streams.
from temporalio.contrib.workflow_streams import WorkflowStream


def test_construct_outside_init_raises_runtime_error():
    # A helper (frame name != "__init__") — this is the @workflow.run / handler /
    # helper case the rule warns about.
    def helper():
        return WorkflowStream()

    with pytest.raises(RuntimeError) as exc:
        helper()

    msg = str(exc.value)
    assert "@workflow.init" in msg
    assert "not from 'helper'" in msg


def test_construct_from_run_named_method_raises():
    # Simulate a @workflow.run body: a method literally named `run`.
    class FakeWorkflow:
        def run(self):
            return WorkflowStream()

    with pytest.raises(RuntimeError) as exc:
        FakeWorkflow().run()

    assert "not from 'run'" in str(exc.value)


def test_init_frame_passes_the_frame_check():
    # A frame named `__init__` passes the frame-name gate. It then fails for a
    # *different* reason (no workflow event loop), proving the frame check itself
    # is not what rejects a correctly-placed construction.
    class FakeWorkflow:
        def __init__(self):
            WorkflowStream()

    with pytest.raises(Exception) as exc:
        FakeWorkflow()

    # Must NOT be the frame-name RuntimeError.
    assert "must be constructed directly from the workflow" not in str(exc.value)
