"""Environment-derived configuration, loaded once at import time.

Kept out of the workflow file: workflow definitions are reloaded on every task in
the determinism sandbox, and reading env vars there would be non-deterministic
anyway. Config is read in the worker/bridge/activity processes, never in workflow
code.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

# Load the repo-root .env if present. Safe no-op when the file is absent.
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "streams-mechanics")

ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")

BRIDGE_HOST = os.getenv("BRIDGE_HOST", "127.0.0.1")
BRIDGE_PORT = int(os.getenv("BRIDGE_PORT", "8000"))


def _int_or_none(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


# ── DEMO-ONLY scaffolding knobs (see .env.example). Not production patterns. ──
DEMO_PAUSE_AT_TOKEN = _int_or_none("DEMO_PAUSE_AT_TOKEN")
DEMO_PAUSE_SECONDS = int(os.getenv("DEMO_PAUSE_SECONDS", "120"))
DEMO_FORCE_LLM_ERROR_AT_TOKEN = _int_or_none("DEMO_FORCE_LLM_ERROR_AT_TOKEN")
