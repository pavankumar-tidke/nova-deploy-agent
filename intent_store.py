"""Persisted agent run/stop intent (shared by watchdog and agent process)."""

from __future__ import annotations

import os
from pathlib import Path

from config import nodehost_home


def runtime_dir() -> Path:
    d = nodehost_home() / "runtime"
    d.mkdir(parents=True, exist_ok=True)
    return d


def intent_path() -> Path:
    return runtime_dir() / "agent_intent"


def read_intent() -> str:
    p = intent_path()
    if not p.exists():
        return "run"
    try:
        raw = p.read_text(encoding="utf-8").strip().lower()
    except OSError:
        return "run"
    return "stop" if raw == "stop" else "run"


def write_intent(value: str) -> None:
    p = intent_path()
    p.write_text(("stop" if value == "stop" else "run") + "\n", encoding="utf-8")
    try:
        os.chmod(p, 0o600)
    except OSError:
        pass
