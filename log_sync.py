from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable

from config import AgentConfig, nodehost_home
from http_client import NodeHttpClient


class LogSync:
    """Append structured log lines to disk; flush to backend when reachable."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._http = NodeHttpClient(logger)
        self._lock = threading.Lock()
        self._queue_path = nodehost_home() / "data" / "pending_logs.jsonl"
        self._queue_path.parent.mkdir(parents=True, exist_ok=True)
        self._pause_log_flush_until: float = 0.0

    def enqueue(self, entry: dict[str, Any]) -> None:
        line = json.dumps(entry, ensure_ascii=False) + "\n"
        with self._lock:
            self._queue_path.parent.mkdir(parents=True, exist_ok=True)
            with self._queue_path.open("a", encoding="utf-8") as f:
                f.write(line)

    def flush_pending(self, config: AgentConfig) -> int:
        if time.time() < self._pause_log_flush_until:
            return 0
        if not self._queue_path.exists():
            return 0
        with self._lock:
            try:
                raw = self._queue_path.read_text(encoding="utf-8")
            except OSError:
                return 0
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        if not lines:
            return 0
        batch: list[dict[str, Any]] = []
        for ln in lines[:500]:
            try:
                batch.append(json.loads(ln))
            except json.JSONDecodeError:
                continue
        if not batch:
            with self._lock:
                self._queue_path.write_text("", encoding="utf-8")
            return 0
        api = config.api_prefix or ""
        url = f"{config.backend_url.rstrip('/')}{api}/internal/nodes/logs"
        payload = {"logs": batch}
        try:
            status, body = self._http.post_json(url, payload, bearer=config.token)
        except OSError:
            return 0
        if status == 401:
            self._pause_log_flush_until = time.time() + 600.0
            self._logger.warning(
                "Log sync: agent token rejected; pausing log upload for 10 min (fix token in config)."
            )
            return 0
        if status >= 400:
            return 0
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(body, dict) and body.get("success") is True:
            pass
        elif status != 200:
            return 0
        with self._lock:
            rest = lines[500:]
            self._queue_path.write_text("\n".join(rest) + ("\n" if rest else ""), encoding="utf-8")
        return len(batch)


def log_sync_loop(
    stop_event: threading.Event,
    get_config: Callable[[], AgentConfig],
    log_sync: LogSync,
    interval_sec: float = 30.0,
) -> None:
    while not stop_event.wait(timeout=interval_sec):
        try:
            cfg = get_config()
            log_sync.flush_pending(cfg)
        except Exception:  # noqa: BLE001
            pass


def start_log_sync_background(
    get_config: Callable[[], AgentConfig],
    log_sync: LogSync,
    interval_sec: float = 30.0,
) -> tuple[threading.Thread, threading.Event]:
    stop_event = threading.Event()

    def _run() -> None:
        log_sync_loop(stop_event, get_config, log_sync, interval_sec=interval_sec)

    t = threading.Thread(target=_run, name="nodehost-log-sync", daemon=True)
    t.start()
    return t, stop_event
