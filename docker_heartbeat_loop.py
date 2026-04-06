"""POST /internal/nodes/heartbeat every ~10s with Docker health + minimal system metrics."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import psutil

from docker_cli import docker_health_for_heartbeat
from http_client import NodeHttpClient


def _heartbeat_url(cfg: Any) -> str:
    b = cfg.backend_url.rstrip("/")
    p = (cfg.api_prefix or "").strip()
    if p and not p.startswith("/"):
        p = f"/{p}"
    return f"{b}{p}/internal/nodes/heartbeat"


def _build_payload() -> dict[str, Any]:
    dh = docker_health_for_heartbeat()
    cpu = float(psutil.cpu_percent(interval=0.15))
    mem = float(psutil.virtual_memory().percent)
    return {
        "system": {
            "cpu": {"usage": round(cpu, 1)},
            "memory": {"usagePercent": round(mem, 1)},
        },
        "docker": {
            "installed": dh["installed"],
            "running": dh["running"],
            "version": dh["version"],
        },
        "timestamp": datetime.now(UTC).isoformat(),
    }


async def run_docker_heartbeat_loop(
    logger: logging.Logger,
    load_config_fn: Callable[[], Any],
    stop_event: asyncio.Event,
    *,
    interval_sec: float = 10.0,
) -> None:
    http = NodeHttpClient(logger)
    while not stop_event.is_set():
        try:
            cfg = load_config_fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("docker heartbeat: load_config failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue

        body = _build_payload()
        url = _heartbeat_url(cfg)
        try:
            code, _resp = await asyncio.to_thread(
                lambda: http.post_json(url, body, bearer=cfg.token, timeout=25.0),
            )
            if code == 401:
                logger.warning("Docker heartbeat rejected (401). Check agent token.")
            elif code != 200:
                logger.debug("docker heartbeat POST %s -> %s", url, code)
        except Exception as exc:  # noqa: BLE001
            logger.debug("docker heartbeat POST failed: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass
