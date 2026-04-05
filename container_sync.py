"""Push Docker container snapshots to the backend on an interval."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from docker_manager import get_docker_manager
from http_client import NodeHttpClient


def _internal_base(backend_url: str, api_prefix: str) -> str:
    b = backend_url.rstrip("/")
    p = (api_prefix or "").strip()
    if p and not p.startswith("/"):
        p = f"/{p}"
    return f"{b}{p}/internal"


def _containers_url(cfg: Any) -> str:
    return f"{_internal_base(cfg.backend_url, cfg.api_prefix)}/nodes/containers"


async def run_container_sync_loop(
    logger: logging.Logger,
    load_config_fn: Callable[[], Any],
    stop_event: asyncio.Event,
    *,
    interval_sec: float = 12.0,
) -> None:
    http = NodeHttpClient(logger)
    while not stop_event.is_set():
        try:
            cfg = load_config_fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("container sync: load_config failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue
        dm = get_docker_manager(logger)
        try:
            items, docker_error = await asyncio.to_thread(dm.list_containers_summary)
        except Exception as exc:  # noqa: BLE001
            logger.debug("container list failed: %s", exc)
            items, docker_error = [], str(exc)
        payload = {"items": items, "docker_error": docker_error}
        try:
            code, body = await asyncio.to_thread(
                lambda: http.post_json(
                    _containers_url(cfg),
                    payload,
                    bearer=cfg.token,
                    timeout=45.0,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("container sync post failed: %s", exc)
            code = 0
        if code == 401:
            logger.warning("Container sync rejected (401). Check agent token.")
        elif code != 200 and code != 0:
            logger.debug("container sync HTTP %s %s", code, body)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass
