"""Poll backend for pending deployment jobs and execute them (long-running; runs in a thread)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from deploy_runner import poll_one_job
from http_client import NodeHttpClient


async def run_deployment_job_loop(
    logger: logging.Logger,
    load_config_fn: Callable[[], Any],
    stop_event: asyncio.Event,
    *,
    interval_sec: float = 6.0,
) -> None:
    http = NodeHttpClient(logger)
    while not stop_event.is_set():
        try:
            cfg = load_config_fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("deployment job poll: load_config failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            did = await asyncio.to_thread(poll_one_job, logger, http, cfg)
            if did:
                interval_sec = 2.0
            else:
                interval_sec = 6.0
        except Exception as exc:  # noqa: BLE001
            logger.warning("deployment job execution failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass
