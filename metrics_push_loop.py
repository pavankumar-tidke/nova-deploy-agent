"""Push host + container metrics to POST /internal/metrics every ~2s."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from docker_cli import docker_capabilities_snapshot
from docker_manager import get_docker_manager
from http_client import NodeHttpClient
from system_monitor import SystemMonitor


_DEPLOY_NAME = re.compile(r"^nodehost-dep-([a-f0-9]{12})$", re.IGNORECASE)


def _internal_metrics_url(cfg: Any) -> str:
    b = cfg.backend_url.rstrip("/")
    p = (cfg.api_prefix or "").strip()
    if p and not p.startswith("/"):
        p = f"/{p}"
    return f"{b}{p}/internal/metrics"


def _system_snapshot(monitor: SystemMonitor) -> dict[str, float]:
    data = monitor.collect()
    sys = data.get("system") or {}
    cpu_block = sys.get("cpu") or {}
    mem_block = sys.get("memory") or {}
    cpu = float(cpu_block.get("usage") or 0)
    mem = float(mem_block.get("usagePercent") or 0)
    disks = sys.get("disks") or []
    disk = 0.0
    if isinstance(disks, list) and disks:
        disk = max(float(d.get("usagePercent") or 0) for d in disks if isinstance(d, dict))
    elif sys.get("disk"):
        disk = float((sys.get("disk") or {}).get("usagePercent") or 0)
    return {
        "cpu": max(0.0, min(100.0, cpu)),
        "memory": max(0.0, min(100.0, mem)),
        "disk": max(0.0, min(100.0, disk)),
    }


def _build_payload(monitor: SystemMonitor, logger: logging.Logger) -> dict[str, Any]:
    sys_pct = _system_snapshot(monitor)
    containers: list[dict[str, Any]] = []
    dm = get_docker_manager(logger)
    try:
        items, err = dm.list_containers_summary()
        if err:
            logger.debug("metrics: container list: %s", err)
            items = []
    except Exception as exc:  # noqa: BLE001
        logger.debug("metrics: list_containers_summary failed: %s", exc)
        items = []

    for item in items:
        if (item.get("status") or "").lower() != "running":
            continue
        name = (item.get("name") or "").strip()
        m = _DEPLOY_NAME.match(name)
        dep_hint = m.group(1).lower() if m else None
        cid = (item.get("id") or "").strip()
        if not cid:
            continue
        cpu = float(item.get("cpu_usage") or 0)
        mem_mb = float(item.get("memory_usage") or 0)
        mem_lim = float(item.get("memory_limit") or 0)
        mem_pct = (100.0 * mem_mb / mem_lim) if mem_lim > 0 else 0.0
        row: dict[str, Any] = {
            "containerId": cid,
            "cpu": max(0.0, min(100.0, cpu)),
            "memory": max(0.0, min(100.0, mem_pct)),
            "restartCount": int(item.get("restart_count") or 0),
        }
        if dep_hint:
            row["deploymentId"] = dep_hint
        containers.append(row)

    caps = docker_capabilities_snapshot()

    return {
        "system": {
            "cpu": sys_pct["cpu"],
            "memory": sys_pct["memory"],
            "disk": sys_pct["disk"],
        },
        "containers": containers,
        "timestamp": datetime.now(UTC).isoformat(),
        **caps,
    }


async def run_metrics_push_loop(
    logger: logging.Logger,
    load_config_fn: Callable[[], Any],
    stop_event: asyncio.Event,
    *,
    interval_sec: float = 1.0,
) -> None:
    http = NodeHttpClient(logger)
    monitor = SystemMonitor()
    while not stop_event.is_set():
        try:
            cfg = load_config_fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("metrics: load_config failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue

        payload = _build_payload(monitor, logger)
        url = _internal_metrics_url(cfg)
        try:
            code, _body = await asyncio.to_thread(
                lambda: http.post_json(url, payload, bearer=cfg.token, timeout=20.0),
            )
            if code == 401:
                logger.warning("Metrics push rejected (401). Check agent token.")
            elif code != 200:
                logger.debug("metrics POST -> %s", code)
        except Exception as exc:  # noqa: BLE001
            logger.debug("metrics POST failed: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass
