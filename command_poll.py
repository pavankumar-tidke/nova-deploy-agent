"""Poll backend for STOP_AGENT / RESTART_AGENT / … so the UI works even if only the agent process runs."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from typing import Any

from http_client import NodeHttpClient
from intent_store import read_intent, write_intent


def _internal_base(backend_url: str, api_prefix: str) -> str:
    b = backend_url.rstrip("/")
    p = (api_prefix or "").strip()
    if p and not p.startswith("/"):
        p = f"/{p}"
    return f"{b}{p}/internal"


def _commands_url(cfg: Any) -> str:
    return f"{_internal_base(cfg.backend_url, cfg.api_prefix)}/nodes/commands"


def _result_url(cfg: Any) -> str:
    return f"{_internal_base(cfg.backend_url, cfg.api_prefix)}/nodes/command-result"


def _post_result(
    http: NodeHttpClient,
    cfg: Any,
    command_id: str,
    status: str,
    detail: str | None,
    payload: dict[str, Any] | None,
) -> None:
    body = {
        "command_id": command_id,
        "status": status,
        "detail": detail,
        "payload": payload or {},
    }
    code, resp = http.post_json(_result_url(cfg), body, bearer=cfg.token, timeout=30.0)
    if code != 200:
        logging.getLogger("nodehost-agent").warning("command-result failed: HTTP %s %s", code, resp)


def _handle_one_as_agent(
    logger: logging.Logger,
    http: NodeHttpClient,
    cfg: Any,
    cmd: dict[str, Any],
    stop_event: asyncio.Event,
) -> None:
    cid = str(cmd.get("id") or "")
    ctype = str(cmd.get("type") or "").strip().upper()
    if not cid:
        return
    logger.info("Remote command %s %s", cid, ctype)
    detail: str | None = None
    extra: dict[str, Any] | None = None
    ok = True
    should_stop = False
    try:
        if ctype == "START_AGENT":
            # Already running in this process.
            pass
        elif ctype == "STOP_AGENT":
            write_intent("stop")
            should_stop = True
        elif ctype == "RESTART_AGENT":
            write_intent("run")
            should_stop = True
        elif ctype == "GET_STATUS":
            extra = {
                "agent_running": True,
                "pid": os.getpid(),
                "intent": read_intent(),
            }
        else:
            ok = False
            detail = f"unknown_command:{ctype}"
    except Exception as exc:  # noqa: BLE001
        ok = False
        detail = str(exc)
    _post_result(http, cfg, cid, "executed" if ok else "failed", detail, extra)
    if should_stop and ok:
        logger.info("Stopping agent process after %s", ctype)
        stop_event.set()


async def run_command_poll_loop(
    logger: logging.Logger,
    load_config_fn: Callable[[], Any],
    stop_event: asyncio.Event,
    *,
    interval_sec: float = 4.0,
) -> None:
    http = NodeHttpClient(logger)
    while not stop_event.is_set():
        try:
            cfg = load_config_fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("command poll: load_config failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            url = _commands_url(cfg)
            code, body = await asyncio.to_thread(
                lambda: http.get_json(url, bearer=cfg.token),
            )
        except Exception as exc:  # noqa: BLE001 — urllib errors, network
            logger.debug("command poll error: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
            continue
        if code == 401:
            logger.warning(
                "Command poll rejected (401). Renew token in config — same as watchdog heartbeat."
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=min(30.0, interval_sec * 4))
            except asyncio.TimeoutError:
                pass
            continue
        if code == 403:
            logger.warning(
                "Command poll forbidden (403). Set NODEHOST_INTERNAL_API_KEY to match backend INTERNAL_API_KEY."
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
            continue
        if code != 200:
            logger.debug("command poll HTTP %s", code)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
            continue
        data = body.get("data")
        if not isinstance(data, list):
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
            continue
        for cmd in data:
            if stop_event.is_set():
                break
            await asyncio.to_thread(_handle_one_as_agent, logger, http, cfg, cmd, stop_event)

        if stop_event.is_set():
            break
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass