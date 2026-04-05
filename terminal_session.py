"""Poll backend for pending terminal sessions and bridge a PTY to the agent WebSocket."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any, Callable
from urllib.parse import urlparse

import websockets
from websockets.exceptions import ConnectionClosed

from config import AgentConfig
from http_client import NodeHttpClient


def _pending_url(cfg: AgentConfig) -> str:
    b = cfg.backend_url.strip().rstrip("/")
    ap = (cfg.api_prefix or "").strip()
    if ap and not ap.startswith("/"):
        ap = f"/{ap}"
    return f"{b}{ap}/internal/nodes/terminal/pending"


def _agent_terminal_ws_url(cfg: AgentConfig, session_id: str) -> str:
    b = cfg.backend_url.strip().rstrip("/")
    ap = (cfg.api_prefix or "").strip()
    if ap and not ap.startswith("/"):
        ap = f"/{ap}"
    path = f"{ap}/internal/ws/terminal/agent/{session_id}"
    parsed = urlparse(b)
    if parsed.scheme in {"http", "https"}:
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        return f"{ws_scheme}://{parsed.netloc}{path}"
    raise ValueError("backendUrl must be http(s)://")


async def _pipe_pty_to_ws(master_fd: int, ws: Any, logger: logging.Logger) -> None:
    while True:
        try:
            data = await asyncio.to_thread(os.read, master_fd, 65536)
        except OSError as exc:
            logger.debug("pty read end: %s", exc)
            break
        if not data:
            break
        try:
            await ws.send(data)
        except ConnectionClosed:
            break


async def _pipe_ws_to_pty(ws: Any, master_fd: int, logger: logging.Logger) -> None:
    try:
        while True:
            try:
                message = await ws.recv()
            except ConnectionClosed:
                break
            if isinstance(message, (bytes, bytearray)):
                raw = bytes(message)
            else:
                raw = str(message).encode("utf-8", errors="replace")
            try:
                await asyncio.to_thread(os.write, master_fd, raw)
            except OSError as exc:
                logger.debug("pty write: %s", exc)
                break
    except ConnectionClosed:
        pass


def _spawn_shell_pty() -> tuple[int, int]:
    import pty as pty_mod

    master, slave = pty_mod.openpty()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(master)
            os.setsid()
            os.dup2(slave, 0)
            os.dup2(slave, 1)
            os.dup2(slave, 2)
            if slave > 2:
                os.close(slave)
            shell = os.environ.get("SHELL", "/bin/bash")
            os.execl(shell, shell, "-l")
        except OSError:
            os._exit(127)
    os.close(slave)
    return pid, master


async def _run_one_session(cfg: AgentConfig, session_id: str, logger: logging.Logger) -> None:
    if os.name == "nt":
        logger.warning("Terminal PTY is not supported on Windows")
        return
    uri = _agent_terminal_ws_url(cfg, session_id)
    hdr = {"Authorization": f"Bearer {cfg.token}"}
    try:
        async with websockets.connect(
            uri,
            additional_headers=hdr,
            max_size=16 * 1024 * 1024,
        ) as ws:
            pid, master_fd = _spawn_shell_pty()
            t1 = asyncio.create_task(_pipe_pty_to_ws(master_fd, ws, logger))
            t2 = asyncio.create_task(_pipe_ws_to_pty(ws, master_fd, logger))
            _done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
            for p in pending:
                p.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            try:
                os.close(master_fd)
            except OSError:
                pass
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("Terminal session %s failed: %s", session_id, exc)


async def terminal_poll_loop(
    logger: logging.Logger,
    load_config_fn: Callable[[], AgentConfig],
    stop_event: asyncio.Event,
    *,
    interval_sec: float = 2.0,
) -> None:
    http = NodeHttpClient(logger)
    while not stop_event.is_set():
        cfg = load_config_fn()
        url = _pending_url(cfg)
        session_id: str | None = None
        try:
            code, body = http.get_json(url, bearer=cfg.token, timeout=25.0)
            if code == 401:
                logger.warning(
                    "Terminal poll: agent token rejected (node deleted or token rotated). Backing off 10 min."
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=600.0)
                except asyncio.TimeoutError:
                    pass
                continue
            if code == 200 and isinstance(body, dict):
                data = body.get("data")
                if isinstance(data, dict):
                    sid = data.get("session_id")
                    if isinstance(sid, str) and sid.strip():
                        session_id = sid.strip()
        except Exception as exc:  # noqa: BLE001
            logger.debug("terminal pending poll: %s", exc)
        if session_id:
            await _run_one_session(cfg, session_id, logger)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass
