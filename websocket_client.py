import asyncio
import json
import logging
import random
from collections.abc import Callable
from typing import Any, Awaitable

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from config import AgentConfig, save_token
from utils import normalize_ws_url


MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]
HeartbeatProvider = Callable[[], dict[str, Any]]
SleepProvider = Callable[[], Awaitable[None]]
ConfigLoader = Callable[[], AgentConfig]


class AuthRejectedError(Exception):
    def __init__(self, message: str, *, expired: bool = False) -> None:
        super().__init__(message)
        self.expired = expired


class NodeWebSocketClient:
    def __init__(
        self,
        config: AgentConfig,
        logger: logging.Logger,
        on_message: MessageHandler,
        heartbeat_provider: HeartbeatProvider,
        heartbeat_wait: SleepProvider,
        load_config_fn: ConfigLoader,
    ) -> None:
        self._config = config
        self._logger = logger
        self._on_message = on_message
        self._heartbeat_provider = heartbeat_provider
        self._heartbeat_wait = heartbeat_wait
        self._load_config_fn = load_config_fn
        self._stop_event = asyncio.Event()
        self._max_backoff = 60.0
        self._token_refresh_interval = 10.0

    def _reload_config(self) -> None:
        self._config = self._load_config_fn()

    async def run_forever(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            ws_url = normalize_ws_url(self._config.backend_url)
            try:
                await self._connect_and_serve(ws_url)
                attempt = 0
            except AuthRejectedError as exc:
                msg = str(exc).lower()
                is_expired = exc.expired or "agent token expired" in msg
                if is_expired:
                    self._logger.warning("Agent token rejected or expired; attempting refresh.")
                    await self._token_refresh_loop()
                else:
                    self._logger.warning("Authentication rejected: %s. Retrying.", exc)
                    await asyncio.sleep(min(self._max_backoff, 15.0))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                delay = min(self._max_backoff, 3.0 * (2 ** min(attempt - 1, 8)))
                jitter = random.uniform(0.0, 2.0)
                wait_time = min(self._max_backoff, delay + jitter)
                self._logger.warning(
                    "Connection error: %s. Reconnecting in %.1f sec (attempt %s).",
                    exc,
                    wait_time,
                    attempt,
                )
                await asyncio.sleep(wait_time)

    async def _token_refresh_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                ok = await asyncio.to_thread(self._sync_refresh_token)
            except Exception as exc:  # noqa: BLE001
                self._logger.warning("Token refresh error: %s", exc)
                ok = False
            if ok:
                self._logger.info("Token refreshed; resuming connection.")
                return
            self._logger.info("Waiting for renewed token on server (retry in %.0f s).", self._token_refresh_interval)
            await asyncio.sleep(self._token_refresh_interval)

    def _sync_refresh_token(self) -> bool:
        from http_client import NodeHttpClient
        from urllib.error import HTTPError, URLError

        cfg = self._load_config_fn()
        api = cfg.api_prefix or ""
        url = f"{cfg.backend_url.rstrip('/')}{api}/internal/nodes/refresh-token"
        http = NodeHttpClient(self._logger)
        try:
            status, body = http.get_json(url, bearer=cfg.token)
        except (HTTPError, URLError, OSError):
            return False
        if status == 401:
            return False
        if status >= 400:
            return False
        if not isinstance(body, dict) or body.get("success") is not True:
            return False
        data = body.get("data")
        if not isinstance(data, dict):
            return False
        new_tok = str(data.get("token", "")).strip()
        if not new_tok:
            return False
        save_token(None, new_tok)
        self._reload_config()
        return True

    async def stop(self) -> None:
        self._stop_event.set()

    async def _connect_and_serve(self, ws_url: str) -> None:
        self._logger.info("Connecting to server: %s", ws_url)
        async with websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=2 * 1024 * 1024,
        ) as websocket:
            self._logger.info("Connected to server")
            await self._authenticate(websocket)
            self._logger.info("Auth success")

            heartbeat_task = asyncio.create_task(self._heartbeat_loop(websocket))
            receive_task = asyncio.create_task(self._receive_loop(websocket))

            done, pending = await asyncio.wait(
                [heartbeat_task, receive_task],
                return_when=asyncio.FIRST_EXCEPTION,
            )

            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for task in done:
                exc = task.exception()
                if exc:
                    raise exc

    async def _authenticate(self, websocket: websockets.WebSocketClientProtocol) -> None:
        auth_payload: dict[str, Any] = {"type": "auth", "token": self._config.token}
        await websocket.send(json.dumps(auth_payload))

        raw = await asyncio.wait_for(websocket.recv(), timeout=15)
        message = self._parse_json(raw)

        if message.get("success") is True:
            return

        if message.get("type") == "auth" and message.get("status") == "success":
            return

        err = message.get("error") if isinstance(message.get("error"), dict) else {}
        detail = ""
        if isinstance(err, dict):
            detail = str(err.get("message", "") or "")
        detail_l = detail.lower()
        expired = "agent token expired" in detail_l
        raise AuthRejectedError(detail or "auth rejected", expired=expired)

    async def _heartbeat_loop(self, websocket: websockets.WebSocketClientProtocol) -> None:
        while not self._stop_event.is_set():
            payload = self._heartbeat_provider()
            await websocket.send(json.dumps(payload))
            sys = payload.get("system") if isinstance(payload.get("system"), dict) else {}
            cpu_b = sys.get("cpu") if isinstance(sys.get("cpu"), dict) else {}
            mem_b = sys.get("memory") if isinstance(sys.get("memory"), dict) else {}
            dsk_b = sys.get("disk") if isinstance(sys.get("disk"), dict) else {}
            self._logger.info(
                "Heartbeat sent cpu=%s memory=%s disk=%s",
                cpu_b.get("usage"),
                mem_b.get("usagePercent"),
                dsk_b.get("usagePercent"),
            )
            await self._heartbeat_wait()

    async def _receive_loop(self, websocket: websockets.WebSocketClientProtocol) -> None:
        while not self._stop_event.is_set():
            try:
                raw = await websocket.recv()
            except ConnectionClosed as exc:
                raise ConnectionError(f"WebSocket closed: code={exc.code}") from exc

            message = self._parse_json(raw)
            await self._on_message(message)

    def _parse_json(self, raw: Any) -> dict[str, Any]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")

        if not isinstance(raw, str):
            raise WebSocketException("Unsupported message payload type")

        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise WebSocketException("Invalid JSON message from server") from exc

        if not isinstance(decoded, dict):
            raise WebSocketException("Expected JSON object message")

        return decoded
