from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class NodeHttpClient:
    """Minimal sync HTTP client (stdlib) for token refresh and log sync."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def _internal_headers(self) -> dict[str, str]:
        k = os.environ.get("NODEHOST_INTERNAL_API_KEY", "").strip()
        return {"X-Internal-Key": k} if k else {}

    def _url(self, base: str, path: str) -> str:
        b = base.rstrip("/")
        p = path if path.startswith("/") else f"/{path}"
        return f"{b}{p}"

    def get_json(
        self,
        url: str,
        *,
        bearer: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> tuple[int, dict[str, Any]]:
        h = {**self._internal_headers(), **(headers or {})}
        if bearer:
            h["Authorization"] = f"Bearer {bearer}"
        req = Request(url, method="GET", headers=h)
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                body = json.loads(raw) if raw.strip() else {}
                return int(resp.status), body if isinstance(body, dict) else {}
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            try:
                body = json.loads(raw) if raw.strip() else {}
            except json.JSONDecodeError:
                body = {}
            return int(exc.code), body if isinstance(body, dict) else {}
        except URLError as exc:
            self._logger.warning("HTTP GET failed: %s", exc)
            raise

    def post_json(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        bearer: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 45.0,
    ) -> tuple[int, dict[str, Any]]:
        data = json.dumps(payload).encode("utf-8")
        h = {"Content-Type": "application/json", **self._internal_headers(), **(headers or {})}
        if bearer:
            h["Authorization"] = f"Bearer {bearer}"
        req = Request(url, data=data, method="POST", headers=h)
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                body = json.loads(raw) if raw.strip() else {}
                return int(resp.status), body if isinstance(body, dict) else {}
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            try:
                body = json.loads(raw) if raw.strip() else {}
            except json.JSONDecodeError:
                body = {}
            return int(exc.code), body if isinstance(body, dict) else {}
        except URLError as exc:
            self._logger.warning("HTTP POST failed: %s", exc)
            raise
