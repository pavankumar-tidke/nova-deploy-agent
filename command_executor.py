"""Execute remote commands for the agent (Docker lifecycle, logs) and POST results to the backend."""

from __future__ import annotations

import logging
from typing import Any

from http_client import NodeHttpClient

from docker_manager import get_docker_manager


def _internal_base(backend_url: str, api_prefix: str) -> str:
    b = backend_url.rstrip("/")
    p = (api_prefix or "").strip()
    if p and not p.startswith("/"):
        p = f"/{p}"
    return f"{b}{p}/internal"


def _result_url(cfg: Any) -> str:
    return f"{_internal_base(cfg.backend_url, cfg.api_prefix)}/nodes/command-result"


def _post_result(
    http: NodeHttpClient,
    cfg: Any,
    command_id: str,
    status: str,
    detail: str | None,
    payload: dict[str, Any] | None,
    *,
    timeout: float = 120.0,
) -> None:
    body = {
        "command_id": command_id,
        "status": status,
        "detail": detail,
        "payload": payload or {},
    }
    code, resp = http.post_json(_result_url(cfg), body, bearer=cfg.token, timeout=timeout)
    if code != 200:
        logging.getLogger("nodehost-agent").warning("command-result failed: HTTP %s %s", code, resp)


def _payload(cmd: dict[str, Any]) -> dict[str, Any]:
    p = cmd.get("payload")
    return p if isinstance(p, dict) else {}


ALLOWED_DOCKER = frozenset(
    {
        "CREATE_CONTAINER",
        "START_CONTAINER",
        "STOP_CONTAINER",
        "RESTART_CONTAINER",
        "DELETE_CONTAINER",
        "GET_CONTAINER_LOGS",
    }
)


def handle_docker_command(
    logger: logging.Logger,
    http: NodeHttpClient,
    cfg: Any,
    cmd: dict[str, Any],
) -> None:
    cid = str(cmd.get("id") or "")
    ctype = str(cmd.get("type") or "").strip().upper()
    if not cid or ctype not in ALLOWED_DOCKER:
        return
    dm = get_docker_manager(logger)
    pl = _payload(cmd)
    extra: dict[str, Any] = {}

    try:
        if ctype == "CREATE_CONTAINER":
            image = str(pl.get("image") or "").strip()
            if not image:
                _post_result(http, cfg, cid, "failed", "missing_image", None)
                return
            name = (pl.get("name") or pl.get("container_name") or None)
            name_str = str(name).strip() if name else None
            ports_raw = pl.get("ports")
            port_list: list[str] = []
            if isinstance(ports_raw, list):
                port_list = [str(x) for x in ports_raw if x]
            elif isinstance(ports_raw, str) and ports_raw.strip():
                port_list = [p.strip() for p in ports_raw.split(",") if p.strip()]
            new_id = dm.create_container(image, name=name_str, ports=port_list or None)
            extra = {"container_id": new_id}
            _post_result(http, cfg, cid, "executed", None, extra, timeout=300.0)
            return

        ref = str(pl.get("container_id") or pl.get("id") or "").strip()
        if ctype != "CREATE_CONTAINER" and not ref:
            _post_result(http, cfg, cid, "failed", "missing_container_id", None)
            return

        if ctype == "DELETE_CONTAINER":
            full_id = dm.resolve_container_id(ref)
            short = full_id[:12]
            dm.remove(ref)
            _post_result(http, cfg, cid, "executed", None, {"container_id": short})
            return
        if ctype == "START_CONTAINER":
            dm.start(ref)
        elif ctype == "STOP_CONTAINER":
            dm.stop(ref)
        elif ctype == "RESTART_CONTAINER":
            dm.restart(ref)
        elif ctype == "GET_CONTAINER_LOGS":
            tail = int(pl.get("tail") or 500)
            tail = max(10, min(tail, 10000))
            full_id = dm.resolve_container_id(ref)
            text = dm.logs_text(ref, tail=tail)
            short = full_id[:12]
            extra = {"container_id": short, "logs": text}
            _post_result(http, cfg, cid, "executed", None, extra, timeout=180.0)
            return

        full_id = dm.resolve_container_id(ref)
        extra = {"container_id": full_id[:12]}
        _post_result(http, cfg, cid, "executed", None, extra)
    except RuntimeError as exc:
        _post_result(http, cfg, cid, "failed", str(exc), None)
    except Exception as exc:  # noqa: BLE001
        logger.exception("docker command %s failed", ctype)
        _post_result(http, cfg, cid, "failed", str(exc), None)
