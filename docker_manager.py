"""Docker daemon access for container list, lifecycle, logs, and stats (via agent only — no remote socket exposure)."""

from __future__ import annotations

import logging
import re
import time
from datetime import UTC, datetime
from typing import Any

try:
    import docker
    from docker.errors import APIError, DockerException, ImageNotFound, NotFound
except ImportError:
    docker = None  # type: ignore[misc, assignment]
    DockerException = Exception  # type: ignore[misc, assignment]
    NotFound = Exception  # type: ignore[misc, assignment]
    ImageNotFound = Exception  # type: ignore[misc, assignment]
    APIError = Exception  # type: ignore[misc, assignment]


def _uptime_from_started(started: str | None) -> str:
    if not started:
        return "—"
    try:
        dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        delta = datetime.now(UTC) - dt
        secs = int(max(0, delta.total_seconds()))
    except (ValueError, TypeError, OSError):
        return "—"
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    if d > 0:
        return f"{d}d {h}h"
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def _cpu_percent(stats: dict[str, Any]) -> float:
    """CPU utilization 0–100 (fraction of total host CPUs, capped)."""
    try:
        cpu_stats = stats.get("cpu_stats") or {}
        precpu = stats.get("precpu_stats") or {}
        cpu_delta = cpu_stats.get("cpu_usage", {}).get("total_usage", 0) - precpu.get("cpu_usage", {}).get(
            "total_usage", 0
        )
        system_delta = cpu_stats.get("system_cpu_usage", 0) - precpu.get("system_cpu_usage", 0)
        if system_delta <= 0 or cpu_delta < 0:
            return 0.0
        # (cpu_delta/system_delta)*100 is bounded ~0–100 for typical usage; avoid *ncpus which inflated to 1000%+.
        pct = (float(cpu_delta) / float(system_delta)) * 100.0
        return round(min(100.0, max(0.0, pct)), 2)
    except (TypeError, ZeroDivisionError, KeyError):
        return 0.0


def _cpu_percent_between(prev: dict[str, Any], curr: dict[str, Any]) -> float:
    """CPU % (0–100) from two consecutive stats snapshots."""
    try:
        p_cpu = prev.get("cpu_stats") or {}
        c_cpu = curr.get("cpu_stats") or {}
        cpu_delta = (c_cpu.get("cpu_usage") or {}).get("total_usage", 0) - (p_cpu.get("cpu_usage") or {}).get(
            "total_usage", 0
        )
        system_delta = (c_cpu.get("system_cpu_usage", 0) or 0) - (p_cpu.get("system_cpu_usage", 0) or 0)
        if system_delta <= 0 or cpu_delta < 0:
            return 0.0
        pct = (float(cpu_delta) / float(system_delta)) * 100.0
        return round(min(100.0, max(0.0, pct)), 2)
    except (TypeError, ZeroDivisionError, KeyError):
        return 0.0


def _ports_str(attrs: dict[str, Any]) -> str:
    ports = (attrs.get("NetworkSettings") or {}).get("Ports") or {}
    parts: list[str] = []
    for container_port, bindings in ports.items():
        if bindings:
            for b in bindings:
                hip = (b.get("HostIp") or "").replace("::", "0.0.0.0")
                hp = b.get("HostPort") or ""
                parts.append(f"{hip}:{hp}->{container_port}".strip(":"))
        else:
            parts.append(container_port)
    return ", ".join(parts) if parts else "—"


def _status_from_attrs(attrs: dict[str, Any]) -> str:
    st = attrs.get("State") or {}
    if st.get("Restarting"):
        return "restarting"
    if st.get("Running"):
        return "running"
    err = (st.get("Error") or "").strip()
    if err:
        return "error"
    if st.get("Status", "").lower().startswith("dead"):
        return "error"
    return "stopped"


def _parse_port_bindings(port_strings: list[str]) -> dict[str, int | tuple[str, int]]:
    """Map '8080:80' / '127.0.0.1:8080:80' to docker-py ports= dict."""
    out: dict[str, int | tuple[str, int]] = {}
    for raw in port_strings:
        spec = (raw or "").strip()
        if not spec or ":" not in spec:
            continue
        parts = spec.split(":")
        if len(parts) == 3:
            bind_ip, host_port, cport = parts[0], parts[1], parts[2]
            key = f"{cport.split('/')[0]}/tcp"
            try:
                out[key] = (bind_ip, int(host_port))
            except ValueError:
                continue
            continue
        if len(parts) == 2:
            host_port, cport = parts[0], parts[1]
            key = f"{cport.split('/')[0]}/tcp"
            try:
                out[key] = int(host_port)
            except ValueError:
                continue
    return out


class DockerManager:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._client: Any = None

    def _connect(self) -> tuple[Any | None, str | None]:
        if docker is None:
            return None, "python_docker_sdk_missing"
        try:
            if self._client is None:
                self._client = docker.from_env()
            self._client.ping()
            return self._client, None
        except DockerException as exc:
            self._logger.warning("Docker unavailable: %s", exc)
            return None, str(exc)
        except OSError as exc:
            self._logger.warning("Docker socket error: %s", exc)
            return None, str(exc)

    def list_containers_summary(self) -> tuple[list[dict[str, Any]], str | None]:
        client, err = self._connect()
        if err or client is None:
            return [], err or "docker_unavailable"
        out: list[dict[str, Any]] = []
        try:
            for container in client.containers.list(all=True):
                attrs = container.attrs
                short_id = ((attrs.get("Id") or container.id) or "")[:12]
                name = (container.name or "").lstrip("/")
                image = attrs.get("Config", {}).get("Image") or ""
                state = attrs.get("State") or {}
                started = state.get("StartedAt")
                restart_count = int(state.get("RestartCount") or 0)
                status = _status_from_attrs(attrs)
                network_mode = (attrs.get("HostConfig") or {}).get("NetworkMode") or ""
                host_cfg = attrs.get("HostConfig") or {}
                docker_mem_cap = int(host_cfg.get("Memory") or 0)

                cpu_usage = 0.0
                mem_usage = 0
                mem_limit = 0
                if state.get("Running"):
                    try:
                        stats1 = container.stats(decode=True, stream=False)
                        time.sleep(0.18)
                        stats2 = container.stats(decode=True, stream=False)
                        cpu_usage = _cpu_percent_between(stats1, stats2)
                        if cpu_usage == 0.0:
                            cpu_usage = _cpu_percent(stats2)
                        mem_stats = stats2.get("memory_stats") or {}
                        mem_usage = int(mem_stats.get("usage") or 0)
                        mem_limit = int(mem_stats.get("limit") or 0)
                        if mem_limit == 0 and docker_mem_cap > 0:
                            mem_limit = docker_mem_cap
                    except (APIError, DockerException, KeyError, TypeError) as exc:
                        self._logger.debug("stats for %s: %s", short_id, exc)

                created = attrs.get("Created") or ""
                out.append(
                    {
                        "id": short_id,
                        "name": name or short_id,
                        "image": image,
                        "status": status,
                        "restart_count": restart_count,
                        "cpu_usage": cpu_usage,
                        "memory_usage": int(round(mem_usage / (1024 * 1024))) if mem_usage else 0,
                        "memory_limit": int(round(mem_limit / (1024 * 1024))) if mem_limit else 0,
                        "ports": _ports_str(attrs),
                        "uptime": _uptime_from_started(started) if state.get("Running") else "—",
                        "created_at": created,
                        "network_mode": network_mode or None,
                    }
                )
        except DockerException as exc:
            return [], str(exc)
        return out, None

    def resolve_container_id(self, ref: str) -> str:
        """Resolve short (12-char) or name to full id."""
        client, err = self._connect()
        if err or client is None:
            raise RuntimeError(err or "docker_unavailable")
        ref = ref.strip()
        if not ref:
            raise ValueError("empty_container_ref")
        try:
            c = client.containers.get(ref)
            return c.id
        except NotFound:
            pass
        except APIError as exc:
            raise RuntimeError(str(exc)) from exc
        for container in client.containers.list(all=True):
            cid = container.id
            if cid.startswith(ref) or (container.name or "").lstrip("/") == ref:
                return cid
        raise RuntimeError(f"no_such_container:{ref}")

    def create_container(
        self,
        image: str,
        name: str | None = None,
        ports: list[str] | None = None,
    ) -> str:
        client, err = self._connect()
        if err or client is None:
            raise RuntimeError(err or "docker_unavailable")
        try:
            client.images.pull(image)
        except ImageNotFound as exc:
            raise RuntimeError(f"image_not_found:{image}") from exc
        except APIError as exc:
            raise RuntimeError(f"image_pull_failed:{exc}") from exc
        kwargs: dict[str, Any] = {"detach": True}
        if name and re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,251}$", name):
            kwargs["name"] = name
        pb = _parse_port_bindings(ports or [])
        if pb:
            kwargs["ports"] = pb
        try:
            container = client.containers.run(image, **kwargs)
            return container.id[:12]
        except APIError as exc:
            raise RuntimeError(str(exc)) from exc

    def start(self, ref: str) -> None:
        c = self._get(ref)
        c.start()

    def stop(self, ref: str, *, timeout: int = 10) -> None:
        c = self._get(ref)
        c.stop(timeout=timeout)

    def restart(self, ref: str, *, timeout: int = 10) -> None:
        c = self._get(ref)
        c.restart(timeout=timeout)

    def remove(self, ref: str) -> None:
        c = self._get(ref)
        c.remove(force=True)

    def logs_text(self, ref: str, *, tail: int = 500) -> str:
        c = self._get(ref)
        raw = c.logs(tail=tail, timestamps=True)
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return str(raw)

    def _get(self, ref: str) -> Any:
        client, err = self._connect()
        if err or client is None:
            raise RuntimeError(err or "docker_unavailable")
        full = self.resolve_container_id(ref)
        try:
            return client.containers.get(full)
        except NotFound as exc:
            raise RuntimeError(f"container_not_found:{ref}") from exc


_docker_singleton: DockerManager | None = None


def get_docker_manager(logger: logging.Logger) -> DockerManager:
    global _docker_singleton
    if _docker_singleton is None:
        _docker_singleton = DockerManager(logger)
    return _docker_singleton
