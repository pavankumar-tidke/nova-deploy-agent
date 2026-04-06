"""Resolve Docker CLI binary and verify it is usable (PATH vs /usr/bin/docker)."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

# Per-registry helpers that require Docker Desktop or a desktop keychain (break headless nodes).
_HEADLESS_DISALLOWED_HELPERS = frozenset(
    {
        "desktop",
        "osxkeychain",
        "wincred",
    }
)

_CONFIG_KEYS_TO_STRIP = frozenset({"credsStore", "credStore"})


def docker_config_dir() -> Path:
    """Directory for Docker client config (~/.docker or DOCKER_CONFIG)."""
    raw = (os.environ.get("DOCKER_CONFIG") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.home() / ".docker").resolve()


def sanitize_docker_config_for_headless(logger: logging.Logger | None = None) -> bool:
    """
    Ensure ~/.docker/config.json does not use Docker Desktop or other broken credential helpers.

    Removes credsStore / credStore and scrubs credHelpers entries that point at desktop/keychain
    helpers. Preserves ``auths`` so ``docker login`` data can remain.

    Returns True if the config file was created or modified.
    """
    log = logger or logging.getLogger(__name__)
    ddir = docker_config_dir()
    path = ddir / "config.json"
    ddir.mkdir(mode=0o700, parents=True, exist_ok=True)

    data: dict[str, Any] = {}
    if path.is_file():
        try:
            raw = path.read_text(encoding="utf-8")
            parsed = json.loads(raw) if raw.strip() else {}
            data = parsed if isinstance(parsed, dict) else {}
        except (OSError, json.JSONDecodeError):
            data = {}

    before = json.dumps(data, sort_keys=True)

    for k in _CONFIG_KEYS_TO_STRIP:
        if k in data:
            del data[k]

    ch = data.get("credHelpers")
    if isinstance(ch, dict):
        new_h = {reg: h for reg, h in ch.items() if str(h).strip().lower() not in _HEADLESS_DISALLOWED_HELPERS}
        if new_h != ch:
            if new_h:
                data["credHelpers"] = new_h
            else:
                data.pop("credHelpers", None)

    if "auths" not in data or not isinstance(data.get("auths"), dict):
        data["auths"] = {}

    after = json.dumps(data, sort_keys=True)
    if after == before and path.is_file():
        return False

    try:
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        path.chmod(0o600)
    except OSError as exc:
        log.warning("Could not write Docker config at %s: %s", path, exc)
        return False
    log.info("Sanitized Docker config for headless environment (%s)", path)
    return True


def resolve_docker_executable() -> str | None:
    which = shutil.which("docker")
    if which:
        return which
    for candidate in ("/usr/bin/docker", "/usr/local/bin/docker"):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def check_docker_cli() -> tuple[bool, str]:
    """Return (ok, error_message). ok True means `docker --version` succeeded."""
    exe = resolve_docker_executable()
    if not exe:
        return False, "Docker is not installed or not accessible on this node (not in PATH)."
    try:
        proc = subprocess.run(
            [exe, "--version"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode == 0:
            return True, ""
        err = (proc.stderr or proc.stdout or "").strip()
        return False, err or "docker --version failed"
    except FileNotFoundError:
        return False, f"Docker executable missing: {exe}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def docker_daemon_reachable() -> tuple[bool, str]:
    """Best-effort `docker info` (may require user in docker group)."""
    exe = resolve_docker_executable()
    if not exe:
        return False, "docker not found"
    try:
        proc = subprocess.run(
            [exe, "info"],
            capture_output=True,
            text=True,
            timeout=45,
        )
        if proc.returncode == 0:
            return True, ""
        err = (proc.stderr or proc.stdout or "").strip()
        return False, err[:2000] if err else "docker info failed"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _short_docker_version(version_line: str) -> str | None:
    """Extract '24.0.7' from 'Docker version 24.0.7, build ...'."""
    line = (version_line or "").strip()
    if not line:
        return None
    parts = line.replace(",", " ").split()
    for i, p in enumerate(parts):
        if p.lower() == "version" and i + 1 < len(parts):
            ver = parts[i + 1].strip()
            return ver or None
    return line[:64] if line else None


def docker_health_for_heartbeat() -> dict[str, Any]:
    """
    Product logic: `docker --version` failure → not installed.
    If installed, `docker info` failure → not running.
    """
    exe = resolve_docker_executable()
    if not exe:
        return {"installed": False, "running": False, "version": None}
    version_line = ""
    try:
        proc = subprocess.run(
            [exe, "--version"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode != 0:
            return {"installed": False, "running": False, "version": None}
        version_line = (proc.stdout or proc.stderr or "").strip()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return {"installed": False, "running": False, "version": None}

    short_ver = _short_docker_version(version_line)
    try:
        inf = subprocess.run(
            [exe, "info"],
            capture_output=True,
            text=True,
            timeout=45,
        )
        running = inf.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        running = False

    return {"installed": True, "running": running, "version": short_ver}


def docker_capabilities_snapshot() -> dict[str, Any]:
    """For metrics POST: camelCase flags + optional version for backend."""
    h = docker_health_for_heartbeat()
    out: dict[str, Any] = {
        "dockerInstalled": bool(h.get("installed")),
        "dockerRunning": bool(h.get("installed")) and bool(h.get("running")),
    }
    ver = h.get("version")
    if isinstance(ver, str) and ver.strip():
        out["dockerVersion"] = ver.strip()
    return out
