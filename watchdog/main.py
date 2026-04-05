"""NodeHost watchdog: control plane — backend connectivity, command polling, agent lifecycle."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

# Agent package root (…/agent)
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from colors_log import TtyColorFormatter  # noqa: E402
from config import ConfigError, load_config, nodehost_home  # noqa: E402
from http_client import NodeHttpClient  # noqa: E402
from intent_store import read_intent, runtime_dir, write_intent  # noqa: E402


def _setup_watchdog_logger() -> logging.Logger:
    log = logging.getLogger("nodehost-watchdog")
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    plain = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log_dir = nodehost_home() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "watchdog.log"
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(plain)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(
        TtyColorFormatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stderr,
        )
    )
    log.addHandler(fh)
    log.addHandler(sh)
    return log


def _pid_path() -> Path:
    return runtime_dir() / "agent.pid"


def _agent_paths() -> tuple[Path, Path]:
    home = nodehost_home()
    venv_py = home / "agent" / ".venv" / "bin" / "python"
    if os.name == "nt":
        venv_py = home / "agent" / ".venv" / "Scripts" / "python.exe"
    main_py = home / "agent" / "main.py"
    return venv_py, main_py


def _read_pid() -> int | None:
    p = _pid_path()
    if not p.exists():
        return None
    try:
        return int(p.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _write_pid(pid: int) -> None:
    pp = _pid_path()
    pp.write_text(str(pid) + "\n", encoding="utf-8")
    try:
        os.chmod(pp, 0o600)
    except OSError:
        pass


def _clear_pid() -> None:
    try:
        _pid_path().unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


def is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def stop_agent_process(logger: logging.Logger) -> None:
    pid = _read_pid()
    if pid is None or not is_pid_alive(pid):
        _clear_pid()
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        logger.warning("SIGTERM agent pid=%s: %s", pid, exc)
    deadline = time.time() + 25.0
    while time.time() < deadline and is_pid_alive(pid):
        time.sleep(0.2)
    if is_pid_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    _clear_pid()


def start_agent_process(logger: logging.Logger, cfg_path: Path) -> bool:
    stop_agent_process(logger)
    venv_py, main_py = _agent_paths()
    if not venv_py.is_file() or not main_py.is_file():
        logger.error("Agent not found at %s / %s", venv_py, main_py)
        return False
    log_path = nodehost_home() / "logs" / "agent.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["NODEHOST_HOME"] = str(nodehost_home())
    env["NODEHOST_AGENT_CONFIG"] = str(cfg_path)
    lf = open(log_path, "a", encoding="utf-8", buffering=1)
    try:
        proc = subprocess.Popen(
            [str(venv_py), str(main_py)],
            cwd=str(_ROOT),
            env=env,
            stdout=lf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        logger.error("Failed to start agent: %s", exc)
        lf.close()
        return False
    _write_pid(proc.pid)
    logger.info("Agent started pid=%s", proc.pid)
    return True


def _compute_error_backoff_seconds(fail_streak: int) -> float:
    """1s base, doubles each failure, capped at 30s (production-friendly)."""
    if fail_streak <= 0:
        return 1.0
    return min(30.0, 1.0 * (2 ** min(fail_streak - 1, 5)))


def _internal_base(backend_url: str, api_prefix: str) -> str:
    b = backend_url.rstrip("/")
    p = (api_prefix or "").strip()
    if p and not p.startswith("/"):
        p = f"/{p}"
    return f"{b}{p}/internal"


class Watchdog:
    """Control plane state: CONNECTED | DISCONNECTED | RECONNECTING (logged)."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._http = NodeHttpClient(logger)
        self._stop = asyncio.Event()
        self._fail_streak = 0
        self._token_reject_streak = 0
        self._token_reject_logged = False
        self._link_state = "DISCONNECTED"
        self._last_success_wall: float | None = None

    def _set_link_state(self, state: str) -> None:
        if state == self._link_state:
            return
        self._logger.info("Control plane: %s → %s", self._link_state, state)
        self._link_state = state

    def _urls(self, cfg: Any) -> tuple[str, str]:
        base = _internal_base(cfg.backend_url, cfg.api_prefix)
        return (
            f"{base}/nodes/watchdog/heartbeat",
            f"{base}/nodes/commands",
            f"{base}/nodes/command-result",
        )

    def _post_heartbeat_sync(self, cfg: Any) -> int:
        url, _, _ = self._urls(cfg)
        code, body = self._http.post_json(url, {}, bearer=cfg.token, timeout=20.0)
        ok = code == 200 and bool(body.get("success", True))
        if not ok and code != 401:
            self._logger.warning("Watchdog heartbeat failed: HTTP %s body=%s", code, body)
        return code

    def _poll_commands_sync(self, cfg: Any) -> tuple[list[dict[str, Any]], int]:
        _, url, _ = self._urls(cfg)
        code, body = self._http.get_json(url, bearer=cfg.token, timeout=30.0)
        if code != 200:
            if code == 403:
                self._logger.warning(
                    "Command poll forbidden (403). Set NODEHOST_INTERNAL_API_KEY to match backend INTERNAL_API_KEY."
                )
            elif code != 401:
                self._logger.warning("Command poll failed: HTTP %s", code)
            return [], code
        data = body.get("data")
        if not isinstance(data, list):
            return [], code
        return data, code

    def _post_result_sync(self, cfg: Any, command_id: str, status: str, detail: str | None, payload: dict | None) -> None:
        _, _, url = self._urls(cfg)
        body = {
            "command_id": command_id,
            "status": status,
            "detail": detail,
            "payload": payload or {},
        }
        code, resp = self._http.post_json(url, body, bearer=cfg.token, timeout=30.0)
        if code != 200:
            self._logger.warning("command-result failed: HTTP %s %s", code, resp)

    def _handle_one_command(self, cfg: Any, cmd: dict[str, Any], cfg_path: Path) -> None:
        cid = str(cmd.get("id") or "")
        ctype = str(cmd.get("type") or "").strip().upper()
        if not cid:
            return
        self._logger.info("Command %s %s", cid, ctype)
        detail: str | None = None
        extra: dict[str, Any] | None = None
        ok = True
        try:
            if ctype == "START_AGENT":
                write_intent("run")
                ok = start_agent_process(self._logger, cfg_path)
                detail = None if ok else "start_failed"
            elif ctype == "STOP_AGENT":
                write_intent("stop")
                stop_agent_process(self._logger)
                ok = True
            elif ctype == "RESTART_AGENT":
                write_intent("run")
                stop_agent_process(self._logger)
                ok = start_agent_process(self._logger, cfg_path)
                detail = None if ok else "restart_failed"
            elif ctype == "GET_STATUS":
                pid = _read_pid()
                running = pid is not None and is_pid_alive(pid)
                extra = {
                    "agent_running": running,
                    "pid": pid,
                    "intent": read_intent(),
                }
            else:
                ok = False
                detail = f"unknown_command:{ctype}"
        except Exception as exc:  # noqa: BLE001
            ok = False
            detail = str(exc)
        self._post_result_sync(cfg, cid, "executed" if ok else "failed", detail, extra)

    def _ensure_agent_by_intent(self, cfg_path: Path) -> None:
        intent = read_intent()
        pid = _read_pid()
        running = pid is not None and is_pid_alive(pid)
        if intent == "run" and not running:
            self._logger.warning("Agent not running (intent=run); restarting")
            start_agent_process(self._logger, cfg_path)
        elif intent == "stop" and running:
            stop_agent_process(self._logger)

    async def _backoff_on_token_rejected(self, cfg_path: Path) -> None:
        """Stop agent and wait; do not spam logs on every retry."""
        self._token_reject_streak += 1
        await asyncio.to_thread(stop_agent_process, self._logger)
        delay = min(3600.0, 60.0 * (2 ** min(self._token_reject_streak, 6)))
        if not self._token_reject_logged:
            self._logger.warning(
                "Backend rejected agent token (wrong token, node deleted, or token renewed). "
                "Stopped local agent. Update token in ~/.nodehost/config/config.json and restart the watchdog."
            )
            self._token_reject_logged = True
        else:
            self._logger.info(
                "Token still rejected (streak=%s); next retry in %.0fs.",
                self._token_reject_streak,
                delay,
            )
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    async def run(self) -> None:
        try:
            cfg = load_config()
        except ConfigError as exc:
            self._logger.error("Config error: %s", exc)
            return
        cfg_path = Path(os.environ.get("NODEHOST_AGENT_CONFIG", "")).expanduser()
        if not cfg_path.is_file():
            cfg_path = Path.home() / ".nodehost" / "config" / "config.json"

        hb_url, _, _ = self._urls(cfg)
        self._logger.info("Watchdog up; backend=%s", hb_url)

        while not self._stop.is_set():
            now_wall = time.time()
            if self._last_success_wall is not None and (now_wall - self._last_success_wall) > 90.0:
                gap = now_wall - self._last_success_wall
                self._logger.info(
                    "Watchdog: %.0fs since last successful backend sync (sleep, network, or DB outage). "
                    "Resetting error backoff; enforcing agent intent.",
                    gap,
                )
                self._fail_streak = 0
            try:
                try:
                    cfg = load_config()
                except ConfigError as ce:
                    self._logger.warning("load_config failed: %s", ce)
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
                    continue
                hb_code = await asyncio.to_thread(self._post_heartbeat_sync, cfg)
                if hb_code == 401:
                    self._set_link_state("DISCONNECTED")
                    await self._backoff_on_token_rejected(cfg_path)
                    continue
                if hb_code != 200:
                    raise RuntimeError(f"watchdog heartbeat HTTP {hb_code}")

                cmds, poll_code = await asyncio.to_thread(self._poll_commands_sync, cfg)
                if poll_code == 401:
                    self._set_link_state("DISCONNECTED")
                    await self._backoff_on_token_rejected(cfg_path)
                    continue
                if poll_code != 200:
                    raise RuntimeError(f"command poll HTTP {poll_code}")

                self._token_reject_streak = 0
                self._token_reject_logged = False
                self._fail_streak = 0
                self._set_link_state("CONNECTED")
                self._last_success_wall = time.time()
                for c in cmds:
                    try:
                        await asyncio.to_thread(self._handle_one_command, cfg, c, cfg_path)
                    except Exception as cmd_exc:  # noqa: BLE001
                        self._logger.error("Command handler failed: %s", cmd_exc)
                try:
                    await asyncio.to_thread(self._ensure_agent_by_intent, cfg_path)
                except Exception as ex:  # noqa: BLE001
                    self._logger.error("ensure_agent_by_intent failed: %s", ex)
            except Exception as exc:  # noqa: BLE001
                self._fail_streak += 1
                delay = _compute_error_backoff_seconds(self._fail_streak)
                self._set_link_state("RECONNECTING")
                self._logger.warning(
                    "Watchdog loop error — %s (attempt %s, backoff %.1fs): %s",
                    self._link_state,
                    self._fail_streak,
                    delay,
                    exc,
                )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=4.0)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        self._stop.set()


async def _run() -> int:
    logger = _setup_watchdog_logger()
    wd = Watchdog(logger)
    loop = asyncio.get_running_loop()

    def _graceful() -> None:
        logger.info("Shutdown signal")
        wd.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _graceful())

    await wd.run()
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
