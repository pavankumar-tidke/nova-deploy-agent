from __future__ import annotations

import asyncio
import logging
import signal
from datetime import UTC, datetime
from typing import Any
from config import ConfigError, load_config, nodehost_home
from executor import CommandExecutor
from log_sync import LogSync, start_log_sync_background
from logger import setup_logger, trim_local_logs_max_age_hours
from command_poll import run_command_poll_loop
from container_sync import run_container_sync_loop
from deployment_job_poll import run_deployment_job_loop
from docker_heartbeat_loop import run_docker_heartbeat_loop
from metrics_push_loop import run_metrics_push_loop
from system_monitor import SystemMonitor
from terminal_session import terminal_poll_loop
from version import AGENT_VERSION
from websocket_client import NodeWebSocketClient


class _BackendLogHandler(logging.Handler):
    def __init__(self, log_sync: LogSync) -> None:
        super().__init__(level=logging.INFO)
        self._log_sync = log_sync

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._log_sync.enqueue(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "level": record.levelname,
                    "message": self.format(record),
                    "type": "system",
                }
            )
        except Exception:
            pass


async def run() -> int:
    logger = setup_logger()

    try:
        cfg = load_config()
    except ConfigError as exc:
        logger.error("Config error: %s", exc)
        return 1

    logger.info("NodeHost agent %s starting", AGENT_VERSION)

    log_sync = LogSync(logger)
    log_handler = _BackendLogHandler(log_sync)
    log_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(log_handler)

    trim_local_logs_max_age_hours(
        nodehost_home() / "logs" / "agent.log",
        max_age_hours=cfg.log_retention_hours,
    )

    def get_cfg_for_sync() -> Any:
        return load_config()

    def load_cfg_fresh() -> Any:
        return load_config()

    _t, log_stop = start_log_sync_background(get_cfg_for_sync, log_sync, interval_sec=25.0)

    monitor = SystemMonitor(region=cfg.region)
    executor = CommandExecutor(logger)

    async def on_message(message: dict[str, Any]) -> None:
        msg_type = str(message.get("type", "")).strip()
        if msg_type in ("command", "action"):
            await executor.execute(message)
        elif msg_type == "auth":
            return
        else:
            logger.info("Unhandled message type: %s", msg_type or "unknown")

    client = NodeWebSocketClient(
        config=cfg,
        logger=logger,
        on_message=on_message,
        heartbeat_provider=monitor.collect,
        heartbeat_wait=monitor.wait_next,
        load_config_fn=load_cfg_fresh,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop() -> None:
        if not stop_event.is_set():
            logger.info("Shutdown signal received")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _stop())

    stop_terminal = asyncio.Event()
    client_task = asyncio.create_task(client.run_forever())
    terminal_task = asyncio.create_task(
        terminal_poll_loop(logger, load_cfg_fresh, stop_terminal, interval_sec=2.0),
    )
    command_poll_task = asyncio.create_task(
        run_command_poll_loop(logger, load_cfg_fresh, stop_event, interval_sec=4.0),
    )
    container_sync_task = asyncio.create_task(
        run_container_sync_loop(logger, load_cfg_fresh, stop_event, interval_sec=12.0),
    )
    deployment_job_task = asyncio.create_task(
        run_deployment_job_loop(logger, load_cfg_fresh, stop_event, interval_sec=6.0),
    )
    metrics_push_task = asyncio.create_task(
        run_metrics_push_loop(logger, load_cfg_fresh, stop_event, interval_sec=2.0),
    )
    docker_hb_task = asyncio.create_task(
        run_docker_heartbeat_loop(logger, load_cfg_fresh, stop_event, interval_sec=10.0),
    )
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        [
            client_task,
            stop_task,
            command_poll_task,
            container_sync_task,
            deployment_job_task,
            metrics_push_task,
            docker_hb_task,
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )

    log_stop.set()
    stop_terminal.set()
    await client.stop()
    try:
        await asyncio.wait_for(terminal_task, timeout=8.0)
    except asyncio.TimeoutError:
        terminal_task.cancel()
        await asyncio.gather(terminal_task, return_exceptions=True)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if not command_poll_task.done():
        command_poll_task.cancel()
        await asyncio.gather(command_poll_task, return_exceptions=True)
    if not container_sync_task.done():
        container_sync_task.cancel()
        await asyncio.gather(container_sync_task, return_exceptions=True)
    if not deployment_job_task.done():
        deployment_job_task.cancel()
        await asyncio.gather(deployment_job_task, return_exceptions=True)
    if not metrics_push_task.done():
        metrics_push_task.cancel()
        await asyncio.gather(metrics_push_task, return_exceptions=True)
    if not docker_hb_task.done():
        docker_hb_task.cancel()
        await asyncio.gather(docker_hb_task, return_exceptions=True)
    await asyncio.gather(client_task, return_exceptions=True)
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()
