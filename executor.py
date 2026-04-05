import logging
from typing import Any


class CommandExecutor:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    async def execute(self, message: dict[str, Any]) -> None:
        action = str(message.get("action", "")).strip()
        self._logger.info("Command received: action=%s payload=%s", action or "unknown", message)
        # Placeholder for future command handling (deploy, logs, updates, etc.).
