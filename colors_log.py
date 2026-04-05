"""TTY color helpers for logging (zsh-like). Plain text when not a terminal or for file handlers."""

from __future__ import annotations

import logging
import sys
from typing import IO, TextIO


class TtyColorFormatter(logging.Formatter):
    """Color full log lines by level on TTY streams; unchanged for files."""

    RESET = "\033[0m"
    COLORS = {
        logging.DEBUG: "\033[36m",  # cyan
        logging.INFO: "\033[32m",  # green
        logging.WARNING: "\033[33m",  # yellow
        logging.ERROR: "\033[31m",  # red
        logging.CRITICAL: "\033[35m",  # magenta
    }

    def __init__(
        self,
        fmt: str,
        datefmt: str | None = None,
        *,
        stream: TextIO | None = None,
    ) -> None:
        super().__init__(fmt, datefmt)
        self._stream: IO[str] = stream or sys.stderr

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        try:
            if not self._stream.isatty():
                return text
        except (AttributeError, ValueError):
            return text
        c = self.COLORS.get(record.levelno)
        if not c:
            return text
        return f"{c}{text}{self.RESET}"
