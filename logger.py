import logging
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from colors_log import TtyColorFormatter
from config import nodehost_home


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("nodehost-agent")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    plain_fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_dir = nodehost_home() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "agent.log"

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(
        TtyColorFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stderr,
        )
    )

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=8 * 1024 * 1024,
        backupCount=4,
        encoding="utf-8",
    )
    file_handler.setFormatter(plain_fmt)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


def trim_local_logs_max_age_hours(log_path: Path, *, max_age_hours: int) -> None:
    """Best-effort trim of rotated log files older than retention window."""
    if max_age_hours <= 0:
        return
    cutoff = time.time() - max_age_hours * 3600
    parent = log_path.parent
    if not parent.exists():
        return
    for p in parent.glob("agent.log*"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink(missing_ok=True)
        except OSError:
            continue
