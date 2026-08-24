"""src/logger.py — non-blocking logging via a background listener thread."""
import atexit
import logging
import os
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from queue import Queue
from typing import Optional

from config import LOG_DIR, LOG_FILE, LOG_LEVEL

_listener: Optional[QueueListener] = None
_log_queue: Optional[Queue] = None


def _ensure_listener() -> Queue:
    global _listener, _log_queue

    if _log_queue is not None:
        return _log_queue

    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)

    _log_queue = Queue(-1)
    _listener = QueueListener(
        _log_queue,
        file_handler,
        console_handler,
        respect_handler_level=True,
    )
    _listener.start()
    atexit.register(_listener.stop)

    return _log_queue


def get_logger(name: str) -> logging.Logger:
    queue = _ensure_listener()
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    logger.addHandler(QueueHandler(queue))
    logger.propagate = False

    return logger
