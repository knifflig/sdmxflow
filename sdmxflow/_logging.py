from __future__ import annotations

import logging


def get_logger(logger: logging.Logger | None, *, name: str = "sdmxflow") -> logging.Logger:
    """Return a usable stdlib logger.

    The package never configures handlers/formatters; the host application owns that.
    """
    return logger if logger is not None else logging.getLogger(name)
