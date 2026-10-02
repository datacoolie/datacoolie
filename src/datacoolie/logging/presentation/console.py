"""Small dependency-free console color support.

The console formatter is intentionally separate from the capture formatter.
Python logging reuses one ``LogRecord`` across handlers, so formatting a copy
prevents ANSI level names, presentation fields, or cached exception text from
leaking into persisted JSON records.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional, TextIO

from datacoolie.logging.configuration.constants import ConsoleColor
from datacoolie.logging.presentation.formatting import ContextFormatter, prepare_record

_RESET = "\x1b[0m"
_DIM = "\x1b[2m"
_CYAN = "\x1b[36m"
_YELLOW = "\x1b[33m"
_RED = "\x1b[31m"
_BOLD_RED = "\x1b[1;31m"


def should_colorize(mode: str, stream: Optional[TextIO] = None) -> bool:
    """Resolve ``auto`` using standard terminal conventions."""
    if mode == ConsoleColor.ALWAYS.value:
        return True
    if mode == ConsoleColor.NEVER.value:
        return False
    if os.environ.get("NO_COLOR") is not None:
        return False
    if os.environ.get("TERM", "").lower() == "dumb":
        return False
    stream = stream or sys.stderr
    try:
        if bool(stream.isatty()):
            return True
    except (AttributeError, OSError):
        pass
    # Notebook streams often expose a display-capable ``isatty``-less object.
    # Only opt in when it advertises an explicit ANSI/display capability.
    return bool(
        getattr(stream, "supports_color", False)
        or getattr(stream, "isatty", False) is True
    )


class ConsoleFormatter(ContextFormatter):
    """Formatter that colors only the level label on a copied record."""

    _colors = {
        "DEBUG": _DIM,
        "INFO": _CYAN,
        "WARNING": _YELLOW,
        "ERROR": _RED,
        "CRITICAL": _BOLD_RED,
    }

    def __init__(
        self,
        fmt: Optional[str] = None,
        *,
        colorize: bool = False,
        datefmt: Optional[str] = None,
        style: str = "%",
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.colorize = colorize

    def format(self, record: logging.LogRecord) -> str:
        isolated = prepare_record(record)
        level = str(record.levelname)
        prefix = self._colors.get(level)
        if self.colorize and prefix:
            isolated.levelname = f"{prefix}{level}{_RESET}"
        # Call the stdlib implementation directly because ContextFormatter's
        # format method would create a second copy.  ``isolated`` already has
        # safe optional fields and a clean exception cache.
        return logging.Formatter.format(self, isolated)
