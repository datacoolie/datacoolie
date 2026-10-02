"""Small helpers for logging at failure-sensitive lifecycle boundaries.

Most framework code should use the standard :mod:`logging` API directly.  A
caller needs this helper only when a logging handler failure must not replace a
business exception or prevent a surrounding cleanup ``finally`` block.
"""

from __future__ import annotations

import logging
from typing import Any, Optional


def emit_safely(
    logger: logging.Logger,
    level: int,
    message: object,
    *args: Any,
    catch_base: bool = False,
    **kwargs: Any,
) -> Optional[BaseException]:
    """Emit one record and return a handler error when it cannot be emitted.

    ``Logger.log`` keeps the normal logging API and avoids string-based method
    dispatch.  ``stacklevel`` points the resulting record at the caller of
    this helper.  Ordinary handler exceptions are always isolated.  Lifecycle
    callers may opt into returning ``KeyboardInterrupt``/``SystemExit`` too;
    they then decide whether a primary exception or teardown policy wins.
    """

    kwargs.setdefault("stacklevel", 2)
    try:
        logger.log(level, message, *args, **kwargs)
    except Exception as exc:
        return exc
    except BaseException as exc:
        if catch_base:
            return exc
        raise
    return None


__all__ = ["emit_safely"]
