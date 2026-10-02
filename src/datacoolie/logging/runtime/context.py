"""Thread-safe dataflow execution context propagation via :mod:`contextvars`.

Stores the current ``dataflow_id`` and execution-instance ``dataflow_run_id``
in :class:`contextvars.ContextVar` values so every log record emitted on the
same thread automatically includes the active scope — without changes to the
many modules that call ``logger.info(…)``.

Usage in driver code::

    tokens = set_dataflow_context(dataflow.dataflow_id, dataflow_run_id)
    try:
        ...  # all logging here carries both execution identifiers
    finally:
        clear_dataflow_context(tokens)
"""

from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager
from typing import Iterator, Optional

_dataflow_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "dataflow_id", default=""
)
_dataflow_run_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "dataflow_run_id", default=""
)


def set_dataflow_id(dataflow_id: str) -> contextvars.Token[str]:
    """Set the current dataflow ID and return a reset token."""
    return _dataflow_id_var.set(dataflow_id)


def get_dataflow_id() -> str:
    """Return the current dataflow ID (empty string when unset)."""
    return _dataflow_id_var.get()


def clear_dataflow_id(token: contextvars.Token[str]) -> None:
    """Restore the previous dataflow ID value."""
    _dataflow_id_var.reset(token)


def set_dataflow_run_id(dataflow_run_id: str) -> contextvars.Token[str]:
    """Set the current execution-instance identifier and return a reset token."""
    return _dataflow_run_id_var.set(dataflow_run_id)


def get_dataflow_run_id() -> str:
    """Return the current execution-instance identifier, if one is bound."""
    return _dataflow_run_id_var.get()


def clear_dataflow_run_id(token: contextvars.Token[str]) -> None:
    """Restore the previous execution-instance identifier."""
    _dataflow_run_id_var.reset(token)


def set_dataflow_context(
    dataflow_id: Optional[str],
    dataflow_run_id: Optional[str],
) -> tuple[contextvars.Token[str], contextvars.Token[str]]:
    """Bind one dataflow scope and return tokens for exact restoration."""
    return (
        set_dataflow_id(dataflow_id or ""),
        set_dataflow_run_id(dataflow_run_id or ""),
    )


def clear_dataflow_context(
    tokens: tuple[contextvars.Token[str], contextvars.Token[str]],
) -> None:
    """Restore a context returned by :func:`set_dataflow_context`."""
    dataflow_token, run_token = tokens
    clear_dataflow_run_id(run_token)
    clear_dataflow_id(dataflow_token)


@contextmanager
def dataflow_context(
    dataflow_id: Optional[str],
    dataflow_run_id: Optional[str],
) -> Iterator[None]:
    """Temporarily bind both dataflow identifiers with parent restoration."""
    tokens = set_dataflow_context(dataflow_id, dataflow_run_id)
    try:
        yield
    finally:
        clear_dataflow_context(tokens)


class DataflowContextFilter(logging.Filter):
    """Attach current dataflow execution identifiers to a Python log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.dataflow_id = get_dataflow_id()
        record.dataflow_run_id = get_dataflow_run_id()
        return True
