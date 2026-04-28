"""Thread-safe dataflow context propagation via :mod:`contextvars`.

Stores the current ``dataflow_id`` in a :class:`contextvars.ContextVar` so that
every log record emitted on the same thread automatically includes it — without
any changes to the 28+ modules that call ``logger.info(…)``.

Usage in driver code::

    token = set_dataflow_id(dataflow.dataflow_id)
    try:
        ...  # all logging here will carry the dataflow_id
    finally:
        clear_dataflow_id(token)
"""

from __future__ import annotations

import contextvars

_dataflow_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "dataflow_id", default=""
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
