"""Semantic identity checks shared by document mapping and providers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from datacoolie.utils.identity import is_usable_identifier


def dataflow_identity_error(
    dataflow_id: object,
    name: object,
    *,
    label: str | None = None,
) -> str | None:
    """Return an actionable error when a dataflow has no usable identity."""

    if is_usable_identifier(dataflow_id) or is_usable_identifier(name):
        return None
    subject = f" '{label}'" if label else ""
    return (
        f"Dataflow{subject} must define a non-empty dataflow_id or name"
    )


def connection_identity_error(
    connection: Any,
    by_id: Mapping[str, Any],
    by_name: Mapping[str, Any],
    *,
    context: str,
) -> str | None:
    """Check one connection against an already-known metadata scope.

    An inline connection may repeat an existing ID/name pair.  An ID/name
    mismatch is rejected.  A different explicit ID may reuse a display name;
    later name-based resolution must then reject the ambiguous name and ask
    for ``connection_id``.  Workspace-aware name indexes remain a separate
    gated contract.
    """

    connection_id = getattr(connection, "connection_id", None)
    name = getattr(connection, "name", None)
    if not is_usable_identifier(connection_id):
        return f"{context} connection must have a non-empty connection_id"
    if not is_usable_identifier(name):
        return f"{context} connection must have a non-empty name"

    known_by_id = by_id.get(connection_id)
    if known_by_id is not None:
        expected_name = getattr(known_by_id, "name", None)
        if name != expected_name:
            return (
                f"{context} connection {connection_id} has conflicting name "
                f"{name!r}; expected {expected_name!r}"
            )
        return None

    # ``name`` is a display label, not a uniqueness constraint when an
    # explicit ID is present.  Keep all candidates in the name index so that
    # a later name-only reference fails deterministically as ambiguous.
    return None


__all__ = [
    "connection_identity_error",
    "dataflow_identity_error",
]
