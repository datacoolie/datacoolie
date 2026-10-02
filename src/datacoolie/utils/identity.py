"""Stable and generated identity helpers."""

from __future__ import annotations

import uuid


_DATACOOLIE_NS = uuid.UUID("da7ac001-e000-4000-8000-000000000000")


def is_usable_identifier(value: object) -> bool:
    """Return whether *value* can be used as an authored identity.

    Identity fields are strings in the public metadata contract.  Treating
    whitespace-only values as absent prevents a document from receiving a
    deterministic ID for a label that cannot be resolved by a user.
    """

    return isinstance(value, str) and bool(value.strip())


def generate_unique_id(prefix: str = "") -> str:
    """Generate a UUID-4 string, optionally prefixed.

    Args:
        prefix: Optional prefix separated by ``_``.

    Returns:
        Unique identifier string.
    """
    uid = str(uuid.uuid4())
    return f"{prefix}_{uid}" if prefix else uid


def name_to_uuid(name: str) -> str:
    """Derive a deterministic UUID-5 from *name*.

    The same *name* always produces the same UUID, enabling stable IDs when
    only a human-readable name is available.
    """
    return str(uuid.uuid5(_DATACOOLIE_NS, name))
