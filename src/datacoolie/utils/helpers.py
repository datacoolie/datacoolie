"""General-purpose helper functions for the DataCoolie framework."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Identity & timestamp helpers
# ---------------------------------------------------------------------------


# Fixed namespace for deterministic name-based UUIDs within DataCoolie.
_DATACOOLIE_NS = uuid.UUID("da7ac001-e000-4000-8000-000000000000")


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


def utc_now() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Collection helpers
# ---------------------------------------------------------------------------


def ensure_list(value: Any) -> list[Any]:
    """Coerce *value* to a ``list``.

    * ``None`` → ``[]``
    * Already a ``list`` → returned as-is.
    * Comma-separated ``str`` → split + stripped.
    * JSON array ``str`` → parsed.

    Args:
        value: Value to convert.

    Returns:
        List representation.
    """
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        # JSON array
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
        # Comma-separated
        return [part.strip() for part in stripped.split(",") if part.strip()]

    # Single scalar → wrap
    return [value]


def chunk_list(lst: list[Any], chunk_size: int) -> list[list[Any]]:
    """Split *lst* into sublists of at most *chunk_size* elements.

    Args:
        lst: List to split.
        chunk_size: Maximum chunk size (must be > 0).

    Returns:
        List of chunks.

    Raises:
        ValueError: If *chunk_size* is not positive.
    """
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


# ---------------------------------------------------------------------------
# Dict helpers
# ---------------------------------------------------------------------------


def merge_dicts(*dicts: dict[str, Any] | None, deep: bool = True) -> dict[str, Any]:
    """Merge multiple dictionaries (later values win).

    Args:
        *dicts: Dictionaries to merge (``None`` entries are skipped).
        deep: Recursively merge nested dicts when ``True``.

    Returns:
        Merged dictionary.
    """
    result: dict[str, Any] = {}
    for d in dicts:
        if d is None:
            continue
        for key, value in d.items():
            if (
                deep
                and key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = merge_dicts(result[key], value, deep=True)
            else:
                result[key] = value
    return result


def flatten_dict(
    d: dict[str, Any],
    parent_key: str = "",
    sep: str = ".",
) -> dict[str, Any]:
    """Flatten a nested dictionary.

    Args:
        d: Dictionary to flatten.
        parent_key: Current key prefix (used internally during recursion).
        sep: Separator between keys.

    Returns:
        Flattened dictionary.
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)



