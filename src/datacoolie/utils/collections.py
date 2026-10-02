"""Collection and mapping helpers."""

from __future__ import annotations

import json
from typing import Any


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
