"""JSON record-path helpers for API sources."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

def extract_records(
    data: Any,
    data_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract the records list from the API response JSON.

    If *data_path* is provided (e.g. ``"data.items"``), drills into
    the nested response. Otherwise expects a top-level list.
    """
    target = data if data_path is None else resolve_path(data, data_path)

    if isinstance(target, list):
        return target
    if isinstance(target, dict):
        return [target]
    return []

def resolve_path(data: Any, path: str) -> Any:
    """Resolve a dot-separated path into a nested dict/list."""
    current = data
    for key in path.split("."):
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and key.isdigit():
            idx = int(key)
            current = current[idx] if idx < len(current) else None
        else:
            return None
        if current is None:
            return None
    return current

__all__ = ["extract_records", "resolve_path"]
