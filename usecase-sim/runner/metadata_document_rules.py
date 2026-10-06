"""Pure path and document validation for simulator metadata references."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Mapping


SIMULATOR_ROOT = Path(__file__).resolve().parents[1]
COMPILED_METADATA_DIRNAME = ".runtime/data/metadata"
_COMPOSITION_KEYS = frozenset(
    {"shared_metadata", "metadata_documents", "flow_order", "dataflows"}
)
_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:(?://|/)")


class MetadataCompositionError(ValueError):
    """Raised when a simulator metadata composition is malformed or unsafe."""


def _is_uri(value: object) -> bool:
    """Return whether *value* is a storage URI rather than a local path."""

    text = str(value)
    # Windows drive letters are paths, not URI schemes.  ``dbfs:/`` and
    # ``artifact:/`` are URI forms even though they have one slash.
    if re.match(r"^[A-Za-z]:[\\/]", text):
        return False
    return bool(_URI_RE.match(text))


def _root_path(root: str | os.PathLike[str] | None) -> Path:
    return Path(root if root is not None else SIMULATOR_ROOT).expanduser().resolve()


def _source_candidate(path: str | os.PathLike[str], root: Path) -> Path:
    """Resolve a caller path while accepting both product and simulator forms."""

    if _is_uri(path):
        raise MetadataCompositionError(
            f"Metadata composition requires a local JSON path, got URI: {path}"
        )
    raw = Path(path).expanduser()
    if raw.is_absolute():
        candidate = raw
    else:
        # Entrypoints are normally run from the product root and historically
        # receive ``usecase-sim/...`` paths.  Authoring references themselves
        # are simulator-root-relative, so accept the product prefix only at
        # this boundary and still apply the same root guard below.
        parts = raw.parts
        if parts and parts[0].casefold() == root.name.casefold():
            candidate = root.parent.joinpath(*parts)
        else:
            candidate = root / raw
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise MetadataCompositionError(
            f"Metadata path escapes simulator root {root}: {path}"
        ) from exc
    return resolved


def _read_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise
    except json.JSONDecodeError as exc:
        raise MetadataCompositionError(f"Invalid JSON metadata: {path}: {exc}") from exc


def _require_mapping(value: Any, path: Path) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetadataCompositionError(f"Metadata document must be a JSON object: {path}")
    return value


def _require_list(document: Mapping[str, Any], key: str, path: Path) -> list[Any]:
    value = document.get(key)
    if not isinstance(value, list):
        raise MetadataCompositionError(f"{path}: '{key}' must be a list")
    return value


def _flow_names(flows: list[Any], path: Path) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for index, flow in enumerate(flows):
        if not isinstance(flow, dict):
            raise MetadataCompositionError(
                f"{path}: dataflows[{index}] must be an object"
            )
        name = flow.get("name")
        if not isinstance(name, str) or not name:
            raise MetadataCompositionError(
                f"{path}: dataflows[{index}] requires a non-empty string name"
            )
        if name in seen:
            raise MetadataCompositionError(f"{path}: duplicate dataflow name: {name}")
        seen.add(name)
        names.append(name)
    return names


def _connection_key(connection: Mapping[str, Any], path: Path, index: int) -> str:
    name = connection.get("name")
    if not isinstance(name, str) or not name:
        raise MetadataCompositionError(
            f"{path}: connections[{index}] requires a non-empty string name"
        )
    return name


def _hint_group_key(group: Mapping[str, Any], path: Path, index: int) -> tuple[Any, ...]:
    if not isinstance(group, dict):
        raise MetadataCompositionError(f"{path}: schema_hints[{index}] must be an object")
    columns = group.get("hints", group.get("columns"))
    if not isinstance(columns, list):
        raise MetadataCompositionError(
            f"{path}: schema_hints[{index}] requires a hints or columns list"
        )
    identity = (
        group.get("connection_id"),
        group.get("connection_name"),
        group.get("schema_name"),
        group.get("table_name"),
    )
    seen_columns: set[str] = set()
    for column_index, column in enumerate(columns):
        if not isinstance(column, dict) or not isinstance(column.get("column_name"), str):
            raise MetadataCompositionError(
                f"{path}: schema_hints[{index}].hints[{column_index}] requires column_name"
            )
        column_name = column["column_name"]
        if column_name in seen_columns:
            raise MetadataCompositionError(
                f"{path}: duplicate schema hint column {column_name!r} in group {identity!r}"
            )
        seen_columns.add(column_name)
    return identity


def _validate_shared(document: Mapping[str, Any], path: Path) -> None:
    if not isinstance(document, dict):
        raise MetadataCompositionError(f"{path}: shared metadata must contain a JSON object")
    if _COMPOSITION_KEYS.intersection(document):
        keys = sorted(_COMPOSITION_KEYS.intersection(document))
        raise MetadataCompositionError(
            f"{path}: shared metadata must be plain connections/schema_hints ({', '.join(keys)})"
        )
    connections = _require_list(document, "connections", path)
    hints = _require_list(document, "schema_hints", path)
    connection_names: set[str] = set()
    for index, connection in enumerate(connections):
        if not isinstance(connection, dict):
            raise MetadataCompositionError(f"{path}: connections[{index}] must be an object")
        name = _connection_key(connection, path, index)
        if name in connection_names:
            raise MetadataCompositionError(f"{path}: duplicate connection name: {name}")
        connection_names.add(name)
    hint_groups: set[tuple[Any, ...]] = set()
    for index, group in enumerate(hints):
        key = _hint_group_key(group, path, index)
        if key in hint_groups:
            raise MetadataCompositionError(f"{path}: duplicate schema hint group: {key!r}")
        hint_groups.add(key)
