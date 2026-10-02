"""Map normalized file documents to DataCoolie metadata models.

The provider keeps ownership of caches and source provenance.  These helpers
only validate/modelize sections and therefore remain reusable by any file
loader that supplies the same document shape.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any, Dict, Iterable, List, Optional, Tuple

from datacoolie.core.exceptions import MetadataError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.transform import SchemaHint
from datacoolie.core.models.source import Source
from datacoolie.core.models.transform import Transform
from datacoolie.metadata.resolution.schema_hints import (
    normalize_grouped_hints,
    normalize_schema_name,
    normalized_key,
)
from datacoolie.metadata.contracts.identity import (
    connection_identity_error,
    dataflow_identity_error,
)


def _source(origins: Optional[Mapping[int, str]], value: Any) -> str:
    return origins.get(id(value), "metadata") if origins is not None else "metadata"


def build_connections(
    raw_list: Any,
    *,
    origins: Optional[Mapping[int, str]] = None,
) -> List[Connection]:
    """Build and validate all connection models from a section list."""

    if not isinstance(raw_list, list):
        raise MetadataError(
            f"'connections' must be a list, got {type(raw_list).__name__!r}"
        )
    built: List[Connection] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_list):
        if not isinstance(raw, dict):
            raise MetadataError(
                f"Invalid connection definition at index {index} "
                f"(source: {_source(origins, raw)})"
            )
        try:
            connection = Connection(**copy.deepcopy(raw))
        except Exception as exc:
            raise MetadataError(
                f"Invalid connection definition: {raw.get('name', '?')} "
                f"(source: {_source(origins, raw)})"
            ) from exc
        if connection.connection_id in seen_ids:
            raise MetadataError(
                f"Duplicate connection_id in metadata: {connection.connection_id} "
                f"(source: {_source(origins, raw)})"
            )
        seen_ids.add(connection.connection_id)
        built.append(connection)
    return built


def resolve_connection(
    name_or_ref: str | Dict[str, Any],
    by_name: Mapping[str, Connection | List[Connection]],
) -> Connection:
    """Resolve a named connection or create an inline connection model."""

    if isinstance(name_or_ref, dict):
        try:
            return Connection(**copy.deepcopy(name_or_ref))
        except Exception as exc:
            raise MetadataError("Invalid inline connection definition") from exc
    if not isinstance(name_or_ref, str) or not name_or_ref.strip():
        raise MetadataError("Connection reference must be a non-empty name or mapping")
    name_or_ref = name_or_ref.strip()
    connection = by_name.get(name_or_ref)
    if connection is None:
        raise MetadataError(f"Connection not found: {name_or_ref}")
    if isinstance(connection, list):
        if len(connection) != 1:
            raise MetadataError(
                f"Connection name is ambiguous: {name_or_ref}; use connection_id"
            )
        return connection[0]
    return connection


def build_single_dataflow(
    raw: Dict[str, Any],
    by_name: Mapping[str, Connection | List[Connection]],
) -> DataFlow:
    """Build one dataflow while resolving source/destination references."""

    raw_copy = copy.deepcopy(raw)
    source_raw: Dict[str, Any] = dict(raw_copy.get("source", {}))
    source_has_name = "connection_name" in source_raw
    source_has_inline = "connection" in source_raw
    if source_has_name and source_has_inline:
        raise MetadataError(
            f"Dataflow '{raw_copy.get('name', '?')}' source cannot define both "
            "'connection_name' and 'connection'"
        )
    source_ref = (
        source_raw.pop("connection_name")
        if source_has_name
        else source_raw.pop("connection", None)
    )
    if source_ref is None:
        raise MetadataError(
            f"Dataflow '{raw_copy.get('name', '?')}' source must have "
            "'connection_name' or 'connection'"
        )
    source = Source(
        connection=resolve_connection(source_ref, by_name),
        **source_raw,
    )

    destination_raw: Dict[str, Any] = dict(raw_copy.get("destination", {}))
    destination_has_name = "connection_name" in destination_raw
    destination_has_inline = "connection" in destination_raw
    if destination_has_name and destination_has_inline:
        raise MetadataError(
            f"Dataflow '{raw_copy.get('name', '?')}' destination cannot define "
            "both 'connection_name' and 'connection'"
        )
    destination_ref = (
        destination_raw.pop("connection_name")
        if destination_has_name
        else destination_raw.pop("connection", None)
    )
    if destination_ref is None:
        raise MetadataError(
            f"Dataflow '{raw_copy.get('name', '?')}' destination must have "
            "'connection_name' or 'connection'"
        )
    destination = Destination(
        connection=resolve_connection(destination_ref, by_name),
        **destination_raw,
    )

    transform_raw = raw_copy.get("transform", {})
    return DataFlow(
        source=source,
        destination=destination,
        transform=Transform(**transform_raw) if transform_raw else Transform(),
        **{
            key: value
            for key, value in raw_copy.items()
            if key not in ("source", "destination", "transform")
        },
    )


def build_dataflows(
    raw_list: Any,
    connections: Iterable[Connection],
    *,
    origins: Optional[Mapping[int, str]] = None,
) -> List[DataFlow]:
    """Build and validate all dataflow models from a section list."""

    if not isinstance(raw_list, list):
        raise MetadataError(
            f"'dataflows' must be a list, got {type(raw_list).__name__!r}"
        )
    connections = list(connections)
    by_name: Dict[str, List[Connection]] = {}
    by_id = {connection.connection_id: connection for connection in connections}
    for connection in connections:
        by_name.setdefault(connection.name, []).append(connection)
    built: List[DataFlow] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_list):
        if not isinstance(raw, dict):
            raise MetadataError(
                f"Invalid dataflow definition at index {index} "
                f"(source: {_source(origins, raw)})"
            )
        source = _source(origins, raw)
        label = raw.get("name") or raw.get("dataflow_id") or "?"
        try:
            dataflow = build_single_dataflow(raw, by_name)
        except MetadataError as exc:
            raise MetadataError(
                f"Invalid dataflow definition {label!r} at index {index} "
                f"(source: {source}): {exc}"
            ) from exc
        except Exception as exc:
            raise MetadataError(
                f"Invalid dataflow definition {label!r} at index {index} "
                f"(source: {source})"
            ) from exc
        identity_error = dataflow_identity_error(
            dataflow.dataflow_id,
            dataflow.name,
            label=str(label),
        )
        if identity_error:
            raise MetadataError(
                f"Invalid dataflow definition {label!r} at index {index} "
                f"(source: {source}): {identity_error}"
            )
        for role, connection in (
            ("source", dataflow.source.connection),
            ("destination", dataflow.destination.connection),
        ):
            identity_error = connection_identity_error(
                connection,
                by_id,
                by_name,
                context=f"Dataflow {label!r} {role}",
            )
            if identity_error:
                raise MetadataError(
                    f"Invalid dataflow definition {label!r} at index {index} "
                    f"(source: {source}): {identity_error}"
                )
            if connection.connection_id not in by_id:
                by_id[connection.connection_id] = connection
                by_name.setdefault(connection.name, []).append(connection)
        if dataflow.dataflow_id in seen_ids:
            raise MetadataError(
                f"Duplicate dataflow_id in metadata: {dataflow.dataflow_id} "
                f"(source: {_source(origins, raw)})"
            )
        seen_ids.add(dataflow.dataflow_id)
        built.append(dataflow)
    return built


def build_grouped_schema_hints(
    raw_hints: Any,
    connections: Iterable[Connection],
    *,
    origins: Optional[Mapping[int, str]] = None,
) -> Dict[Tuple[str, Optional[str], str], List[SchemaHint]]:
    """Build, normalize, and validate all schema-hint groups."""

    if not isinstance(raw_hints, list):
        raise MetadataError(
            "'schema_hints' must be a list of group objects, got "
            f"{type(raw_hints).__name__!r}. Expected format: "
            '[{"connection_name": "...", "table_name": "...", "hints": [...]}]'
        )
    name_to_ids: Dict[str, List[str]] = {}
    for connection in connections:
        name_to_ids.setdefault(connection.name, []).append(connection.connection_id)
    valid_ids = {
        connection_id
        for connection_ids in name_to_ids.values()
        for connection_id in connection_ids
    }
    grouped: Dict[Tuple[str, Optional[str], str], List[SchemaHint]] = {}
    for index, group in enumerate(raw_hints):
        if not isinstance(group, dict):
            raise MetadataError(
                f"'schema_hints[{index}]' must be a dict, got "
                f"{type(group).__name__!r}. Source: {_source(origins, group)}. "
                'Expected format: {"connection_name": "...", "table_name": "...", "hints": [...]} '
            )
        connection_ref = group.get("connection_name") or group.get("connection_id")
        table_name = group.get("table_name")
        if not connection_ref or not table_name:
            raise MetadataError(
                f"Schema hint group {index} is missing connection and table "
                f"(source: {_source(origins, group)})"
            )
        if not isinstance(connection_ref, str):
            raise MetadataError(
                f"Schema hint group {index} connection reference must be a string "
                f"(source: {_source(origins, group)})"
            )
        connection_ref = connection_ref.strip()
        if not connection_ref:
            raise MetadataError(
                f"Schema hint group {index} is missing connection and table "
                f"(source: {_source(origins, group)})"
            )
        if connection_ref in valid_ids:
            connection_id = connection_ref
        else:
            matching_ids = name_to_ids.get(connection_ref, [])
            if len(matching_ids) > 1:
                raise MetadataError(
                    f"Schema hint group {index} connection name {connection_ref!r} "
                    "is ambiguous; use connection_id"
                )
            connection_id = matching_ids[0] if matching_ids else None
        if not connection_id:
            raise MetadataError(
                f"Schema hint group {index} references unknown connection: {connection_ref} "
                f"(source: {_source(origins, group)})"
            )
        hints_raw = group.get("hints", [])
        if not isinstance(hints_raw, list):
            raise MetadataError(
                f"Schema hint group {index} 'hints' must be a list "
                f"(source: {_source(origins, group)})"
            )
        schema = normalize_schema_name(group.get("schema_name"))
        key = normalized_key(connection_id, schema, table_name)
        for hint_index, hint_raw in enumerate(hints_raw):
            if not isinstance(hint_raw, dict):
                raise MetadataError(
                    f"Invalid schema hint at group {index}, index {hint_index}: {hint_raw}"
                )
            try:
                hint = SchemaHint(**hint_raw)
            except Exception as exc:
                raise MetadataError(
                    f"Invalid schema hint for {table_name}: {hint_raw} "
                    f"(source: {_source(origins, group)})"
                ) from exc
            grouped.setdefault(key, []).append(hint)
    return normalize_grouped_hints(grouped)


__all__ = [
    "build_connections",
    "build_dataflows",
    "build_grouped_schema_hints",
    "build_single_dataflow",
    "resolve_connection",
]
