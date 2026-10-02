"""Validation of source-aware schema-hint declarations."""

from __future__ import annotations

from typing import Any, Sequence

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.transform import SchemaHint
from datacoolie.engines.data_types import infer_type_system, resolve_schema_hint
from .reports import Diagnostic, _error

def _validate_datatype_hints(
    dataflows: Sequence[Any],
    errors: list[Diagnostic],
) -> int:
    """Validate authored hint dialects and parameters without reading data.

    This is an offline preparation check. It deliberately resolves only
    declarations that carry enough information in metadata; connector result
    schemas, value overflow, and persisted format behavior remain runtime
    checks.
    """

    checked = 0
    for dataflow_index, dataflow in enumerate(dataflows):
        transform = dataflow.transform
        try:
            type_system = infer_type_system(
                database_type=dataflow.source.connection.database_type,
                explicit_type_system=dataflow.source.connection.schema_hint_type_system,
            )
        except ConfigurationError as exc:
            _error(
                errors,
                "metadata.datatype",
                str(exc),
                f"dataflows[{dataflow_index}].source.connection.configure.schema_hint_type_system",
            )
            continue
        for hint_index, hint in enumerate(transform.schema_hints):
            if not hint.is_active:
                continue
            checked += 1
            try:
                resolve_schema_hint(
                    hint.data_type,
                    type_system=type_system,
                    precision=hint.precision,
                    scale=hint.scale,
                )
            except ConfigurationError as exc:
                _error(
                    errors,
                    "metadata.datatype",
                    str(exc),
                    f"dataflows[{dataflow_index}].transform.schema_hints[{hint_index}]",
                )
    return checked

def _validate_grouped_datatype_hints(
    raw_groups: Any,
    connections: Sequence[Any],
    errors: list[Diagnostic],
) -> int:
    """Validate shared schema-hint groups against their source dialect.

    Shared hints are applied by the metadata provider after a dataflow has
    been selected, so they do not appear in ``dataflows[*].transform``.  The
    CLI must nevertheless validate them with the same source-aware resolver;
    otherwise a malformed decimal or vendor type would pass offline checks
    and fail only after a provider has started.
    """

    if not isinstance(raw_groups, list):
        return 0
    by_id = {
        connection.connection_id: connection
        for connection in connections
        if connection.connection_id
    }
    by_name: dict[str, list[Any]] = {}
    for connection in connections:
        if connection.name:
            by_name.setdefault(connection.name, []).append(connection)
    checked = 0
    for group_index, group in enumerate(raw_groups):
        if not isinstance(group, dict):
            continue
        connection_ref = group.get("connection_name") or group.get("connection_id")
        if connection_ref in by_id:
            connection = by_id[connection_ref]
        else:
            name_matches = by_name.get(connection_ref, [])
            connection = name_matches[0] if len(name_matches) == 1 else None
        if connection is None:
            # The metadata model validation already reports the unknown
            # connection.  Avoid emitting a duplicate datatype diagnostic.
            continue
        hints = group.get("hints", [])
        if not isinstance(hints, list):
            continue
        try:
            type_system = infer_type_system(
                database_type=connection.database_type,
                explicit_type_system=connection.schema_hint_type_system,
            )
        except ConfigurationError as exc:
            _error(
                errors,
                "metadata.datatype",
                str(exc),
                f"schema_hints[{group_index}]",
            )
            continue
        for hint_index, hint in enumerate(hints):
            if not isinstance(hint, dict):
                continue
            try:
                normalized_hint = SchemaHint(**hint)
            except ConfigurationError:
                # The grouped model builder has already reported malformed
                # hints; do not duplicate its structural diagnostic here.
                continue
            if not normalized_hint.is_active:
                continue
            try:
                resolved = resolve_schema_hint(
                    normalized_hint.data_type,
                    type_system=type_system,
                    precision=normalized_hint.precision,
                    scale=normalized_hint.scale,
                )
            except (ConfigurationError, TypeError) as exc:
                _error(
                    errors,
                    "metadata.datatype",
                    str(exc),
                    f"schema_hints[{group_index}].hints[{hint_index}]",
                )
                continue
            _ = resolved
            checked += 1
    return checked
