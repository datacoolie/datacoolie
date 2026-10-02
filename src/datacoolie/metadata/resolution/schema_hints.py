"""Shared schema-hint normalization and lookup rules.

Schema hints are represented by a mapping keyed by
``(connection_id, schema_name, table_name)``.  The storage backends use
different comparison mechanisms (JSON objects, SQL rows, or API payloads),
so lookup belongs in one small, backend-independent module.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from datacoolie.core.exceptions import MetadataError
from datacoolie.core.models.transform import SchemaHint


SchemaHintKey = Tuple[str, Optional[str], str]


def normalize_schema_name(value: object) -> Optional[str]:
    """Return a schema name with blank values represented as ``None``."""

    if value is None:
        return None
    if not isinstance(value, str):
        raise MetadataError(
            f"schema_name must be a string or null, got {type(value).__name__}"
        )
    value = value.strip()
    return value or None


def normalize_identifier(value: object, field_name: str) -> str:
    """Validate and normalize a case-insensitive metadata identifier."""

    if not isinstance(value, str) or not value.strip():
        raise MetadataError(f"{field_name} must be a non-empty string")
    return value.strip()


def normalized_key(
    connection_id: object,
    schema_name: object,
    table_name: object,
) -> SchemaHintKey:
    """Return a canonical key for grouping schema hints.

    Connection IDs remain exact identifiers.  Schema and table names are
    normalized for case-insensitive matching while retaining their original
    spelling in the returned key only for the connection id; callers should
    use this key for cache storage.
    """

    connection = normalize_identifier(connection_id, "connection_id")
    schema = normalize_schema_name(schema_name)
    table = normalize_identifier(table_name, "table_name")
    return connection, schema, table


def canonical_key(key: SchemaHintKey) -> tuple[str, Optional[str], str]:
    """Return a comparison key for a normalized or raw mapping key."""

    connection, schema, table = key
    return (
        connection,
        schema.casefold() if schema is not None else None,
        table.casefold(),
    )


def validate_hint_columns(
    key: SchemaHintKey,
    hints: Iterable[SchemaHint],
) -> List[SchemaHint]:
    """Validate duplicate columns and return a deterministic hint list."""

    result = list(hints)
    seen: set[str] = set()
    for hint in result:
        column = normalize_identifier(hint.column_name, "column_name")
        folded = column.casefold()
        if folded in seen:
            raise MetadataError(
                "Duplicate schema hint column in metadata: "
                f"{(*key, hint.column_name)}"
            )
        seen.add(folded)

    # Providers normally return ordinal order already.  Stable sorting keeps
    # that order for equal/default ordinals while making independently loaded
    # groups deterministic when ordinals are present.
    return sorted(
        result,
        key=lambda hint: (
            hint.ordinal_position is None,
            hint.ordinal_position if hint.ordinal_position is not None else 0,
        ),
    )


def normalize_grouped_hints(
    grouped: Dict[SchemaHintKey, Iterable[SchemaHint]],
) -> Dict[SchemaHintKey, List[SchemaHint]]:
    """Canonicalize grouped hints and validate duplicate columns.

    Keys that differ only by schema/table case are treated as one logical
    group.  Combining such groups is only valid when their columns are
    distinct; duplicate columns fail with a contextual ``MetadataError``.
    """

    canonical: Dict[tuple[str, Optional[str], str], tuple[SchemaHintKey, List[SchemaHint]]] = {}
    for raw_key, raw_hints in grouped.items():
        key = normalized_key(*raw_key)
        comparison = canonical_key(key)
        if comparison in canonical:
            existing_key, existing_hints = canonical[comparison]
            existing_hints.extend(list(raw_hints))
            validate_hint_columns(existing_key, existing_hints)
        else:
            hints = list(raw_hints)
            validate_hint_columns(key, hints)
            canonical[comparison] = (key, hints)
    return {
        key: validate_hint_columns(key, hints)
        for key, hints in canonical.values()
    }


def select_schema_hints(
    grouped: Dict[SchemaHintKey, Iterable[SchemaHint]],
    connection_id: str,
    schema_name: Optional[str],
    table_name: str,
) -> Optional[List[SchemaHint]]:
    """Select one logical hint group using the framework-wide contract.

    A named schema only matches that schema.  A ``None`` schema is valid and
    selects an unqualified/qualified group when exactly one schema group is
    available for the connection and table.  Multiple schema groups are
    ambiguous and therefore raise instead of silently selecting a database
    default or mixing columns.
    """

    requested_connection = normalize_identifier(connection_id, "connection_id")
    requested_schema = normalize_schema_name(schema_name)
    requested_table = normalize_identifier(table_name, "table_name")
    wanted_table = requested_table.casefold()

    candidates: Dict[tuple[Optional[str], str], List[SchemaHint]] = {}
    display_schemas: Dict[Optional[str], Optional[str]] = {}
    for raw_key, raw_hints in grouped.items():
        key = normalized_key(*raw_key)
        if key[0] != requested_connection or key[2].casefold() != wanted_table:
            continue
        schema_folded = key[1].casefold() if key[1] is not None else None
        candidate_key = (schema_folded, key[2].casefold())
        candidates.setdefault(candidate_key, []).extend(list(raw_hints))
        display_schemas.setdefault(schema_folded, key[1])

    if requested_schema is not None:
        selected = candidates.get((requested_schema.casefold(), wanted_table))
        if selected is None:
            return None
        key = (requested_connection, requested_schema, requested_table)
        return validate_hint_columns(key, selected)

    schema_groups = sorted(display_schemas, key=lambda value: value or "")
    if not schema_groups:
        return None
    if len(schema_groups) > 1:
        labels = [display_schemas[value] or "<unqualified>" for value in schema_groups]
        raise MetadataError(
            "Schema hint lookup is ambiguous for "
            f"{requested_connection}.{requested_table}; candidates: {', '.join(labels)}"
        )
    selected = candidates[(schema_groups[0], wanted_table)]
    key = (requested_connection, display_schemas[schema_groups[0]], requested_table)
    return validate_hint_columns(key, selected)


__all__ = [
    "SchemaHintKey",
    "canonical_key",
    "normalize_grouped_hints",
    "normalize_identifier",
    "normalize_schema_name",
    "normalized_key",
    "select_schema_hints",
    "validate_hint_columns",
]
