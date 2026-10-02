"""Focused tests for normalized file-metadata mapping rules."""

from __future__ import annotations

import pytest

from datacoolie.core.exceptions import MetadataError
from datacoolie.core.models.connection import Connection
from datacoolie.metadata.documents.mapping import (
    build_connections,
    build_dataflows,
    build_grouped_schema_hints,
    build_single_dataflow,
)


def _connections() -> dict[str, Connection]:
    connection = Connection(
        connection_id="source-id",
        name="source",
        connection_type="file",
        format="parquet",
    )
    return {connection.name: connection}


def test_source_rejects_named_and_inline_connection_together() -> None:
    with pytest.raises(MetadataError, match="source cannot define both"):
        build_single_dataflow(
            {
                "name": "orders",
                "source": {
                    "connection_name": "source",
                    "connection": "source",
                    "table": "orders",
                },
                "destination": {
                    "connection_name": "source",
                    "table": "orders_out",
                },
            },
            _connections(),
        )


def test_destination_rejects_named_and_inline_connection_together() -> None:
    with pytest.raises(MetadataError, match="destination cannot define both"):
        build_single_dataflow(
            {
                "name": "orders",
                "source": {"connection_name": "source", "table": "orders"},
                "destination": {
                    "connection_name": "source",
                    "connection": "source",
                    "table": "orders_out",
                },
            },
            _connections(),
        )


def test_distinct_connection_ids_may_share_a_name_but_name_reference_is_ambiguous() -> None:
    connections = build_connections(
        [
            {"connection_id": "source-1", "name": "shared"},
            {"connection_id": "source-2", "name": "shared"},
        ]
    )
    assert {connection.connection_id for connection in connections} == {
        "source-1",
        "source-2",
    }

    with pytest.raises(MetadataError, match="ambiguous"):
        build_dataflows(
            [
                {
                    "name": "orders",
                    "source": {"connection_name": "shared"},
                    "destination": {"connection_name": "shared", "table": "out"},
                }
            ],
            connections,
        )


def test_explicit_connection_ids_resolve_duplicate_display_names() -> None:
    connections = build_connections(
        [
            {"connection_id": "source-1", "name": "shared"},
            {"connection_id": "source-2", "name": "shared"},
        ]
    )
    dataflows = build_dataflows(
        [
            {
                "name": "orders",
                "source": {
                    "connection": {"connection_id": "source-1", "name": "shared"}
                },
                "destination": {
                    "connection": {"connection_id": "source-2", "name": "shared"},
                    "table": "out",
                },
            }
        ],
        connections,
    )
    assert dataflows[0].source.connection.connection_id == "source-1"
    assert dataflows[0].destination.connection.connection_id == "source-2"


def test_inline_connection_with_new_id_may_reuse_display_name() -> None:
    connections = build_connections(
        [{"connection_id": "source-1", "name": "shared"}]
    )
    dataflows = build_dataflows(
        [
            {
                "name": "orders",
                "source": {"connection": {"connection_id": "source-2", "name": "shared"}},
                "destination": {
                    "connection": {"connection_id": "source-1", "name": "shared"},
                    "table": "out",
                },
            }
        ],
        connections,
    )
    assert dataflows[0].source.connection.connection_id == "source-2"


def test_shared_hint_requires_connection_id_when_name_is_ambiguous() -> None:
    connections = build_connections(
        [
            {"connection_id": "source-1", "name": "shared"},
            {"connection_id": "source-2", "name": "shared"},
        ]
    )
    with pytest.raises(MetadataError, match="ambiguous"):
        build_grouped_schema_hints(
            [
                {
                    "connection_name": "shared",
                    "table_name": "orders",
                    "hints": [],
                }
            ],
            connections,
        )
