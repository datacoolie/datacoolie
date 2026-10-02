"""Preparation boundary tests for executable DataFlow copies."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import MetadataError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.core.secrets.provider import SecretStr, unwrap_secret
from datacoolie.orchestration.preparation import (
    PreparedDataFlow,
    prepare_execution_dataflow,
)
from datacoolie.platforms.base import BasePlatform


def _dataflow(*, query: str | None = None) -> DataFlow:
    source_connection = Connection(
        connection_id="source",
        name="source",
        format="sql",
        configure={"password": "source-ref"},
        secrets_ref={"scope": ["password"]},
    )
    destination_connection = Connection(
        connection_id="destination",
        name="destination",
        format="delta",
        configure={"password": "destination-ref"},
        secrets_ref={"scope": ["password"]},
    )
    return DataFlow(
        dataflow_id="df-1",
        source=Source(connection=source_connection, query=query),
        destination=Destination(connection=destination_connection, table="target"),
    )


class TestPrepareExecutionDataFlow:
    def test_resolves_query_and_secrets_on_an_isolated_copy(self) -> None:
        platform = MagicMock(spec=BasePlatform)
        platform.read_file_under_base.return_value = "SELECT 1;"
        original = _dataflow(query="orders.sql")
        resolved_connections: list[Connection] = []

        def resolve(connection: Connection) -> None:
            resolved_connections.append(connection)
            connection.configure["password"] = SecretStr(
                f"resolved-{connection.connection_id}"
            )

        prepared = prepare_execution_dataflow(
            original,
            platform=platform,
            resolve_connection_secrets=resolve,
            sql_base_path="release/sql",
        )

        assert isinstance(prepared, PreparedDataFlow)
        assert prepared.execution is not original
        assert prepared.metadata is not original
        assert prepared.execution.source.query == "SELECT 1;"
        assert prepared.metadata.source.query == "orders.sql"
        assert original.source.query == "orders.sql"
        assert original.source.connection.configure["password"] == "source-ref"
        assert len(resolved_connections) == 2
        assert unwrap_secret(prepared.execution.source.connection.configure["password"]) == (
            "resolved-source"
        )
        platform.read_file_under_base.assert_called_once_with("release/sql", "orders.sql")

    def test_preparation_failure_does_not_return_partial_wrapper(self) -> None:
        platform = MagicMock(spec=BasePlatform)
        original = _dataflow(query="orders.sql")

        with pytest.raises(MetadataError, match="requires artifact_base_path"):
            prepare_execution_dataflow(
                original,
                platform=platform,
                resolve_connection_secrets=lambda _connection: None,
            )

        assert original.source.query == "orders.sql"
        platform.read_file_under_base.assert_not_called()

    def test_maintenance_preparation_skips_query_and_hydrates_destination(self) -> None:
        platform = MagicMock(spec=BasePlatform)
        original = _dataflow(query="orders.sql")
        resolver = MagicMock()

        prepared = prepare_execution_dataflow(
            original,
            platform=platform,
            resolve_connection_secrets=resolver,
            operation_type="maintenance",
        )

        assert prepared.execution.source.query == "orders.sql"
        resolver.assert_called_once_with(prepared.execution.destination.connection)
        platform.read_file_under_base.assert_not_called()
