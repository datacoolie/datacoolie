from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._aws.catalog import CatalogBackend
from datacoolie.platforms._aws.secrets import SecretsBackend


def test_secrets_backend_extracts_json_field() -> None:
    client = MagicMock()
    client.get_secret_value.return_value = {"SecretString": '{"token": "value"}'}
    backend = SecretsBackend(lambda service: client)

    assert backend.fetch_secret("token", "secret-name") == "value"
    client.get_secret_value.assert_called_once_with(SecretId="secret-name")


def test_catalog_backend_uses_facade_callbacks_for_registration() -> None:
    execute_ddl = MagicMock(return_value="query-id")
    delete_table = MagicMock()
    backend = CatalogBackend(
        lambda service: MagicMock(),
        delete_table=delete_table,
        execute_ddl=execute_ddl,
    )

    backend.register_delta_table(
        "table",
        "s3://bucket/table",
        database="db",
        output_location="s3://bucket/results",
        recreate=True,
    )

    delete_table.assert_called_once_with("db", "table")
    execute_ddl.assert_called_once()


def test_catalog_backend_handles_client_creation_failure() -> None:
    backend = CatalogBackend(
        lambda service: (_ for _ in ()).throw(RuntimeError("client unavailable"))
    )

    try:
        backend.delete_glue_table("db", "table")
    except PlatformError as exc:
        assert "Failed to delete Glue table" in str(exc)
    else:
        raise AssertionError("client creation failure was swallowed")


def test_catalog_backend_surfaces_glue_authorization_failure() -> None:
    client = MagicMock()
    client.delete_table.side_effect = RuntimeError("AccessDenied")
    backend = CatalogBackend(lambda service: client)

    with pytest.raises(PlatformError, match="Failed to delete Glue table"):
        backend.delete_glue_table("db", "table")


def test_catalog_backend_only_suppresses_exact_entity_not_found() -> None:
    class EntityNotFoundException(Exception):
        pass

    client = MagicMock()
    client.delete_table.side_effect = EntityNotFoundException("missing")
    backend = CatalogBackend(lambda service: client)

    backend.delete_glue_table("db", "table")


def test_catalog_athena_location_escapes_generated_literal(monkeypatch) -> None:
    athena = MagicMock()
    athena.start_query_execution.return_value = {"QueryExecutionId": "q-1"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    backend = CatalogBackend(lambda service: athena)

    backend.register_delta_table(
        "table",
        "s3://bucket/a'b",
        database="db",
        output_location="s3://bucket/results",
    )

    sql = athena.start_query_execution.call_args.kwargs["QueryString"]
    assert "LOCATION 's3://bucket/a''b'" in sql


def test_catalog_athena_timeout_uses_monotonic_deadline(monkeypatch) -> None:
    athena = MagicMock()
    athena.start_query_execution.return_value = {"QueryExecutionId": "q-timeout"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "RUNNING"}}
    }
    backend = CatalogBackend(lambda service: athena)
    clock = iter([0.0, 0.0, 61.0])
    monkeypatch.setattr(
        "datacoolie.platforms._aws.catalog.time.monotonic", lambda: next(clock)
    )
    monkeypatch.setattr("datacoolie.platforms._aws.catalog.time.sleep", lambda _: None)

    with pytest.raises(PlatformError, match="q-timeout.*not cancelled"):
        backend.execute_athena_ddl("SELECT 1", output_location="s3://bucket/results")
