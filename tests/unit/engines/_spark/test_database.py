from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import DatabaseAuthType
from datacoolie.engines._spark import database


def test_read_database_copies_options_and_builds_query_relation() -> None:
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    options = {"url": "jdbc:test", "user": "reader"}

    database.read_database(
        spark,
        table=None,
        query=" SELECT 1 ",
        options=options,
        driver_connection_keys=(),
    )

    assert options == {"url": "jdbc:test", "user": "reader"}
    assert ("dbtable", "(SELECT 1) q") in [
        call.args for call in reader.option.call_args_list
    ]


@pytest.mark.parametrize(
    ("database_type", "expected"),
    [
        ("mysql", "jdbc:mysql://db:3306/warehouse"),
        ("postgresql", "jdbc:postgresql://db:5432/warehouse"),
        ("oracle", "jdbc:oracle:thin:@db:1521/warehouse"),
    ],
)
def test_build_jdbc_url_defaults(database_type: str, expected: str) -> None:
    options = {"database_type": database_type, "host": "db", "database": "warehouse"}
    assert database.build_jdbc_url(options, ()) == expected
    assert "driver" in options


def test_service_principal_auth_consumes_framework_fields() -> None:
    options = {
        "auth_type": DatabaseAuthType.SERVICE_PRINCIPAL,
        "user": "client",
        "password": "secret",
        "tenant_id": "tenant",
    }
    assert database.build_jdbc_auth_properties(options) == {
        "authentication": "ActiveDirectoryServicePrincipal",
        "AADSecurePrincipalId": "client",
        "AADSecurePrincipalSecret": "secret",
    }
    assert options == {}


def test_password_auth_preserves_jdbc_credentials() -> None:
    options = {"auth_type": DatabaseAuthType.PASSWORD, "user": "u", "password": "p"}
    assert database.build_jdbc_auth_properties(options) == {}
    assert options == {"user": "u", "password": "p"}


@pytest.mark.parametrize(("user", "client_id"), [(None, None), ("client", "client")])
def test_managed_identity_auth(user: str | None, client_id: str | None) -> None:
    options = {
        "auth_type": DatabaseAuthType.MANAGED_IDENTITY,
        "database_type": "mssql",
    }
    if user:
        options["user"] = user
    properties = database.build_jdbc_auth_properties(options)
    assert properties["authentication"] == "ActiveDirectoryMSI"
    assert properties.get("msiClientId") == client_id


@pytest.mark.parametrize(
    ("database_type", "property_name"),
    [("mssql", "accessToken"), ("postgresql", None)],
)
def test_access_token_auth(database_type: str, property_name: str | None) -> None:
    options = {
        "auth_type": DatabaseAuthType.ACCESS_TOKEN,
        "database_type": database_type,
        "token": "token",
    }
    properties = database.build_jdbc_auth_properties(options)
    if property_name:
        assert properties[property_name] == "token"
    else:
        assert properties == {}
        assert options["password"] == "token"
