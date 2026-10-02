from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType, VarcharType

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


def test_read_database_adds_default_driver_for_explicit_jdbc_url() -> None:
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader

    database.read_database(
        spark,
        table="orders",
        query=None,
        options={
            "database_type": "postgresql",
            "url": "jdbc:postgresql://db:5432/warehouse",
        },
        driver_connection_keys=(),
    )

    assert ("driver", "org.postgresql.Driver") in [
        call.args for call in reader.option.call_args_list
    ]


@pytest.mark.parametrize(
    "options",
    [
        {"database_type": "mysql", "host": "db", "database": "warehouse"},
        {"url": "jdbc:mysql://db/warehouse"},
        {"url": "jdbc:mysql://db/warehouse?useSSL=false"},
    ],
)
def test_mysql_year_defaults_to_integer(options: dict) -> None:
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    original = dict(options)
    database.read_database(
        spark, table="years", query=None, options=options,
        driver_connection_keys=(),
    )
    forwarded = dict(call.args for call in reader.option.call_args_list)
    assert forwarded["yearIsDateType"] == "false"
    assert options == original


@pytest.mark.parametrize(
    "options",
    [
        {"url": "jdbc:mysql://db/warehouse?yearIsDateType=true"},
        {"url": "jdbc:mysql://db/warehouse", "yearIsDateType": "true"},
        {"url": "jdbc:mysql://db/warehouse", "yearisdatetype": "true"},
        {"url": "jdbc:postgresql://db/warehouse"},
    ],
)
def test_year_default_preserves_explicit_options_and_other_databases(options: dict) -> None:
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    database.read_database(
        spark, table="years", query=None, options=options,
        driver_connection_keys=(),
    )
    forwarded = dict(call.args for call in reader.option.call_args_list)
    assert forwarded == {**options, "dbtable": "years"}


def test_read_database_does_not_forward_schema_hint_configuration_to_jdbc() -> None:
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader

    database.read_database(
        spark,
        table="orders",
        query=None,
        options={
            "database_type": "postgresql",
            "url": "jdbc:postgresql://db:5432/warehouse",
            "use_schema_hint": True,
            "schema_hint_type_system": "postgresql",
            "database_read_engine": "native",
        },
        driver_connection_keys=(),
    )

    option_names = {call.args[0] for call in reader.option.call_args_list}
    assert option_names.isdisjoint(
        {"use_schema_hint", "schema_hint_type_system", "database_read_engine"}
    )


def test_jdbc_string_types_are_normalized_for_writers() -> None:
    frame = MagicMock()
    frame.schema.fields = [
        SimpleNamespace(name="label", dataType=StringType()),
        SimpleNamespace(name="zero_length", dataType=VarcharType(0)),
    ]
    frame.withColumn.side_effect = lambda *_args: frame

    column = MagicMock()
    with patch("pyspark.sql.functions.col", return_value=column) as col:
        result = database._normalise_jdbc_string_types(
            frame, database_type="sqlite"
        )

    assert result is frame
    assert [call.args[0] for call in col.call_args_list] == [
        "label",
        "zero_length",
    ]
    assert all(
        isinstance(call.args[0], StringType)
        for call in column.cast.call_args_list
    )


def test_jdbc_bounded_string_types_are_preserved_for_non_sqlite() -> None:
    frame = MagicMock()
    frame.schema.fields = [
        SimpleNamespace(name="label", dataType=VarcharType(12)),
    ]

    result = database._normalise_jdbc_string_types(
        frame, database_type="postgresql"
    )

    assert result is frame
    frame.withColumn.assert_not_called()


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
