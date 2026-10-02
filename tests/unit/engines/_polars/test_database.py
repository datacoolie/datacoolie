"""Tests for private Polars database helpers."""

import sys
import sqlite3
import types
from decimal import Decimal

import polars as pl
import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.database import (
    build_connection_string,
    read_database,
    read_database_native,
    read_database_oracle,
)
from datacoolie.engines.polars_engine import PolarsEngine


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (
            {
                "database_type": "mysql",
                "user": "root",
                "password": "pass",
                "host": "localhost",
                "database": "mydb",
            },
            "mysql://root:pass@localhost:3306/mydb",
        ),
        (
            {
                "database_type": "mssql",
                "user": "sa",
                "password": "pw",
                "host": "server",
                "database": "db",
            },
            "mssql://sa:pw@server:1433/db",
        ),
        (
            {
                "database_type": "postgresql",
                "user": "u",
                "password": "p",
                "host": "h",
                "database": "d",
            },
            "postgresql://u:p@h:5432/d",
        ),
        (
            {
                "database_type": "oracle",
                "user": "u",
                "password": "p",
                "host": "h",
                "database": "db",
            },
            "oracle://u:p@h:1521/db",
        ),
    ],
)
def test_connection_string_defaults(options: dict, expected: str) -> None:
    assert build_connection_string(options) == expected


def test_sqlite_relative_path_is_absolute() -> None:
    result = build_connection_string(
        {"database_type": "sqlite", "database": "mydb.sqlite"}
    )
    assert result.startswith("sqlite:///")
    assert result.endswith("mydb.sqlite")


def test_native_sqlite_reader_preserves_temporal_text(tmp_path) -> None:
    database_path = tmp_path / "temporal.sqlite"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "CREATE TABLE values_table (event_date DATE, event_timestamp TIMESTAMP)"
        )
        connection.execute(
            "INSERT INTO values_table VALUES (?, ?)",
            ("2024-01-15 00:00:00.000000", "2024-01-15 10:30:45.123456"),
        )
        connection.commit()
    finally:
        connection.close()

    result = read_database(
        table="values_table",
        query=None,
        options={
            "database_type": "sqlite",
            "url": f"sqlite:///{database_path.as_posix()}",
            "database_read_engine": "native",
        },
        driver_connection_keys=(),
    ).collect()

    assert result.to_dict(as_series=False) == {
        "event_date": ["2024-01-15 00:00:00.000000"],
        "event_timestamp": ["2024-01-15 10:30:45.123456"],
    }


def test_unsupported_database_type_raises() -> None:
    with pytest.raises(EngineError, match="unsupported database_type"):
        build_connection_string({"database_type": "redis"})


def test_public_reader_requires_connection_type_or_url() -> None:
    with pytest.raises(EngineError, match="requires"):
        PolarsEngine().read_database(table="t", options={"database": "db"})


def test_native_database_reader_preserves_exact_python_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        description = [("amount",), ("value",), ("created",)]

        def execute(self, sql: str) -> None:
            assert sql == "SELECT amount, value, created FROM orders"

        def fetchall(self):
            from datetime import datetime

            return [
                (
                    Decimal("9999999999999999.99"),
                    18446744073709551615,
                    datetime(2024, 1, 15, 10, 30, 45, 123456),
                )
            ]

        def close(self) -> None:
            pass

    class Connection:
        def __init__(self) -> None:
            self.closed = False

        def cursor(self) -> Cursor:
            return Cursor()

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    fake_driver = types.SimpleNamespace(connect=lambda **kwargs: connection)
    monkeypatch.setitem(sys.modules, "pymysql", fake_driver)

    result = read_database_native(
        "SELECT amount, value, created FROM orders",
        db_type="mysql",
        connection_uri="mysql://user:password@localhost:3306/db",
    ).collect()

    assert result.schema == {
        "amount": pl.Decimal(precision=38, scale=2),
        "value": pl.Int128,
        "created": pl.Datetime("us"),
    }
    assert result["amount"].item() == Decimal("9999999999999999.99")
    assert result["value"].item() == 18446744073709551615
    assert connection.closed is True


def test_native_postgresql_reader_preserves_source_decimal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Description(tuple):
        def __new__(cls, name: str, precision: int, scale: int):
            return super().__new__(
                cls,
                (name, object(), None, None, precision, scale, True),
            )

    class Cursor:
        # A driver may expose precision/scale for an integer column as well;
        # it must not turn the BIGINT semantic into Decimal.
        description = [Description("amount", 18, 2), Description("id", 19, 0)]

        def execute(self, sql: str) -> None:
            assert sql == "SELECT amount, id FROM orders"

        def fetchall(self):
            return [(Decimal("12.30"), 1)]

        def close(self) -> None:
            pass

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def close(self) -> None:
            pass

    connection = Connection()
    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        types.SimpleNamespace(connect=lambda **kwargs: connection),
    )

    result = read_database_native(
        "SELECT amount, id FROM orders",
        db_type="postgresql",
        connection_uri="postgresql://user:password@localhost:5432/db",
    ).collect()

    assert result.schema == {
        "amount": pl.Decimal(precision=18, scale=2),
        "id": pl.Int64,
    }


def test_native_mysql_reader_normalizes_display_width_precision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        # PyMySQL reports DECIMAL(18,2) as display width 20 (sign + dot).
        description = [("amount", object(), None, 20, 20, 2, True)]

        def execute(self, sql: str) -> None:
            assert sql == "SELECT amount FROM orders"

        def fetchall(self):
            return [(Decimal("12.30"),)]

        def close(self) -> None:
            pass

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def close(self) -> None:
            pass

    connection = Connection()
    monkeypatch.setitem(
        sys.modules,
        "pymysql",
        types.SimpleNamespace(connect=lambda **kwargs: connection),
    )

    result = read_database_native(
        "SELECT amount FROM orders",
        db_type="mysql",
        connection_uri="mysql://user:password@localhost:3306/db",
    ).collect()

    assert result.schema == {"amount": pl.Decimal(precision=18, scale=2)}


def test_native_database_reader_rejects_unknown_options() -> None:
    with pytest.raises(EngineError, match="does not support"):
        read_database_native(
            "SELECT 1",
            db_type="mysql",
            connection_uri="mysql://user:password@localhost:3306/db",
            partition_on="id",
        )


def test_native_database_reader_rejects_ignored_uri_parameters() -> None:
    with pytest.raises(EngineError, match="unsupported query parameters"):
        read_database_native(
            "SELECT 1",
            db_type="mysql",
            connection_uri="mysql://user:secret@localhost:3306/db?ssl=true",
        )


def test_native_dispatch_does_not_drop_driver_specific_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "pymssql", types.SimpleNamespace())
    with pytest.raises(EngineError, match="does not support"):
        read_database(
            table="orders",
            query=None,
            options={
                "database_type": "mssql",
                "url": "mssql://user:password@localhost:1433/db",
                "encrypt": "true",
            },
            driver_connection_keys=frozenset({"encrypt"}),
        )


def test_postgresql_defaults_to_connectorx_without_explicit_native_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_read_database_uri(sql: str, uri: str, **options):
        captured.update({"sql": sql, "uri": uri, **options})
        return pl.DataFrame({"id": [1]}).lazy()

    monkeypatch.setattr(pl, "read_database_uri", fake_read_database_uri)
    result = read_database(
        table="orders",
        query=None,
        options={
            "database_type": "postgresql",
            "url": "postgresql://user:password@localhost:5432/db",
        },
        driver_connection_keys=(),
    )

    assert result.collect().to_dict(as_series=False) == {"id": [1]}
    assert captured["uri"] == "postgresql://user:password@localhost:5432/db"


def test_oracle_uri_parse_error_does_not_echo_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "oracledb", types.SimpleNamespace())
    with pytest.raises(EngineError) as exc_info:
        read_database_oracle(
            "SELECT 1",
            "oracle+oracledb://user:secret@localhost:1521/FREEPDB1#fragment",
        )
    assert "secret" not in str(exc_info.value)


def test_oracle_uri_rejects_conflicting_service_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "oracledb", types.SimpleNamespace())
    with pytest.raises(EngineError, match="conflicting"):
        read_database_oracle(
            "SELECT 1",
            "oracle+oracledb://user:secret@localhost:1521/FREEPDB1?service_name=OTHER",
        )


def test_oracle_reader_supports_driver_url_and_preserves_decimal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        description = [("amount",), ("created",)]

        def execute(self, sql: str) -> None:
            assert sql == "SELECT amount, created FROM orders"

        def fetchall(self):
            from datetime import datetime

            return [(Decimal("9999999999999999.99"), datetime(2024, 1, 15, 10, 30))]

        def close(self) -> None:
            pass

    class Connection:
        outputtypehandler = None

        def __init__(self) -> None:
            self.closed = False

        def cursor(self) -> Cursor:
            return Cursor()

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    captured: dict[str, str] = {}

    def connect(**kwargs):
        captured.update(kwargs)
        return connection

    fake_driver = types.SimpleNamespace(
        DB_TYPE_NUMBER=object(),
        connect=connect,
    )
    monkeypatch.setitem(sys.modules, "oracledb", fake_driver)

    result = read_database_oracle(
        "SELECT amount, created FROM orders",
        "oracle+oracledb://user:password@localhost:1521/?service_name=FREEPDB1",
    ).collect()

    assert result.schema == {
        "amount": pl.Decimal(precision=38, scale=2),
        "created": pl.Datetime("us"),
    }
    assert captured["dsn"] == "localhost:1521/FREEPDB1"
    assert connection.closed is True
