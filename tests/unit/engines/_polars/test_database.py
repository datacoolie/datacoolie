"""Tests for private Polars database helpers."""

import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.database import build_connection_string
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


def test_unsupported_database_type_raises() -> None:
    with pytest.raises(EngineError, match="unsupported database_type"):
        build_connection_string({"database_type": "redis"})


def test_public_reader_requires_connection_type_or_url() -> None:
    with pytest.raises(EngineError, match="requires"):
        PolarsEngine().read_database(table="t", options={"database": "db"})
