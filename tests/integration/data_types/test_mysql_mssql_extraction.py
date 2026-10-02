"""Qualification cells for exact MySQL and SQL Server result extraction."""

from __future__ import annotations

import os
import uuid

import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.transform import SchemaHint
from datacoolie.core.models.source import Source
from datacoolie.core.models.transform import Transform
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.transformers.schema_converter import SchemaConverter

from tests.support.datatype_config import require_docker_services


pytestmark = [pytest.mark.integration, pytest.mark.datatype_qualification]


def _dataflow(
    database_type: str,
    table: str,
    url: str,
    hints: list[SchemaHint],
) -> DataFlow:
    source = Connection(
        name=f"qualification-{database_type}",
        connection_type="database",
        format="sql",
        configure={
            "database_type": database_type,
            "url": url,
            "use_schema_hint": True,
            "schema_hint_type_system": database_type,
        },
    )
    destination = Connection(
        name="qualification-output",
        connection_type="file",
        format="parquet",
        configure={"base_path": ".scratch/datatype-qualification"},
    )
    return DataFlow(
        source=Source(connection=source, table=table),
        destination=Destination(connection=destination, table="output"),
        transform=Transform(schema_hints=hints),
    )


def test_mysql_native_extraction_preserves_unsigned_and_decimal() -> None:
    """ConnectorX's lossy MySQL projection is not used for exact reads."""

    require_docker_services(("mysql",))
    polars = pytest.importorskip("polars")
    sqlalchemy = pytest.importorskip("sqlalchemy")
    create_engine = sqlalchemy.create_engine
    text = sqlalchemy.text

    sqlalchemy_url = os.getenv(
        "DATACOOLIE_QUALIFICATION_MYSQL_URL",
        "mysql+pymysql://datacoolie:datacoolie@localhost:3306/datacoolie",
    )
    polars_url = os.getenv(
        "DATACOOLIE_QUALIFICATION_MYSQL_POLARS_URL",
        "mysql://datacoolie:datacoolie@localhost:3306/datacoolie",
    )
    table = f"dc_dtype_qualification_{uuid.uuid4().hex[:12]}"
    database = create_engine(sqlalchemy_url)
    dataflow = _dataflow(
        "mysql",
        table,
        polars_url,
        [
            SchemaHint(column_name="tiny_value", data_type="tinyint unsigned"),
            SchemaHint(column_name="unsigned_value", data_type="bigint unsigned"),
            SchemaHint(
                column_name="decimal_value",
                data_type="decimal",
                precision=18,
                scale=2,
            ),
            SchemaHint(column_name="created_value", data_type="datetime"),
        ],
    )
    with database.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE {table} ("
                "tiny_value TINYINT UNSIGNED, "
                "unsigned_value BIGINT UNSIGNED, "
                "decimal_value DECIMAL(18,2), "
                "created_value DATETIME(6))"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {table} VALUES "
                "(255,18446744073709551615,9999999999999999.99,"
                "'2024-01-15 10:30:45.123456')"
            )
        )

    try:
        engine = PolarsEngine()
        raw = engine.read_database(
            query=f"SELECT * FROM {table}",
            options={
                "database_type": "mysql",
                "url": polars_url,
                "use_schema_hint": True,
            },
        ).collect()
        assert raw.schema["unsigned_value"] == polars.Int128
        # The native DB-API route preserves the declared DECIMAL(18,2)
        # precision instead of widening it to ConnectorX's generic Decimal.
        assert raw.schema["decimal_value"] == polars.Decimal(18, 2)
        assert raw["unsigned_value"].item() == 18446744073709551615
        assert str(raw["decimal_value"].item()) == "9999999999999999.99"

        converted = SchemaConverter(engine).transform(
            engine.read_database(
                query=f"SELECT * FROM {table}",
                options={
                    "database_type": "mysql",
                    "url": polars_url,
                    "use_schema_hint": True,
                },
            ),
            dataflow,
        ).collect()
        assert converted.schema["tiny_value"] == polars.Int16
        assert converted.schema["unsigned_value"] == polars.Decimal(20, 0)
        assert converted.schema["decimal_value"] == polars.Decimal(18, 2)
        assert converted["unsigned_value"].item() == 18446744073709551615
        assert str(converted["decimal_value"].item()) == "9999999999999999.99"
    finally:
        with database.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
        database.dispose()


def test_mssql_native_extraction_preserves_decimal_and_tinyint() -> None:
    """SQL Server ``tinyint`` is unsigned and must not become Spark tinyint."""

    require_docker_services(("mssql",))
    polars = pytest.importorskip("polars")
    sqlalchemy = pytest.importorskip("sqlalchemy")
    create_engine = sqlalchemy.create_engine
    text = sqlalchemy.text

    sqlalchemy_url = os.getenv(
        "DATACOOLIE_QUALIFICATION_MSSQL_URL",
        "mssql+pymssql://sa:Datacoolie%401@localhost:1433/datacoolie",
    )
    polars_url = os.getenv(
        "DATACOOLIE_QUALIFICATION_MSSQL_POLARS_URL",
        "mssql://sa:Datacoolie%401@localhost:1433/datacoolie",
    )
    table = f"dc_dtype_qualification_{uuid.uuid4().hex[:12]}"
    database = create_engine(sqlalchemy_url)
    dataflow = _dataflow(
        "mssql",
        table,
        polars_url,
        [
            SchemaHint(column_name="tiny_value", data_type="tinyint"),
            SchemaHint(
                column_name="decimal_value",
                data_type="decimal",
                precision=18,
                scale=2,
            ),
            SchemaHint(column_name="created_value", data_type="datetime2"),
        ],
    )
    with database.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE {table} ("
                "tiny_value TINYINT, "
                "decimal_value DECIMAL(18,2), "
                "created_value DATETIME2(6))"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {table} VALUES "
                "(255,9999999999999999.99,'2024-01-15 10:30:45.123456')"
            )
        )

    try:
        engine = PolarsEngine()
        raw = engine.read_database(
            query=f"SELECT * FROM {table}",
            options={
                "database_type": "mssql",
                "url": polars_url,
                "use_schema_hint": True,
            },
        ).collect()
        assert raw.schema["decimal_value"] == polars.Decimal(38, 2)
        assert raw["tiny_value"].item() == 255
        assert str(raw["decimal_value"].item()) == "9999999999999999.99"

        converted = SchemaConverter(engine).transform(
            engine.read_database(
                query=f"SELECT * FROM {table}",
                options={
                    "database_type": "mssql",
                    "url": polars_url,
                    "use_schema_hint": True,
                },
            ),
            dataflow,
        ).collect()
        assert converted.schema["tiny_value"] == polars.Int16
        assert converted.schema["decimal_value"] == polars.Decimal(18, 2)
        assert converted["tiny_value"].item() == 255
        assert str(converted["decimal_value"].item()) == "9999999999999999.99"
    finally:
        with database.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
        database.dispose()
