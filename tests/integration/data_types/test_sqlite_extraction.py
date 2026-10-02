"""Qualification cell for SQLite affinity and source-aware type hints."""

from __future__ import annotations

import sqlite3

import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.transform import SchemaHint
from datacoolie.core.models.source import Source
from datacoolie.core.models.transform import Transform
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.transformers.schema_converter import SchemaConverter


pytestmark = [pytest.mark.integration, pytest.mark.datatype_qualification]


def test_sqlite_integer_affinity_and_schema_hints(tmp_path) -> None:
    """SQLite INTEGER remains exact and resolves to signed 64-bit semantics."""

    polars = pytest.importorskip("polars")
    pytest.importorskip("connectorx")
    database_path = tmp_path / "qualification.sqlite"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "CREATE TABLE values_table (integer_value INTEGER, text_value TEXT)"
        )
        connection.execute(
            "INSERT INTO values_table VALUES (?, ?)",
            (9223372036854775807, "0000123"),
        )
        connection.commit()
    finally:
        connection.close()

    source = Connection(
        name="qualification-sqlite",
        connection_type="database",
        format="sql",
        configure={
            "database_type": "sqlite",
            "database": str(database_path),
            "use_schema_hint": True,
            "schema_hint_type_system": "sqlite",
        },
    )
    dataflow = DataFlow(
        source=Source(connection=source, table="values_table"),
        destination=Destination(
            connection=Connection(
                name="qualification-output",
                connection_type="file",
                format="parquet",
                configure={"base_path": ".scratch/datatype-qualification"},
            ),
            table="output",
        ),
        transform=Transform(
            schema_hints=[
                SchemaHint(column_name="integer_value", data_type="INTEGER"),
                SchemaHint(column_name="text_value", data_type="TEXT"),
            ],
        ),
    )

    engine = PolarsEngine()
    query = "SELECT integer_value, text_value FROM values_table"
    raw = engine.read_database(
        query=query,
        options={
            "database_type": "sqlite",
            "database": str(database_path),
            "use_schema_hint": True,
        },
    ).collect()
    assert raw.schema["integer_value"] == polars.Int64
    assert raw["integer_value"].item() == 9223372036854775807
    assert raw["text_value"].item() == "0000123"

    converted = SchemaConverter(engine).transform(
        engine.read_database(
            query=query,
            options={
                "database_type": "sqlite",
                "database": str(database_path),
                "use_schema_hint": True,
            },
        ),
        dataflow,
    ).collect()
    assert converted.schema["integer_value"] == polars.Int64
    assert converted.schema["text_value"] == polars.String
