"""Qualification cell for Oracle result typing and source-aware hints."""

from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

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


def test_oracle_native_types_and_schema_hints() -> None:
    """Oracle NUMBER/DATE values survive extraction before engine casts."""

    require_docker_services(("oracle",))
    polars = pytest.importorskip("polars")
    oracledb = pytest.importorskip("oracledb")
    url = os.getenv(
        "DATACOOLIE_QUALIFICATION_ORACLE_POLARS_URL",
        "oracle://datacoolie:datacoolie@localhost:1521/FREEPDB1",
    )
    connection = oracledb.connect(
        user=os.getenv("DATACOOLIE_QUALIFICATION_ORACLE_USER", "datacoolie"),
        password=os.getenv(
            "DATACOOLIE_QUALIFICATION_ORACLE_PASSWORD", "datacoolie"
        ),
        dsn=os.getenv(
            "DATACOOLIE_QUALIFICATION_ORACLE_DSN", "localhost:1521/FREEPDB1"
        ),
    )
    table = f"DC_DTYPE_{uuid4().hex[:12].upper()}"
    created = False
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE {table} ("
                "NUMBER_VALUE NUMBER(18,2), DATE_VALUE DATE)"
            )
            cursor.execute(
                f"INSERT INTO {table} VALUES (:amount, :created)",
                {
                    "amount": Decimal("9999999999999999.99"),
                    "created": datetime(2024, 1, 15, 10, 30, 45),
                },
            )
            connection.commit()
        created = True

        source = Connection(
            name="qualification-oracle",
            connection_type="database",
            format="sql",
            configure={
                "database_type": "oracle",
                "url": url,
                "use_schema_hint": True,
                "schema_hint_type_system": "oracle",
            },
        )
        dataflow = DataFlow(
            source=Source(connection=source, table=table),
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
                    SchemaHint(
                        column_name="NUMBER_VALUE",
                        data_type="NUMBER(18,2)",
                    ),
                    SchemaHint(column_name="DATE_VALUE", data_type="DATE"),
                ],
            ),
        )

        engine = PolarsEngine()
        query = f"SELECT NUMBER_VALUE, DATE_VALUE FROM {table}"
        raw = engine.read_database(
            query=query,
            options={
                "database_type": "oracle",
                "url": url,
                "use_schema_hint": True,
            },
        ).collect()
        assert raw.schema["NUMBER_VALUE"] == polars.Decimal(38, 2)
        assert raw.schema["DATE_VALUE"] == polars.Datetime("us")
        assert str(raw["NUMBER_VALUE"].item()) == "9999999999999999.99"
        assert raw["DATE_VALUE"].item() == datetime(2024, 1, 15, 10, 30, 45)

        converted = SchemaConverter(engine).transform(
            engine.read_database(
                query=query,
                options={
                    "database_type": "oracle",
                    "url": url,
                    "use_schema_hint": True,
                },
            ),
            dataflow,
        ).collect()
        assert converted.schema["NUMBER_VALUE"] == polars.Decimal(18, 2)
        assert converted.schema["DATE_VALUE"] == polars.Datetime("us")
        assert str(converted["NUMBER_VALUE"].item()) == "9999999999999999.99"
    finally:
        if created:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE {table} PURGE")
                connection.commit()
        connection.close()
