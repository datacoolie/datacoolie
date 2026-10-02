"""Qualification cell for PostgreSQL extraction and schema-hint casting.

This is intentionally opt-in.  It creates one uniquely named table in the
local usecase-sim database, reads it through the real Polars database adapter,
applies the real :class:`SchemaConverter`, then removes only that table.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal

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

DEFAULT_POSTGRES_URL = (
    "postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie"
)
DEFAULT_POLARS_URL = (
    "postgresql://datacoolie:datacoolie@localhost:5432/datacoolie"
)


def test_postgresql_connectorx_transport_widens_decimal_explicitly() -> None:
    """Keep ConnectorX's current precision widening visible as a policy cell.

    The default PostgreSQL Polars route is ConnectorX.  It preserves values
    but currently exposes ``NUMERIC(18,2)`` as ``Decimal(38,10)``; the paired
    persisted gate opts into the native DB-API route when exact declared
    precision is part of the contract.  This direct qualification prevents a
    future transport change from silently changing that documented behavior.
    """

    require_docker_services(("postgres",))
    polars = pytest.importorskip("polars")
    sqlalchemy = pytest.importorskip("sqlalchemy")
    table = f"dc_dtype_connectorx_{uuid.uuid4().hex[:12]}"
    database = sqlalchemy.create_engine(DEFAULT_POSTGRES_URL)
    try:
        with database.begin() as connection:
            connection.execute(
                sqlalchemy.text(
                    f"CREATE TABLE {table} (amount NUMERIC(18,2))"
                )
            )
            connection.execute(
                sqlalchemy.text(
                    f"INSERT INTO {table} (amount) VALUES (:amount)"
                ),
                {"amount": Decimal("12.30")},
            )

        frame = PolarsEngine().read_database(
            table=table,
            options={
                "database_type": "postgresql",
                "url": DEFAULT_POLARS_URL,
            },
        ).collect()

        assert frame.schema["amount"] == polars.Decimal(38, 10)
        assert frame["amount"].item() == Decimal("12.3000000000")
    finally:
        with database.begin() as connection:
            connection.execute(
                sqlalchemy.text(f"DROP TABLE IF EXISTS {table}")
            )
        database.dispose()


def test_postgresql_native_types_and_schema_hints() -> None:
    """Verify source extraction preserves values and hints set logical types."""

    require_docker_services(("postgres",))
    polars = pytest.importorskip("polars")
    sqlalchemy = pytest.importorskip("sqlalchemy")
    create_engine = sqlalchemy.create_engine
    text = sqlalchemy.text

    sqlalchemy_url = os.getenv(
        "DATACOOLIE_QUALIFICATION_POSTGRES_URL", DEFAULT_POSTGRES_URL
    )
    polars_url = os.getenv(
        "DATACOOLIE_QUALIFICATION_POSTGRES_POLARS_URL", DEFAULT_POLARS_URL
    )
    table = f"dc_dtype_qualification_{uuid.uuid4().hex[:12]}"
    database = create_engine(sqlalchemy_url)
    source = Connection(
        name="qualification-postgres",
        connection_type="database",
        format="sql",
        configure={
            "database_type": "postgresql",
            "url": polars_url,
            "use_schema_hint": True,
            "schema_hint_type_system": "postgresql",
        },
    )
    destination = Connection(
        name="qualification-output",
        connection_type="file",
        format="parquet",
        configure={"base_path": ".scratch/datatype-qualification"},
    )
    dataflow = DataFlow(
        source=Source(connection=source, table=table),
        destination=Destination(connection=destination, table="output"),
        transform=Transform(
            schema_hints=[
                SchemaHint(column_name="int8_value", data_type="int8"),
                SchemaHint(
                    column_name="numeric_value",
                    data_type="numeric",
                    precision=18,
                    scale=2,
                ),
                SchemaHint(
                    column_name="instant_value",
                    data_type="timestamp with time zone",
                ),
                SchemaHint(
                    column_name="wall_value",
                    data_type="timestamp without time zone",
                ),
            ],
        ),
    )

    with database.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE {table} ("
                "id SERIAL PRIMARY KEY, "
                "int8_value BIGINT, "
                "numeric_value NUMERIC(18,2), "
                "instant_value TIMESTAMP WITH TIME ZONE, "
                "wall_value TIMESTAMP WITHOUT TIME ZONE)"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {table} "
                "(int8_value,numeric_value,instant_value,wall_value) "
                "VALUES (:int8_value,:numeric_value,:instant_value,:wall_value), "
                "(NULL,NULL,NULL,NULL)"
            ),
            {
                "int8_value": 9223372036854775807,
                "numeric_value": "9999999999999999.99",
                "instant_value": "2024-01-15 03:30:45.123456+00",
                "wall_value": "2024-01-15 10:30:45",
            },
        )

    try:
        engine = PolarsEngine()
        query = (
            f"SELECT int8_value, numeric_value, instant_value, wall_value "
            f"FROM {table} ORDER BY id"
        )
        raw = engine.read_database(
            query=query,
            options={"database_type": "postgresql", "url": polars_url},
        )
        raw_schema = raw.collect_schema()
        assert raw_schema["int8_value"] == polars.Int64
        assert raw_schema["instant_value"] == polars.Datetime("us", "UTC")
        assert raw_schema["wall_value"] == polars.Datetime("us")

        converted = SchemaConverter(engine).transform(raw, dataflow)
        schema = converted.collect_schema()
        assert schema["numeric_value"] == polars.Decimal(18, 2)
        assert schema["instant_value"] == polars.Datetime("us", "UTC")
        assert schema["wall_value"] == polars.Datetime("us")

        frame = converted.collect()
        assert frame["int8_value"].to_list() == [9223372036854775807, None]
        assert str(frame["numeric_value"].to_list()[0]) == "9999999999999999.99"
        assert frame["numeric_value"].to_list()[1] is None
    finally:
        with database.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
