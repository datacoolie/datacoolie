"""Opt-in Iceberg REST/RustFS qualification for the Polars writer."""

from __future__ import annotations

import os
import warnings
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from tests.support.datatype_config import require_docker_services
from tests.support.s3_config import host_s3_config


pytestmark = [pytest.mark.integration, pytest.mark.datatype_qualification]


def test_polars_iceberg_rest_schema_and_values() -> None:
    """Verify a typed frame through the real REST catalog and object store."""

    require_docker_services(("rustfs", "iceberg-rest"))
    polars = pytest.importorskip("polars")
    load_catalog = pytest.importorskip("pyiceberg.catalog").load_catalog
    from datacoolie.engines.polars_engine import PolarsEngine

    catalog_uri = os.getenv(
        "DATACOOLIE_QUALIFICATION_ICEBERG_URI", "http://localhost:8181"
    )
    s3_config = host_s3_config()
    storage_options = s3_config.storage_options()
    catalog = load_catalog(
        "datacoolie-qualification",
        type="rest",
        uri=catalog_uri,
        **s3_config.iceberg_properties(),
    )
    namespace = f"datatype_qualification_{uuid4().hex[:10]}"
    table_name = f"{namespace}.typed_values"
    catalog.create_namespace_if_not_exists(namespace)
    frame = polars.DataFrame(
        {
            "id": polars.Series([1, 2], dtype=polars.Int64),
            "amount": polars.Series(
                [Decimal("12.30"), None], dtype=polars.Decimal(10, 2)
            ),
            "instant": polars.Series(
                [
                    datetime(2024, 1, 15, 3, 30, 45, 123456, tzinfo=timezone.utc),
                    None,
                ],
                dtype=polars.Datetime("us", "UTC"),
            ),
            "wall_clock": polars.Series(
                [datetime(2024, 1, 15, 10, 30, 45), None],
                dtype=polars.Datetime("us"),
            ),
        }
    ).lazy()
    engine = PolarsEngine(
        iceberg_catalog=catalog,
        storage_options=storage_options,
    )

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Delete operation did not match any records",
                category=UserWarning,
            )
            engine.write_to_table(frame, table_name, mode="overwrite", fmt="iceberg")
        table = catalog.load_table(table_name)
        arrow_schema = table.schema().as_arrow()
        assert str(arrow_schema.field("id").type) == "int64"
        assert str(arrow_schema.field("amount").type) == "decimal128(10, 2)"
        assert str(arrow_schema.field("instant").type) == "timestamp[us, tz=UTC]"
        assert str(arrow_schema.field("wall_clock").type) == "timestamp[us]"

        read_back = engine.read_table(table_name, fmt="iceberg").collect().sort("id")
        assert read_back.schema["id"] == polars.Int64
        assert read_back.schema["amount"] == polars.Decimal(10, 2)
        assert read_back.schema["instant"] == polars.Datetime("us", "UTC")
        assert read_back.schema["wall_clock"] == polars.Datetime("us")
        assert read_back["id"].to_list() == [1, 2]
        assert str(read_back["amount"].to_list()[0]) == "12.30"
        assert read_back["amount"].to_list()[1] is None
    finally:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Delete operation did not match any records",
                category=UserWarning,
            )
            try:
                catalog.drop_table(table_name)
            finally:
                warnings.filterwarnings(
                    "ignore",
                    message="Delete operation did not match any records",
                    category=UserWarning,
                )
                catalog.drop_namespace(namespace)
