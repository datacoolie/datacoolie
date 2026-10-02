"""Qualification cells for local Polars Parquet and Delta round trips."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.datatype_qualification]


@pytest.mark.parametrize("output_format", ["parquet", "delta"])
def test_polars_persisted_schema_and_values(
    output_format: str, tmp_path: Path
) -> None:
    """A typed frame keeps decimal and temporal semantics after persistence."""

    polars = pytest.importorskip("polars")
    from datacoolie.engines.polars_engine import PolarsEngine

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
    path = tmp_path / output_format
    engine = PolarsEngine()
    engine.write_to_path(frame, str(path), mode="overwrite", fmt=output_format)

    read_back = engine.read_path(str(path), output_format).collect().sort("id")
    schema = read_back.schema
    assert schema["id"] == polars.Int64
    assert schema["amount"] == polars.Decimal(10, 2)
    assert schema["instant"] == polars.Datetime("us", "UTC")
    assert schema["wall_clock"] == polars.Datetime("us")
    assert read_back["id"].to_list() == [1, 2]
    assert str(read_back["amount"].to_list()[0]) == "12.30"
    assert read_back["amount"].to_list()[1] is None
    assert read_back["instant"].to_list()[0].timestamp() == pytest.approx(
        datetime(2024, 1, 15, 3, 30, 45, 123456, tzinfo=timezone.utc).timestamp()
    )
    assert read_back["wall_clock"].to_list()[0] == datetime(2024, 1, 15, 10, 30, 45)
