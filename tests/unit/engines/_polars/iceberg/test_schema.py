"""Focused tests for Iceberg schema planning."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from datacoolie.engines._polars.iceberg.schema import (
    align_arrow_to_schema,
    expected_column_order,
    snapshot_schema_is_stale,
    table_id,
)


@pytest.mark.parametrize(
    ("qualified", "expected"),
    [
        ("table", "table"),
        ("schema.table", "schema.table"),
        ("cat.db.schema.table", "db.schema.table"),
    ],
)
def test_table_id(qualified: str, expected: str) -> None:
    assert table_id(qualified) == expected


def test_expected_column_order_moves_technical_tail() -> None:
    assert expected_column_order(["id", "__created_at", "value", "__file_name"]) == (
        "id",
        "value",
        "__file_name",
        "__created_at",
    )


def test_snapshot_staleness_is_structured() -> None:
    table = MagicMock()
    table.current_snapshot.return_value = SimpleNamespace(schema_id=1)
    table.metadata.current_schema_id = 2
    assert snapshot_schema_is_stale(table) is True


def test_arrow_alignment_backfills_and_reorders() -> None:
    pa = pytest.importorskip("pyarrow")
    source = pa.table({"ID": [1]})
    target = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
    result = align_arrow_to_schema(source, target)
    assert result.column_names == ["id", "value"]
    assert result["value"].null_count == 1
