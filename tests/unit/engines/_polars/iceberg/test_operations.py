"""Focused tests for Iceberg catalog operations."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datacoolie.engines._polars.iceberg import operations


def test_table_exists_normalizes_catalog_prefix() -> None:
    catalog = MagicMock()
    catalog.table_exists.return_value = True
    assert operations.table_exists("catalog.namespace.table", catalog=catalog) is True
    catalog.table_exists.assert_called_once_with("namespace.table")


def test_table_exists_propagates_catalog_error() -> None:
    catalog = MagicMock()
    catalog.table_exists.side_effect = RuntimeError("offline")
    with pytest.raises(RuntimeError, match="offline"):
        operations.table_exists("namespace.table", catalog=catalog)


def test_write_table_creates_a_new_table_when_catalog_reports_absent() -> None:
    catalog = MagicMock()
    catalog.table_exists.return_value = False
    created = MagicMock()
    created.properties = {"schema.name-mapping.default": "{}"}
    catalog.create_table.return_value = created

    frame = pl.DataFrame({"id": [1]}).lazy()
    with patch.object(
        operations.iceberg_schema,
        "align_arrow_to_table",
        side_effect=lambda arrow, _table: arrow,
    ):
        operations.write_table(
            frame,
            "catalog.default.new_table",
            "overwrite",
            None,
            catalog=catalog,
        )

    catalog.create_namespace_if_not_exists.assert_called_once_with("default")
    catalog.create_table.assert_called_once()
    created.overwrite.assert_called_once()
