"""Focused tests for Iceberg catalog operations."""

from unittest.mock import MagicMock

from datacoolie.engines._polars.iceberg.operations import table_exists


def test_table_exists_normalizes_catalog_prefix() -> None:
    catalog = MagicMock()
    catalog.table_exists.return_value = True
    assert table_exists("catalog.namespace.table", catalog=catalog) is True
    catalog.table_exists.assert_called_once_with("namespace.table")


def test_table_exists_treats_catalog_error_as_absent() -> None:
    catalog = MagicMock()
    catalog.table_exists.side_effect = RuntimeError("offline")
    assert table_exists("namespace.table", catalog=catalog) is False
