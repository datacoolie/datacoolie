from __future__ import annotations

from unittest.mock import Mock

import pytest

from datacoolie.engines._polars.relations import RegistrationLimitError
from datacoolie.engines._polars.table_discovery import (
    discover_iceberg_catalog_tables,
    discover_path_tables,
)


def test_recursive_delta_discovery_stops_at_table_and_does_not_load() -> None:
    platform = Mock()
    folders = {
        "/lake": ["/lake/db"],
        "/lake/db": ["/lake/db/schema"],
        "/lake/db/schema": ["/lake/db/schema/orders"],
    }

    def list_folders(path: str) -> list[str]:
        if path not in folders:
            pytest.fail(f"unexpected traversal below table: {path}")
        return folders[path]

    platform.list_folders.side_effect = list_folders
    platform.folder_exists.side_effect = lambda path: (
        path == "/lake/db/schema/orders/_delta_log"
    )
    loader = Mock()

    relations, failures = discover_path_tables(
        platform=platform,
        base_path="/lake",
        marker_directory="_delta_log",
        source_kind="delta",
        loader_factory=loader,
        logical_prefix=("catalog",),
        recursive=True,
    )

    assert failures == ()
    assert [str(item.logical_name) for item in relations] == [
        "catalog.db.schema.orders"
    ]
    loader.assert_not_called()


def test_path_discovery_filters_before_loader_and_enforces_limit() -> None:
    platform = Mock()
    platform.list_folders.return_value = ["/root/d_orders", "/root/tmp"]
    platform.folder_exists.return_value = True
    loader = Mock()

    relations, _ = discover_path_tables(
        platform=platform,
        base_path="/root",
        marker_directory="_delta_log",
        source_kind="delta",
        loader_factory=loader,
        include="d_*",
    )
    assert [str(item.logical_name) for item in relations] == ["d_orders"]
    loader.assert_not_called()

    with pytest.raises(RegistrationLimitError):
        discover_path_tables(
            platform=platform,
            base_path="/root",
            marker_directory="_delta_log",
            source_kind="delta",
            loader_factory=loader,
            max_tables=1,
        )


def test_iceberg_catalog_mapping_and_filter_happen_before_load() -> None:
    catalog = Mock()
    catalog.name = "catalog_A"
    catalog.list_tables.side_effect = lambda namespace: {
        ("database_B",): [("database_B", "orders")],
        ("database_B", "sales"): [("database_B", "sales", "d_sales")],
    }.get(tuple(namespace), [])
    catalog.list_namespaces.side_effect = lambda namespace: (
        [("database_B", "sales")] if tuple(namespace) == ("database_B",) else []
    )
    loader = Mock()

    relations, failures = discover_iceberg_catalog_tables(
        catalog=catalog,
        namespace=("database_B",),
        loader_factory=loader,
        recursive=True,
        include="database_B.**.d_*",
    )

    assert failures == ()
    assert [str(item.logical_name) for item in relations] == [
        "catalog_A.database_B.sales.d_sales"
    ]
    loader.assert_not_called()
