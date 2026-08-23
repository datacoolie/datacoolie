"""Polars relation discovery, indexing, and optional eager registration."""

from __future__ import annotations

from typing import Any, Callable, Literal, Sequence

import polars as pl

from datacoolie.core.exceptions import EngineError
from datacoolie.core.qualified_names import NameInput
from datacoolie.engines._polars.relations import (
    DiscoveredTable,
    PatternInput,
    PolarsRelationRegistry,
)
from datacoolie.engines._polars.table_discovery import (
    discover_iceberg_catalog_tables,
    discover_path_tables,
)


def index_discovered_relations(
    registry: PolarsRelationRegistry,
    sql_context: pl.SQLContext,
    relations: Sequence[DiscoveredTable],
    *,
    failures: Sequence[str],
    preload: bool,
    on_error: Literal["raise", "skip"],
) -> list[str]:
    registry.add_batch(relations, failures=failures)
    names = sorted({str(relation.logical_name) for relation in relations})
    if not preload or not relations:
        return names
    indexed = [registry.get(relation.logical_name) for relation in relations]
    try:
        materialized = registry.materialize(indexed, sql_context)
    except EngineError as exc:
        if on_error == "raise":
            raise
        registry.update_last_report_failure(str(exc))
        return names
    registry.update_last_report_materialized(materialized)
    return names


def register_delta_tables(
    *,
    platform: Any,
    sql_context: pl.SQLContext,
    registry: PolarsRelationRegistry,
    loader_factory: Callable[[str], pl.LazyFrame],
    base_path: str,
    logical_prefix: NameInput | None = (),
    recursive: bool = False,
    max_depth: int | None = None,
    include: PatternInput = None,
    exclude: PatternInput = None,
    max_tables: int | None = None,
    preload: bool = False,
    on_error: Literal["raise", "skip"] = "raise",
) -> list[str]:
    if platform is None:
        raise EngineError(
            "register_delta_tables requires a platform — call set_platform() first"
        )
    relations, failures = discover_path_tables(
        platform=platform,
        base_path=base_path,
        marker_directory="_delta_log",
        source_kind="delta",
        loader_factory=loader_factory,
        logical_prefix=logical_prefix,
        recursive=recursive,
        max_depth=max_depth,
        include=include,
        exclude=exclude,
        max_tables=max_tables,
        on_error=on_error,
    )
    return index_discovered_relations(
        registry,
        sql_context,
        relations,
        failures=failures,
        preload=preload,
        on_error=on_error,
    )


def register_iceberg_tables(
    *,
    catalog: Any,
    platform: Any,
    storage_options: dict[str, str],
    sql_context: pl.SQLContext,
    registry: PolarsRelationRegistry,
    path_loader_factory: Callable[[str], pl.LazyFrame],
    namespace: NameInput | None = None,
    base_path: str | None = None,
    logical_prefix: NameInput | None = None,
    recursive: bool = False,
    max_depth: int | None = None,
    include: PatternInput = None,
    exclude: PatternInput = None,
    max_tables: int | None = None,
    preload: bool = False,
    on_error: Literal["raise", "skip"] = "raise",
) -> list[str]:
    if namespace is not None and base_path is not None:
        raise EngineError(
            "register_iceberg_tables: namespace and base_path are mutually exclusive"
        )
    if base_path is None and catalog is not None:
        scan_options: dict[str, Any] = {}
        if storage_options:
            scan_options["storage_options"] = storage_options

        def load_catalog_table(table_id: Any) -> pl.LazyFrame:
            return pl.scan_iceberg(catalog.load_table(table_id), **scan_options)

        relations, failures = discover_iceberg_catalog_tables(
            catalog=catalog,
            namespace=namespace,
            loader_factory=load_catalog_table,
            logical_prefix=logical_prefix,
            recursive=recursive,
            max_depth=max_depth,
            include=include,
            exclude=exclude,
            max_tables=max_tables,
            on_error=on_error,
        )
        return index_discovered_relations(
            registry,
            sql_context,
            relations,
            failures=failures,
            preload=preload,
            on_error=on_error,
        )
    if base_path is None:
        raise EngineError(
            "register_iceberg_tables: iceberg_catalog is not configured "
            "— provide base_path for path-based discovery or call set_iceberg_catalog()"
        )
    if platform is None:
        raise EngineError(
            "register_iceberg_tables with base_path requires a platform — call set_platform() first"
        )
    relations, failures = discover_path_tables(
        platform=platform,
        base_path=base_path,
        marker_directory="metadata",
        source_kind="iceberg",
        loader_factory=path_loader_factory,
        logical_prefix=logical_prefix,
        recursive=recursive,
        max_depth=max_depth,
        include=include,
        exclude=exclude,
        max_tables=max_tables,
        on_error=on_error,
    )
    return index_discovered_relations(
        registry,
        sql_context,
        relations,
        failures=failures,
        preload=preload,
        on_error=on_error,
    )
