from __future__ import annotations

import builtins
from unittest.mock import Mock

import polars as pl
import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.core.qualified_names import QualifiedTableName
from datacoolie.engines._polars.relations import (
    AmbiguousTableReferenceError,
    DiscoveredTable,
    PolarsRelationRegistry,
)
from datacoolie.engines._polars.sql import PolarsSqlResolver


def relation(
    name: str, values: list[int], loader: Mock | None = None
) -> DiscoveredTable:
    return DiscoveredTable(
        logical_name=QualifiedTableName.parse(name),
        source_kind="delta",
        source_identifier=f"/{name}",
        loader=loader or Mock(return_value=pl.DataFrame({"id": values}).lazy()),
    )


def test_rewrites_qualified_sources_and_reuses_registered_frame() -> None:
    loader = Mock(return_value=pl.DataFrame({"id": [1, 2]}).lazy())
    orders = relation("catalog.db.schema.orders", [1, 2], loader)
    registry = PolarsRelationRegistry()
    registry.add_batch([orders])
    context = pl.SQLContext()
    resolver = PolarsSqlResolver()

    sql = "SELECT * FROM db.schema.orders WHERE id = 2"
    first = resolver.prepare(sql, registry=registry, sql_context=context)
    second = resolver.prepare(sql, registry=registry, sql_context=context)

    assert first == second
    assert first == f"SELECT * FROM {orders.alias} WHERE id = 2"
    assert context.execute(first).collect()["id"].to_list() == [2]
    loader.assert_called_once_with()


def test_cte_name_is_not_rewritten_but_its_external_source_is() -> None:
    orders = relation("catalog.db.schema.orders", [1])
    registry = PolarsRelationRegistry()
    registry.add_batch([orders])
    context = pl.SQLContext()
    sql = "WITH orders AS (SELECT * FROM catalog.db.schema.orders) SELECT * FROM orders"

    rewritten = PolarsSqlResolver().prepare(sql, registry=registry, sql_context=context)

    assert (
        rewritten
        == f"WITH orders AS (SELECT * FROM {orders.alias}) SELECT * FROM orders"
    )


def test_literals_comments_and_unrelated_text_are_preserved() -> None:
    orders = relation("catalog.db.schema.orders", [1])
    registry = PolarsRelationRegistry()
    registry.add_batch([orders])
    context = pl.SQLContext()
    sql = (
        "SELECT 'catalog.db.schema.orders' AS source_name "
        "-- catalog.db.schema.orders\nFROM db.schema.orders"
    )

    rewritten = PolarsSqlResolver().prepare(sql, registry=registry, sql_context=context)

    assert rewritten == (
        "SELECT 'catalog.db.schema.orders' AS source_name "
        f"-- catalog.db.schema.orders\nFROM {orders.alias}"
    )


def test_ambiguous_short_source_fails_before_binding() -> None:
    registry = PolarsRelationRegistry()
    registry.add_batch(
        [
            relation("cat_a.db.schema.orders", [1]),
            relation("cat_b.db.schema.orders", [2]),
        ]
    )
    context = pl.SQLContext()

    with pytest.raises(AmbiguousTableReferenceError):
        PolarsSqlResolver().prepare(
            "SELECT * FROM schema.orders", registry=registry, sql_context=context
        )
    assert context.tables() == []


def test_registry_change_invalidates_cached_resolution() -> None:
    registry = PolarsRelationRegistry()
    first = relation("cat_a.db.schema.orders", [1])
    registry.add_batch([first])
    context = pl.SQLContext()
    resolver = PolarsSqlResolver()
    sql = "SELECT * FROM orders"
    resolver.prepare(sql, registry=registry, sql_context=context)

    registry.add_batch([relation("cat_b.db.schema.orders", [2])])

    with pytest.raises(AmbiguousTableReferenceError):
        resolver.prepare(sql, registry=registry, sql_context=context)


def test_rejects_multiple_statements_before_loading() -> None:
    loader = Mock(return_value=pl.DataFrame({"id": [1]}).lazy())
    registry = PolarsRelationRegistry()
    registry.add_batch([relation("orders", [1], loader)])

    with pytest.raises(EngineError, match="exactly one"):
        PolarsSqlResolver().prepare(
            "SELECT * FROM orders; SELECT * FROM orders",
            registry=registry,
            sql_context=pl.SQLContext(),
        )
    loader.assert_not_called()


def test_missing_sqlglot_returns_optional_extra_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = PolarsRelationRegistry()
    registry.add_batch([relation("orders", [1])])
    original_import = builtins.__import__

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "sqlglot" or name.startswith("sqlglot."):
            raise ImportError("not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    with pytest.raises(EngineError, match=r"datacoolie\[polars-sql\]"):
        PolarsSqlResolver().prepare(
            "SELECT * FROM orders", registry=registry, sql_context=pl.SQLContext()
        )
