from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import Mock

import polars as pl
import pytest

from datacoolie.core.qualified_names import QualifiedTableName
from datacoolie.engines._polars.relations import (
    AmbiguousTableReferenceError,
    DiscoveredTable,
    DuplicateTableRegistrationError,
    PolarsRelationRegistry,
    RelationMaterializationError,
    logical_name_selected,
)


def descriptor(name: str, source: str, loader: Mock | None = None) -> DiscoveredTable:
    return DiscoveredTable(
        logical_name=QualifiedTableName.parse(name),
        source_kind="delta",
        source_identifier=source,
        loader=loader or Mock(return_value=pl.DataFrame({"id": [1]}).lazy()),
    )


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("catalog_a.database_b.**", True),
        ("database_b.**", True),
        ("database_b.**.d_*", True),
        ("d_*", True),
        ("database_x.**", False),
    ],
)
def test_hierarchical_patterns_match_full_name_and_suffixes(
    pattern: str, expected: bool
) -> None:
    name = QualifiedTableName.parse("catalog_A.database_B.sales.d_orders")
    assert logical_name_selected(name, include=pattern) is expected


def test_exclude_wins_over_include() -> None:
    name = QualifiedTableName.parse("catalog.database.tmp.d_orders_backup")
    assert not logical_name_selected(
        name,
        include="database.**.d_*",
        exclude=("**.tmp.**", "**.*_backup"),
    )


def test_suffix_resolution_and_ambiguity() -> None:
    registry = PolarsRelationRegistry()
    first = descriptor("cat_a.db.sales.orders", "/a")
    second = descriptor("cat_b.db.sales.orders", "/b")
    registry.add_batch([first, second])

    assert registry.resolve(("cat_a", "db", "sales", "orders")) is first
    with pytest.raises(AmbiguousTableReferenceError, match="cat_a.db.sales.orders"):
        registry.resolve(("sales", "orders"))


def test_quoted_components_require_exact_case() -> None:
    registry = PolarsRelationRegistry()
    relation = descriptor("Catalog.DB.Orders", "/orders")
    registry.add_batch([relation])

    assert registry.resolve(("orders",), quoted=(False,)) is relation
    assert registry.resolve(("Orders",), quoted=(True,)) is relation
    assert registry.resolve(("orders",), quoted=(True,)) is None


def test_duplicate_same_source_is_idempotent_but_conflict_fails() -> None:
    registry = PolarsRelationRegistry()
    registry.add_batch([descriptor("db.orders", "/same")])
    report = registry.add_batch([descriptor("DB.ORDERS", "/same")])
    assert report.skipped == ("DB.ORDERS",)

    with pytest.raises(DuplicateTableRegistrationError):
        registry.add_batch([descriptor("db.orders", "/different")])


def test_loader_runs_once_and_context_registration_is_reused() -> None:
    loader = Mock(return_value=pl.DataFrame({"id": [1]}).lazy())
    relation = descriptor("catalog.db.schema.orders", "/orders", loader)
    registry = PolarsRelationRegistry()
    registry.add_batch([relation])
    context = pl.SQLContext()

    registry.materialize([relation], context)
    registry.materialize([relation], context)

    loader.assert_called_once_with()
    assert relation.alias in context.tables()


def test_concurrent_first_use_loads_and_registers_once() -> None:
    loader = Mock(return_value=pl.DataFrame({"id": [1]}).lazy())
    relation = descriptor("catalog.db.schema.orders", "/orders", loader)
    registry = PolarsRelationRegistry()
    registry.add_batch([relation])
    context = pl.SQLContext()
    start = Barrier(2)

    def materialize() -> tuple[str, ...]:
        start.wait()
        return registry.materialize([relation], context)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: materialize(), range(2)))

    loader.assert_called_once_with()
    assert sorted(len(result) for result in results) == [0, 1]
    assert context.tables() == [relation.alias]


def test_external_alias_unregister_rebinds_cached_frame_without_reloading() -> None:
    loader = Mock(return_value=pl.DataFrame({"id": [1]}).lazy())
    relation = descriptor("db.orders", "/orders", loader)
    registry = PolarsRelationRegistry()
    registry.add_batch([relation])
    context = pl.SQLContext()
    registry.materialize([relation], context)

    context.unregister(relation.alias)
    registry.materialize([relation], context)

    loader.assert_called_once_with()
    assert context.tables() == [relation.alias]


def test_failed_batch_load_registers_nothing() -> None:
    good = descriptor("db.good", "/good")
    bad = descriptor("db.bad", "/bad", Mock(side_effect=RuntimeError("boom")))
    registry = PolarsRelationRegistry()
    registry.add_batch([good, bad])
    context = pl.SQLContext()

    with pytest.raises(RelationMaterializationError, match="boom"):
        registry.materialize([good, bad], context)

    assert context.tables() == []
    assert good.state == "indexed"
