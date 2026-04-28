"""Tests for ColumnAdder and SystemColumnAdder transformers."""

from __future__ import annotations

import pytest

from datacoolie.core.models import AdditionalColumn
from datacoolie.transformers.column_adder import ColumnAdder, SystemColumnAdder
from tests.unit.transformers.support import MockEngine, make_dataflow


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


_make_dataflow = make_dataflow


class TestColumnAdder:
    def test_order_is_30(self, engine: MockEngine) -> None:
        ca = ColumnAdder(engine)
        assert ca.order == 30

    def test_removes_system_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow()
        ca = ColumnAdder(engine)
        ca.transform({"id": 1, "__created_at": "old"}, df)
        assert engine._removed_system is True

    def test_does_not_add_system_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow()
        ca = ColumnAdder(engine)
        result = ca.transform({"id": 1}, df)
        assert "__created_at" not in result
        assert "__updated_at" not in result
        assert "__updated_by" not in result

    def test_adds_additional_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            additional_cols=[
                AdditionalColumn(column="region", expression="'APAC'"),
                AdditionalColumn(column="year_val", expression="year(order_date)"),
            ]
        )
        ca = ColumnAdder(engine)
        ca.transform({"id": 1}, df)
        assert len(engine._added_columns) == 2
        assert ("region", "'APAC'") in engine._added_columns

    def test_no_additional_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow(additional_cols=[])
        ca = ColumnAdder(engine)
        ca.transform({"id": 1}, df)
        assert len(engine._added_columns) == 0

    def test_skips_additional_column_without_expression(self, engine: MockEngine) -> None:
        # Construct invalid metadata intentionally to cover defensive branch.
        additional = [AdditionalColumn.model_construct(column="region", expression="")]
        df = _make_dataflow(additional_cols=additional)
        ca = ColumnAdder(engine)
        result = ca.transform({"id": 1}, df)
        assert result["id"] == 1
        assert len(engine._added_columns) == 0


class TestSystemColumnAdder:
    def test_order_is_70(self, engine: MockEngine) -> None:
        sca = SystemColumnAdder(engine)
        assert sca.order == 70

    def test_adds_system_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow()
        sca = SystemColumnAdder(engine)
        result = sca.transform({"id": 1}, df)
        assert "__created_at" in result
        assert "__updated_at" in result
        assert "__updated_by" in result

    def test_custom_author(self, engine: MockEngine) -> None:
        df = _make_dataflow()
        sca = SystemColumnAdder(engine, author="MyApp")
        result = sca.transform({"id": 1}, df)
        assert result["__updated_by"] == "MyApp"
