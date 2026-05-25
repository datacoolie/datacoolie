"""Tests for ColumnAdder and SystemColumnAdder transformers."""

from __future__ import annotations

import pytest

from datacoolie.core.models import AdditionalColumn
from datacoolie.transformers.column_adder import ColumnAdder, SystemColumnAdder, SCD2ColumnAdder
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


class TestSCD2ColumnAdder:
    def test_order_is_60(self, engine: MockEngine) -> None:
        adder = SCD2ColumnAdder(engine)
        assert adder.order == 60

    def test_skips_non_scd2_load_type(self, engine: MockEngine) -> None:
        from datacoolie.core.models import LoadType
        df = _make_dataflow(load_type=LoadType.MERGE_UPSERT.value)
        adder = SCD2ColumnAdder(engine)
        adder.transform({"id": 1}, df)
        assert len(engine._added_columns) == 0

    def test_skips_when_no_effective_column(self, engine: MockEngine) -> None:
        from datacoolie.core.models import LoadType
        df = _make_dataflow(load_type=LoadType.SCD2.value)
        adder = SCD2ColumnAdder(engine)
        adder.transform({"id": 1}, df)
        assert len(engine._added_columns) == 0

    def test_adds_scd2_columns_when_configured(self, engine: MockEngine) -> None:
        from datacoolie.core.models import LoadType, Connection, Source, Destination, DataFlow, Transform, Format
        src_conn = Connection(name="src", connection_type="lakehouse", format=Format.DELTA.value, configure={"base_path": "/a"})
        dst_conn = Connection(name="dst", connection_type="lakehouse", format=Format.DELTA.value, configure={"base_path": "/b"})
        df = DataFlow(
            name="t",
            source=Source(connection=src_conn, schema_name="raw", table="t", watermark_columns=[]),
            destination=Destination(connection=dst_conn, schema_name="cur", table="t2", load_type=LoadType.SCD2.value, merge_keys=[], partition_columns=[], configure={"scd2_effective_column": "updated_at"}),
            transform=Transform(additional_columns=[], schema_hints=[], latest_data_columns=[]),
        )
        adder = SCD2ColumnAdder(engine)
        adder.transform({"id": 1}, df)
        added_cols = [c[0] for c in engine._added_columns]
        assert "__valid_from" in added_cols
        assert "__valid_to" in added_cols
        assert "__is_current" in added_cols
