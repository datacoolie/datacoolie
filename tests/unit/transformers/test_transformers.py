"""Cross-transformer ordering and remaining partition/sanitizer contracts."""

from __future__ import annotations


import pytest

from datacoolie.core.models.destination import PartitionColumn
from datacoolie.transformers.base import TransformerPipeline
from datacoolie.transformers.column_adder import ColumnAdder, SystemColumnAdder
from datacoolie.transformers.column_name_sanitizer import ColumnNameSanitizer
from datacoolie.transformers.deduplicator import Deduplicator
from datacoolie.transformers.partition_handler import PartitionHandler
from datacoolie.transformers.schema_converter import SchemaConverter
from tests.unit.transformers.support import MockEngine, make_dataflow


# ============================================================================
# Fixtures / helpers
# ============================================================================


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


_make_dataflow = make_dataflow


# ============================================================================
# SystemColumnAdder tests
# ============================================================================


class TestSystemColumnAdder:
    def test_sanitizer_runs_last_in_pipeline(self, engine: MockEngine) -> None:
        """Sanitization runs after partition and system columns have been added."""
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(SchemaConverter(engine))
        pipeline.add_transformer(Deduplicator(engine))
        pipeline.add_transformer(ColumnAdder(engine))
        pipeline.add_transformer(PartitionHandler(engine))
        pipeline.add_transformer(ColumnNameSanitizer(engine))
        pipeline.add_transformer(SystemColumnAdder(engine))
        assert pipeline.transformers[-1].order == 90
        assert isinstance(pipeline.transformers[-1], ColumnNameSanitizer)


# ============================================================================
# ColumnNameSanitizer tests
# ============================================================================


class TestColumnNameSanitizer:
    def test_renames_special_characters(self, engine: MockEngine) -> None:
        engine.set_columns(["Order Date", "order-id", "Amount$"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({"Order Date": 1, "order-id": 2, "Amount$": 3}, df)
        renamed_map = {r[0]: r[1] for r in engine._renamed}
        assert renamed_map["Order Date"] == "order_date"
        assert renamed_map["order-id"] == "order_id"
        assert renamed_map["Amount$"] == "amount"

    def test_noop_for_clean_columns(self, engine: MockEngine) -> None:
        engine.set_columns(["id", "user_name", "created_at"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({"id": 1, "user_name": "a", "created_at": "b"}, df)
        assert len(engine._renamed) == 0


# ============================================================================
# PartitionHandler tests
# ============================================================================


class TestPartitionHandler:
    def test_order_is_80(self, engine: MockEngine) -> None:
        ph = PartitionHandler(engine)
        assert ph.order == 80

    def test_adds_partition_columns_with_expression(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            partition_cols=[
                PartitionColumn(column="year", expression="year(order_date)"),
                PartitionColumn(column="month", expression="month(order_date)"),
            ]
        )
        ph = PartitionHandler(engine)
        ph.transform({"order_date": "2024-01-15"}, df)
        assert len(engine._added_columns) == 2
        assert ("year", "year(order_date)") in engine._added_columns
        assert ("month", "month(order_date)") in engine._added_columns

    def test_skips_columns_without_expression(self, engine: MockEngine) -> None:
        engine.set_columns(["id", "region"])
        df = _make_dataflow(
            partition_cols=[
                PartitionColumn(column="region"),  # no expression
            ]
        )
        ph = PartitionHandler(engine)
        ph.transform({"id": 1, "region": "US"}, df)
        assert len(engine._added_columns) == 0

    def test_no_partition_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow(partition_cols=[])
        ph = PartitionHandler(engine)
        ph.transform({"id": 1}, df)
        assert len(engine._added_columns) == 0

    def test_warns_missing_column_no_expression(self, engine: MockEngine) -> None:
        engine.set_columns(["id"])
        df = _make_dataflow(
            partition_cols=[
                PartitionColumn(column="missing_col"),
            ]
        )
        ph = PartitionHandler(engine)
        # Should not raise, just warn
        ph.transform({"id": 1}, df)
        assert len(engine._added_columns) == 0
