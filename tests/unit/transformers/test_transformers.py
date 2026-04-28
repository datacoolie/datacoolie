"""Tests for datacoolie.transformers — pipeline, schema, dedup, columns, partition."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import (
    LoadType,
)
from datacoolie.core.models import (
    AdditionalColumn,
    PartitionColumn,
    SchemaHint,
)
from datacoolie.transformers.base import (
    TransformerPipeline,
)
from datacoolie.transformers.column_adder import ColumnAdder, SystemColumnAdder
from datacoolie.transformers.column_name_sanitizer import ColumnNameSanitizer
from datacoolie.transformers.deduplicator import Deduplicator
from datacoolie.transformers.partition_handler import PartitionHandler
from datacoolie.transformers.schema_converter import SchemaConverter
from datacoolie.utils.converters import to_snake_case
from tests.unit.transformers.support import MockEngine, make_dataflow


# ============================================================================
# Fixtures / helpers
# ============================================================================


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


_make_dataflow = make_dataflow


# ============================================================================
# SchemaConverter tests
# ============================================================================


class TestSchemaConverter:
    def test_order_is_10(self, engine: MockEngine) -> None:
        sc = SchemaConverter(engine)
        assert sc.order == 10

    def test_noop_when_schema_hint_disabled(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            use_schema_hint=False,
            schema_hints=[
                SchemaHint(column_name="amount", data_type="DECIMAL", precision=18, scale=2),
            ],
        )
        sc = SchemaConverter(engine)
        sc.transform({"amount": 100}, df)
        assert len(engine._casts) == 0

    def test_casts_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            use_schema_hint=True,
            schema_hints=[
                SchemaHint(column_name="order_date", data_type="DATE", format="yyyy-MM-dd"),
                SchemaHint(column_name="amount", data_type="DECIMAL", precision=18, scale=2),
            ],
        )
        sc = SchemaConverter(engine)
        sc.transform({"order_date": "x", "amount": "y"}, df)
        assert len(engine._casts) == 2
        # Check types were mapped
        cast_types = {c[0]: c[1] for c in engine._casts}
        assert cast_types["order_date"] == "DATE"
        assert cast_types["amount"] == "DECIMAL(18,2)"

    def test_skips_inactive_hints(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            use_schema_hint=True,
            schema_hints=[
                SchemaHint(column_name="amount", data_type="INT", is_active=False),
            ],
        )
        sc = SchemaConverter(engine)
        sc.transform({"amount": 100}, df)
        assert len(engine._casts) == 0

    def test_skips_missing_columns(self, engine: MockEngine) -> None:
        engine.set_columns(["id"])
        df = _make_dataflow(
            use_schema_hint=True,
            schema_hints=[
                SchemaHint(column_name="nonexistent", data_type="STRING"),
            ],
        )
        sc = SchemaConverter(engine)
        sc.transform({"id": 1}, df)
        assert len(engine._casts) == 0

    def test_case_insensitive_column_match(self, engine: MockEngine) -> None:
        engine.set_columns(["Amount"])
        df = _make_dataflow(
            use_schema_hint=True,
            schema_hints=[
                SchemaHint(column_name="amount", data_type="INT"),
            ],
        )
        sc = SchemaConverter(engine)
        sc.transform({"Amount": 100}, df)
        assert len(engine._casts) == 1
        assert engine._casts[0][0] == "Amount"

    def test_no_hints_noop(self, engine: MockEngine) -> None:
        df = _make_dataflow(use_schema_hint=True, schema_hints=[])
        sc = SchemaConverter(engine)
        sc.transform({"x": 1}, df)
        assert len(engine._casts) == 0

    def test_no_timestamp_ntz_conversion_when_disabled(self, engine: MockEngine) -> None:
        # Use a direct mock assignment to verify branch behavior.
        engine.convert_timestamp_ntz_to_timestamp = MagicMock(side_effect=lambda x: x)
        df = _make_dataflow(
            use_schema_hint=True,
            schema_hints=[],
            transform_configure={"convert_timestamp_ntz": False},
        )
        sc = SchemaConverter(engine)
        sc.transform({"x": 1}, df)
        engine.convert_timestamp_ntz_to_timestamp.assert_not_called()

    def test_unknown_schema_hint_type_is_passed_through(self, engine: MockEngine) -> None:
        engine.set_columns(["amount"])
        df = _make_dataflow(
            use_schema_hint=True,
            schema_hints=[
                SchemaHint(column_name="amount", data_type="UNKNOWN_TYPE"),
            ],
        )
        sc = SchemaConverter(engine)
        sc.transform({"amount": 100}, df)
        assert len(engine._casts) == 1
        assert engine._casts[0][1] == "UNKNOWN_TYPE"


class TestDeduplicator:
    def test_order_is_20(self, engine: MockEngine) -> None:
        d = Deduplicator(engine)
        assert d.order == 20

    def test_noop_no_partition_cols(self, engine: MockEngine) -> None:
        df = _make_dataflow(dedup_cols=None, latest_cols=["modified_at"])
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert not engine._deduplicated
        assert not engine._dedup_by_rank

    def test_noop_no_latest_cols_and_no_watermark(self, engine: MockEngine) -> None:
        """No latest_data_columns and no source watermark_columns \u2192 no-op."""
        df = _make_dataflow(dedup_cols=["id"], latest_cols=[], watermark_cols=[])
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert not engine._deduplicated
        assert not engine._dedup_by_rank

    def test_row_number_dedup_for_merge_upsert(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True
        assert engine._dedup_by_rank is False

    def test_rank_dedup_for_merge_overwrite(self, engine: MockEngine) -> None:
        """MERGE_OVERWRITE + merge_keys + no explicit dedup_cols → rank."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["id"],
            dedup_cols=None,
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._dedup_by_rank is True
        assert engine._deduplicated is False

    def test_merge_overwrite_explicit_dedup_uses_row_number(self, engine: MockEngine) -> None:
        """MERGE_OVERWRITE with explicit dedup_cols → ROW_NUMBER, not rank."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["id"],
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True
        assert engine._dedup_by_rank is False

    def test_merge_overwrite_no_merge_keys_uses_row_number(self, engine: MockEngine) -> None:
        """MERGE_OVERWRITE but merge_keys is empty → ROW_NUMBER."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=[],
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True
        assert engine._dedup_by_rank is False

    def test_rank_dedup_via_configure_flag(self, engine: MockEngine) -> None:
        """transform.configure[deduplicate_by_rank]=True → rank regardless of load type."""
        df = _make_dataflow(
            load_type=LoadType.APPEND.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
            transform_configure={"deduplicate_by_rank": True},
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._dedup_by_rank is True
        assert engine._deduplicated is False

    def test_watermark_fallback_for_order_cols(self, engine: MockEngine) -> None:
        """latest_data_columns is empty → falls back to source.watermark_columns."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            dedup_cols=["id"],
            latest_cols=[],
            watermark_cols=["updated_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True

    def test_noop_no_order_cols_and_no_watermark(self, engine: MockEngine) -> None:
        """latest_data_columns and watermark_columns both empty → no-op."""
        df = _make_dataflow(
            dedup_cols=["id"],
            latest_cols=[],
            watermark_cols=[],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert not engine._deduplicated
        assert not engine._dedup_by_rank

    def test_row_number_dedup_for_overwrite(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.OVERWRITE.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True

    def test_row_number_dedup_for_append(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.APPEND.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True

    def test_fallback_to_merge_keys(self, engine: MockEngine) -> None:
        """When deduplicate_columns is None, falls back to merge_keys."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=["order_id"],
            dedup_cols=None,
            latest_cols=["modified_at"],
        )
        # deduplicate_columns on DataFlow falls back to merge_keys
        assert df.deduplicate_columns == ["order_id"]
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True


# ============================================================================
# ColumnAdder tests
# ============================================================================


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


# ============================================================================
# SystemColumnAdder tests
# ============================================================================


class TestSystemColumnAdder:
    def test_order_is_60(self, engine: MockEngine) -> None:
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

    def test_is_last_in_pipeline(self, engine: MockEngine) -> None:
        """SystemColumnAdder has the second-highest order (70) in the default pipeline."""
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


class TestToSnakeCase:
    """Unit tests for the standalone to_snake_case helper."""

    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("already_snake", "already_snake"),
            ("MyColumn", "my_column"),
            ("myColumn", "my_column"),
            ("HTTPStatus", "http_status"),
            ("getHTTPSResponse", "get_https_response"),
            ("Column Name", "column_name"),
            ("order-date", "order_date"),
            ("Column.Name", "column_name"),
            ("col@#name", "col_name"),
            ("   spaces   ", "spaces"),
            ("multiple   spaces", "multiple_spaces"),
            ("123", "_123"),
            ("123abc", "_123abc"),
            ("col_123", "col_123"),
            ("ALLCAPS", "allcaps"),
            ("a", "a"),
            # underscore-prefix passthrough
            ("__created_at", "__created_at"),
            ("_private", "_private"),
        ],
    )
    def test_conversion(self, input_name: str, expected: str) -> None:
        assert to_snake_case(input_name) == expected


class TestColumnNameSanitizer:
    def test_order_is_50(self, engine: MockEngine) -> None:
        s = ColumnNameSanitizer(engine)
        assert s.order == 90

    def test_renames_special_characters(self, engine: MockEngine) -> None:
        engine.set_columns(["Order Date", "order-id", "Amount$"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({"Order Date": 1, "order-id": 2, "Amount$": 3}, df)
        renamed_map = {r[0]: r[1] for r in engine._renamed}
        assert renamed_map["Order Date"] == "order_date"
        assert renamed_map["order-id"] == "order_id"
        assert renamed_map["Amount$"] == "amount"

    def test_renames_numeric_columns(self, engine: MockEngine) -> None:
        engine.set_columns(["123", "456", "name"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({"123": "a", "456": "b", "name": "c"}, df)
        renamed_map = {r[0]: r[1] for r in engine._renamed}
        assert renamed_map["123"] == "_123"
        assert renamed_map["456"] == "_456"
        # "name" is already clean snake_case — not renamed
        assert "name" not in renamed_map

    def test_renames_camel_case(self, engine: MockEngine) -> None:
        engine.set_columns(["myColumn", "HTTPStatus"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({"myColumn": 1, "HTTPStatus": 2}, df)
        renamed_map = {r[0]: r[1] for r in engine._renamed}
        assert renamed_map["myColumn"] == "mycolumn"
        assert renamed_map["HTTPStatus"] == "httpstatus"

    def test_noop_for_clean_columns(self, engine: MockEngine) -> None:
        engine.set_columns(["id", "user_name", "created_at"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({"id": 1, "user_name": "a", "created_at": "b"}, df)
        assert len(engine._renamed) == 0

    def test_skips_underscore_prefixed_columns(self, engine: MockEngine) -> None:
        """Columns starting with _ (system columns) are never renamed."""
        engine.set_columns(["__created_at", "__updated_at", "_internal"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({}, df)
        assert len(engine._renamed) == 0

    def test_is_before_system_column_adder(self, engine: MockEngine) -> None:
        """ColumnNameSanitizer (90) runs after SystemColumnAdder (70)."""
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(ColumnNameSanitizer(engine))
        pipeline.add_transformer(SystemColumnAdder(engine))
        orders = [t.order for t in pipeline.transformers]
        assert orders == [70, 90]


# ============================================================================
# PartitionHandler tests
# ============================================================================


class TestPartitionHandler:
    def test_order_is_40(self, engine: MockEngine) -> None:
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
