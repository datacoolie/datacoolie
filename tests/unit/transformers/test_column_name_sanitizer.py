"""Tests for ColumnNameSanitizer transformer."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import ColumnCaseMode
from datacoolie.transformers.base import TransformerPipeline
from datacoolie.transformers.column_adder import SystemColumnAdder
from datacoolie.transformers.column_name_sanitizer import ColumnNameSanitizer
from datacoolie.utils.converters import to_lower_case, to_snake_case
from tests.unit.transformers.support import MockEngine, make_dataflow


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


_make_dataflow = make_dataflow


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
    def test_order_is_90(self, engine: MockEngine) -> None:
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

    def test_renames_camel_case_lower_mode(self, engine: MockEngine) -> None:
        """Default lower mode: no underscore insertion at word boundaries."""
        engine.set_columns(["myColumn", "HTTPStatus"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine)
        s.transform({"myColumn": 1, "HTTPStatus": 2}, df)
        renamed_map = {r[0]: r[1] for r in engine._renamed}
        assert renamed_map["myColumn"] == "mycolumn"
        assert renamed_map["HTTPStatus"] == "httpstatus"

    def test_renames_camel_case_snake_mode(self, engine: MockEngine) -> None:
        """Snake mode inserts underscores at word boundaries."""
        engine.set_columns(["myColumn", "HTTPStatus"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine, mode=ColumnCaseMode.SNAKE)
        s.transform({"myColumn": 1, "HTTPStatus": 2}, df)
        renamed_map = {r[0]: r[1] for r in engine._renamed}
        assert renamed_map["myColumn"] == "my_column"
        assert renamed_map["HTTPStatus"] == "http_status"

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

    def test_mode_defaults_to_lower(self, engine: MockEngine) -> None:
        s = ColumnNameSanitizer(engine)
        assert s._mode == ColumnCaseMode.LOWER

    def test_mode_accepts_string(self, engine: MockEngine) -> None:
        s = ColumnNameSanitizer(engine, mode="snake")
        assert s._mode == ColumnCaseMode.SNAKE

    def test_snake_mode_special_characters(self, engine: MockEngine) -> None:
        engine.set_columns(["Order Date", "order-id"])
        df = _make_dataflow()
        s = ColumnNameSanitizer(engine, mode=ColumnCaseMode.SNAKE)
        s.transform({"Order Date": 1, "order-id": 2}, df)
        renamed_map = {r[0]: r[1] for r in engine._renamed}
        assert renamed_map["Order Date"] == "order_date"
        assert renamed_map["order-id"] == "order_id"

    def test_invalid_mode_raises(self, engine: MockEngine) -> None:
        with pytest.raises(ValueError):
            ColumnNameSanitizer(engine, mode="invalid")


class TestToLowerCase:
    """Unit tests for the standalone to_lower_case helper."""

    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("already_lower", "already_lower"),
            ("MyColumn", "mycolumn"),
            ("myColumn", "mycolumn"),
            ("HTTPStatus", "httpstatus"),
            ("getHTTPSResponse", "gethttpsresponse"),
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
            ("__created_at", "__created_at"),
            ("_private", "_private"),
        ],
    )
    def test_conversion(self, input_name: str, expected: str) -> None:
        assert to_lower_case(input_name) == expected
