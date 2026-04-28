"""Tests for SchemaConverter transformer."""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from datacoolie.core.models import SchemaHint
from datacoolie.transformers.schema_converter import SchemaConverter
from tests.unit.transformers.support import MockEngine, make_dataflow


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


_make_dataflow = make_dataflow


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
        # Unknown types are passed through to the engine unchanged
        assert len(engine._casts) == 1
        assert engine._casts[0][1] == "UNKNOWN_TYPE"


class TestBuildTypeString:
    def test_plain_type_returned_as_is(self) -> None:
        hint = SchemaHint(column_name="x", data_type="DATE")
        assert SchemaConverter._build_type_string(hint) == "DATE"

    def test_precision_and_scale_appended(self) -> None:
        hint = SchemaHint(column_name="x", data_type="DECIMAL", precision=18, scale=2)
        assert SchemaConverter._build_type_string(hint) == "DECIMAL(18,2)"

    def test_precision_without_scale_defaults_zero(self) -> None:
        hint = SchemaHint(column_name="x", data_type="DECIMAL", precision=10)
        assert SchemaConverter._build_type_string(hint) == "DECIMAL(10,0)"

    def test_varchar_plain(self) -> None:
        hint = SchemaHint(column_name="x", data_type="VARCHAR")
        assert SchemaConverter._build_type_string(hint) == "VARCHAR"

    def test_numeric_with_precision(self) -> None:
        hint = SchemaHint(column_name="x", data_type="NUMERIC", precision=12, scale=4)
        assert SchemaConverter._build_type_string(hint) == "NUMERIC(12,4)"
