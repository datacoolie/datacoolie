"""Tests for RowFilter transformer."""

from __future__ import annotations

import pytest

from datacoolie.transformers.row_filter import RowFilter
from tests.unit.transformers.support import MockEngine, make_dataflow


class TestRowFilter:
    def test_order(self):
        engine = MockEngine()
        assert RowFilter(engine).order == 35

    def test_skipped_when_no_expression(self):
        engine = MockEngine()
        rf = RowFilter(engine)
        df = {"rows": 10}
        dataflow = make_dataflow()
        result = rf.transform(df, dataflow)
        assert result is df
        assert rf.applied_label is None

    def test_applies_expression(self):
        engine = MockEngine()
        rf = RowFilter(engine)
        df = {"rows": 10}
        dataflow = make_dataflow(filter_expression="amount > 100")
        result = rf.transform(df, dataflow)
        assert result is df  # MockEngine.filter_rows returns df unchanged
        assert rf.applied_label is not None

    def test_applied_label(self):
        engine = MockEngine()
        rf = RowFilter(engine)
        expr = "status != 'cancelled'"
        dataflow = make_dataflow(filter_expression=expr)
        rf.transform({}, dataflow)
        assert rf.applied_label == f"RowFilter({expr})"

    def test_skipped_on_empty_string(self):
        engine = MockEngine()
        rf = RowFilter(engine)
        dataflow = make_dataflow(filter_expression="")
        rf.transform({}, dataflow)
        assert rf.applied_label is None
