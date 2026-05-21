"""Tests for watermark_end (upper watermark bound) across all readers.

Verifies that the watermark_end parameter is correctly passed through
the read pipeline and applied via _apply_watermark_filter for each reader.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from datacoolie.core.models import Connection, Source
from datacoolie.sources.delta_reader import DeltaReader
from datacoolie.sources.iceberg_reader import IcebergReader
from datacoolie.sources.database_reader import DatabaseReader
from datacoolie.sources.file_reader import FileReader
from datacoolie.sources.python_function_reader import PythonFunctionReader

from tests.unit.sources.support import MockEngine, delta_source, engine, db_source, file_source


# ============================================================================
# DeltaReader with watermark_end
# ============================================================================


class TestDeltaReaderUpperWatermark:
    def test_read_with_both_bounds(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-15"})
        reader = DeltaReader(engine)
        lower = {"modified_at": "2024-06-01"}
        upper = {"modified_at": "2024-06-20"}
        result = reader.read(delta_source, watermark_start=lower, watermark_end=upper)
        assert result is not None
        assert engine._filtered is True
        assert engine._filter_end == {"modified_at": "2024-06-20"}
        assert engine._filter_watermark == {"modified_at": "2024-06-01"}

    def test_read_with_end_only(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-15"})
        reader = DeltaReader(engine)
        upper = {"modified_at": "2024-06-20"}
        result = reader.read(delta_source, watermark_start=None, watermark_end=upper)
        assert result is not None
        assert engine._filtered is True
        assert engine._filter_end == {"modified_at": "2024-06-20"}

    def test_read_without_end_unchanged(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-15"})
        reader = DeltaReader(engine)
        lower = {"modified_at": "2024-06-01"}
        result = reader.read(delta_source, watermark_start=lower)
        assert result is not None
        assert engine._filtered is True
        # No end bound — _filter_end should be None
        assert engine._filter_end is None


# ============================================================================
# IcebergReader with watermark_end
# ============================================================================


class TestIcebergReaderUpperWatermark:
    def test_read_with_both_bounds(self, engine: MockEngine) -> None:
        conn = Connection(
            name="iceberg_conn",
            connection_type="lakehouse",
            format="iceberg",
            configure={"base_path": "/data/iceberg"},
        )
        src = Source(connection=conn, table="events", watermark_columns=["updated_at"])
        engine.set_max_values({"updated_at": "2024-07-10"})
        reader = IcebergReader(engine)
        lower = {"updated_at": "2024-07-01"}
        upper = {"updated_at": "2024-07-15"}
        result = reader.read(src, watermark_start=lower, watermark_end=upper)
        assert result is not None
        assert engine._filter_watermark == {"updated_at": "2024-07-01"}
        assert engine._filter_end == {"updated_at": "2024-07-15"}


# ============================================================================
# DatabaseReader with watermark_end
# ============================================================================


class TestDatabaseReaderUpperWatermark:
    def test_read_with_end_generates_window_sql(self, engine: MockEngine, db_source: Source) -> None:
        engine.set_max_values({"updated_at": "2024-06-15"})
        reader = DatabaseReader(engine)
        lower = {"updated_at": "2024-06-01"}
        upper = {"updated_at": "2024-06-20"}
        result = reader.read(db_source, watermark_start=lower, watermark_end=upper)
        assert result is not None
        # DatabaseReader pushes window to SQL
        assert engine._last_query is not None
        assert "updated_at > '2024-06-01'" in engine._last_query
        assert "updated_at < '2024-06-20'" in engine._last_query

    def test_read_with_end_only_generates_upper_sql(self, engine: MockEngine, db_source: Source) -> None:
        engine.set_max_values({"updated_at": "2024-06-15"})
        reader = DatabaseReader(engine)
        upper = {"updated_at": "2024-06-20"}
        result = reader.read(db_source, watermark_start=None, watermark_end=upper)
        assert result is not None
        assert engine._last_query is not None
        assert "updated_at < '2024-06-20'" in engine._last_query

    def test_build_window_where_clause_both_bounds(self) -> None:
        clause = DatabaseReader._build_window_where_clause(
            lower={"col_a": "2024-01-01"},
            upper={"col_a": "2024-02-01"},
        )
        assert "col_a > '2024-01-01'" in clause
        assert "col_a < '2024-02-01'" in clause
        assert "AND" in clause

    def test_build_window_where_clause_multi_column(self) -> None:
        clause = DatabaseReader._build_window_where_clause(
            lower={"col_a": 10, "col_b": 50},
            upper={"col_a": 20, "col_b": 90},
        )
        assert "col_a > 10" in clause
        assert "col_a < 20" in clause
        assert "col_b > 50" in clause
        assert "col_b < 90" in clause
        assert "OR" in clause

    def test_build_window_where_clause_asymmetric(self) -> None:
        """Lower has col_a, upper has col_b — each generates single condition."""
        clause = DatabaseReader._build_window_where_clause(
            lower={"col_a": 10},
            upper={"col_b": 90},
        )
        assert "col_a > 10" in clause
        assert "col_b < 90" in clause

    def test_build_window_where_clause_inclusive_lower(self) -> None:
        clause = DatabaseReader._build_window_where_clause(
            lower={"col": 5},
            upper={"col": 15},
            lower_op=">=",
        )
        assert "col >= 5" in clause
        assert "col < 15" in clause


# ============================================================================
# FileReader with watermark_upper
# ============================================================================


class TestFileReaderUpperWatermark:
    def test_read_with_upper_bound(self, engine: MockEngine, file_source: Source) -> None:
        engine.set_max_values({"event_time": "2024-06-15"})
        reader = FileReader(engine)
        lower = {"event_time": "2024-06-01"}
        upper = {"event_time": "2024-06-20"}
        result = reader.read(file_source, watermark_start=lower, watermark_end=upper)
        assert result is not None
        assert engine._filtered is True
        assert engine._filter_watermark == {"event_time": "2024-06-01"}
        assert engine._filter_end == {"event_time": "2024-06-20"}


# ============================================================================
# PythonFunctionReader with watermark_end
# ============================================================================


class TestPythonFunctionReaderUpperWatermark:
    def test_read_with_upper_bound(self, engine: MockEngine) -> None:
        conn = Connection(
            name="func_conn",
            connection_type="function",
            format="function",
            configure={},
        )
        src = Source(
            connection=conn,
            watermark_columns=["ts"],
            configure={"python_function": "tests.unit.sources.test_watermark_upper._dummy_loader"},
        )
        src.python_function = "tests.unit.sources.test_watermark_upper._dummy_loader"
        engine.set_max_values({"ts": "2024-06-10"})
        reader = PythonFunctionReader(engine)
        lower = {"ts": "2024-06-01"}
        upper = {"ts": "2024-06-15"}
        result = reader.read(src, watermark_start=lower, watermark_end=upper)
        assert result is not None
        assert engine._filtered is True
        assert engine._filter_watermark == {"ts": "2024-06-01"}
        assert engine._filter_end == {"ts": "2024-06-15"}


def _dummy_loader(engine, source, watermark_start=None):
    """Dummy function for PythonFunctionReader tests."""
    return {"ts": ["2024-06-05", "2024-06-10"], "val": [1, 2]}
