"""Tests for PythonFunctionReader.

This module verifies Python function loading, execution, error handling,
allowed_prefixes security, and plugin registration.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import patch

import pytest

from datacoolie.core.constants import DataFlowStatus, Format
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.sources import SourceReadRange
from datacoolie.sources.python_function_reader import PythonFunctionReader

from tests.unit.sources.support import MockEngine, engine


# ============================================================================
# Helper functions and fixtures
# ============================================================================


def _make_python_function_source(func_path: str, **kw) -> Source:
    """Create a Source configured to use a Python function."""
    conn = Connection(
        name="dummy",
        connection_type="function",
        format=Format.FUNCTION.value,
        configure={"base_path": "/unused"},
    )
    return Source(connection=conn, python_function=func_path, **kw)


def _sample_loader(engine, source, watermark_start=None, watermark_end=None):
    """Returns mock data via the engine."""
    return engine._data


def _none_loader(engine, source, watermark_start=None, watermark_end=None):
    """Returns None."""
    return None


def _failing_loader(engine, source, watermark_start=None, watermark_end=None):
    """Raises an intentional error."""
    raise ValueError("intentional boom")


def _bounded_loader(
    engine,
    source,
    watermark_start=None,
    watermark_end=None,
    *,
    read_range=None,
):
    """Accepts the source-owned bounded-read keyword."""
    return engine._data


# ============================================================================
# PythonFunctionReader tests
# ============================================================================


class TestPythonFunctionReader:
    """Tests for PythonFunctionReader."""

    def test_read_calls_function(self, engine: MockEngine) -> None:
        """Read succeeds when the function returns a dict (mock DF)."""
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
        )
        result = reader.read(src)
        assert result is not None
        info = reader.get_runtime_info()
        assert info.rows_read == 3
        assert info.status == DataFlowStatus.SUCCEEDED.value

    def test_read_function_returns_none(self, engine: MockEngine) -> None:
        """Function returning None results in None."""
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._none_loader",
        )
        result = reader.read(src)
        assert result is None

    def test_read_zero_rows(self, engine: MockEngine) -> None:
        """Engine returns zero rows after reading."""
        engine.set_data({}, 0)
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
        )
        result = reader.read(src)
        assert result is None

    def test_read_with_watermark_columns(self, engine: MockEngine) -> None:
        """Watermark filtering is applied after function returns data."""
        engine.set_max_values({"modified_at": "2024-12-01"})
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
            watermark_columns=["modified_at"],
        )
        result = reader.read(src, watermark_start={"modified_at": "2024-01-01"})
        assert result is not None
        wm = reader.get_new_watermark()
        assert wm == {"modified_at": "2024-12-01"}
        # Post-function watermark filter should have been applied
        assert engine._filtered is True

    def test_watermark_passed_to_function(self, engine: MockEngine) -> None:
        """The watermark dict is forwarded to the user function (pre-filter)."""
        captured: Dict[str, Any] = {}

        def _capture_loader(engine, source, watermark_start=None, watermark_end=None):
            captured["watermark_start"] = watermark_start
            return engine._data

        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
            watermark_columns=["modified_at"],
        )
        # Patch the resolved function to our capturing one
        with patch.object(
            PythonFunctionReader, "_resolve_function", return_value=_capture_loader,
        ):
            reader.read(src, watermark_start={"modified_at": "2024-01-01"})
        assert captured["watermark_start"] == {"modified_at": "2024-01-01"}

    def test_bounded_read_forwards_range_and_null_incremental_bounds(
        self, engine: MockEngine
    ) -> None:
        captured: Dict[str, Any] = {}

        def _capture_bounded_loader(
            engine,
            source,
            watermark_start=None,
            watermark_end=None,
            *,
            read_range=None,
        ):
            captured.update(
                watermark_start=watermark_start,
                watermark_end=watermark_end,
                read_range=read_range,
            )
            return engine._data

        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._bounded_loader",
            watermark_columns=["id"],
        )
        bounded = SourceReadRange("id", 1, 3)
        with patch.object(
            PythonFunctionReader,
            "_resolve_function",
            return_value=_capture_bounded_loader,
        ):
            result = reader.read(src, read_range=bounded)

        assert result is not None
        assert captured == {
            "watermark_start": None,
            "watermark_end": None,
            "read_range": bounded,
        }
        assert reader.get_runtime_info().watermark_start_operator == ">="
        assert reader.get_runtime_info().watermark_end_operator == "<"

    def test_bounded_read_rejects_legacy_function_before_invocation(
        self, engine: MockEngine
    ) -> None:
        invoked = False

        def _legacy_loader(engine, source, watermark_start=None, watermark_end=None):
            nonlocal invoked
            invoked = True
            return engine._data

        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader"
        )
        with patch.object(
            PythonFunctionReader,
            "_resolve_function",
            return_value=_legacy_loader,
        ):
            with pytest.raises(SourceError, match="must accept a read_range keyword"):
                reader.read(src, read_range=SourceReadRange("id", 1, 3))

        assert invoked is False

    def test_bounded_kwargs_failure_is_not_retried_unbounded(self, engine: MockEngine) -> None:
        calls = []
        bounded = SourceReadRange("id", 1, 3)

        def loader(engine, source, **kwargs):
            calls.append(kwargs)
            raise RuntimeError("bounded loader failed")

        source = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
        )
        with patch.object(PythonFunctionReader, "_resolve_function", return_value=loader):
            with pytest.raises(SourceError, match="bounded loader failed"):
                PythonFunctionReader(engine).read(source, read_range=bounded)
        assert len(calls) == 1
        assert calls[0]["read_range"] == bounded
        assert calls[0]["watermark_start"] is None
        assert calls[0]["watermark_end"] is None

    def test_no_post_filter_without_watermark(self, engine: MockEngine) -> None:
        """When no watermark is passed, post-filter is skipped."""
        engine._filtered = False
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
            watermark_columns=["modified_at"],
        )
        reader.read(src)  # no watermark
        assert engine._filtered is False

    def test_source_action_recorded(self, engine: MockEngine) -> None:
        """Source action contains reader type and function path."""
        reader = PythonFunctionReader(engine)
        func_path = "tests.unit.sources.test_python_function_reader._sample_loader"
        src = _make_python_function_source(func_path)
        reader.read(src)
        info = reader.get_runtime_info()
        assert info.source_action == {"reader": "PythonFunctionReader", "function": func_path}

    def test_missing_python_function_raises(self, engine: MockEngine) -> None:
        """Source without python_function attribute raises error."""
        conn = Connection(
            name="dummy", connection_type="file", format="parquet", configure={},
        )
        src = Source(connection=conn)  # no python_function
        reader = PythonFunctionReader(engine)
        with pytest.raises(SourceError, match="requires source.python_function"):
            reader.read(src)

    def test_invalid_dotted_path_raises(self, engine: MockEngine) -> None:
        """Function path without dot raises error."""
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source("no_dot_here")
        with pytest.raises(SourceError, match="dotted path"):
            reader.read(src)

    def test_import_failure_raises(self, engine: MockEngine) -> None:
        """Module import failure raises wrappedSourceError."""
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source("nonexistent_pkg.func")
        with pytest.raises(SourceError, match="Cannot import module"):
            reader.read(src)

    def test_missing_attribute_raises(self, engine: MockEngine) -> None:
        """Missing function attribute raises SourceError."""
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source("os.path.no_such_attr_xyz")
        with pytest.raises(SourceError, match="has no attribute"):
            reader.read(src)

    def test_non_callable_raises(self, engine: MockEngine) -> None:
        """Non-callable attribute raises SourceError."""
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source("os.sep")
        with pytest.raises(SourceError, match="not callable"):
            reader.read(src)

    def test_function_exception_raises_source_error(self, engine: MockEngine) -> None:
        """Exception in user function is wrapped in SourceError."""
        reader = PythonFunctionReader(engine)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._failing_loader",
        )
        with pytest.raises(SourceError, match="raised an error"):
            reader.read(src)


class TestPythonFunctionReaderRegistry:
    def test_function_registered_in_source_registry(self) -> None:
        from datacoolie import source_registry

        assert source_registry.is_available(Format.FUNCTION.value)
        assert Format.FUNCTION.value in source_registry.list_plugins()

    def test_python_alias_kept_for_backward_compatibility(self) -> None:
        from datacoolie import source_registry

        assert source_registry.is_available("function")
        assert "function" in source_registry.list_plugins()


# ============================================================================
# Phase B — allowed_prefixes security
# ============================================================================


class TestAllowedPrefixes:
    """Verify allowed_prefixes restricts which modules can be imported."""

    def test_disallowed_prefix_raises(self, engine: MockEngine) -> None:
        reader = PythonFunctionReader(engine, allowed_prefixes=["safe_pkg."])
        src = _make_python_function_source("evil_pkg.steal_data")
        with pytest.raises(SourceError, match="not allowed"):
            reader.read(src)

    def test_allowed_prefix_passes(self, engine: MockEngine) -> None:
        reader = PythonFunctionReader(engine, allowed_prefixes=["tests.unit.sources."])
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
        )
        result = reader.read(src)
        assert result is not None

    def test_empty_prefixes_allows_all(self, engine: MockEngine) -> None:
        reader = PythonFunctionReader(engine, allowed_prefixes=[])
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
        )
        result = reader.read(src)
        assert result is not None

    def test_none_prefixes_allows_all(self, engine: MockEngine) -> None:
        reader = PythonFunctionReader(engine, allowed_prefixes=None)
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
        )
        result = reader.read(src)
        assert result is not None

    def test_multiple_prefixes_any_match(self, engine: MockEngine) -> None:
        reader = PythonFunctionReader(engine, allowed_prefixes=["a.", "tests.unit.sources."])
        src = _make_python_function_source(
            "tests.unit.sources.test_python_function_reader._sample_loader",
        )
        result = reader.read(src)
        assert result is not None
