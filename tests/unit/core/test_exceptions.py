"""Tests for datacoolie.core.exceptions.

This module verifies the exception hierarchy, message formatting, and details
handling for all DataCoolie exception classes.
"""

import pytest

from datacoolie.core.exceptions import (
    ConfigurationError,
    DataCoolieError,
    DataFlowError,
    DestinationError,
    EngineError,
    MetadataError,
    PipelineError,
    PlatformError,
    SourceError,
    TransformError,
    WatermarkError,
)


# ============================================================================
# Base Exception
# ============================================================================


class TestDataCoolieError:
    """Verify base exception functionality and formatting."""

    def test_message_only(self) -> None:
        err = DataCoolieError("something broke")
        assert str(err) == "something broke"
        assert err.message == "something broke"
        assert err.details == {}

    def test_with_details(self) -> None:
        err = DataCoolieError("fail", details={"key": "val"})
        assert "Details:" in str(err)
        assert err.details == {"key": "val"}

    def test_is_exception(self) -> None:
        with pytest.raises(DataCoolieError):
            raise DataCoolieError("boom")


# ============================================================================
# Exception Hierarchy
# ============================================================================


class TestSubclassHierarchy:
    """Every component exception inherits from DataCoolieError."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            ConfigurationError,
            MetadataError,
            SourceError,
            TransformError,
            DestinationError,
            WatermarkError,
            EngineError,
            PlatformError,
        ],
    )
    def test_inherits_base(self, exc_class: type) -> None:
        err = exc_class("test")
        assert isinstance(err, DataCoolieError)
        assert isinstance(err, Exception)

    @pytest.mark.parametrize(
        "exc_class",
        [
            ConfigurationError,
            MetadataError,
            SourceError,
            TransformError,
            DestinationError,
            WatermarkError,
            EngineError,
            PlatformError,
        ],
    )
    def test_catchable_as_base(self, exc_class: type) -> None:
        with pytest.raises(DataCoolieError):
            raise exc_class("catch me")


# ============================================================================
# DataFlow and Pipeline Exceptions
# ============================================================================


class TestDataFlowError:
    """Verify DataFlowError with dataflow_id and stage context."""
    def test_message_only(self) -> None:
        err = DataFlowError("pipeline failed")
        assert "pipeline failed" in str(err)
        assert err.dataflow_id is None
        assert err.stage is None

    def test_with_context(self) -> None:
        err = DataFlowError(
            "merge conflict",
            dataflow_id="df-123",
            stage="bronze2silver",
            details={"table": "orders"},
        )
        s = str(err)
        assert "merge conflict" in s
        assert "df-123" in s
        assert "bronze2silver" in s
        assert "orders" in str(err.details)

    def test_inherits_base(self) -> None:
        assert issubclass(DataFlowError, DataCoolieError)


class TestPipelineError:
    """Verify PipelineError preserves partial results on failure."""
    def test_partial_result_is_preserved(self) -> None:
        err = PipelineError("step failed", partial_result={"rows": 10}, details={"stage": "read"})
        assert err.message == "step failed"
        assert err.partial_result == {"rows": 10}
        assert err.details == {"stage": "read"}
