"""Tests for BaseDestinationWriter and maintenance operations."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from datacoolie.core.constants import (
    DEFAULT_RETENTION_HOURS,
    DataFlowStatus,
    MaintenanceType,
)
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import DataFlow
from datacoolie.destinations.base import BaseDestinationWriter

from tests.unit.destinations.support import MockEngine, _make_dataflow, engine


# ============================================================================
# Test helpers
# ============================================================================


def _default_maintain(
    writer: BaseDestinationWriter,
    dataflow: DataFlow,
    *,
    do_compact: bool,
    do_cleanup: bool,
    retention_hours: int,
) -> "tuple[List[Dict[str, Any]], List[str]]":
    """Reference implementation mirroring the old base default.

    Used by test writers that don't need format-specific handle resolution.
    Passes both ``full_table_name`` and ``path`` to the engine (which prefers
    ``table_name`` when both are set).
    """
    dest = dataflow.destination
    fmt = dest.connection.format
    table_name = dest.full_table_name
    path = dest.path
    location = table_name or path or "<unknown>"

    sub_results: List[Dict[str, Any]] = []
    errors: List[str] = []

    table_exists = writer._engine.exists(table_name=table_name, path=path, fmt=fmt)

    if do_compact:
        sub_results.append(writer._run_op(
            op_name=MaintenanceType.COMPACT.value,
            table_exists=table_exists,
            fn=lambda: writer._engine.compact(table_name=table_name, path=path, fmt=fmt),
            errors=errors,
            location=location,
        ))

    if do_cleanup:
        sub_results.append(writer._run_op(
            op_name=MaintenanceType.CLEANUP.value,
            table_exists=table_exists,
            fn=lambda: writer._engine.cleanup(
                table_name=table_name, path=path,
                retention_hours=retention_hours, fmt=fmt,
            ),
            errors=errors,
            location=location,
        ))

    return sub_results, errors


class ConcreteWriter(BaseDestinationWriter[dict]):
    """Concrete writer for testing the base class."""

    def _write_internal(self, df, dataflow):
        pass

    def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
        return _default_maintain(
            self, dataflow,
            do_compact=do_compact, do_cleanup=do_cleanup,
            retention_hours=retention_hours,
        )


class FailingWriter(BaseDestinationWriter[dict]):
    """Writer that always fails."""

    def _write_internal(self, df, dataflow):
        raise RuntimeError("Write failed!")

    def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
        return _default_maintain(
            self, dataflow,
            do_compact=do_compact, do_cleanup=do_cleanup,
            retention_hours=retention_hours,
        )


class MetricsWriter(BaseDestinationWriter[dict]):
    """Writer that returns synthetic maintenance metrics for coverage tests."""

    def _write_internal(self, df, dataflow):
        pass

    def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
        return _default_maintain(
            self, dataflow,
            do_compact=do_compact, do_cleanup=do_cleanup,
            retention_hours=retention_hours,
        )

    def _parse_maintenance_metrics(self, history):
        return {
            "compact": {
                "files_added": 2,
                "files_removed": 1,
                "bytes_added": 200,
                "bytes_removed": 100,
            }
        }


class NoHistoryEngine(MockEngine):
    """Engine variant that returns no history entries."""

    def get_history_by_path(self, path, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        return []

    def get_history_by_name(self, table_name, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        return []


class FailingHistoryEngine(MockEngine):
    """Engine variant that raises when history is requested."""

    def get_history_by_path(self, path, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        raise RuntimeError("history unavailable")

    def get_history_by_name(self, table_name, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        raise RuntimeError("history unavailable")


# ============================================================================
# BaseDestinationWriter tests
# ============================================================================


class TestBaseDestinationWriter:
    def test_cannot_instantiate_abc(self) -> None:
        with pytest.raises(TypeError):
            BaseDestinationWriter(MockEngine())  # type: ignore[abstract]

    def test_write_succeeds(self, engine: MockEngine) -> None:
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        # ConcreteWriter uses the default parser which returns zeros
        assert info.rows_written == 0

    def test_write_no_path_succeeds_with_table_name(self, engine: MockEngine) -> None:
        writer = ConcreteWriter(engine)
        df = _make_dataflow(has_path=False)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value

    def test_write_failure_wraps_exception(self, engine: MockEngine) -> None:
        writer = FailingWriter(engine)
        df = _make_dataflow()
        with pytest.raises(DestinationError, match="Failed to write destination"):
            writer.write({"data": 1}, df)
        info = writer.get_runtime_info()
        assert info.status == DataFlowStatus.FAILED.value

    def test_write_records_timing(self, engine: MockEngine) -> None:
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.write({"data": 1}, df)
        assert info.start_time is not None
        assert info.end_time is not None

    def test_write_succeeds_when_history_is_empty(self) -> None:
        writer = ConcreteWriter(NoHistoryEngine())
        df = _make_dataflow()
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert info.operation_details == []

    def test_write_succeeds_when_history_fetch_fails(self) -> None:
        writer = ConcreteWriter(FailingHistoryEngine())
        df = _make_dataflow()
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value


# ============================================================================
# Maintenance operations tests
# ============================================================================


class TestMaintenanceOperations:
    def test_run_maintenance_both_succeeded(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert info.operation_type == "maintenance"
        ops = {e["operation"]: e for e in info.operation_details}
        assert "compact" in ops
        assert "cleanup" in ops
        assert ops["compact"]["status"] == DataFlowStatus.SUCCEEDED.value
        assert ops["cleanup"]["status"] == DataFlowStatus.SUCCEEDED.value
        assert len(engine._compacted) == 1
        assert len(engine._cleaned) == 1

    def test_run_maintenance_compact_only(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df, do_cleanup=False)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        ops = {e["operation"] for e in info.operation_details}
        assert "compact" in ops
        assert "cleanup" not in ops
        assert len(engine._compacted) == 1
        assert len(engine._cleaned) == 0

    def test_run_maintenance_cleanup_only(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df, do_compact=False)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        ops = {e["operation"] for e in info.operation_details}
        assert "cleanup" in ops
        assert "compact" not in ops
        assert len(engine._cleaned) == 1
        assert len(engine._compacted) == 0

    def test_run_maintenance_custom_retention(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df, do_compact=False, retention_hours=24)
        assert engine._cleaned[0]["hours"] == 24

    def test_run_maintenance_default_retention(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df, do_compact=False)
        assert engine._cleaned[0]["hours"] == DEFAULT_RETENTION_HOURS

    def test_run_maintenance_table_not_exists_skipped(self, engine: MockEngine) -> None:
        engine.set_table_exists(False)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df)
        assert info.status == DataFlowStatus.SKIPPED.value

    def test_run_maintenance_no_path_uses_table_name(self, engine: MockEngine) -> None:
        """When no path is set, maintenance routes to _by_name methods using full_table_name."""
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow(has_path=False)
        info = writer.run_maintenance(df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._compacted) == 1
        assert len(engine._cleaned) == 1

    def test_run_maintenance_no_path_table_not_exists_skipped(self, engine: MockEngine) -> None:
        """When no path and table does not exist, maintenance is skipped (not failed)."""
        writer = ConcreteWriter(engine)  # _table_exists_result defaults to False
        df = _make_dataflow(has_path=False)
        info = writer.run_maintenance(df)
        assert info.status == DataFlowStatus.SKIPPED.value

    def test_run_maintenance_records_timing(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df)
        assert info.start_time is not None
        assert info.end_time is not None

    def test_run_maintenance_sub_results_have_duration(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df)
        for key in ("compact", "cleanup"):
            entry = next(e for e in info.operation_details if e.get("operation") == key)
            assert "duration_seconds" in entry

    def test_run_maintenance_fails_when_no_path_and_no_table_name(self, engine: MockEngine) -> None:
        writer = ConcreteWriter(engine)
        dataflow = SimpleNamespace(
            destination=SimpleNamespace(
                path="",
                full_table_name="",
                connection=SimpleNamespace(format="delta"),
            )
        )
        info = writer.run_maintenance(dataflow)  # type: ignore[arg-type]
        assert info.status == DataFlowStatus.FAILED.value
        assert info.error_message == "Destination path or table name is required"

    def test_run_maintenance_enriches_sub_results_with_engine_metrics(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        writer = MetricsWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df, do_cleanup=False)
        compact = next(e for e in info.operation_details if e.get("operation") == "compact")
        assert compact["engine_metrics"]["files_added"] == 2
        assert info.files_added == 2
        assert info.files_removed == 1
        assert info.bytes_added == 200
        assert info.bytes_removed == 100

    def test_run_maintenance_skips_history_entry_when_history_empty(self) -> None:
        engine = NoHistoryEngine()
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df)
        ops = [e.get("operation") for e in info.operation_details]
        assert "history" not in ops

    def test_run_maintenance_ignores_history_errors(self) -> None:
        engine = FailingHistoryEngine()
        engine.set_table_exists(True)
        writer = ConcreteWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df)
        assert info.status == DataFlowStatus.SUCCEEDED.value

    def test_run_maintenance_metrics_match_second_entry_and_unmatched_key(self, engine: MockEngine) -> None:
        class LoopCoverageWriter(ConcreteWriter):
            def _parse_maintenance_metrics(self, history):
                return {
                    "cleanup": {
                        "files_added": 1,
                        "files_removed": 2,
                        "bytes_added": 10,
                        "bytes_removed": 20,
                    },
                    "unknown": {
                        "files_added": 7,
                        "files_removed": 8,
                        "bytes_added": 70,
                        "bytes_removed": 80,
                    },
                }

        engine.set_table_exists(True)
        writer = LoopCoverageWriter(engine)
        df = _make_dataflow()
        info = writer.run_maintenance(df, do_compact=True, do_cleanup=True)
        cleanup = next(e for e in info.operation_details if e.get("operation") == "cleanup")
        compact = next(e for e in info.operation_details if e.get("operation") == "compact")
        assert cleanup["engine_metrics"]["files_removed"] == 2
        assert "engine_metrics" not in compact
