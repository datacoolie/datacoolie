"""Focused tests for Driver-independent single-attempt pipeline bodies."""

from __future__ import annotations

from decimal import Decimal

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.runtime import DestinationRuntimeInfo, SourceRuntimeInfo, TransformRuntimeInfo
from datacoolie.core.models.source import Source
from datacoolie.sources.base import SourceReadRange
from datacoolie.orchestration.execution.pipeline import (
    execute_etl_pipeline,
    execute_maintenance_pipeline,
)


def _dataflow() -> DataFlow:
    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    return DataFlow(
        dataflow_id="df-1",
        source=Source(connection=connection, table="orders"),
        destination=Destination(connection=connection, table="orders_out"),
    )


@pytest.mark.parametrize(
    ("do_compact", "do_cleanup", "operation_details", "expected"),
    [
        (False, False, [], "No maintenance operations enabled"),
        (
            True,
            True,
            [{"status": "skipped", "message": "Destination table does not exist"}],
            "Destination table does not exist",
        ),
        (True, True, [], "Maintenance performed no operations"),
    ],
)
def test_maintenance_message_tracks_known_and_unknown_no_ops(
    do_compact, do_cleanup, operation_details, expected
) -> None:
    class SkippingWriter:
        def run_maintenance(self, **_kwargs):
            return DestinationRuntimeInfo(
                status=DataFlowStatus.SKIPPED.value,
                operation_details=operation_details,
            )

    result = execute_maintenance_pipeline(
        _dataflow(),
        create_destination_writer=lambda _fmt: SkippingWriter(),
        retention_hours=168,
        do_compact=do_compact,
        do_cleanup=do_cleanup,
    )

    assert result.status == DataFlowStatus.SKIPPED.value
    assert result.message == expected


class _Reader:
    def __init__(self) -> None:
        self.runtime = SourceRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=1,
        )

    def read(self, *_args, **kwargs):
        assert "read_type_requirements" not in kwargs
        return object()

    def get_runtime_info(self):
        return self.runtime

    def get_new_watermark(self):
        return None


class _Pipeline:
    def __init__(self) -> None:
        self.runtime = TransformRuntimeInfo(status=DataFlowStatus.SUCCEEDED.value)

    def transform(self, frame, _dataflow):
        return frame

    def get_runtime_info(self):
        return self.runtime

    def get_column_mapping(self):
        return None

    def get_output_columns(self):
        return []


class _Writer:
    def __init__(self) -> None:
        self.runtime = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_written=1,
        )

    def write(self, _frame, _dataflow, *, watermark_window=None):
        self.window = watermark_window
        return None

    def get_runtime_info(self):
        return self.runtime


def test_etl_attempt_uses_explicit_components_without_driver() -> None:
    reader = _Reader()
    pipeline = _Pipeline()
    writer = _Writer()
    calls: list[str] = []

    result = execute_etl_pipeline(
        _dataflow(),
        "run-1",
        column_name_mode="lower",
        watermark_start=None,
        watermark_end=None,
        save_watermark=False,
        watermark_start_operator=">",
        watermark_end_operator="<",
        watermark_manager=None,
        job_id="job-1",
        create_source_reader=lambda _fmt: (calls.append("read") or reader),
        create_transformer_pipeline=lambda **_kwargs: (calls.append("transform") or pipeline),
        create_destination_writer=lambda _fmt: (calls.append("write") or writer),
        validate_watermark_storage=lambda *_args, **_kwargs: calls.append("validate"),
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert result.source.rows_read == 1
    assert result.destination.rows_written == 1
    assert calls == ["read", "transform", "write"]


def test_confirmed_empty_replay_reaches_writer_with_window() -> None:
    class EmptyReader(_Reader):
        def __init__(self) -> None:
            self.runtime = SourceRuntimeInfo(
                status=DataFlowStatus.SUCCEEDED.value,
                rows_read=0,
                watermark_effective=None,
                watermark_after=None,
            )

        def read(self, *_args, **_kwargs):
            return {}

    class EmptyPipeline(_Pipeline):
        def get_column_mapping(self):
            from datacoolie.transformers.base import ColumnMapping

            return ColumnMapping({"id": "id"})

        def get_output_columns(self):
            return ["id"]

    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    dataflow = DataFlow(
        dataflow_id="df-empty-replay",
        source=Source(
            connection=connection,
            table="orders",
            watermark_columns=["id"],
            configure={"backward_days": 1},
        ),
        destination=Destination(
            connection=connection,
            table="orders_out",
            load_type="merge_overwrite",
            merge_keys=[],
            configure={"replace_by_watermark": True},
        ),
    )
    reader = EmptyReader()
    pipeline = EmptyPipeline()
    writer = _Writer()

    result = execute_etl_pipeline(
        dataflow,
        "run-empty",
        column_name_mode="lower",
        watermark_start={"id": 1},
        watermark_end={"id": 5},
        save_watermark=False,
        watermark_start_operator=">=",
        watermark_end_operator="<",
        watermark_manager=None,
        job_id="job-empty",
        create_source_reader=lambda _fmt: reader,
        create_transformer_pipeline=lambda **_kwargs: pipeline,
        create_destination_writer=lambda _fmt: writer,
        validate_watermark_storage=lambda *_args, **_kwargs: None,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert writer.window is not None
    assert writer.window.bounds == {"id": (1, 5)}
    assert writer.window.lower_operator == ">="
    assert writer.window.upper_operator == "<"


def test_failed_write_does_not_commit_new_watermark() -> None:
    """A watermark becomes durable only after the destination succeeds."""

    class WatermarkReader(_Reader):
        def __init__(self) -> None:
            self.runtime = SourceRuntimeInfo(
                status=DataFlowStatus.SUCCEEDED.value,
                rows_read=1,
                watermark_after={"id": 2},
            )

        def get_new_watermark(self):
            return {"id": 2}

    class FailingWriter(_Writer):
        def write(self, _frame, _dataflow, *, watermark_window=None):
            raise RuntimeError("destination unavailable")

    class WatermarkManager:
        def __init__(self) -> None:
            self.saved = []

        def get_watermark(self, **_kwargs):
            return None

        def save_watermark(self, **kwargs):
            self.saved.append(kwargs)

    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    dataflow = DataFlow(
        dataflow_id="df-watermark-failure",
        source=Source(connection=connection, table="orders", watermark_columns=["id"]),
        destination=Destination(connection=connection, table="orders_out"),
    )
    reader = WatermarkReader()
    manager = WatermarkManager()

    with pytest.raises(PipelineError, match="destination unavailable"):
        execute_etl_pipeline(
            dataflow,
            "run-watermark-failure",
            column_name_mode="lower",
            watermark_start=None,
            watermark_end=None,
            save_watermark=True,
            watermark_start_operator=">",
            watermark_end_operator="<",
            watermark_manager=manager,
            job_id="job-watermark-failure",
            create_source_reader=lambda _fmt: reader,
            create_transformer_pipeline=lambda **_kwargs: _Pipeline(),
            create_destination_writer=lambda _fmt: FailingWriter(),
            validate_watermark_storage=lambda *_args, **_kwargs: None,
        )

    assert manager.saved == []


def test_pipeline_uses_instance_reader_merge_without_manager_second_merge() -> None:
    class InstanceMergeReader(_Reader):
        def __init__(self) -> None:
            super().__init__()
            self.runtime.watermark_after = {"cursor": "new"}
            self.merge_calls = []
            self.merge_watermark = self._merge_watermark

        def get_new_watermark(self):
            return {"cursor": "new"}

        def _merge_watermark(self, existing, candidate):
            self.merge_calls.append((existing, candidate))
            return {"cursor": "reader-owned"}

    class Manager:
        def __init__(self) -> None:
            self.saved = None
            self.manager_merge_called = False

        def get_watermark(self, **_kwargs):
            return {"cursor": "stored"}

        def merge_watermark(self, *_args, **_kwargs):
            self.manager_merge_called = True
            raise AssertionError("manager must not provide a second merge")

        def serialize(self, value):
            return str(value)

        def save_watermark(self, **kwargs):
            self.saved = kwargs

    reader = InstanceMergeReader()
    manager = Manager()
    writer = _Writer()

    result = execute_etl_pipeline(
        _dataflow(),
        "run-reader-merge",
        column_name_mode="lower",
        watermark_start=None,
        watermark_end=None,
        save_watermark=True,
        watermark_start_operator=None,
        watermark_end_operator=None,
        watermark_manager=manager,
        job_id="job-reader-merge",
        create_source_reader=lambda _fmt: reader,
        create_transformer_pipeline=lambda **_kwargs: _Pipeline(),
        create_destination_writer=lambda _fmt: writer,
        validate_watermark_storage=lambda *_args, **_kwargs: None,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert reader.merge_calls == [({"cursor": "stored"}, {"cursor": "new"})]
    assert manager.manager_merge_called is False
    assert manager.saved["watermark"] == {"cursor": "reader-owned"}


@pytest.mark.parametrize("save,candidate,rows", [
    (False, {"id": 2}, 1), (True, {}, 1), (True, {}, 0),
    (True, {"id": 2}, 1), (True, {"id": None}, 0),
    (True, {"id": 0}, 1), (True, {"id": None, "cursor": "token"}, 1),
], ids=["save-off", "no-observation", "typed-empty", "save-on", "null-only-empty",
        "zero-observation", "mixed-null-observation"])
def test_replay_selection_and_save_order(save, candidate, rows) -> None:
    """Saving state cannot influence selection; preparation precedes mutation."""
    events = []
    selected_range = SourceReadRange("id", 1, 5)

    class Reader(_Reader):
        def __init__(self):
            super().__init__()
            self.runtime.rows_read = rows
            self.runtime.watermark_after = candidate or None

        def read(self, source, watermark_start, **kwargs):
            assert watermark_start is None
            assert kwargs["watermark_end"] is None
            assert kwargs["read_range"] == selected_range
            events.append("read")
            return object()

        def get_new_watermark(self):
            return candidate

        def merge_watermark(self, existing, observed):
            assert existing == {"id": 9}
            assert observed == candidate
            events.append("merge")
            return {"id": 9}

    class Manager:
        def get_watermark(self, **kwargs):
            events.append("get-state")
            return {"id": 9}

        def serialize(self, value):
            assert value == {"id": 9}
            events.append("serialize")
            return '{"id":9}'

        def save_watermark(self, **kwargs):
            assert kwargs["watermark"] == {"id": 9}
            events.append("save")

    class Pipeline(_Pipeline):
        def transform(self, frame, dataflow):
            events.append("transform")
            return frame

    class Writer(_Writer):
        def write(self, frame, dataflow, **kwargs):
            events.append("write")

    result = execute_etl_pipeline(
        _dataflow(), "range-order", column_name_mode="lower",
        watermark_start={"id": 1}, watermark_end={"id": 5},
        read_range=selected_range, save_watermark=save,
        watermark_manager=Manager(), job_id="job-order",
        create_source_reader=lambda fmt: Reader(),
        create_transformer_pipeline=lambda **kwargs: Pipeline(),
        create_destination_writer=lambda fmt: Writer(),
        validate_watermark_storage=lambda *args, **kwargs: events.append("validate"),
    )
    assert result.status == DataFlowStatus.SUCCEEDED.value
    if save and any(value is not None for value in candidate.values()):
        assert events == ["read", "get-state", "merge", "serialize", "validate",
                          "transform", "write", "save"]
    else:
        assert events == ["read", "transform", "write"]


def test_invalid_watermark_serialization_is_rejected_before_destination_write() -> None:
    class DecimalReader(_Reader):
        def __init__(self) -> None:
            self.runtime = SourceRuntimeInfo(
                status=DataFlowStatus.SUCCEEDED.value,
                rows_read=1,
                watermark_after={"id": Decimal("1.25")},
            )

        def get_new_watermark(self):
            return {"id": Decimal("NaN")}

    class PreparingManager:
        def __init__(self) -> None:
            self.saved = False

        def get_watermark(self, **_kwargs):
            return None

        def serialize(self, value):
            raise TypeError(f"unsupported watermark: {value['id']}")

        def save_watermark(self, **_kwargs):
            self.saved = True

    class TrackingWriter(_Writer):
        def __init__(self) -> None:
            super().__init__()
            self.called = False

        def write(self, _frame, _dataflow, *, watermark_window=None):
            self.called = True

    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    dataflow = DataFlow(
        dataflow_id="df-watermark-preflight",
        source=Source(connection=connection, table="orders", watermark_columns=["id"]),
        destination=Destination(connection=connection, table="orders_out"),
    )
    reader = DecimalReader()
    manager = PreparingManager()
    writer = TrackingWriter()

    with pytest.raises(PipelineError, match="unsupported watermark"):
        execute_etl_pipeline(
            dataflow,
            "run-watermark-preflight",
            column_name_mode="lower",
            watermark_start=None,
            watermark_end=None,
            save_watermark=True,
            watermark_start_operator=">",
            watermark_end_operator="<",
            watermark_manager=manager,
            job_id="job-watermark-preflight",
            create_source_reader=lambda _fmt: reader,
            create_transformer_pipeline=lambda **_kwargs: _Pipeline(),
            create_destination_writer=lambda _fmt: writer,
            validate_watermark_storage=lambda *_args, **_kwargs: None,
        )

    assert writer.called is False
    assert manager.saved is False


def test_binary_watermark_candidate_fails_before_destination_or_checkpoint() -> None:
    class BinaryReader(_Reader):
        def get_new_watermark(self):
            return {"version": b"\x00\x01"}

    class WatermarkManager:
        def __init__(self) -> None:
            self.saved = False

        def get_watermark(self, **_kwargs):
            return None

        def serialize(self, value):
            from datacoolie.watermark.base import WatermarkSerializer

            return WatermarkSerializer.serialize(value)

        def save_watermark(self, **_kwargs):
            self.saved = True

    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    dataflow = DataFlow(
        dataflow_id="df-binary-candidate",
        source=Source(
            connection=connection, table="orders", watermark_columns=["version"]
        ),
        destination=Destination(connection=connection, table="orders_out"),
    )
    reader = BinaryReader()
    manager = WatermarkManager()
    writer = _Writer()

    with pytest.raises(PipelineError, match="Binary watermark comparison"):
        execute_etl_pipeline(
            dataflow,
            "run-binary-candidate",
            column_name_mode="lower",
            watermark_start=None,
            watermark_end=None,
            save_watermark=True,
            watermark_start_operator=">",
            watermark_end_operator="<",
            watermark_manager=manager,
            job_id="job-binary-candidate",
            create_source_reader=lambda _fmt: reader,
            create_transformer_pipeline=lambda **_kwargs: _Pipeline(),
            create_destination_writer=lambda _fmt: writer,
            validate_watermark_storage=lambda *_args, **_kwargs: None,
        )

    assert manager.saved is False
    assert not hasattr(writer, "window")


def test_binary_watermark_lower_bound_fails_before_source_read() -> None:
    class TrackingReader(_Reader):
        def __init__(self) -> None:
            super().__init__()
            self.read_called = False

        def read(self, *_args, **_kwargs):
            self.read_called = True
            return object()

    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    dataflow = DataFlow(
        dataflow_id="df-binary-lower",
        source=Source(
            connection=connection, table="orders", watermark_columns=["version"]
        ),
        destination=Destination(connection=connection, table="orders_out"),
    )
    reader = TrackingReader()

    with pytest.raises(PipelineError, match="Binary watermark comparison"):
        execute_etl_pipeline(
            dataflow,
            "run-binary-lower",
            column_name_mode="lower",
            watermark_start={"version": b"\x00\x01"},
            watermark_end=None,
            save_watermark=False,
            watermark_start_operator=">",
            watermark_end_operator="<",
            watermark_manager=None,
            job_id="job-binary-lower",
            create_source_reader=lambda _fmt: reader,
            create_transformer_pipeline=lambda **_kwargs: _Pipeline(),
            create_destination_writer=lambda _fmt: _Writer(),
            validate_watermark_storage=lambda *_args, **_kwargs: None,
        )

    assert reader.read_called is False
