"""Tests for datacoolie.orchestration.driver (DataCoolieDriver + create_driver).

This module verifies driver initialization, dataflow processing, maintenance
operations, retry handling, and parallel execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from datacoolie.core.constants import ColumnCaseMode, DataFlowStatus, ExecutionType, Format, LoadType
from datacoolie.core.exceptions import DataCoolieError
from datacoolie.core.models import (
    Connection,
    DataCoolieRunConfig,
    DataFlow,
    DataFlowRuntimeInfo,
    Destination,
    DestinationRuntimeInfo,
    Source,
    SourceRuntimeInfo,
    TransformRuntimeInfo,
)
from datacoolie.orchestration.driver import DataCoolieDriver, PipelineError, create_driver
from datacoolie.orchestration.parallel_executor import ExecutionResult


# ============================================================================
# Helpers
# ============================================================================


def _conn(
    fmt: str = Format.DELTA.value,
    name: str = "test",
    connection_id: str | None = None,
) -> Connection:
    """Create a minimal Connection for testing."""
    kw: dict = {"name": name, "format": fmt, "configure": {"base_path": "/data"}}
    if connection_id is not None:
        kw["connection_id"] = connection_id
    return Connection(**kw)


def _dataflow(
    dataflow_id: str = "df-1",
    fmt: str = Format.DELTA.value,
    group_number: int | None = None,
    is_active: bool = True,
    dest_conn: Connection | None = None,
) -> DataFlow:
    """Create a minimal DataFlow for testing."""
    conn = dest_conn or _conn(fmt=fmt)
    return DataFlow(
        dataflow_id=dataflow_id,
        group_number=group_number,
        is_active=is_active,
        source=Source(connection=conn, table="src"),
        destination=Destination(connection=conn, table="dst", load_type=LoadType.APPEND.value),
    )


def _mock_engine():
    engine = MagicMock()
    engine.platform = None
    engine.set_platform.side_effect = lambda p: setattr(engine, 'platform', p)
    engine.count_rows.return_value = 5
    engine.table_exists.return_value = True
    engine.get_maintenance_metrics.return_value = {}
    return engine


def _mock_platform():
    return MagicMock()


def _mock_metadata(dataflows: List[DataFlow] | None = None):
    md = MagicMock()
    md.get_dataflows.return_value = dataflows or []
    md.get_watermark.return_value = {}
    md.update_watermark.return_value = None
    return md


def _mock_watermark():
    wm = MagicMock()
    wm.get_watermark.return_value = {}
    wm.save_watermark.return_value = None
    return wm


def _make_driver(
    dataflows: List[DataFlow] | None = None,
    config: DataCoolieRunConfig | None = None,
    etl_logger: Any = None,
    system_logger: Any = None,
) -> tuple:
    """Return (driver, engine, metadata, watermark) with mocked deps."""
    engine = _mock_engine()
    platform = _mock_platform()
    metadata = _mock_metadata(dataflows)
    watermark = _mock_watermark()
    cfg = config or DataCoolieRunConfig()

    driver = DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        watermark_manager=watermark,
        config=cfg,
        system_logger=system_logger,
        etl_logger=etl_logger,
    )
    return driver, engine, metadata, watermark


# ============================================================================
# Init / properties
# ============================================================================


class TestDataCoolieDriverInit:
    def test_default_config(self):
        d, *_ = _make_driver()
        assert d.config.job_num == 1
        assert d.config.job_index == 0
        assert d.job_id == d.config.job_id  # delegates to config

    def test_custom_config(self):
        cfg = DataCoolieRunConfig(job_num=3, job_index=1, max_workers=4)
        d, *_ = _make_driver(config=cfg)
        assert d.config.job_num == 3
        assert d.config.job_index == 1

    def test_context_manager(self):
        d, *_ = _make_driver()
        with d as driver:
            assert driver is d
        # close should have been called — dataflows cleared
        assert d._dataflows == []


# ============================================================================
# Logger wiring
# ============================================================================


class TestDataCoolieDriverLoggers:
    """Tests for base_log_path auto-creation and job_id sync."""

    def _base_kwargs(self):
        return dict(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
        )

    def test_no_loggers_by_default(self):
        d, *_ = _make_driver()
        assert d._system_logger is None
        assert d._etl_logger is None

    def test_base_log_path_creates_both_loggers(self):
        from datacoolie.logging import ETLLogger, SystemLogger

        d = DataCoolieDriver(base_log_path="/logs", **self._base_kwargs())

        assert isinstance(d._system_logger, SystemLogger)
        assert isinstance(d._etl_logger, ETLLogger)
        assert d._system_logger.config.output_path == "/logs/system_logs"
        assert d._etl_logger.config.output_path == "/logs/etl_logs"

    def test_base_log_path_strips_trailing_slash(self):
        d = DataCoolieDriver(base_log_path="/logs/", **self._base_kwargs())

        assert d._system_logger.config.output_path == "/logs/system_logs"
        assert d._etl_logger.config.output_path == "/logs/etl_logs"

    def test_base_log_path_auto_loggers_carry_job_id(self):
        d = DataCoolieDriver(base_log_path="/logs", **self._base_kwargs())

        assert d._system_logger.run_config is d.config
        assert d._etl_logger.run_config is d.config
        assert d._etl_logger._job_info.job_id == d.job_id

    def test_explicit_system_logger_not_replaced_by_base_log_path(self):
        """Explicit system_logger takes precedence; only etl_logger is auto-created."""
        from datacoolie.logging import ETLLogger

        explicit_sys = MagicMock()
        d = DataCoolieDriver(base_log_path="/logs", system_logger=explicit_sys, **self._base_kwargs())

        assert d._system_logger is explicit_sys
        assert isinstance(d._etl_logger, ETLLogger)

    def test_explicit_etl_logger_not_replaced_by_base_log_path(self):
        """Explicit etl_logger takes precedence; only system_logger is auto-created."""
        from datacoolie.logging import SystemLogger

        explicit_etl = MagicMock()
        d = DataCoolieDriver(base_log_path="/logs", etl_logger=explicit_etl, **self._base_kwargs())

        assert d._etl_logger is explicit_etl
        assert isinstance(d._system_logger, SystemLogger)

    def test_provided_loggers_get_job_id_synced(self):
        """Externally created loggers always receive the driver's run config."""
        sys_lgr = MagicMock()
        etl_lgr = MagicMock()
        d = DataCoolieDriver(system_logger=sys_lgr, etl_logger=etl_lgr, **self._base_kwargs())

        sys_lgr.set_run_config.assert_called_once_with(d.config)
        etl_lgr.set_run_config.assert_called_once_with(d.config)

    def test_log_config_used_for_auto_created_loggers(self):
        """User-supplied LogConfig is used as template for auto-created loggers."""
        from datacoolie.logging import LogConfig

        custom = LogConfig(
            log_level="DEBUG",
            storage_mode="file",
            partition_by_date=False,
            partition_pattern="year={Y}",
            flush_interval_seconds=30,
        )
        d = DataCoolieDriver(
            base_log_path="/logs",
            log_config=custom,
            **self._base_kwargs(),
        )

        for lgr in (d._system_logger, d._etl_logger):
            assert lgr.config.log_level == "DEBUG"
            assert lgr.config.storage_mode == "file"
            assert lgr.config.partition_by_date is False
            assert lgr.config.partition_pattern == "year={Y}"
            assert lgr.config.flush_interval_seconds == 30

        assert d._system_logger.config.output_path == "/logs/system_logs"
        assert d._etl_logger.config.output_path == "/logs/etl_logs"

    def test_log_config_alone_creates_loggers(self):
        """LogConfig with output_path (no base_log_path) auto-creates loggers."""
        from datacoolie.logging import ETLLogger, LogConfig, SystemLogger

        cfg = LogConfig(output_path="/data/logs", log_level="WARNING")
        d = DataCoolieDriver(log_config=cfg, **self._base_kwargs())

        assert isinstance(d._system_logger, SystemLogger)
        assert isinstance(d._etl_logger, ETLLogger)
        assert d._system_logger.config.output_path == "/data/logs/system_logs"
        assert d._etl_logger.config.output_path == "/data/logs/etl_logs"
        assert d._system_logger.config.log_level == "WARNING"
        assert d._etl_logger.config.log_level == "WARNING"

    def test_base_log_path_overrides_log_config_output_path(self):
        """base_log_path takes precedence over log_config.output_path."""
        from datacoolie.logging import LogConfig

        cfg = LogConfig(output_path="/ignored/path", log_level="DEBUG")
        d = DataCoolieDriver(
            base_log_path="/preferred",
            log_config=cfg,
            **self._base_kwargs(),
        )

        assert d._system_logger.config.output_path == "/preferred/system_logs"
        assert d._etl_logger.config.output_path == "/preferred/etl_logs"
        # Other LogConfig fields are still preserved.
        assert d._system_logger.config.log_level == "DEBUG"

    def test_log_config_without_output_path_no_loggers(self):
        """LogConfig with output_path=None and no base_log_path creates no loggers."""
        from datacoolie.logging import LogConfig

        cfg = LogConfig(log_level="DEBUG")  # output_path defaults to None
        d = DataCoolieDriver(log_config=cfg, **self._base_kwargs())

        assert d._system_logger is None
        assert d._etl_logger is None


# ============================================================================
# load_dataflows
# ============================================================================


class TestLoadDataflows:
    def test_loads_and_filters(self):
        dfs = [_dataflow("a"), _dataflow("b"), _dataflow("c", is_active=False)]
        d, _, md, _ = _make_driver(dataflows=dfs)

        result = d.load_dataflows(stage="bronze")
        md.get_dataflows.assert_called_once_with(
            stage="bronze", active_only=True, attach_schema_hints=True,
        )
        # Single job → all active pass through
        assert len(result) == 2

    def test_empty(self):
        d, _, md, _ = _make_driver(dataflows=[])
        result = d.load_dataflows()
        assert result == []


# ============================================================================
# run — routing
# ============================================================================


class TestRun:
    def test_is_alias_for_run_dataflow(self):
        d, *_ = _make_driver()
        df = _dataflow()

        with patch.object(d, "run_dataflow") as mock_rf:
            mock_rf.return_value = ExecutionResult(total=1, succeeded=1)
            result = d.run(stage="bronze", dataflows=[df])

        mock_rf.assert_called_once_with(
            stage="bronze", dataflows=[df], column_name_mode=ColumnCaseMode.LOWER,
        )
        assert result.total == 1

    def test_forwards_column_name_mode(self):
        d, *_ = _make_driver()
        df = _dataflow()

        with patch.object(d, "run_dataflow") as mock_rf:
            mock_rf.return_value = ExecutionResult(total=1, succeeded=1)
            d.run(stage="bronze", dataflows=[df], column_name_mode="snake")

        mock_rf.assert_called_once_with(
            stage="bronze", dataflows=[df], column_name_mode="snake",
        )


# ============================================================================
# run_dataflow — dry run
# ============================================================================


class TestRunDataflowDryRun:
    def test_dry_run_no_processing(self):
        cfg = DataCoolieRunConfig(dry_run=True)
        d, engine, _, _ = _make_driver(config=cfg)
        df = _dataflow()

        result = d.run_dataflow(dataflows=[df])
        assert result.total == 1
        assert result.succeeded == 0  # no actual processing

    def test_no_dataflows(self):
        d, *_ = _make_driver()
        result = d.run_dataflow()
        assert result.total == 0

    def test_column_name_mode_stored_on_driver(self):
        d, *_ = _make_driver()
        d.run_dataflow(column_name_mode="snake")
        assert d._column_name_mode == ColumnCaseMode.SNAKE

    def test_column_name_mode_defaults_to_lower(self):
        d, *_ = _make_driver()
        d.run_dataflow()
        assert d._column_name_mode == ColumnCaseMode.LOWER

    def test_invalid_column_name_mode_raises(self):
        d, *_ = _make_driver()
        with pytest.raises(ValueError):
            d.run_dataflow(column_name_mode="invalid")

    def test_with_stage_loads_dataflows(self):
        dfs = [_dataflow("a"), _dataflow("b")]
        d, _, md, _ = _make_driver(dataflows=dfs)

        with patch.object(d, "_process_dataflow") as mock_proc:
            mock_proc.return_value = DataFlowRuntimeInfo(
                dataflow_id="df-1",
                status=DataFlowStatus.SUCCEEDED.value,
            )
            result = d.run_dataflow(stage="bronze")

        md.get_dataflows.assert_called_once_with(
            stage="bronze", active_only=True, attach_schema_hints=True,
        )
        assert result.total == 2


# ============================================================================
# _process_dataflow
# ============================================================================


class TestProcessDataflow:
    def test_success_flow(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        # Mock reader
        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=10, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {"col": "val"}

        # Mock pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "transformed_df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            transformers_applied=["SchemaConverter"],
            status=DataFlowStatus.SUCCEEDED.value,
        )

        # Mock writer
        mock_writer = MagicMock()
        mock_writer.write.return_value = DestinationRuntimeInfo(
            rows_written=10, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_writer.get_runtime_info.return_value = DestinationRuntimeInfo(
            rows_written=10, status=DataFlowStatus.SUCCEEDED.value
        )

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline), \
             patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.dataflow_id == "df-1"
        wm.save_watermark.assert_called_once()

    def test_no_data_skipped(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = None
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=0, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {}

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.SKIPPED.value
        wm.save_watermark.assert_not_called()

    def test_failure(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.side_effect = RuntimeError("read error")

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "read error" in result.error_message

    def test_failure_preserves_partial_source_runtime(self):
        """When transform fails, the already-collected source runtime is kept."""
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=42, status=DataFlowStatus.SUCCEEDED.value
        )

        mock_pipeline = MagicMock()
        mock_pipeline.transform.side_effect = RuntimeError("transform boom")

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "transform boom" in result.error_message
        assert result.source.rows_read == 42
        assert result.source.status == DataFlowStatus.SUCCEEDED.value

    def test_failure_preserves_partial_transform_runtime(self):
        """When write fails, source and transform runtimes are preserved."""
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=10, status=DataFlowStatus.SUCCEEDED.value
        )

        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "transformed_df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            transformers_applied=["SchemaConverter"],
            status=DataFlowStatus.SUCCEEDED.value,
        )

        mock_writer = MagicMock()
        mock_writer.write.side_effect = RuntimeError("write boom")

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline), \
             patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "write boom" in result.error_message
        assert result.source.rows_read == 10
        assert result.transform.transformers_applied == ["SchemaConverter"]

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_retry_on_exception(self, mock_sleep):
        cfg = DataCoolieRunConfig(retry_count=2, retry_delay=0.01)
        d, engine, _, wm = _make_driver(config=cfg)
        df = _dataflow()

        call_count = 0

        def fake_read(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RuntimeError("transient")
            return "fake_df"

        mock_reader = MagicMock()
        mock_reader.read.side_effect = fake_read
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=5, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {}

        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value
        )

        mock_writer = MagicMock()
        mock_writer.write.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value
        )
        mock_writer.get_runtime_info.return_value = DestinationRuntimeInfo(
            rows_written=5, status=DataFlowStatus.SUCCEEDED.value
        )

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline), \
             patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.retry_attempts == 2

    def test_etl_logger_called(self):
        etl_logger = MagicMock()
        d, *_ = _make_driver(etl_logger=etl_logger)
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = None
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=0)
        mock_reader.get_new_watermark.return_value = {}

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            d._process_dataflow(df)

        etl_logger.log.assert_called_once()


# ============================================================================
# _process_maintenance
# ============================================================================


class TestProcessMaintenance:
    def test_success(self):
        d, engine, _, _ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
            files_added=1,
            files_removed=3,
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.destination.files_added == 1
        mock_writer.run_maintenance.assert_called_once()
        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is True
        assert call_kwargs.kwargs["do_cleanup"] is True

    def test_compact_only(self):
        """do_cleanup=False skips vacuum; do_compact=True runs optimize."""
        d, *_ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df, do_compact=True, do_cleanup=False)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is True
        assert call_kwargs.kwargs["do_cleanup"] is False

    def test_cleanup_only(self):
        """do_compact=False skips optimize; do_cleanup=True runs vacuum."""
        d, *_ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df, do_compact=False, do_cleanup=True)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is False
        assert call_kwargs.kwargs["do_cleanup"] is True

    def test_failure(self):
        d, *_ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.side_effect = RuntimeError("maint err")

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "maint err" in result.error_message

    def test_etl_logger_not_failing(self):
        """Even if etl_logger.log raises, _process_maintenance doesn't fail."""
        lgr = MagicMock()
        lgr.log.side_effect = RuntimeError("log error")
        d, *_ = _make_driver(etl_logger=lgr)
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value, operation_type="maintenance"
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value


# ============================================================================
# run_maintenance — connection filter + lakehouse only
# ============================================================================


class TestRunMaintenance:
    """run_maintenance delegates to get_maintenance_dataflows."""

    def test_no_args_calls_get_maintenance_dataflows(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        d.run_maintenance()
        md.get_maintenance_dataflows.assert_called_once_with(
            connection=None,
        )

    def test_connection_string_forwarded(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        d.run_maintenance(connection="my_lakehouse")
        md.get_maintenance_dataflows.assert_called_once_with(
            connection="my_lakehouse",
        )

    def test_connection_list_forwarded(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        d.run_maintenance(connection=["lh_a", "lh_b"])
        md.get_maintenance_dataflows.assert_called_once_with(
            connection=["lh_a", "lh_b"],
        )

    def test_pre_loaded_dataflows_skip_metadata(self):
        """When dataflows are passed directly, metadata is not called."""
        d, _, md, _ = _make_driver()
        dfs = [_dataflow()]
        with patch.object(d, "_process_maintenance") as mock_proc:
            mock_proc.return_value = DataFlowRuntimeInfo(
                dataflow_id="df-1",
                status=DataFlowStatus.SUCCEEDED.value,
            )
            d.run_maintenance(dataflows=dfs)
        md.get_maintenance_dataflows.assert_not_called()

    def test_run_maintenance_flags_thread_to_writer(self):
        """do_compact/do_cleanup passed to run_maintenance reach writer."""
        d, _, md, _ = _make_driver()
        df = _dataflow()
        md.get_maintenance_dataflows.return_value = [df]

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            d.run_maintenance(do_compact=False, do_cleanup=True)

        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is False
        assert call_kwargs.kwargs["do_cleanup"] is True

    def test_empty_result_when_no_lakehouse_dataflows(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        result = d.run_maintenance()
        assert result.total == 0
        assert result.succeeded == 0


# ============================================================================
# Factory methods
# ============================================================================


class TestFactoryMethods:
    def test_create_delta_reader(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt=Format.DELTA.value)
        reader = d._create_source_reader(df)
        from datacoolie.sources import DeltaReader
        assert isinstance(reader, DeltaReader)

    def test_create_parquet_reader(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt=Format.PARQUET.value)
        reader = d._create_source_reader(df)
        from datacoolie.sources import FileReader
        assert isinstance(reader, FileReader)

    def test_create_jdbc_reader(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt=Format.SQL.value)
        reader = d._create_source_reader(df)
        from datacoolie.sources import DatabaseReader
        assert isinstance(reader, DatabaseReader)

    def test_create_function_reader(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt=Format.FUNCTION.value)
        reader = d._create_source_reader(df)
        from datacoolie.sources import PythonFunctionReader
        assert isinstance(reader, PythonFunctionReader)

    def test_create_delta_writer(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt=Format.DELTA.value)
        writer = d._create_destination_writer(df)
        from datacoolie.destinations import DeltaWriter
        assert isinstance(writer, DeltaWriter)

    def test_create_iceberg_writer(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt=Format.ICEBERG.value)
        writer = d._create_destination_writer(df)
        from datacoolie.destinations import IcebergWriter
        assert isinstance(writer, IcebergWriter)

    def test_unsupported_source_format(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt="unknown")
        with pytest.raises(DataCoolieError, match="No plugin registered for 'unknown'"):
            d._create_source_reader(df)

    def test_unsupported_dest_format(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt="unknown")
        with pytest.raises(DataCoolieError, match="No plugin registered for 'unknown'"):
            d._create_destination_writer(df)

    def test_create_transformer_pipeline(self):
        d, *_ = _make_driver()
        pipeline = d._create_transformer_pipeline()
        from datacoolie.transformers import TransformerPipeline
        assert isinstance(pipeline, TransformerPipeline)
        assert len(pipeline.transformers) == 7

    def test_create_transformer_pipeline_uses_column_name_mode(self):
        d, *_ = _make_driver()
        d._column_name_mode = ColumnCaseMode.SNAKE
        pipeline = d._create_transformer_pipeline()
        from datacoolie.transformers import ColumnNameSanitizer
        sanitizers = [t for t in pipeline.transformers if isinstance(t, ColumnNameSanitizer)]
        assert len(sanitizers) == 1
        assert sanitizers[0]._mode == ColumnCaseMode.SNAKE

    def test_create_transformer_pipeline_defaults_to_lower(self):
        d, *_ = _make_driver()
        # No _column_name_mode set → falls back to LOWER
        pipeline = d._create_transformer_pipeline()
        from datacoolie.transformers import ColumnNameSanitizer
        sanitizers = [t for t in pipeline.transformers if isinstance(t, ColumnNameSanitizer)]
        assert len(sanitizers) == 1
        assert sanitizers[0]._mode == ColumnCaseMode.LOWER


# ============================================================================
# create_driver factory function
# ============================================================================


class TestCreateDriver:
    def test_basic(self):
        driver = create_driver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
            job_num=2,
            job_index=1,
            max_workers=4,
        )
        assert isinstance(driver, DataCoolieDriver)
        assert driver.config.job_num == 2
        assert driver.config.job_index == 1
        assert driver.config.max_workers == 4

    def test_extra_kwargs(self):
        driver = create_driver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
            dry_run=True,
            retry_count=3,
        )
        assert driver.config.dry_run is True
        assert driver.config.retry_count == 3

    def test_with_base_log_path(self):
        from datacoolie.logging import ETLLogger, SystemLogger

        driver = create_driver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
            base_log_path="/logs",
        )

        assert isinstance(driver._system_logger, SystemLogger)
        assert isinstance(driver._etl_logger, ETLLogger)
        assert driver._system_logger.config.output_path == "/logs/system_logs"
        assert driver._etl_logger.config.output_path == "/logs/etl_logs"


# ============================================================================
# Additional edge cases (merged from edge-case module)
# ============================================================================


class TestDriverInitEdgeCases:
    def test_raises_when_platform_type_mismatch(self):
        class P1: ...

        class P2: ...

        engine = _mock_engine()
        engine.platform = P1()
        with pytest.raises(DataCoolieError, match="Platform type mismatch"):
            DataCoolieDriver(
                engine=engine,
                platform=P2(),
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
            )

    def test_raises_when_no_platform_anywhere(self):
        engine = _mock_engine()
        engine.platform = None
        with pytest.raises(DataCoolieError, match="A platform is required"):
            DataCoolieDriver(
                engine=engine,
                platform=None,
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
            )

    def test_uses_existing_engine_platform_when_no_platform_arg(self):
        existing = _mock_platform()
        engine = _mock_engine()
        engine.platform = existing
        d = DataCoolieDriver(
            engine=engine,
            platform=None,
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
        )
        assert d._engine.platform is existing

    def test_same_platform_type_does_not_raise(self):
        platform = _mock_platform()
        engine = _mock_engine()
        engine.platform = platform
        d = DataCoolieDriver(
            engine=engine,
            platform=platform,
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
        )
        assert d._engine.platform is platform


class TestDriverProcessingEdgeCases:
    def test_process_dataflow_pipeline_error_without_partial(self):
        d, *_ = _make_driver(etl_logger=None)
        df = _dataflow()
        d._retry_handler.execute = MagicMock(side_effect=PipelineError("boom", partial_result=None))

        result = d._process_dataflow(df)
        assert result.status == DataFlowStatus.FAILED.value
        assert result.error_message == "boom"

    def test_process_dataflow_generic_exception_branch(self):
        d, *_ = _make_driver(etl_logger=None)
        df = _dataflow()
        d._retry_handler.execute = MagicMock(side_effect=RuntimeError("explode"))

        result = d._process_dataflow(df)
        assert result.status == DataFlowStatus.FAILED.value
        assert result.error_message == "explode"

    def test_process_dataflow_etl_logger_failure_swallowed(self):
        etl_logger = MagicMock()
        etl_logger.log.side_effect = RuntimeError("log fail")
        d, *_ = _make_driver(etl_logger=etl_logger)
        df = _dataflow()

        src = SourceRuntimeInfo(rows_read=1, status=DataFlowStatus.SUCCEEDED.value)
        trn = TransformRuntimeInfo(status=DataFlowStatus.SUCCEEDED.value)
        dst = DestinationRuntimeInfo(rows_written=1, status=DataFlowStatus.SUCCEEDED.value)
        d._retry_handler.execute = MagicMock(return_value=((src, trn, dst, DataFlowStatus.SUCCEEDED), 1))

        result = d._process_dataflow(df)
        assert result.status == DataFlowStatus.SUCCEEDED.value

    def test_process_maintenance_pipeline_error_with_partial(self):
        d, *_ = _make_driver(etl_logger=None)
        df = _dataflow()
        partial = DestinationRuntimeInfo(status=DataFlowStatus.FAILED.value, error_message="partial")
        d._retry_handler.execute = MagicMock(side_effect=PipelineError("x", partial_result=partial))

        result = d._process_maintenance(df)
        assert result.destination is partial
        assert result.status == DataFlowStatus.FAILED.value

    def test_process_maintenance_generic_exception_branch(self):
        d, *_ = _make_driver(etl_logger=None)
        df = _dataflow()
        d._retry_handler.execute = MagicMock(side_effect=RuntimeError("maint fail"))

        result = d._process_maintenance(df)
        assert result.status == DataFlowStatus.FAILED.value
        assert result.error_message == "maint fail"


class TestDriverHelperBranches:
    def test_resolve_connection_secrets_handles_missing_parts(self):
        d, *_ = _make_driver(config=DataCoolieRunConfig(), etl_logger=None)
        d._secret_provider = MagicMock()
        fake_df = SimpleNamespace(source=None, destination=None)
        d._resolve_connection_secrets(fake_df)  # type: ignore[arg-type]

    def test_create_source_reader_python_function_path(self):
        d, *_ = _make_driver()
        df = _dataflow(fmt=Format.FUNCTION.value)
        df.source.python_function = "module.fn"
        reader = d._create_source_reader(df)
        from datacoolie.sources import PythonFunctionReader

        assert isinstance(reader, PythonFunctionReader)

    def test_flush_logs_swallows_logger_close_errors(self):
        good = MagicMock()
        bad = MagicMock()
        bad.close.side_effect = RuntimeError("close fail")

        d, *_ = _make_driver(system_logger=good, etl_logger=bad)
        d._flush_logs()  # should not raise


# ============================================================================
# Phase G — deep-copy DataFlow isolation
# ============================================================================


class TestDeepCopyDataFlow:
    """Verify _process_dataflow does not mutate the original DataFlow."""

    def test_process_dataflow_does_not_mutate_original(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()
        original_id = df.dataflow_id
        original_table = df.source.table

        mock_reader = MagicMock()
        mock_reader.read.return_value = None
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=0, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {}

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            d._process_dataflow(df)

        # Original DataFlow remains untouched
        assert df.dataflow_id == original_id
        assert df.source.table == original_table


# ============================================================================
# Phase I — watermark skipped on write failure
# ============================================================================


class TestWatermarkAtomicSave:
    """Verify watermark is NOT saved when write fails."""

    def test_write_failure_skips_watermark_save(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=10, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {"col": "2024-01-01"}

        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "transformed_df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            transformers_applied=["SchemaConverter"],
            status=DataFlowStatus.SUCCEEDED.value,
        )

        mock_writer = MagicMock()
        mock_writer.write.side_effect = RuntimeError("write boom")

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline), \
             patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        wm.save_watermark.assert_not_called()
