"""Structured ETL execution logger.

``ETLLogger`` accumulates dataflow / maintenance runtime entries across
one or more ``driver.run()`` invocations within a single job session
and writes them **once** when :meth:`close` is called.

Usage pattern::

    lgr = create_etl_logger(output_path="logs/", job_id="job-1", platform=plat)
    driver.run(stage="bronze")
    driver.run(stage="silver")
    driver.run_maintenance()
    lgr.close()            # single write of all accumulated data

Outputs
-------
Debug JSONL
    A single JSONL file per session (appended to on each periodic flush).
    Per-dataflow entries + job summary as the last line.
    Datetime values are serialised as ISO-8601 strings.

Analyst Parquet (requires ``pyarrow``)
    Two Parquet files per session, each with an explicit PyArrow schema:

    * ``dataflow_<stem>.parquet`` — one row per dataflow/maintenance execution.
    * ``job_summary_<stem>.parquet`` — a single-row job aggregate.

    Datetime columns use ``timestamp[us, tz=UTC]`` for native query support.

Partition layout::

    <output_path>/<purpose>/<log_type>/run_date=yyyy-mm-dd/<filename>
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from datacoolie.core.constants import DataFlowStatus, ExecutionType, LogPurpose, LogType
from datacoolie.core.models import (
    DataCoolieRunConfig,
    DataFlow,
    DataFlowRuntimeInfo,
    JobRuntimeInfo,
)
from datacoolie.logging.base import (
    BaseLogger,
    LogConfig,
    _FlushOperation,
    _FlushSkipped,
    format_partition_path,
    get_logger,
)
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.converters import as_json as _as_json, json_default as _json_default
from datacoolie.utils.helpers import utc_now


_logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Analyst Parquet schema — explicit PyArrow types for every column
# ---------------------------------------------------------------------------


def _build_dataflow_schema(pa: Any) -> Any:
    """Build the PyArrow schema for the dataflow runtime Parquet."""
    return pa.schema([
        # entry markers
        ("_type", pa.string()),
        ("job_id", pa.string()),
        # dataflow identity
        ("dataflow_id", pa.string()),
        ("workspace_id", pa.string()),
        ("dataflow_name", pa.string()),
        ("dataflow_description", pa.string()),
        ("stage", pa.string()),
        ("group_number", pa.int64()),
        ("execution_order", pa.int64()),
        ("processing_mode", pa.string()),
        ("is_active", pa.bool_()),
        ("configure", pa.string()),
        # source config
        ("source_id", pa.string()),
        ("source_name", pa.string()),
        ("source_connection_type", pa.string()),
        ("source_format", pa.string()),
        ("source_catalog", pa.string()),
        ("source_database", pa.string()),
        ("source_schema", pa.string()),
        ("source_table", pa.string()),
        ("source_full_table", pa.string()),
        ("source_path", pa.string()),
        ("source_query", pa.string()),
        ("source_python_function", pa.string()),
        ("source_watermark_columns", pa.string()),
        ("source_filter_expression", pa.string()),
        ("source_configure", pa.string()),
        # transform config
        ("transform_deduplicate_columns", pa.string()),
        ("transform_latest_data_columns", pa.string()),
        ("transform_filter_expression", pa.string()),
        ("transform_additional_columns", pa.string()),
        ("transform_schema_hints", pa.string()),
        ("transform_configure", pa.string()),
        # destination config
        ("destination_id", pa.string()),
        ("destination_name", pa.string()),
        ("destination_connection_type", pa.string()),
        ("destination_format", pa.string()),
        ("destination_catalog", pa.string()),
        ("destination_database", pa.string()),
        ("destination_schema", pa.string()),
        ("destination_table", pa.string()),
        ("destination_full_table", pa.string()),
        ("destination_path", pa.string()),
        ("destination_load_type", pa.string()),
        ("destination_merge_keys", pa.string()),
        ("destination_partition_columns", pa.string()),
        ("destination_configure", pa.string()),
        # run identity
        ("dataflow_run_id", pa.string()),
        ("operation_type", pa.string()),
        # overall timing / status
        ("start_time", pa.timestamp('us', tz='UTC')),
        ("end_time", pa.timestamp('us', tz='UTC')),
        ("duration_seconds", pa.float64()),
        ("overhead_duration_seconds", pa.float64()),
        ("status", pa.string()),
        ("error_message", pa.string()),
        ("retry_attempts", pa.int64()),
        # source runtime
        ("source_start_time", pa.timestamp('us', tz='UTC')),
        ("source_end_time", pa.timestamp('us', tz='UTC')),
        ("source_duration_seconds", pa.float64()),
        ("source_status", pa.string()),
        ("source_error_message", pa.string()),
        ("source_rows_read", pa.int64()),
        ("source_action", pa.string()),
        ("source_watermark_before", pa.string()),
        ("source_watermark_after", pa.string()),
        ("source_watermark_effective", pa.string()),
        # transform runtime
        ("transform_start_time", pa.timestamp('us', tz='UTC')),
        ("transform_end_time", pa.timestamp('us', tz='UTC')),
        ("transform_duration_seconds", pa.float64()),
        ("transform_status", pa.string()),
        ("transform_error_message", pa.string()),
        ("transformers_applied", pa.string()),
        # destination runtime
        ("destination_start_time", pa.timestamp('us', tz='UTC')),
        ("destination_end_time", pa.timestamp('us', tz='UTC')),
        ("destination_duration_seconds", pa.float64()),
        ("destination_status", pa.string()),
        ("destination_error_message", pa.string()),
        ("destination_operation_type", pa.string()),
        ("destination_rows_written", pa.int64()),
        ("destination_rows_inserted", pa.int64()),
        ("destination_rows_updated", pa.int64()),
        ("destination_rows_deleted", pa.int64()),
        ("destination_files_added", pa.int64()),
        ("destination_files_removed", pa.int64()),
        ("destination_bytes_added", pa.int64()),
        ("destination_bytes_removed", pa.int64()),
        ("destination_bytes_saved", pa.int64()),
        ("destination_operation_details", pa.string()),
    ])


# ============================================================================
# ETLLogger
# ============================================================================


class ETLLogger(BaseLogger):
    """Structured ETL execution logger — accumulate-then-flush.

    All :meth:`log` calls accumulate entries in memory.  A single
    :class:`~datacoolie.core.models.JobRuntimeInfo` tracks session-level
    aggregates. On :meth:`close`, the logger appends debug JSONL and writes
    analyst JSONL and Parquet from one terminal snapshot.
    """

    _periodic_sink_name = "debug_jsonl"

    def __init__(
        self,
        config: LogConfig,
        platform: Optional[BasePlatform] = None,
    ) -> None:
        super().__init__(config, platform)
        self._runtime_logs: List[Dict[str, Any]] = []
        self._job_info: JobRuntimeInfo = JobRuntimeInfo(start_time=utc_now())
        self._component_names: Dict[str, Optional[str]] = {}
        self._debug_remote_path: Optional[str] = None
        self._debug_flushed_count = 0
        self._state_lock = threading.RLock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_run_config(self, run_config: DataCoolieRunConfig) -> None:
        with self._state_lock:
            super().set_run_config(run_config)
            self._apply_run_config()

    def set_component_names(
        self,
        engine_name: Optional[str] = None,
        platform_name: Optional[str] = None,
        metadata_provider_name: Optional[str] = None,
        watermark_manager_name: Optional[str] = None,
    ) -> None:
        with self._state_lock:
            self._component_names = {
                "engine_name": engine_name,
                "platform_name": platform_name,
                "metadata_provider_name": metadata_provider_name,
                "watermark_manager_name": watermark_manager_name,
            }
            self._apply_component_names()

    def _apply_run_config(self) -> None:
        rc = self._run_config
        if rc is None:
            return
        self._job_info.job_id = rc.job_id
        self._job_info.job_num = rc.job_num
        self._job_info.job_index = rc.job_index
        self._job_info.max_workers = rc.max_workers
        self._job_info.stop_on_error = rc.stop_on_error
        self._job_info.retry_count = rc.retry_count
        self._job_info.retry_delay = rc.retry_delay
        self._job_info.dry_run = rc.dry_run
        self._job_info.retention_hours = rc.retention_hours

    def _apply_component_names(self) -> None:
        for key, value in self._component_names.items():
            setattr(self._job_info, key, value)

    def log(
        self,
        dataflow: DataFlow,
        runtime_info: DataFlowRuntimeInfo,
    ) -> None:
        """Record one dataflow or maintenance execution result.

        Updates the in-memory source of truth and session aggregates.
        """
        if self._is_closed or self._is_closing:
            return
        entry = self._build_entry(dataflow, runtime_info)
        with self._close_lock:
            if self._is_closed or self._is_closing:
                return
            with self._state_lock:
                self._runtime_logs.append(entry)
                self._update_job_runtime(
                    runtime_info,
                    dataflow.name or dataflow.dataflow_id,
                )

    # ------------------------------------------------------------------
    # Entry construction
    # ------------------------------------------------------------------

    def _build_entry(
        self,
        dataflow: DataFlow,
        runtime: DataFlowRuntimeInfo,
    ) -> Dict[str, Any]:
        """Merge full dataflow config + flattened runtime into one flat row."""
        src = dataflow.source
        dst = dataflow.destination
        trn = dataflow.transform

        entry: Dict[str, Any] = {
            # entry type marker
            "_type":                    LogType.DATAFLOW_RUN_LOG.value,
            "job_id":                   self._job_info.job_id,
            # dataflow identity
            "dataflow_id":              dataflow.dataflow_id,
            "workspace_id":             dataflow.workspace_id,
            "dataflow_name":            dataflow.name,
            "dataflow_description":     dataflow.description,
            "stage":                    dataflow.stage,
            "group_number":             dataflow.group_number,
            "execution_order":          dataflow.execution_order,
            "processing_mode":          dataflow.processing_mode,
            "is_active":                dataflow.is_active,
            "configure":                _as_json(dataflow.configure),
            # source config
            "source_id":                 src.connection.connection_id,
            "source_name":               src.connection.name,
            "source_connection_type":    src.connection.connection_type,
            "source_format":             src.connection.format,
            "source_catalog":            src.connection.catalog,
            "source_database":           src.connection.database,
            "source_schema":             src.schema_name,
            "source_table":              src.table,
            "source_full_table":         src.full_table_name,
            "source_path":               src.path,
            "source_query":              src.query,
            "source_python_function":    src.python_function,
            "source_watermark_columns":  _as_json(src.watermark_columns),
            "source_filter_expression":  src.filter_expression,
            "source_configure":          _as_json(src.configure),
            # transform config
            "transform_deduplicate_columns":  _as_json(trn.deduplicate_columns),
            "transform_latest_data_columns":  _as_json(trn.latest_data_columns),
            "transform_filter_expression":    trn.filter_expression,
            "transform_additional_columns":   _as_json(
                [c.model_dump() for c in trn.additional_columns] if trn.additional_columns else None
            ),
            "transform_schema_hints":         _as_json(
                {h.column_name: h.data_type for h in trn.schema_hints} if trn.schema_hints else None
            ),
            "transform_configure":  _as_json(trn.configure),
            # destination config
            "destination_id":                 dst.connection.connection_id,
            "destination_name":               dst.connection.name,
            "destination_connection_type":    dst.connection.connection_type,
            "destination_format":             dst.connection.format,
            "destination_catalog":            dst.connection.catalog,
            "destination_database":           dst.connection.database,
            "destination_schema":             dst.schema_name,
            "destination_table":              dst.table,
            "destination_full_table":         dst.full_table_name,
            "destination_path":               dst.path,
            "destination_load_type":          dst.load_type,
            "destination_merge_keys":         _as_json(dst.merge_keys),
            "destination_partition_columns":  _as_json(dst.partition_column_names),
            "destination_configure":          _as_json(dst.configure),
        }
        entry.update(self._flatten_dataflow_runtime(runtime))
        return entry

    # ------------------------------------------------------------------
    # Flattening helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_dataflow_runtime(runtime: DataFlowRuntimeInfo) -> Dict[str, Any]:
        """Flatten :class:`DataFlowRuntimeInfo` sub-models into a flat dict.

        Fields are prefixed ``source_*``, ``transform_*``, ``dest_*``.
        Each ``operation_details`` key is also promoted as a
        ``dest_detail_<key>`` column for direct SQL filtering.
        """
        src = runtime.source
        trn = runtime.transform
        dst = runtime.destination

        flat: Dict[str, Any] = {
            # overall timing / status
            "dataflow_run_id":  runtime.dataflow_run_id,
            "operation_type":   runtime.operation_type,
            # overall timing / status
            "start_time":       runtime.start_time,
            "end_time":         runtime.end_time,
            "duration_seconds": runtime.duration_seconds,
            "overhead_duration_seconds": (
                round(
                    runtime.duration_seconds
                    - (src.duration_seconds or 0.0)
                    - (trn.duration_seconds or 0.0)
                    - (dst.duration_seconds or 0.0),
                    6,
                )
                if runtime.duration_seconds is not None else None
            ),
            "status":           runtime.status,
            "error_message":    runtime.error_message,
            "retry_attempts":   runtime.retry_attempts,
            # source runtime
            "source_start_time":        src.start_time,
            "source_end_time":          src.end_time,
            "source_duration_seconds":  src.duration_seconds,
            "source_status":            src.status,
            "source_error_message":     src.error_message,
            "source_rows_read":         src.rows_read,
            "source_action":            _as_json(src.source_action),
            "source_watermark_before":    _as_json(src.watermark_before),
            "source_watermark_after":     _as_json(src.watermark_after),
            "source_watermark_effective": _as_json(src.watermark_effective),
            # transform runtime
            "transform_start_time":         trn.start_time,
            "transform_end_time":           trn.end_time,
            "transform_duration_seconds":   trn.duration_seconds,
            "transform_status":             trn.status,
            "transform_error_message":      trn.error_message,
            "transformers_applied":         _as_json(trn.transformers_applied),
            # destination runtime
            "destination_start_time":          dst.start_time,
            "destination_end_time":            dst.end_time,
            "destination_duration_seconds":    dst.duration_seconds,
            "destination_status":              dst.status,
            "destination_error_message":       dst.error_message,
            "destination_operation_type":      dst.operation_type,
            "destination_rows_written":        dst.rows_written,
            "destination_rows_inserted":       dst.rows_inserted,
            "destination_rows_updated":        dst.rows_updated,
            "destination_rows_deleted":        dst.rows_deleted,
            "destination_files_added":         dst.files_added,
            "destination_files_removed":       dst.files_removed,
            "destination_bytes_added":         dst.bytes_added,
            "destination_bytes_removed":       dst.bytes_removed,
            "destination_bytes_saved":         dst.bytes_saved,
            # operation_details: full-fidelity JSON + promoted scalar columns
            "destination_operation_details":   _as_json(dst.operation_details),
        }

        return flat

    @staticmethod
    def _flatten_job_runtime(ji: JobRuntimeInfo) -> Dict[str, Any]:
        """Flatten :class:`JobRuntimeInfo` into a flat dict.

        ``dataflow_results`` is excluded — entries are stored separately.
        """
        return {
            "job_id":                    ji.job_id,
            "job_num":                   ji.job_num,
            "job_index":                 ji.job_index,
            "workspace_id":              ji.workspace_id,
            "stages":                    _as_json(ji.stages) if isinstance(ji.stages, list) else ji.stages,
            "engine_name":               ji.engine_name,
            "platform_name":             ji.platform_name,
            "metadata_provider_name":    ji.metadata_provider_name,
            "watermark_manager_name":    ji.watermark_manager_name,
            "max_workers":               ji.max_workers,
            "stop_on_error":             ji.stop_on_error,
            "retry_count":               ji.retry_count,
            "retry_delay":               ji.retry_delay,
            "dry_run":                   ji.dry_run,
            "retention_hours":           ji.retention_hours,
            "start_time":                ji.start_time,
            "end_time":                  ji.end_time,
            "duration_seconds":          ji.duration_seconds,
            "status":                    ji.status,
            "error_message":             ji.error_message,
            "total_dataflows":           ji.total_dataflows,
            "total_succeeded":           ji.total_succeeded,
            "total_failed":              ji.total_failed,
            "total_skipped":             ji.total_skipped,
            "total_running":             ji.total_running,
            "total_pending":             ji.total_pending,
            "operation_types":           _as_json(ji.operation_types) if isinstance(ji.operation_types, list) else ji.operation_types,
            "total_rows_read":           ji.total_rows_read,
            "total_rows_written":        ji.total_rows_written,
            "total_rows_inserted":       ji.total_rows_inserted,
            "total_rows_updated":        ji.total_rows_updated,
            "total_rows_deleted":        ji.total_rows_deleted,
            "total_files_added":         ji.total_files_added,
            "total_files_removed":       ji.total_files_removed,
            "total_bytes_added":         ji.total_bytes_added,
            "total_bytes_removed":       ji.total_bytes_removed,
        }

    # ------------------------------------------------------------------
    # Job-runtime aggregation
    # ------------------------------------------------------------------

    def _update_job_runtime(self, runtime: DataFlowRuntimeInfo, dataflow_id_or_name: str | None = None) -> None:
        """Incrementally update session-level counters."""
        ji = self._job_info
        ji.total_dataflows += 1

        status = runtime.status
        if status == DataFlowStatus.SUCCEEDED.value:
            ji.total_succeeded += 1
        elif status == DataFlowStatus.FAILED.value:
            ji.total_failed += 1
            label = dataflow_id_or_name or (runtime.error_message or "")[:128]
            ji.error_message = f"{ji.error_message} {label};" if ji.error_message else f"{label};"
        elif status == DataFlowStatus.SKIPPED.value:
            ji.total_skipped += 1
        elif status == DataFlowStatus.RUNNING.value:
            ji.total_running += 1
        elif status == DataFlowStatus.PENDING.value:
            ji.total_pending += 1

        if runtime.operation_type != ExecutionType.MAINTENANCE.value:
            ji.total_rows_read      += runtime.rows_read
            ji.total_rows_written   += runtime.rows_written
            ji.total_rows_inserted  += runtime.rows_inserted
            ji.total_rows_updated   += runtime.rows_updated
            ji.total_rows_deleted   += runtime.rows_deleted

        ji.total_files_added    += runtime.destination.files_added
        ji.total_files_removed  += runtime.destination.files_removed
        ji.total_bytes_added    += runtime.destination.bytes_added
        ji.total_bytes_removed  += runtime.destination.bytes_removed

    def _build_job_summary(self) -> Dict[str, Any]:
        """Construct the final job summary dict for JSONL and Parquet output."""
        with self._state_lock:
            ji = self._job_info
            ji.end_time = utc_now()

            stages = list(
                dict.fromkeys(
                    entry["stage"]
                    for entry in self._runtime_logs
                    if entry.get("stage")
                )
            )
            ji.stages = stages[0] if len(stages) == 1 else (stages or None)

            op_types = list(
                dict.fromkeys(
                    entry["operation_type"]
                    for entry in self._runtime_logs
                    if entry.get("operation_type")
                )
            )
            ji.operation_types = (
                op_types[0] if len(op_types) == 1 else (op_types or None)
            )

            if ji.status == DataFlowStatus.PENDING.value:
                if ji.total_failed > 0:
                    ji.status = DataFlowStatus.FAILED.value
                elif ji.total_succeeded > 0:
                    ji.status = DataFlowStatus.SUCCEEDED.value
                elif ji.total_skipped > 0:
                    ji.status = DataFlowStatus.SKIPPED.value

            return self._flatten_job_runtime(ji)

    # ------------------------------------------------------------------
    # Debug checkpoints
    # ------------------------------------------------------------------

    def _ensure_debug_remote_path(self) -> str:
        """Return (and lazily compute) the single remote JSONL path."""
        with self._state_lock:
            if self._debug_remote_path is None:
                now = utc_now()
                path = self._partition_path(
                    LogPurpose.DEBUG.value,
                    LogType.JOB_RUN_LOG.value,
                    now,
                )
                self._debug_remote_path = (
                    f"{path}/job_{self._file_stem(now)}.jsonl"
                )
            return self._debug_remote_path

    def _flush_periodic(self) -> None:
        """Append records after the committed debug cursor."""
        if not self._platform or not self._config.output_path:
            return

        with self._state_lock:
            start = self._debug_flushed_count
            end = len(self._runtime_logs)
            records = tuple(self._runtime_logs[start:end])

        if not records:
            return
        content = "".join(
            json.dumps(entry, default=_json_default) + "\n"
            for entry in records
        )
        full_path = self._ensure_debug_remote_path()
        self._platform.append_file(full_path, content)
        with self._state_lock:
            if not self._is_closed:
                self._debug_flushed_count = max(
                    self._debug_flushed_count,
                    end,
                )
        _logger.debug("Debug JSONL periodic flush: %s", full_path)

    # ------------------------------------------------------------------
    # Terminal snapshot
    # ------------------------------------------------------------------

    def _build_final_operations(
        self,
        *,
        periodic_in_flight: bool,
    ) -> Sequence[_FlushOperation]:
        """Build independent terminal sinks from one complete job snapshot."""
        if not self._config.output_path or not self._platform:
            return ()

        with self._state_lock:
            if not self._runtime_logs:
                return ()
            runtime_logs = tuple(self._runtime_logs)
            debug_start = min(self._debug_flushed_count, len(runtime_logs))
            summary = self._build_job_summary()
            now = utc_now()
            stem = self._file_stem(now)

        operations: list[_FlushOperation] = []
        if not periodic_in_flight:
            debug_path = self._ensure_debug_remote_path()
            debug_content = self._build_debug_content(
                runtime_logs[debug_start:],
                summary,
            )
            operations.append(
                _FlushOperation(
                    "debug_jsonl",
                    lambda: self._append_text(debug_path, debug_content),
                    path=debug_path,
                )
            )

        job_path, job_content = self._build_analyst_job_payload(summary, now)
        operations.append(
            _FlushOperation(
                "analyst_job_jsonl",
                lambda: self._append_text(job_path, job_content),
                path=job_path,
            )
        )

        dataflow_path = self._build_analyst_dataflow_path(stem, now)
        operations.append(
            _FlushOperation(
                "analyst_dataflow_parquet",
                lambda: self._write_analyst_dataflow_output(
                    runtime_logs,
                    dataflow_path,
                ),
                path=dataflow_path,
            )
        )
        return tuple(operations)

    # ------------------------------------------------------------------
    # Storage writers
    # ------------------------------------------------------------------

    def _file_stem(self, now: datetime) -> str:
        """Build the common ``<ts>_<job_num>_<job_index>_<job_id>`` stem."""
        ts = now.strftime("%Y%m%d_%H%M%S")
        rc = self._run_config
        job_id = (rc.job_id if rc else None) or "default"
        job_num = rc.job_num if rc else 1
        job_index = rc.job_index if rc else 0
        return f"{ts}_{job_num}_{job_index}_{job_id}"

    def _partition_path(self, purpose: str, log_type: str, run_date: datetime) -> str:
        base = (self._config.output_path or "").rstrip('/')
        path = f"{base}/{purpose}/{log_type}"
        if self._config.partition_by_date:
            path = format_partition_path(path, run_date, pattern=self._config.partition_pattern)
        return path

    @staticmethod
    def _build_debug_content(
        runtime_logs: Sequence[Dict[str, Any]],
        summary: Dict[str, Any],
    ) -> str:
        """Serialize remaining debug rows and the terminal summary."""
        lines = [
            json.dumps(entry, default=_json_default)
            for entry in runtime_logs
        ]
        lines.append(
            json.dumps(
                {"_type": LogType.JOB_RUN_LOG.value, **summary},
                default=_json_default,
            )
        )
        return "\n".join(lines) + "\n"

    def _append_text(self, path: str, content: str) -> None:
        """Append a prepared immutable text payload."""
        if self._platform is None:
            return
        self._platform.append_file(path, content)

    def _build_analyst_job_payload(
        self,
        summary: Dict[str, Any],
        run_date: datetime,
    ) -> tuple[str, str]:
        """Build the existing analyst job-summary path and JSONL row."""
        summary_row: Dict[str, Any] = {"_type": LogType.JOB_RUN_LOG.value, **summary}
        job_log_dir = self._partition_path(
            LogPurpose.ANALYST.value, LogType.JOB_RUN_LOG.value, run_date,
        )
        # Derive date stem from partition_pattern (same values as format_partition_path)
        # e.g. "__run_date={year}-{month}-{day}" → "__run_date=2026-05-24" → "20260524"
        _pf = self._config.partition_pattern.format(
            year=run_date.year,
            month=f"{run_date.month:02d}",
            day=f"{run_date.day:02d}",
            hour=f"{run_date.hour:02d}",
        )
        date_stem = re.sub(r"\D", "", _pf)
        job_log_path = f"{job_log_dir}/job_run_log_{date_stem}.jsonl"
        job_line = json.dumps(summary_row, default=_json_default) + "\n"
        return job_log_path, job_line

    def _build_analyst_dataflow_path(
        self,
        stem: str,
        run_date: datetime,
    ) -> str:
        """Build the existing per-run analyst Parquet path."""
        path = self._partition_path(
            LogPurpose.ANALYST.value,
            LogType.DATAFLOW_RUN_LOG.value,
            run_date,
        )
        return f"{path}/dataflow_{stem}.parquet"

    def _write_analyst_dataflow_output(
        self,
        runtime_logs: Sequence[Dict[str, Any]],
        full_path: str,
    ) -> None:
        """Upload the existing analyst dataflow Parquet artifact."""
        if not runtime_logs:
            return
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            _logger.warning("pyarrow not installed — Parquet output skipped.")
            raise _FlushSkipped("pyarrow is not installed") from None

        schema = _build_dataflow_schema(pa)
        self._write_parquet_file(
            runtime_logs,
            full_path,
            pa,
            pq,
            schema=schema,
        )

    def _write_parquet_file(
        self,
        data: Sequence[Dict[str, Any]],
        full_path: str,
        pa: Any,
        pq: Any,
        schema: Any = None,
    ) -> None:
        if schema:
            schema_names = tuple(field.name for field in schema)
            schema_name_set = set(schema_names)
            unknown_keys = set().union(*(row.keys() for row in data)) - schema_name_set
            if unknown_keys:
                _logger.warning(
                    "ETL Parquet projection omitted %d unknown field(s): %s",
                    len(unknown_keys),
                    ", ".join(sorted(unknown_keys)),
                )
            projected = [
                {
                    name: (
                        json.dumps(value, default=_json_default)
                        if isinstance(value := row.get(name), (dict, list))
                        else value
                    )
                    for name in schema_names
                }
                for row in data
            ]
            table = pa.Table.from_pylist(projected, schema=schema)
        else:
            sanitized = [
                {
                    key: (
                        json.dumps(value, default=_json_default)
                        if isinstance(value, (dict, list))
                        else value
                    )
                    for key, value in row.items()
                }
                for row in data
            ]
            table = pa.Table.from_pylist(sanitized)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            pq.write_table(table, tmp_path, compression="snappy")
            self._platform.upload_file(tmp_path, full_path)
            _logger.debug("Parquet written: %s", full_path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _cleanup(self) -> None:
        for outcome in self._terminal_outcomes:
            if outcome.status == "succeeded" and outcome.path:
                _logger.info("ETL log pushed: %s", outcome.path)
        super()._cleanup()
        with self._state_lock:
            self._runtime_logs.clear()
            self._debug_remote_path = None
            self._debug_flushed_count = 0
            self._job_info = JobRuntimeInfo(start_time=utc_now())
            self._apply_run_config()
            self._apply_component_names()


# ============================================================================
# Factory
# ============================================================================


def create_etl_logger(
    output_path: Optional[str] = None,
    platform: Optional[BasePlatform] = None,
) -> ETLLogger:
    """Create an :class:`ETLLogger` with common configuration."""
    config = LogConfig(output_path=output_path)
    return ETLLogger(config, platform)
