"""Structured execution and job-runtime logging."""

from __future__ import annotations

import copy
import json
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from datacoolie.core.constants import DataFlowStatus, ExecutionType
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo, DestinationRuntimeInfo, JobRuntimeInfo, SourceRuntimeInfo, TransformRuntimeInfo
from datacoolie.logging.base import (
    BaseLogger,
    _FlushOperation,
)
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import FlushResult, LogType
from datacoolie.logging.persistence.layout import build_job_stem, format_partition_path
from datacoolie.logging.persistence.writer import (
    JsonLogWriter,
    PersistenceStats,
    SharedByteBudget,
)
from datacoolie.logging.persistence.projection import (
    build_dataflow_entry,
    flatten_dataflow_runtime,
    flatten_job_runtime,
)
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.time import utc_now


_EXECUTION_DATAFLOW_SINK_NAME = "execution_dataflow_log"
_EXECUTION_JOB_SINK_NAME = "execution_job_log"
_TERMINAL_STATUSES = {
    DataFlowStatus.SUCCEEDED.value,
    DataFlowStatus.FAILED.value,
    DataFlowStatus.SKIPPED.value,
}
_JOB_TERMINAL_STATUSES = _TERMINAL_STATUSES


class ExecutionLogger(BaseLogger):
    """Persist terminal dataflow rows and one mutable job summary.

    The projection module owns record shape while this logger owns counters and
    lifecycle; :class:`JsonLogWriter` owns encoded buffering and remote
    publication.  Dataflow rows are accepted only
    at the terminal observational boundary so a killed operation does not
    create a misleading partial record.
    """

    _periodic_sink_name = _EXECUTION_DATAFLOW_SINK_NAME
    _max_message_bytes = 16 * 1024

    def __init__(
        self,
        config: LogConfig,
        platform: Optional[BasePlatform] = None,
    ) -> None:
        super().__init__(config, platform)
        self._job_info = JobRuntimeInfo(start_time=utc_now())
        self._component_names: Dict[str, Optional[str]] = {}
        self._state_lock = threading.RLock()
        self._dataflow_writer: Optional[JsonLogWriter] = None
        self._job_writer: Optional[JsonLogWriter] = None
        self._job_stem: Optional[str] = None
        self._stages: List[str] = []
        self._operation_types: List[str] = []
        self._job_summary_dirty = True
        self._job_summary_revision = 0
        self._job_finalized = False
        self._log_records_dropped = 0
        self._log_bytes_dropped = 0

    # ------------------------------------------------------------------
    # Configuration and lifecycle
    # ------------------------------------------------------------------

    def set_component_names(
        self,
        engine_name: Optional[str] = None,
        platform_name: Optional[str] = None,
        metadata_provider_name: Optional[str] = None,
        watermark_manager_name: Optional[str] = None,
    ) -> None:
        with self._close_lock:
            if self._is_active or self._is_closing or self._is_closed:
                raise ConfigurationError(
                    f"{type(self).__name__} component names cannot change after activation"
                )
            with self._state_lock:
                self._component_names = {
                    "engine_name": engine_name,
                    "platform_name": platform_name,
                    "metadata_provider_name": metadata_provider_name,
                    "watermark_manager_name": watermark_manager_name,
                }
                self._apply_component_names()

    def _activate(self) -> None:
        with self._state_lock:
            started_at = self.started_at or utc_now()
            self._job_info.start_time = started_at
            self._job_info.end_time = None
            self._job_info.status = DataFlowStatus.RUNNING.value
            self._job_stem = self._build_job_stem(started_at)
            self._create_writers(started_at)
            self._job_summary_dirty = True

        # Startup checkpoint is observational.  A storage failure must be
        # visible through logger health but must not reject a valid Driver.
        self._run_bounded_startup(
            lambda: self._flush_job_snapshot(force=True),
            sink_name=_EXECUTION_JOB_SINK_NAME,
        )

    def _create_writers(self, started_at: datetime) -> None:
        if not self._platform or not self._config.output_path or self._run_config is None:
            return
        stem = self._job_stem or self._build_job_stem(started_at)
        job_dir = self._partition_path(LogType.JOB_RUN_LOG.value, started_at)
        dataflow_dir = self._partition_path(LogType.DATAFLOW_RUN_LOG.value, started_at)
        job_path = f"{job_dir}/job_{stem}.json"
        dataflow_path = f"{dataflow_dir}/dataflow_{stem}.json"
        capacity_budget = SharedByteBudget(
            self._config.spool_max_bytes,
            # Keep a bounded headroom for the replace-one job summary even
            # when terminal dataflow rows consume the regular stream budget.
            protected_limit=min(64 * 1024, self._config.spool_max_bytes // 4),
        )

        def batch_path(log_type: str, prefix: str, sequence: int) -> str:
            return (
                f"{self._partition_path(log_type, utc_now())}/"
                f"{prefix}_{stem}_part_{sequence:08d}.json"
            )

        self._dataflow_writer = JsonLogWriter(
            self._platform,
            dataflow_path,
            self._config,
            batch_path_factory=lambda seq: batch_path(
                LogType.DATAFLOW_RUN_LOG.value, "dataflow", seq
            ),
            name="execution_dataflow",
            capacity_budget=capacity_budget,
        )
        job_config = copy.deepcopy(self._config)
        job_config.persistence_mode = "snapshot"
        self._job_writer = JsonLogWriter(
            self._platform,
            job_path,
            job_config,
            name="execution_job",
            capacity_budget=capacity_budget,
            protected_budget=True,
        )

    def _activation_cleanup(self) -> None:
        self._close_writers()

    def _close_writers(self) -> None:
        for writer in (self._dataflow_writer, self._job_writer):
            if writer is not None:
                writer.close()
        self._dataflow_writer = None
        self._job_writer = None

    # ------------------------------------------------------------------
    # Public logging and finalization
    # ------------------------------------------------------------------

    def log(self, dataflow: DataFlow, runtime_info: DataFlowRuntimeInfo) -> None:
        """Record one terminal dataflow or maintenance observation."""

        if not isinstance(dataflow, DataFlow) or not isinstance(
            runtime_info, DataFlowRuntimeInfo
        ):
            raise ConfigurationError(
                "ExecutionLogger.log requires DataFlow and DataFlowRuntimeInfo instances"
            )
        if not all(
            isinstance(value, expected)
            for value, expected in (
                (runtime_info.source, SourceRuntimeInfo),
                (runtime_info.transform, TransformRuntimeInfo),
                (runtime_info.destination, DestinationRuntimeInfo),
            )
        ):
            raise ConfigurationError(
                "ExecutionLogger.log requires source, transform and destination runtime details"
            )
        if runtime_info.status not in _TERMINAL_STATUSES:
            raise ConfigurationError(
                "ExecutionLogger.log accepts only terminal dataflow statuses: "
                f"{sorted(_TERMINAL_STATUSES)}"
            )
        with self._close_lock:
            if self._is_closed or self._is_closing:
                raise ConfigurationError("ExecutionLogger.log is not available while closing")
            if not self._is_active:
                raise ConfigurationError(
                    "ExecutionLogger.log requires an activated logger"
                )
            with self._state_lock:
                if self._job_finalized:
                    raise ConfigurationError(
                        "ExecutionLogger.log cannot add observations after job finalization"
                    )
                # Project and aggregate under the same lifecycle/state
                # boundary so cleanup cannot reset the job snapshot while a
                # terminal record is being built.
                entry = self._build_entry(dataflow, runtime_info)
                self._update_job_runtime(
                    runtime_info,
                    dataflow.name or dataflow.dataflow_id,
                )
                stage = dataflow.stage
                if stage and stage not in self._stages:
                    self._stages.append(stage)
                operation_type = runtime_info.operation_type
                if operation_type and operation_type not in self._operation_types:
                    self._operation_types.append(operation_type)
                # The business observation and its aggregate summary remain
                # meaningful even when detail persistence raises unexpectedly.
                # Mark the summary dirty before entering the writer boundary so
                # a later checkpoint can still publish those counters.
                self._job_summary_dirty = True
                self._job_summary_revision += 1
                if self._dataflow_writer is not None:
                    dropped_before = self._dataflow_writer.stats.dropped_bytes
                    admitted = self._dataflow_writer.append(entry)
                    if not admitted:
                        stats = self._dataflow_writer.stats
                        self._log_records_dropped += 1
                        self._log_bytes_dropped += max(
                            0, stats.dropped_bytes - dropped_before
                        )
                        self._job_info.log_records_dropped = self._log_records_dropped
                        self._job_info.log_bytes_dropped = self._log_bytes_dropped
                    elif self._dataflow_writer.should_flush:
                        self._request_periodic_flush()

    def finish_job(
        self,
        status: str,
        *,
        end_time: Optional[datetime] = None,
        message: Optional[str] = None,
    ) -> None:
        """Set the business outcome used by the final job snapshot."""

        if status not in _JOB_TERMINAL_STATUSES:
            raise ConfigurationError(
                f"job status must be one of {sorted(_JOB_TERMINAL_STATUSES)}"
            )
        with self._close_lock:
            if self._is_closed:
                return
            if not self._is_active and not self._is_closing:
                raise ConfigurationError(
                    "ExecutionLogger.finish_job requires an activated logger"
                )
            with self._state_lock:
                if self._job_finalized:
                    if self._job_info.status != status:
                        raise ConfigurationError("job status cannot be finalized twice with conflicting values")
                    return
                self._job_info.status = status
                self._job_info.end_time = end_time or utc_now()
                if message:
                    self._set_message(message)
                elif status == DataFlowStatus.SKIPPED.value and not self._job_info.message:
                    self._job_info.message = (
                        "All dataflows were skipped"
                        if self._job_info.total_dataflows
                        else "No dataflows were executed"
                    )
                self._job_finalized = True
                self._job_summary_dirty = True
                self._job_summary_revision += 1

    @property
    def job_runtime(self) -> JobRuntimeInfo:
        with self._state_lock:
            return copy.deepcopy(self._job_info)

    @property
    def persistence_stats(self) -> Dict[str, PersistenceStats]:
        return {
            name: writer.stats
            for name, writer in (
                ("job", self._job_writer),
                ("dataflow", self._dataflow_writer),
            )
            if writer is not None
        }

    # ------------------------------------------------------------------
    # BaseLogger hooks
    # ------------------------------------------------------------------

    def _flush_periodic(self) -> FlushResult:
        if not self._platform or not self._config.output_path:
            return FlushResult.NO_WORK
        errors: list[Exception] = []
        results: list[FlushResult] = []
        if self._dataflow_writer is not None:
            try:
                if self._dataflow_writer.pending and (
                    self._config.persistence_mode == "snapshot"
                    or self._dataflow_writer.should_flush
                    or self._periodic_time_due
                ):
                    result = self._dataflow_writer.flush(force=True)
                    results.append(result)
                    if result is FlushResult.WRITTEN:
                        self._clear_flush_error(_EXECUTION_DATAFLOW_SINK_NAME)
            except Exception as exc:
                self._set_flush_error(_EXECUTION_DATAFLOW_SINK_NAME, exc)
                errors.append(exc)
        try:
            result = self._flush_job_snapshot(force=False)
            results.append(result)
            if result is FlushResult.WRITTEN:
                self._clear_flush_error(_EXECUTION_JOB_SINK_NAME)
        except Exception as exc:
            self._set_flush_error(_EXECUTION_JOB_SINK_NAME, exc)
            errors.append(exc)
        if errors:
            raise errors[-1]
        if FlushResult.IN_FLIGHT in results:
            return FlushResult.IN_FLIGHT
        if FlushResult.WRITTEN in results:
            return FlushResult.WRITTEN
        return FlushResult.NO_WORK

    def _on_periodic_timeout(self) -> None:
        for writer in (self._dataflow_writer, self._job_writer):
            if writer is not None and writer.in_flight:
                writer.mark_timed_out()

    def _on_startup_timeout(self, sink_name: str) -> None:
        if sink_name == _EXECUTION_JOB_SINK_NAME and self._job_writer is not None:
            self._job_writer.mark_timed_out()

    def _periodic_timeout_sinks(self) -> tuple[str, ...]:
        sinks = tuple(
            sink_name
            for sink_name, writer in (
                (_EXECUTION_DATAFLOW_SINK_NAME, self._dataflow_writer),
                (_EXECUTION_JOB_SINK_NAME, self._job_writer),
            )
            if writer is not None and writer.in_flight
        )
        return sinks or super()._periodic_timeout_sinks()

    def _on_terminal_timeout(self, sink_name: str) -> None:
        writer = (
            self._dataflow_writer
            if sink_name == _EXECUTION_DATAFLOW_SINK_NAME
            else self._job_writer
            if sink_name == _EXECUTION_JOB_SINK_NAME
            else None
        )
        if writer is not None:
            writer.mark_timed_out()

    def _build_final_operations(
        self,
        *,
        periodic_in_flight: bool,
    ) -> Sequence[_FlushOperation]:
        operations: list[_FlushOperation] = []
        dataflow_writer = self._dataflow_writer
        if (
            dataflow_writer is not None
            and not periodic_in_flight
            and dataflow_writer.pending
            and not dataflow_writer.in_flight
        ):
            operations.append(
                _FlushOperation(
                    _EXECUTION_DATAFLOW_SINK_NAME,
                    lambda writer=dataflow_writer: self._flush_dataflow_terminal(
                        writer
                    ),
                )
            )
        job_writer = self._job_writer
        if job_writer is not None and not job_writer.in_flight and (
            self._job_summary_dirty or job_writer.pending
        ):
            operations.append(
                _FlushOperation(
                    _EXECUTION_JOB_SINK_NAME,
                    lambda writer=job_writer: self._flush_job_terminal(writer),
                )
            )
        return tuple(operations)

    @staticmethod
    def _flush_dataflow_terminal(writer: JsonLogWriter) -> FlushResult:
        """Drain every admitted dataflow part during the terminal attempt.

        A failed batch retry and records admitted while that retry was in
        flight are separate immutable parts.  Closing after only the first
        successful retry would otherwise discard the newer active part.
        """

        result = FlushResult.NO_WORK
        while writer.pending:
            result = writer.flush(force=True)
            if result is not FlushResult.WRITTEN:
                return result
        return result

    def _flush_job_terminal(self, writer: JsonLogWriter) -> FlushResult:
        """Persist the latest JobRuntime revision before terminal cleanup."""

        result = FlushResult.NO_WORK
        while self._job_summary_dirty or writer.pending:
            result = self._flush_job_snapshot(force=True, writer=writer)
            if result is not FlushResult.WRITTEN:
                return result
        return result

    def _flush_job_snapshot(
        self,
        *,
        force: bool,
        writer: Optional[JsonLogWriter] = None,
    ) -> FlushResult:
        if writer is None:
            writer = self._job_writer
        if writer is None:
            return FlushResult.NO_WORK
        with self._state_lock:
            if not self._job_summary_dirty and not force:
                return FlushResult.NO_WORK
            record = self._job_record()
            revision = self._job_summary_revision
            dropped_before = writer.stats.dropped_bytes
            admitted = writer.replace(record)
            if not admitted:
                self._log_records_dropped += 1
                self._log_bytes_dropped += max(
                    0,
                    writer.stats.dropped_bytes - dropped_before,
                )
                self._job_info.log_records_dropped = self._log_records_dropped
                self._job_info.log_bytes_dropped = self._log_bytes_dropped
                return FlushResult.NO_WORK
        result = writer.flush(force=True)
        with self._state_lock:
            # A concurrent replacement can be coalesced behind an immutable
            # in-flight snapshot.  Keep the summary dirty when that flush was
            # skipped so the next periodic/final attempt publishes the latest
            # record instead of silently losing the update.
            flush_succeeded = result is FlushResult.WRITTEN
            self._job_summary_dirty = (
                self._job_summary_revision != revision
                or not flush_succeeded
                # A successful retry may promote a queued replacement into
                # the writer's active snapshot.  The queued marker is then
                # cleared, but the promoted snapshot is still dirty.
                or writer.pending
            )
        return result

    def _cleanup(self) -> None:
        super()._cleanup()
        with self._state_lock:
            self._close_writers()
            # BaseLogger instances are terminal after close; retain the final
            # job snapshot for post-close inspection instead of resetting it
            # into a misleading pending/empty state.
            self._job_stem = None

    # ------------------------------------------------------------------
    # Record projection
    # ------------------------------------------------------------------

    def _job_record(self) -> Dict[str, Any]:
        return self._build_job_summary(observed_at=utc_now())

    def _build_entry(self, dataflow: DataFlow, runtime: DataFlowRuntimeInfo) -> Dict[str, Any]:
        return build_dataflow_entry(
            dataflow,
            runtime,
            job_info=self._job_info,
            log_session_id=self.log_session_id,
        )

    @staticmethod
    def _flatten_dataflow_runtime(runtime: DataFlowRuntimeInfo) -> Dict[str, Any]:
        """Delegate runtime projection to the standalone projection module."""
        return flatten_dataflow_runtime(runtime)

    # ------------------------------------------------------------------
    # Aggregation and paths
    # ------------------------------------------------------------------

    def _apply_run_config(self) -> None:
        with self._state_lock:
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
            self._job_info.run_attributes = (
                json.dumps(
                    rc.run_attributes,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                if rc.run_attributes is not None else None
            )

    def _apply_component_names(self) -> None:
        for key, value in self._component_names.items():
            setattr(self._job_info, key, value)

    def _update_job_runtime(self, runtime: DataFlowRuntimeInfo, label: Optional[str]) -> None:
        ji = self._job_info
        ji.total_dataflows += 1
        if runtime.status == DataFlowStatus.SUCCEEDED.value:
            ji.total_succeeded += 1
        elif runtime.status == DataFlowStatus.FAILED.value:
            ji.total_failed += 1
            # JobRuntime is a compact index of failed dataflows.  The full
            # error and phase details remain on the dataflow runtime record;
            # including them here would duplicate the Driver's session error
            # channel and make a summary harder to scan.
            identity = label or runtime.dataflow_id or "dataflow"
            if runtime.dataflow_id and label and label != runtime.dataflow_id:
                identity = f"{label} [{runtime.dataflow_id}]"
            self._set_message(identity)
        elif runtime.status == DataFlowStatus.SKIPPED.value:
            ji.total_skipped += 1

        if runtime.operation_type != ExecutionType.MAINTENANCE.value:
            ji.total_rows_read += runtime.rows_read
            ji.total_rows_written += runtime.rows_written
            ji.total_rows_inserted += runtime.rows_inserted
            ji.total_rows_updated += runtime.rows_updated
            ji.total_rows_deleted += runtime.rows_deleted
        ji.total_files_added += runtime.destination.files_added
        ji.total_files_removed += runtime.destination.files_removed
        ji.total_bytes_added += runtime.destination.bytes_added
        ji.total_bytes_removed += runtime.destination.bytes_removed
        ji.log_records_dropped = self._log_records_dropped
        ji.log_bytes_dropped = self._log_bytes_dropped

    def _set_message(self, message: str) -> None:
        existing = self._job_info.message or ""
        candidate = f"{existing}; {message}" if existing else message
        encoded = candidate.encode("utf-8")
        if len(encoded) > self._max_message_bytes:
            encoded = encoded[: self._max_message_bytes]
            candidate = encoded.decode("utf-8", errors="ignore")
            self._job_info.message_truncated = True
        self._job_info.message = candidate

    def _build_job_summary(
        self,
        *,
        observed_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        with self._state_lock:
            ji = self._job_info
            ji.stages = self._stages[0] if len(self._stages) == 1 else (list(self._stages) or None)
            ji.operation_types = (
                self._operation_types[0]
                if len(self._operation_types) == 1
                else (list(self._operation_types) or None)
            )
            ji.log_records_dropped = self._log_records_dropped
            ji.log_bytes_dropped = self._log_bytes_dropped
            return flatten_job_runtime(
                ji,
                observed_at=observed_at,
                log_session_id=self.log_session_id,
            )

    def _build_job_stem(self, started_at: datetime) -> str:
        rc = self._run_config
        job_id = rc.job_id if rc else self._job_info.job_id
        job_num = rc.job_num if rc else self._job_info.job_num
        job_index = rc.job_index if rc else self._job_info.job_index
        return build_job_stem(
            started_at,
            job_id=job_id,
            job_num=job_num,
            job_index=job_index,
        )

    def _partition_path(self, log_type: str, run_date: datetime) -> str:
        base = (self._config.output_path or "").rstrip("/")
        path = f"{base}/{log_type}"
        if self._config.partition_by_date:
            path = format_partition_path(
                path,
                run_date,
                pattern=self._config.partition_pattern,
            )
        return path


def create_execution_logger(
    output_path: Optional[str] = None,
    platform: Optional[BasePlatform] = None,
    config: Optional[LogConfig] = None,
) -> ExecutionLogger:
    """Create an execution logger with the framework defaults."""

    effective = copy.deepcopy(config) if config is not None else LogConfig()
    if output_path is not None:
        effective.output_path = output_path
    return ExecutionLogger(effective, platform)
