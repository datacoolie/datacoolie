"""Pure structured projections for persisted logging streams.

Projection has no writer, lock or lifecycle state.  Keeping this transformation
separate makes the persisted contract testable without constructing a logger.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from datacoolie import __version__
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo, JobRuntimeInfo
from datacoolie.logging.runtime.capture import LogRecord
from datacoolie.logging.configuration.constants import LOG_SCHEMA_VERSION, LogType
from datacoolie.utils.converters import as_json as _as_json


def flatten_dataflow_runtime(runtime: DataFlowRuntimeInfo) -> Dict[str, Any]:
    src = runtime.source
    trn = runtime.transform
    dst = runtime.destination
    return {
        "dataflow_run_id": runtime.dataflow_run_id,
        "operation_type": runtime.operation_type,
        "start_time": runtime.start_time,
        "end_time": runtime.end_time,
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
        "status": runtime.status,
        "message": runtime.message,
        "retry_attempts": runtime.retry_attempts,
        "source_start_time": src.start_time,
        "source_end_time": src.end_time,
        "source_duration_seconds": src.duration_seconds,
        "source_status": src.status,
        "source_message": src.message,
        "source_rows_read": src.rows_read,
        "source_action": _as_json(src.source_action),
        "source_watermark_before": _as_json(src.watermark_before),
        "source_watermark_after": _as_json(src.watermark_after),
        "source_watermark_effective": _as_json(src.watermark_effective),
        "source_watermark_start_operator": src.watermark_start_operator,
        "source_watermark_end_operator": src.watermark_end_operator,
        "source_watermark_kind": src.watermark_kind,
        "transform_start_time": trn.start_time,
        "transform_end_time": trn.end_time,
        "transform_duration_seconds": trn.duration_seconds,
        "transform_status": trn.status,
        "transform_message": trn.message,
        "transformers_applied": _as_json(trn.transformers_applied),
        "destination_start_time": dst.start_time,
        "destination_end_time": dst.end_time,
        "destination_duration_seconds": dst.duration_seconds,
        "destination_status": dst.status,
        "destination_message": dst.message,
        "destination_operation_type": dst.operation_type,
        "destination_rows_written": dst.rows_written,
        "destination_rows_inserted": dst.rows_inserted,
        "destination_rows_updated": dst.rows_updated,
        "destination_rows_deleted": dst.rows_deleted,
        "destination_files_added": dst.files_added,
        "destination_files_removed": dst.files_removed,
        "destination_bytes_added": dst.bytes_added,
        "destination_bytes_removed": dst.bytes_removed,
        "destination_bytes_saved": dst.bytes_saved,
        "destination_operation_details": _as_json(dst.operation_details),
    }


def flatten_job_runtime(
    runtime: JobRuntimeInfo,
    *,
    observed_at: Optional[datetime] = None,
    log_session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Project a complete job-runtime record.

    ``observed_at`` is supplied by the logger at the snapshot boundary so this
    pure projection does not read the clock.  Omitting it keeps the helper
    useful for callers that only need the current aggregate summary.
    """

    entry: Dict[str, Any] = {
        "log_schema_version": LOG_SCHEMA_VERSION,
        "_type": LogType.JOB_RUN_LOG.value,
        "datacoolie_version": __version__,
        "log_session_id": log_session_id,
        "job_id": runtime.job_id,
        "job_num": runtime.job_num,
        "job_index": runtime.job_index,
        "workspace_id": runtime.workspace_id,
        "stages": _as_json(runtime.stages) if isinstance(runtime.stages, list) else runtime.stages,
        "engine_name": runtime.engine_name,
        "platform_name": runtime.platform_name,
        "metadata_provider_name": runtime.metadata_provider_name,
        "watermark_manager_name": runtime.watermark_manager_name,
        "max_workers": runtime.max_workers,
        "stop_on_error": runtime.stop_on_error,
        "retry_count": runtime.retry_count,
        "retry_delay": runtime.retry_delay,
        "dry_run": runtime.dry_run,
        "retention_hours": runtime.retention_hours,
        "run_attributes": runtime.run_attributes,
        "start_time": runtime.start_time,
        "end_time": runtime.end_time,
        "duration_seconds": runtime.duration_seconds,
        "status": runtime.status,
        "message": runtime.message,
        "total_dataflows": runtime.total_dataflows,
        "total_succeeded": runtime.total_succeeded,
        "total_failed": runtime.total_failed,
        "total_skipped": runtime.total_skipped,
        "total_running": runtime.total_running,
        "total_pending": runtime.total_pending,
        "total_rows_read": runtime.total_rows_read,
        "total_rows_written": runtime.total_rows_written,
        "total_rows_inserted": runtime.total_rows_inserted,
        "total_rows_updated": runtime.total_rows_updated,
        "total_rows_deleted": runtime.total_rows_deleted,
        "total_files_added": runtime.total_files_added,
        "total_files_removed": runtime.total_files_removed,
        "total_bytes_added": runtime.total_bytes_added,
        "total_bytes_removed": runtime.total_bytes_removed,
        "log_records_dropped": runtime.log_records_dropped,
        "log_bytes_dropped": runtime.log_bytes_dropped,
        "message_truncated": runtime.message_truncated,
        "operation_types": (
            _as_json(runtime.operation_types)
            if isinstance(runtime.operation_types, list)
            else runtime.operation_types
        ),
    }
    if observed_at is not None:
        entry["observed_at"] = observed_at
    return entry


def build_dataflow_entry(
    dataflow: DataFlow,
    runtime: DataFlowRuntimeInfo,
    *,
    job_info: JobRuntimeInfo,
    log_session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Project declarative dataflow metadata plus terminal runtime facts."""
    src = dataflow.source
    dst = dataflow.destination
    trn = dataflow.transform
    entry: Dict[str, Any] = {
        "log_schema_version": LOG_SCHEMA_VERSION,
        "_type": LogType.DATAFLOW_RUN_LOG.value,
        "datacoolie_version": __version__,
        "log_session_id": log_session_id,
        "job_id": job_info.job_id,
        "job_num": job_info.job_num,
        "job_index": job_info.job_index,
        "dataflow_id": dataflow.dataflow_id,
        "workspace_id": dataflow.workspace_id,
        "dataflow_name": dataflow.name,
        "dataflow_description": dataflow.description,
        "stage": dataflow.stage,
        "group_number": dataflow.group_number,
        "execution_order": dataflow.execution_order,
        "processing_mode": dataflow.processing_mode,
        "is_active": dataflow.is_active,
        "configure": _as_json(dataflow.configure),
        "source_id": src.connection.connection_id,
        "source_name": src.connection.name,
        "source_connection_type": src.connection.connection_type,
        "source_format": src.connection.format,
        "source_catalog": src.connection.catalog,
        "source_database": src.connection.database,
        "source_schema": src.schema_name,
        "source_table": src.table,
        "source_full_table": src.full_table_name,
        "source_path": src.path,
        "source_query": src.query,
        "source_python_function": src.python_function,
        "source_watermark_columns": _as_json(src.watermark_columns),
        "source_filter_expression": src.filter_expression,
        "source_configure": _as_json(src.configure),
        "transform_deduplicate_columns": _as_json(trn.deduplicate_columns),
        "transform_latest_data_columns": _as_json(trn.latest_data_columns),
        "transform_filter_expression": trn.filter_expression,
        "transform_additional_columns": _as_json(
            [column.model_dump() for column in trn.additional_columns]
            if trn.additional_columns else None
        ),
        "transform_schema_hints": _as_json(
            {hint.column_name: hint.data_type for hint in trn.schema_hints}
            if trn.schema_hints else None
        ),
        "transform_select_columns": _as_json(trn.select_columns),
        "transform_drop_columns": _as_json(trn.drop_columns),
        "transform_rename_columns": _as_json(trn.rename_columns),
        "transform_value_rules": _as_json(
            [rule.model_dump() for rule in trn.value_rules]
            if trn.value_rules else None
        ),
        "transform_hash_columns": _as_json(
            [column.model_dump() for column in trn.hash_columns]
            if trn.hash_columns else None
        ),
        "transform_masking_rules": _as_json(
            [rule.model_dump() for rule in trn.masking_rules]
            if trn.masking_rules else None
        ),
        "transform_configure": _as_json(trn.configure),
        "destination_id": dst.connection.connection_id,
        "destination_name": dst.connection.name,
        "destination_connection_type": dst.connection.connection_type,
        "destination_format": dst.connection.format,
        "destination_catalog": dst.connection.catalog,
        "destination_database": dst.connection.database,
        "destination_schema": dst.schema_name,
        "destination_table": dst.table,
        "destination_full_table": dst.full_table_name,
        "destination_path": dst.path,
        "destination_load_type": dst.load_type,
        "destination_merge_keys": _as_json(dst.merge_keys),
        "destination_partition_columns": _as_json(dst.partition_column_names),
        "destination_configure": _as_json(dst.configure),
    }
    entry.update(flatten_dataflow_runtime(runtime))
    return entry


def build_system_entry(
    record: LogRecord,
    *,
    job_id: Optional[str],
    job_num: Optional[int],
    job_index: Optional[int],
    log_session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Project one captured Python log record into the system stream."""

    return {
        "log_schema_version": LOG_SCHEMA_VERSION,
        "_type": LogType.SYSTEM_LOG.value,
        "datacoolie_version": __version__,
        "log_session_id": log_session_id,
        "job_id": job_id,
        "job_num": job_num,
        "job_index": job_index,
        **record.to_dict(),
    }
