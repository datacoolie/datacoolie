"""Pure structured projections for the v6 persisted logging streams.

Declarative metadata keeps its historical field names and encoding. Runtime
observations use the explicit nested serializers from :mod:`serialization` so
adding a Python model attribute cannot silently change the log schema.
"""

from __future__ import annotations

from datetime import date, datetime, time
from dataclasses import fields, is_dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, Optional

from datacoolie import __version__
from datacoolie.logging.configuration.constants import LOG_SCHEMA_VERSION, LogType
from datacoolie.logging.persistence.serialization import (
    serialize_attempt,
    serialize_metrics,
    serialize_read_request,
)
from datacoolie.logging.runtime.capture import LogRecord
from datacoolie.logging.persistence.bounded_encoder import (
    BoundedEncodingError, bounded_json_dumps, bounded_json_size,
)
from datacoolie.logging.persistence.diagnostics import DiagnosticBudget, OMITTED

if TYPE_CHECKING:
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.orchestration.execution.runtime import DataFlowRuntimeInfo
    from datacoolie.orchestration.runtime import JobRuntimeInfo


def _timestamp(value: Any) -> Any:
    """Return an ISO timestamp while preserving optional values."""

    if isinstance(value, datetime):
        return datetime.isoformat(value)
    return value


def flatten_dataflow_runtime(
    runtime: "DataFlowRuntimeInfo", *, diagnostic_max_bytes: int = 65536,
    max_record_bytes: int = 1048576,
) -> Dict[str, Any]:
    """Project the runtime root with one shared diagnostic budget."""

    budget = DiagnosticBudget(max_bytes=diagnostic_max_bytes)
    # Authoritative subtrees are screened before tagged codec round trips.
    _screen_authoritative(runtime.read_request, max_record_bytes)
    result = {
        "dataflow_run_id": runtime.dataflow_run_id,
        "operation_type": runtime.operation_type,
        "start_time": _timestamp(runtime.start_time),
        "end_time": _timestamp(runtime.end_time),
        "duration_seconds": runtime.duration_seconds,
        "status": runtime.status,
        "retry_attempts": runtime.retry_attempts,
        "read_request": serialize_read_request(runtime.read_request),
        "metrics": serialize_metrics(runtime.metrics),
    }
    message = budget.normalize(runtime.message, path="/message")
    if message is not OMITTED:
        result["message"] = message
    result["attempts"] = []
    remaining = max_record_bytes
    for index, attempt in enumerate(runtime.attempts):
        if attempt.source is not None:
            _screen_authoritative(attempt.source.selection, remaining)
        _screen_authoritative(attempt.watermark, remaining)
        projected = serialize_attempt(attempt, budget=budget, path=f"/attempts/{index}")
        size = bounded_json_size(projected, max_bytes=remaining, allow_nan=False)
        remaining -= size + 1
        if remaining <= 0:
            raise ValueError("attempt history exceeds logging record limit")
        result["attempts"].append(projected)
    if budget.issues:
        result["diagnostic_issues"] = budget.issues
    return result


def _screen_authoritative(value: Any, max_bytes: int) -> None:
    # Dataclass fields are read lazily during the count pass, before model_dump
    # copies authored dictionaries/lists. No public record is built by reflection.
    def default(item: Any) -> Any:
        if (
            is_dataclass(item) and not isinstance(item, type)
            and type(item).__module__.startswith("datacoolie.")
        ):
            return {field.name: getattr(item, field.name) for field in fields(item)}
        if isinstance(item, datetime):
            return datetime.isoformat(item)
        if isinstance(item, date):
            return date.isoformat(item)
        if isinstance(item, time):
            return time.isoformat(item)
        if type(item).__name__ == "SecretStr":
            return "***"
        if isinstance(item, bytes):
            if len(item) > max_bytes:
                raise BoundedEncodingError("authoritative bytes exceed record limit")
            return bytes.__str__(item)
        if isinstance(item, Decimal):
            if not Decimal.is_finite(item):
                raise TypeError("non-finite authoritative decimal")
            if Decimal.__sizeof__(item) > max_bytes:
                raise BoundedEncodingError("authoritative decimal exceeds record limit")
            # Decimal stores coefficient digits compactly; with storage bounded,
            # scientific exponents do not create expanded fixed-point strings.
            return Decimal.__str__(item)
        raise TypeError("unsupported authoritative logging value")
    bounded_json_size(value, max_bytes=max_bytes, default=default, allow_nan=False)


def _metadata_json(value: Any, max_bytes: int) -> str | None:
    if not value:
        return None
    def default(item: Any) -> Any:
        if isinstance(item, (datetime, date, time, Decimal, bytes)) or type(item).__name__ == "SecretStr":
            _screen_authoritative(item, max_bytes)
            if isinstance(item, datetime):
                return datetime.isoformat(item)
            if isinstance(item, date):
                return date.isoformat(item)
            if isinstance(item, time):
                return time.isoformat(item)
            if isinstance(item, Decimal):
                return Decimal.__str__(item)
            if isinstance(item, bytes):
                return bytes.__str__(item)
            return "***"
        raise TypeError("unsupported authored logging metadata value")
    return bounded_json_dumps(value, max_bytes=max_bytes, default=default)


def flatten_job_runtime(
    runtime: "JobRuntimeInfo",
    *,
    observed_at: Optional[datetime] = None,
    log_session_id: Optional[str] = None,
    max_record_bytes: int = 1048576,
) -> Dict[str, Any]:
    """Project one complete job-runtime record."""

    _screen_authoritative(runtime.stages, max_record_bytes)
    _screen_authoritative(runtime.operation_types, max_record_bytes)
    entry: Dict[str, Any] = {
        "log_schema_version": LOG_SCHEMA_VERSION,
        "_type": LogType.JOB_RUN_LOG.value,
        "datacoolie_version": __version__,
        "log_session_id": log_session_id,
        "job_id": runtime.job_id,
        "job_num": runtime.job_num,
        "job_index": runtime.job_index,
        "workspace_id": runtime.workspace_id,
        "stages": _metadata_json(runtime.stages, max_record_bytes)
        if isinstance(runtime.stages, list)
        else runtime.stages,
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
        "start_time": _timestamp(runtime.start_time),
        "end_time": _timestamp(runtime.end_time),
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
        "operation_types": _metadata_json(runtime.operation_types, max_record_bytes)
        if isinstance(runtime.operation_types, list)
        else runtime.operation_types,
    }
    if observed_at is not None:
        entry["observed_at"] = _timestamp(observed_at)
    return entry


def build_dataflow_entry(
    dataflow: "DataFlow",
    runtime: "DataFlowRuntimeInfo",
    *,
    job_info: "JobRuntimeInfo",
    log_session_id: Optional[str] = None,
    diagnostic_max_bytes: int = 65536,
    max_record_bytes: int = 1048576,
) -> Dict[str, Any]:
    """Project declarative dataflow metadata plus typed runtime facts."""

    src = dataflow.source
    dst = dataflow.destination
    trn = dataflow.transform
    _screen_authoritative(dataflow, max_record_bytes)
    def _as_json(value: Any) -> str | None:
        return _metadata_json(value, max_record_bytes)
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
            if trn.additional_columns
            else None
        ),
        "transform_schema_hints": _as_json(
            {hint.column_name: hint.data_type for hint in trn.schema_hints}
            if trn.schema_hints
            else None
        ),
        "transform_select_columns": _as_json(trn.select_columns),
        "transform_drop_columns": _as_json(trn.drop_columns),
        "transform_rename_columns": _as_json(trn.rename_columns),
        "transform_value_rules": _as_json(
            [rule.model_dump() for rule in trn.value_rules]
            if trn.value_rules
            else None
        ),
        "transform_hash_columns": _as_json(
            [column.model_dump() for column in trn.hash_columns]
            if trn.hash_columns
            else None
        ),
        "transform_masking_rules": _as_json(
            [rule.model_dump() for rule in trn.masking_rules]
            if trn.masking_rules
            else None
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
    entry.update(flatten_dataflow_runtime(
        runtime, diagnostic_max_bytes=diagnostic_max_bytes,
        max_record_bytes=max_record_bytes,
    ))
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

    record_entry = record.to_dict()
    return {
        "log_schema_version": LOG_SCHEMA_VERSION,
        "_type": LogType.SYSTEM_LOG.value,
        "datacoolie_version": __version__,
        "log_session_id": log_session_id,
        "job_id": job_id,
        "job_num": job_num,
        "job_index": job_index,
        **record_entry,
    }
