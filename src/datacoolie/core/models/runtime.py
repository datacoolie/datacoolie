"""Runtime metrics and execution result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETENTION_HOURS,
    DataFlowStatus,
)
from datacoolie.utils.identity import generate_unique_id
from datacoolie.utils.time import utc_now


@dataclass
class RuntimeInfo:
    """Base timing / status model for execution tracking."""

    start_time: datetime = field(default_factory=utc_now)
    end_time: Optional[datetime] = None
    status: str = DataFlowStatus.PENDING.value
    message: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class SourceRuntimeInfo(RuntimeInfo):
    """Runtime metrics for source reading."""

    rows_read: int = 0
    source_action: Dict[str, Any] = field(default_factory=dict)
    watermark_before: Optional[Dict[str, Any]] = None
    watermark_after: Optional[Dict[str, Any]] = None
    watermark_effective: Optional[Dict[str, Any]] = None
    watermark_start_operator: str = ">"
    watermark_end_operator: str = "<"
    watermark_kind: Optional[str] = None


@dataclass
class TransformRuntimeInfo(RuntimeInfo):
    """Runtime metrics for transformation."""

    transformers_applied: List[str] = field(default_factory=list)


@dataclass
class DestinationRuntimeInfo(RuntimeInfo):
    """Runtime metrics for destination writing or maintenance."""

    operation_type: Optional[str] = (
        None  # e.g. "merge", "overwrite", "append", "maintenance", etc.
    )
    rows_written: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    files_added: int = 0
    files_removed: int = 0
    bytes_added: int = 0
    bytes_removed: int = 0
    operation_details: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def bytes_saved(self) -> int:
        return max(0, self.bytes_removed - self.bytes_added)


@dataclass
class PipelineAttemptResult:
    """Terminal phase results from one retryable pipeline attempt."""

    status: str
    source: Optional[SourceRuntimeInfo] = None
    transform: Optional[TransformRuntimeInfo] = None
    destination: Optional[DestinationRuntimeInfo] = None
    message: Optional[str] = None


@dataclass
class DataFlowRuntimeInfo(RuntimeInfo):
    """Mutable orchestration record for one dataflow execution."""

    dataflow_run_id: str = field(default_factory=generate_unique_id)
    dataflow_id: Optional[str] = None
    operation_type: Optional[str] = None  # e.g. "etl", "maintenance"
    source: SourceRuntimeInfo = field(default_factory=SourceRuntimeInfo)
    transform: TransformRuntimeInfo = field(default_factory=TransformRuntimeInfo)
    destination: DestinationRuntimeInfo = field(default_factory=DestinationRuntimeInfo)
    retry_attempts: int = 0

    @property
    def rows_read(self) -> int:
        return self.source.rows_read

    @property
    def rows_written(self) -> int:
        return self.destination.rows_written

    @property
    def rows_inserted(self) -> int:
        return self.destination.rows_inserted

    @property
    def rows_updated(self) -> int:
        return self.destination.rows_updated

    @property
    def rows_deleted(self) -> int:
        return self.destination.rows_deleted

    @property
    def is_success(self) -> bool:
        return self.status == DataFlowStatus.SUCCEEDED.value

    @property
    def is_failed(self) -> bool:
        return self.status == DataFlowStatus.FAILED.value


@dataclass
class JobRuntimeInfo(RuntimeInfo):
    """Aggregated metrics for an entire DataCoolie job run."""

    job_id: str = field(default_factory=generate_unique_id)
    job_num: int = 1
    job_index: int = 0
    workspace_id: Optional[str] = None
    stages: Optional[str | List[str]] = None  # single stage or list of stages

    # Component names (set by driver from type(obj).__name__)
    engine_name: Optional[str] = None
    platform_name: Optional[str] = None
    metadata_provider_name: Optional[str] = None
    watermark_manager_name: Optional[str] = None

    # RunConfig attributes
    max_workers: int = DEFAULT_MAX_WORKERS
    stop_on_error: bool = False
    retry_count: int = DEFAULT_RETRY_COUNT
    retry_delay: float = DEFAULT_RETRY_DELAY
    dry_run: bool = False
    retention_hours: int = DEFAULT_RETENTION_HOURS
    run_attributes: Optional[str] = None

    # Execution logging persistence health; these describe log records that could
    # not be retained, not business records dropped by the job.
    log_records_dropped: int = 0
    log_bytes_dropped: int = 0
    message_truncated: bool = False

    total_dataflows: int = 0
    total_succeeded: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    # ExecutionLogger observes terminal results only; scheduler live counts are not
    # known at job-summary time unless a caller supplies them explicitly.
    total_running: Optional[int] = None
    total_pending: Optional[int] = None

    total_rows_read: int = 0
    total_rows_written: int = 0
    total_rows_inserted: int = 0
    total_rows_updated: int = 0
    total_rows_deleted: int = 0

    total_files_added: int = 0
    total_files_removed: int = 0

    total_bytes_added: int = 0
    total_bytes_removed: int = 0

    operation_types: Optional[str | List[str]] = None
