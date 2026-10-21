"""Job-level observations and nullable ingestion totals."""

from dataclasses import dataclass, field
from typing import Optional

from datacoolie.core.constants import (
    DEFAULT_MAX_WORKERS, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DELAY,
    DEFAULT_RETENTION_HOURS,
)
from datacoolie.core.models.runtime import RuntimeInfo
from datacoolie.utils.identity import generate_unique_id


@dataclass
class JobRuntimeInfo(RuntimeInfo):
    job_id: str = field(default_factory=generate_unique_id)
    job_num: int = 1
    job_index: int = 0
    workspace_id: Optional[str] = None
    stages: Optional[str | list[str]] = None
    engine_name: Optional[str] = None
    platform_name: Optional[str] = None
    metadata_provider_name: Optional[str] = None
    watermark_manager_name: Optional[str] = None
    max_workers: int = DEFAULT_MAX_WORKERS
    stop_on_error: bool = False
    retry_count: int = DEFAULT_RETRY_COUNT
    retry_delay: float = DEFAULT_RETRY_DELAY
    dry_run: bool = False
    retention_hours: int = DEFAULT_RETENTION_HOURS
    run_attributes: Optional[str] = None
    log_records_dropped: int = 0
    log_bytes_dropped: int = 0
    message_truncated: bool = False
    total_dataflows: int = 0
    total_succeeded: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    total_running: Optional[int] = None
    total_pending: Optional[int] = None
    total_rows_read: Optional[int] = None
    total_rows_written: Optional[int] = None
    total_rows_inserted: Optional[int] = None
    total_rows_updated: Optional[int] = None
    total_rows_deleted: Optional[int] = None
    total_files_added: Optional[int] = None
    total_files_removed: Optional[int] = None
    total_bytes_added: Optional[int] = None
    total_bytes_removed: Optional[int] = None
    operation_types: Optional[str | list[str]] = None
