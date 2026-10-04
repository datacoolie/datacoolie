"""Run and attempt observations, independent of pipeline control state."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Iterable, Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models.runtime import RuntimeInfo
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.sources.contracts import SourceReadRequest
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.watermark.runtime import WatermarkRuntimeInfo
from datacoolie.utils.identity import generate_unique_id


@dataclass
class ExecutionMetrics:
    rows_read: Optional[int] = 0
    rows_written: Optional[int] = 0
    rows_inserted: Optional[int] = 0
    rows_updated: Optional[int] = 0
    rows_deleted: Optional[int] = 0
    files_added: Optional[int] = 0
    files_removed: Optional[int] = 0
    bytes_added: Optional[int] = 0
    bytes_removed: Optional[int] = 0
    bytes_saved: Optional[int] = 0

    @classmethod
    def from_attempt(cls, attempt: Optional[AttemptRuntimeInfo]) -> ExecutionMetrics:
        if attempt is None:
            return cls()
        destination = attempt.destination
        return cls(
            rows_read=attempt.source.rows_read if attempt.source is not None else 0,
            rows_written=destination.rows_written if destination is not None else 0,
            rows_inserted=destination.rows_inserted if destination is not None else 0,
            rows_updated=destination.rows_updated if destination is not None else 0,
            rows_deleted=destination.rows_deleted if destination is not None else 0,
            files_added=destination.files_added if destination is not None else 0,
            files_removed=destination.files_removed if destination is not None else 0,
            bytes_added=destination.bytes_added if destination is not None else 0,
            bytes_removed=destination.bytes_removed if destination is not None else 0,
            bytes_saved=destination.bytes_saved if destination is not None else 0,
        )

    @classmethod
    def sum(cls, metrics: Iterable[ExecutionMetrics]) -> ExecutionMetrics:
        values = list(metrics)
        totals = {}
        for item in fields(cls):
            numbers = [getattr(value, item.name) for value in values]
            totals[item.name] = None if any(n is None for n in numbers) else sum(numbers)
        return cls(**totals)


@dataclass
class AttemptRuntimeInfo(RuntimeInfo):
    attempt_number: int = 1
    source: Optional[SourceRuntimeInfo] = None
    transform: Optional[TransformRuntimeInfo] = None
    destination: Optional[DestinationRuntimeInfo] = None
    watermark: Optional[WatermarkRuntimeInfo] = None


@dataclass
class DataFlowRuntimeInfo(RuntimeInfo):
    dataflow_run_id: str = field(default_factory=generate_unique_id)
    dataflow_id: Optional[str] = None
    operation_type: Optional[str] = None
    read_request: Optional[SourceReadRequest] = None
    attempts: list[AttemptRuntimeInfo] = field(default_factory=list)

    @property
    def final_attempt(self) -> Optional[AttemptRuntimeInfo]:
        return self.attempts[-1] if self.attempts else None

    @property
    def retry_attempts(self) -> int:
        return max(0, len(self.attempts) - 1)

    @property
    def metrics(self) -> ExecutionMetrics:
        return ExecutionMetrics.from_attempt(self.final_attempt)

    @property
    def is_success(self) -> bool:
        return self.status == DataFlowStatus.SUCCEEDED.value

    @property
    def is_failed(self) -> bool:
        return self.status == DataFlowStatus.FAILED.value


@dataclass
class ReplayRuntimeInfo(RuntimeInfo):
    dataflow_run_id: str = field(default_factory=generate_unique_id)
    dataflow_id: Optional[str] = None
    operation_type: Optional[str] = None
    chunk_run_ids: list[str] = field(default_factory=list)
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    retry_attempts: int = 0
