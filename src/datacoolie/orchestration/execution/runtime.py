"""Run and attempt observations, independent of pipeline control state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models.metrics import ExecutionMetrics
from datacoolie.core.models.runtime import RuntimeInfo
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.sources.contracts import SourceReadRequest
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.watermark.runtime import WatermarkRuntimeInfo
from datacoolie.utils.identity import generate_unique_id


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


__all__ = [
    "ExecutionMetrics",
    "AttemptRuntimeInfo",
    "DataFlowRuntimeInfo",
    "ReplayRuntimeInfo",
]
