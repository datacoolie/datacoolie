"""Exception hierarchy for the DataCoolie framework.

All framework exceptions inherit from :class:`DataCoolieError` which carries
a human-readable ``message`` and an optional ``details`` dict for structured
error context.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo
    from datacoolie.sources.runtime import SourceRuntimeInfo
    from datacoolie.transformers.runtime import TransformRuntimeInfo
    from datacoolie.destinations.runtime import DestinationRuntimeInfo


class DataCoolieError(Exception):
    """Base exception for all DataCoolie errors.

    Attributes:
        message: Human-readable error description.
        details: Optional structured context for logging / debugging.
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


# ---------------------------------------------------------------------------
# Component-specific errors
# ---------------------------------------------------------------------------


class ConfigurationError(DataCoolieError):
    """Raised for invalid or missing configuration."""


class MetadataError(DataCoolieError):
    """Raised for metadata loading / parsing failures."""


class SourceError(DataCoolieError):
    """Raised when reading from a data source fails."""

    def __init__(self, message: str, details: dict[str, Any] | None = None, *, runtime_info: SourceRuntimeInfo | None = None) -> None:
        super().__init__(message, details)
        self.runtime_info = runtime_info


class TransformError(DataCoolieError):
    """Raised when a transformation step fails."""

    def __init__(self, message: str, details: dict[str, Any] | None = None, *, runtime_info: TransformRuntimeInfo | None = None) -> None:
        super().__init__(message, details)
        self.runtime_info = runtime_info


class DestinationError(DataCoolieError):
    """Raised when writing to a destination fails."""

    def __init__(self, message: str, details: dict[str, Any] | None = None, *, runtime_info: DestinationRuntimeInfo | None = None) -> None:
        super().__init__(message, details)
        self.runtime_info = runtime_info


class WatermarkError(DataCoolieError):
    """Raised for watermark read / write / parse failures."""


class EngineError(DataCoolieError):
    """Raised for engine-level failures (Spark, Polars)."""


class PlatformError(DataCoolieError):
    """Raised for platform-specific failures (Fabric, Databricks, AWS)."""


class PipelineError(DataCoolieError):
    """Raised when a pipeline step fails, carrying partial runtime results.

    Attributes:
        attempt: The attempt observation collected before the failure.
    """

    def __init__(
        self,
        message: str,
        attempt: AttemptRuntimeInfo | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.attempt = attempt


class DataFlowError(DataCoolieError):
    """Raised for dataflow-level execution errors.

    Attributes:
        dataflow_id: Identifier of the failed dataflow.
        stage: Pipeline stage where the error occurred.
    """

    def __init__(
        self,
        message: str,
        dataflow_id: str | None = None,
        stage: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.dataflow_id = dataflow_id
        self.stage = stage
        super().__init__(message, details)

    def __str__(self) -> str:
        parts = [self.message]
        if self.dataflow_id:
            parts.append(f"DataFlow: {self.dataflow_id}")
        if self.stage:
            parts.append(f"Stage: {self.stage}")
        if self.details:
            parts.append(f"Details: {self.details}")
        return " | ".join(parts)

