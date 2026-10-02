"""Prepare declarative DataFlows for execution."""

from __future__ import annotations

from dataclasses import dataclass
import time
from collections.abc import Sequence
from typing import Callable, Optional

from datacoolie.core.constants import ExecutionType
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.orchestration.preparation.query import resolve_query
from datacoolie.platforms.base import BasePlatform


ConnectionSecretResolver = Callable[[Connection], None]
logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class PreparedDataFlow:
    """Declarative metadata paired with its isolated execution copy."""

    metadata: DataFlow
    execution: DataFlow


def prepare_execution_dataflow(
    dataflow: DataFlow,
    *,
    platform: BasePlatform,
    resolve_connection_secrets: ConnectionSecretResolver,
    sql_base_path: str | Sequence[str] | None = None,
    artifact_base_path: Optional[str] = None,
    operation_type: str = ExecutionType.ETL.value,
) -> PreparedDataFlow:
    """Return a prepared deep copy of *dataflow*.

    Query files and secret references are resolved only on this execution
    copy.  The caller's declarative metadata object remains unchanged for
    metadata logging.
    """

    started = time.perf_counter()
    if operation_type not in {
        ExecutionType.ETL.value,
        ExecutionType.REPLAY.value,
        ExecutionType.MAINTENANCE.value,
    }:
        raise ValueError(f"Unsupported preparation operation: {operation_type!r}")

    metadata: DataFlow = dataflow.model_copy(deep=True)
    execution: DataFlow = metadata.model_copy(deep=True)

    if operation_type != ExecutionType.MAINTENANCE.value and execution.source and execution.source.query:
        execution.source.query = resolve_query(
            execution.source.query,
            platform,
            sql_base_path=sql_base_path,
            artifact_base_path=artifact_base_path,
        )

    if operation_type != ExecutionType.MAINTENANCE.value and execution.source and execution.source.connection:
        resolve_connection_secrets(execution.source.connection)
    if execution.destination and execution.destination.connection:
        resolve_connection_secrets(execution.destination.connection)

    prepared = PreparedDataFlow(metadata=metadata, execution=execution)
    logger.debug(
        "Prepared dataflow %s for %s in %.3fs",
        dataflow.dataflow_id,
        operation_type,
        time.perf_counter() - started,
        extra={"event_name": LogEvent.PREPARATION_FINISHED.value},
    )
    return prepared


def validate_preparation(
    dataflow: DataFlow,
    *,
    platform: BasePlatform,
    sql_base_path: str | Sequence[str] | None = None,
    artifact_base_path: Optional[str] = None,
    operation_type: str = ExecutionType.ETL.value,
) -> None:
    """Validate operation-specific file preparation without hydrating secrets."""
    if operation_type not in {
        ExecutionType.ETL.value,
        ExecutionType.REPLAY.value,
        ExecutionType.MAINTENANCE.value,
    }:
        raise ValueError(f"Unsupported preparation operation: {operation_type!r}")
    if operation_type != ExecutionType.MAINTENANCE.value and dataflow.source and dataflow.source.query:
        resolve_query(
            dataflow.source.query,
            platform,
            sql_base_path=sql_base_path,
            artifact_base_path=artifact_base_path,
        )


__all__ = [
    "ConnectionSecretResolver",
    "PreparedDataFlow",
    "prepare_execution_dataflow",
    "validate_preparation",
]
