"""Abstract base classes for destination writers and load strategies.

``BaseDestinationWriter[DF]`` uses the **Template Method** pattern for
writes and the **Strategy** pattern for load-type dispatch.

``BaseLoadStrategy`` defines the contract for individual load strategies
(overwrite, append, merge_upsert, merge_overwrite, scd2).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional

from datacoolie.core.constants import DEFAULT_RETENTION_HOURS, DataFlowStatus, ExecutionType
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import DataFlow, DestinationRuntimeInfo
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.utils.helpers import utc_now

logger = get_logger(__name__)


# ============================================================================
# Load Strategy ABC
# ============================================================================


class BaseLoadStrategy(ABC):
    """Abstract load strategy (Strategy pattern).

    Each strategy knows how to write a DataFrame to a destination path
    using a specific load type (overwrite, append, merge, etc.).
    """

    @property
    @abstractmethod
    def load_type(self) -> str:
        """The load type this strategy handles (e.g. ``"overwrite"``)."""

    @abstractmethod
    def execute(
        self,
        df: DF,
        table_name: str,
        dataflow: DataFlow,
        engine: BaseEngine[DF],
        path: Optional[str] = None,
    ) -> None:
        """Execute the load strategy.

        Args:
            df: DataFrame to write.
            table_name: Fully qualified table name (always available).
            dataflow: Pipeline configuration.
            engine: DataFrame engine.
            path: Optional destination path for path-based fallback.
        """


# ============================================================================
# Destination Writer ABC
# ============================================================================


class BaseDestinationWriter(ABC, Generic[DF]):
    """Abstract destination writer with Template Method lifecycle.

    Subclasses implement :meth:`_write_internal`.  The base class
    manages timing, error wrapping, runtime info, and maintenance.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine
        self._runtime_info = DestinationRuntimeInfo()

    # ------------------------------------------------------------------
    # Write (Template Method)
    # ------------------------------------------------------------------

    def write(self, df: DF, dataflow: DataFlow) -> DestinationRuntimeInfo:
        """Write data to the destination (Template Method).

        1. Validate destination path.
        2. Delegate to :meth:`_write_internal`.
        3. Populate and return runtime info.

        Args:
            df: DataFrame to write.
            dataflow: Full pipeline configuration.

        Returns:
            Runtime metrics for the write operation.

        Raises:
            DestinationError: On any write failure.
        """
        self._runtime_info = DestinationRuntimeInfo(
            start_time=utc_now(),
            status=DataFlowStatus.RUNNING.value,
            operation_type=dataflow.load_type,
        )

        dest_path = dataflow.destination.path
        table_name = dataflow.destination.full_table_name
        
        try:
            self._write_internal(df, dataflow)

            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.SUCCEEDED.value

            # Fetch history once — enrich metrics + store in operation_details
            try:
                history = self._get_history(
                    dataflow, limit=2,
                    start_time=self._runtime_info.start_time,
                    end_time=self._runtime_info.end_time,
                )
                
                if history:
                    self._runtime_info.operation_details = history
                    engine_metrics = self._parse_write_metrics(history)
                    self._runtime_info.rows_written = engine_metrics.get("rows_written", 0)
                    self._runtime_info.rows_inserted = engine_metrics.get("rows_inserted", 0)
                    self._runtime_info.rows_updated = engine_metrics.get("rows_updated", 0)
                    self._runtime_info.rows_deleted = engine_metrics.get("rows_deleted", 0)
                    self._runtime_info.files_added = engine_metrics.get("files_added", 0)
                    self._runtime_info.files_removed = engine_metrics.get("files_removed", 0)
                    self._runtime_info.bytes_added = engine_metrics.get("bytes_added", 0)
                    self._runtime_info.bytes_removed = engine_metrics.get("bytes_removed", 0)
            except Exception:
                logger.debug("Could not retrieve table history for operation_details")

            return self._runtime_info

        except DestinationError:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            raise
        except Exception as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.error_message = str(exc)
            raise DestinationError(
                f"Failed to write destination: {exc}",
                details={"table_name": table_name, "path": dest_path, "load_type": dataflow.load_type},
            ) from exc

    def get_runtime_info(self) -> DestinationRuntimeInfo:
        """Return runtime information from the most recent write."""
        return self._runtime_info

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def run_maintenance(
        self,
        dataflow: DataFlow,
        *,
        do_compact: bool = True,
        do_cleanup: bool = True,
        retention_hours: Optional[int] = None,
    ) -> DestinationRuntimeInfo:
        """Run maintenance operations (compact + cleanup) as a single result.

        Sub-operation outcomes are stored in ``operation_details`` as a list
        of dicts, each with an ``"operation"`` key (``"compact"`` / ``"cleanup"``
        / ``"history"``).  File/byte metrics are summed across both operations.

        Args:
            dataflow: Pipeline configuration.
            do_compact: Whether to run compaction.
            do_cleanup: Whether to run cleanup.
            retention_hours: Cleanup retention override (default: 168 h).

        Returns:
            A single :class:`DestinationRuntimeInfo` covering the full run.
        """
        info = DestinationRuntimeInfo(
            start_time=utc_now(),
            status=DataFlowStatus.RUNNING.value,
            operation_type=ExecutionType.MAINTENANCE.value,
        )

        dest_path = dataflow.destination.path
        dest_table_name = dataflow.destination.full_table_name
        dest_fmt = dataflow.destination.connection.format
        
        if not dest_path and not dest_table_name:
            info.end_time = utc_now()
            info.status = DataFlowStatus.FAILED.value
            info.error_message = "Destination path or table name is required"
            return info

        hours = retention_hours if retention_hours is not None else DEFAULT_RETENTION_HOURS

        sub_results: List[Dict[str, Any]] = []
        errors: List[str] = []
        try:
            sub_results, errors = self._maintain_internal(
                dataflow,
                do_compact=do_compact,
                do_cleanup=do_cleanup,
                retention_hours=hours,
            )
        except Exception as exc:
            errors.append(str(exc))
            logger.error("Maintenance failed for %s: %s", dest_table_name or dest_path, exc)
        any_succeeded = any(
            entry.get("status") == DataFlowStatus.SUCCEEDED.value for entry in sub_results
        )

        # Fetch history once — enrich sub-results + store raw history
        try:
            history = self._get_history(
                dataflow, limit=5,
                start_time=info.start_time, end_time=info.end_time,
            )
            maint_metrics = self._parse_maintenance_metrics(history)
            for op_key, metrics in maint_metrics.items():
                info.files_added += metrics.get("files_added", 0)
                info.files_removed += metrics.get("files_removed", 0)
                info.bytes_added += metrics.get("bytes_added", 0)
                info.bytes_removed += metrics.get("bytes_removed", 0)
                for entry in sub_results:
                    if entry.get("operation") == op_key:
                        entry["engine_metrics"] = metrics
                        break
            if history:
                sub_results.append({"operation": "history", "data": history})
        except Exception as exc:
            logger.warning("Failed to get maintenance metrics: %s", exc)

        info.operation_details = sub_results

        if errors:
            info.status = DataFlowStatus.FAILED.value
            info.error_message = "; ".join(errors)
        elif any_succeeded:
            info.status = DataFlowStatus.SUCCEEDED.value
        else:
            info.status = DataFlowStatus.SKIPPED.value

        info.end_time = utc_now()
        return info

    def _run_op(
        self,
        *,
        op_name: str,
        table_exists: bool,
        fn,
        errors: List[str],
        location: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute a maintenance sub-operation with timing and error capture.

        Helper for :meth:`_maintain_internal` implementations. Invokes *fn*
        when *table_exists* is True, otherwise records a SKIPPED result.
        Appends to *errors* on failure.

        Args:
            op_name: Operation label (e.g. ``"compact"``, ``"cleanup"``).
            table_exists: Whether the destination table currently exists.
            fn: Zero-argument callable performing the engine operation.
            errors: Shared error-accumulator populated on failure.
            location: Human-readable location for log messages.

        Returns:
            Sub-result dict with ``operation``, ``status``, ``duration_seconds``
            and ``error_message`` keys.
        """
        start = utc_now()
        status = DataFlowStatus.SKIPPED.value
        error_message: Optional[str] = None

        try:
            if table_exists:
                fn()
                status = DataFlowStatus.SUCCEEDED.value
            else:
                logger.info("%s skipped — table does not exist at %s", op_name.capitalize(), location)
        except Exception as exc:
            status = DataFlowStatus.FAILED.value
            error_message = str(exc)
            errors.append(f"{op_name.capitalize()}: {error_message}")
            logger.error("%s failed for %s: %s", op_name.capitalize(), location, exc)

        end = utc_now()
        return {
            "operation": op_name,
            "status": status,
            "duration_seconds": (end - start).total_seconds(),
            "error_message": error_message,
        }

    # ------------------------------------------------------------------
    # Abstract / overridable methods
    # ------------------------------------------------------------------

    @abstractmethod
    def _write_internal(self, df: DF, dataflow: DataFlow) -> None:
        """Execute the write operation (subclass implementation)."""

    @abstractmethod
    def _maintain_internal(
        self,
        dataflow: DataFlow,
        *,
        do_compact: bool,
        do_cleanup: bool,
        retention_hours: int,
    ) -> "tuple[List[Dict[str, Any]], List[str]]":
        """Execute maintenance operations (subclass implementation).

        Subclasses own the engine interaction: resolving the canonical
        table handle (path vs. name), invoking ``exists``/``compact``/
        ``cleanup`` on the engine, and producing structured sub-results.
        Use :meth:`_run_op` to build uniform sub-result dicts.

        Args:
            dataflow: Pipeline configuration.
            do_compact: Whether to run compaction.
            do_cleanup: Whether to run cleanup.
            retention_hours: Cleanup retention window in hours.

        Returns:
            Tuple of ``(sub_results, errors)`` where ``sub_results`` is a
            list of per-operation dicts (compact, cleanup) and ``errors``
            is a list of error messages to surface in the final status.
        """

    def _get_history(
        self,
        dataflow: DataFlow,
        *,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch table history for metrics enrichment.

        Default: passes both ``table_name`` and ``path`` to the engine,
        which prefers ``table_name``.  Subclasses may override to change
        lookup priority (e.g. Delta on AWS always uses path).
        """
        dest = dataflow.destination
        return self._engine.get_history(
            table_name=dest.full_table_name,
            path=dest.path,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            fmt=dest.connection.format,
        )

    def _parse_write_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Parse write metrics from raw history entries.

        Override in format-specific writers (e.g. :class:`DeltaWriter`,
        :class:`IcebergWriter`) to extract row/file/byte counts.
        """
        return {
            "rows_written": 0, "rows_inserted": 0, "rows_updated": 0,
            "rows_deleted": 0, "files_added": 0, "files_removed": 0,
            "bytes_added": 0, "bytes_removed": 0,
        }

    def _parse_maintenance_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, int]]:
        """Parse maintenance metrics from raw history entries.

        Override in format-specific writers to classify history entries
        into ``"compact"`` / ``"cleanup"`` metric buckets.
        """
        return {}
