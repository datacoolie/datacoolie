"""Abstract base classes for destination writers and load strategies.

``BaseDestinationWriter[DF]`` uses the **Template Method** pattern for
writes and the **Strategy** pattern for load-type dispatch.

``BaseLoadStrategy`` defines the contract for individual load strategies
(overwrite, append, merge_upsert, merge_overwrite, scd2).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
from math import isfinite
from numbers import Integral, Real
from typing import Any, Dict, Generic, List, Optional

from datacoolie.core.constants import (
    DEFAULT_RETENTION_HOURS,
    DataFlowStatus,
    ExecutionType,
    LoadType,
    MaintenanceType,
)
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.destinations.resolution.target import ResolvedDestination, resolve_destination_target
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.engines.contracts.windows import WindowSpec, normalize_window
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.utils.time import utc_now

logger = get_logger(__name__)


_DESTINATION_METRIC_FIELDS = (
    "rows_written",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "files_added",
    "files_removed",
    "bytes_added",
    "bytes_removed",
)
_MetricValues = Dict[str, Optional[int]]


def _empty_metric_values() -> _MetricValues:
    """Return a nullable metric map with no fabricated measurements."""

    return {name: None for name in _DESTINATION_METRIC_FIELDS}


def _coerce_metric_value(value: Any) -> Optional[int]:
    """Accept non-negative integral counts without lossy coercion."""

    if isinstance(value, bool):
        return None
    if isinstance(value, Integral):
        result = int(value)
    elif isinstance(value, Real):
        numeric = float(value)
        if not isfinite(numeric) or not numeric.is_integer():
            return None
        result = int(numeric)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            result = int(text, 10)
        except ValueError:
            return None
    else:
        return None
    return result if result >= 0 else None


def _parse_history_metric_fields(
    history: List[Dict[str, Any]],
    *,
    container_key: str,
    aliases: Dict[str, tuple[str, ...]],
) -> _MetricValues:
    """Parse sparse history without turning missing keys into zero.

    A metric is known only when every relevant history entry supplies a valid
    value for that metric.  This keeps an explicit zero measurable while
    preserving uncertainty from an absent or sparse entry.
    """

    result = _empty_metric_values()
    if not history:
        return result

    totals = {name: 0 for name in aliases}
    seen = {name: False for name in aliases}
    unknown = {name: False for name in aliases}

    for entry in history:
        container = entry.get(container_key) if isinstance(entry, Mapping) else None
        if not isinstance(container, Mapping):
            container = {}
        for name, keys in aliases.items():
            found = False
            value: Optional[int] = None
            for key in keys:
                if key not in container:
                    continue
                found = True
                raw_value = container[key]
                if raw_value is not None:
                    value = _coerce_metric_value(raw_value)
                break
            if not found or value is None:
                unknown[name] = True
            else:
                seen[name] = True
                totals[name] += value

    for name in aliases:
        result[name] = None if unknown[name] or not seen[name] else totals[name]
    return result


def _normalise_metric_values(values: Mapping[str, Any] | None) -> _MetricValues:
    """Project a parser result onto the destination metric contract."""

    result = _empty_metric_values()
    if not values:
        return result
    for name in _DESTINATION_METRIC_FIELDS:
        value = values.get(name)
        if value is not None:
            result[name] = _coerce_metric_value(value)
    return result


def _apply_known_zero_metrics(metrics: _MetricValues, names: set[str]) -> None:
    """Fill only metrics proven unaffected by the operation semantics."""

    for name in names:
        if metrics[name] is None:
            metrics[name] = 0


def _known_write_zero_metrics(load_type: str) -> set[str]:
    """Return write metrics that the selected operation cannot affect."""

    if load_type == LoadType.APPEND.value:
        return {
            "rows_updated",
            "rows_deleted",
            "files_removed",
            "bytes_removed",
        }
    if load_type == LoadType.MERGE_UPSERT.value:
        return {"rows_deleted"}
    return set()


def _known_maintenance_zero_metrics(operation: str) -> set[str]:
    """Return metrics unaffected by a successful physical maintenance op."""

    if operation not in {"compact", "cleanup"}:
        return set()
    names = {
        "rows_written",
        "rows_inserted",
        "rows_updated",
        "rows_deleted",
    }
    if operation == "cleanup":
        names.update({"files_added", "bytes_added"})
    return names


def _aggregate_maintenance_metrics(
    parsed: Mapping[str, Mapping[str, Any]],
    sub_results: List[Dict[str, Any]],
    *,
    requested_operations: set[str],
    operation_errors: bool = False,
) -> _MetricValues:
    """Aggregate known maintenance operations with nullable propagation."""

    operation_status = {
        entry.get("operation"): entry.get("status")
        for entry in sub_results
        if isinstance(entry, Mapping)
        and entry.get("operation") in {"compact", "cleanup"}
    }
    per_operation: Dict[str, _MetricValues] = {}
    for operation, status in operation_status.items():
        if status == DataFlowStatus.SKIPPED.value:
            continue
        if status == DataFlowStatus.SUCCEEDED.value and operation in parsed:
            metrics = _normalise_metric_values(parsed[operation])
        else:
            # A failed or malformed entered operation has no trustworthy
            # affected-dimension measurement.  Keep only the dimensions its
            # operation semantics prove cannot change.
            metrics = _empty_metric_values()
        _apply_known_zero_metrics(metrics, _known_maintenance_zero_metrics(operation))
        per_operation[operation] = metrics

    if not per_operation:
        skipped_operations = {
            operation
            for operation, status in operation_status.items()
            if status == DataFlowStatus.SKIPPED.value
        }
        all_requested_not_performed = requested_operations.issubset(skipped_operations)
        if operation_errors or (
            requested_operations and not all_requested_not_performed
        ):
            return _empty_metric_values()
        return {name: 0 for name in _DESTINATION_METRIC_FIELDS}

    result = _empty_metric_values()
    for name in _DESTINATION_METRIC_FIELDS:
        values = [metrics[name] for metrics in per_operation.values()]
        result[name] = None if any(value is None for value in values) else sum(values)
    return result


def _safe_debug(message: str, *args: Any) -> None:
    """Keep optional diagnostic logging from changing the business result."""

    try:
        logger.debug(message, *args)
    except Exception:  # noqa: BLE001
        pass


def _safe_warning(message: str, *args: Any) -> None:
    """Keep optional maintenance diagnostics from changing the outcome."""

    try:
        logger.warning(message, *args)
    except Exception:  # noqa: BLE001
        pass


def _safe_info(message: str, *args: Any) -> None:
    """Keep informational maintenance diagnostics from changing the outcome."""

    try:
        logger.info(message, *args)
    except Exception:  # noqa: BLE001
        pass


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
        table_name: Optional[str],
        dataflow: DataFlow,
        engine: BaseEngine[DF],
        path: Optional[str] = None,
        *,
        watermark_window: Optional[WindowSpec] = None,
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

    def write(
        self,
        df: DF,
        dataflow: DataFlow,
        *,
        watermark_window: Optional[WindowSpec] = None,
    ) -> DestinationRuntimeInfo:
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

        dest_path: Optional[str] = None
        table_name = ""
        
        try:
            if watermark_window is not None:
                # Reject legacy mappings at the writer boundary instead of
                # allowing bootstrap/fallback logic to treat them as a
                # usable replacement window.
                normalize_window(watermark_window)
            # Resolve the destination inside the phase boundary.  A malformed
            # destination is a failed destination phase, not a leaked RUNNING
            # runtime caused by property access before ``try``.
            target = self._resolve_target(dataflow)
            dest_path = target.path
            table_name = target.table_name or ""
            if watermark_window is None:
                # Keep ordinary writer subclasses free of execution state;
                # only replacement writes need the explicit per-call value.
                self._write_internal(df, dataflow)
            else:
                self._write_internal(
                    df,
                    dataflow,
                    watermark_window=watermark_window,
                )

        except DestinationError as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.message = str(exc) or type(exc).__name__
            attached_runtime = getattr(exc, "runtime_info", None)
            if attached_runtime is None or attached_runtime is self._runtime_info:
                exc.runtime_info = self._detached_runtime_info()
            _safe_debug("Destination write failed: %s", exc)
            raise
        except Exception as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.message = str(exc) or type(exc).__name__
            _safe_debug("Destination write failed: %s", exc)
            raise DestinationError(
                f"Failed to write destination: {exc}",
                details={"table_name": table_name, "path": dest_path, "load_type": self._runtime_info.operation_type},
                runtime_info=self._detached_runtime_info(),
            ) from exc

        # The physical write has acknowledged success.  History lookup,
        # parsing, logging, and operation-zero enrichment are diagnostics and
        # must never turn that acknowledged result into a retryable failure.
        self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
        try:
            self._runtime_info.end_time = utc_now()
        except Exception as exc:  # noqa: BLE001
            _safe_debug("Destination end-time capture failed: %s", exc)
        try:
            self._enrich_write_runtime(dataflow)
        except Exception as exc:  # noqa: BLE001
            _safe_debug("Destination metric enrichment failed: %s", exc)
        return self._runtime_info

    def get_runtime_info(self) -> DestinationRuntimeInfo:
        """Return runtime information from the most recent write."""
        return self._runtime_info

    def _detached_runtime_info(self) -> DestinationRuntimeInfo:
        """Return a detached partial snapshot for a component error."""

        try:
            return deepcopy(self._runtime_info)
        except Exception:  # noqa: BLE001
            snapshot = DestinationRuntimeInfo(
                start_time=self._runtime_info.start_time,
                end_time=self._runtime_info.end_time,
                status=self._runtime_info.status,
                message=self._runtime_info.message,
                operation_type=self._runtime_info.operation_type,
            )
            for name in _DESTINATION_METRIC_FIELDS:
                try:
                    setattr(snapshot, name, getattr(self._runtime_info, name))
                except Exception:  # noqa: BLE001
                    pass
            return snapshot

    def _enrich_write_runtime(self, dataflow: DataFlow) -> None:
        """Best-effort history/metric enrichment after write acknowledgement."""

        history: List[Dict[str, Any]] = []
        try:
            candidate = self._get_history(
                dataflow,
                limit=2,
                start_time=self._runtime_info.start_time,
                end_time=self._runtime_info.end_time,
            )
            if isinstance(candidate, list):
                history = candidate
        except Exception as exc:  # noqa: BLE001
            _safe_debug("Could not retrieve table history for operation_details: %s", exc)

        if history:
            try:
                self._runtime_info.operation_details = deepcopy(history)
            except Exception as exc:  # noqa: BLE001
                _safe_debug("Could not retain destination history: %s", exc)

        metrics = _empty_metric_values()
        try:
            metrics = _normalise_metric_values(self._parse_write_metrics(history))
        except Exception as exc:  # noqa: BLE001
            _safe_debug("Could not parse destination history metrics: %s", exc)

        try:
            _apply_known_zero_metrics(
                metrics,
                _known_write_zero_metrics(dataflow.load_type),
            )
        except Exception as exc:  # noqa: BLE001
            _safe_debug("Could not derive operation-proven destination zeros: %s", exc)

        try:
            for name in _DESTINATION_METRIC_FIELDS:
                setattr(self._runtime_info, name, metrics[name])
        except Exception as exc:  # noqa: BLE001
            _safe_debug("Could not attach destination metrics: %s", exc)

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
        self._runtime_info = DestinationRuntimeInfo(
            start_time=utc_now(),
            status=DataFlowStatus.RUNNING.value,
            operation_type=ExecutionType.MAINTENANCE.value,
        )

        dest_path: Optional[str] = None
        dest_table_name = ""
        dest_fmt = ""

        # Keep destination preflight in the maintenance phase boundary.  This
        # method returns a runtime object for maintenance, so convert even a
        # property/configuration failure into a terminal failed runtime.
        try:
            target = self._resolve_target(dataflow)
            dest_path = target.path
            dest_table_name = target.table_name or ""
            dest_fmt = target.format
        except Exception as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.message = str(exc) or type(exc).__name__
            _safe_debug("Maintenance preflight failed: %s", exc)
            return self._runtime_info

        if not dest_path and not dest_table_name:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.message = "Destination path or table name is required"
            return self._runtime_info

        hours = retention_hours if retention_hours is not None else DEFAULT_RETENTION_HOURS

        _safe_debug(
            "Maintenance start — table=%s, path=%s, format=%s, do_compact=%s, "
            "do_cleanup=%s, retention_hours=%d",
            dest_table_name, dest_path, dest_fmt, do_compact, do_cleanup, hours,
        )

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
            errors.append(str(exc) or type(exc).__name__)
            # The orchestration boundary owns the terminal traceback.  Keep
            # this handled partial-maintenance detail at DEBUG so a single
            # failure is not emitted once per provider/wrapper layer.
            _safe_debug("Maintenance failed for %s: %s", dest_table_name or dest_path, exc)
        any_succeeded = any(
            entry.get("status") == DataFlowStatus.SUCCEEDED.value for entry in sub_results
        )
        if sub_results:
            _safe_debug(
                "Maintenance sub-operations — %s",
                ", ".join(
                    f"{e.get('operation')}={e.get('status')}"
                    f"({e.get('duration_seconds', 0):.2f}s)"
                    for e in sub_results
                ),
            )

        try:
            self._enrich_maintenance_runtime(
                dataflow,
                sub_results,
                requested_operations={
                    operation
                    for operation, enabled in (
                        (MaintenanceType.COMPACT.value, do_compact),
                        (MaintenanceType.CLEANUP.value, do_cleanup),
                    )
                    if enabled
                },
                operation_errors=bool(errors),
            )
        except Exception as exc:  # noqa: BLE001
            _safe_warning("Maintenance metric enrichment failed: %s", exc)
        self._runtime_info.operation_details = sub_results

        if errors:
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.message = "; ".join(errors)
        elif any_succeeded:
            self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
        else:
            self._runtime_info.status = DataFlowStatus.SKIPPED.value

        self._runtime_info.end_time = utc_now()
        return self._runtime_info

    def _enrich_maintenance_runtime(
        self,
        dataflow: DataFlow,
        sub_results: List[Dict[str, Any]],
        *,
        requested_operations: set[str],
        operation_errors: bool,
    ) -> None:
        """Best-effort maintenance history enrichment after operations run."""

        history: List[Dict[str, Any]] = []
        try:
            candidate = self._get_history(
                dataflow,
                limit=5,
                start_time=self._runtime_info.start_time,
                end_time=self._runtime_info.end_time,
            )
            if isinstance(candidate, list):
                history = candidate
        except Exception as exc:  # noqa: BLE001
            _safe_warning("Failed to get maintenance history: %s", exc)

        if history:
            try:
                sub_results.append({"operation": "history", "data": deepcopy(history)})
            except Exception as exc:  # noqa: BLE001
                _safe_debug("Could not retain maintenance history: %s", exc)

        parsed: Dict[str, Mapping[str, Any]] = {}
        try:
            candidate = self._parse_maintenance_metrics(history)
            if isinstance(candidate, Mapping):
                parsed = {
                    operation: values
                    for operation, values in candidate.items()
                    if isinstance(operation, str) and isinstance(values, Mapping)
                }
        except Exception as exc:  # noqa: BLE001
            _safe_warning("Failed to parse maintenance metrics: %s", exc)

        for operation, metrics in parsed.items():
            if operation not in {"compact", "cleanup"}:
                continue
            try:
                for entry in sub_results:
                    if (
                        entry.get("operation") == operation
                        and entry.get("status") == DataFlowStatus.SUCCEEDED.value
                    ):
                        entry["engine_metrics"] = dict(metrics)
                        break
            except Exception as exc:  # noqa: BLE001
                _safe_debug("Could not attach %s maintenance metrics: %s", operation, exc)

        try:
            aggregate = _aggregate_maintenance_metrics(
                parsed,
                sub_results,
                requested_operations=requested_operations,
                operation_errors=operation_errors,
            )
        except Exception as exc:  # noqa: BLE001
            _safe_warning("Failed to aggregate maintenance metrics: %s", exc)
            aggregate = _empty_metric_values()
        try:
            for name in _DESTINATION_METRIC_FIELDS:
                setattr(self._runtime_info, name, aggregate[name])
        except Exception as exc:  # noqa: BLE001
            _safe_debug("Could not attach maintenance metrics: %s", exc)

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
            and ``message`` keys.
        """
        start = utc_now()
        status = DataFlowStatus.SKIPPED.value
        message: Optional[str] = None

        try:
            if table_exists:
                _safe_debug("%s starting at %s", op_name.capitalize(), location)
                fn()
                status = DataFlowStatus.SUCCEEDED.value
            else:
                _safe_info("%s skipped — table does not exist at %s", op_name.capitalize(), location)
        except Exception as exc:
            status = DataFlowStatus.FAILED.value
            message = str(exc) or type(exc).__name__
            errors.append(f"{op_name.capitalize()}: {message}")
            # This helper records the partial result and lets the outer
            # execution boundary emit the one terminal traceback.
            _safe_debug("%s failed for %s: %s", op_name.capitalize(), location, exc)

        end = utc_now()
        duration = (end - start).total_seconds()
        if status == DataFlowStatus.SUCCEEDED.value:
            _safe_debug("%s succeeded at %s (%.2fs)", op_name.capitalize(), location, duration)
        result = {
            "operation": op_name,
            "status": status,
            "duration_seconds": duration,
            "message": message,
        }
        if status == DataFlowStatus.SKIPPED.value:
            result["message"] = "Destination table does not exist"
        return result

    # ------------------------------------------------------------------
    # Abstract / overridable methods
    # ------------------------------------------------------------------

    @abstractmethod
    def _write_internal(
        self,
        df: DF,
        dataflow: DataFlow,
        *,
        watermark_window: Optional[WindowSpec] = None,
    ) -> None:
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
        target = self._resolve_target(dataflow)
        return self._engine.get_history(
            table_name=target.table_name,
            path=target.path,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            fmt=target.format,
        )

    def _resolve_target(self, dataflow: DataFlow) -> ResolvedDestination:
        """Return the writer/engine target contract for *dataflow*."""

        return resolve_destination_target(dataflow.destination, require_target=False)

    def _parse_write_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, Optional[int]]:
        """Parse write metrics from raw history entries.

        Override in format-specific writers (e.g. :class:`DeltaWriter`,
        :class:`IcebergWriter`) to extract row/file/byte counts.
        """
        return _empty_metric_values()

    def _parse_maintenance_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Optional[int]]]:
        """Parse maintenance metrics from raw history entries.

        Override in format-specific writers to classify history entries
        into ``"compact"`` / ``"cleanup"`` metric buckets.
        """
        return {}
