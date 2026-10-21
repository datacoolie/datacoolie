"""Iceberg destination writer.

Writes DataFrames to Iceberg tables, preferring table-name-based
operations (DataFrameWriterV2 / SQL MERGE) via catalog.
Falls back to path-based operations when no catalog is configured.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from datacoolie.core.constants import Format, MaintenanceType
from datacoolie.core.models.metrics import MetricAccumulator
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.engines.contracts.maintenance import COMPACT_ACTIONS, CLEANUP_ACTIONS, selected_actions
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.destinations.base import (
    BaseDestinationWriter,
    _parse_history_metric_fields,
)
from datacoolie.destinations.strategies.load import get_load_strategy
from datacoolie.destinations.resolution.target import resolve_destination_target
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.engines.contracts.windows import WindowSpec
from datacoolie.logging.runtime.manager import get_logger

logger = get_logger(__name__)


class IcebergWriter(BaseDestinationWriter[DF]):
    """Destination writer for Apache Iceberg tables.

    Prefers table-name-based operations (catalog-aware) for Iceberg.
    Falls back to path-based operations when no catalog is configured.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        super().__init__(engine)

    def _resolve_handle(
        self, dataflow: DataFlow
    ) -> "tuple[Optional[str], Optional[str]]":
        """Return ``(table_name, path)`` for engine calls.

        Iceberg is catalog-first: prefer ``full_table_name`` when set,
        fall back to path otherwise.
        """
        target = resolve_destination_target(dataflow.destination, engine=self._engine)
        return target.table_name, target.path

    def _write_internal(
        self,
        df: DF,
        dataflow: DataFlow,
        *,
        watermark_window: Optional[WindowSpec] = None,
    ) -> None:
        """Write a DataFrame to an Iceberg table via load strategy.

        Uses ``full_table_name`` as the primary table identifier.
        Path is passed as optional fallback.
        """
        dest = dataflow.destination
        dest_fmt = dest.connection.format

        if dest_fmt != Format.ICEBERG.value:
            raise DestinationError(
                f"IcebergWriter only supports Iceberg format, got: {dest_fmt}",
                details={"format": dest_fmt, "table": dest.full_table_name, "path": dest.path},
            )

        table_name, path = self._resolve_handle(dataflow)

        load_type = dataflow.load_type
        strategy = get_load_strategy(load_type)

        logger.debug(
            "IcebergWriter: writing to %s (load_type=%s)",
            table_name or path,
            load_type,
        )
        strategy.execute(
            df,
            table_name,
            dataflow,
            self._engine,
            path=path,
            watermark_window=watermark_window,
        )

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def run_selected_maintenance(self, dataflow, *, prepared=None, do_compact=True,
                                 do_cleanup=True, retention_hours=None,
                                 compact_options=None, cleanup_options=None):
        """Execute all preflighted native actions against one retained binding."""
        from datacoolie.engines.contracts.maintenance import default_options
        compact_options, cleanup_options = default_options(compact_options), default_options(cleanup_options)
        started = datetime.now(timezone.utc)
        self._runtime_info = DestinationRuntimeInfo(start_time=started, status="running",
                                                    operation_type="maintenance")
        details = []
        self._runtime_info.operation_details = details
        try:
            actions = selected_actions(do_compact=do_compact, do_cleanup=do_cleanup,
                                       compact_options=compact_options, cleanup_options=cleanup_options)
            if prepared is not None and prepared.missing:
                self._runtime_info.status = "skipped"
                self._runtime_info.message = "Prepared maintenance target is missing"
                return self._runtime_info
            if prepared is None or prepared.binding is None:
                if actions and compact_options is None and cleanup_options is None:
                    return super().run_selected_maintenance(dataflow, prepared=prepared,
                        do_compact=do_compact, do_cleanup=do_cleanup, retention_hours=retention_hours)
                if actions:
                    raise DestinationError("Explicit Iceberg actions require a prepared native binding")
            if actions:
                unsupported = set(actions) - prepared.capabilities
                if unsupported:
                    raise DestinationError("Unsupported Iceberg maintenance actions: " + ", ".join(sorted(unsupported)))
            if not actions:
                details.append({"operation": "cleanup" if do_cleanup else "compact", "status": "skipped",
                    "reason": "No maintenance actions selected", "actions": [],
                    "metrics": {key: None for key in ("files_added", "files_removed", "bytes_added", "bytes_removed")}})
                self._runtime_info.status = "succeeded"
                self._runtime_info.message = "No maintenance actions selected"
                return self._runtime_info
            for group, known in (("compact", COMPACT_ACTIONS), ("cleanup", CLEANUP_ACTIONS)):
                requested = [action for action in actions if action in known]
                if not requested:
                    continue
                entry = {"operation": group, "status": "running", "actions": [], "metrics": {}}
                details.append(entry)
                for action in requested:
                    try:
                        detail = self._engine.execute_maintenance_action(prepared, action=action,
                            retention_hours=168 if retention_hours is None else retention_hours)
                        if not isinstance(detail, dict) or detail.get("status") != "succeeded":
                            raise DestinationError("Native maintenance action returned no successful evidence")
                        entry["actions"].append(detail)
                    except Exception as exc:
                        evidence = getattr(exc, "action_detail", None)
                        entry["actions"].append(evidence if isinstance(evidence, dict) else {
                            "action": action, "status": "failed", "reason": str(exc), "metrics": {}})
                        entry["status"] = "failed"
                        entry["reason"] = str(exc)
                        self._set_native_metrics(details)
                        raise
                entry["status"] = "succeeded"
                self._set_native_metrics(details)
            self._runtime_info.status = "succeeded"
            # Diagnostics observe the same incarnation. A diagnostic failure
            # cannot turn acknowledged action effects into false failure.
            try:
                history = self._engine.maintenance_history(prepared, limit=5, start_time=started)
                if history:
                    details.append({"operation": "history", "data": history})
            except Exception as exc:
                details.append({"operation": "history", "reason": str(exc)})
        except Exception as exc:
            self._runtime_info.status = "failed"
            self._runtime_info.message = str(exc) or type(exc).__name__
        finally:
            self._runtime_info.end_time = datetime.now(timezone.utc)
        return self._runtime_info

    def _set_native_metrics(self, groups):
        keys = ("files_added", "files_removed", "bytes_added", "bytes_removed")
        aggregate = MetricAccumulator()
        action_groups = [
            group
            for group in groups
            if isinstance(group, dict) and isinstance(group.get("actions"), list)
        ]
        for group in action_groups:
            action_accumulator = MetricAccumulator()
            for action in group.get("actions", []):
                action_accumulator.add(
                    action.get("metrics") if isinstance(action, dict) else None,
                )
            group_metrics = action_accumulator.metrics
            group["metrics"] = {
                key: getattr(group_metrics, key)
                for key in keys
            }
            aggregate.add(group["metrics"])

        aggregate_metrics = aggregate.metrics
        for key in keys:
            setattr(self._runtime_info, key, getattr(aggregate_metrics, key))
        for key in ("rows_written", "rows_inserted", "rows_updated", "rows_deleted"):
            setattr(self._runtime_info, key, None)

    def _maintain_internal(
        self,
        dataflow: DataFlow,
        *,
        do_compact: bool,
        do_cleanup: bool,
        retention_hours: int,
    ) -> "tuple[List[Dict[str, Any]], List[str]]":
        dest = dataflow.destination
        fmt = dest.connection.format
        table_name, path = self._resolve_handle(dataflow)
        location = table_name or path or "<unknown>"

        sub_results: List[Dict[str, Any]] = []
        errors: List[str] = []

        table_exists = self._engine.exists(table_name=table_name, path=path, fmt=fmt)
        logger.debug(
            "Iceberg maintenance — location=%s, table_exists=%s, do_compact=%s, "
            "do_cleanup=%s, retention_hours=%d",
            location, table_exists, do_compact, do_cleanup, retention_hours,
        )

        if do_compact:
            sub_results.append(self._run_op(
                op_name=MaintenanceType.COMPACT.value,
                table_exists=table_exists,
                fn=lambda: self._engine.compact(table_name=table_name, path=path, fmt=fmt),
                errors=errors,
                location=location,
            ))

        if do_cleanup:
            sub_results.append(self._run_op(
                op_name=MaintenanceType.CLEANUP.value,
                table_exists=table_exists,
                fn=lambda: self._engine.cleanup(
                    table_name=table_name, path=path,
                    retention_hours=retention_hours, fmt=fmt,
                ),
                errors=errors,
                location=location,
            ))

        return sub_results, errors

    # ------------------------------------------------------------------
    # Metrics parsers
    # ------------------------------------------------------------------

    def _parse_write_metrics(self, history: List[Dict[str, Any]]) -> Dict[str, Optional[int]]:
        """Parse sparse write metrics from Iceberg history entries."""

        return _parse_history_metric_fields(
            history,
            container_key="summary",
            aliases={
                "rows_written": ("added-records",),
                "rows_inserted": ("added-records",),
                "rows_updated": ("updated-records",),
                "rows_deleted": ("deleted-records",),
                "files_added": ("added-data-files",),
                "files_removed": ("deleted-data-files",),
                "bytes_added": ("added-files-size",),
                "bytes_removed": ("removed-files-size",),
            },
        )

    def _parse_maintenance_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Optional[int]]]:
        """Parse sparse compaction and cleanup metrics from Iceberg history."""
        op_map = {
            "replace": MaintenanceType.COMPACT.value,
            "overwrite": MaintenanceType.COMPACT.value,
            "delete": MaintenanceType.CLEANUP.value,
            "expire_snapshots": MaintenanceType.CLEANUP.value,
            "remove_orphan_files": MaintenanceType.CLEANUP.value,
        }
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for entry in history:
            op = str(entry.get("operation", "")).lower()
            if op in op_map:
                grouped.setdefault(op_map[op], []).append(entry)

        result: Dict[str, Dict[str, Optional[int]]] = {}
        for key, entries in grouped.items():
            metrics = _parse_history_metric_fields(
                entries,
                container_key="summary",
                aliases={
                    "files_added": ("added-data-files",),
                    "files_removed": ("deleted-data-files",),
                    "bytes_added": ("added-files-size",),
                    "bytes_removed": ("removed-files-size",),
                },
            )
            result[key] = metrics
        return result
