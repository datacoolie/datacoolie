"""Iceberg destination writer.

Writes DataFrames to Iceberg tables, preferring table-name-based
operations (DataFrameWriterV2 / SQL MERGE) via catalog.
Falls back to path-based operations when no catalog is configured.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from datacoolie.core.constants import Format, MaintenanceType
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
        target = resolve_destination_target(dataflow.destination)
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
            metrics.update(
                {
                    "rows_written": 0,
                    "rows_inserted": 0,
                    "rows_updated": 0,
                    "rows_deleted": 0,
                }
            )
            if key == MaintenanceType.CLEANUP.value:
                metrics["files_added"] = 0
                metrics["bytes_added"] = 0
            result[key] = metrics
        return result
