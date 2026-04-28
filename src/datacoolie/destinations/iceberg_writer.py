"""Iceberg destination writer.

Writes DataFrames to Iceberg tables, preferring table-name-based
operations (DataFrameWriterV2 / SQL MERGE) via catalog.
Falls back to path-based operations when no catalog is configured.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from datacoolie.core.constants import Format, MaintenanceType
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import DataFlow
from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.destinations.load_strategies import get_load_strategy
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger

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
        dest = dataflow.destination
        if dest.full_table_name:
            return dest.full_table_name, dest.path
        return None, dest.path

    def _write_internal(self, df: DF, dataflow: DataFlow) -> None:
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

        logger.info(
            "IcebergWriter: writing to %s (load_type=%s)",
            table_name or path,
            load_type,
        )
        strategy.execute(df, table_name, dataflow, self._engine, path=path)

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

    def _parse_write_metrics(self, history: List[Dict[str, Any]]) -> Dict[str, int]:
        """Parse write metrics from Iceberg history entries."""
        result = {
            "rows_written": 0, "rows_inserted": 0, "rows_updated": 0,
            "rows_deleted": 0, "files_added": 0, "files_removed": 0,
            "bytes_added": 0, "bytes_removed": 0,
        }
        for entry in history:
            m = entry.get("summary", {})
            result["rows_written"] += int(m.get("added-records", 0))
            result["rows_inserted"] += int(m.get("added-records", 0))
            result["rows_updated"] += 0
            result["rows_deleted"] += int(m.get("deleted-records", 0))
            result["files_added"] += int(m.get("added-data-files", 0))
            result["files_removed"] += int(m.get("deleted-data-files", 0))
            result["bytes_added"] += int(m.get("added-files-size", 0))
            result["bytes_removed"] += int(m.get("removed-files-size", 0))
        return result

    def _parse_maintenance_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, int]]:
        """Parse maintenance metrics from Iceberg history entries."""
        op_map = {
            "replace": MaintenanceType.COMPACT.value,
            "overwrite": MaintenanceType.COMPACT.value,
            "delete": MaintenanceType.CLEANUP.value,
            "expire_snapshots": MaintenanceType.CLEANUP.value,
            "remove_orphan_files": MaintenanceType.CLEANUP.value,
        }
        result: Dict[str, Dict[str, int]] = {}
        for entry in history:
            op = entry.get("operation", "").lower()
            if op in op_map:
                m = entry.get("summary", {})
                key = op_map[op]
                existing = result.get(key, {
                    "files_added": 0, "files_removed": 0,
                    "bytes_added": 0, "bytes_removed": 0,
                })
                existing["files_added"] += int(m.get("added-data-files", 0))
                existing["files_removed"] += int(m.get("deleted-data-files", 0))
                existing["bytes_added"] += int(m.get("added-files-size", 0))
                existing["bytes_removed"] += int(m.get("removed-files-size", 0))
                result[key] = existing
        return result
