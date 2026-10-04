"""Delta Lake destination writer.

The writer owns Delta load-strategy and maintenance orchestration. Optional
AWS Glue/Athena catalog work lives in :mod:`datacoolie.destinations._delta.aws_catalog`
so non-AWS Delta writes do not carry catalog-specific branching.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import Format, MaintenanceType
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.destinations._delta.aws_catalog import (
    AwsDeltaState,
    AwsDeltaCatalogCoordinator,
)
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
_UNSET = object()


class DeltaWriter(BaseDestinationWriter[DF]):
    """Destination writer for Delta Lake tables."""

    def __init__(self, engine: BaseEngine[DF]) -> None:
        super().__init__(engine)
        self._aws_catalog = AwsDeltaCatalogCoordinator(engine)

    # ------------------------------------------------------------------
    # Path-only routing and shared compatibility delegates
    # ------------------------------------------------------------------

    def _resolve_handle(
        self, dataflow: DataFlow
    ) -> tuple[Optional[str], Optional[str]]:
        target = resolve_destination_target(dataflow.destination)
        return target.table_name, target.path

    def _capture_aws_state(self, dataflow: DataFlow) -> Optional[AwsDeltaState]:
        return self._aws_catalog.capture_state(dataflow)

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def _get_history(
        self,
        dataflow: DataFlow,
        *,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch Delta table history using the resolved handle."""
        dest = dataflow.destination
        table_name, path = self._resolve_handle(dataflow)
        return self._engine.get_history(
            table_name=table_name,
            path=path,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            fmt=dest.connection.format,
        )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def _write_internal(
        self,
        df: DF,
        dataflow: DataFlow,
        *,
        watermark_window: Optional[WindowSpec] = None,
    ) -> None:
        dest = dataflow.destination
        dest_fmt = dest.connection.format
        if dest_fmt != Format.DELTA.value:
            raise DestinationError(
                f"DeltaWriter only supports Delta format, got: {dest_fmt}",
                details={
                    "format": dest_fmt,
                    "table": dest.full_table_name,
                    "path": dest.path,
                },
            )

        table_name, path = self._resolve_handle(dataflow)
        load_type = dataflow.load_type
        strategy = get_load_strategy(load_type)
        logger.debug(
            "DeltaWriter: writing to %s (load_type=%s)",
            dest.full_table_name,
            load_type,
        )
        aws_state = self._capture_aws_state(dataflow)
        strategy.execute(
            df,
            table_name,
            dataflow,
            self._engine,
            path=path,
            watermark_window=watermark_window,
        )
        self._post_write_catalog(dataflow, aws_state=aws_state)

    # ------------------------------------------------------------------
    # AWS catalog delegates
    # ------------------------------------------------------------------

    def _post_write_catalog(
        self,
        dataflow: DataFlow,
        *,
        aws_state: AwsDeltaState | None | object = _UNSET,
    ) -> None:
        """Apply optional Glue/manifest actions after a successful write."""
        if aws_state is _UNSET:
            self._aws_catalog.post_write(dataflow)
        else:
            self._aws_catalog.post_write(dataflow, aws_state=aws_state)

    # ------------------------------------------------------------------
    # Metrics parsers
    # ------------------------------------------------------------------

    def _parse_write_metrics(self, history: List[Dict[str, Any]]) -> Dict[str, Optional[int]]:
        """Parse sparse write metrics from Delta history entries."""

        return _parse_history_metric_fields(
            history,
            container_key="operationMetrics",
            aliases={
                "rows_written": (
                    "numOutputRows",
                    "num_output_rows",
                    "numAddedRows",
                    "num_added_rows",
                ),
                "rows_inserted": (
                    "numTargetRowsInserted",
                    "num_target_rows_inserted",
                    "numOutputRows",
                    "num_output_rows",
                    "numAddedRows",
                    "num_added_rows",
                ),
                "rows_updated": (
                    "numTargetRowsUpdated",
                    "num_target_rows_updated",
                    "numUpdatedRows",
                    "num_updated_rows",
                ),
                "rows_deleted": (
                    "numTargetRowsDeleted",
                    "num_target_rows_deleted",
                    "numDeletedRows",
                    "num_deleted_rows",
                ),
                "files_added": (
                    "numTargetFilesAdded",
                    "num_target_files_added",
                    "numFiles",
                    "num_files",
                    "numAddedFiles",
                    "num_added_files",
                ),
                "files_removed": (
                    "numTargetFilesRemoved",
                    "num_target_files_removed",
                    "numRemovedFiles",
                    "num_removed_files",
                    "numDeletedFiles",
                    "num_deleted_files",
                ),
                "bytes_added": (
                    "numTargetBytesAdded",
                    "num_target_bytes_added",
                    "numAddedBytes",
                    "num_added_bytes",
                    "numOutputBytes",
                    "num_output_bytes",
                ),
                "bytes_removed": (
                    "numTargetBytesRemoved",
                    "num_target_bytes_removed",
                    "numRemovedBytes",
                    "num_removed_bytes",
                    "numDeletedBytes",
                    "num_deleted_bytes",
                ),
            },
        )

    def _parse_maintenance_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Optional[int]]]:
        """Parse sparse compaction and cleanup metrics from Delta history."""
        op_map = {
            "optimize": MaintenanceType.COMPACT.value,
            "vacuum start": MaintenanceType.CLEANUP.value,
            "vacuum end": MaintenanceType.CLEANUP.value,
        }
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for entry in history:
            operation = str(entry.get("operation", "")).lower()
            if operation in op_map:
                grouped.setdefault(op_map[operation], []).append(entry)

        result: Dict[str, Dict[str, Optional[int]]] = {}
        for key, entries in grouped.items():
            metrics = _parse_history_metric_fields(
                entries,
                container_key="operationMetrics",
                aliases={
                    "files_added": (
                        "numAddedFiles",
                        "num_added_files",
                    ),
                    "files_removed": (
                        "numRemovedFiles",
                        "num_removed_files",
                        "numDeletedFiles",
                        "num_deleted_files",
                    ),
                    "bytes_added": (
                        "numAddedBytes",
                        "num_added_bytes",
                    ),
                    "bytes_removed": (
                        "numRemovedBytes",
                        "num_removed_bytes",
                        "sizeOfDataToDelete",
                        "size_of_data_to_delete",
                    ),
                },
            )
            # Compaction and cleanup cannot change logical row counts. Cleanup
            # cannot add physical files or bytes, but its removals remain
            # unknown unless history measures them.
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
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        dest = dataflow.destination
        fmt = dest.connection.format
        table_name, path = self._resolve_handle(dataflow)
        location = table_name or path or "<unknown>"
        sub_results: List[Dict[str, Any]] = []
        errors: List[str] = []
        table_exists = self._engine.exists(table_name=table_name, path=path, fmt=fmt)
        aws_state = self._capture_aws_state(dataflow)
        logger.debug(
            "Delta maintenance — location=%s, table_exists=%s, do_compact=%s, "
            "do_cleanup=%s, retention_hours=%d",
            location,
            table_exists,
            do_compact,
            do_cleanup,
            retention_hours,
        )

        if do_compact:
            sub_results.append(
                self._run_op(
                    op_name=MaintenanceType.COMPACT.value,
                    table_exists=table_exists,
                    fn=lambda: self._engine.compact(
                        table_name=table_name, path=path, fmt=fmt
                    ),
                    errors=errors,
                    location=location,
                )
            )

        if do_cleanup:
            sub_results.append(
                self._run_op(
                    op_name=MaintenanceType.CLEANUP.value,
                    table_exists=table_exists,
                    fn=lambda: self._engine.cleanup(
                        table_name=table_name,
                        path=path,
                        retention_hours=retention_hours,
                        fmt=fmt,
                    ),
                    errors=errors,
                    location=location,
                )
            )

        self._post_maintenance_catalog(dataflow, aws_state=aws_state)
        return sub_results, errors

    def _post_maintenance_catalog(
        self,
        dataflow: DataFlow,
        *,
        aws_state: AwsDeltaState | None | object = _UNSET,
    ) -> None:
        """Apply optional Glue/manifest actions after maintenance."""
        if aws_state is _UNSET:
            self._aws_catalog.post_maintenance(dataflow)
        else:
            self._aws_catalog.post_maintenance(dataflow, aws_state=aws_state)


__all__ = ["DeltaWriter"]
