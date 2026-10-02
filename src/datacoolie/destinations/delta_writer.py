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
from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.destinations.strategies.load import get_load_strategy
from datacoolie.destinations.resolution.target import resolve_destination_target
from datacoolie.engines.base import DF
from datacoolie.engines.contracts.windows import WindowSpec
from datacoolie.logging.runtime.manager import get_logger

logger = get_logger(__name__)
_UNSET = object()


class DeltaWriter(BaseDestinationWriter[DF]):
    """Destination writer for Delta Lake tables."""

    def __init__(self, engine: Any) -> None:
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

    def _parse_write_metrics(self, history: List[Dict[str, Any]]) -> Dict[str, int]:
        """Parse write metrics from Delta history entries."""
        result = {
            "rows_written": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "files_added": 0,
            "files_removed": 0,
            "bytes_added": 0,
            "bytes_removed": 0,
        }
        for entry in history:
            metrics = entry.get("operationMetrics", {})
            rows_out = int(
                metrics.get(
                    "numOutputRows",
                    metrics.get(
                        "num_output_rows",
                        metrics.get("numAddedRows", metrics.get("num_added_rows", 0)),
                    ),
                )
            )
            result["rows_written"] += rows_out
            result["rows_inserted"] += int(
                metrics.get(
                    "numTargetRowsInserted",
                    metrics.get(
                        "num_target_rows_inserted",
                        metrics.get(
                            "numOutputRows",
                            metrics.get(
                                "num_output_rows",
                                metrics.get(
                                    "numAddedRows",
                                    metrics.get("num_added_rows", 0),
                                ),
                            ),
                        ),
                    ),
                )
            )
            result["rows_updated"] += int(
                metrics.get(
                    "numTargetRowsUpdated",
                    metrics.get(
                        "num_target_rows_updated",
                        metrics.get("numUpdatedRows", metrics.get("num_updated_rows", 0)),
                    ),
                )
            )
            result["rows_deleted"] += int(
                metrics.get(
                    "numTargetRowsDeleted",
                    metrics.get(
                        "num_target_rows_deleted",
                        metrics.get("numDeletedRows", metrics.get("num_deleted_rows", 0)),
                    ),
                )
            )
            result["files_added"] += int(
                metrics.get(
                    "numTargetFilesAdded",
                    metrics.get(
                        "num_target_files_added",
                        metrics.get(
                            "numFiles",
                            metrics.get(
                                "num_files",
                                metrics.get(
                                    "numAddedFiles",
                                    metrics.get("num_added_files", 0),
                                ),
                            ),
                        ),
                    ),
                )
            )
            result["files_removed"] += int(
                metrics.get(
                    "numTargetFilesRemoved",
                    metrics.get(
                        "num_target_files_removed",
                        metrics.get(
                            "numRemovedFiles",
                            metrics.get(
                                "num_removed_files",
                                metrics.get(
                                    "numDeletedFiles",
                                    metrics.get("num_deleted_files", 0),
                                ),
                            ),
                        ),
                    ),
                )
            )
            result["bytes_added"] += int(
                metrics.get(
                    "numTargetBytesAdded",
                    metrics.get(
                        "num_target_bytes_added",
                        metrics.get(
                            "numAddedBytes",
                            metrics.get(
                                "num_added_bytes",
                                metrics.get(
                                    "numOutputBytes",
                                    metrics.get("num_output_bytes", 0),
                                ),
                            ),
                        ),
                    ),
                )
            )
            result["bytes_removed"] += int(
                metrics.get(
                    "numTargetBytesRemoved",
                    metrics.get(
                        "num_target_bytes_removed",
                        metrics.get(
                            "numRemovedBytes",
                            metrics.get(
                                "num_removed_bytes",
                                metrics.get(
                                    "numDeletedBytes",
                                    metrics.get("num_deleted_bytes", 0),
                                ),
                            ),
                        ),
                    ),
                )
            )
        return result

    def _parse_maintenance_metrics(
        self, history: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, int]]:
        """Parse maintenance metrics from Delta history entries."""
        op_map = {
            "optimize": MaintenanceType.COMPACT.value,
            "vacuum start": MaintenanceType.CLEANUP.value,
            "vacuum end": MaintenanceType.CLEANUP.value,
        }
        result: Dict[str, Dict[str, int]] = {}
        for entry in history:
            operation = entry.get("operation", "").lower()
            if operation not in op_map:
                continue
            key = op_map[operation]
            metrics = entry.get("operationMetrics", {})
            result.setdefault(
                key,
                {
                    "files_added": 0,
                    "files_removed": 0,
                    "bytes_added": 0,
                    "bytes_removed": 0,
                },
            )
            result[key]["files_added"] += int(
                metrics.get("numAddedFiles", metrics.get("num_added_files", 0))
            )
            result[key]["files_removed"] += int(
                metrics.get(
                    "numRemovedFiles",
                    metrics.get(
                        "num_removed_files",
                        metrics.get("numDeletedFiles", metrics.get("num_deleted_files", 0)),
                    ),
                )
            )
            result[key]["bytes_added"] += int(
                metrics.get("numAddedBytes", metrics.get("num_added_bytes", 0))
            )
            result[key]["bytes_removed"] += int(
                metrics.get(
                    "numRemovedBytes",
                    metrics.get(
                        "num_removed_bytes",
                        metrics.get("sizeOfDataToDelete", metrics.get("size_of_data_to_delete", 0)),
                    ),
                )
            )
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
