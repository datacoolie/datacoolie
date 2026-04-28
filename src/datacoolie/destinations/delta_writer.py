"""Delta Lake destination writer.

Writes DataFrames to Delta Lake tables, dispatching to the appropriate
load strategy based on ``dataflow.load_type``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import Format, MaintenanceType
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import DataFlow
from datacoolie.destinations.load_strategies import get_load_strategy
from datacoolie.engines.base import DF
from datacoolie.platforms.aws_platform import AWSPlatform

from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.logging.base import get_logger

logger = get_logger(__name__)


class DeltaWriter(BaseDestinationWriter[DF]):
    """Destination writer for Delta Lake tables.

    Delegates to :func:`get_load_strategy` for the actual write logic
    based on the dataflow's load type.

    Also supports:
    * Maintenance operations (compact, cleanup) via the base class.
    """

    # ------------------------------------------------------------------
    # Path-only routing
    # ------------------------------------------------------------------

    def _use_path_only(self, dataflow: DataFlow) -> bool:
        """Return True when operations must use path instead of table name.

        True when ``athena_output_location`` is set (we register via Athena
        DDL, not Spark's metastore) or when no catalog/database is present.
        Always False when no path is configured.
        """
        dest = dataflow.destination
        if not dest.path:
            return False
        return bool(
            dest.connection.athena_output_location
            or (not dest.connection.catalog and not dest.connection.database)
        )

    def _resolve_handle(
        self, dataflow: DataFlow
    ) -> "tuple[Optional[str], Optional[str]]":
        """Return ``(table_name, path)`` for engine calls.

        Shared by :meth:`_write_internal`, :meth:`_get_history` and
        :meth:`_maintain_internal` so that write and maintenance use the
        same canonical handle.
        """
        dest = dataflow.destination
        table_name = None if self._use_path_only(dataflow) else dest.full_table_name
        return table_name, dest.path

    # ------------------------------------------------------------------
    # History override — prefer path for Delta
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

    def _write_internal(self, df: DF, dataflow: DataFlow) -> None:
        """Write a DataFrame to a Delta table via load strategy.

        Uses ``full_table_name`` as the primary table identifier.
        Path is passed as optional fallback.

        When ``athena_output_location`` is set the write is forced to use the
        path so that the Glue Catalog entry is always created via Athena
        DDL with the correct ``TBLPROPERTIES ('table_type'='DELTA')``.

        Raises:
            DestinationError: If the format is not Delta.
        """
        dest = dataflow.destination
        dest_fmt = dest.connection.format

        if dest_fmt != Format.DELTA.value:
            raise DestinationError(
                f"DeltaWriter only supports Delta format, got: {dest_fmt}",
                details={"format": dest_fmt, "table": dest.full_table_name, "path": dest.path},
            )

        table_name, path = self._resolve_handle(dataflow)

        load_type = dataflow.load_type
        strategy = get_load_strategy(load_type)

        logger.info(
            "DeltaWriter: writing to %s (load_type=%s)",
            dest.full_table_name,
            load_type,
        )
        strategy.execute(df, table_name, dataflow, self._engine, path=path)

        # Post-write: register in Glue Catalog / generate symlink manifest
        self._post_write_catalog(dataflow)

    # ------------------------------------------------------------------
    # Glue Catalog / Symlink manifest
    # ------------------------------------------------------------------

    def _post_write_catalog(self, dataflow: DataFlow) -> None:
        """Register the Delta table in AWS Glue Catalog after writes.

        Always performs a DROP + CREATE of the native Delta table so that
        the Glue Catalog entry has ``TBLPROPERTIES ('table_type'='DELTA')``.

        When ``generate_manifest`` (or ``register_symlink_table``) is
        enabled, also generates the ``_symlink_format_manifest/``.

        When ``register_symlink_table`` is enabled, additionally registers
        a ``SymlinkTextInputFormat`` table for Redshift Spectrum / pre-v3
        Athena / Presto.

        Guarded by ``isinstance(platform, AWSPlatform)`` and the
        ``athena_output_location`` connection setting.
        """
        platform = getattr(self._engine, "platform", None)
        if not isinstance(platform, AWSPlatform):
            return

        dest = dataflow.destination
        output_location = dest.connection.athena_output_location
        if not output_location:
            if dest.connection.generate_manifest or dest.connection.register_symlink_table:
                logger.warning(
                    "generate_manifest/register_symlink_table is set for table %r "
                    "but athena_output_location is missing — skipping catalog registration",
                    dest.table,
                )
            return

        path = dest.path
        if not path:
            return

        database = dest.connection.database
        tbl = dest.table

        # 1. Always register native Delta table (DROP + CREATE via Athena)
        try:
            platform.register_delta_table(
                tbl, path, database=database, output_location=output_location,
            )
        except Exception as exc:
            logger.warning("Failed to register native Delta table: %s", exc)

        # 2. Generate symlink manifest if requested
        if dest.connection.generate_manifest or dest.connection.register_symlink_table:
            try:
                self._engine.generate_symlink_manifest(path)
            except Exception as exc:
                logger.warning("Failed to generate symlink manifest: %s", exc)
                return  # skip symlink table registration if manifest fails

        # 3. Register symlink table if requested
        if dest.connection.register_symlink_table:
            try:
                prefix = dest.connection.symlink_database_prefix
                symlink_db = f"{prefix}{database}" if database else None
                schema_ddl = self._build_schema_ddl(dataflow)
                partition_ddl = self._build_partition_ddl(dataflow)
                platform.register_symlink_table(
                    tbl, path,
                    database=symlink_db,
                    output_location=output_location,
                    schema_ddl=schema_ddl,
                    partition_ddl=partition_ddl,
                )
            except Exception as exc:
                logger.warning("Failed to register symlink table: %s", exc)

    def _build_schema_ddl(self, dataflow: DataFlow) -> str:
        """Build column DDL string from the engine's schema of the written table."""
        dest = dataflow.destination
        path = dest.path
        partition_cols = {pc.column.lower() for pc in dest.partition_columns}

        try:
            df = self._engine.read(fmt=Format.DELTA.value, path=path)
            hive_schema = self._engine.get_hive_schema(df)
        except Exception as exc:
            logger.debug("Could not read Delta schema for DDL: %s", exc)
            return ""

        # Exclude partition columns from the schema DDL (they go in PARTITIONED BY)
        columns = []
        for col_name, hive_type in hive_schema.items():
            if col_name.lower() in partition_cols:
                continue
            columns.append(f"`{col_name}` {hive_type}")
        return ",\n  ".join(columns)

    def _build_partition_ddl(self, dataflow: DataFlow) -> str:
        """Build PARTITIONED BY clause from destination partition columns."""
        dest = dataflow.destination
        if not dest.partition_columns:
            return ""

        try:
            df = self._engine.read(fmt=Format.DELTA.value, path=dest.path)
            hive_schema = self._engine.get_hive_schema(df)
        except Exception as exc:
            # Fallback: assume STRING type for partition columns
            logger.debug("Could not infer partition types: %s", exc)
            hive_schema = {}

        parts = []
        for pc in dest.partition_columns:
            hive_type = hive_schema.get(pc.column, "STRING")
            parts.append(f"`{pc.column}` {hive_type}")
        return f"PARTITIONED BY ({', '.join(parts)})\n"

    # ------------------------------------------------------------------
    # Metrics parsers
    # ------------------------------------------------------------------

    def _parse_write_metrics(self, history: List[Dict[str, Any]]) -> Dict[str, int]:
        """Parse write metrics from Delta history entries."""
        result = {
            "rows_written": 0, "rows_inserted": 0, "rows_updated": 0,
            "rows_deleted": 0, "files_added": 0, "files_removed": 0,
            "bytes_added": 0, "bytes_removed": 0,
        }
        for entry in history:
            m = entry.get("operationMetrics", {})
            # Spark Delta uses camelCase; delta-rs uses snake_case
            rows_out = int(m.get("numOutputRows", m.get("num_output_rows", m.get("numAddedRows", m.get("num_added_rows", 0)))))
            result["rows_written"] += rows_out
            result["rows_inserted"] += int(
                m.get("numTargetRowsInserted", m.get("num_target_rows_inserted",
                    m.get("numOutputRows", m.get("num_output_rows", m.get("numAddedRows", m.get("num_added_rows", 0))))))
            )
            result["rows_updated"] += int(
                m.get("numTargetRowsUpdated", m.get("num_target_rows_updated",
                    m.get("numUpdatedRows", m.get("num_updated_rows", 0))))
            )
            result["rows_deleted"] += int(
                m.get("numTargetRowsDeleted", m.get("num_target_rows_deleted",
                    m.get("numDeletedRows", m.get("num_deleted_rows", 0))))
            )
            result["files_added"] += int(
                m.get("numTargetFilesAdded", m.get("num_target_files_added",
                    m.get("numFiles", m.get("num_files", m.get("numAddedFiles", m.get("num_added_files", 0))))))
            )
            result["files_removed"] += int(
                m.get("numTargetFilesRemoved", m.get("num_target_files_removed",
                    m.get("numRemovedFiles", m.get("num_removed_files", m.get("numDeletedFiles", m.get("num_deleted_files", 0))))))
            )
            result["bytes_added"] += int(
                m.get("numTargetBytesAdded", m.get("num_target_bytes_added",
                    m.get("numAddedBytes", m.get("num_added_bytes", m.get("numOutputBytes", m.get("num_output_bytes", 0))))))
            )
            result["bytes_removed"] += int(
                m.get("numTargetBytesRemoved", m.get("num_target_bytes_removed",
                    m.get("numRemovedBytes", m.get("num_removed_bytes", m.get("numDeletedBytes", m.get("num_deleted_bytes", 0))))))
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
            op = entry.get("operation", "").lower()
            if op in op_map:
                key = op_map[op]
                m = entry.get("operationMetrics", {})
                if key not in result:
                    result[key] = {"files_added": 0, "files_removed": 0, "bytes_added": 0, "bytes_removed": 0}
                result[key]["files_added"] += int(m.get("numAddedFiles", m.get("num_added_files", 0)))
                result[key]["files_removed"] += int(
                    m.get("numRemovedFiles", m.get("num_removed_files",
                        m.get("numDeletedFiles", m.get("num_deleted_files", 0))))
                )
                result[key]["bytes_added"] += int(m.get("numAddedBytes", m.get("num_added_bytes", 0)))
                result[key]["bytes_removed"] += int(
                    m.get("numRemovedBytes", m.get("num_removed_bytes",
                        m.get("sizeOfDataToDelete", m.get("size_of_data_to_delete", 0))))
                )
        return result

    # ------------------------------------------------------------------
    # Maintenance — compact + vacuum, then regenerate symlink / catalog
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

        # Re-register native table + regenerate symlink manifest after maintenance.
        self._post_maintenance_catalog(dataflow)

        return sub_results, errors

    def _post_maintenance_catalog(self, dataflow: DataFlow) -> None:
        """Re-register native table and regenerate symlink after maintenance.

        Uses the same DROP + CREATE logic as :meth:`_post_write_catalog`.
        """
        self._post_write_catalog(dataflow)