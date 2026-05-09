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


# ----------------------------------------------------------------------
# Module-level schema drift helpers (no IO, pure string comparison)
# ----------------------------------------------------------------------


def _struct_field_names(hive_type: str) -> frozenset:
    """Return the top-level field names of a Hive ``STRUCT<...>`` type string.

    Parsing is depth-aware so nested angle brackets inside field types
    (e.g. ``STRUCT<a:ARRAY<INT>>``) are handled correctly.

    Returns an empty ``frozenset`` for any non-STRUCT type.
    """
    trimmed = hive_type.strip()
    if not trimmed.upper().startswith("STRUCT<") or not trimmed.endswith(">"):
        return frozenset()
    inner = trimmed[7:-1]  # strip "STRUCT<" prefix and ">" suffix
    fields: list = []
    depth = 0
    start = 0
    for i, ch in enumerate(inner):
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        elif ch == "," and depth == 0:
            fields.append(inner[start:i].strip())
            start = i + 1
    fields.append(inner[start:].strip())
    result: set = set()
    for field in fields:
        colon = field.find(":")
        if colon > 0:
            result.add(field[:colon].lower())
    return frozenset(result)


def _has_new_columns(
    pre: Dict[str, str],
    post: Dict[str, str],
    *,
    strict: bool = False,
) -> bool:
    """Return ``True`` when *post* schema has grown or drifted compared to *pre*.

    Rules for **native** Delta tables (``strict=False``):
    * New top-level columns → recreate (Athena DDL must be refreshed).
    * New attributes inside STRUCT columns → recreate.
    * Type-only changes are **ignored** — Athena v3 auto-infers the
      Delta schema from the transaction log so the catalog stays valid.

    Rules for **symlink** tables (``strict=True``):
    * All of the above, *plus* any type drift in existing columns, because
      the symlink table stores explicit Hive DDL and a stale DDL causes
      incorrect query results in Redshift Spectrum / Athena.

    Comparison is **case-insensitive** on column names, matching the
    Delta / Hive behavior for identifiers.
    """
    pre_lower = {k.lower(): v for k, v in pre.items()}
    post_lower = {k.lower(): v for k, v in post.items()}

    # 1. New top-level columns
    if set(post_lower) - set(pre_lower):
        return True

    # 2. Type drift (symlink only — explicit DDL must not go stale)
    if strict:
        for col, pre_type in pre_lower.items():
            if post_lower.get(col, pre_type) != pre_type:
                return True

    # 3. New attributes inside STRUCT columns
    for col in pre_lower:
        pre_fields = _struct_field_names(pre_lower[col])
        post_fields = _struct_field_names(post_lower.get(col, ""))
        if post_fields - pre_fields:
            return True

    return False


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

    def _read_delta_hive_schema(
        self,
        path: Optional[str],
        *,
        log_context: str,
    ) -> Dict[str, str]:
        """Read Hive schema from a Delta table path.

        Returns an empty dict when the schema cannot be read.
        """
        if not path or not self._engine.exists(path=path, fmt=Format.DELTA.value):
            return {}

        try:
            df = self._engine.read(fmt=Format.DELTA.value, path=path)
            return self._engine.get_hive_schema(df)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not read %s: %s", log_context, exc)
            return {}

    def _capture_aws_state(
        self,
        dataflow: DataFlow,
    ) -> Optional[Dict[str, Any]]:
        """Capture AWS catalog runtime state for post-write and maintenance planners.

        Stores only values that cannot be derived from *dataflow*: the platform
        instance, the Glue existence flags, and the pre-write Delta table schema.

        The pre-write schema is read from the Delta path only when at least one
        Glue entry already exists — first-write path skips the disk read entirely.

        Returns ``None`` when AWS catalog sync is not applicable.
        """
        platform = getattr(self._engine, "platform", None)
        if not isinstance(platform, AWSPlatform):
            return None

        dest = dataflow.destination
        if not dest.connection.athena_output_location:
            if dest.connection.generate_manifest or dest.connection.register_symlink_table:
                logger.warning(
                    "generate_manifest/register_symlink_table is set for table %r "
                    "but athena_output_location is missing — skipping catalog registration",
                    dest.table,
                )
            return None

        _, path = self._resolve_handle(dataflow)
        if not path or not dest.connection.database:
            return None

        database = dest.connection.database
        tbl = dest.table
        prefix = dest.connection.symlink_database_prefix
        symlink_db = f"{prefix}{database}" if prefix and database else None

        native_exists = platform.glue_table_exists(database, tbl)
        symlink_exists = (
            platform.glue_table_exists(symlink_db, tbl)
            if dest.connection.register_symlink_table and symlink_db
            else False
        )

        # Read pre-write Delta schema only when a Glue entry already exists.
        # First-write path (both flags False) skips this disk read entirely.
        pre_schema: Dict[str, str] = (
            self._read_delta_hive_schema(path, log_context="pre-write Delta schema")
            if native_exists or symlink_exists
            else {}
        )

        return {
            "platform": platform,
            "native_exists": native_exists,
            "symlink_exists": symlink_exists,
            "pre_schema": pre_schema,
        }

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

        # Capture pre-write Delta schema before the write executes.
        aws_state = self._capture_aws_state(dataflow)

        strategy.execute(df, table_name, dataflow, self._engine, path=path)

        # Post-write: register in Glue Catalog / generate symlink manifest
        self._post_write_catalog(dataflow, aws_state=aws_state)

    # ------------------------------------------------------------------
    # Glue Catalog / Symlink manifest
    # ------------------------------------------------------------------

    def _post_write_catalog(
        self,
        dataflow: DataFlow,
        *,
        aws_state: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register the Delta table in AWS Glue Catalog after a write.

        Decision planner — compares the captured pre-write AWS state against
        the post-write table schema, then applies the minimum catalog actions
        needed:

        **Native Delta table** (Athena v3):

        * First write (not in Glue): ``CREATE`` with ``recreate=False``.
        * Subsequent write, schema unchanged: skip — no catalog action.
        * Subsequent write, additive schema growth or new STRUCT attributes:
          ``DELETE`` (Glue API) + ``CREATE`` with ``recreate=True``.

        **Symlink table** (Redshift Spectrum / pre-v3 Athena):

        * Always regenerates ``_symlink_format_manifest/`` after a write.
        * First write: ``CREATE`` with ``recreate=False``; ``MSCK REPAIR``
          when partitioned.
        * Subsequent write, no DDL drift: skip re-register; ``MSCK REPAIR``
          when partitioned (to pick up new partition paths).
        * Subsequent write with DDL drift (new columns, type change, or
          new STRUCT fields): ``DELETE`` + ``CREATE`` with ``recreate=True``;
          ``MSCK REPAIR`` when partitioned.

        Guarded by ``isinstance(platform, AWSPlatform)`` and the
        ``athena_output_location`` connection setting.
        """
        aws_state = aws_state or self._capture_aws_state(dataflow)
        if not aws_state:
            return

        platform: AWSPlatform = aws_state["platform"]
        native_exists: bool = aws_state["native_exists"]
        symlink_exists: bool = aws_state["symlink_exists"]
        pre_schema: Dict[str, str] = aws_state["pre_schema"]

        dest = dataflow.destination
        _, path = self._resolve_handle(dataflow)
        database: str = dest.connection.database
        tbl: str = dest.table
        output_location: str = dest.connection.athena_output_location
        prefix = dest.connection.symlink_database_prefix
        symlink_db = f"{prefix}{database}" if prefix and database else None

        # Read post-write Delta schema once — reused for drift detection and DDL.
        # Skip on a plain first-write where neither comparison nor DDL is needed.
        needs_post_schema = native_exists or dest.connection.register_symlink_table
        post_schema: Dict[str, str] = (
            self._read_delta_hive_schema(path, log_context="post-write Delta schema")
            if needs_post_schema
            else {}
        )

        # ------------------------------------------------------------------
        # 1. Native Delta table
        # ------------------------------------------------------------------
        if native_exists:
            native_recreate = _has_new_columns(pre_schema, post_schema, strict=True)
            if not native_recreate:
                logger.debug(
                    "Native Glue entry unchanged for %s.%s — skipping", database, tbl
                )
        else:
            native_recreate = False  # first write: create-only

        if not native_exists or native_recreate:
            try:
                platform.register_delta_table(
                    tbl, path,
                    database=database,
                    output_location=output_location,
                    recreate=native_recreate,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to register native Delta table: %s", exc)

        # ------------------------------------------------------------------
        # 2. Symlink manifest + table
        # ------------------------------------------------------------------
        if not (dest.connection.generate_manifest or dest.connection.register_symlink_table):
            return

        try:
            self._engine.generate_symlink_manifest(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to generate symlink manifest: %s", exc)
            return  # skip symlink table registration if manifest fails

        if not dest.connection.register_symlink_table:
            return

        is_partitioned = bool(dest.partition_columns)

        try:
            symlink_recreate = symlink_exists and _has_new_columns(
                pre_schema, post_schema, strict=True
            )
            if not symlink_exists or symlink_recreate:
                schema_ddl = self._build_schema_ddl_from_schema(dataflow, post_schema)
                partition_ddl = self._build_partition_ddl_from_schema(dataflow, post_schema)
                platform.register_symlink_table(
                    tbl, path,
                    database=symlink_db,
                    output_location=output_location,
                    schema_ddl=schema_ddl,
                    partition_ddl=partition_ddl,
                    recreate=symlink_recreate,
                    run_msck=is_partitioned,
                )
            elif is_partitioned:
                platform.repair_table_partitions(
                    tbl,
                    database=symlink_db,
                    output_location=output_location,
                )
            else:
                logger.debug(
                    "Symlink Glue entry unchanged for %s.%s — skipping", symlink_db, tbl
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to register/update symlink table: %s", exc)

    def _build_schema_ddl_from_schema(
        self,
        dataflow: DataFlow,
        hive_schema: Dict[str, str],
    ) -> str:
        """Build non-partition column DDL from a pre-computed Hive schema dict."""
        partition_cols = {
            partition.column.lower() for partition in dataflow.destination.partition_columns
        }
        columns = []
        for col_name, hive_type in hive_schema.items():
            if col_name.lower() in partition_cols:
                continue
            columns.append(f"`{col_name}` {hive_type}")
        return ",\n  ".join(columns)

    def _build_partition_ddl_from_schema(
        self, dataflow: DataFlow, hive_schema: Dict[str, str]
    ) -> str:
        """Build PARTITIONED BY clause from a pre-computed Hive schema dict."""
        dest = dataflow.destination
        if not dest.partition_columns:
            return ""
        parts = []
        for pc in dest.partition_columns:
            hive_type = (
                hive_schema.get(pc.column)
                or hive_schema.get(pc.column.lower())
                or "STRING"
            )
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
        aws_state = self._capture_aws_state(dataflow)

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
        self._post_maintenance_catalog(dataflow, aws_state=aws_state)

        return sub_results, errors

    def _post_maintenance_catalog(
        self,
        dataflow: DataFlow,
        *,
        aws_state: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Repair Glue catalog entries and regenerate symlink after maintenance.

        Same planner as :meth:`_post_write_catalog` but with schema evolution
        disabled: maintenance never recreates an existing Glue entry, it only
        creates missing ones and refreshes manifests / partitions.
        """
        aws_state = aws_state or self._capture_aws_state(dataflow)
        if not aws_state:
            return

        platform: AWSPlatform = aws_state["platform"]
        native_exists: bool = aws_state["native_exists"]
        symlink_exists: bool = aws_state["symlink_exists"]

        dest = dataflow.destination
        _, path = self._resolve_handle(dataflow)
        database: str = dest.connection.database
        tbl: str = dest.table
        output_location: str = dest.connection.athena_output_location
        prefix = dest.connection.symlink_database_prefix
        symlink_db = f"{prefix}{database}" if prefix and database else None
        is_partitioned = bool(dest.partition_columns)

        # 1. Native — create if missing, never recreate during maintenance
        if not native_exists:
            try:
                platform.register_delta_table(
                    tbl, path,
                    database=database,
                    output_location=output_location,
                    recreate=False,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to register native Delta table after maintenance: %s", exc
                )

        # 2. Symlink — always regenerate manifest
        if not (dest.connection.generate_manifest or dest.connection.register_symlink_table):
            return

        try:
            self._engine.generate_symlink_manifest(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to generate symlink manifest after maintenance: %s", exc)
            return

        if not dest.connection.register_symlink_table:
            return

        try:
            if not symlink_exists:
                # Entry missing — create it (read schema for DDL)
                post_schema = self._read_delta_hive_schema(
                    path,
                    log_context="schema for maintenance symlink DDL",
                )
                schema_ddl = self._build_schema_ddl_from_schema(dataflow, post_schema)
                partition_ddl = self._build_partition_ddl_from_schema(dataflow, post_schema)
                platform.register_symlink_table(
                    tbl, path,
                    database=symlink_db,
                    output_location=output_location,
                    schema_ddl=schema_ddl,
                    partition_ddl=partition_ddl,
                    recreate=False,
                    run_msck=is_partitioned,
                )
            elif is_partitioned:
                # Existing symlink, partitioned — repair partitions only
                platform.repair_table_partitions(
                    tbl,
                    database=symlink_db,
                    output_location=output_location,
                )
            else:
                logger.debug(
                    "Symlink Glue entry unchanged after maintenance for %s.%s — skipping",
                    symlink_db,
                    tbl,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to update symlink table after maintenance: %s", exc)