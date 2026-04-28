"""Abstract base class for DataFrame engines.

``BaseEngine[DF]`` is a generic ABC parameterised by DataFrame type.
Concrete implementations (Spark, Polars) bind ``DF`` to their native
DataFrame class.

Section layout
--------------
- **Construction** — ``__init__``, ``platform``, ``set_platform``
- **Read** — ``read_parquet``, ``read_delta``, ``read_iceberg``, ``read_csv``,
  ``read_json``, ``read_jsonl``, ``read_avro``, ``read_excel``, ``read_path``, ``read_database``,
  ``create_dataframe``, ``execute_sql``, ``read_table``
- **Write** — ``write_to_path``, ``write_to_table``
- **Merge** — ``merge_to_path``, ``merge_overwrite_to_path``,
  ``merge_to_table``, ``merge_overwrite_to_table``
- **Transform** — ``add_column``, ``drop_columns``, ``select_columns``,
  ``rename_column``, ``filter_rows``, ``apply_watermark_filter``,
  ``deduplicate``, ``deduplicate_by_rank``, ``cast_column``
- **System columns** — ``add_system_columns``, ``add_file_info_columns``,
  ``remove_system_columns``, ``convert_timestamp_ntz_to_timestamp``
- **Metrics** — ``count_rows``, ``is_empty``, ``get_columns``,
  ``get_schema``, ``get_max_values``, ``get_count_and_max_values``
- **Maintenance** — ``table_exists_by_path``, ``table_exists_by_name``,
  ``get_history_by_path``, ``get_history_by_name``,
  ``compact_by_path``, ``compact_by_name``,
  ``cleanup_by_path``, ``cleanup_by_name``
- **Navigation** (concrete dispatchers) — ``read``, ``write``, ``merge``,
  ``merge_overwrite``, ``exists``, ``get_history``, ``compact``, ``cleanup``
- **SCD Type 2** — ``scd2_to_path``, ``scd2_to_table``, ``scd2``
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

from datacoolie.core.constants import SystemColumn, Format
from datacoolie.core.exceptions import EngineError
from datacoolie.platforms.base import BasePlatform, FileInfo
from datacoolie.logging.base import get_logger

logger = get_logger(__name__)

# DataFrame type variable — bound to nothing so any DF library can be used.
DF = TypeVar("DF")


class BaseEngine(ABC, Generic[DF]):
    """Engine abstraction for DataFrame-based read / write / transform.

    Type parameter *DF* is the concrete DataFrame class
    (e.g. ``pyspark.sql.DataFrame`` or ``polars.DataFrame``).
    """

    # ==================================================================
    # Construction
    # ==================================================================

    def __init__(self, platform: Optional[BasePlatform] = None) -> None:
        self._platform = platform

    @property
    def platform(self) -> Optional[BasePlatform]:
        """The platform attached to this engine, if any."""
        return self._platform

    def set_platform(self, platform: BasePlatform) -> None:
        """Attach a platform to this engine."""
        self._platform = platform

    # ------------------------------------------------------------------
    # Format → file extension mapping (only exceptions to f".{fmt}")
    # ------------------------------------------------------------------

    FORMAT_EXTENSIONS: Dict[str, str] = {
        Format.EXCEL.value: ".xlsx",
    }

    # ------------------------------------------------------------------
    # Driver-specific connection keys
    # ------------------------------------------------------------------
    # Keys that are specific to JDBC or native database drivers and should
    # not leak into higher-level connection APIs (e.g. connectorx).
    # Spark embeds these into the JDBC URL; Polars strips them.
    # Extend this set when adding new driver-specific options.

    DRIVER_CONNECTION_KEYS: frozenset[str] = frozenset({
        "encrypt",
        "trustServerCertificate",
        "hostNameInCertificate",
    })

    # ------------------------------------------------------------------
    # File path helpers
    # ------------------------------------------------------------------

    def _resolve_file_paths(
        self,
        path: str | list[str],
        extension: str,
    ) -> list[str]:
        """Resolve directory paths to concrete file paths with the given extension.

        When a path already ends with *extension* it is returned as-is.
        Otherwise :attr:`platform` is used to list all files matching
        *extension* under that directory path.
        """
        resolved: list[str] = []
        for p in (path if isinstance(path, list) else [path]):
            if p.endswith(extension):
                resolved.append(p)
            else:
                resolved.extend(
                    fi.path
                    for fi in sorted(
                        self._platform.list_files(p, extension=extension),
                        key=lambda fi: fi.path,
                    )
                )
        return resolved

    # ==================================================================
    # Column resolution
    # ==================================================================

    @staticmethod
    def _resolve_column_name(actual_columns: List[str], column: str) -> str:
        """Return the actual column name matching *column* case-insensitively.

        If *column* already matches an element of *actual_columns* exactly it
        is returned as-is (fast path).  Otherwise a case-insensitive scan is
        performed against *actual_columns*.

        Args:
            actual_columns: Column names present in the DataFrame schema.
            column: User-supplied column name (potentially wrong case).

        Returns:
            The exact column name from *actual_columns*.

        Raises:
            EngineError: If no case-insensitive match is found.
        """
        if column in actual_columns:
            return column
        lower = column.lower()
        for actual in actual_columns:
            if actual.lower() == lower:
                return actual
        raise EngineError(
            f"Column {column!r} not found",
            details={"available_columns": actual_columns},
        )

    @staticmethod
    def _resolve_column_names(actual_columns: List[str], columns: List[str]) -> List[str]:
        """Resolve a list of column names case-insensitively.

        Delegates to :meth:`_resolve_column_name` for each entry.

        Args:
            actual_columns: Column names present in the DataFrame schema.
            columns: User-supplied column names.

        Returns:
            List of exact column names from *actual_columns*.

        Raises:
            EngineError: If any column is not found.
        """
        return [BaseEngine._resolve_column_name(actual_columns, c) for c in columns]

    # ==================================================================
    # Read
    # ==================================================================

    @abstractmethod
    def read_parquet(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read Parquet file(s) into a DataFrame."""

    @abstractmethod
    def read_delta(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read a Delta Lake table into a DataFrame."""

    @abstractmethod
    def read_iceberg(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read an Iceberg table into a DataFrame."""

    @abstractmethod
    def read_csv(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read CSV file(s) into a DataFrame."""

    @abstractmethod
    def read_json(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read JSON file(s) into a DataFrame."""

    @abstractmethod
    def read_jsonl(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read JSONL (newline-delimited JSON) file(s) into a DataFrame."""

    @abstractmethod
    def read_avro(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read Avro file(s) into a DataFrame."""

    @abstractmethod
    def read_excel(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read Excel file(s) into a DataFrame."""

    @abstractmethod
    def read_path(
        self,
        path: str | list[str],
        fmt: str,
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read a file or directory at *path* using the given format.

        This is the unified path-based reader.  Implementations should
        dispatch to the appropriate format reader (``read_delta``,
        ``read_parquet``, etc.) based on *fmt*.

        Args:
            path: File or directory path (scalar or list for multi-path reads).
            fmt: Format string — ``"delta"``, ``"iceberg"``, ``"parquet"``,
                ``"csv"``, or ``"json"``.
            options: Additional reader options.

        Raises:
            EngineError: If the format is not supported.
        """

    @abstractmethod
    def read_database(
        self,
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Read from an external database.

        Exactly one of ``table`` or ``query`` must be provided.

        Args:
            table: Plain table or view name (e.g. ``"schema.orders"``).
            query: Full SQL query (e.g. ``"SELECT * FROM orders WHERE id > 100"``).
            options: All connection and reader settings in one flat dict.
                Must include at minimum ``url`` and ``driver``.
                May also carry JDBC tuning keys such as ``fetchsize``,
                ``numPartitions``, ``partitionColumn``, etc.

        In Spark this maps to ``spark.read.format("jdbc").option(...).load()``.
        Use this for reading from *external* databases.  For running
        queries in the engine's own SQL context, use :meth:`execute_sql`.
        """

    @abstractmethod
    def create_dataframe(
        self,
        records: List[Dict[str, Any]],
    ) -> DF:
        """Create a DataFrame from a list of dicts.

        Records may have different keys (heterogeneous schema).  Missing
        fields are filled with ``null`` so that the resulting DataFrame
        has the union of all keys as its columns.

        Args:
            records: List of flat dicts.  Values may be ``None``.

        Returns:
            A DataFrame with one row per record and one column per
            distinct key across all records.
        """

    @abstractmethod
    def execute_sql(
        self,
        sql: str,
        parameters: Optional[Dict[Any, Any]] = None,
    ) -> DF:
        """Execute a SQL query and return a DataFrame."""

    @abstractmethod
    def read_table(
        self,
        table_name: str,
        fmt: str = "delta",
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read a named table and return a DataFrame.

        Args:
            table_name: Fully qualified table name
                (e.g. ``"`catalog`.`db`.`schema`.`table`"``).
            fmt: Table format (``"delta"``, ``"iceberg"``, etc.).
            options: Additional read options.
        """

    # ==================================================================
    # Write
    # ==================================================================

    @abstractmethod
    def write_to_path(
        self,
        df: DF,
        path: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Write a DataFrame to a path."""

    @abstractmethod
    def write_to_table(
        self,
        df: DF,
        table_name: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Write a DataFrame to a named table using DataFrameWriterV2.

        Args:
            df: DataFrame to write.
            table_name: Fully qualified table name (e.g. ``catalog.db.schema.table``).
            mode: Write mode — ``LoadType.OVERWRITE`` (``"overwrite"``),
                ``LoadType.APPEND`` (``"append"``), or ``LoadType.FULL_LOAD`` (``"full_load"``).
            fmt: Table format (``"delta"``, ``"iceberg"``).
            partition_columns: Optional partition column names.
            options: Additional writer options.
        """

    # ==================================================================
    # Merge
    # ==================================================================

    @abstractmethod
    def merge_to_path(
        self,
        df: DF,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge (upsert) a DataFrame into a table at *path*."""

    @abstractmethod
    def merge_overwrite_to_path(
        self,
        df: DF,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge with full-row overwrite (rolling overwrite / SCD1-style).

        Existing rows matching merge keys are *deleted* and re-inserted
        from the source DataFrame.
        """

    @abstractmethod
    def merge_to_table(
        self,
        df: DF,
        table_name: str,
        merge_keys: List[str],
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge (upsert) a DataFrame into a named table via SQL MERGE.

        Args:
            df: Source DataFrame.
            table_name: Fully qualified target table name.
            merge_keys: Join columns for the merge condition.
            fmt: Table format.
            partition_columns: Optional partition column names.
            options: Additional options.
        """

    @abstractmethod
    def merge_overwrite_to_table(
        self,
        df: DF,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge-overwrite (delete + append) a named table via SQL MERGE + WriterV2.

        Args:
            df: Source DataFrame.
            table_name: Fully qualified target table name.
            merge_keys: Join columns for the delete condition.
            fmt: Table format.
            partition_columns: Optional partition column names.
            options: Additional options.
        """

    # ==================================================================
    # Transform
    # ==================================================================

    @abstractmethod
    def add_column(self, df: DF, column_name: str, expression: str) -> DF:
        """Add (or replace) a column using a SQL expression."""

    @abstractmethod
    def drop_columns(self, df: DF, columns: List[str]) -> DF:
        """Drop one or more columns from a DataFrame."""

    @abstractmethod
    def select_columns(self, df: DF, columns: List[str]) -> DF:
        """Return *df* with only the given *columns*, in the specified order."""

    @abstractmethod
    def rename_column(self, df: DF, old_name: str, new_name: str) -> DF:
        """Rename a column."""

    @abstractmethod
    def filter_rows(self, df: DF, condition: str) -> DF:
        """Filter rows using a SQL expression."""

    @abstractmethod
    def apply_watermark_filter(
        self,
        df: DF,
        watermark_columns: List[str],
        watermark: Dict[str, Any],
    ) -> DF:
        """Filter rows where any watermark column exceeds its stored value.

        Builds an OR condition: ``col1 > val1 OR col2 > val2 ...``
        using native DataFrame API.

        Args:
            df: Input DataFrame.
            watermark_columns: Column names to compare.
            watermark: ``{column: threshold_value}`` mapping.

        Returns:
            Filtered DataFrame.
        """

    @abstractmethod
    def deduplicate(
        self,
        df: DF,
        partition_columns: List[str],
        order_columns: Optional[List[str]] = None,
        order: str = "desc",
    ) -> DF:
        """Remove duplicate rows.

        Uses ``ROW_NUMBER()`` window partitioned by *partition_columns*
        and ordered by *order_columns*.
        """

    @abstractmethod
    def deduplicate_by_rank(
        self,
        df: DF,
        partition_columns: List[str],
        order_columns: List[str],
        order: str = "desc",
    ) -> DF:
        """Deduplicate using ``RANK()``, keeping ties."""

    @abstractmethod
    def cast_column(
        self,
        df: DF,
        column_name: str,
        target_type: str,
        fmt: Optional[str] = None,
    ) -> DF:
        """Cast a column to *target_type*, optionally using *fmt*."""

    # ==================================================================
    # System columns
    # ==================================================================

    @abstractmethod
    def add_system_columns(self, df: DF, author: Optional[str] = None) -> DF:
        """Add ``__created_at``, ``__updated_at``, ``__updated_by``."""

    @abstractmethod
    def add_file_info_columns(
        self,
        df: DF,
        file_infos: Optional[List[FileInfo]] = None,
    ) -> DF:
        """Add ``__file_name``, ``__file_path``, ``__file_modification_time``.

        Engines that embed the source path at scan time (e.g. Polars via
        ``include_file_paths``) map *file_infos* onto the already-present
        ``__file_path`` column to inject ``__file_name`` and
        ``__file_modification_time``.  Engines with native metadata support
        (e.g. Spark ``_metadata``) ignore *file_infos* entirely.

        Args:
            df: Input DataFrame — for Polars this must already contain the
                ``__file_path`` column added by the scan reader.
            file_infos: Ordered list of :class:`~datacoolie.platforms.base.FileInfo`
                objects used to resolve name / modification-time per path, or
                ``None`` when only the path itself is available.
        """

    def remove_system_columns(self, df: DF) -> DF:
        """Drop system columns (``__created_at``, etc.).

        This is a **concrete** convenience method that delegates to
        :meth:`drop_columns`.
        """
        sys_cols = list(SystemColumn)
        existing = self.get_columns(df)
        to_drop = [c for c in sys_cols if c in existing]
        return self.drop_columns(df, to_drop) if to_drop else df

    @abstractmethod
    def convert_timestamp_ntz_to_timestamp(self, df: DF) -> DF:
        """Convert all ``timestamp_ntz`` (no-TZ) columns to TZ-aware UTC timestamps."""

    # ==================================================================
    # Symlink manifest
    # ==================================================================

    @abstractmethod
    def generate_symlink_manifest(self, path: str) -> None:
        """Generate a symlink-format manifest for the Delta table at *path*.

        Creates the ``_symlink_format_manifest/`` directory alongside the
        Delta log, enabling query engines that don't natively support Delta
        (e.g. Redshift Spectrum) to read the table via
        ``SymlinkTextInputFormat``.

        Args:
            path: Root path of the Delta table.
        """

    # ==================================================================
    # Metrics
    # ==================================================================

    @abstractmethod
    def count_rows(self, df: DF) -> int:
        """Return the number of rows in *df*."""

    @abstractmethod
    def is_empty(self, df: DF) -> bool:
        """Return ``True`` if the DataFrame has zero rows."""

    @abstractmethod
    def get_columns(self, df: DF) -> List[str]:
        """Return the column names of *df*."""

    @abstractmethod
    def get_schema(self, df: DF) -> Dict[str, str]:
        """Return ``{column_name: data_type_string}``."""

    @abstractmethod
    def get_hive_schema(self, df: DF) -> Dict[str, str]:
        """Return ``{column_name: hive_type_string}`` using native type objects.

        Each engine inspects its native DataFrame schema (e.g.
        ``pyspark.sql.types``, ``polars.datatypes``) to produce
        Hive/Athena-compatible DDL type strings directly — no string
        parsing required.
        """

    @abstractmethod
    def get_max_values(
        self,
        df: DF,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Return the maximum value for each of *columns*."""

    @abstractmethod
    def get_count_and_max_values(
        self,
        df: DF,
        columns: List[str],
    ) -> Tuple[int, Dict[str, Any]]:
        """Return ``(row_count, {column: max_value})`` in one pass."""

    # ==================================================================
    # Maintenance
    # ==================================================================

    @abstractmethod
    def table_exists_by_path(self, path: str, *, fmt: str = "delta") -> bool:
        """Return ``True`` if a table exists at *path*."""

    @abstractmethod
    def table_exists_by_name(self, table_name: str, *, fmt: str = "delta") -> bool:
        """Return ``True`` if a named table exists in the catalog."""

    @abstractmethod
    def get_history_by_path(
        self,
        path: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        """Return audit history entries for the table at *path*.

        Args:
            path: Table location.
            limit: Maximum number of history entries to return.
            start_time: If provided, only return entries whose
                ``timestamp`` is >= *start_time*.
            end_time: If provided, only return entries whose
                ``timestamp`` is <= *end_time*.
            fmt: Table format (``"delta"`` or ``"iceberg"``).
        """

    @abstractmethod
    def get_history_by_name(
        self,
        table_name: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        """Return audit history entries for a named table.

        Args:
            table_name: Fully qualified table name.
            limit: Maximum number of history entries to return.
            start_time: If provided, only return entries whose
                ``timestamp`` / ``made_current_at`` is >= *start_time*.
            end_time: If provided, only return entries whose
                ``timestamp`` / ``made_current_at`` is <= *end_time*.
            fmt: Table format (``"delta"`` or ``"iceberg"``).
        """
        
    @abstractmethod
    def compact_by_path(self, path: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None) -> None:
        """Run compaction (``OPTIMIZE`` or equivalent) on the table at *path*."""

    @abstractmethod
    def compact_by_name(self, table_name: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None) -> None:
        """Run compaction (``OPTIMIZE`` or equivalent) on a named table."""

    @abstractmethod
    def cleanup_by_path(
        self,
        path: str,
        retention_hours: int = 168,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Run cleanup (``VACUUM`` or equivalent) on the table at *path*."""

    @abstractmethod
    def cleanup_by_name(
        self,
        table_name: str,
        retention_hours: int = 168,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Run cleanup (``VACUUM`` or equivalent) on a named table."""

    # ==================================================================
    # Navigation
    # ==================================================================

    def read(
        self,
        fmt: str,
        *,
        table_name: Optional[str] = None,
        path: Optional[str | list[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> DF:
        """Read data by table name (preferred) or by path.

        Args:
            fmt: Format string — ``"delta"``, ``"parquet"``, ``"csv"``, or ``"json"``.
            table_name: Fully qualified table name.  Takes precedence over *path*.
            path: File or directory path (used when *table_name* is not given).
            options: Additional reader options.

        Raises:
            EngineError: If neither *table_name* nor *path* is provided, or if
                the format has no path reader.
        """
        if table_name:
            return self.read_table(table_name, fmt=fmt, options=options)
        if path:
            _format_readers = {
                Format.DELTA.value: self.read_delta,
                Format.ICEBERG.value: self.read_iceberg,
                Format.PARQUET.value: self.read_parquet,
                Format.CSV.value: self.read_csv,
                Format.JSON.value: self.read_json,
                Format.JSONL.value: self.read_jsonl,
                Format.AVRO.value: self.read_avro,
                Format.EXCEL.value: self.read_excel,
            }
            reader = _format_readers.get(fmt.lower())
            if reader is not None:
                return reader(path, options)  # type: ignore[arg-type]
            return self.read_path(path, fmt, options)
        raise EngineError("read() requires table_name or path")

    def write(
        self,
        df: DF,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Write *df* to a table name (preferred) or to a path.

        Args:
            df: DataFrame to write.
            table_name: Target table name.  Takes precedence over *path*.
            path: Target file path (used when *table_name* is not given).
            mode: Write mode (``"overwrite"``, ``"append"``, …).
            fmt: Table / file format.
            partition_columns: Optional partition column names.
            options: Additional writer options.

        Raises:
            EngineError: If neither *table_name* nor *path* is provided.
        """
        if table_name:
            self.write_to_table(
                df, table_name, mode=mode, fmt=fmt,
                partition_columns=partition_columns, options=options,
            )
        elif path:
            self.write_to_path(
                df, path, mode=mode, fmt=fmt,
                partition_columns=partition_columns, options=options,
            )
        else:
            raise EngineError("write() requires table_name or path")

    def merge(
        self,
        df: DF,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge (upsert) *df* into a table name (preferred) or a path.

        Args:
            df: Source DataFrame.
            table_name: Target table name.  Takes precedence over *path*.
            path: Target file path (used when *table_name* is not given).
            merge_keys: Join columns for the merge condition.
            fmt: Table / file format.
            partition_columns: Optional partition column names.
            options: Additional options.

        Raises:
            EngineError: If neither *table_name* nor *path* is provided.
        """
        if table_name:
            self.merge_to_table(
                df, table_name, merge_keys=merge_keys, fmt=fmt,
                partition_columns=partition_columns, options=options,
            )
        elif path:
            self.merge_to_path(
                df, path, merge_keys=merge_keys, fmt=fmt,
                partition_columns=partition_columns, options=options,
            )
        else:
            raise EngineError("merge() requires table_name or path")

    def merge_overwrite(
        self,
        df: DF,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge-overwrite (delete + append) into a table name (preferred) or a path.

        Args:
            df: Source DataFrame.
            table_name: Target table name.  Takes precedence over *path*.
            path: Target file path (used when *table_name* is not given).
            merge_keys: Join columns for the delete condition.
            fmt: Table / file format.
            partition_columns: Optional partition column names.
            options: Additional options.

        Raises:
            EngineError: If neither *table_name* nor *path* is provided.
        """
        if table_name:
            self.merge_overwrite_to_table(
                df, table_name, merge_keys=merge_keys, fmt=fmt,
                partition_columns=partition_columns, options=options,
            )
        elif path:
            self.merge_overwrite_to_path(
                df, path, merge_keys=merge_keys, fmt=fmt,
                partition_columns=partition_columns, options=options,
            )
        else:
            raise EngineError("merge_overwrite() requires table_name or path")

    def exists(
        self,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        fmt: str = "delta",
    ) -> bool:
        """Return ``True`` if the target table or path exists.

        Checks by table name first (via :meth:`table_exists_by_name`),
        then by path (via :meth:`table_exists_by_path`).  Returns ``False`` if
        neither is provided.

        Args:
            table_name: Fully qualified table name.
            path: Table file path.
        """
        if table_name:
            return self.table_exists_by_name(table_name, fmt=fmt)
        if path:
            return self.table_exists_by_path(path, fmt=fmt)
        return False

    def get_history(
        self,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        """Return audit history for a table name (preferred) or a path.

        Args:
            table_name: Fully qualified table name.  Takes precedence over *path*.
            path: Table file path (used when *table_name* is not given).
            limit: Maximum number of history entries to return.
            start_time: If provided, only return entries whose timestamp is
                >= *start_time*.
            end_time: If provided, only return entries whose timestamp is
                <= *end_time*.
            fmt: Table format (``"delta"`` or ``"iceberg"``).

        Raises:
            EngineError: If neither *table_name* nor *path* is provided.
        """
        if table_name:
            return self.get_history_by_name(table_name, limit, start_time, end_time=end_time, fmt=fmt)
        if path:
            return self.get_history_by_path(path, limit, start_time, end_time=end_time, fmt=fmt)
        raise EngineError("get_history() requires table_name or path")

    def compact(
        self,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Run compaction on a table name (preferred) or a path.

        Args:
            table_name: Fully qualified table name.  Takes precedence over *path*.
            path: Table file path (used when *table_name* is not given).
            fmt: Table format (``"delta"`` or ``"iceberg"``).
            options: Format-specific options for compaction sub-operations.

        Raises:
            EngineError: If neither *table_name* nor *path* is provided.
        """
        if table_name:
            self.compact_by_name(table_name, fmt=fmt, options=options)
        elif path:
            self.compact_by_path(path, fmt=fmt, options=options)
        else:
            raise EngineError("compact() requires table_name or path")

    def cleanup(
        self,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        retention_hours: int = 168,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Run cleanup on a table name (preferred) or a path.

        Args:
            table_name: Fully qualified table name.  Takes precedence over *path*.
            path: Table file path (used when *table_name* is not given).
            retention_hours: Minimum age in hours of files to retain.
            fmt: Table format (``"delta"`` or ``"iceberg"``).
            options: Format-specific options for cleanup sub-operations.

        Raises:
            EngineError: If neither *table_name* nor *path* is provided.
        """
        if table_name:
            self.cleanup_by_name(table_name, retention_hours=retention_hours, fmt=fmt, options=options)
        elif path:
            self.cleanup_by_path(path, retention_hours=retention_hours, fmt=fmt, options=options)
        else:
            raise EngineError("cleanup() requires table_name or path")

    # ==================================================================
    # SCD Type 2
    # ==================================================================

    def scd2_to_path(
        self,
        df: DF,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """SCD Type 2 write to a path-based table.

        Tracks historical changes by closing existing current records
        (setting ``scd2_end_date``, ``scd2_is_current = false``) and
        inserting new versions with ``scd2_start_date``,
        ``scd2_is_current = true``.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support scd2_to_path")

    def scd2_to_table(
        self,
        df: DF,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """SCD Type 2 write to a named table."""
        raise NotImplementedError(f"{type(self).__name__} does not support scd2_to_table")

    def scd2(
        self,
        df: DF,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """SCD Type 2 write to a table name (preferred) or a path."""
        if table_name:
            self.scd2_to_table(df, table_name, merge_keys=merge_keys, fmt=fmt,
                              partition_columns=partition_columns, options=options)
        elif path:
            self.scd2_to_path(df, path, merge_keys=merge_keys, fmt=fmt,
                             partition_columns=partition_columns, options=options)
        else:
            raise EngineError("scd2() requires table_name or path")


