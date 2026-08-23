"""Polars + Delta Lake + Iceberg engine implementation.

:class:`PolarsEngine` binds :class:`BaseEngine` to
``polars.LazyFrame`` and implements every abstract method using
the Polars LazyFrame API, the ``deltalake`` library, and optional
``pyiceberg`` integration.

Reads return lazy frames via ``scan_*`` APIs; materialisation
(``.collect()``) happens only at write / merge / metrics boundaries.

Catalog support:
    - **Iceberg Catalog** via ``iceberg_catalog`` (a
      ``pyiceberg.catalog.Catalog`` instance).
    - **SQLContext** for executing SQL queries against registered tables.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import polars as pl

from datacoolie.core.constants import Format
from datacoolie.core.exceptions import EngineError
from datacoolie.core.models import HashColumn, MaskingRule, ValueRule
from datacoolie.core.qualified_names import NameInput
from datacoolie.platforms.base import BasePlatform
from datacoolie.engines.base import BaseEngine, FileInfo
from datacoolie.engines._polars.relations import (
    PatternInput,
    PolarsRelationRegistry,
    RegistrationReport,
)
from datacoolie.engines._polars.sql import PolarsSqlResolver
from datacoolie.engines._polars import registration as polars_registration
from datacoolie.engines._polars.database import read_database as read_database_frame
from datacoolie.engines._polars import delta as delta_ops
from datacoolie.engines._polars import metrics as polars_metrics
from datacoolie.engines._polars import temporal as polars_temporal
from datacoolie.engines._polars import transforms as polars_transforms
from datacoolie.engines._polars.iceberg import operations as iceberg_ops
from datacoolie.engines._polars.file_io import (
    add_file_info_columns as add_polars_file_info_columns,
    read_avro_files,
    read_excel_files,
    read_json_files,
    scan_csv,
    scan_jsonl,
    scan_parquet,
    write_flat_eager,
    write_flat_sink,
)
from datacoolie.engines._polars.type_mapping import (
    build_cast_expr,
)
from datacoolie.logging.base import get_logger

# LazyFrame.sink_delta was added in Polars 0.20.x.
# Fall back to DataFrame.collect().write_delta on older installations.
logger = get_logger(__name__)


# Matches pure-numeric segments (e.g. "2026", "04") and hive key=value
# segments (e.g. "year=2026", "region=US-East") that are appended to a
# path when writing date- or hive-partitioned data.
class PolarsEngine(BaseEngine["pl.LazyFrame"]):
    """Polars implementation of :class:`BaseEngine` bound to ``pl.LazyFrame``.

    All reads return lazy frames via ``scan_*`` APIs.  Materialisation
    (``.collect()``) happens only at write boundaries, merge operations,
    and metric computations.

    Args:
        storage_options: Cloud storage credentials forwarded to
            Polars readers and ``deltalake`` operations.
        iceberg_catalog: A ``pyiceberg.catalog.Catalog`` instance
            for Iceberg table discovery and management.
        sql_context: An existing ``polars.SQLContext`` to reuse.  A new
            context is created when *None* (the default).
        platform: Optional platform to attach to this engine immediately.
    """

    # Default target file size for PartitionBy-based writes.
    # delta-rs does not auto-create checkpoints; replicate Spark's default interval.
    # ==================================================================
    # Construction
    # ==================================================================

    def __init__(
        self,
        platform: Optional[BasePlatform] = None,
        storage_options: Optional[Dict[str, str]] = None,
        *,
        iceberg_catalog: Optional[Any] = None,
        sql_context: Optional[pl.SQLContext] = None,
        sql_dialect: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(platform=platform)
        self._storage_options = storage_options or {}
        self._iceberg_catalog = iceberg_catalog
        self._sql_context = sql_context if sql_context is not None else pl.SQLContext()
        self._relation_registry = PolarsRelationRegistry()
        self._sql_resolver = PolarsSqlResolver(dialect=sql_dialect)

    # ==================================================================
    # Polars extras
    # ==================================================================

    def set_iceberg_catalog(self, catalog: Any) -> None:
        """Replace the Iceberg catalog used for Iceberg table operations."""
        self._iceberg_catalog = catalog

    @property
    def sql_context(self) -> pl.SQLContext:
        """Return the engine's :class:`polars.SQLContext`."""
        return self._sql_context

    @property
    def delta(self) -> type:
        """Lazily imported ``DeltaTable`` class from the ``deltalake`` package."""
        try:
            from deltalake import DeltaTable  # noqa: PLC0415
        except ImportError as exc:
            raise EngineError(
                "deltalake package is required for Delta operations — pip install deltalake"
            ) from exc
        return DeltaTable

    def register_table(
        self,
        name: str,
        data: Union["pl.LazyFrame", "pl.DataFrame"],
    ) -> None:
        """Register a frame as a named table in the SQLContext."""
        self._sql_context.register(name, data)

    def registered_tables(self) -> List[str]:
        """Return sorted logical names indexed by discovery registration."""

        return self._relation_registry.registered_tables()

    @property
    def last_registration_report(self) -> RegistrationReport:
        """Return the observable result of the latest discovery call."""

        return self._relation_registry.last_report

    def register_delta_tables(
        self,
        base_path: str,
        *,
        logical_prefix: NameInput | None = (),
        recursive: bool = False,
        max_depth: Optional[int] = None,
        include: PatternInput = None,
        exclude: PatternInput = None,
        max_tables: Optional[int] = None,
        preload: bool = False,
        on_error: Literal["raise", "skip"] = "raise",
    ) -> List[str]:
        """Index Delta tables below *base_path* for lazy SQL registration.

        Relative table-directory components are appended to ``logical_prefix``.
        Unique trailing suffixes of the resulting 1-4 part name can be used in
        :meth:`execute_sql`. With the default ``preload=False``, table scans are
        created only when a query first references them.

        Args:
            base_path: Physical folder root to enumerate.
            logical_prefix: Structured SQL components prepended to relative paths.
            recursive: Descend into non-table directories.
            max_depth: Optional traversal depth relative to ``base_path``.
            include: Component glob or globs selecting logical names.
            exclude: Component glob or globs removed after include matching.
            max_tables: Safety ceiling; exceeding it aborts without indexing.
            preload: Create and bind every discovered frame immediately.
            on_error: ``"raise"`` or observable best-effort ``"skip"``.
        """
        return polars_registration.register_delta_tables(
            platform=self._platform,
            sql_context=self._sql_context,
            registry=self._relation_registry,
            loader_factory=self.read_delta,
            base_path=base_path,
            logical_prefix=logical_prefix,
            recursive=recursive,
            max_depth=max_depth,
            include=include,
            exclude=exclude,
            max_tables=max_tables,
            preload=preload,
            on_error=on_error,
        )

    def register_iceberg_tables(
        self,
        namespace: NameInput | None = None,
        base_path: Optional[str] = None,
        *,
        logical_prefix: NameInput | None = None,
        recursive: bool = False,
        max_depth: Optional[int] = None,
        include: PatternInput = None,
        exclude: PatternInput = None,
        max_tables: Optional[int] = None,
        preload: bool = False,
        on_error: Literal["raise", "skip"] = "raise",
    ) -> List[str]:
        """Index Iceberg catalog identifiers or table paths for lazy SQL use.

        Catalog mode is used when ``base_path`` is absent and an Iceberg catalog
        is attached. ``namespace=None`` starts at the catalog root. A supplied
        ``logical_prefix`` replaces the catalog/root-namespace mapping; relative
        child namespaces and the table name are appended.

        Args:
            namespace: Physical Iceberg namespace root in catalog mode.
            base_path: Compatibility path-discovery root; mutually exclusive
                with ``namespace``.
            logical_prefix: Replacement logical root, or ``None`` to preserve
                catalog name and root namespace.
            recursive: Enumerate child namespaces/directories.
            max_depth: Optional traversal depth relative to the source root.
            include: Component glob or globs selecting logical names.
            exclude: Component glob or globs removed after include matching.
            max_tables: Safety ceiling; exceeding it aborts without indexing.
            preload: Load metadata and bind all discovered frames immediately.
            on_error: ``"raise"`` or observable best-effort ``"skip"``.
        """
        return polars_registration.register_iceberg_tables(
            catalog=self._iceberg_catalog,
            platform=self._platform,
            storage_options=self._storage_options,
            sql_context=self._sql_context,
            registry=self._relation_registry,
            path_loader_factory=self.read_iceberg,
            namespace=namespace,
            base_path=base_path,
            logical_prefix=logical_prefix,
            recursive=recursive,
            max_depth=max_depth,
            include=include,
            exclude=exclude,
            max_tables=max_tables,
            preload=preload,
            on_error=on_error,
        )

    # ==================================================================
    # Read
    # ==================================================================

    def read_parquet(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        return scan_parquet(path, options, self._storage_options)

    def read_delta(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        return delta_ops.scan_delta(path, options, self._storage_options)

    def read_iceberg(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        """Read an Iceberg table directly from *path*.

        For catalog-based lookup by table name use :meth:`read_table` with
        ``fmt="iceberg"`` and an ``iceberg_catalog`` configured.
        """
        merged: Dict[str, Any] = dict(options or {})
        if self._storage_options:
            merged["storage_options"] = self._storage_options
        return pl.scan_iceberg(path, **merged)

    def read_csv(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        return scan_csv(path, options, self._storage_options)

    def read_json(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        ext = self.FORMAT_EXTENSIONS.get(Format.JSON.value, f".{Format.JSON.value}")
        resolved = self._resolve_file_paths(path, ext)
        if not resolved:
            raise FileNotFoundError(f"No JSON files found at: {path}")
        return read_json_files(resolved, self.platform)

    def read_jsonl(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        return scan_jsonl(path, options, self._storage_options)

    def read_avro(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        ext = self.FORMAT_EXTENSIONS.get(Format.AVRO.value, f".{Format.AVRO.value}")
        resolved = self._resolve_file_paths(path, ext)
        if not resolved:
            raise FileNotFoundError(f"No Avro files found at: {path}")
        return read_avro_files(resolved, self.platform, options)

    def read_excel(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        ext = self.FORMAT_EXTENSIONS.get(Format.EXCEL.value, ".xlsx")
        resolved = self._resolve_file_paths(path, ext)
        if not resolved:
            raise FileNotFoundError(f"No Excel files found at: {path}")
        return read_excel_files(resolved, self.platform, options)

    # Keys consumed by the framework that must not leak to connectorx / pyodbc.
    def read_database(
        self,
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> pl.LazyFrame:
        return read_database_frame(
            table=table,
            query=query,
            options=options,
            driver_connection_keys=self.DRIVER_CONNECTION_KEYS,
        )

    @staticmethod
    def _schema_names(df: "pl.LazyFrame") -> List[str]:
        """Return column names from the LazyFrame schema without collecting data."""
        return df.collect_schema().names()

    def create_dataframe(
        self,
        records: List[Dict[str, Any]],
    ) -> pl.LazyFrame:
        """Create a Polars LazyFrame from a list of heterogeneous dicts.

        ``pl.from_dicts`` with ``infer_schema_length=None`` scans all
        records to compute the union schema and fills missing fields
        with ``null``.
        """
        if not records:
            return pl.DataFrame().lazy()
        return pl.from_dicts(records, infer_schema_length=None).lazy()

    def execute_sql(
        self,
        sql: str,
        parameters: Optional[Dict[Any, Any]] = None,
    ) -> pl.LazyFrame:
        """Execute a SQL query against the engine's :class:`polars.SQLContext`.

        Register tables first via :meth:`register_table`,
        :meth:`register_delta_tables`, or :meth:`register_iceberg_tables`.
        """
        prepared_sql = (
            self._sql_resolver.prepare(
                sql,
                registry=self._relation_registry,
                sql_context=self._sql_context,
            )
            if self._relation_registry
            else sql
        )
        return self._sql_context.execute(prepared_sql)

    def read_table(
        self,
        table_name: str,
        fmt: str = "delta",
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use read_delta(path) instead"
            )
        if fmt_lower == Format.ICEBERG.value:
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine.read_table requires iceberg_catalog for Iceberg table lookup — "
                    "pass iceberg_catalog= to the constructor or call set_iceberg_catalog(). "
                    "For path-based reads use read_iceberg(path) instead."
                )
            return iceberg_ops.read_table(
                table_name,
                catalog=self._iceberg_catalog,
                storage_options=self._storage_options,
            )
        raise EngineError(f"PolarsEngine.read_table: unsupported format {fmt!r}")

    def read_path(
        self,
        path: str | list[str],
        fmt: str,
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        _format_readers = {
            Format.PARQUET.value: self.read_parquet,
            Format.DELTA.value: self.read_delta,
            Format.ICEBERG.value: self.read_iceberg,
            Format.CSV.value: self.read_csv,
            Format.JSON.value: self.read_json,
            Format.JSONL.value: self.read_jsonl,
            Format.AVRO.value: self.read_avro,
            Format.EXCEL.value: self.read_excel,
        }
        reader = _format_readers.get(fmt.lower())
        if reader is None:
            raise EngineError(f"PolarsEngine: unsupported format {fmt!r}")
        return reader(path, options)  # type: ignore[arg-type]

    # ==================================================================
    # Write
    # ==================================================================

    def write_to_path(
        self,
        df: pl.LazyFrame,
        path: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Write a LazyFrame to *path* (always treated as a folder).

        Behaviour by format:

        * **Delta / Iceberg** — uses ``sink_delta`` (streaming);
          ``partition_columns`` forwarded via ``delta_write_options``.
        * **Parquet / CSV / JSONL** — uses streaming ``sink_*``.  When
          *partition_columns* are provided the target is a
          :class:`polars.PartitionBy` so data is split by those columns.
        * **JSON / Avro** — no ``sink_`` available; falls back to eager
          ``write_*`` to a single file (partition_columns not supported).

        Write modes:

        * *overwrite* / *full_load* — existing files in the folder are
          removed before writing.  File name: ``{folder_name}.{ext}``.
        * *append* — a new file is added.  File name includes a
          timestamp: ``{folder_name}_{yyyyMMdd_HHmmss}.{ext}``.
        """
        merged: Dict[str, Any] = dict(options or {})

        if partition_columns:
            partition_columns = self._resolve_column_names(
                self._schema_names(df), partition_columns
            )

        if fmt == Format.DELTA.value:
            delta_ops.write_path(
                df,
                path,
                mode,
                partition_columns,
                merged,
                storage_options=self._storage_options,
                delta_table_cls=self.delta,
            )
        elif fmt in (Format.PARQUET.value, Format.CSV.value, Format.JSONL.value):
            write_flat_sink(
                df,
                path,
                mode,
                fmt,
                partition_columns,
                merged,
                platform=self._platform,
                storage_options=self._storage_options,
            )
        elif fmt in (Format.JSON.value, Format.AVRO.value):
            write_flat_eager(df, path, mode, fmt, merged, platform=self.platform)
        else:
            raise EngineError(f"PolarsEngine write_to_path: unsupported format {fmt!r}")

    def write_to_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use write_to_path(path) instead"
            )
        elif fmt_lower == Format.ICEBERG.value:
            if partition_columns:
                partition_columns = self._resolve_column_names(
                    self._schema_names(df), partition_columns
                )
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine._write_iceberg_table requires iceberg_catalog"
                )
            iceberg_ops.write_table(
                df,
                table_name,
                mode,
                partition_columns,
                catalog=self._iceberg_catalog,
            )
        else:
            raise EngineError(
                f"PolarsEngine.write_to_table: unsupported format {fmt!r}"
            )

    # ------------------------------------------------------------------
    # Iceberg write helpers
    # ------------------------------------------------------------------

    # ==================================================================
    # Merge
    # ==================================================================

    def merge_to_path(
        self,
        df: pl.LazyFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if fmt.lower() != Format.DELTA.value:
            raise EngineError(
                f"PolarsEngine merge_to_path only supports Delta, got {fmt!r}"
            )

        actual = self._schema_names(df)
        merge_keys = self._resolve_column_names(actual, merge_keys)
        if partition_columns:
            partition_columns = self._resolve_column_names(actual, partition_columns)

        delta_ops.merge_path(
            df,
            path,
            actual,
            merge_keys,
            options,
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    def merge_overwrite_to_path(
        self,
        df: pl.LazyFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Rolling overwrite via MERGE DELETE + APPEND (mirrors SparkEngine)."""
        if fmt.lower() != Format.DELTA.value:
            raise EngineError(
                f"PolarsEngine merge_overwrite_to_path only supports Delta, got {fmt!r}"
            )

        actual = self._schema_names(df)
        merge_keys = self._resolve_column_names(actual, merge_keys)
        if partition_columns:
            partition_columns = self._resolve_column_names(actual, partition_columns)

        delta_ops.merge_overwrite_path(
            df,
            path,
            merge_keys,
            partition_columns,
            options,
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    def merge_to_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use merge_to_path(path) instead"
            )
        elif fmt_lower == Format.ICEBERG.value:
            actual = self._schema_names(df)
            merge_keys = self._resolve_column_names(actual, merge_keys)
            if partition_columns:
                partition_columns = self._resolve_column_names(
                    actual, partition_columns
                )
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine._merge_iceberg_table requires iceberg_catalog"
                )
            iceberg_ops.merge_table(
                df,
                table_name,
                merge_keys,
                partition_columns,
                catalog=self._iceberg_catalog,
            )
        else:
            raise EngineError(
                f"PolarsEngine.merge_to_table: unsupported format {fmt!r}"
            )

    def merge_overwrite_to_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use merge_overwrite_to_path(path) instead"
            )
        elif fmt_lower == Format.ICEBERG.value:
            actual = self._schema_names(df)
            merge_keys = self._resolve_column_names(actual, merge_keys)
            if partition_columns:
                partition_columns = self._resolve_column_names(
                    actual, partition_columns
                )
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine._merge_overwrite_iceberg_table requires iceberg_catalog"
                )
            iceberg_ops.merge_overwrite_table(
                df,
                table_name,
                merge_keys,
                partition_columns,
                catalog=self._iceberg_catalog,
            )
        else:
            raise EngineError(
                f"PolarsEngine.merge_overwrite_to_table: unsupported format {fmt!r}"
            )

    # ==================================================================
    # Delete by window (replace_by_watermark)
    # ==================================================================

    def delete_by_window_path(
        self,
        path: str,
        window: Dict[str, tuple],
        fmt: str = "delta",
    ) -> None:
        """Delete rows in a Delta path where columns fall within the window bounds."""
        if fmt.lower() != Format.DELTA.value:
            raise EngineError(
                f"PolarsEngine delete_by_window_path only supports Delta, got {fmt!r}"
            )

        delta_ops.delete_by_window_path(
            path,
            polars_temporal.build_window_predicate(window),
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    def delete_by_window_table(
        self,
        table_name: str,
        window: Dict[str, tuple],
        fmt: str = "delta",
    ) -> None:
        """Delete rows in a named table within the value window."""
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use delete_by_window_path(path) instead"
            )
        if fmt_lower == Format.ICEBERG.value:
            if not self._iceberg_catalog:
                raise EngineError(
                    "PolarsEngine.delete_by_window_table requires iceberg_catalog for Iceberg format"
                )
            # PyIceberg requires strict ISO-8601 timestamps (T separator, not space).
            iso_window = {
                col: (
                    polars_temporal.to_iso8601(lower),
                    polars_temporal.to_iso8601(upper),
                )
                for col, (lower, upper) in window.items()
            }
            predicate = polars_temporal.build_window_predicate(
                iso_window, quote_char=""
            )
            iceberg_ops.delete_by_window(
                table_name, predicate, catalog=self._iceberg_catalog
            )
            return
        raise EngineError(
            f"PolarsEngine.delete_by_window_table: unsupported format {fmt!r}"
        )

    # ==================================================================
    # SCD Type 2
    # ==================================================================

    def scd2_to_path(
        self,
        df: pl.LazyFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """SCD2 via two-step MERGE + APPEND on a Delta table.

        Step 1: MERGE to close current rows (set ``__valid_to``,
                ``__is_current = false``) where source is strictly newer.
        Step 2: APPEND all source rows as new versions.

        This avoids the staged-UNION pattern (2× data duplication) by
        splitting close and insert into separate operations — same
        approach as :meth:`merge_overwrite_to_path`.
        """
        if fmt.lower() != Format.DELTA.value:
            raise EngineError(
                f"PolarsEngine scd2_to_path only supports Delta, got {fmt!r}"
            )

        actual = self._schema_names(df)
        merge_keys = self._resolve_column_names(actual, merge_keys)
        if partition_columns:
            partition_columns = self._resolve_column_names(actual, partition_columns)

        delta_ops.scd2_path(
            df,
            path,
            merge_keys,
            partition_columns,
            options,
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    def scd2_to_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """SCD2 for named tables (Iceberg two-step, non-atomic).

        1. Upsert to close current matched rows (``__valid_to``,
           ``__is_current = false``).
        2. Append all source rows as new versions.
        """
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use scd2_to_path(path) instead"
            )
        elif fmt_lower == Format.ICEBERG.value:
            actual = self._schema_names(df)
            merge_keys = self._resolve_column_names(actual, merge_keys)
            if partition_columns:
                partition_columns = self._resolve_column_names(
                    actual, partition_columns
                )
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine._scd2_iceberg_table requires iceberg_catalog"
                )
            iceberg_ops.scd2_table(
                df,
                table_name,
                merge_keys,
                partition_columns,
                catalog=self._iceberg_catalog,
            )
        else:
            raise EngineError(f"PolarsEngine.scd2_to_table: unsupported format {fmt!r}")

    # ------------------------------------------------------------------
    # Iceberg merge helpers
    # ------------------------------------------------------------------

    # ==================================================================
    # Transform
    # ==================================================================

    def add_column(
        self, df: pl.LazyFrame, column_name: str, expression: str
    ) -> pl.LazyFrame:
        return polars_transforms.add_column(df, column_name, expression)

    def drop_columns(self, df: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        actual = self._schema_names(df)
        lower_map = {c.lower(): c for c in actual}
        to_drop = [lower_map[c.lower()] for c in columns if c.lower() in lower_map]
        return polars_transforms.drop_columns(df, to_drop)

    def select_columns(self, df: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        resolved = self._resolve_column_names(self._schema_names(df), columns)
        return polars_transforms.select_columns(df, resolved)

    def rename_column(
        self, df: pl.LazyFrame, old_name: str, new_name: str
    ) -> pl.LazyFrame:
        actual_old = self._resolve_column_name(self._schema_names(df), old_name)
        return polars_transforms.rename_column(df, actual_old, new_name)

    def rename_columns(self, df: pl.LazyFrame, mapping: Dict[str, str]) -> pl.LazyFrame:
        if not mapping:
            return df
        actual = self._schema_names(df)
        resolved = {
            self._resolve_column_name(actual, old_name): new_name
            for old_name, new_name in mapping.items()
        }
        return polars_transforms.rename_columns(df, resolved)

    def apply_value_rule(
        self, df: pl.LazyFrame, rule: ValueRule, *, missing_column_policy: str = "error"
    ) -> pl.LazyFrame:
        schema = df.collect_schema()
        actual = schema.names()
        resolved_columns: List[str] = []
        for requested in rule.columns:
            try:
                resolved_columns.append(self._resolve_column_name(actual, requested))
            except EngineError:
                if missing_column_policy == "ignore":
                    continue
                raise

        return polars_transforms.apply_value_rule(df, rule, schema, resolved_columns)

    def apply_masking_rule(
        self,
        df: pl.LazyFrame,
        rule: MaskingRule,
        *,
        missing_column_policy: str = "error",
    ) -> pl.LazyFrame:
        schema = df.collect_schema()
        actual = schema.names()
        resolved_columns: List[str] = []
        for requested in rule.columns:
            try:
                resolved_columns.append(self._resolve_column_name(actual, requested))
            except EngineError:
                if missing_column_policy == "ignore":
                    continue
                raise

        return polars_transforms.apply_masking_rule(df, rule, schema, resolved_columns)

    def add_hash_column(self, df: pl.LazyFrame, definition: HashColumn) -> pl.LazyFrame:
        """Add a stable hash over DataCoolie's canonical scalar payload."""
        schema = df.collect_schema()
        actual = schema.names()
        columns = [
            self._resolve_column_name(actual, requested)
            for requested in definition.columns
        ]
        return polars_transforms.add_hash_column(df, definition, schema, columns)

    def filter_rows(self, df: pl.LazyFrame, condition: str) -> pl.LazyFrame:
        return polars_transforms.filter_rows(df, condition)

    def apply_watermark_filter(
        self,
        df: pl.LazyFrame,
        watermark_columns: List[str],
        watermark_start: Dict[str, Any],
        *,
        start_operator: str = ">",
        watermark_end: Optional[Dict[str, Any]] = None,
        end_operator: str = "<",
    ) -> pl.LazyFrame:
        actual = self._schema_names(df)
        resolved = []
        for col_name in watermark_columns:
            lower_val = watermark_start.get(col_name)
            upper_val = (watermark_end or {}).get(col_name)
            if lower_val is None and upper_val is None:
                continue
            resolved_col = self._resolve_column_name(actual, col_name)
            resolved.append((resolved_col, lower_val, upper_val))
        return polars_transforms.apply_watermark_filter(
            df,
            resolved,
            start_operator=start_operator,
            end_operator=end_operator,
        )

    def deduplicate(
        self,
        df: pl.LazyFrame,
        partition_columns: List[str],
        order_columns: Optional[List[str]] = None,
        order: str = "desc",
    ) -> pl.LazyFrame:
        actual = self._schema_names(df)
        resolved_partition = self._resolve_column_names(actual, partition_columns)
        resolved_order = (
            self._resolve_column_names(actual, order_columns) if order_columns else None
        )
        return polars_transforms.deduplicate(
            df, resolved_partition, resolved_order, order
        )

    def deduplicate_by_rank(
        self,
        df: pl.LazyFrame,
        partition_columns: List[str],
        order_columns: List[str],
        order: str = "desc",
    ) -> pl.LazyFrame:
        actual = self._schema_names(df)
        resolved_partition = self._resolve_column_names(actual, partition_columns)
        resolved_order = self._resolve_column_names(actual, order_columns)
        return polars_transforms.deduplicate_by_rank(
            df, resolved_partition, resolved_order, order
        )

    def cast_column(
        self,
        df: pl.LazyFrame,
        column_name: str,
        target_type: str,
        fmt: Optional[str] = None,
    ) -> pl.LazyFrame:
        # Collect schema once — reused for name resolution and dtype inspection.
        schema = df.collect_schema()
        actual_name = self._resolve_column_name(schema.names(), column_name)
        expr = build_cast_expr(actual_name, target_type, schema[actual_name], fmt)
        if expr is None:
            return df
        return df.with_columns(expr.alias(actual_name))

    # ==================================================================
    # System columns
    # ==================================================================

    def add_system_columns(
        self,
        df: pl.LazyFrame,
        author: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> pl.LazyFrame:
        return polars_transforms.add_system_columns(df, author, dataflow_run_id)

    def convert_timestamp_ntz_to_timestamp(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return polars_transforms.convert_timestamp_ntz_to_timestamp(df)

    def add_file_info_columns(
        self,
        df: pl.LazyFrame,
        file_infos: Optional[List[FileInfo]] = None,
    ) -> pl.LazyFrame:
        """Map file metadata onto rows using the embedded ``__file_path`` column.

        The scan readers (``read_parquet``, ``read_csv``, ``read_json``) embed
        the source path via ``include_file_paths`` / manual injection, so
        ``__file_path`` is already present in *df*.

        When *file_infos* is provided a small mapping LazyFrame is joined on
        ``__file_path`` to resolve ``__file_name`` and
        ``__file_modification_time``.  When *file_infos* is ``None`` the name
        is derived from the path and modification-time is set to ``null``.
        """
        return add_polars_file_info_columns(df, file_infos)

    # ==================================================================
    # Symlink manifest
    # ==================================================================

    def generate_symlink_manifest(self, path: str) -> None:
        """Generate a symlink manifest using delta-rs ``DeltaTable.generate()``."""
        delta_ops.generate_manifest(
            path,
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    # ==================================================================
    # Metrics
    # ==================================================================

    def count_rows(self, df: pl.LazyFrame) -> int:
        return polars_metrics.count_rows(df)

    def is_empty(self, df: pl.LazyFrame) -> bool:
        return polars_metrics.is_empty(df)

    def get_columns(self, df: pl.LazyFrame) -> List[str]:
        return polars_metrics.get_columns(df)

    def get_schema(self, df: pl.LazyFrame) -> Dict[str, str]:
        return polars_metrics.get_schema(df)

    def get_hive_schema(self, df: pl.LazyFrame) -> Dict[str, str]:
        """Return ``{column_name: hive_type}`` using native Polars dtype objects."""
        return polars_metrics.get_hive_schema(df)

    def get_max_values(self, df: pl.LazyFrame, columns: List[str]) -> Dict[str, Any]:
        resolved = self._resolve_column_names(self._schema_names(df), columns)
        return polars_metrics.get_max_values(df, resolved)

    def get_count_and_max_values(
        self,
        df: pl.LazyFrame,
        columns: List[str],
    ) -> Tuple[int, Dict[str, Any]]:
        resolved = self._resolve_column_names(self._schema_names(df), columns)
        return polars_metrics.get_count_and_max_values(df, resolved)

    # ==================================================================
    # Maintenance
    # ==================================================================

    def table_exists_by_path(self, path: str, *, fmt: str = "delta") -> bool:
        try:
            fmt_lower = fmt.lower()
            if fmt_lower == Format.DELTA.value:
                return delta_ops.table_exists(
                    path,
                    platform=self._platform,
                    storage_options=self._storage_options,
                    delta_table_cls=self.delta,
                )
            if fmt_lower == Format.ICEBERG.value:
                # Fast path: Iceberg tables always contain a metadata/ subdirectory.
                if self._platform is not None:
                    return self._platform.folder_exists(f"{path.rstrip('/')}/metadata")
                return False
            logger.warning(
                "PolarsEngine table_exists_by_path: unsupported format %s",
                fmt,
            )
            return False
        except Exception:  # noqa: BLE001
            return False

    def table_exists_by_name(self, table_name: str, *, fmt: str = "delta") -> bool:
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use table_exists_by_path(path) instead"
            )
        if fmt_lower == Format.ICEBERG.value:
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine.table_exists_by_name requires iceberg_catalog for Iceberg"
                )
            return iceberg_ops.table_exists(table_name, catalog=self._iceberg_catalog)
        raise EngineError(
            f"PolarsEngine.table_exists_by_name: unsupported format {fmt!r}"
        )

    def get_history_by_path(
        self,
        path: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        if fmt.lower() != Format.DELTA.value:
            return []
        _start, _end = polars_temporal.align_ms_boundaries(start_time, end_time)
        return delta_ops.history(
            path,
            limit,
            _start,
            _end,
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    def get_history_by_name(
        self,
        table_name: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use get_history_by_path(path) instead"
            )
        if fmt_lower == Format.ICEBERG.value:
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine.get_history_by_name requires iceberg_catalog for Iceberg"
                )
            _start, _end = polars_temporal.align_ms_boundaries(start_time, end_time)
            return iceberg_ops.history(
                table_name,
                limit,
                _start,
                _end,
                catalog=self._iceberg_catalog,
            )
        raise EngineError(
            f"PolarsEngine.get_history_by_name: unsupported format {fmt!r}"
        )

    def compact_by_path(
        self, path: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None
    ) -> None:
        if fmt.lower() != Format.DELTA.value:
            logger.warning(
                "PolarsEngine compact_by_path only supports Delta, got %s",
                fmt,
            )
            return
        delta_ops.compact(
            path,
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    def compact_by_name(
        self,
        table_name: str,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        fmt_lower = fmt.lower()
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use compact_by_path(path) instead"
            )
        if fmt_lower == Format.ICEBERG.value:
            # pyiceberg does not yet provide compaction APIs (rewrite_data_files,
            # rewrite_position_delete_files, rewrite_manifests). Log and no-op.
            logger.warning(
                "PolarsEngine compact_by_name: pyiceberg does not support compaction procedures; skipping"
            )
            return
        raise EngineError(f"PolarsEngine.compact_by_name: unsupported format {fmt!r}")

    def cleanup_by_path(
        self,
        path: str,
        retention_hours: int = 168,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        if fmt.lower() != Format.DELTA.value:
            logger.warning(
                "PolarsEngine cleanup_by_path only supports Delta, got %s",
                fmt,
            )
            return
        delta_ops.cleanup(
            path,
            retention_hours,
            storage_options=self._storage_options,
            delta_table_cls=self.delta,
        )

    def cleanup_by_name(
        self,
        table_name: str,
        retention_hours: int = 168,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        fmt_lower = fmt.lower()
        opts = options or {}
        if fmt_lower == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — use cleanup_by_path(path) instead"
            )
        if fmt_lower == Format.ICEBERG.value:
            if self._iceberg_catalog is None:
                raise EngineError(
                    "PolarsEngine.cleanup_by_name requires iceberg_catalog for Iceberg"
                )
            iceberg_ops.cleanup(
                table_name,
                retention_hours,
                opts,
                catalog=self._iceberg_catalog,
            )
            return
        raise EngineError(f"PolarsEngine.cleanup_by_name: unsupported format {fmt!r}")

    # ==================================================================
    # Navigation
    # ==================================================================
    # Polars overrides BaseEngine's dispatch methods with format-aware routing.
    # Delta  → path-based  (delta-rs has no catalog support)
    # Iceberg → table_name (pyiceberg is catalog-based)
    # Flat files (parquet, csv, json, …) → path
    # When both are supplied the format-preferred target wins.
    # When only the non-preferred target is supplied a helpful error is raised.

    def read(
        self,
        fmt: str,
        *,
        table_name: Optional[str] = None,
        path: Optional[str | list[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        # Iceberg catalog lookup (requires table_name; path falls through to read_path below).
        if fmt.lower() == Format.ICEBERG.value and table_name:
            return self.read_table(table_name, fmt=fmt, options=options)
        # Path-based dispatch — read_path handles all formats including Delta & Iceberg by path.
        if path:
            return self.read_path(path, fmt, options)
        if table_name and fmt.lower() == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        raise EngineError("read() requires table_name or path")

    def write(
        self,
        df: pl.LazyFrame,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                self.write_to_table(
                    df,
                    table_name,
                    mode=mode,
                    fmt=fmt,
                    partition_columns=partition_columns,
                    options=options,
                )
                return
            if path:
                raise EngineError(
                    "PolarsEngine Iceberg writes require table_name (catalog) — pass table_name instead of path"
                )
            raise EngineError("write() requires table_name or path")
        # Delta and flat-file formats → path-based
        if path:
            self.write_to_path(
                df,
                path,
                mode=mode,
                fmt=fmt,
                partition_columns=partition_columns,
                options=options,
            )
        elif table_name and fmt.lower() == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        elif table_name:
            self.write_to_table(
                df,
                table_name,
                mode=mode,
                fmt=fmt,
                partition_columns=partition_columns,
                options=options,
            )
        else:
            raise EngineError("write() requires table_name or path")

    def merge(
        self,
        df: pl.LazyFrame,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                self.merge_to_table(
                    df,
                    table_name,
                    merge_keys=merge_keys,
                    fmt=fmt,
                    partition_columns=partition_columns,
                    options=options,
                )
                return
            if path:
                raise EngineError(
                    "PolarsEngine Iceberg merges require table_name (catalog) — pass table_name instead of path"
                )
            raise EngineError("merge() requires table_name or path")
        # Delta → path-based
        if path:
            self.merge_to_path(
                df,
                path,
                merge_keys=merge_keys,
                fmt=fmt,
                partition_columns=partition_columns,
                options=options,
            )
        elif table_name:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        else:
            raise EngineError("merge() requires table_name or path")

    def merge_overwrite(
        self,
        df: pl.LazyFrame,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                self.merge_overwrite_to_table(
                    df,
                    table_name,
                    merge_keys=merge_keys,
                    fmt=fmt,
                    partition_columns=partition_columns,
                    options=options,
                )
                return
            if path:
                raise EngineError(
                    "PolarsEngine Iceberg merge_overwrite requires table_name (catalog) — pass table_name instead of path"
                )
            raise EngineError("merge_overwrite() requires table_name or path")
        # Delta → path-based
        if path:
            self.merge_overwrite_to_path(
                df,
                path,
                merge_keys=merge_keys,
                fmt=fmt,
                partition_columns=partition_columns,
                options=options,
            )
        elif table_name:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        else:
            raise EngineError("merge_overwrite() requires table_name or path")

    def scd2(
        self,
        df: pl.LazyFrame,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                self.scd2_to_table(
                    df,
                    table_name,
                    merge_keys=merge_keys,
                    fmt=fmt,
                    partition_columns=partition_columns,
                    options=options,
                )
                return
            if path:
                raise EngineError(
                    "PolarsEngine Iceberg scd2 requires table_name (catalog) — pass table_name instead of path"
                )
            raise EngineError("scd2() requires table_name or path")
        # Delta → path-based
        if path:
            self.scd2_to_path(
                df,
                path,
                merge_keys=merge_keys,
                fmt=fmt,
                partition_columns=partition_columns,
                options=options,
            )
        elif table_name:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        else:
            raise EngineError("scd2() requires table_name or path")

    def exists(
        self,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        fmt: str = "delta",
    ) -> bool:
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                return self.table_exists_by_name(table_name, fmt=fmt)
            if path:
                return self.table_exists_by_path(path, fmt=fmt)
            return False
        # Delta and other formats → path-based
        if path:
            return self.table_exists_by_path(path, fmt=fmt)
        if table_name and fmt.lower() == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        if table_name:
            return self.table_exists_by_name(table_name, fmt=fmt)
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
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                return self.get_history_by_name(
                    table_name, limit, start_time, end_time=end_time, fmt=fmt
                )
            if path:
                return self.get_history_by_path(
                    path, limit, start_time, end_time=end_time, fmt=fmt
                )
            raise EngineError("get_history() requires table_name or path")
        # Delta → path-based
        if path:
            return self.get_history_by_path(
                path, limit, start_time, end_time=end_time, fmt=fmt
            )
        if table_name:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        raise EngineError("get_history() requires table_name or path")

    def compact(
        self,
        *,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                self.compact_by_name(table_name, fmt=fmt, options=options)
                return
            if path:
                self.compact_by_path(path, fmt=fmt, options=options)
                return
            raise EngineError("compact() requires table_name or path")
        # Delta → path-based
        if path:
            self.compact_by_path(path, fmt=fmt, options=options)
        elif table_name:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
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
        if fmt.lower() == Format.ICEBERG.value:
            if table_name:
                self.cleanup_by_name(
                    table_name,
                    retention_hours=retention_hours,
                    fmt=fmt,
                    options=options,
                )
                return
            if path:
                self.cleanup_by_path(
                    path, retention_hours=retention_hours, fmt=fmt, options=options
                )
                return
            raise EngineError("cleanup() requires table_name or path")
        # Delta → path-based
        if path:
            self.cleanup_by_path(
                path, retention_hours=retention_hours, fmt=fmt, options=options
            )
        elif table_name:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        else:
            raise EngineError("cleanup() requires table_name or path")
