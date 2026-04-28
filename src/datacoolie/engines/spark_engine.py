"""PySpark + Delta Lake engine implementation.

:class:`SparkEngine` binds :class:`BaseEngine` to
``pyspark.sql.DataFrame`` and implements every abstract method using
the Spark DataFrame API and the ``delta-spark`` library.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from functools import reduce
from operator import and_

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as sf
from pyspark.sql import types as T

from datacoolie.core.constants import (
    DEFAULT_AUTHOR,
    DatabaseType,
    FileInfoColumn,
    SCD2Column,
    SystemColumn,
    MaintenanceType,
    LoadType,
    Format,
    TRAILING_COLUMNS,
)
from datacoolie.core.exceptions import EngineError
from datacoolie.platforms.base import BasePlatform
from datacoolie.engines.base import BaseEngine
from datacoolie.engines.spark_session_builder import (
    get_or_create_spark_session,
)
from datacoolie.logging.base import get_logger

logger = get_logger(__name__)

# SQL alias -> Spark native type.
# Native Spark types are listed explicitly (identity mapping) so that
# _resolve_type returns a non-None value for them and the cast is applied.
# Aliases that Spark's .cast() does NOT recognise natively are mapped to the
# closest native equivalent.
# NOTE: Spark's ``timestamp`` is timezone-aware (stores UTC internally).
# SQL types that carry no timezone info are mapped to ``timestamp_ntz`` to
# preserve their no-TZ semantics (Spark 3.4+ / Databricks).
_SPARK_TYPE_MAP: Dict[str, str] = {
    # ---- Spark native types (identity) ----
    # Listed in type-family order: string → boolean → integer → float →
    # double → decimal → date → timestamp → timestamp_ntz → interval →
    # void → binary
    "string": "string",
    "boolean": "boolean",
    "byte": "byte",
    "tinyint": "tinyint",
    "short": "short",
    "smallint": "smallint",
    "int": "int",
    "integer": "integer",
    "long": "long",
    "bigint": "bigint",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp_ntz",   # Spark 3.4+ / Databricks
    "interval": "interval",
    "void": "void",
    "binary": "binary",
    # ---- String / text aliases ----
    "str": "string",
    "varchar": "string",
    "varchar2": "string",         # Oracle
    "nvarchar": "string",
    "nvarchar2": "string",        # Oracle
    "char": "string",
    "nchar": "string",
    "character": "string",        # SQL standard
    "character varying": "string", # SQL standard
    "text": "string",
    "ntext": "string",            # SQL Server
    "tinytext": "string",         # MySQL
    "mediumtext": "string",       # MySQL
    "longtext": "string",         # MySQL
    "clob": "string",             # Oracle / DB2
    "nclob": "string",            # Oracle
    "enum": "string",             # MySQL
    "set": "string",              # MySQL
    "uuid": "string",             # PostgreSQL
    "uniqueidentifier": "string", # SQL Server
    "json": "string",             # PostgreSQL / MySQL
    "jsonb": "string",            # PostgreSQL
    "xml": "string",              # SQL Server / PostgreSQL
    "citext": "string",           # PostgreSQL
    # Time has no Spark native equivalent; store as string
    "time": "string",
    "timetz": "string",           # PostgreSQL
    "time with time zone": "string",
    "time without time zone": "string",
    # ---- Boolean ----
    "bool": "boolean",
    "bit": "boolean",             # SQL Server / MySQL
    "logical": "boolean",
    # ---- Integer (byte / tinyint) ----
    "byteint": "byte",            # Teradata
    "uint8": "tinyint",           # unsigned mapped to nearest signed
    # ---- Integer (smallint) ----
    "int2": "smallint",           # PostgreSQL
    "int16": "smallint",
    "smallserial": "smallint",    # PostgreSQL
    "uint16": "smallint",
    # ---- Integer (int) ----
    "int4": "int",                # PostgreSQL
    "int32": "int",
    "mediumint": "int",           # MySQL
    "serial": "int",              # PostgreSQL
    "uint32": "int",
    # ---- Integer (bigint) ----
    "int8": "bigint",             # PostgreSQL
    "int64": "bigint",
    "hugeint": "bigint",          # DuckDB (promote to bigint)
    "bigserial": "bigint",        # PostgreSQL
    "uint64": "bigint",
    "unsigned": "bigint",
    # ---- Float ----
    "real": "float",
    "float4": "float",            # PostgreSQL
    "float32": "float",
    # ---- Double ----
    "float8": "double",           # PostgreSQL
    "float64": "double",
    "double precision": "double", # SQL standard
    # ---- Decimal / exact numeric ----
    "numeric": "decimal",
    "dec": "decimal",             # SQL standard short form
    "number": "decimal",          # Oracle
    "money": "decimal",           # SQL Server / PostgreSQL
    "smallmoney": "decimal",      # SQL Server
    # ---- Timestamp with time zone → timestamp ----
    "timestamptz": "timestamp",              # PostgreSQL shorthand
    "timestamp_tz": "timestamp",
    "timestamp with time zone": "timestamp",
    "datetimeoffset": "timestamp",           # SQL Server (carries tz offset)
    # ---- Timestamp without time zone → timestamp_ntz ----
    "datetime": "timestamp_ntz",
    "datetime2": "timestamp_ntz",            # SQL Server (no TZ)
    "smalldatetime": "timestamp_ntz",        # SQL Server (no TZ)
    "timestamp without time zone": "timestamp_ntz",
    # ---- Binary ----
    "varbinary": "binary",
    "bytea": "binary",            # PostgreSQL
    "blob": "binary",             # MySQL / SQLite
    "tinyblob": "binary",         # MySQL
    "mediumblob": "binary",       # MySQL
    "longblob": "binary",         # MySQL
    "image": "binary",            # SQL Server (deprecated)
    "bytes": "binary",
    "raw": "binary",              # Oracle
    "long raw": "binary",         # Oracle (deprecated)
}
class SparkEngine(BaseEngine[DataFrame]):
    """PySpark implementation of :class:`BaseEngine`.

    Args:
        spark_session: Existing ``SparkSession`` (notebook environments).
            If ``None``, a fresh session with Delta extensions is created.
        config: Extra Spark configuration overrides applied on top of
            :data:`DEFAULT_SPARK_CONFIGS`.
        platform: Optional platform to attach to this engine immediately.
    """

    # ==================================================================
    # Construction
    # ==================================================================

    def __init__(
        self,
        spark_session: Optional[SparkSession] = None,
        config: Optional[Dict[str, str]] = None,
        platform: Optional[BasePlatform] = None,
    ) -> None:
        super().__init__(platform=platform)
        self._spark = get_or_create_spark_session(
            app_name="DataCoolie_SparkEngine",
            config=config,
            existing_session=spark_session,
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def spark(self) -> SparkSession:
        """Return the underlying ``SparkSession``."""
        return self._spark

    @property
    def spark_major_version(self) -> int:
        """Return the major version of the Spark runtime (e.g. ``3`` or ``4``)."""
        return int(self._spark.version.split(".")[0])

    @property
    def _supports_merge_into(self) -> bool:
        """Return ``True`` if the active Spark build exposes ``DataFrame.mergeInto``.

        Checked via ``hasattr`` so custom Databricks runtimes that back-ported
        the API below Spark 4.0 are handled correctly without version parsing.
        """
        return hasattr(DataFrame, "mergeInto")

    @staticmethod
    def _safe_cache(df: DataFrame) -> DataFrame:
        """Try to cache *df* and return it.

        On runtimes that do not support caching (e.g. Databricks Serverless)
        ``DataFrame.cache()`` raises an exception.  In that case the original
        DataFrame is returned uncached so callers continue without it.
        """
        try:
            return df.cache()
        except Exception:  # noqa: BLE001
            logger.debug("DataFrame.cache() is not supported on this runtime; continuing without caching.")
            return df

    @staticmethod
    def _safe_unpersist(df: DataFrame) -> None:
        """Unpersist *df*, ignoring errors (e.g. when caching was skipped)."""
        try:
            df.unpersist()
        except Exception:  # noqa: BLE001
            pass

    @property
    def _delta_table(self) -> type:
        """Lazily import and return the ``DeltaTable`` class."""
        from delta import DeltaTable  # noqa: PLC0415
        return DeltaTable

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_options(
        reader_or_writer: Any,
        options: Optional[Dict[str, Any]],
    ) -> Any:
        """Apply a dict of options to a Spark reader/writer."""
        if options:
            for key, value in options.items():
                reader_or_writer = reader_or_writer.option(key, value)
        return reader_or_writer

    # ==================================================================
    # Read
    # ==================================================================

    def read_parquet(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        merged = {"mergeSchema": "true", **(options or {})}
        base_path = merged.pop("use_hive_partitioning", None)
        if base_path:
            merged.setdefault("basePath", base_path)
        reader = self._apply_options(self._spark.read.format("parquet"), merged)
        df: DataFrame = reader.load(path)
        return df

    def read_delta(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        reader = self._apply_options(self._spark.read.format("delta"), options)
        df: DataFrame = reader.load(path)
        return df

    def read_iceberg(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        reader = self._apply_options(self._spark.read.format("iceberg"), options)
        df: DataFrame = reader.load(path)
        return df

    def read_csv(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        merged = {
            "header": "true", "quote": '"', "escape": '"', "escapeQuotes": "true", "multiLine": "true", 
            **(options or {})
        }
        base_path = merged.pop("use_hive_partitioning", None)
        if base_path:
            merged.setdefault("basePath", base_path)
        reader = self._apply_options(self._spark.read.format("csv"), merged)
        df: DataFrame = reader.load(path)
        return df

    def read_json(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        merged = {"multiLine": "true", **(options or {})}
        base_path = merged.pop("use_hive_partitioning", None)
        if base_path:
            merged.setdefault("basePath", base_path)
        reader = self._apply_options(self._spark.read.format("json"), merged)
        df: DataFrame = reader.load(path)
        return df

    def read_jsonl(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        merged = {"multiLine": "false", **(options or {})}
        base_path = merged.pop("use_hive_partitioning", None)
        if base_path:
            merged.setdefault("basePath", base_path)
        reader = self._apply_options(self._spark.read.format("json"), merged)
        df: DataFrame = reader.load(path)
        return df

    def read_avro(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        merged = {**(options or {})}
        base_path = merged.pop("use_hive_partitioning", None)
        if base_path:
            merged.setdefault("basePath", base_path)
        reader = self._apply_options(self._spark.read.format("avro"), merged)
        df: DataFrame = reader.load(path)
        return df

    def read_excel(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        import pandas as pd  # noqa: PLC0415

        merged: Dict[str, Any] = dict(options or {})
        merged.pop("use_hive_partitioning", None)

        # Translate Spark-style options to pandas.read_excel kwargs
        pd_kwargs: Dict[str, Any] = {}
        if merged.get("header", "true").lower() == "false":
            pd_kwargs["header"] = None
        if "sheet_name" in merged:
            pd_kwargs["sheet_name"] = merged["sheet_name"]

        ext = self.FORMAT_EXTENSIONS.get(Format.EXCEL.value, ".xlsx")
        resolved = self._resolve_file_paths(path, ext)
        if not resolved:
            raise FileNotFoundError(f"No Excel files found at: {path}")

        frames = []
        for p in resolved:
            pdf = pd.read_excel(p, **pd_kwargs)
            pdf[FileInfoColumn.FILE_PATH.value] = p
            frames.append(pdf)

        combined = pd.concat(frames, ignore_index=True)
        return self._spark.createDataFrame(combined)

    def read_database(
        self,
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        merged: Dict[str, Any] = dict(options or {})
        dbtable = f"({query.strip()}) q" if query else table
        merged["dbtable"] = dbtable

        # Build JDBC URL if not explicitly provided
        if "url" not in merged:
            merged["url"] = self._build_jdbc_url(merged)
        # Clean up framework keys that Spark JDBC doesn't understand
        for key in ("database_type", "host", "port", "database"):
            merged.pop(key, None)

        reader = self._apply_options(self._spark.read.format("jdbc"), merged)
        return reader.load()

    # ------------------------------------------------------------------
    # JDBC URL helpers
    # ------------------------------------------------------------------

    # Default JDBC drivers per database type
    _JDBC_DRIVERS: Dict[str, str] = {
        DatabaseType.MYSQL: "com.mysql.cj.jdbc.Driver",
        DatabaseType.MSSQL: "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        DatabaseType.POSTGRESQL: "org.postgresql.Driver",
        DatabaseType.ORACLE: "oracle.jdbc.OracleDriver",
        DatabaseType.SQLITE: "org.sqlite.JDBC",
    }

    @staticmethod
    def _build_jdbc_url(opts: Dict[str, Any]) -> str:
        """Build a JDBC URL from ``database_type``, ``host``, ``port``, ``database``.

        Raises:
            EngineError: If ``database_type`` is missing or the combination
                of host/port/database is insufficient.
        """
        db_type = opts.get("database_type")
        if not db_type:
            raise EngineError(
                "SparkEngine.read_database requires 'url' or 'database_type' in options"
            )
        host = opts.get("host", "localhost")
        port = opts.get("port")
        database = opts.get("database", "")

        if db_type == DatabaseType.MYSQL:
            port = port or 3306
            url = f"jdbc:mysql://{host}:{port}/{database}"
        elif db_type == DatabaseType.MSSQL:
            port = port or 1433
            url = f"jdbc:sqlserver://{host}:{port};databaseName={database}"
            # MSSQL JDBC 12.x defaults encrypt=true; embed driver-specific
            # props into the URL where the driver reliably reads them
            for _prop in SparkEngine.DRIVER_CONNECTION_KEYS:
                if _prop in opts:
                    url += f";{_prop}={opts.pop(_prop)}"
        elif db_type == DatabaseType.POSTGRESQL:
            port = port or 5432
            url = f"jdbc:postgresql://{host}:{port}/{database}"
        elif db_type == DatabaseType.ORACLE:
            port = port or 1521
            url = f"jdbc:oracle:thin:@{host}:{port}/{database}"
        elif db_type == DatabaseType.SQLITE:
            url = f"jdbc:sqlite:{database}"
        else:
            raise EngineError(f"SparkEngine: unsupported database_type {db_type!r}")

        # Set default driver if not already specified
        if "driver" not in opts:
            driver = SparkEngine._JDBC_DRIVERS.get(db_type)
            if driver:
                opts["driver"] = driver

        return url

    def create_dataframe(
        self,
        records: List[Dict[str, Any]],
    ) -> DataFrame:
        """Create a DataFrame from a list of heterogeneous dicts.

        Spark's ``createDataFrame`` infers a merged schema automatically
        when ``samplingRatio=1.0`` is used, filling missing fields with
        ``null``.  An explicit union of all keys is pre-computed so that
        every row dict contains all columns before passing to Spark,
        ensuring consistent schema inference even for small datasets.
        """
        if not records:
            return self._spark.createDataFrame([], schema="string")
        all_keys: list[str] = []
        seen: set[str] = set()
        for record in records:
            for k in record:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)
        normalised = [{k: row.get(k) for k in all_keys} for row in records]
        return self._spark.createDataFrame(normalised)

    def execute_sql(
        self,
        sql: str,
        parameters: Optional[Dict[Any, Any]] = None,
    ) -> DataFrame:
        if parameters:
            return self._spark.sql(sql, parameters)
        return self._spark.sql(sql)

    def read_table(
        self,
        table_name: str,
        fmt: str = "delta",
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        return self._spark.table(table_name)

    def read_path(
        self,
        path: str | list[str],
        fmt: str,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        reader = self._apply_options(self._spark.read.format(fmt), options)
        df: DataFrame = reader.load(path)
        return df

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def _add_file_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Add ``__file_*`` columns from Spark ``_metadata``."""
        return df.selectExpr(
            "*",
            f"_metadata.file_path as {FileInfoColumn.FILE_PATH.value}",
            f"_metadata.file_name as {FileInfoColumn.FILE_NAME.value}",
            f"_metadata.file_modification_time as {FileInfoColumn.FILE_MODIFICATION_TIME.value}",
        )

    # ==================================================================
    # Write
    # ==================================================================

    def write_to_path(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        fmt = Format.JSON.value if fmt.lower() == Format.JSONL.value else fmt
        writer = df.write.format(fmt).mode(mode)

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        # Sensible defaults per mode
        mode_opts: Dict[str, str] = {}
        if mode == LoadType.OVERWRITE.value:
            mode_opts["overwriteSchema"] = "true"
        else:
            mode_opts["mergeSchema"] = "true"
        if options:
            mode_opts.update(options)

        writer = self._apply_options(writer, mode_opts)
        writer.save(path)

    def write_to_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
        _skip_iceberg_evolution: bool = False,
    ) -> None:
        """Write via DataFrameWriterV2 (``df.writeTo``).

        Supported *mode* values:

        - ``LoadType.OVERWRITE`` / ``LoadType.FULL_LOAD`` — create or replace.
        - ``LoadType.APPEND`` — append rows.  When the target table does
          not yet exist, behaves as create-or-replace.

        Column matching between DataFrame and existing table is
        case-insensitive, consistent with Spark's default catalog resolver.
        """
        fmt = Format.JSON.value if fmt.lower() == Format.JSONL.value else fmt
        is_overwrite = mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
        is_append = mode in (LoadType.APPEND.value,)
        if not (is_overwrite or is_append):
            raise EngineError(f"Unsupported write_to_table mode: {mode!r}")

        # --- create-or-replace (overwrite, or first write of a new table) ---
        if is_overwrite or not self._spark.catalog.tableExists(table_name):
            writer = self._build_table_writer(df, table_name, fmt, overwrite=True, options=options)
            if partition_columns:
                writer = writer.partitionedBy(*[sf.col(c) for c in partition_columns])
            writer.createOrReplace()
            return

        # --- append path ---
        # Iceberg: metadata-only schema / partition evolution, then align
        # the DataFrame to the table schema (case-insensitive, typed-null
        # fill for missing columns).  Delta handles evolution natively via
        # the ``mergeSchema`` option.
        if fmt.lower() == Format.ICEBERG.value:
            if not _skip_iceberg_evolution:
                self._evolve_and_reorder_iceberg_schema(
                    df, table_name, partition_columns=partition_columns,
                )
            df = self._align_df_to_table_schema(df, table_name)

        writer = self._build_table_writer(df, table_name, fmt, overwrite=False, options=options)
        writer.append()

    def _build_table_writer(
        self,
        df: DataFrame,
        table_name: str,
        fmt: str,
        *,
        overwrite: bool,
        options: Optional[Dict[str, str]],
    ) -> Any:
        """Return a ``DataFrameWriterV2`` with default + user options applied."""
        mode_opts: Dict[str, str] = {
            "overwriteSchema" if overwrite else "mergeSchema": "true",
        }
        if options:
            mode_opts.update(options)
        writer = df.writeTo(table_name).using(fmt)
        for k, v in mode_opts.items():
            writer = writer.option(k, v)
        return writer

    def _align_df_to_table_schema(self, df: DataFrame, table_name: str) -> DataFrame:
        """Reorder DataFrame columns to match the target table schema.

        - Column matching is case-insensitive.
        - Columns absent from the DataFrame are filled with typed nulls.
        - Output column names use the table's casing.

        Required for Iceberg ``writeTo().append()`` which matches columns
        positionally and rejects DataFrames missing any table column,
        unlike pyiceberg which auto-fills absent columns.
        """
        table_fields = self._spark.table(table_name).schema.fields
        df_cols_ci = {c.lower(): c for c in df.columns}
        select_exprs = [
            sf.col(f"`{df_cols_ci[f.name.lower()]}`").alias(f.name)
            if f.name.lower() in df_cols_ci
            else sf.lit(None).cast(f.dataType).alias(f.name)
            for f in table_fields
        ]
        return df.select(*select_exprs)

    # ------------------------------------------------------------------
    # Iceberg schema / partition evolution (metadata-only DDL)
    # ------------------------------------------------------------------

    def _evolve_iceberg_schema(
        self,
        df: DataFrame,
        table_name: str,
        existing_col_names: Optional[List[str]] = None,
    ) -> List[T.StructField]:
        """Add new columns to an Iceberg table.

        Compares *df* columns against the existing table schema
        (case-insensitive) and runs ``ALTER TABLE ... ADD COLUMNS`` for
        any that are missing.  Metadata-only operation (no data I/O).

        Args:
            existing_col_names: Pre-fetched ordered list of existing
                table column names.  If ``None``, the schema is read
                from the catalog.

        Returns:
            Newly-added fields (empty when nothing changed).
        """
        if existing_col_names is None:
            existing_col_names = [f.name for f in self._spark.table(table_name).schema.fields]
        existing_lower = {c.lower() for c in existing_col_names}
        new_fields = [f for f in df.schema.fields if f.name.lower() not in existing_lower]
        if not new_fields:
            return []

        col_defs = ", ".join(
            f"`{f.name}` {f.dataType.simpleString()}" for f in new_fields
        )
        self._spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col_defs})")
        logger.info(
            "Evolved Iceberg schema for %s: added %s",
            table_name,
            [f.name for f in new_fields],
        )
        return new_fields

    def _reorder_iceberg_trailing_columns(
        self,
        table_name: str,
        current_columns: Optional[List[str]] = None,
    ) -> bool:
        """Reorder the Iceberg schema so ``TRAILING_COLUMNS`` sit at the end.

        Uses ``ALTER TABLE … ALTER COLUMN … AFTER …`` (metadata-only).
        Matching against ``TRAILING_COLUMNS`` is case-insensitive; the
        reorder preserves the table's actual column casing.

        Args:
            current_columns: Pre-fetched ordered column name list.
                If ``None``, the schema is read from the catalog.

        Returns:
            ``True`` if the schema was reordered.
        """
        if current_columns is None:
            current_columns = [f.name for f in self._spark.table(table_name).schema.fields]

        trailing_lower = {c.lower() for c in TRAILING_COLUMNS}
        col_by_lower = {c.lower(): c for c in current_columns}
        leading = [c for c in current_columns if c.lower() not in trailing_lower]
        trailing_present = [
            col_by_lower[c.lower()] for c in TRAILING_COLUMNS if c.lower() in col_by_lower
        ]

        if not trailing_present:
            return False

        expected = leading + trailing_present
        if current_columns == expected:
            return False

        prev = leading[-1] if leading else None
        for tc in trailing_present:
            if prev is None:
                self._spark.sql(
                    f"ALTER TABLE {table_name} ALTER COLUMN `{tc}` FIRST"
                )
            else:
                self._spark.sql(
                    f"ALTER TABLE {table_name} ALTER COLUMN `{tc}` AFTER `{prev}`"
                )
            prev = tc

        logger.info("Reordered trailing columns for %s", table_name)
        return True

    def _evolve_and_reorder_iceberg_schema(
        self,
        df: DataFrame,
        table_name: str,
        partition_columns: Optional[List[str]] = None,
    ) -> None:
        """Evolve the Iceberg schema and keep trailing columns at the end.

        Combines :meth:`_evolve_iceberg_schema` (add new columns),
        :meth:`_reorder_iceberg_trailing_columns` (move SCD2 / FileInfo /
        System columns to the tail), and optional partition spec
        evolution via :meth:`_ensure_iceberg_partition_spec`.  All
        operations are metadata-only.

        Mirrors :meth:`PolarsEngine._align_and_evolve_iceberg` minus the
        name-mapping refresh and casing alignment (Spark manages field-IDs
        natively and Iceberg-Spark handles casing via catalog config).
        """
        # Single schema fetch — shared by evolve + reorder.
        current_columns = [f.name for f in self._spark.table(table_name).schema.fields]

        new_fields = self._evolve_iceberg_schema(df, table_name, current_columns)
        if new_fields:
            # ALTER ADD COLUMNS appends at the tail; compute the post-
            # evolve order locally to avoid a second schema fetch.
            current_columns = current_columns + [f.name for f in new_fields]
            self._reorder_iceberg_trailing_columns(table_name, current_columns)

        if partition_columns:
            self._ensure_iceberg_partition_spec(table_name, partition_columns)

    def _ensure_iceberg_partition_spec(
        self,
        table_name: str,
        partition_columns: List[str],
    ) -> None:
        """Ensure the Iceberg table has identity partition fields.

        Uses ``ALTER TABLE … ADD PARTITION FIELD`` (metadata-only).
        Matching against existing partition fields is case-insensitive.
        """
        existing_lower = self._existing_iceberg_identity_partitions(table_name)
        for col in partition_columns:
            if col.lower() in existing_lower:
                continue
            self._spark.sql(
                f"ALTER TABLE {table_name} ADD PARTITION FIELD `{col}`"
            )
            logger.info(
                "Added identity partition field `%s` to %s", col, table_name,
            )

    def _existing_iceberg_identity_partitions(self, table_name: str) -> set[str]:
        """Return lowercase column names of existing identity partitions.

        Parses the ``# Partition Information`` section of
        ``DESCRIBE TABLE EXTENDED``.  Returns an empty set on any parse
        failure so callers fall through to an explicit ADD PARTITION
        FIELD, which surfaces the underlying error clearly.

        Expected DESCRIBE TABLE EXTENDED layout::

            | # Partition Information |  |  |
            | # col_name              |  |  |   <- sub-header, skip
            | order_year              |int|  |   <- partition column
            |                         |  |  |   <- blank row ends section
        """
        try:
            rows = self._spark.sql(
                f"DESCRIBE TABLE EXTENDED {table_name}"
            ).collect()
        except Exception:  # noqa: BLE001
            logger.debug(
                "Could not parse partition spec for %s; will attempt ADD PARTITION FIELD unconditionally",
                table_name,
            )
            return set()

        existing: set[str] = set()
        in_partitioning = False
        for row in rows:
            col_name = (row[0] or "").strip()
            if "# partition" in col_name.lower():
                in_partitioning = True
                continue
            if not in_partitioning:
                continue
            if col_name.startswith("#"):   # sub-header
                continue
            if not col_name:               # blank row ends the section
                break
            existing.add(col_name.lower())
        return existing

    # ==================================================================
    # Merge
    # ==================================================================

    def merge_to_path(
        self,
        df: DataFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if fmt.lower() != Format.DELTA.value:
            raise EngineError(f"merge_to_path only supports delta format, got {fmt!r}")

        merge_cond = " AND ".join(f"target.`{c}` = source.`{c}`" for c in merge_keys)

        exclude = set(merge_keys)
        exclude.add(SystemColumn.CREATED_AT)
        update_cols = [c for c in df.columns if c not in exclude]
        update_map = {f"`{c}`": f"source.`{c}`" for c in update_cols}
        insert_map = {f"`{c}`": f"source.`{c}`" for c in df.columns}

        try:
            (
                self._delta_table.forPath(self._spark, path).alias("target")
                .merge(df.alias("source"), merge_cond)
                .whenMatchedUpdate(set=update_map)
                .whenNotMatchedInsert(values=insert_map)
                .execute()
            )
        except Exception as exc:
            raise EngineError(
                f"Merge failed — target path does not exist or is not a {fmt} table: {path}"
            ) from exc

    def merge_overwrite_to_path(
        self,
        df: DataFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Rolling overwrite via DeltaTable MERGE DELETE + APPEND."""
        if fmt.lower() != Format.DELTA.value:
            raise EngineError(f"merge_overwrite_to_path only supports delta format, got {fmt!r}")

        df = self._safe_cache(df)  # Cache to avoid re-computation during delete + append
        try:
            keys_df = df.select(merge_keys).dropDuplicates()
            merge_cond = " AND ".join(f"target.`{c}` = source.`{c}`" for c in merge_keys)

            (
                self._delta_table.forPath(self._spark, path).alias("target")
                .merge(keys_df.alias("source"), merge_cond)
                .whenMatchedDelete()
                .execute()
            )

            self.write_to_path(
                df,
                path,
                mode=LoadType.APPEND.value,
                fmt=fmt,
                partition_columns=partition_columns,
                options=options,
            )
        finally:
            self._safe_unpersist(df)

    def merge_to_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge (upsert) into a named table.

        Uses the DataFrame ``mergeInto`` API on Spark >= 4.0, otherwise
        falls back to SQL MERGE.  For Iceberg, schema + partition spec
        are evolved up-front (metadata-only) so new columns are available
        before the MERGE runs.  MERGE uses explicit column names, so no
        DataFrame reorder is required.
        """
        self._maybe_evolve_iceberg(df, table_name, fmt, partition_columns)
        if self._supports_merge_into:
            self._merge_into_upsert(df, table_name, merge_keys)
        else:
            self._merge_sql_upsert(df, table_name, merge_keys)

    def merge_overwrite_to_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Merge-overwrite (delete + append) for a named table.

        Uses the DataFrame ``mergeInto`` API on Spark >= 4.0, otherwise
        falls back to SQL MERGE DELETE + WriterV2 append.  Iceberg
        schema evolution runs once up-front so both steps observe the
        updated schema.
        """
        iceberg_evolved = self._maybe_evolve_iceberg(df, table_name, fmt, partition_columns)

        df = self._safe_cache(df)  # reused by delete + append
        try:
            if self._supports_merge_into:
                self._merge_into_overwrite(
                    df, table_name, merge_keys, fmt=fmt,
                    partition_columns=partition_columns,
                    options=options,
                    _skip_iceberg_evolution=iceberg_evolved,
                )
            else:
                self._merge_sql_overwrite(
                    df, table_name, merge_keys, fmt=fmt,
                    partition_columns=partition_columns,
                    options=options,
                    _skip_iceberg_evolution=iceberg_evolved,
                )
        finally:
            self._safe_unpersist(df)

    # ------------------------------------------------------------------
    # SCD Type 2
    # ------------------------------------------------------------------

    def scd2_to_path(
        self,
        df: DataFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """SCD2 via two-step MERGE + APPEND on a path-based Delta table.

        Step 1: MERGE to close current rows where source is strictly newer.
        Step 2: APPEND all source rows as new versions.
        """
        if fmt.lower() != Format.DELTA.value:
            raise EngineError(f"scd2_to_path only supports Delta, got {fmt!r}")

        key_cond = " AND ".join(f"target.`{k}` = source.`{k}`" for k in merge_keys)
        merge_cond = (
            f"{key_cond}"
            f" AND target.`{SCD2Column.IS_CURRENT.value}` = true"
        )

        update_map = {
            f"`{SCD2Column.VALID_TO.value}`": f"source.`{SCD2Column.VALID_FROM.value}`",
            f"`{SCD2Column.IS_CURRENT.value}`": "false",
        }
        late_guard = (
            f"source.`{SCD2Column.VALID_FROM.value}` > "
            f"target.`{SCD2Column.VALID_FROM.value}`"
        )

        try:
            (
                self._delta_table.forPath(self._spark, path).alias("target")
                .merge(df.alias("source"), merge_cond)
                .whenMatchedUpdate(
                    condition=late_guard,
                    set=update_map,
                )
                .execute()
            )
        except Exception as exc:
            raise EngineError(
                f"SCD2 merge failed — target path does not exist or is not a Delta table: {path}"
            ) from exc

        # Step 2: APPEND all source rows as new versions
        self.write_to_path(
            df, path, mode=LoadType.APPEND.value, fmt=fmt,
            partition_columns=partition_columns, options=options,
        )

    def scd2_to_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """SCD2 via two-step MERGE + APPEND on a named table."""
        iceberg_evolved = self._maybe_evolve_iceberg(df, table_name, fmt, partition_columns)

        df = self._safe_cache(df)  # Used twice: MERGE step + APPEND step
        try:
            # Step 1: MERGE — close current rows where source is strictly newer
            if self._supports_merge_into:
                merge_cond_col, update_map_col, late_guard_col = (
                    self._scd2_merge_parts_col(df, table_name, merge_keys)
                )
                (
                    df.mergeInto(table_name, merge_cond_col)
                    .whenMatched(condition=late_guard_col)
                    .update(update_map_col)
                    .merge()
                )
            else:
                merge_cond, update_map, late_guard = self._scd2_merge_parts(merge_keys)
                update_clause = ", ".join(
                    f"target.{k} = {v}" for k, v in update_map.items()
                )
                sql = (
                    f"MERGE INTO {table_name} AS target "
                    f"USING {{source_df}} AS source "
                    f"ON {merge_cond} "
                    f"WHEN MATCHED AND {late_guard} THEN UPDATE SET {update_clause}"
                )
                self._spark.sql(sql, source_df=df)

            # Step 2: APPEND all source rows as new versions
            self.write_to_table(
                df, table_name, mode=LoadType.APPEND.value, fmt=fmt,
                partition_columns=partition_columns, options=options,
                _skip_iceberg_evolution=iceberg_evolved,
            )
        finally:
            self._safe_unpersist(df)

    @staticmethod
    def _scd2_merge_parts(merge_keys: List[str]) -> Tuple[str, Dict[str, str], str]:
        """Return ``(merge_cond, update_map, late_guard)`` for SCD2 close-step."""
        key_cond = SparkEngine._build_merge_condition(merge_keys)
        merge_cond = f"{key_cond} AND target.`{SCD2Column.IS_CURRENT.value}` = true"
        update_map = {
            f"`{SCD2Column.VALID_TO.value}`": f"source.`{SCD2Column.VALID_FROM.value}`",
            f"`{SCD2Column.IS_CURRENT.value}`": "false",
        }
        late_guard = (
            f"source.`{SCD2Column.VALID_FROM.value}` > "
            f"target.`{SCD2Column.VALID_FROM.value}`"
        )
        return merge_cond, update_map, late_guard

    # ------------------------------------------------------------------
    # Merge helpers (Spark 4.0 mergeInto / SQL fallback)
    # ------------------------------------------------------------------

    def _maybe_evolve_iceberg(
        self,
        df: DataFrame,
        table_name: str,
        fmt: str,
        partition_columns: Optional[List[str]],
    ) -> bool:
        """For Iceberg: evolve schema + partition spec (metadata-only).

        Returns ``True`` when evolution ran so downstream writers can
        skip their own evolution step via ``_skip_iceberg_evolution``.
        """
        if fmt.lower() != Format.ICEBERG.value:
            return False
        self._evolve_and_reorder_iceberg_schema(
            df, table_name, partition_columns=partition_columns,
        )
        return True

    @staticmethod
    def _build_merge_condition(merge_keys: List[str]) -> str:
        """Build a ``target.k = source.k AND …`` equi-join condition."""
        return " AND ".join(f"target.`{c}` = source.`{c}`" for c in merge_keys)

    @staticmethod
    def _build_merge_condition_col(
        df: DataFrame, table_name: str, merge_keys: List[str]
    ) -> Column:
        """Build a Column equi-join condition for DataFrame mergeInto (Spark 4.0+).

        Uses ``eqNullSafe`` so ``NULL`` keys match correctly.  Source columns
        are referenced via the calling DataFrame; target columns via the
        fully-qualified ``table_name`` to avoid AMBIGUOUS_REFERENCE errors.
        """
        parts = [df[k].eqNullSafe(sf.col(f"{table_name}.`{k}`")) for k in merge_keys]
        return reduce(and_, parts)

    @staticmethod
    def _scd2_merge_parts_col(
        df: DataFrame, table_name: str, merge_keys: List[str]
    ) -> Tuple[Column, Dict[str, Column], Column]:
        """Column-typed (merge_cond, update_map, late_guard) for SCD2 mergeInto."""
        key_cond = reduce(
            and_,
            [df[k].eqNullSafe(sf.col(f"{table_name}.`{k}`")) for k in merge_keys],
        )
        merge_cond: Column = key_cond & (
            sf.col(f"{table_name}.`{SCD2Column.IS_CURRENT.value}`") == sf.lit(True)
        )
        update_map: Dict[str, Column] = {
            f"`{SCD2Column.VALID_TO.value}`": df[SCD2Column.VALID_FROM.value],
            f"`{SCD2Column.IS_CURRENT.value}`": sf.lit(False),
        }
        late_guard: Column = (
            df[SCD2Column.VALID_FROM.value]
            > sf.col(f"{table_name}.`{SCD2Column.VALID_FROM.value}`")
        )
        return merge_cond, update_map, late_guard

    @staticmethod
    def _update_columns(df_columns: List[str], merge_keys: List[str]) -> List[str]:
        """Columns to update on MATCHED: all DataFrame columns except
        merge keys and ``created_at``.  Matching is case-insensitive."""
        exclude_lower = {c.lower() for c in merge_keys}
        exclude_lower.add(SystemColumn.CREATED_AT.lower())
        return [c for c in df_columns if c.lower() not in exclude_lower]

    def _merge_sql_upsert(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
    ) -> None:
        """SQL MERGE upsert (Spark < 4.0)."""
        merge_cond = self._build_merge_condition(merge_keys)
        update_cols = self._update_columns(df.columns, merge_keys)
        update_clause = ", ".join(f"target.`{c}` = source.`{c}`" for c in update_cols)
        insert_cols = ", ".join(f"`{c}`" for c in df.columns)
        insert_vals = ", ".join(f"source.`{c}`" for c in df.columns)

        sql = (
            f"MERGE INTO {table_name} AS target "
            f"USING {{source_df}} AS source "
            f"ON {merge_cond} "
            f"WHEN MATCHED THEN UPDATE SET {update_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        self._spark.sql(sql, source_df=df)

    def _merge_into_upsert(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
    ) -> None:
        """DataFrame mergeInto upsert (Spark >= 4.0)."""
        on = self._build_merge_condition_col(df, table_name, merge_keys)
        update_map: Dict[str, Column] = {
            c: df[c] for c in self._update_columns(df.columns, merge_keys)
        }
        (
            df.mergeInto(table_name, on)
            .whenMatched()
            .update(update_map)
            .whenNotMatched()
            .insertAll()
            .merge()
        )

    def _merge_sql_overwrite(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
        _skip_iceberg_evolution: bool = False,
    ) -> None:
        """SQL MERGE DELETE + WriterV2 append (Spark < 4.0)."""
        keys_df = df.select(merge_keys).dropDuplicates()
        merge_cond = self._build_merge_condition(merge_keys)

        sql = (
            f"MERGE INTO {table_name} AS target "
            f"USING {{keys_df}} AS source "
            f"ON {merge_cond} "
            f"WHEN MATCHED THEN DELETE"
        )
        self._spark.sql(sql, keys_df=keys_df)

        self.write_to_table(
            df, table_name, mode=LoadType.APPEND.value, fmt=fmt,
            partition_columns=partition_columns, options=options,
            _skip_iceberg_evolution=_skip_iceberg_evolution,
        )

    def _merge_into_overwrite(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
        _skip_iceberg_evolution: bool = False,
    ) -> None:
        """DataFrame mergeInto delete-then-insert (Spark >= 4.0)."""
        keys_df = df.select(merge_keys).dropDuplicates()
        on = self._build_merge_condition_col(keys_df, table_name, merge_keys)
        keys_df.mergeInto(table_name, on).whenMatched().delete().merge()

        self.write_to_table(
            df, table_name, mode=LoadType.APPEND.value, fmt=fmt,
            partition_columns=partition_columns, options=options,
            _skip_iceberg_evolution=_skip_iceberg_evolution,
        )

    # ==================================================================
    # Transform
    # ==================================================================

    def add_column(self, df: DataFrame, column_name: str, expression: str) -> DataFrame:
        return df.withColumn(column_name, sf.expr(expression))

    def drop_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        existing = [c for c in columns if c in df.columns]
        return df.drop(*existing) if existing else df

    def select_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        return df.select(*columns)

    def rename_column(self, df: DataFrame, old_name: str, new_name: str) -> DataFrame:
        return df.withColumnRenamed(old_name, new_name)

    def filter_rows(self, df: DataFrame, condition: str) -> DataFrame:
        return df.filter(condition)

    def apply_watermark_filter(
        self,
        df: DataFrame,
        watermark_columns: List[str],
        watermark: Dict[str, Any],
    ) -> DataFrame:
        combined = None
        for col_name in watermark_columns:
            value = watermark.get(col_name)
            if value is None:
                continue
            if isinstance(value, (datetime, date)):
                value = value.isoformat()
            condition = sf.col(col_name) > sf.lit(value)
            combined = condition if combined is None else (combined | condition)

        if combined is None:
            return df
        return df.filter(combined)

    def deduplicate(
        self,
        df: DataFrame,
        partition_columns: List[str],
        order_columns: Optional[List[str]] = None,
        order: str = "desc",
    ) -> DataFrame:
        _order = order_columns or partition_columns
        order_exprs = (
            [sf.col(c).asc() for c in _order]
            if order == "asc"
            else [sf.col(c).desc() for c in _order]
        )
        window = Window.partitionBy(partition_columns).orderBy(*order_exprs)
        return (
            df.withColumn("__row_number", sf.row_number().over(window))
            .filter(sf.col("__row_number") == 1)
            .drop("__row_number")
        )

    def deduplicate_by_rank(
        self,
        df: DataFrame,
        partition_columns: List[str],
        order_columns: List[str],
        order: str = "desc",
    ) -> DataFrame:
        order_exprs = (
            [sf.col(c).asc() for c in order_columns]
            if order == "asc"
            else [sf.col(c).desc() for c in order_columns]
        )
        window = Window.partitionBy(partition_columns).orderBy(*order_exprs)
        return (
            df.withColumn("__rank", sf.rank().over(window))
            .filter(sf.col("__rank") == 1)
            .drop("__rank")
        )

    def cast_column(
        self,
        df: DataFrame,
        column_name: str,
        target_type: str,
        fmt: Optional[str] = None,
    ) -> DataFrame:
        resolved = self._resolve_type(target_type)
        if resolved is None:
            return df
        lower = resolved.lower()
        if lower == "date" and fmt:
            return df.withColumn(column_name, sf.to_date(sf.col(column_name), fmt))
        if lower == "timestamp" and fmt:
            return df.withColumn(column_name, sf.to_timestamp(sf.col(column_name), fmt))
        return df.withColumn(column_name, sf.col(column_name).cast(resolved))

    @staticmethod
    def _resolve_type(target_type: str) -> Optional[str]:
        """Resolve SQL type aliases to Spark-native type names.

        Handles parameterised types such as ``NUMERIC(18,2)`` by splitting at
        ``(`` before the alias lookup, then re-attaching the parameter suffix.
        Returns ``None`` for unknown types — the caller should bypass the cast.
        """
        if "(" in target_type:
            base, params = target_type.split("(", 1)
            resolved_base = _SPARK_TYPE_MAP.get(base.lower())
            if resolved_base is None:
                logger.debug(
                    "SparkEngine.cast_column: unknown type %r — bypassing cast, column kept as-is",
                    target_type,
                )
                return None
            return f"{resolved_base}({params}"
        resolved = _SPARK_TYPE_MAP.get(target_type.lower())
        if resolved is not None:
            return resolved
        logger.debug(
            "SparkEngine.cast_column: unknown type %r — bypassing cast, column kept as-is",
            target_type,
        )
        return None

    # ==================================================================
    # System columns
    # ==================================================================

    def add_system_columns(self, df: DataFrame, author: Optional[str] = None) -> DataFrame:
        _author = author or DEFAULT_AUTHOR
        return (
            df.withColumn(
                SystemColumn.CREATED_AT,
                sf.from_utc_timestamp(sf.current_timestamp(), "UTC"),
            )
            .withColumn(
                SystemColumn.UPDATED_AT,
                sf.from_utc_timestamp(sf.current_timestamp(), "UTC"),
            )
            .withColumn(SystemColumn.UPDATED_BY, sf.lit(_author))
        )

    def add_file_info_columns(
        self,
        df: DataFrame,
        file_infos=None,
    ) -> DataFrame:
        path_col = FileInfoColumn.FILE_PATH.value
        name_col = FileInfoColumn.FILE_NAME.value
        mtime_col = FileInfoColumn.FILE_MODIFICATION_TIME.value

        # Excel: __file_path already embedded via pandas — broadcast join.
        if path_col in df.columns:
            if file_infos:
                from pyspark.sql import Row  # noqa: PLC0415
                mapping_rows = [
                    Row(**{path_col: fi.path, name_col: fi.name, mtime_col: fi.modification_time})
                    for fi in file_infos
                ]
                mapping_df = self._spark.createDataFrame(mapping_rows)
                return df.join(sf.broadcast(mapping_df), on=path_col, how="left")
            return (
                df.withColumn(name_col, sf.element_at(sf.split(sf.col(path_col), r"[/\\]"), -1))
                .withColumn(mtime_col, sf.lit(None).cast("timestamp"))
            )

        # All other file formats — use Spark _metadata pseudo-column.
        return self._add_file_metadata_columns(df)

    def convert_timestamp_ntz_to_timestamp(self, df: DataFrame) -> DataFrame:
        for col_name, dtype in df.dtypes:
            if dtype == "timestamp_ntz":
                # Cast to timestamp (Spark timestamp is always UTC-aware)
                df = df.withColumn(col_name, sf.col(col_name).cast("timestamp"))
        return df

    # ==================================================================
    # Metrics
    # ==================================================================


    def count_rows(self, df: DataFrame) -> int:
        return df.count()

    def is_empty(self, df: DataFrame) -> bool:
        return df.isEmpty()

    def get_columns(self, df: DataFrame) -> List[str]:
        return df.columns  # type: ignore[return-value]

    def get_schema(self, df: DataFrame) -> Dict[str, str]:
        return {f.name: str(f.dataType) for f in df.schema.fields}

    def get_hive_schema(self, df: DataFrame) -> Dict[str, str]:
        """Return ``{column_name: hive_type}`` using native PySpark type objects."""
        return {
            f.name: self._spark_type_to_hive(f.dataType)
            for f in df.schema.fields
        }

    @staticmethod
    def _spark_type_to_hive(dt: T.DataType) -> str:
        """Recursively convert a PySpark DataType to a Hive/Athena DDL string."""
        if isinstance(dt, T.LongType):
            return "BIGINT"
        if isinstance(dt, T.IntegerType):
            return "INT"
        if isinstance(dt, T.ShortType):
            return "SMALLINT"
        if isinstance(dt, (T.ByteType, T.NullType)):
            return "TINYINT"
        if isinstance(dt, T.FloatType):
            return "FLOAT"
        if isinstance(dt, T.DoubleType):
            return "DOUBLE"
        if isinstance(dt, T.BooleanType):
            return "BOOLEAN"
        if isinstance(dt, T.BinaryType):
            return "BINARY"
        if isinstance(dt, T.DateType):
            return "DATE"
        if isinstance(dt, (T.TimestampType, T.TimestampNTZType)):
            return "TIMESTAMP"
        if isinstance(dt, T.DecimalType):
            return f"DECIMAL({dt.precision},{dt.scale})"
        if isinstance(dt, T.StringType):
            return "STRING"
        if isinstance(dt, T.ArrayType):
            return f"ARRAY<{SparkEngine._spark_type_to_hive(dt.elementType)}>"
        if isinstance(dt, T.MapType):
            k = SparkEngine._spark_type_to_hive(dt.keyType)
            v = SparkEngine._spark_type_to_hive(dt.valueType)
            return f"MAP<{k},{v}>"
        if isinstance(dt, T.StructType):
            fields = [
                f"{f.name}:{SparkEngine._spark_type_to_hive(f.dataType)}"
                for f in dt.fields
            ]
            return f"STRUCT<{','.join(fields)}>"
        return "STRING"

    def get_max_values(self, df: DataFrame, columns: List[str]) -> Dict[str, Any]:
        agg_exprs = [sf.max(c).alias(c) for c in columns]
        row = df.agg(*agg_exprs).collect()[0]
        return row.asDict()

    def get_count_and_max_values(
        self,
        df: DataFrame,
        columns: List[str],
    ) -> Tuple[int, Dict[str, Any]]:
        agg_exprs = [sf.count(sf.lit(1)).alias("__row_count")]
        agg_exprs.extend(sf.max(c).alias(c) for c in columns)
        row = df.agg(*agg_exprs).collect()[0]
        d = row.asDict()
        count = d.pop("__row_count", 0)
        return count, d

    # ==================================================================
    # Maintenance
    # ==================================================================


    def table_exists_by_path(self, path: str, *, fmt: str = "delta") -> bool:
        try:
            if fmt.lower() == Format.ICEBERG.value:
                try:
                    if self._platform:
                        return self._platform.folder_exists(f"{path.rstrip('/')}/metadata")
                    self._spark.read.format(fmt).load(f"{path}#metadata_log_entries")
                    return True
                except Exception:  # noqa: BLE001
                    return False
            if fmt.lower() == Format.DELTA.value:
                try:
                    if self._platform:
                        return self._platform.folder_exists(f"{path.rstrip('/')}/_delta_log")
                    self._spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
                    return True
                except Exception:  # noqa: BLE001
                    return False
            return self._platform.folder_exists(f"{path}")
        except Exception:  # noqa: BLE001
            return False

    def table_exists_by_name(self, table_name: str, *, fmt: str = "delta") -> bool:
        try:
            return self._spark.catalog.tableExists(table_name)
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # History — format-specific helpers
    # ------------------------------------------------------------------

    def _get_delta_history(
        self,
        identifier: str,
        limit: int,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        *,
        is_path: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch Delta history via ``DESCRIBE HISTORY``."""
        try:
            if is_path:
                hist_df = self._delta_table.forPath(self._spark, identifier).history(limit)
            else:
                hist_df = self._delta_table.forName(self._spark, identifier).history(limit)
        except Exception:  # noqa: BLE001
            return []
        if start_time is not None:
            _start = start_time.replace(microsecond=(start_time.microsecond // 1000) * 1000)
            hist_df = hist_df.where(sf.col("timestamp") >= _start)
        if end_time is not None:
            ceil_us = ((end_time.microsecond + 999) // 1000) * 1000
            if ceil_us >= 1_000_000:
                _end = end_time.replace(microsecond=0) + timedelta(seconds=1)
            else:
                _end = end_time.replace(microsecond=ceil_us)
            hist_df = hist_df.where(sf.col("timestamp") <= _end)
        return [row.asDict() for row in hist_df.collect()]

    def _get_iceberg_history(
        self,
        identifier: str,
        limit: int,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        *,
        is_path: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch Iceberg history from the snapshots metadata table.

        For path-based access uses ``spark.read.format("iceberg").load()``
        with the ``#snapshots`` suffix.  For name-based access uses the
        SQL metadata table (``<table>.snapshots``).

        Schema matches :meth:`PolarsEngine.get_history_by_name`:
        ``snapshot_id``, ``parent_id``, ``timestamp``, ``operation``,
        ``manifest_list``, ``summary``.
        """
        try:
            if is_path:
                snap_df = self._spark.read.format("iceberg").load(f"{identifier}#snapshots")
            else:
                snap_df = self._spark.sql(f"SELECT * FROM {identifier}.snapshots")

            if start_time is not None:
                _start = start_time.replace(microsecond=(start_time.microsecond // 1000) * 1000)
                snap_df = snap_df.where(sf.col("committed_at") >= _start)
            if end_time is not None:
                ceil_us = ((end_time.microsecond + 999) // 1000) * 1000
                if ceil_us >= 1_000_000:
                    _end = end_time.replace(microsecond=0) + timedelta(seconds=1)
                else:
                    _end = end_time.replace(microsecond=ceil_us)
                snap_df = snap_df.where(sf.col("committed_at") <= _end)

            rows = (
                snap_df
                .select(
                    sf.col("snapshot_id"),
                    sf.col("parent_id"),
                    sf.col("committed_at").alias("timestamp"),
                    sf.col("operation"),
                    sf.col("manifest_list"),
                    sf.col("summary"),
                )
                .orderBy(sf.col("timestamp").desc())
                .limit(limit)
                .collect()
            )
            return [row.asDict() for row in rows]
        except Exception:  # noqa: BLE001
            return []

    # ------------------------------------------------------------------
    # History — public API
    # ------------------------------------------------------------------

    def get_history_by_path(
        self,
        path: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        if fmt.lower() == Format.ICEBERG.value:
            return self._get_iceberg_history(path, limit, start_time, end_time, is_path=True)
        elif fmt.lower() == Format.DELTA.value:
            return self._get_delta_history(path, limit, start_time, end_time, is_path=True)
        return []

    def get_history_by_name(
        self,
        table_name: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        if fmt.lower() == Format.ICEBERG.value:
            return self._get_iceberg_history(table_name, limit, start_time, end_time)
        elif fmt.lower() == Format.DELTA.value:
            return self._get_delta_history(table_name, limit, start_time, end_time)
        return []

    def compact_by_path(self, path: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None) -> None:
        if fmt.lower() != Format.DELTA.value:
            logger.warning(
                "compact_by_path: compaction by path is only supported for Delta, skipping %s", fmt
            )
            return
        # Use DeltaTable API rather than ``OPTIMIZE delta.`path```  SQL so
        # relative / filesystem paths resolve correctly regardless of
        # active catalog configuration.
        self._delta_table.forPath(self._spark, path).optimize().executeCompaction()

    def compact_by_name(self, table_name: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None) -> None:
        opts = options or {}
        if fmt.lower() == Format.ICEBERG.value:
            if opts.get("rewrite_data_files", True):
                self._iceberg_rewrite_data_files(table_name)
            if opts.get("rewrite_position_delete_files", True):
                self._iceberg_rewrite_position_delete_files(table_name)
            if opts.get("rewrite_manifests", True):
                self._iceberg_rewrite_manifests(table_name)
        elif fmt.lower() == Format.DELTA.value:
            self._spark.sql(f"OPTIMIZE {table_name}")
        else:
            logger.warning(
                "compact_by_name: compaction by name is only supported for Delta and Iceberg, skipping %s", fmt
            )

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
                "cleanup_by_path: cleanup by path is only supported for Delta, skipping %s", fmt
            )
            return
        # Use DeltaTable API rather than ``VACUUM delta.`path``` SQL so
        # relative / filesystem paths resolve correctly regardless of
        # active catalog configuration.
        self._delta_table.forPath(self._spark, path).vacuum(float(retention_hours))

    def cleanup_by_name(
        self,
        table_name: str,
        retention_hours: int = 168,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        opts = options or {}
        if fmt.lower() == Format.ICEBERG.value:
            if opts.get("expire_snapshots", True):
                self._iceberg_expire_snapshots(table_name, retention_hours)
            if opts.get("remove_orphan_files", True):
                self._iceberg_remove_orphan_files(table_name, retention_hours)
        elif fmt.lower() == Format.DELTA.value:
            self._spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        else:
            logger.warning(
                "cleanup_by_name: cleanup by name is only supported for Delta and Iceberg, skipping %s", fmt
            )

    # ------------------------------------------------------------------
    # Symlink manifest (Delta-only)
    # ------------------------------------------------------------------

    def generate_symlink_manifest(self, path: str) -> None:
        """Generate a symlink manifest using Spark's DeltaTable API."""
        logger.info("SparkEngine: generating symlink manifest for %s", path)
        dt = self._delta_table.forPath(self._spark, path)
        dt.generate("symlink_format_manifest")

    # ------------------------------------------------------------------
    # Iceberg maintenance helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_catalog(table_name: str) -> str:
        """Extract the catalog prefix from a fully qualified table name."""
        return table_name.split(".")[0]

    def _iceberg_rewrite_data_files(self, table_name: str) -> None:
        catalog = self._extract_catalog(table_name)
        self._spark.sql(
            f"CALL {catalog}.system.rewrite_data_files(table => '{table_name}')"
        )

    def _iceberg_rewrite_position_delete_files(self, table_name: str) -> None:
        catalog = self._extract_catalog(table_name)
        self._spark.sql(
            f"CALL {catalog}.system.rewrite_position_delete_files(table => '{table_name}')"
        )

    def _iceberg_rewrite_manifests(self, table_name: str) -> None:
        catalog = self._extract_catalog(table_name)
        self._spark.sql(
            f"CALL {catalog}.system.rewrite_manifests(table => '{table_name}')"
        )

    def _iceberg_expire_snapshots(self, table_name: str, retention_hours: int) -> None:
        ts = datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        catalog = self._extract_catalog(table_name)
        self._spark.sql(
            f"CALL {catalog}.system.expire_snapshots("
            f"table => '{table_name}', older_than => TIMESTAMP '{ts_str}')"
        )

    def _iceberg_remove_orphan_files(self, table_name: str, retention_hours: int) -> None:
        ts = datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        catalog = self._extract_catalog(table_name)
        self._spark.sql(
            f"CALL {catalog}.system.remove_orphan_files("
            f"table => '{table_name}', older_than => TIMESTAMP '{ts_str}')"
        )


