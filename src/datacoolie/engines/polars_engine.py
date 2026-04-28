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

import inspect
import io
import os
import re
from urllib.parse import quote as _url_quote, unquote as _url_unquote
from functools import reduce  # noqa: PLC0415
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import polars as pl

# Cached set of parameter names accepted by pl.scan_csv.
# Computed once at import time so runtime checks are O(1).
_SCAN_CSV_PARAMS: frozenset[str] = frozenset(inspect.signature(pl.scan_csv).parameters)

# fastexcel powers the Polars "calamine" Excel engine (Polars >= 1.0 default).
# Fall back to openpyxl when fastexcel is not installed.
try:
    import fastexcel as _fastexcel  # noqa: F401
    _FASTEXCEL_AVAILABLE: bool = True
except ImportError:
    _FASTEXCEL_AVAILABLE = False

# LazyFrame.sink_delta was added in Polars 0.20.x.
# Fall back to DataFrame.collect().write_delta on older installations.
_SINK_DELTA_AVAILABLE: bool = hasattr(pl.LazyFrame, "sink_delta")

from datacoolie.core.constants import (
    DEFAULT_AUTHOR,
    DatabaseType,
    FileInfoColumn,
    SCD2Column,
    SystemColumn,
    Format,
    LoadType,
    TRAILING_COLUMNS,
)
from datacoolie.core.exceptions import EngineError
from datacoolie.platforms.base import BasePlatform
from datacoolie.engines.base import BaseEngine, FileInfo
from datacoolie.logging.base import get_logger
from datacoolie.utils.path_utils import normalize_path

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Polars type-mapping for cast_column
# Keys cover engine-native types and all common SQL/database aliases an end-user
# might provide (SQL standard, PostgreSQL, MySQL, SQL Server, Oracle, Teradata,
# Snowflake, DuckDB, Spark, and Polars-native names).
# ---------------------------------------------------------------------------
_POLARS_TYPE_MAP: Dict[str, pl.DataType] = {
    # ---- String / text ----
    "string": pl.Utf8,
    "str": pl.Utf8,
    "utf8": pl.Utf8,
    "varchar": pl.Utf8,
    "varchar2": pl.Utf8,          # Oracle
    "nvarchar": pl.Utf8,
    "nvarchar2": pl.Utf8,         # Oracle
    "char": pl.Utf8,
    "nchar": pl.Utf8,
    "character": pl.Utf8,         # SQL standard
    "character varying": pl.Utf8, # SQL standard
    "text": pl.Utf8,
    "ntext": pl.Utf8,             # SQL Server
    "tinytext": pl.Utf8,          # MySQL
    "mediumtext": pl.Utf8,        # MySQL
    "longtext": pl.Utf8,          # MySQL
    "clob": pl.Utf8,              # Oracle / DB2
    "nclob": pl.Utf8,             # Oracle
    "enum": pl.Utf8,              # MySQL
    "set": pl.Utf8,               # MySQL
    "uuid": pl.Utf8,              # PostgreSQL
    "uniqueidentifier": pl.Utf8,  # SQL Server
    "json": pl.Utf8,              # PostgreSQL / MySQL
    "jsonb": pl.Utf8,             # PostgreSQL
    "xml": pl.Utf8,               # SQL Server / PostgreSQL
    "citext": pl.Utf8,            # PostgreSQL (case-insensitive text)
    # ---- Integer (signed) ----
    # Widths mirror SparkEngine._SPARK_TYPE_MAP so that a metadata alias
    # resolves to the same logical width across engines.  In particular
    # ``int``/``integer`` → 32-bit (SQL standard), ``long``/``bigint`` → 64-bit,
    # ``tinyint`` → 8-bit, ``smallint`` → 16-bit.
    "byte": pl.Int8,
    "short": pl.Int16,
    "int": pl.Int32,
    "integer": pl.Int32,
    "int2": pl.Int16,             # PostgreSQL
    "int4": pl.Int32,             # PostgreSQL
    "int8": pl.Int64,             # PostgreSQL
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "long": pl.Int64,
    "tinyint": pl.Int8,
    "smallint": pl.Int16,
    "mediumint": pl.Int32,        # MySQL
    "bigint": pl.Int64,
    "byteint": pl.Int8,           # Teradata
    "hugeint": pl.Int64,          # DuckDB (no Int128 in Polars; promote)
    "serial": pl.Int32,           # PostgreSQL auto-increment
    "smallserial": pl.Int16,      # PostgreSQL
    "bigserial": pl.Int64,        # PostgreSQL
    "number": pl.Decimal,         # Oracle (treated as decimal; see also below)
    # ---- Integer (unsigned) ----
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "unsigned": pl.UInt64,        # MySQL shorthand
    # ---- Float ----
    # Widths mirror SparkEngine: ``float``/``real`` → 32-bit,
    # ``double``/``float8``/``double precision`` → 64-bit.
    "float": pl.Float32,
    "real": pl.Float32,
    "float4": pl.Float32,         # PostgreSQL
    "float8": pl.Float64,         # PostgreSQL
    "float32": pl.Float32,
    "float64": pl.Float64,
    "double": pl.Float64,
    "double precision": pl.Float64,  # SQL standard
    # ---- Decimal / exact numeric ----
    "decimal": pl.Decimal,
    "numeric": pl.Decimal,
    "dec": pl.Decimal,            # SQL standard short form
    "money": pl.Decimal,          # SQL Server / PostgreSQL
    "smallmoney": pl.Decimal,     # SQL Server
    # ---- Boolean ----
    "boolean": pl.Boolean,
    "bool": pl.Boolean,
    "bit": pl.Boolean,            # SQL Server / MySQL
    "logical": pl.Boolean,        # some systems
    # ---- Date ----
    "date": pl.Date,
    "date32": pl.Date,            # Arrow / Polars internal
    # ---- Time ----
    "time": pl.Time,
    "timetz": pl.Time,            # PostgreSQL
    "time with time zone": pl.Time,
    "time without time zone": pl.Time,
    # ---- Timestamp with time zone → Datetime("us", "UTC") ----
    # Mirrors Spark: timestamp is TZ-aware (stores UTC internally)
    "timestamp": pl.Datetime("us", "UTC"),
    "timestamptz": pl.Datetime("us", "UTC"),          # PostgreSQL shorthand
    "timestamp_tz": pl.Datetime("us", "UTC"),
    "timestamp with time zone": pl.Datetime("us", "UTC"),
    "datetimeoffset": pl.Datetime("us", "UTC"),        # SQL Server (carries tz offset)
    # ---- Timestamp without time zone → Datetime("us") ----
    # Mirrors Spark: no-TZ aliases map to timestamp_ntz semantics
    "timestamp_ntz": pl.Datetime("us"),               # Snowflake / Spark
    "datetime": pl.Datetime("us"),
    "datetime2": pl.Datetime("us"),                    # SQL Server (no TZ)
    "smalldatetime": pl.Datetime("us"),               # SQL Server (no TZ)
    "timestamp without time zone": pl.Datetime("us"),
    # ---- Duration / interval ----
    "interval": pl.Duration,      # SQL standard
    "duration": pl.Duration,      # Arrow / Polars
    # ---- Binary ----
    "binary": pl.Binary,
    "varbinary": pl.Binary,
    "bytea": pl.Binary,           # PostgreSQL
    "blob": pl.Binary,            # MySQL / SQLite
    "tinyblob": pl.Binary,        # MySQL
    "mediumblob": pl.Binary,      # MySQL
    "longblob": pl.Binary,        # MySQL
    "image": pl.Binary,           # SQL Server (deprecated)
    "bytes": pl.Binary,
    "raw": pl.Binary,             # Oracle
    "long raw": pl.Binary,        # Oracle (deprecated)
}

_DECIMAL_RE = re.compile(r"^(?:decimal|numeric|dec|number)\((\d+),\s*(\d+)\)$")

# Matches pure-numeric segments (e.g. "2026", "04") and hive key=value
# segments (e.g. "year=2026", "region=US-East") that are appended to a
# path when writing date- or hive-partitioned data.
_PARTITION_SEGMENT_RE: re.Pattern[str] = re.compile(r"^\d+$|^[^=]+=.+$")


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
    _DEFAULT_BYTES_PER_FILE: int = 256 * 1024 * 1024  # 256 MB

    # delta-rs does not auto-create checkpoints; replicate Spark's default interval.
    _DELTA_CHECKPOINT_INTERVAL: int = 10

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
        **kwargs: Any,
    ) -> None:
        super().__init__(platform=platform)
        self._storage_options = storage_options or {}
        self._iceberg_catalog = iceberg_catalog
        self._sql_context = sql_context if sql_context is not None else pl.SQLContext()

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

    def register_delta_tables(
        self,
        base_path: str,
        *,
        prefix: str = "",
    ) -> List[str]:
        """Discover Delta tables under *base_path* and register them.

        Each immediate sub-directory that is a valid Delta table is
        registered with its folder name as the table name.

        Requires :attr:`platform` to be set.

        Args:
            base_path: Root directory to scan for Delta tables.
            prefix: Optional prefix prepended to every registered table name
                (e.g. ``"db1__"`` → ``"db1__orders"``).  Useful when
                registering tables from multiple databases into the same
                SQLContext to avoid name collisions.

        Returns:
            List of registered table names (including any prefix).

        Raises:
            EngineError: If no platform is attached.
        """
        if self._platform is None:
            raise EngineError(
                "register_delta_tables requires a platform — call set_platform() first"
            )
        registered: List[str] = []
        try:
            child_paths = self._platform.list_folders(base_path)
        except Exception:  # noqa: BLE001
            return registered

        for child in sorted(child_paths):
            stripped = normalize_path(child)
            entry = prefix + stripped.rsplit("/", 1)[-1]
            if not self._platform.folder_exists(stripped + "/_delta_log"):
                continue
            try:
                lf = self.read_delta(child)
            except Exception:  # noqa: BLE001
                continue
            self._sql_context.register(entry, lf)
            registered.append(entry)
        return registered

    def register_iceberg_tables(
        self,
        namespace: Optional[str] = None,
        *,
        base_path: Optional[str] = None,
        prefix: str = "",
    ) -> List[str]:
        """Discover Iceberg tables and register them in the SQLContext.

        Discovery strategy (evaluated in order):

        1. **Catalog + namespace** — if *namespace* is given and
           ``iceberg_catalog`` is configured, list tables from the catalog
           (preferred).
        2. **Path scan** — if *base_path* is given, scan immediate
           sub-directories for an Iceberg ``metadata/`` directory and
           register each valid table.

        At least one of *namespace* or *base_path* must be provided.

        Args:
            namespace: Catalog namespace to list tables from.
            base_path: Filesystem path whose sub-directories are scanned
                for Iceberg tables (fallback when no catalog is configured).
            prefix: Optional prefix prepended to every registered table name
                (e.g. ``"cat1__"`` → ``"cat1__orders"``).  Useful when
                registering tables from multiple catalogs/namespaces into
                the same SQLContext to avoid name collisions.

        Returns:
            List of registered table names (including any prefix).
        """
        if namespace is None and base_path is None:
            raise EngineError(
                "register_iceberg_tables: provide namespace (catalog) or base_path"
            )

        # --- catalog-based discovery (preferred) ---
        if namespace is not None and self._iceberg_catalog is not None:
            registered: List[str] = []
            scan_kwargs: Dict[str, Any] = {}
            if self._storage_options:
                scan_kwargs["storage_options"] = self._storage_options
            tables = self._iceberg_catalog.list_tables(namespace)
            for table_id in tables:
                tbl_name = prefix + (table_id[-1] if isinstance(table_id, (list, tuple)) else str(table_id))
                ice_table = self._iceberg_catalog.load_table(table_id)
                lf = pl.scan_iceberg(ice_table, **scan_kwargs)
                self._sql_context.register(tbl_name, lf)
                registered.append(tbl_name)
            return registered

        # --- path-based discovery ---
        if base_path is None:
            raise EngineError(
                "register_iceberg_tables: iceberg_catalog is not configured "
                "— provide base_path for path-based discovery or call set_iceberg_catalog()"
            )
        if self._platform is None:
            raise EngineError(
                "register_iceberg_tables with base_path requires a platform — call set_platform() first"
            )

        registered = []
        try:
            child_paths_ice = self._platform.list_folders(base_path)
        except Exception:  # noqa: BLE001
            return registered

        for child in sorted(child_paths_ice):
            # Iceberg tables always contain a metadata/ subdirectory.
            metadata_dir = normalize_path(child) + "/metadata"
            if not self._platform.folder_exists(metadata_dir):
                continue
            try:
                lf = self.read_iceberg(child)
            except Exception:  # noqa: BLE001
                continue
            tbl_name = prefix + normalize_path(child).rsplit("/", 1)[-1]
            self._sql_context.register(tbl_name, lf)
            registered.append(tbl_name)
        return registered

    # ==================================================================
    # Read
    # ==================================================================

    def read_parquet(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        merged: Dict[str, Any] = dict(options or {})
        if merged.pop("use_hive_partitioning", None):
            merged["hive_partitioning"] = True
        merged.setdefault("missing_columns", "insert")
        merged.setdefault("extra_columns", "ignore")
        if self._storage_options:
            merged["storage_options"] = self._storage_options
        return pl.scan_parquet(
            path,
            include_file_paths=FileInfoColumn.FILE_PATH,
            **merged,
        )

    def read_delta(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        merged: Dict[str, Any] = dict(options or {})
        if self._storage_options:
            merged["storage_options"] = self._storage_options
        return pl.scan_delta(path, **merged)

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
        merged: Dict[str, Any] = dict(options or {})
        merged.pop("use_hive_partitioning", None)  # scan_csv has no hive_partitioning support
        # Normalise framework-level aliases to Polars kwarg names
        if "sep" in merged:
            merged.setdefault("separator", merged.pop("sep"))
        else:
            merged.setdefault("separator", ",")
        if "header" in merged:
            val = merged.pop("header")
            merged.setdefault("has_header", str(val).lower() == "true")
        else:
            merged.setdefault("has_header", True)
        if "quote" in merged:
            merged.setdefault("quote_char", merged.pop("quote"))
        else:
            merged.setdefault("quote_char", '"')
        if "inferSchema" in merged:
            val = merged.pop("inferSchema")
            merged.setdefault("infer_schema", str(val).lower() == "true")
        else:
            merged.setdefault("infer_schema", True)
        if "missing_columns" in _SCAN_CSV_PARAMS:
            merged.setdefault("missing_columns", "insert")
        if self._storage_options:
            merged["storage_options"] = self._storage_options
        return pl.scan_csv(
            path,
            include_file_paths=FileInfoColumn.FILE_PATH,
            **merged,
        )

    def read_json(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        # Polars has no scan_json — read eagerly then convert to lazy.
        # Inject __file_path manually so add_file_info_columns can map metadata.
        merged: Dict[str, Any] = dict(options or {})
        merged.pop("use_hive_partitioning", None)
        path_col = FileInfoColumn.FILE_PATH

        ext = self.FORMAT_EXTENSIONS.get(Format.JSON.value, f".{Format.JSON.value}")
        resolved = self._resolve_file_paths(path, ext)
        if not resolved:
            raise FileNotFoundError(f"No JSON files found at: {path}")
        # pl.read_json has no storage_options support — fetch bytes via the
        # platform so any backend (local, S3, ADLS, DBFS) works uniformly.
        frames = [
            pl.read_json(io.BytesIO(self.platform.read_bytes(f))).with_columns(
                pl.lit(f).alias(path_col)
            )
            for f in resolved
        ]
        return pl.concat(frames).lazy()

    def read_jsonl(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        merged: Dict[str, Any] = dict(options or {})
        merged.pop("use_hive_partitioning", None)
        if self._storage_options:
            merged["storage_options"] = self._storage_options
        return pl.scan_ndjson(
            path,
            include_file_paths=FileInfoColumn.FILE_PATH,
            **merged,
        )

    def read_avro(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        # Polars has no scan_avro — read eagerly then convert to lazy.
        # Inject __file_path manually so add_file_info_columns can map metadata.
        merged: Dict[str, Any] = dict(options or {})
        merged.pop("use_hive_partitioning", None)
        path_col = FileInfoColumn.FILE_PATH

        ext = self.FORMAT_EXTENSIONS.get(Format.AVRO.value, f".{Format.AVRO.value}")
        resolved = self._resolve_file_paths(path, ext)
        if not resolved:
            raise FileNotFoundError(f"No Avro files found at: {path}")
        # pl.read_avro has no storage_options support — fetch bytes via the
        # platform so any backend (local, S3, ADLS, DBFS) works uniformly.
        frames = [
            pl.read_avro(io.BytesIO(self.platform.read_bytes(f)), **merged).with_columns(
                pl.lit(f).alias(path_col)
            )
            for f in resolved
        ]
        return pl.concat(frames).lazy()

    def read_excel(
        self,
        path: str | list[str],
        options: Optional[Dict[str, str]] = None,
    ) -> pl.LazyFrame:
        # Polars has no scan_excel — read eagerly then convert to lazy.
        # Inject __file_path manually so add_file_info_columns can map metadata.
        merged: Dict[str, Any] = dict(options or {})
        merged.pop("use_hive_partitioning", None)
        path_col = FileInfoColumn.FILE_PATH

        ext = self.FORMAT_EXTENSIONS.get(Format.EXCEL.value, ".xlsx")
        resolved = self._resolve_file_paths(path, ext)
        if not resolved:
            raise FileNotFoundError(f"No Excel files found at: {path}")
        # pl.read_excel has no storage_options support — fetch bytes via the
        # platform so any backend (local, S3, ADLS, DBFS) works uniformly.
        # Default to calamine (fastexcel); fall back to openpyxl if not installed.
        merged.setdefault("engine", "calamine" if _FASTEXCEL_AVAILABLE else "openpyxl")
        frames = []
        for f in resolved:
            result = pl.read_excel(io.BytesIO(self.platform.read_bytes(f)), **merged)
            if isinstance(result, dict):
                # Multi-sheet workbook: concat all sheets into a single frame.
                frame = pl.concat(result.values())
            else:
                frame = result
            frames.append(frame.with_columns(pl.lit(f).alias(path_col)))
        return pl.concat(frames).lazy()

    def read_database(
        self,
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> pl.LazyFrame:
        merged: Dict[str, Any] = dict(options or {})
        db_type = merged.get("database_type", "")
        connection_uri = merged.pop("url", None)
        if connection_uri is None:
            connection_uri = self._build_connection_string(merged)
        # Clean up framework keys that Polars doesn't need
        for key in ("database_type", "host", "port", "database", "user", "password", "driver"):
            merged.pop(key, None)
        # Remove driver-specific options not understood by connectorx
        for key in self.DRIVER_CONNECTION_KEYS:
            merged.pop(key, None)
        sql = query if query else f"SELECT * FROM {table}"

        # Oracle: use oracledb thin mode (no Oracle Instant Client needed)
        if db_type in (DatabaseType.ORACLE, "oracle"):
            return self._read_database_oracle(sql, connection_uri, **merged)

        return pl.read_database_uri(sql, connection_uri, **merged).lazy()

    @staticmethod
    def _read_database_oracle(sql: str, connection_uri: str, **kwargs: Any) -> pl.LazyFrame:
        """Read from Oracle using ``oracledb`` in thin mode.

        connectorx requires Oracle Instant Client which is often unavailable.
        This method uses ``oracledb`` (thin mode, pure-Python) via
        ``pl.read_database`` instead.
        """
        try:
            import oracledb  # noqa: PLC0415
        except ImportError as exc:
            raise EngineError(
                "oracledb package is required for Oracle reads — pip install oracledb"
            ) from exc

        # Parse oracle://user:pass@host:port/service from the connection URI
        m = re.match(
            r"oracle://(?:([^:@]+)(?::([^@]*))?@)?([^:/]+):(\d+)/(.+)",
            connection_uri,
        )
        if not m:
            raise EngineError(f"Cannot parse Oracle connection URI: {connection_uri!r}")

        user, password, host, port, service = m.groups()
        conn = oracledb.connect(
            user=_url_unquote(user) if user else user,
            password=_url_unquote(password) if password else password,
            dsn=f"{host}:{port}/{service}",
        )
        try:
            return pl.read_database(sql, connection=conn, **kwargs).lazy()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Connection string helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_connection_string(opts: Dict[str, Any]) -> str:
        """Build a connection string from ``database_type``, ``user``, ``password``,
        ``host``, ``port``, ``database``.

        Generates URIs compatible with connectorx / SQLAlchemy
        (e.g. ``postgresql://user:pass@host:5432/db``).

        Raises:
            EngineError: If ``database_type`` is missing.
        """
        db_type = opts.get("database_type")
        if not db_type:
            raise EngineError(
                "PolarsEngine.read_database requires 'url' or 'database_type' in options"
            )
        user = opts.get("user", "")
        password = opts.get("password", "")
        host = opts.get("host", "localhost")
        port = opts.get("port")
        database = opts.get("database", "")

        if db_type == DatabaseType.MYSQL:
            port = port or 3306
            scheme = "mysql"
        elif db_type == DatabaseType.MSSQL:
            port = port or 1433
            scheme = "mssql"
        elif db_type == DatabaseType.POSTGRESQL:
            port = port or 5432
            scheme = "postgresql"
        elif db_type == DatabaseType.ORACLE:
            port = port or 1521
            scheme = "oracle"
        elif db_type == DatabaseType.SQLITE:
            # connectorx requires an absolute path; resolve relative paths
            if not os.path.isabs(database):
                database = os.path.abspath(database)
            return f"sqlite:///{database}"
        else:
            raise EngineError(f"PolarsEngine: unsupported database_type {db_type!r}")

        auth = f"{_url_quote(user, safe='')}:{_url_quote(password, safe='')}@" if user else ""
        return f"{scheme}://{auth}{host}:{port}/{database}"

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
        return self._sql_context.execute(sql)

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
            pyice_id = self._pyiceberg_table_id(table_name)
            ice_table = self._iceberg_catalog.load_table(pyice_id)
            scan_kwargs: Dict[str, Any] = {}
            if self._storage_options:
                scan_kwargs["storage_options"] = self._storage_options
            return pl.scan_iceberg(ice_table, **scan_kwargs)
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
            partition_columns = self._resolve_column_names(self._schema_names(df), partition_columns)

        if fmt == Format.DELTA.value:
            self._write_delta_path(df, path, mode, partition_columns, merged)
        elif fmt in (Format.PARQUET.value, Format.CSV.value, Format.JSONL.value):
            self._write_flat_sink(df, path, mode, fmt, partition_columns, merged)
        elif fmt in (Format.JSON.value, Format.AVRO.value):
            self._write_flat_eager(df, path, mode, fmt, merged)
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
                partition_columns = self._resolve_column_names(self._schema_names(df), partition_columns)
            self._write_iceberg_table(df, table_name, mode, partition_columns=partition_columns)
        else:
            raise EngineError(f"PolarsEngine.write_to_table: unsupported format {fmt!r}")

    # ------------------------------------------------------------------
    # Delta write helpers
    # ------------------------------------------------------------------

    def _write_delta_path(
        self,
        df: pl.LazyFrame,
        path: str,
        mode: str,
        partition_columns: Optional[List[str]],
        options: Dict[str, Any],
    ) -> None:
        if mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value):
            delta_mode = "overwrite"
            schema_mode = "overwrite"
        elif mode in (LoadType.APPEND.value,):
            delta_mode = "append"
            schema_mode = "merge"
        else:
            raise EngineError(f"PolarsEngine._write_delta_path: unsupported mode {mode!r}")

        write_opts: Dict[str, Any] = {}
        if self._storage_options:
            write_opts["storage_options"] = self._storage_options
        delta_write_options: Dict[str, Any] = dict(options)
        if partition_columns:
            delta_write_options["partition_by"] = partition_columns
        delta_write_options.setdefault("schema_mode", schema_mode)
        write_opts["delta_write_options"] = delta_write_options
        self._sink_or_write_delta(df, path, mode=delta_mode, **write_opts)
        self._delta_post_commit(path)

    def _delta_post_commit(self, path: str) -> None:
        """Run post-commit maintenance matching Spark's automatic behaviour.

        delta-rs does not auto-create checkpoints, compact the log, or
        clean up expired metadata the way Spark does.  This helper
        replicates that behaviour:

        1. **Checkpoint** — created every ``_DELTA_CHECKPOINT_INTERVAL``
           commits (default 10, same as Spark's ``delta.checkpointInterval``).
        2. **Cleanup metadata** — removes log files older than
           ``delta.logRetentionDuration`` (30 days by default in delta-rs).
        """
        try:
            dt = self.delta(path, storage_options=self._storage_options or None)
            version = dt.version()
            if version % self._DELTA_CHECKPOINT_INTERVAL == 0 and version > 0:
                dt.create_checkpoint()
            dt.cleanup_metadata()
        except Exception:  # noqa: BLE001
            logger.debug(
                "Delta post-commit maintenance skipped for %s", path, exc_info=True,
            )

    def _sink_or_write_delta(self, df: pl.LazyFrame, path: str, **kwargs: Any) -> Any:
        """Dispatch to ``sink_delta`` (streaming) or ``collect + write_delta`` (eager fallback).

        ``LazyFrame.sink_delta`` was introduced in Polars 0.20.x.  On older
        installations ``DataFrame.write_delta`` is used instead — it accepts
        the same keyword arguments (``mode``, ``storage_options``,
        ``delta_write_options``, ``delta_merge_options``) and, for
        ``mode="merge"``, returns the same ``TableMerger`` object so caller
        builder chains work unchanged.
        """
        if _SINK_DELTA_AVAILABLE:
            return df.sink_delta(path, **kwargs)
        return df.collect().write_delta(path, **kwargs)

    # ------------------------------------------------------------------
    # Flat-file writes (streaming via sink_)
    # ------------------------------------------------------------------

    def _write_flat_sink(
        self,
        df: pl.LazyFrame,
        path: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]],
        options: Dict[str, Any],
    ) -> None:
        """Write parquet / csv / jsonl using streaming ``sink_*``.

        When *partition_columns* are provided the target is a
        :class:`polars.PartitionBy` so that data is split into
        sub-directories by the given columns.  Otherwise files are
        written directly into the folder at *path*.
        """
        is_overwrite = mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
        if is_overwrite and self._platform is not None:
            try:
                self._platform.delete_folder(path, recursive=True)
            except Exception:  # noqa: BLE001
                pass  # folder may not exist on first write

        sink_opts: Dict[str, Any] = dict(options)
        if self._storage_options:
            sink_opts["storage_options"] = self._storage_options
        sink_opts.setdefault("mkdir", True)

        if partition_columns:
            target: str | pl.PartitionBy = pl.PartitionBy(path, key=partition_columns, approximate_bytes_per_file=self._DEFAULT_BYTES_PER_FILE)
        else:
            file_name = self._make_file_name(path, fmt, is_overwrite)
            target = normalize_path(path) + "/" + file_name

        if fmt == Format.PARQUET.value:
            df.sink_parquet(target, **sink_opts)
        elif fmt == Format.CSV.value:
            df.sink_csv(target, **sink_opts)
        elif fmt == Format.JSONL.value:
            df.sink_ndjson(target, **sink_opts)
        else:
            raise EngineError(f"PolarsEngine._write_flat_sink: unsupported format {fmt!r}")

    # ------------------------------------------------------------------
    # Flat-file writes (eager fallback for json / avro)
    # ------------------------------------------------------------------

    def _write_flat_eager(
        self,
        df: pl.LazyFrame,
        path: str,
        mode: str,
        fmt: str,
        options: Dict[str, Any],
    ) -> None:
        """Write json / avro — no ``sink_`` available, collect first."""
        is_overwrite = mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
        if is_overwrite and self._platform is not None:
            try:
                self._platform.delete_folder(path, recursive=True)
            except Exception:  # noqa: BLE001
                pass  # folder may not exist on first write

        file_name = self._make_file_name(path, fmt, is_overwrite)
        file_path = normalize_path(path) + "/" + file_name
        self._platform.create_folder(path)

        write_opts: Dict[str, Any] = dict(options)
        if fmt == Format.AVRO.value:
            # Polars cannot write TZ-aware timestamps to Avro ("not yet implemented"),
            # and TZ-naive timestamps produce "local-timestamp-micros" which DuckDB
            # and many other readers do not support.  Cast all Datetime columns to
            # UTF-8 ISO strings so the output is universally readable.
            dt_casts = [
                pl.col(name).cast(pl.String)
                for name, dtype in df.collect_schema().items()
                if isinstance(dtype, pl.Datetime)
            ]
            if dt_casts:
                df = df.with_columns(dt_casts)
            # Polars defaults to name="" which is invalid per the Avro spec.
            # Use the folder name so external tools can open the file.
            write_opts.setdefault("name", normalize_path(path).rsplit("/", 1)[-1])
        collected = df.collect()
        # write_json / write_avro have no storage_options support — serialise
        # to bytes in memory, then push via the platform so any backend
        # (local, S3, ADLS, DBFS) works uniformly.
        buf = io.BytesIO()
        if fmt == Format.JSON.value:
            collected.write_json(buf)
        elif fmt == Format.AVRO.value:
            collected.write_avro(buf, **write_opts)
        self._platform.write_bytes(file_path, buf.getvalue(), overwrite=True)

    # ------------------------------------------------------------------
    # Delta merge helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_delta_merge_options(
        merge_keys: List[str],
        options: Optional[Dict[str, str]] = None,
    ) -> Tuple[Dict[str, Any], str, str]:
        """Build ``delta_merge_options`` with source/target aliases and a key predicate.

        Centralises the alias + predicate construction used by
        ``merge_to_path``, ``merge_overwrite_to_path`` and ``scd2_to_path``.

        Returns ``(options_dict, src_alias, tgt_alias)``.  Callers that
        need extra predicate clauses (e.g. SCD2's ``is_current = true``)
        append them to ``options_dict["predicate"]`` using the returned
        aliases.
        """
        merged: Dict[str, Any] = dict(options or {})
        merged.setdefault("source_alias", "source")
        merged.setdefault("target_alias", "target")
        src_alias = merged["source_alias"]
        tgt_alias = merged["target_alias"]
        merged["predicate"] = " AND ".join(
            f"{tgt_alias}.`{c}` = {src_alias}.`{c}`" for c in merge_keys
        )
        return merged, src_alias, tgt_alias

    @staticmethod
    def _raise_if_delta_target_missing(exc: Exception, path: str, op: str) -> None:
        """Re-raise *exc* as EngineError when it indicates a missing Delta target."""
        name = type(exc).__name__
        msg = str(exc).lower()
        if "NotFound" in name or "not found" in msg or "does not exist" in msg:
            raise EngineError(
                f"{op} failed — target path does not exist or is not a Delta table: {path}"
            ) from exc

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------


    @staticmethod
    def _make_file_name(path: str, fmt: str, is_overwrite: bool) -> str:
        """Build the output file name.

        When *path* contains trailing date or hive-partition segments the
        logical folder name is recovered by stripping those segments first:

        * ``…/orders_dest/2026/04/09``       → folder ``orders_dest``
        * ``…/orders_dest_02/year=2026/m=04``   → folder ``orders_dest_02``

        Otherwise the deepest directory component is used as-is.

        * overwrite → ``{folder_name}.{ext}``
        * append    → ``{folder_name}_{yyyyMMdd_HHmmss}.{ext}``
        """
        parts = [p for p in normalize_path(path).split("/") if p]
        # Strip trailing partition segments, keeping at least 1 part (the logical folder name).
        while len(parts) > 1 and _PARTITION_SEGMENT_RE.match(parts[-1]):
            parts.pop()
        folder_name = parts[-1] if parts else "data"
        if is_overwrite:
            return f"{folder_name}.{fmt}"
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"{folder_name}_{ts}.{fmt}"

    # ------------------------------------------------------------------
    # Iceberg write helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pyiceberg_table_id(table_name: str) -> str:
        """Strip backticks and drop the catalog prefix for pyiceberg.

        ``full_table_name`` returns e.g. `` `catalog`.`ns`.`table` ``.
        pyiceberg expects ``ns.table`` (the catalog is implicit in the
        Catalog instance).
        """
        parts = [p.strip().strip("`") for p in table_name.split(".")]
        # 3+ parts → first part is the catalog prefix, drop it
        if len(parts) >= 3:
            return ".".join(parts[1:])
        # 2-part → namespace.table (already correct)
        if len(parts) == 2:
            return f"{parts[0]}.{parts[1]}"
        return parts[0]

    @staticmethod
    def _reorder_arrow_to_iceberg(arrow_table: Any, ice_table: Any) -> Any:
        """Align Arrow table columns to the Iceberg table schema.

        pyiceberg's ``_check_schema_compatible`` performs a positional
        name check against the Iceberg schema — it does not auto-fill
        missing columns.  This helper does the minimal work needed:

        1. **Fast path**: when ``arrow_table.column_names`` already match
           the Iceberg schema order, return *arrow_table* unchanged.
        2. Append a typed NULL array for each Iceberg column missing from
           the source (using ``pa.nulls`` with the Iceberg field's type).
        3. ``select`` the Iceberg column order, which also drops extras.

        No type casting is performed on existing columns — pyiceberg
        handles safe promotions (e.g. int→long, float→double, timestamp
        unit widening) internally via ``_cast_if_needed``, and raises
        ``ValueError`` for incompatible types so the user can fix them in
        the transform step.
        """
        import pyarrow as pa  # noqa: PLC0415

        target_schema = ice_table.schema().as_arrow()
        target_names = target_schema.names

        # Fast path: no reorder, no backfill, no drop needed.
        if arrow_table.column_names == target_names:
            return arrow_table

        # Case-insensitive lookup of source columns.
        # Built once here and reused for both missing-detection and casing rename.
        arrow_col_lower = {n.lower(): n for n in arrow_table.column_names}

        # 1) Backfill missing Iceberg columns with typed NULLs.
        #    "Missing" means no case-insensitive match in the source.
        missing = [f for f in target_schema if f.name.lower() not in arrow_col_lower]
        if missing:
            num_rows = arrow_table.num_rows
            for field in missing:
                # append_column accepts a pa.Field; the column is added at the
                # end with field.name (Iceberg-canonical casing).
                arrow_table = arrow_table.append_column(
                    field, pa.nulls(num_rows, type=field.type),
                )
            # Refresh the lookup so the rename step below includes new columns.
            arrow_col_lower = {n.lower(): n for n in arrow_table.column_names}

        # 2) Rename any case-mismatched columns to Iceberg-canonical names so
        #    that select(target_names) works with exact positional matching.
        #    This makes the function self-sufficient even when called without
        #    a prior _align_arrow_to_iceberg_casing pass.
        ice_name_by_lower = {n.lower(): n for n in target_names}
        renamed = [ice_name_by_lower.get(c.lower(), c) for c in arrow_table.column_names]
        if renamed != arrow_table.column_names:
            arrow_table = arrow_table.rename_columns(renamed)

        # 3) Reorder to Iceberg order and drop any source-only extras.
        return arrow_table.select(target_names)

    @staticmethod
    def _align_arrow_to_iceberg_casing(arrow_table: Any, ice_table: Any) -> Any:
        """Rename Arrow columns to match existing Iceberg columns case-insensitively.

        When an Arrow column matches an Iceberg column by lowercase name but
        with different casing (e.g. ``Name`` vs ``name``), rename the Arrow
        column to the Iceberg casing.  This prevents
        :meth:`_evolve_iceberg_schema` from treating case-variants as new
        columns, and lets :meth:`_reorder_arrow_to_iceberg` keep its exact
        positional matching.

        No-op when every Arrow column already has an exact match or no
        case-insensitive match in the Iceberg schema.
        """
        ice_names = [f.name for f in ice_table.schema().fields]
        ice_exact = set(ice_names)
        ice_by_lower = {n.lower(): n for n in ice_names}

        renames: Dict[str, str] = {}
        for arrow_name in arrow_table.column_names:
            if arrow_name in ice_exact:
                continue
            ice_match = ice_by_lower.get(arrow_name.lower())
            if ice_match is not None:
                renames[arrow_name] = ice_match

        if not renames:
            return arrow_table
        new_names = [renames.get(n, n) for n in arrow_table.column_names]
        return arrow_table.rename_columns(new_names)

    def _write_iceberg_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        mode: str,
        partition_columns: Optional[List[str]] = None,
    ) -> None:
        """Write a LazyFrame to an Iceberg table via pyiceberg (Arrow path)."""
        if self._iceberg_catalog is None:
            raise EngineError(
                "PolarsEngine._write_iceberg_table requires iceberg_catalog"
            )
        from pyiceberg.exceptions import (  # noqa: PLC0415
            NoSuchTableError,
            TableAlreadyExistsError,
        )

        pyice_id = self._pyiceberg_table_id(table_name)

        # Single materialisation: LazyFrame → Polars → Arrow once.
        arrow_table = df.collect().to_arrow()

        table_created = False
        try:
            ice_table = self._iceberg_catalog.load_table(pyice_id)
        except NoSuchTableError:
            # Table genuinely absent (HTTP 404) — create with Arrow schema.
            ns = pyice_id.rsplit(".", 1)[0] if "." in pyice_id else "default"
            self._iceberg_catalog.create_namespace_if_not_exists(ns)
            try:
                ice_table = self._iceberg_catalog.create_table(pyice_id, schema=arrow_table.schema)
                table_created = True
            except TableAlreadyExistsError:
                # Race condition: another writer created the table between our
                # load_table and create_table calls.  Load it normally.
                ice_table = self._iceberg_catalog.load_table(pyice_id)

        mode_lower = mode.lower()
        if mode_lower in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value):
            iceberg_mode = "overwrite"
        elif mode_lower == LoadType.APPEND.value:
            iceberg_mode = "append"
        else:
            raise EngineError(f"PolarsEngine: unsupported Iceberg write mode {mode!r}")

        # 1) Align casing + evolve schema + refresh name mapping.  New columns
        #    must exist before being referenced by a partition spec.
        if not table_created:
            ice_table, arrow_table = self._align_and_evolve_iceberg(
                ice_table, arrow_table, pyice_id,
            )
        elif "schema.name-mapping.default" not in (ice_table.properties or {}):
            # New table — set initial name mapping.
            ice_table = self._refresh_iceberg_name_mapping(ice_table, pyice_id)

        # 2) Partition spec evolution — after schema so new columns are present.
        if partition_columns:
            ice_table = self._ensure_iceberg_partition_spec(
                ice_table, partition_columns, pyice_id,
            )

        # 3) Write via pyiceberg Arrow API — works for both partitioned and
        #    unpartitioned tables, and correctly handles timestamptz columns.
        collected = self._reorder_arrow_to_iceberg(arrow_table, ice_table)
        if iceberg_mode == "overwrite":
            ice_table.overwrite(collected)
        elif iceberg_mode == "append":
            ice_table.append(collected)
        else:
            raise EngineError(f"PolarsEngine: unsupported Iceberg write mode {mode!r}")

    def _refresh_iceberg_name_mapping(self, ice_table: Any, pyice_id: str) -> Any:
        """Set ``schema.name-mapping.default`` and return the reloaded table.

        Single source of truth for Iceberg name-mapping updates — called
        after table creation and after schema evolution / reorder.
        """
        from pyiceberg.table.name_mapping import create_mapping_from_schema  # noqa: PLC0415

        nm = create_mapping_from_schema(ice_table.schema())
        with ice_table.transaction() as txn:
            txn.set_properties({"schema.name-mapping.default": nm.model_dump_json()})
        return self._iceberg_catalog.load_table(pyice_id)

    def _align_and_evolve_iceberg(
        self, ice_table: Any, arrow_table: Any, pyice_id: str,
    ) -> Tuple[Any, Any]:
        """Align Arrow casing to Iceberg, evolve schema, reorder, refresh mapping.

        Single seam for the Iceberg pre-write flow:

        1. Rename Arrow columns whose lowercase name matches an existing
           Iceberg column with different casing (e.g. ``Name`` vs ``name``)
           so schema evolution doesn't add case-variants and
           :meth:`_reorder_arrow_to_iceberg` keeps exact positional matching.
        2. ``union_by_name`` any truly-new columns into the Iceberg schema.
        3. Reorder trailing columns (SCD2 → FileInfo → System) to canonical
           order, mirroring ``ColumnNameSanitizer(90)``.
        4. Refresh ``schema.name-mapping.default`` only when the schema
           actually changed.

        Returns:
            Tuple of the (possibly reloaded) ``ice_table`` and the
            (possibly renamed) ``arrow_table``.
        """
        # 1) Align source casing to existing Iceberg columns.
        arrow_table = self._align_arrow_to_iceberg_casing(arrow_table, ice_table)

        # 2) Evolve schema with aligned names.
        schema_evolved = self._evolve_iceberg_schema(ice_table, arrow_table.schema)
        if schema_evolved:
            # Reload once after evolve so the reorder sees the new columns.
            ice_table = self._iceberg_catalog.load_table(pyice_id)

        # 3) Reorder trailing columns in the Iceberg schema.
        reordered = self._reorder_iceberg_trailing_columns(ice_table)

        # 4) Refresh name-mapping only when the schema actually changed.
        if schema_evolved or reordered:
            if reordered:
                # Reorder committed a new schema version; reload before mapping.
                ice_table = self._iceberg_catalog.load_table(pyice_id)
            ice_table = self._refresh_iceberg_name_mapping(ice_table, pyice_id)
        return ice_table, arrow_table

    @staticmethod
    def _evolve_iceberg_schema(ice_table: Any, arrow_schema: Any) -> bool:
        """Union new source columns into the Iceberg table schema (mergeSchema).

        Accepts a ``pa.Schema`` (Arrow) derived from the materialized data
        and uses ``union_by_name`` which natively accepts ``pa.Schema``,
        eliminating custom Polars→Iceberg type mapping.

        Returns:
            True if new columns were added, False if the schema was unchanged.
        """
        import pyarrow as pa  # noqa: PLC0415

        target_col_names = {f.name for f in ice_table.schema().fields}
        new_fields = [f for f in arrow_schema if f.name not in target_col_names]
        if not new_fields:
            return False

        with ice_table.update_schema() as update:
            update.union_by_name(pa.schema(new_fields))
        return True

    @staticmethod
    def _reorder_iceberg_trailing_columns(ice_table: Any) -> bool:
        """Reorder the Iceberg schema so ``TRAILING_COLUMNS`` are at the end.

        Uses pyiceberg ``move_after`` to keep the Iceberg table schema
        in the same canonical order that :class:`ColumnNameSanitizer`
        produces, preventing positional mismatches on subsequent writes.

        Returns True if the schema was reordered, False if already correct.
        """
        current = [f.name for f in ice_table.schema().fields]
        trailing_set = set(TRAILING_COLUMNS)
        current_set = set(current)
        leading = [c for c in current if c not in trailing_set]
        trailing_present = [c for c in TRAILING_COLUMNS if c in current_set]

        if not trailing_present:
            return False

        expected = leading + trailing_present
        if current == expected:
            return False

        with ice_table.update_schema() as update:
            prev = leading[-1] if leading else None
            for tc in trailing_present:
                if prev is None:
                    update.move_first(tc)
                else:
                    update.move_after(tc, prev)
                prev = tc
        return True

    def _ensure_iceberg_partition_spec(
        self,
        ice_table: Any,
        partition_columns: List[str],
        pyice_id: str,
    ) -> Any:
        """Ensure the Iceberg table has identity partition fields for the given columns.

        Uses ``update_spec().add_identity()`` which handles field-ID
        assignment automatically (pyiceberg best practice).  Only adds
        fields that are not already present in the current spec, making
        this safe to call on every write (idempotent).

        Returns the (possibly reloaded) ``ice_table``.
        """
        from pyiceberg.transforms import IdentityTransform  # noqa: PLC0415

        spec = ice_table.spec()
        schema = ice_table.schema()

        # Build a set of source column names that already have an identity partition.
        existing_identity_names: set[str] = set()
        for pf in spec.fields:
            if isinstance(pf.transform, IdentityTransform):
                try:
                    existing_identity_names.add(schema.find_field(pf.source_id).name)
                except Exception:  # noqa: BLE001
                    pass

        missing = [c for c in partition_columns if c not in existing_identity_names]
        if not missing:
            return ice_table

        with ice_table.update_spec() as update_spec:
            for col in missing:
                update_spec.add_identity(col)

        return self._iceberg_catalog.load_table(pyice_id)

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
            raise EngineError(f"PolarsEngine merge_to_path only supports Delta, got {fmt!r}")

        actual = self._schema_names(df)
        merge_keys = self._resolve_column_names(actual, merge_keys)
        if partition_columns:
            partition_columns = self._resolve_column_names(actual, partition_columns)

        delta_merge_options, src_alias, _ = self._build_delta_merge_options(
            merge_keys, options,
        )

        write_opts: Dict[str, Any] = {}
        if self._storage_options:
            write_opts["storage_options"] = self._storage_options

        exclude = set(merge_keys) | {SystemColumn.CREATED_AT}
        update_cols = {
            c: f"{src_alias}.`{c}`" for c in actual if c not in exclude
        }

        try:
            (
                self._sink_or_write_delta(
                    df,
                    path,
                    mode="merge",
                    delta_merge_options=delta_merge_options,
                    **write_opts,
                )
                .when_matched_update(updates=update_cols)
                .when_not_matched_insert_all()
                .execute()
            )
        except Exception as exc:
            self._raise_if_delta_target_missing(exc, path, "Merge")
            raise
        self._delta_post_commit(path)

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
            raise EngineError(f"PolarsEngine merge_overwrite_to_path only supports Delta, got {fmt!r}")

        actual = self._schema_names(df)
        merge_keys = self._resolve_column_names(actual, merge_keys)
        if partition_columns:
            partition_columns = self._resolve_column_names(actual, partition_columns)

        # Step 1: use only distinct merge keys to DELETE matched rows.
        keys_df = df.select(merge_keys).unique()

        delta_merge_options, _, _ = self._build_delta_merge_options(
            merge_keys, options,
        )

        write_opts: Dict[str, Any] = {}
        if self._storage_options:
            write_opts["storage_options"] = self._storage_options

        try:
            (
                self._sink_or_write_delta(
                    keys_df,
                    path,
                    mode="merge",
                    delta_merge_options=delta_merge_options,
                    **write_opts,
                )
                .when_matched_delete()
                .execute()
            )
        except Exception as exc:
            self._raise_if_delta_target_missing(exc, path, "Merge")
            raise

        # Step 2: append all source rows (also triggers _delta_post_commit via _write_delta_path).
        self.write_to_path(
            df, path, mode=LoadType.APPEND.value, fmt=fmt,
            partition_columns=partition_columns, options=options,
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
                partition_columns = self._resolve_column_names(actual, partition_columns)
            self._merge_iceberg_table(df, table_name, merge_keys, partition_columns=partition_columns)
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
                partition_columns = self._resolve_column_names(actual, partition_columns)
            self._merge_overwrite_iceberg_table(df, table_name, merge_keys, partition_columns=partition_columns)
        else:
            raise EngineError(
                f"PolarsEngine.merge_overwrite_to_table: unsupported format {fmt!r}"
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
            raise EngineError(f"PolarsEngine scd2_to_path only supports Delta, got {fmt!r}")

        actual = self._schema_names(df)
        merge_keys = self._resolve_column_names(actual, merge_keys)
        if partition_columns:
            partition_columns = self._resolve_column_names(actual, partition_columns)

        delta_merge_options, src_alias, tgt_alias = self._build_delta_merge_options(
            merge_keys, options,
        )
        delta_merge_options["predicate"] = (
            f"{delta_merge_options['predicate']}"
            f" AND {tgt_alias}.`{SCD2Column.IS_CURRENT.value}` = true"
        )

        write_opts: Dict[str, Any] = {}
        if self._storage_options:
            write_opts["storage_options"] = self._storage_options

        # Step 1: MERGE — close current rows where source is strictly newer
        update_set = {
            SCD2Column.VALID_TO.value: f"{src_alias}.`{SCD2Column.VALID_FROM.value}`",
            SCD2Column.IS_CURRENT.value: "false",
        }
        late_guard = (
            f"{src_alias}.`{SCD2Column.VALID_FROM.value}` > "
            f"{tgt_alias}.`{SCD2Column.VALID_FROM.value}`"
        )

        try:
            (
                self._sink_or_write_delta(
                    df,
                    path,
                    mode="merge",
                    delta_merge_options=delta_merge_options,
                    **write_opts,
                )
                .when_matched_update(
                    updates=update_set,
                    predicate=late_guard,
                )
                .execute()
            )
        except Exception as exc:
            self._raise_if_delta_target_missing(exc, path, "SCD2 merge")
            raise

        # Step 2: APPEND all source rows as new versions
        self.write_to_path(
            df, path, mode=LoadType.APPEND.value, fmt=fmt,
            partition_columns=partition_columns, options=options,
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
                partition_columns = self._resolve_column_names(actual, partition_columns)
            self._scd2_iceberg_table(df, table_name, merge_keys, partition_columns=partition_columns)
        else:
            raise EngineError(
                f"PolarsEngine.scd2_to_table: unsupported format {fmt!r}"
            )

    # ------------------------------------------------------------------
    # Iceberg merge helpers
    # ------------------------------------------------------------------

    def _iceberg_prepare_target(
        self,
        df: pl.LazyFrame,
        table_name: str,
        partition_columns: Optional[List[str]],
        *,
        caller: str,
    ) -> Tuple[Any, "pl.DataFrame", Any, str]:
        """Load target table, materialise *df*, evolve schema + partition spec.

        Shared preamble for ``_merge_iceberg_table``,
        ``_merge_overwrite_iceberg_table`` and ``_scd2_iceberg_table``.

        Returns ``(ice_table, collected, arrow_table, pyice_id)``.  The caller
        performs the actual write (``overwrite``, ``append``, or ``upsert``).
        """
        if self._iceberg_catalog is None:
            raise EngineError(
                f"PolarsEngine.{caller} requires iceberg_catalog"
            )
        pyice_id = self._pyiceberg_table_id(table_name)
        ice_table = self._iceberg_catalog.load_table(pyice_id)
        collected = df.collect()
        arrow_table = collected.to_arrow()
        ice_table, arrow_table = self._align_and_evolve_iceberg(
            ice_table, arrow_table, pyice_id,
        )
        if partition_columns:
            ice_table = self._ensure_iceberg_partition_spec(
                ice_table, partition_columns, pyice_id,
            )
        return ice_table, collected, arrow_table, pyice_id

    @staticmethod
    def _build_iceberg_key_filter(
        collected: pl.DataFrame, merge_keys: List[str],
    ) -> Any:
        """Build a pyiceberg filter expression matching source key combos."""
        from pyiceberg.expressions import And, EqualTo, In, Or  # noqa: PLC0415

        if len(merge_keys) == 1:
            key = merge_keys[0]
            values = tuple(collected.get_column(key).unique().to_list())
            return In(key, values)

        # Compound keys: OR of AND(key1 == v1, key2 == v2, ...)
        key_combos = collected.select(merge_keys).unique()
        conditions = []
        for row in key_combos.iter_rows(named=True):
            parts = [EqualTo(k, row[k]) for k in merge_keys]
            conditions.append(reduce(lambda a, b: And(a, b), parts))
        return reduce(lambda a, b: Or(a, b), conditions)

    def _merge_iceberg_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        merge_keys: List[str],
        partition_columns: Optional[List[str]] = None,
    ) -> None:
        """Upsert into an Iceberg table via ``ice_table.upsert(join_cols=merge_keys)``.

        Passes *merge_keys* as ``join_cols`` so no ``identifier_field_ids``
        configuration is required on the Iceberg table schema.

        Columns missing in the source are backfilled with NULL by
        :meth:`_reorder_arrow_to_iceberg`, which — in an upsert — will
        overwrite existing target values with NULL for matched rows.
        Callers that need to preserve non-key columns should include them
        in the source dataframe or use ``merge_overwrite_to_table`` for a
        rolling overwrite.
        """
        ice_table, _, arrow_table, _ = self._iceberg_prepare_target(
            df, table_name, partition_columns, caller="_merge_iceberg_table",
        )
        ice_table.upsert(
            self._reorder_arrow_to_iceberg(arrow_table, ice_table),
            join_cols=merge_keys,
        )

    def _merge_overwrite_iceberg_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        merge_keys: List[str],
        partition_columns: Optional[List[str]] = None,
    ) -> None:
        """Rolling overwrite for an Iceberg table (mirrors merge_overwrite_to_path).

        Deletes target rows matching any source key combination, then
        appends all source rows.  Uses ``overwrite(arrow, overwrite_filter=...)``
        for atomicity.
        """
        ice_table, collected, arrow_table, _ = self._iceberg_prepare_target(
            df, table_name, partition_columns,
            caller="_merge_overwrite_iceberg_table",
        )
        key_filter = self._build_iceberg_key_filter(collected, merge_keys)
        ice_table.overwrite(
            self._reorder_arrow_to_iceberg(arrow_table, ice_table),
            overwrite_filter=key_filter,
        )

    def _scd2_iceberg_table(
        self,
        df: pl.LazyFrame,
        table_name: str,
        merge_keys: List[str],
        partition_columns: Optional[List[str]] = None,
    ) -> None:
        """SCD2 for Iceberg tables (two-step, non-atomic).

        Step 1: Close current rows (``__valid_to``, ``__is_current = false``)
                via ``overwrite(overwrite_filter=...)`` where source
                ``__valid_from > target``.
        Step 2: Append all source rows as new versions.

        Uses ``overwrite`` instead of ``upsert`` because pyiceberg's
        ``upsert`` silently skips rows whose non-key columns appear
        unchanged after Arrow ↔ Iceberg type coercion, causing
        ``__valid_to`` / ``__is_current`` updates to be lost.
        ``overwrite`` atomically deletes + inserts without row-level
        diffing, matching the reliability of Delta's MERGE.
        """
        ice_table, collected, arrow_table, _ = self._iceberg_prepare_target(
            df, table_name, partition_columns, caller="_scd2_iceberg_table",
        )

        valid_from_col = SCD2Column.VALID_FROM.value
        valid_to_col = SCD2Column.VALID_TO.value
        is_current_col = SCD2Column.IS_CURRENT.value

        # Step 1: close current rows via overwrite
        key_filter = self._build_iceberg_key_filter(collected, merge_keys)

        from pyiceberg.expressions import And, EqualTo  # noqa: PLC0415

        current_filter = And(key_filter, EqualTo(is_current_col, True))
        target_arrow = ice_table.scan(row_filter=current_filter).to_arrow()

        if len(target_arrow) > 0:
            target_df = pl.from_arrow(target_arrow)
            src_vf = (
                collected
                .select(merge_keys + [pl.col(valid_from_col).alias("__src_vf")])
                .unique(subset=merge_keys)
            )

            # Build an expression that casts __src_vf to match
            # __valid_to's type (typically Timestamp from
            # SCD2ColumnAdder's CAST(NULL AS TIMESTAMP)).
            # __src_vf inherits the effective column's type, which
            # may be String when the source has a text date column.
            _target_vt_dtype = target_df[valid_to_col].dtype
            _src_vf_dtype = src_vf.schema["__src_vf"]
            if _src_vf_dtype == _target_vt_dtype:
                _vt_expr = pl.col("__src_vf")
            elif _src_vf_dtype == pl.Utf8 and isinstance(_target_vt_dtype, pl.Datetime):
                _vt_expr = pl.col("__src_vf").str.to_datetime(
                    time_unit=_target_vt_dtype.time_unit,
                    time_zone=_target_vt_dtype.time_zone,
                )
            else:
                _vt_expr = pl.col("__src_vf").cast(_target_vt_dtype)

            closed = (
                target_df.join(src_vf, on=merge_keys)
                .filter(pl.col("__src_vf") > pl.col(valid_from_col))
                .with_columns(
                    _vt_expr.alias(valid_to_col),
                    pl.lit(False).alias(is_current_col),
                )
                .drop("__src_vf")
            )
            if len(closed) > 0:
                close_filter = self._build_iceberg_key_filter(
                    closed, merge_keys + [valid_from_col],
                )
                ice_table.overwrite(
                    self._reorder_arrow_to_iceberg(closed.to_arrow(), ice_table),
                    overwrite_filter=close_filter,
                )

        # Step 2: APPEND all source rows as new versions.
        ice_table.append(self._reorder_arrow_to_iceberg(arrow_table, ice_table))

    # ==================================================================
    # Transform
    # ==================================================================

    def add_column(self, df: pl.LazyFrame, column_name: str, expression: str) -> pl.LazyFrame:
        return df.with_columns(pl.sql_expr(expression).alias(column_name))

    def drop_columns(self, df: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        actual = self._schema_names(df)
        lower_map = {c.lower(): c for c in actual}
        to_drop = [lower_map[c.lower()] for c in columns if c.lower() in lower_map]
        return df.drop(to_drop) if to_drop else df

    def select_columns(self, df: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        resolved = self._resolve_column_names(self._schema_names(df), columns)
        return df.select(resolved)

    def rename_column(self, df: pl.LazyFrame, old_name: str, new_name: str) -> pl.LazyFrame:
        actual_old = self._resolve_column_name(self._schema_names(df), old_name)
        return df.rename({actual_old: new_name})

    def filter_rows(self, df: pl.LazyFrame, condition: str) -> pl.LazyFrame:
        return df.filter(pl.sql_expr(condition))

    def apply_watermark_filter(
        self,
        df: pl.LazyFrame,
        watermark_columns: List[str],
        watermark: Dict[str, Any],
    ) -> pl.LazyFrame:
        actual = self._schema_names(df)
        combined: Optional[pl.Expr] = None
        for col_name in watermark_columns:
            value = watermark.get(col_name)
            if value is None:
                continue
            resolved_col = self._resolve_column_name(actual, col_name)
            if isinstance(value, (datetime, date)):
                value = value.isoformat()
            condition = pl.col(resolved_col) > pl.lit(value)
            combined = condition if combined is None else (combined | condition)

        if combined is None:
            return df
        return df.filter(combined)

    def deduplicate(
        self,
        df: pl.LazyFrame,
        partition_columns: List[str],
        order_columns: Optional[List[str]] = None,
        order: str = "desc",
    ) -> pl.LazyFrame:
        actual = self._schema_names(df)
        resolved_partition = self._resolve_column_names(actual, partition_columns)
        resolved_order = self._resolve_column_names(actual, order_columns) if order_columns else None
        _order = resolved_order or resolved_partition
        descending = order != "asc"
        df_sorted = df.sort(_order, descending=descending)
        return df_sorted.unique(subset=resolved_partition, keep="first")

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
        descending = order != "asc"
        if len(resolved_order) == 1:
            # Single column: O(n) max/min scan per partition, no sort overhead.
            col = resolved_order[0]
            best = (
                pl.col(col).max() if descending else pl.col(col).min()
            ).over(resolved_partition)
            return df.filter(pl.col(col) == best)
        # Multiple columns: lexicographic sort within each partition, take the
        # extremal tuple, filter all rows that match (keeps ties).
        descending_flags = [descending] * len(resolved_order)
        best_exprs = [
            pl.col(c)
            .sort_by(resolved_order, descending=descending_flags)
            .first()
            .over(resolved_partition)
            .alias(f"__best_{c}")
            for c in resolved_order
        ]
        filter_expr = pl.all_horizontal(
            pl.col(c) == pl.col(f"__best_{c}") for c in resolved_order
        )
        return (
            df.with_columns(best_exprs)
            .filter(filter_expr)
            .drop([f"__best_{c}" for c in resolved_order])
        )

    @staticmethod
    def _to_chrono_fmt(fmt: str) -> str:
        """Convert a Java DateTimeFormatter pattern to chrono (strftime)."""
        if "%" in fmt:
            return fmt
        
        # Replace longer tokens first to avoid partial matches
        _JAVA_TOKENS = [
            ("yyyy", "%Y"), ("yy", "%y"),
            ("MM", "%m"), ("dd", "%d"),
            ("HH", "%H"), ("mm", "%M"), ("ss", "%S"),
            ("SSS", "%f"),
        ]
        result = fmt
        for java, chrono in _JAVA_TOKENS:
            result = result.replace(java, chrono)
        return result

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
        expr = self._build_cast_expr(actual_name, target_type.lower(), schema[actual_name], fmt)
        if expr is None:
            return df
        return df.with_columns(expr.alias(actual_name))

    @staticmethod
    def _build_cast_expr(
        col_name: str,
        target_lower: str,
        src_dtype: pl.DataType,
        fmt: Optional[str],
    ) -> Optional[pl.Expr]:
        """Build a Polars expression that casts *col_name* to *target_lower*.

        Returns ``None`` when the type is unknown (caller should bypass the
        cast).  This is a pure static method — no DataFrame access — so it is
        fast and easily unit-tested.
        """
        col = pl.col(col_name)

        # ---- Parameterised decimal: DECIMAL(18,2), NUMERIC(10,4), … ----
        m = _DECIMAL_RE.match(target_lower)
        if m:
            precision, scale = int(m.group(1)), int(m.group(2))
            return col.cast(pl.Decimal(precision=precision, scale=scale))

        # ---- Type-alias lookup ----
        pl_type = _POLARS_TYPE_MAP.get(target_lower)
        if pl_type is None:
            logger.debug(
                "PolarsEngine.cast_column: unknown type %r — bypassing cast, column kept as-is",
                target_lower,
            )
            return None

        # ---- Date ----
        if pl_type == pl.Date:
            if fmt:
                return col.str.to_date(PolarsEngine._to_chrono_fmt(fmt))
            return col.cast(pl.Date)

        # ---- Datetime (with or without TZ, with or without format) ----
        if isinstance(pl_type, pl.Datetime):
            tz = pl_type.time_zone
            tu = pl_type.time_unit or "us"

            if fmt:
                # str.to_datetime parses the string using the format and attaches
                # the target TZ (tz=None → naive).
                return col.str.to_datetime(
                    PolarsEngine._to_chrono_fmt(fmt), time_unit=tu, time_zone=tz
                )

            if tz:
                # No format string — source-dtype-aware TZ handling:
                #   String         → parse ISO, attach/convert to target TZ
                #   TZ-aware dt    → shift instant to target TZ
                #   Naive dt/other → stamp TZ without shifting (wall-clock preserved)
                if isinstance(src_dtype, pl.String):
                    return col.str.to_datetime(time_unit=tu, time_zone=tz)
                if isinstance(src_dtype, pl.Datetime) and src_dtype.time_zone:
                    return col.dt.convert_time_zone(tz)
                return col.cast(pl.Datetime(tu)).dt.replace_time_zone(tz)

            # No TZ (timestamp_ntz semantics)
            return col.cast(pl.Datetime(tu))

        # ---- All other types ----
        return col.cast(pl_type)

    # ==================================================================
    # System columns
    # ==================================================================

    def add_system_columns(self, df: pl.LazyFrame, author: Optional[str] = None) -> pl.LazyFrame:
        _author = author or DEFAULT_AUTHOR
        now = datetime.now(tz=timezone.utc)
        return df.with_columns(
            pl.lit(now).alias(SystemColumn.CREATED_AT),
            pl.lit(now).alias(SystemColumn.UPDATED_AT),
            pl.lit(_author).alias(SystemColumn.UPDATED_BY),
        )

    def convert_timestamp_ntz_to_timestamp(self, df: pl.LazyFrame) -> pl.LazyFrame:
        schema = df.collect_schema()
        conversions = [
            pl.col(name)
            .cast(pl.Datetime(dtype.time_unit or "us"))
            .dt.replace_time_zone("UTC")
            .alias(name)
            for name, dtype in schema.items()
            if isinstance(dtype, pl.Datetime) and dtype.time_zone is None
        ]
        return df.with_columns(conversions) if conversions else df

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
        path_col = FileInfoColumn.FILE_PATH
        name_col = FileInfoColumn.FILE_NAME
        mtime_col = FileInfoColumn.FILE_MODIFICATION_TIME

        if file_infos:
            # Polars normalises embedded paths to forward slashes regardless of OS,
            # so normalise fi.path the same way before building the join key.
            mapping = pl.LazyFrame(
                {
                    path_col: [fi.path.replace("\\", "/") for fi in file_infos],
                    name_col: [fi.name for fi in file_infos],
                    mtime_col: [fi.modification_time for fi in file_infos],
                }
            )
            return df.join(mapping, on=path_col, how="left")

        # No file_infos: extract name from the embedded path, mtime unknown.
        return df.with_columns(
            pl.col(path_col).str.split("/").list.last().alias(name_col),
            pl.lit(None).cast(pl.Datetime(time_zone="UTC")).alias(mtime_col),
        )

    # ==================================================================
    # Symlink manifest
    # ==================================================================

    def generate_symlink_manifest(self, path: str) -> None:
        """Generate a symlink manifest using delta-rs ``DeltaTable.generate()``."""
        logger.info("PolarsEngine: generating symlink manifest for %s", path)
        dt = self.delta(path, storage_options=self._storage_options or None)
        dt.generate()

    # ==================================================================
    # Metrics
    # ==================================================================

    def count_rows(self, df: pl.LazyFrame) -> int:
        return df.select(pl.len()).collect().item()

    def is_empty(self, df: pl.LazyFrame) -> bool:
        return len(df.head(1).collect()) == 0

    def get_columns(self, df: pl.LazyFrame) -> List[str]:
        return self._schema_names(df)

    def get_schema(self, df: pl.LazyFrame) -> Dict[str, str]:
        schema = df.collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def get_hive_schema(self, df: pl.LazyFrame) -> Dict[str, str]:
        """Return ``{column_name: hive_type}`` using native Polars dtype objects."""
        schema = df.collect_schema()
        return {
            name: self._polars_type_to_hive(dtype)
            for name, dtype in schema.items()
        }

    @staticmethod
    def _polars_type_to_hive(dtype: pl.DataType) -> str:
        """Recursively convert a Polars DataType to a Hive/Athena DDL string."""
        if isinstance(dtype, pl.Int64):
            return "BIGINT"
        if isinstance(dtype, pl.Int32):
            return "INT"
        if isinstance(dtype, pl.Int16):
            return "SMALLINT"
        if isinstance(dtype, pl.Int8):
            return "TINYINT"
        if isinstance(dtype, pl.UInt8):
            return "SMALLINT"
        if isinstance(dtype, pl.UInt16):
            return "INT"
        if isinstance(dtype, (pl.UInt32, pl.UInt64)):
            return "BIGINT"
        if isinstance(dtype, pl.Float32):
            return "FLOAT"
        if isinstance(dtype, pl.Float64):
            return "DOUBLE"
        if isinstance(dtype, pl.Boolean):
            return "BOOLEAN"
        if isinstance(dtype, pl.Binary):
            return "BINARY"
        if isinstance(dtype, pl.Date):
            return "DATE"
        if isinstance(dtype, pl.Datetime):
            return "TIMESTAMP"
        if isinstance(dtype, pl.Decimal):
            p = dtype.precision if dtype.precision is not None else 38
            s = dtype.scale if dtype.scale is not None else 0
            return f"DECIMAL({p},{s})"
        if isinstance(dtype, (pl.Duration, pl.Time)):
            return "STRING"
        if isinstance(dtype, pl.Null):
            return "STRING"
        if isinstance(dtype, (pl.String, pl.Utf8, pl.Categorical)):
            return "STRING"
        if isinstance(dtype, pl.List):
            return f"ARRAY<{PolarsEngine._polars_type_to_hive(dtype.inner)}>"
        if isinstance(dtype, pl.Struct):
            fields = [
                f"{f.name}:{PolarsEngine._polars_type_to_hive(f.dtype)}"
                for f in dtype.fields
            ]
            return f"STRUCT<{','.join(fields)}>"
        return "STRING"

    @staticmethod
    def _collect_row_safe(result: pl.LazyFrame) -> Dict[str, Any]:
        """Collect a single-row LazyFrame to a dict, avoiding ``zoneinfo`` failures
        on Windows (Microsoft Store Python) where ``ZoneInfo('UTC')`` raises
        ``ZoneInfoNotFoundError``.  TZ-aware datetime columns are stripped to
        naive UTC before ``.row()`` and reattached using ``datetime.timezone.utc``.
        """
        schema = result.collect_schema()
        tz_cols = [
            name
            for name, dtype in schema.items()
            if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None
        ]
        if tz_cols:
            result = result.with_columns(
                pl.col(c).dt.convert_time_zone("UTC").dt.replace_time_zone(None) for c in tz_cols
            )
        row = result.collect().row(0, named=True)
        d = dict(row)
        for c in tz_cols:
            if d.get(c) is not None:
                d[c] = d[c].replace(tzinfo=timezone.utc)
        return d

    def get_max_values(self, df: pl.LazyFrame, columns: List[str]) -> Dict[str, Any]:
        resolved = self._resolve_column_names(self._schema_names(df), columns)
        agg_exprs = [pl.col(c).max().alias(c) for c in resolved]
        return self._collect_row_safe(df.select(agg_exprs))

    def get_count_and_max_values(
        self,
        df: pl.LazyFrame,
        columns: List[str],
    ) -> Tuple[int, Dict[str, Any]]:
        resolved = self._resolve_column_names(self._schema_names(df), columns)
        agg_exprs = [pl.len().alias("__row_count")]
        agg_exprs.extend(pl.col(c).max().alias(c) for c in resolved)
        d = self._collect_row_safe(df.select(agg_exprs))
        count = d.pop("__row_count", 0)
        return count, d

    # ==================================================================
    # Maintenance
    # ==================================================================

    @staticmethod
    def _align_ms_boundaries(
        start_time: "Optional[datetime]",
        end_time: "Optional[datetime]",
    ) -> "Tuple[Optional[datetime], Optional[datetime]]":
        """Truncate *start_time* / ceil *end_time* to millisecond precision.

        Delta and Iceberg timestamps are stored as millisecond integers.
        Aligning comparison boundaries avoids sub-millisecond mismatches
        (mirrors SparkEngine behaviour).
        """
        _start = None
        _end = None
        if start_time is not None:
            _start = start_time.replace(
                microsecond=(start_time.microsecond // 1000) * 1000
            )
        if end_time is not None:
            ceil_us = ((end_time.microsecond + 999) // 1000) * 1000
            if ceil_us >= 1_000_000:
                _end = end_time.replace(microsecond=0) + timedelta(seconds=1)
            else:
                _end = end_time.replace(microsecond=ceil_us)
        return _start, _end

    def table_exists_by_path(self, path: str, *, fmt: str = "delta") -> bool:
        try:
            fmt_lower = fmt.lower()
            if fmt_lower == Format.DELTA.value:
                # Fast path: check marker directory without loading the whole table.
                if self._platform is not None:
                    return self._platform.folder_exists(f"{path.rstrip('/')}/_delta_log")
                return self.delta.is_deltatable(
                    path, storage_options=self._storage_options or None
                )
            if fmt_lower == Format.ICEBERG.value:
                # Fast path: Iceberg tables always contain a metadata/ subdirectory.
                if self._platform is not None:
                    return self._platform.folder_exists(f"{path.rstrip('/')}/metadata")
                return False
            logger.warning(
                "PolarsEngine table_exists_by_path: unsupported format %s", fmt,
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
            pyice_id = self._pyiceberg_table_id(table_name)
            try:
                return self._iceberg_catalog.table_exists(pyice_id)
            except Exception as exc:
                logger.debug("table_exists check failed, assuming absent: %s", exc)
                return False
        raise EngineError(f"PolarsEngine.table_exists_by_name: unsupported format {fmt!r}")

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
        try:
            dt = self.delta(path, storage_options=self._storage_options or None)
            history = dt.history(limit)
        except Exception:  # noqa: BLE001
            return []

        _start, _end = self._align_ms_boundaries(start_time, end_time)

        result: List[Dict[str, Any]] = []
        for entry in history:
            ts = entry.get("timestamp")
            if ts is not None and isinstance(ts, (int, float)):
                entry = {**entry, "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc)}
            ts_val = entry.get("timestamp")
            if _start is not None and ts_val is not None and ts_val <= _start:
                continue
            if _end is not None and ts_val is not None and ts_val >= _end:
                continue
            result.append(entry)
        return result

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
            pyice_id = self._pyiceberg_table_id(table_name)
            try:
                ice_table = self._iceberg_catalog.load_table(pyice_id)
                _start, _end = self._align_ms_boundaries(start_time, end_time)

                # Pre-filter snapshots by time range, then intersect with
                # history() to keep only "current" snapshots (meaningful
                # writes).  Intermediate DELETE steps from overwrite ops
                # exist in snapshots() but not in history().
                snap_index: Dict[int, Any] = {}
                for s in ice_table.snapshots() or []:
                    ts = datetime.fromtimestamp(s.timestamp_ms / 1000, tz=timezone.utc)
                    if _start is not None and ts <= _start:
                        continue
                    if _end is not None and ts >= _end:
                        continue
                    snap_index[s.snapshot_id] = s
            except Exception:  # noqa: BLE001
                return []

            result: List[Dict[str, Any]] = []
            for snap in sorted(
                snap_index.values(),
                key=lambda s: s.timestamp_ms,
                reverse=True,
            ):
                _snap = snap.dict()
                result.append({
                    "snapshot_id": _snap.get("snapshot_id"),
                    "parent_id": _snap.get("parent_snapshot_id"),
                    "timestamp": datetime.fromtimestamp(
                        _snap.get("timestamp_ms") / 1000, tz=timezone.utc,
                    ),
                    "operation": (
                        _snap.get("summary").get("operation") if _snap.get("summary") else None
                    ),
                    "manifest_list": _snap.get("manifest_list"),
                    "summary": dict(_snap.get("summary")) if _snap.get("summary") else None,
                })
            return result[:limit]
        raise EngineError(f"PolarsEngine.get_history_by_name: unsupported format {fmt!r}")

    def compact_by_path(self, path: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None) -> None:
        if fmt.lower() != Format.DELTA.value:
            logger.warning(
                "PolarsEngine compact_by_path only supports Delta, got %s", fmt,
            )
            return
        dt = self.delta(path, storage_options=self._storage_options or None)
        dt.optimize.compact()

    def compact_by_name(self, table_name: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None) -> None:
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
                "PolarsEngine cleanup_by_path only supports Delta, got %s", fmt,
            )
            return
        dt = self.delta(path, storage_options=self._storage_options or None)
        dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=False)

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
            pyice_id = self._pyiceberg_table_id(table_name)
            ice_table = self._iceberg_catalog.load_table(pyice_id)
            if opts.get("expire_snapshots", True):
                self._iceberg_expire_snapshots(ice_table, retention_hours)
            if opts.get("remove_orphan_files", True):
                logger.warning(
                    "PolarsEngine cleanup_by_name: pyiceberg does not support remove_orphan_files; skipping"
                )
            return
        raise EngineError(f"PolarsEngine.cleanup_by_name: unsupported format {fmt!r}")

    @staticmethod
    def _iceberg_expire_snapshots(ice_table: Any, retention_hours: int) -> None:
        """Expire Iceberg snapshots older than *retention_hours*.

        Pre-checks snapshot metadata so that no-op calls are skipped
        entirely — pyiceberg otherwise sends an empty remove-snapshots
        request which the REST catalog rejects with HTTP 500.
        """
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        current_id = getattr(ice_table.metadata, "current_snapshot_id", None)
        expired = [
            s for s in (ice_table.snapshots() or [])
            if getattr(s, "timestamp_ms", 0) < cutoff_ms
            and s.snapshot_id != current_id
        ]
        if not expired:
            logger.info(
                "PolarsEngine cleanup_by_name: no snapshots older than %dh; skipping expire_snapshots",
                retention_hours,
            )
            return
        try:
            ice_table.maintenance.expire_snapshots().older_than(cutoff).commit()
        except Exception as exc:
            logger.warning(
                "PolarsEngine cleanup_by_name: expire_snapshots skipped (%s)", exc,
            )

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
                self.write_to_table(df, table_name, mode=mode, fmt=fmt, partition_columns=partition_columns, options=options)
                return
            if path:
                raise EngineError(
                    "PolarsEngine Iceberg writes require table_name (catalog) — pass table_name instead of path"
                )
            raise EngineError("write() requires table_name or path")
        # Delta and flat-file formats → path-based
        if path:
            self.write_to_path(df, path, mode=mode, fmt=fmt, partition_columns=partition_columns, options=options)
        elif table_name and fmt.lower() == Format.DELTA.value:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        elif table_name:
            self.write_to_table(df, table_name, mode=mode, fmt=fmt, partition_columns=partition_columns, options=options)
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
                    df, table_name, merge_keys=merge_keys, fmt=fmt,
                    partition_columns=partition_columns, options=options,
                )
                return
            if path:
                raise EngineError(
                    "PolarsEngine Iceberg merges require table_name (catalog) — pass table_name instead of path"
                )
            raise EngineError("merge() requires table_name or path")
        # Delta → path-based
        if path:
            self.merge_to_path(df, path, merge_keys=merge_keys, fmt=fmt, partition_columns=partition_columns, options=options)
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
                    df, table_name, merge_keys=merge_keys, fmt=fmt,
                    partition_columns=partition_columns, options=options,
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
                df, path, merge_keys=merge_keys, fmt=fmt,
                partition_columns=partition_columns, options=options,
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
                    df, table_name, merge_keys=merge_keys, fmt=fmt,
                    partition_columns=partition_columns, options=options,
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
                df, path, merge_keys=merge_keys, fmt=fmt,
                partition_columns=partition_columns, options=options,
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
                return self.get_history_by_name(table_name, limit, start_time, end_time=end_time, fmt=fmt)
            if path:
                return self.get_history_by_path(path, limit, start_time, end_time=end_time, fmt=fmt)
            raise EngineError("get_history() requires table_name or path")
        # Delta → path-based
        if path:
            return self.get_history_by_path(path, limit, start_time, end_time=end_time, fmt=fmt)
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
                self.cleanup_by_name(table_name, retention_hours=retention_hours, fmt=fmt, options=options)
                return
            if path:
                self.cleanup_by_path(path, retention_hours=retention_hours, fmt=fmt, options=options)
                return
            raise EngineError("cleanup() requires table_name or path")
        # Delta → path-based
        if path:
            self.cleanup_by_path(path, retention_hours=retention_hours, fmt=fmt, options=options)
        elif table_name:
            raise EngineError(
                "PolarsEngine does not support named Delta tables — pass path instead of table_name"
            )
        else:
            raise EngineError("cleanup() requires table_name or path")
