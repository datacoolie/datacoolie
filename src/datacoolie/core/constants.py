"""Framework-wide enums and constants for the DataCoolie framework.

Every enum inherits from ``str`` so its ``.value`` is a plain string
that serialises naturally to JSON/YAML and matches database values.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Dict, List


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class LoadType(str, Enum):
    """Supported load (write) strategies."""

    FULL_LOAD = "full_load"
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE_UPSERT = "merge_upsert"
    MERGE_OVERWRITE = "merge_overwrite"
    SCD2 = "scd2"


class Format(str, Enum):
    """Supported data formats."""

    DELTA = "delta"
    ICEBERG = "iceberg"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    AVRO = "avro"
    EXCEL = "excel"
    SQL = "sql"
    API = "api"
    FUNCTION = "function"


class ConnectionType(str, Enum):
    """Connection endpoint categories."""

    LAKEHOUSE = "lakehouse"
    FILE = "file"
    DATABASE = "database"
    API = "api"
    STREAMING = "streaming"
    FUNCTION = "function"


# ---------------------------------------------------------------------------
# Connection type ↔ format mapping
# ---------------------------------------------------------------------------

CONNECTION_TYPE_FORMATS: Dict[str, frozenset] = {
    ConnectionType.FILE.value: frozenset({
        Format.PARQUET.value, 
        Format.CSV.value, 
        Format.JSON.value,
        Format.JSONL.value, 
        Format.AVRO.value,
        Format.EXCEL.value,
    }),
    ConnectionType.LAKEHOUSE.value: frozenset({
        Format.DELTA.value, 
        Format.ICEBERG.value,
    }),
    ConnectionType.DATABASE.value: frozenset({
        Format.SQL.value,
    }),
    ConnectionType.API.value: frozenset({
        Format.API.value,
    }),
    ConnectionType.FUNCTION.value: frozenset({
        Format.FUNCTION.value,
    }),
    ConnectionType.STREAMING.value: frozenset(

    ),
}


class ProcessingMode(str, Enum):
    """ETL processing modes."""

    BATCH = "batch"
    MICROBATCH = "microbatch"
    STREAMING = "streaming"


class DataFlowStatus(str, Enum):
    """Dataflow execution statuses."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class ExecutionType(str, Enum):
    """ETL execution types."""

    ETL = "etl"
    MAINTENANCE = "maintenance"


class DatabaseType(str, Enum):
    """Supported database types for connection URL generation."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    SQLITE = "sqlite"


class MaintenanceType(str, Enum):
    """Maintenance operation types."""

    COMPACT = "compact"
    CLEANUP = "cleanup"


class ColumnCaseMode(str, Enum):
    """Column name case-conversion modes used by :class:`ColumnNameSanitizer`."""

    LOWER = "lower"
    SNAKE = "snake"


class LogType(str, Enum):
    """Log entry types for structured logging."""

    JOB_RUN_LOG = "job_run_log"
    DATAFLOW_RUN_LOG = "dataflow_run_log"


class LogPurpose(str, Enum):
    """Log output purposes / formats."""

    DEBUG = "debug_json"
    ANALYST = "analyst"


# ---------------------------------------------------------------------------
# System columns (auto-added to destination data)
# ---------------------------------------------------------------------------

class SystemColumn(str, Enum):
    """Column names automatically added to every destination write."""

    CREATED_AT = "__created_at"
    UPDATED_AT = "__updated_at"
    UPDATED_BY = "__updated_by"


class FileInfoColumn(str, Enum):
    """Column names injected for file-based sources."""

    FILE_NAME = "__file_name"
    FILE_PATH = "__file_path"
    FILE_MODIFICATION_TIME = "__file_modification_time"


class SCD2Column(str, Enum):
    """Column names for Slowly Changing Dimension Type 2."""

    VALID_FROM = "__valid_from"
    VALID_TO = "__valid_to"
    IS_CURRENT = "__is_current"


# Canonical order for columns that should always appear at the end of a
# DataFrame / table schema.  Shared by the transformer pipeline
# (``ColumnNameSanitizer``) and the Iceberg adapter
# (``_reorder_iceberg_trailing_columns``).
TRAILING_COLUMNS: tuple[str, ...] = (
    *(c.value for c in SCD2Column),
    *(c.value for c in FileInfoColumn),
    *(c.value for c in SystemColumn),
)

# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

WATERMARK_FILE_NAME: str = "watermark_value.json"
DATETIME_PATTERN: str = "__datetime__"
DATE_PATTERN: str = "__date__"
TIME_PATTERN: str = "__time__"
DATE_FOLDER_PARTITION_KEY: str = "__date_folder_partition__"

# ---------------------------------------------------------------------------
# Date folder partition placeholders
# ---------------------------------------------------------------------------

DATE_PLACEHOLDERS: Dict[str, tuple[str, str]] = {
    "{year}": (r"(\d{4})", "year"),
    "{month}": (r"(\d{2})", "month"),
    "{day}": (r"(\d{2})", "day"),
    "{hour}": (r"(\d{2})", "hour"),
}

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# Log partition pattern default is date-based, but can be overridden by config
DEFAULT_PARTITION_PATTERN = "__run_date={year}-{month}-{day}"

# Default values for various parameters across the framework
DEFAULT_AUTHOR: str = "DataCoolie"
DEFAULT_MAX_WORKERS: int = 8
DEFAULT_RETRY_COUNT: int = 0
DEFAULT_RETRY_DELAY: float = 5.0
DEFAULT_RETENTION_HOURS: int = 168  # 7 days

