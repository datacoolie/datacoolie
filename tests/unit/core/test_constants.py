"""Tests for datacoolie.core.constants.

This module verifies the enumeration values, string representations, and default
constants used throughout the ETL framework.
"""

from datacoolie.core.constants import (
    CONNECTION_TYPE_FORMATS,
    DEFAULT_AUTHOR,
    DEFAULT_MAX_WORKERS,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETENTION_HOURS,
    DATETIME_PATTERN,
    DATE_FOLDER_PARTITION_KEY,
    ColumnCaseMode,
    FileInfoColumn,
    SystemColumn,
    WATERMARK_FILE_NAME,
    ConnectionType,
    DataFlowStatus,
    ExecutionType,
    Format,
    LoadType,
    LogPurpose,
    LogType,
    MaintenanceType,
    ProcessingMode,
)


# ============================================================================
# Load Type Enum
# ============================================================================


class TestLoadType:
    """Verify LoadType enum values for write strategies."""

    def test_values(self) -> None:
        assert LoadType.OVERWRITE.value == "overwrite"
        assert LoadType.APPEND.value == "append"
        assert LoadType.MERGE_UPSERT.value == "merge_upsert"
        assert LoadType.MERGE_OVERWRITE.value == "merge_overwrite"
        assert LoadType.SCD2.value == "scd2"
        assert LoadType.FULL_LOAD.value == "full_load"

    def test_str_enum(self) -> None:
        assert isinstance(LoadType.OVERWRITE, str)
        assert LoadType.OVERWRITE == "overwrite"

    def test_all_members(self) -> None:
        assert len(LoadType) == 6


# ============================================================================
# Format Enum
# ============================================================================


class TestFormat:
    """Verify Format enum values for file/data formats."""
    def test_values(self) -> None:
        assert Format.PARQUET.value == "parquet"
        assert Format.DELTA.value == "delta"
        assert Format.ICEBERG.value == "iceberg"
        assert Format.CSV.value == "csv"
        assert Format.JSON.value == "json"
        assert Format.JSONL.value == "jsonl"
        assert Format.AVRO.value == "avro"
        assert Format.EXCEL.value == "excel"
        assert Format.FUNCTION.value == "function"
        assert Format.API.value == "api"
        assert Format.SQL.value == "sql"

    def test_all_members(self) -> None:
        assert len(Format) == 11


# ============================================================================
# Connection Type Enum
# ============================================================================


class TestConnectionType:
    """Verify ConnectionType enum values."""
    def test_values(self) -> None:
        assert ConnectionType.LAKEHOUSE.value == "lakehouse"
        assert ConnectionType.FILE.value == "file"
        assert ConnectionType.DATABASE.value == "database"
        assert ConnectionType.API.value == "api"
        assert ConnectionType.STREAMING.value == "streaming"
        assert ConnectionType.FUNCTION.value == "function"


# ============================================================================
# Connection Type to Format Mapping
# ============================================================================


class TestConnectionTypeFormats:
    """Verify CONNECTION_TYPE_FORMATS mapping validity."""
    def test_all_connection_types_covered(self) -> None:
        for ct in ConnectionType:
            assert ct.value in CONNECTION_TYPE_FORMATS, f"{ct.value!r} missing from CONNECTION_TYPE_FORMATS"

    def test_file_formats(self) -> None:
        allowed = CONNECTION_TYPE_FORMATS[ConnectionType.FILE.value]
        assert Format.PARQUET.value in allowed
        assert Format.CSV.value in allowed
        assert Format.JSON.value in allowed
        assert Format.JSONL.value in allowed
        assert Format.AVRO.value in allowed
        assert Format.EXCEL.value in allowed
        assert Format.DELTA.value not in allowed
        assert Format.ICEBERG.value not in allowed

    def test_lakehouse_formats(self) -> None:
        allowed = CONNECTION_TYPE_FORMATS[ConnectionType.LAKEHOUSE.value]
        assert Format.DELTA.value in allowed
        assert Format.ICEBERG.value in allowed
        assert Format.PARQUET.value not in allowed

    def test_database_formats(self) -> None:
        allowed = CONNECTION_TYPE_FORMATS[ConnectionType.DATABASE.value]
        assert Format.SQL.value in allowed

    def test_api_formats(self) -> None:
        allowed = CONNECTION_TYPE_FORMATS[ConnectionType.API.value]
        assert Format.API.value in allowed

    def test_function_formats(self) -> None:
        allowed = CONNECTION_TYPE_FORMATS[ConnectionType.FUNCTION.value]
        assert Format.FUNCTION.value in allowed

    def test_streaming_empty(self) -> None:
        assert len(CONNECTION_TYPE_FORMATS[ConnectionType.STREAMING.value]) == 0

    def test_all_formats_in_exactly_one_connection_type(self) -> None:
        for fmt in Format:
            matches = [ct for ct, fmts in CONNECTION_TYPE_FORMATS.items() if fmt.value in fmts]
            assert len(matches) == 1, f"{fmt.value!r} in {len(matches)} types: {matches}"


# ============================================================================
# Processing and Status Enums
# ============================================================================


class TestProcessingMode:
    """Verify ProcessingMode enum values."""
    def test_values(self) -> None:
        assert ProcessingMode.BATCH.value == "batch"
        assert ProcessingMode.MICROBATCH.value == "microbatch"
        assert ProcessingMode.STREAMING.value == "streaming"


class TestDataFlowStatus:
    """Verify DataFlowStatus enum values."""
    def test_values(self) -> None:
        assert DataFlowStatus.PENDING.value == "pending"
        assert DataFlowStatus.RUNNING.value == "running"
        assert DataFlowStatus.SUCCEEDED.value == "succeeded"
        assert DataFlowStatus.FAILED.value == "failed"
        assert DataFlowStatus.SKIPPED.value == "skipped"


class TestExecutionType:
    """Verify ExecutionType enum values."""
    def test_values(self) -> None:
        assert ExecutionType.ETL.value == "etl"
        assert ExecutionType.MAINTENANCE.value == "maintenance"


class TestMaintenanceType:
    """Verify MaintenanceType enum values."""
    def test_values(self) -> None:
        assert MaintenanceType.COMPACT.value == "compact"
        assert MaintenanceType.CLEANUP.value == "cleanup"


# ============================================================================
# Column Case Mode
# ============================================================================


class TestColumnCaseMode:
    """Verify ColumnCaseMode enum values and string construction."""
    def test_values(self) -> None:
        assert ColumnCaseMode.LOWER.value == "lower"
        assert ColumnCaseMode.SNAKE.value == "snake"

    def test_str_enum(self) -> None:
        assert isinstance(ColumnCaseMode.LOWER, str)
        assert ColumnCaseMode.LOWER == "lower"

    def test_all_members(self) -> None:
        assert len(ColumnCaseMode) == 2

    def test_construct_from_string(self) -> None:
        assert ColumnCaseMode("lower") is ColumnCaseMode.LOWER
        assert ColumnCaseMode("snake") is ColumnCaseMode.SNAKE


# ============================================================================
# Logging Constants
# ============================================================================


class TestLogType:
    """Verify LogType enum values."""
    def test_values(self) -> None:
        assert LogType.JOB_RUN_LOG.value == "job_run_log"
        assert LogType.DATAFLOW_RUN_LOG.value == "dataflow_run_log"


class TestLogPurpose:
    """Verify LogPurpose enum values."""
    def test_values(self) -> None:
        assert LogPurpose.DEBUG.value == "debug_json"
        assert LogPurpose.ANALYST.value == "analyst"


# ============================================================================
# System Constants and Defaults
# ============================================================================


class TestSystemConstants:
    """Verify system column names and default configuration values."""
    def test_system_columns(self) -> None:
        assert SystemColumn.CREATED_AT == "__created_at"
        assert SystemColumn.UPDATED_AT == "__updated_at"
        assert SystemColumn.UPDATED_BY == "__updated_by"

    def test_file_info_columns(self) -> None:
        assert FileInfoColumn.FILE_NAME == "__file_name"
        assert FileInfoColumn.FILE_PATH == "__file_path"
        assert FileInfoColumn.FILE_MODIFICATION_TIME == "__file_modification_time"

    def test_defaults(self) -> None:
        assert DEFAULT_AUTHOR == "DataCoolie"
        assert DEFAULT_MAX_WORKERS == 8
        assert DEFAULT_RETRY_COUNT == 0
        assert DEFAULT_RETRY_DELAY == 5.0
        assert DEFAULT_RETENTION_HOURS == 168

    def test_watermark_constants(self) -> None:
        assert WATERMARK_FILE_NAME == "watermark_value.json"
        assert DATETIME_PATTERN == "__datetime__"
        assert DATE_FOLDER_PARTITION_KEY == "__date_folder_partition__"

