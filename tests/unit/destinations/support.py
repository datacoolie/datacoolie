"""Shared test scaffolding for destinations tests.

Non-test module (no test_ prefix) containing:
  - MockEngine: comprehensive mock for BaseEngine[dict]
  - DataFlow builders: _make_dataflow, _make_file_dataflow
  - Engine fixture: engine()
"""

from __future__ import annotations

from typing import Any, Dict

import pytest

from datacoolie.core.constants import (
    Format,
    LoadType,
)
from datacoolie.core.models import (
    Connection,
    DataFlow,
    Destination,
    PartitionColumn,
    Source,
    Transform,
)
from datacoolie.engines.base import BaseEngine


# ============================================================================
# Mock engine — full BaseEngine[dict] implementation
# ============================================================================


class MockEngine(BaseEngine[dict]):
    """Mock engine for destination tests.
    
    Tracks all write operations, merges, compactions, cleanups, and SQL executions.
    Supports configurable state (table_exists_result) for testing conditional logic.
    """

    def __init__(self) -> None:
        self._table_exists_result: bool = False
        self._written: list = []
        self._merged: list = []
        self._merge_overwritten: list = []
        self._scd2: list = []
        self._compacted: list = []
        self._cleaned: list = []
        self._executed_sql: list = []

    def set_table_exists(self, exists: bool) -> None:
        """Control whether table_exists methods return True or False."""
        self._table_exists_result = exists

    def create_dataframe(self, records):
        return records

    # --- Read ---
    def read_parquet(self, path, options=None): return {}
    def read_delta(self, path, options=None): return {}
    def read_iceberg(self, path, options=None): return {}
    def read_csv(self, path, options=None): return {}
    def read_json(self, path, options=None): return {}
    def read_jsonl(self, path, options=None): return {}
    def read_avro(self, path, options=None): return {}
    def read_excel(self, path, options=None): return {}
    def read_path(self, path, fmt, options=None): return {}
    def read_database(self, *, table=None, query=None, options=None): return {}
    def execute_sql(self, sql, parameters=None):
        self._executed_sql.append(sql)
        return {}

    def read_table(self, table_name, fmt="delta", options=None):
        return {}

    # --- Write ---
    def write_to_path(self, df, path, mode, fmt, partition_columns=None, options=None):
        self._written.append({"path": path, "mode": mode, "fmt": fmt})

    def merge_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
        self._merged.append({"path": path, "merge_keys": merge_keys})

    def merge_overwrite_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
        self._merge_overwritten.append({"path": path, "merge_keys": merge_keys})

    def write_to_table(self, df, table_name, mode, fmt, partition_columns=None, options=None):
        self._written.append({"table_name": table_name, "mode": mode, "fmt": fmt})

    def merge_to_table(self, df, table_name, merge_keys, fmt, partition_columns=None, options=None):
        self._merged.append({"table_name": table_name, "merge_keys": merge_keys})

    def merge_overwrite_to_table(self, df, table_name, merge_keys, fmt="delta", partition_columns=None, options=None):
        self._merge_overwritten.append({"table_name": table_name, "merge_keys": merge_keys})

    def scd2_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
        self._scd2.append({"path": path, "merge_keys": merge_keys})

    def scd2_to_table(self, df, table_name, merge_keys, fmt="delta", partition_columns=None, options=None):
        self._scd2.append({"table_name": table_name, "merge_keys": merge_keys})

    # --- Transform ---
    def add_column(self, df, column_name, expression): return df
    def drop_columns(self, df, columns): return df
    def select_columns(self, df, columns): return df
    def rename_column(self, df, old_name, new_name): return df
    def filter_rows(self, df, condition): return df
    def apply_watermark_filter(self, df, watermark_columns, watermark): return df
    def deduplicate(self, df, partition_columns, order_columns=None, order="desc"): return df
    def deduplicate_by_rank(self, df, partition_columns, order_columns, order="desc"): return df
    def cast_column(self, df, column_name, target_type, fmt=None): return df

    # --- System ---
    def add_system_columns(self, df, author=None): return df
    def add_file_info_columns(self, df): return df
    def convert_timestamp_ntz_to_timestamp(self, df): return df
    def generate_symlink_manifest(self, path): pass

    # --- Metrics ---
    def count_rows(self, df): return 0
    def is_empty(self, df): return True
    def get_columns(self, df): return []
    def get_schema(self, df): return {}
    def get_hive_schema(self, df): return {}
    def get_max_values(self, df, columns): return {}
    def get_count_and_max_values(self, df, columns): return (0, {})

    # --- Table ops ---
    def table_exists_by_path(self, path, *, fmt="delta"):
        return self._table_exists_result

    def table_exists_by_name(self, table_name, *, fmt="delta"):
        return self._table_exists_result

    def get_history_by_path(self, path, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        return [{"version": 0}][:limit]

    def compact_by_path(self, path, *, fmt="delta", options=None):
        self._compacted.append(path)

    def cleanup_by_path(self, path, retention_hours=168, *, fmt="delta", options=None):
        self._cleaned.append({"path": path, "hours": retention_hours})

    # --- Table ops by name ---
    def get_history_by_name(self, table_name, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        return [{"version": 0}][:limit]

    def compact_by_name(self, table_name, *, fmt="delta", options=None):
        self._compacted.append(table_name)

    def cleanup_by_name(self, table_name, retention_hours=168, *, fmt="delta", options=None):
        self._cleaned.append({"table_name": table_name, "hours": retention_hours})


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture()
def engine() -> MockEngine:
    """Provide a fresh MockEngine for each test."""
    return MockEngine()


# ============================================================================
# DataFlow builders
# ============================================================================


def _make_dataflow(
    load_type: str = LoadType.OVERWRITE.value,
    merge_keys: list | None = None,
    partition_cols: list | None = None,
    dest_format: str = Format.DELTA.value,
    has_path: bool = True,
    dest_configure: Dict[str, Any] | None = None,
) -> DataFlow:
    """Build a DataFlow targeting a table destination (delta/iceberg).
    
    Args:
        load_type: LoadType string value (OVERWRITE, APPEND, MERGE_UPSERT, etc.)
        merge_keys: List of merge key column names (for merge operations)
        partition_cols: List of PartitionColumn objects or None
        dest_format: Destination format (DELTA, ICEBERG, etc.)
        has_path: Whether to add base_path to destination config
        dest_configure: Extra entries for Destination.configure dict
        
    Returns:
        Configured DataFlow ready for testing destination writers.
    """
    base_path = "/data/silver" if has_path else ""
    dst_config: Dict[str, Any] = {"format": dest_format}
    if has_path:
        dst_config["base_path"] = base_path
    dst_conn = Connection(
        name="dst",
        format=dest_format,
        configure=dst_config,
    )
    dest_cfg: Dict[str, Any] = {}
    if dest_configure:
        dest_cfg.update(dest_configure)
    src_conn = Connection(
        name="src",
        connection_type="lakehouse",
        format=Format.DELTA.value,
        configure={"base_path": "/data/bronze"},
    )
    return DataFlow(
        name="test_flow",
        source=Source(connection=src_conn, schema_name="raw", table="orders"),
        destination=Destination(
            connection=dst_conn,
            schema_name="curated",
            table="dim_orders",
            load_type=load_type,
            merge_keys=merge_keys or [],
            partition_columns=partition_cols or [],
            configure=dest_cfg,
        ),
        transform=Transform(),
    )


def _make_file_dataflow(
    load_type: str = LoadType.OVERWRITE.value,
    dest_format: str = Format.PARQUET.value,
    has_path: bool = True,
    partition_cols: list | None = None,
    date_folder_partitions: str | None = None,
) -> DataFlow:
    """Build a DataFlow targeting a flat-file destination.
    
    Args:
        load_type: LoadType string value (OVERWRITE, APPEND, etc.)
        dest_format: File format (PARQUET, CSV, JSON, etc.)
        has_path: Whether to add base_path to destination config
        partition_cols: List of PartitionColumn objects or None
        date_folder_partitions: Date folder partition pattern (e.g., "{year}/{month}/{day}")
        
    Returns:
        Configured DataFlow for file-based destination testing.
    """
    dst_config: Dict[str, Any] = {"format": dest_format}
    if has_path:
        dst_config["base_path"] = "/data/output"
    if date_folder_partitions:
        dst_config["date_folder_partitions"] = date_folder_partitions

    dst_conn = Connection(
        name="file_dst",
        connection_type="file",
        format=dest_format,
        configure=dst_config,
    )
    src_conn = Connection(
        name="src",
        connection_type="file",
        format=Format.PARQUET.value,
        configure={"base_path": "/data/input"},
    )
    return DataFlow(
        name="file_flow",
        source=Source(connection=src_conn, schema_name="raw", table="events"),
        destination=Destination(
            connection=dst_conn,
            schema_name="curated",
            table="dim_events",
            load_type=load_type,
            partition_columns=partition_cols or [],
        ),
        transform=Transform(),
    )
