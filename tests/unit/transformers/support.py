"""Support utilities for transformer unit tests.

This module intentionally avoids the `test_` prefix so tests can safely
import from it without cross-test-module coupling.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from datacoolie.core.constants import DEFAULT_AUTHOR, Format, LoadType
from datacoolie.core.models import (
    AdditionalColumn,
    Connection,
    DataFlow,
    Destination,
    PartitionColumn,
    SchemaHint,
    Source,
    Transform,
)
from datacoolie.engines.base import BaseEngine


class MockEngine(BaseEngine[dict]):
    """Shared mock engine for transformer tests."""

    def __init__(self) -> None:
        self._columns: List[str] = ["id", "name", "amount", "order_date"]
        self._casts: List[tuple] = []
        self._added_columns: List[tuple] = []
        self._removed_system: bool = False
        self._renamed: List[tuple] = []
        self._deduplicated: bool = False
        self._dedup_by_rank: bool = False

    def set_columns(self, columns: List[str]) -> None:
        self._columns = columns

    def create_dataframe(self, records):
        merged = {}
        for r in records:
            for k in r:
                merged.setdefault(k, None)
        return {k: [r.get(k) for r in records] for k in merged}

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
    def execute_sql(self, sql, parameters=None): return {}

    def read_table(self, table_name, fmt="delta", options=None): return {}

    # --- Write ---
    def write_to_path(self, df, path, mode, fmt, partition_columns=None, options=None):
        pass

    def merge_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
        pass

    def merge_overwrite_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
        pass

    def write_to_table(self, df, table_name, mode, fmt, partition_columns=None, options=None):
        pass

    def merge_to_table(self, df, table_name, merge_keys, fmt, partition_columns=None, options=None):
        pass

    def merge_overwrite_to_table(self, df, table_name, merge_keys, fmt="delta", partition_columns=None, options=None):
        pass

    # --- Transform ---
    def add_column(self, df, column_name, expression):
        self._added_columns.append((column_name, expression))
        result = dict(df)
        result[column_name] = f"expr:{expression}"
        return result

    def drop_columns(self, df, columns):
        return {k: v for k, v in df.items() if k not in columns}

    def select_columns(self, df, columns):
        return {c: df[c] for c in columns if c in df}

    def rename_column(self, df, old_name, new_name):
        self._renamed.append((old_name, new_name))
        result = dict(df)
        if old_name in result:
            result[new_name] = result.pop(old_name)
        return result

    def filter_rows(self, df, condition):
        return df

    def apply_watermark_filter(self, df, watermark_columns, watermark):
        return df

    def deduplicate(self, df, partition_columns, order_columns=None, order="desc"):
        self._deduplicated = True
        return df

    def deduplicate_by_rank(self, df, partition_columns, order_columns, order="desc"):
        self._dedup_by_rank = True
        return df

    def cast_column(self, df, column_name, target_type, fmt=None):
        self._casts.append((column_name, target_type, fmt))
        return df

    # --- System ---
    def add_system_columns(self, df, author=None):
        result = dict(df)
        result["__created_at"] = "now"
        result["__updated_at"] = "now"
        result["__updated_by"] = author or DEFAULT_AUTHOR
        return result

    def add_file_info_columns(self, df, file_infos=None):
        return df

    def remove_system_columns(self, df):
        self._removed_system = True
        return super().remove_system_columns(df)

    def convert_timestamp_ntz_to_timestamp(self, df):
        return df

    def generate_symlink_manifest(self, path):
        pass

    # --- Metrics ---
    def count_rows(self, df): return 0
    def is_empty(self, df): return True
    def get_columns(self, df): return self._columns
    def get_schema(self, df): return {c: "string" for c in self._columns}
    def get_hive_schema(self, df): return {c: "string" for c in self._columns}
    def get_max_values(self, df, columns): return {}
    def get_count_and_max_values(self, df, columns): return (0, {})

    # --- Table ops ---
    def table_exists_by_path(self, path, *, fmt="delta"): return False
    def table_exists_by_name(self, table_name, *, fmt="delta"): return False
    def get_history_by_path(self, path, limit=1, start_time=None, end_time=None, *, fmt="delta"): return []
    def compact_by_path(self, path, *, fmt="delta", options=None): pass
    def cleanup_by_path(self, path, retention_hours=168, *, fmt="delta", options=None): pass

    # --- Table ops by name ---
    def get_history_by_name(self, table_name, limit=1, start_time=None, end_time=None, *, fmt="delta"): return []
    def compact_by_name(self, table_name, *, fmt="delta", options=None): pass
    def cleanup_by_name(self, table_name, retention_hours=168, *, fmt="delta", options=None): pass


def make_dataflow(
    load_type: str = LoadType.MERGE_UPSERT.value,
    merge_keys: Optional[List[str]] = None,
    dedup_cols: Optional[List[str]] = None,
    latest_cols: Optional[List[str]] = None,
    additional_cols: Optional[List[AdditionalColumn]] = None,
    schema_hints: Optional[List[SchemaHint]] = None,
    partition_cols: Optional[List[PartitionColumn]] = None,
    use_schema_hint: bool = True,
    watermark_cols: Optional[List[str]] = None,
    transform_configure: Optional[Dict[str, Any]] = None,
    deduplicate_by_rank: Optional[bool] = None,
) -> DataFlow:
    src_conn = Connection(
        name="src",
        connection_type="lakehouse",
        format=Format.DELTA.value,
        configure={"base_path": "/data/bronze", "use_schema_hint": use_schema_hint},
    )
    dst_conn = Connection(
        name="dst",
        connection_type="lakehouse",
        format=Format.DELTA.value,
        configure={"base_path": "/data/silver"},
    )

    cfg = dict(transform_configure or {})
    if deduplicate_by_rank is not None:
        cfg["deduplicate_by_rank"] = deduplicate_by_rank

    return DataFlow(
        name="test_flow",
        source=Source(
            connection=src_conn,
            schema_name="raw",
            table="orders",
            watermark_columns=watermark_cols or [],
        ),
        destination=Destination(
            connection=dst_conn,
            schema_name="curated",
            table="dim_orders",
            load_type=load_type,
            merge_keys=merge_keys or [],
            partition_columns=partition_cols or [],
        ),
        transform=Transform(
            deduplicate_columns=dedup_cols,
            latest_data_columns=latest_cols or [],
            additional_columns=additional_cols or [],
            schema_hints=schema_hints or [],
            configure=cfg,
        ),
    )
