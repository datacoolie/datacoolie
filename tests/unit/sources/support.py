"""Shared test infrastructure for datacoolie.sources tests."""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import (
    DATE_FOLDER_PARTITION_KEY,
    Format,
)
from datacoolie.core.models import Connection, Source
from datacoolie.engines.base import BaseEngine
from datacoolie.sources.base import BaseSourceReader


# ============================================================================
# MockEngine
# ============================================================================


class MockEngine(BaseEngine[dict]):
    """Mock engine for testing source readers."""

    def __init__(self) -> None:
        super().__init__()
        self._data: dict = {"id": [1, 2, 3], "value": [10, 20, 30]}
        self._row_count: int = 3
        self._max_values: Dict[str, Any] = {}
        self._filtered: bool = False
        self._table_exists_result: bool = True
        self._history: list = []
        self._columns: List[str] = ["id", "value"]
        self._platform: Any = None

    # Configurable behavior
    def set_data(self, data: dict, row_count: int = 3) -> None:
        self._data = data
        self._row_count = row_count

    def set_max_values(self, max_vals: Dict[str, Any]) -> None:
        self._max_values = max_vals

    def set_table_exists(self, exists: bool) -> None:
        self._table_exists_result = exists

    def set_history(self, history: list) -> None:
        self._history = history

    def set_columns(self, columns: List[str]) -> None:
        self._columns = columns

    def set_platform(self, platform: Any) -> None:
        self._platform = platform

    def get_platform(self):
        return self._platform

    def create_dataframe(self, records):
        merged = {}
        for r in records:
            for k in r:
                merged.setdefault(k, None)
        return {k: [r.get(k) for r in records] for k in merged}

    # --- Read ---
    def read_parquet(self, path, options=None):
        return self._data

    def read_delta(self, path, options=None):
        return self._data

    def read_iceberg(self, path, options=None):
        return self._data

    def read_csv(self, path, options=None):
        return self._data

    def read_json(self, path, options=None):
        return self._data

    def read_jsonl(self, path, options=None):
        return self._data

    def read_avro(self, path, options=None):
        return self._data

    def read_excel(self, path, options=None):
        return self._data

    def read_path(self, path, fmt, options=None):
        _supported = {"delta", "iceberg", "parquet", "csv", "json", "excel"}
        if fmt.lower() not in _supported:
            raise Exception(f"Unsupported format: {fmt!r}")
        return self._data

    def read_database(self, *, table=None, query=None, options=None):
        self._last_table = table
        self._last_query = query
        return self._data

    def execute_sql(self, sql, parameters=None):
        return self._data

    def read_table(self, table_name, fmt="delta", options=None):
        return self._data

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
        return df

    def drop_columns(self, df, columns):
        return {k: v for k, v in df.items() if k not in columns}

    def select_columns(self, df, columns):
        return {c: df[c] for c in columns if c in df}

    def rename_column(self, df, old_name, new_name):
        result = dict(df)
        if old_name in result:
            result[new_name] = result.pop(old_name)
        return result

    def filter_rows(self, df, condition):
        self._filtered = True
        return df

    def apply_watermark_filter(self, df, watermark_columns, watermark):
        self._filtered = True
        return df

    def deduplicate(self, df, partition_columns, order_columns=None, order="desc"):
        return df

    def deduplicate_by_rank(self, df, partition_columns, order_columns, order="desc"):
        return df

    def cast_column(self, df, column_name, target_type, fmt=None):
        return df

    # --- System ---
    def add_system_columns(self, df, author=None):
        result = dict(df)
        result["__created_at"] = "now"
        result["__updated_at"] = "now"
        result["__updated_by"] = author or "test"
        return result

    def add_file_info_columns(self, df, file_infos=None):
        result = dict(df)
        if file_infos:
            result["__file_name"] = file_infos[0].name
            result["__file_path"] = file_infos[0].path
            result["__file_modification_time"] = file_infos[0].modification_time
        else:
            result["__file_name"] = "test.parquet"
            result["__file_path"] = "/path/test.parquet"
            result["__file_modification_time"] = "2024-01-01"
        return result

    # --- Metrics ---
    def count_rows(self, df):
        return self._row_count

    def is_empty(self, df):
        return self._row_count == 0

    def get_columns(self, df):
        return self._columns

    def get_schema(self, df):
        return {c: "string" for c in self._columns}

    def get_max_values(self, df, columns):
        return {c: self._max_values.get(c, 0) for c in columns}

    def get_count_and_max_values(self, df, columns):
        return self._row_count, {c: self._max_values.get(c, 0) for c in columns}

    # --- Table ops ---
    def table_exists_by_path(self, path, *, fmt="delta"):
        return self._table_exists_result

    def table_exists_by_name(self, table_name, *, fmt="delta"):
        return self._table_exists_result

    def get_history_by_path(self, path, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        return self._history[:limit]

    def compact_by_path(self, path, *, fmt="delta", options=None):
        pass

    def cleanup_by_path(self, path, retention_hours=168, *, fmt="delta", options=None):
        pass

    # --- Table ops by name ---
    def get_history_by_name(self, table_name, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        return self._history[:limit]

    def compact_by_name(self, table_name, *, fmt="delta", options=None):
        pass

    def cleanup_by_name(self, table_name, retention_hours=168, *, fmt="delta", options=None):
        pass

    def get_hive_schema(self, df):
        return {c: "string" for c in self._columns}

    def convert_timestamp_ntz_to_timestamp(self, df):
        return df

    def generate_symlink_manifest(self, path):
        pass


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


@pytest.fixture()
def delta_source() -> Source:
    conn = Connection(
        name="bronze_delta",
        connection_type="lakehouse",
        format=Format.DELTA.value,
        configure={"base_path": "/data/bronze"},
    )
    return Source(
        connection=conn,
        schema_name="sales",
        table="orders",
        watermark_columns=["modified_at"],
    )


@pytest.fixture()
def query_source() -> Source:
    conn = Connection(
        name="sql_conn",
        connection_type="database",
        format=Format.SQL.value,
        configure={"base_path": "/data"},
    )
    return Source(
        connection=conn,
        query="SELECT * FROM orders WHERE active = 1",
        watermark_columns=["modified_at"],
    )


@pytest.fixture()
def file_source() -> Source:
    conn = Connection(
        name="bronze_parquet",
        connection_type="file",
        format=Format.PARQUET.value,
        configure={"base_path": "/data/raw"},
    )
    return Source(
        connection=conn,
        schema_name="events",
        table="clicks",
        watermark_columns=["event_time"],
    )


@pytest.fixture()
def file_source_csv() -> Source:
    conn = Connection(
        name="csv_conn",
        connection_type="file",
        format=Format.CSV.value,
        configure={"base_path": "/data/raw"},
    )
    return Source(
        connection=conn,
        schema_name="data",
        table="report",
    )


@pytest.fixture()
def db_source() -> Source:
    conn = Connection(
        name="source_db",
        connection_type="database",
        format=Format.SQL.value,
        database="mydb",
        configure={
            "host": "db.example.com",
            "port": 5432,
            "username": "reader",
            "password": "secret",
        },
    )
    return Source(
        connection=conn,
        schema_name="public",
        table="users",
        watermark_columns=["updated_at"],
    )


@pytest.fixture()
def date_folder_source() -> Source:
    conn = Connection(
        name="date_folder",
        connection_type="file",
        format=Format.PARQUET.value,
        configure={"base_path": "/data/events", "date_folder_partitions": "{year}/{month}/{day}"},
    )
    return Source(
        connection=conn,
        schema_name="raw",
        table="clicks",
        watermark_columns=["event_time", DATE_FOLDER_PARTITION_KEY],
    )


# ============================================================================
# Helper classes for testing base class
# ============================================================================


class ConcreteSourceReader(BaseSourceReader[dict]):
    """Concrete reader for testing the base class."""

    def __init__(self, engine, return_none: bool = False):
        super().__init__(engine)
        self._return_none = return_none

    def _read_internal(self, source, watermark=None):
        if self._return_none:
            return None
        return self._engine.read_delta(source.path or "")

    def _read_data(self, source, configure=None):
        return self._engine.read_delta(source.path or "")


class FailingSourceReader(BaseSourceReader[dict]):
    """Reader that always raises an exception."""

    def _read_internal(self, source, watermark=None):
        raise RuntimeError("Boom!")

    def _read_data(self, source, configure=None):
        raise RuntimeError("Boom!")
