"""Tests for datacoolie.engines.base — ABC contract tests."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines.base import BaseEngine


class StubEngine(BaseEngine[dict]):
    """Minimal concrete subclass for testing the ABC contract."""

    # --- Read ---
    def read_parquet(self, path, options=None):
        raise NotImplementedError

    def read_delta(self, path, options=None):
        raise NotImplementedError

    def read_iceberg(self, path, options=None):
        raise NotImplementedError

    def read_csv(self, path, options=None):
        raise NotImplementedError

    def read_json(self, path, options=None):
        raise NotImplementedError

    def read_jsonl(self, path, options=None):
        raise NotImplementedError

    def read_avro(self, path, options=None):
        raise NotImplementedError

    def read_excel(self, path, options=None):
        raise NotImplementedError

    def read_path(self, path, fmt, options=None):
        raise NotImplementedError

    def read_database(self, *, table=None, query=None, options=None):
        raise NotImplementedError

    def create_dataframe(self, records):
        raise NotImplementedError

    def execute_sql(self, sql, parameters=None):
        raise NotImplementedError

    def read_table(self, table_name, fmt="delta", options=None):
        raise NotImplementedError

    # --- Write ---
    def write_to_path(self, df, path, mode, fmt, partition_columns=None, options=None):
        raise NotImplementedError

    def merge_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
        raise NotImplementedError

    def merge_overwrite_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
        raise NotImplementedError

    def write_to_table(self, df, table_name, mode, fmt, partition_columns=None, options=None):
        raise NotImplementedError

    def merge_to_table(self, df, table_name, merge_keys, fmt, partition_columns=None, options=None):
        raise NotImplementedError

    def merge_overwrite_to_table(self, df, table_name, merge_keys, fmt="delta", partition_columns=None, options=None):
        raise NotImplementedError

    # --- Transform ---
    def add_column(self, df, column_name, expression):
        raise NotImplementedError

    def drop_columns(self, df, columns):
        return {k: v for k, v in df.items() if k not in columns}

    def select_columns(self, df, columns):
        return {c: df[c] for c in columns if c in df}

    def rename_column(self, df, old_name, new_name):
        raise NotImplementedError

    def filter_rows(self, df, condition):
        raise NotImplementedError

    def apply_watermark_filter(self, df, watermark_columns, watermark):
        raise NotImplementedError

    def deduplicate(self, df, partition_columns, order_columns=None, keep="first"):
        raise NotImplementedError

    def deduplicate_by_rank(self, df, partition_columns, order_columns, keep="first"):
        raise NotImplementedError

    def cast_column(self, df, column_name, target_type, fmt=None):
        raise NotImplementedError

    # --- System ---
    def add_system_columns(self, df, author=None):
        raise NotImplementedError

    def add_file_info_columns(self, df):
        raise NotImplementedError

    def convert_timestamp_ntz_to_timestamp(self, df):
        return df

    def generate_symlink_manifest(self, path):
        pass

    # --- Metrics ---
    def count_rows(self, df):
        raise NotImplementedError

    def is_empty(self, df):
        raise NotImplementedError

    def get_columns(self, df):
        return list(df.keys())

    def get_schema(self, df):
        raise NotImplementedError

    def get_hive_schema(self, df):
        raise NotImplementedError

    def get_max_values(self, df, columns):
        raise NotImplementedError

    def get_count_and_max_values(self, df, columns):
        raise NotImplementedError

    # --- Table ops ---
    def table_exists_by_path(self, path, *, fmt="delta"):
        raise NotImplementedError

    def table_exists_by_name(self, table_name, *, fmt="delta"):
        raise NotImplementedError

    def get_history_by_path(self, path, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        raise NotImplementedError

    def compact_by_path(self, path, *, fmt="delta", options=None):
        raise NotImplementedError

    def cleanup_by_path(self, path, retention_hours=168, *, fmt="delta", options=None):
        raise NotImplementedError

    # --- Table ops by name ---
    def get_history_by_name(self, table_name, limit=1, start_time=None, end_time=None, *, fmt="delta"):
        raise NotImplementedError

    def compact_by_name(self, table_name, *, fmt="delta", options=None):
        raise NotImplementedError

    def cleanup_by_name(self, table_name, retention_hours=168, *, fmt="delta", options=None):
        raise NotImplementedError


class TestBaseEngineContract:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BaseEngine()  # type: ignore[abstract]

    def test_stub_instantiates(self) -> None:
        engine = StubEngine()
        assert isinstance(engine, BaseEngine)

    def test_abstract_method_count(self) -> None:
        # 33 abstract methods remaining after metrics moved to writers
        abstracts = getattr(BaseEngine, "__abstractmethods__", frozenset())
        assert len(abstracts) >= 31


class TestConcreteDefaultMethods:
    """Test the concrete methods on BaseEngine (remove_system_columns, convert_timestamp_ntz)."""

    def test_format_extensions_excel_maps_to_xlsx(self) -> None:
        engine = StubEngine()
        assert engine.FORMAT_EXTENSIONS["excel"] == ".xlsx"

    def test_format_extensions_fallback(self) -> None:
        engine = StubEngine()
        assert engine.FORMAT_EXTENSIONS.get("parquet", ".parquet") == ".parquet"

    def test_remove_system_columns_drops_existing(self) -> None:
        engine = StubEngine()
        df = {
            "id": 1,
            "name": "test",
            "__created_at": "2026-01-01",
            "__updated_at": "2026-01-01",
            "__updated_by": "bot",
        }
        result = engine.remove_system_columns(df)
        assert "__created_at" not in result
        assert "__updated_at" not in result
        assert "__updated_by" not in result
        assert result["id"] == 1

    def test_remove_system_columns_noop_when_absent(self) -> None:
        engine = StubEngine()
        df = {"id": 1, "name": "test"}
        result = engine.remove_system_columns(df)
        assert result == df

    def test_convert_timestamp_ntz_default_noop(self) -> None:
        engine = StubEngine()
        df = {"col": "value"}
        assert engine.convert_timestamp_ntz_to_timestamp(df) is df


class TestNavigationMethods:
    """Test the concrete navigation dispatchers on BaseEngine."""

    class _Nav(StubEngine):
        """StubEngine that records which primitive was called last."""

        last_route: Optional[str] = None

        def read_table(self, table_name, fmt="delta", options=None):
            self.last_route = "read_table"
            return {}

        def read_iceberg(self, path, options=None):
            self.last_route = "read_iceberg"
            return {}

        def read_path(self, path, fmt, options=None):
            self.last_route = "read_path"
            return {}

        def read_delta(self, path, options=None):
            self.last_route = "read_delta"
            return {}

        def read_parquet(self, path, options=None):
            self.last_route = "read_parquet"
            return {}

        def read_avro(self, path, options=None):
            self.last_route = "read_avro"
            return {}

        def read_excel(self, path, options=None):
            self.last_route = "read_excel"
            return {}

        def write_to_table(self, df, table_name, mode, fmt, partition_columns=None, options=None):
            self.last_route = "write_to_table"

        def write_to_path(self, df, path, mode, fmt, partition_columns=None, options=None):
            self.last_route = "write_to_path"

        def merge_to_table(self, df, table_name, merge_keys, fmt, partition_columns=None, options=None):
            self.last_route = "merge_to_table"

        def merge_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
            self.last_route = "merge_to_path"

        def merge_overwrite_to_table(self, df, table_name, merge_keys, fmt="delta", partition_columns=None, options=None):
            self.last_route = "merge_overwrite_to_table"

        def merge_overwrite_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None):
            self.last_route = "merge_overwrite_to_path"

        def table_exists_by_name(self, table_name, *, fmt="delta"):
            return True

        def table_exists_by_path(self, path, *, fmt="delta"):
            return False

        def get_history_by_path(self, path, limit=1, start_time=None, end_time=None, *, fmt="delta"):
            self.last_route = "get_table_history"
            return []

        def get_history_by_name(self, table_name, limit=1, start_time=None, end_time=None, *, fmt="delta"):
            self.last_route = "get_table_history_by_name"
            return []

        def compact_by_path(self, path, *, fmt="delta", options=None):
            self.last_route = "compact_table"

        def compact_by_name(self, table_name, *, fmt="delta", options=None):
            self.last_route = "compact_table_by_name"

        def cleanup_by_path(self, path, retention_hours=168, *, fmt="delta", options=None):
            self.last_route = "cleanup_table"

        def cleanup_by_name(self, table_name, retention_hours=168, *, fmt="delta", options=None):
            self.last_route = "cleanup_table_by_name"

    # --- write() ---

    def test_write_prefers_table_name(self) -> None:
        e = self._Nav()
        e.write({}, table_name="cat.db.tbl", path="/some/path", mode="overwrite", fmt="delta")
        assert e.last_route == "write_to_table"

    def test_write_falls_back_to_path(self) -> None:
        e = self._Nav()
        e.write({}, path="/some/path", mode="overwrite", fmt="delta")
        assert e.last_route == "write_to_path"

    def test_write_raises_without_target(self) -> None:
        e = self._Nav()
        with pytest.raises(EngineError, match=r"write\(\) requires"):
            e.write({}, mode="overwrite", fmt="delta")

    # --- merge() ---

    def test_merge_prefers_table_name(self) -> None:
        e = self._Nav()
        e.merge({}, table_name="cat.db.tbl", merge_keys=["id"])
        assert e.last_route == "merge_to_table"

    def test_merge_falls_back_to_path(self) -> None:
        e = self._Nav()
        e.merge({}, path="/some/path", merge_keys=["id"])
        assert e.last_route == "merge_to_path"

    def test_merge_raises_without_target(self) -> None:
        e = self._Nav()
        with pytest.raises(EngineError, match=r"merge\(\) requires"):
            e.merge({}, merge_keys=["id"])

    # --- merge_overwrite() ---

    def test_merge_overwrite_prefers_table_name(self) -> None:
        e = self._Nav()
        e.merge_overwrite({}, table_name="cat.db.tbl", merge_keys=["id"])
        assert e.last_route == "merge_overwrite_to_table"

    def test_merge_overwrite_falls_back_to_path(self) -> None:
        e = self._Nav()
        e.merge_overwrite({}, path="/some/path", merge_keys=["id"])
        assert e.last_route == "merge_overwrite_to_path"

    def test_merge_overwrite_raises_without_target(self) -> None:
        e = self._Nav()
        with pytest.raises(EngineError, match=r"merge_overwrite\(\) requires"):
            e.merge_overwrite({}, merge_keys=["id"])

    # --- exists() ---

    def test_exists_by_table_name(self) -> None:
        e = self._Nav()
        assert e.exists(table_name="cat.db.tbl") is True

    def test_exists_by_path_returns_false(self) -> None:
        e = self._Nav()
        # stub table_exists(path) returns False
        assert e.exists(path="/some/path") is False

    def test_exists_no_args_returns_false(self) -> None:
        e = self._Nav()
        assert e.exists() is False

    # --- read() ---

    def test_read_prefers_table_name(self) -> None:
        e = self._Nav()
        e.read("delta", table_name="cat.db.tbl")
        assert e.last_route == "read_table"

    def test_read_known_format_uses_specific_reader(self) -> None:
        e = self._Nav()
        e.read("delta", path="/some/path")
        assert e.last_route == "read_delta"

    def test_read_parquet_format_uses_specific_reader(self) -> None:
        e = self._Nav()
        e.read("parquet", path="/some/path")
        assert e.last_route == "read_parquet"

    def test_read_excel_format_uses_specific_reader(self) -> None:
        e = self._Nav()
        e.read("excel", path="/some/path")
        assert e.last_route == "read_excel"

    def test_read_unknown_format_falls_back_to_read_path(self) -> None:
        e = self._Nav()
        e.read("xml", path="/some/path")
        assert e.last_route == "read_path"

    def test_read_raises_without_target(self) -> None:
        e = self._Nav()
        with pytest.raises(EngineError, match=r"read\(\) requires"):
            e.read("delta")

    # --- get_history() ---

    def test_get_history_prefers_table_name(self) -> None:
        e = self._Nav()
        e.get_history(table_name="cat.db.tbl", path="/some/path")
        assert e.last_route == "get_table_history_by_name"

    def test_get_history_falls_back_to_path(self) -> None:
        e = self._Nav()
        e.get_history(path="/some/path")
        assert e.last_route == "get_table_history"

    def test_get_history_raises_without_target(self) -> None:
        e = self._Nav()
        with pytest.raises(EngineError, match=r"get_history\(\) requires"):
            e.get_history()

    # --- compact() ---

    def test_compact_prefers_table_name(self) -> None:
        e = self._Nav()
        e.compact(table_name="cat.db.tbl", path="/some/path")
        assert e.last_route == "compact_table_by_name"

    def test_compact_falls_back_to_path(self) -> None:
        e = self._Nav()
        e.compact(path="/some/path")
        assert e.last_route == "compact_table"

    def test_compact_raises_without_target(self) -> None:
        e = self._Nav()
        with pytest.raises(EngineError, match=r"compact\(\) requires"):
            e.compact()

    # --- cleanup() ---

    def test_cleanup_prefers_table_name(self) -> None:
        e = self._Nav()
        e.cleanup(table_name="cat.db.tbl", path="/some/path")
        assert e.last_route == "cleanup_table_by_name"

    def test_cleanup_falls_back_to_path(self) -> None:
        e = self._Nav()
        e.cleanup(path="/some/path")
        assert e.last_route == "cleanup_table"

    def test_cleanup_raises_without_target(self) -> None:
        e = self._Nav()
        with pytest.raises(EngineError, match=r"cleanup\(\) requires"):
            e.cleanup()
