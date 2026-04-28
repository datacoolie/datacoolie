"""Tests for datacoolie.engines.spark_engine — requires PySpark + Delta.

All tests in this module are marked with ``@pytest.mark.spark`` so they
can be excluded from fast unit test runs via ``-m "not spark"``.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
delta = pytest.importorskip("delta", reason="delta-spark not installed")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as sf  # noqa: E402

from datacoolie.core.constants import (  # noqa: E402
    DEFAULT_AUTHOR,
    FileInfoColumn,
    SystemColumn,
)
from datacoolie.core.exceptions import EngineError  # noqa: E402
from datacoolie.destinations.delta_writer import DeltaWriter  # noqa: E402
from datacoolie.engines.spark_engine import SparkEngine  # noqa: E402

# Mark every test in this module; group into one xdist worker to share the JVM.
pytestmark = [pytest.mark.spark, pytest.mark.xdist_group("spark")]


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Module-scoped local SparkSession with Delta."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("datacoolie-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        # Optimise for small-data unit tests
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def _data_dir(tmp_path_factory: pytest.TempPathFactory, spark: SparkSession) -> Path:
    """Write all test data to Parquet once, shared across the module."""
    d = tmp_path_factory.mktemp("spark_test_data")
    spark.createDataFrame(
        [(1, "Alice", "2026-01-01"), (2, "Bob", "2026-01-02"), (3, "Carol", "2026-01-03")],
        ["id", "name", "date_str"],
    ).write.parquet(str(d / "sample"))
    spark.createDataFrame(
        [(1, "x"), (2, "y")], ["id", "val"],
    ).write.parquet(str(d / "two_rows"))
    spark.createDataFrame(
        [(1, "a", 2026), (2, "b", 2025)], ["id", "val", "year"],
    ).write.parquet(str(d / "partition"))
    spark.createDataFrame(
        [(1, "Alice", 10), (2, "Bob", 20)], ["id", "name", "score"],
    ).write.parquet(str(d / "merge_seed_2"))
    spark.createDataFrame(
        [(1, "Alice_v2", 15), (3, "Carol", 30)], ["id", "name", "score"],
    ).write.parquet(str(d / "merge_upsert"))
    spark.createDataFrame(
        [(1, "Alice", 10), (2, "Bob", 20), (3, "Carol", 30)], ["id", "name", "score"],
    ).write.parquet(str(d / "merge_seed_3"))
    spark.createDataFrame(
        [(1, "Alice_v2", 15), (2, "Bob", 20), (4, "Dave", 40)], ["id", "name", "score"],
    ).write.parquet(str(d / "merge_overwrite"))
    spark.createDataFrame(
        [(1, "a", 10), (1, "a", 20), (2, "b", 30)], ["id", "name", "score"],
    ).write.parquet(str(d / "dedup"))
    spark.createDataFrame(
        [(1, 10), (1, 10), (1, 20), (2, 30)], ["id", "score"],
    ).write.parquet(str(d / "rank"))
    spark.createDataFrame(
        [(1, "2026-01-01")], ["id", "ts"],
    ).write.parquet(str(d / "timestamp"))
    return d


@pytest.fixture()
def engine(spark: SparkSession) -> SparkEngine:
    """SparkEngine backed by the local test session."""
    return SparkEngine(spark_session=spark)


@pytest.fixture(scope="module")
def sample_df(spark: SparkSession, _data_dir: Path) -> DataFrame:
    """Module-scoped sample DataFrame read from Parquet."""
    return spark.read.parquet(str(_data_dir / "sample"))


@pytest.fixture()
def delta_path(tmp_path: Path) -> str:
    """Path for a temporary Delta table."""
    return str(tmp_path / "delta_table")


@pytest.fixture(scope="module")
def parquet_path(_data_dir: Path) -> str:
    """Path with sample Parquet files (written once by _data_dir)."""
    return str(_data_dir / "two_rows")


# =====================================================================
# Read tests
# =====================================================================


class TestReadParquet:
    def test_read(self, engine: SparkEngine, parquet_path: str) -> None:
        df = engine.read_parquet(parquet_path)
        assert engine.count_rows(df) == 2
        assert "id" in engine.get_columns(df)

    def test_file_info_columns_added(self, engine: SparkEngine, parquet_path: str) -> None:
        df = engine.read_parquet(parquet_path)
        df = engine.add_file_info_columns(df)
        cols = engine.get_columns(df)
        assert FileInfoColumn.FILE_NAME in cols
        assert FileInfoColumn.FILE_PATH in cols


class TestReadDelta:
    def test_read(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        sample_df.write.format("delta").save(delta_path)
        df = engine.read_delta(delta_path)
        assert engine.count_rows(df) == 3


class TestReadCsv:
    def test_read(self, engine: SparkEngine, tmp_path: Path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
        df = engine.read_csv(str(csv_file))
        assert engine.count_rows(df) == 2


class TestReadJson:
    def test_read(self, engine: SparkEngine, tmp_path: Path) -> None:
        json_file = tmp_path / "data.json"
        json_file.write_text(
            json.dumps([{"id": 1, "name": "Alice"}]),
            encoding="utf-8",
        )
        df = engine.read_json(str(json_file))
        assert engine.count_rows(df) >= 1


# =====================================================================
# Write tests
# =====================================================================


class TestWriteTable:
    def test_overwrite(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        engine.write_to_path(sample_df, delta_path, "overwrite", "delta")
        # read back
        df = engine.read_delta(delta_path)
        assert engine.count_rows(df) == 3

    def test_append(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        engine.write_to_path(sample_df, delta_path, "overwrite", "delta")
        engine.write_to_path(sample_df, delta_path, "append", "delta")
        df = engine.read_delta(delta_path)
        assert engine.count_rows(df) == 6

    def test_with_partition(
        self, engine: SparkEngine, spark: SparkSession, delta_path: str, _data_dir: Path,
    ) -> None:
        df = spark.read.parquet(str(_data_dir / "partition"))
        engine.write_to_path(df, delta_path, "overwrite", "delta", partition_columns=["year"])
        result = engine.read_delta(delta_path)
        assert engine.count_rows(result) == 2


class TestMergeTable:
    def test_upsert(
        self, engine: SparkEngine, spark: SparkSession, delta_path: str, _data_dir: Path,
    ) -> None:
        # Seed
        initial = spark.read.parquet(str(_data_dir / "merge_seed_2"))
        initial = engine.add_system_columns(initial)
        engine.write_to_path(initial, delta_path, "overwrite", "delta")

        # Merge — update id=1, insert id=3
        incoming = spark.read.parquet(str(_data_dir / "merge_upsert"))
        incoming = engine.add_system_columns(incoming)
        engine.merge_to_path(incoming, delta_path, merge_keys=["id"])

        result = engine.read_delta(delta_path)
        assert engine.count_rows(result) == 3
        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["name"] == "Alice_v2"
        assert rows[3]["name"] == "Carol"

    def test_merge_no_table_raises(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        with pytest.raises(EngineError, match="does not exist"):
            engine.merge_to_path(sample_df, delta_path, merge_keys=["id"])


class TestMergeOverwrite:
    def test_rolling_overwrite(
        self, engine: SparkEngine, spark: SparkSession, delta_path: str, _data_dir: Path,
    ) -> None:
        # Seed
        initial = spark.read.parquet(str(_data_dir / "merge_seed_3"))
        initial = engine.add_system_columns(initial)
        engine.write_to_path(initial, delta_path, "overwrite", "delta")

        # Overwrite id=1 (updated), id=2 (unchanged), id=4 (new)
        incoming = spark.read.parquet(str(_data_dir / "merge_overwrite"))
        incoming = engine.add_system_columns(incoming)
        engine.merge_overwrite_to_path(incoming, delta_path, merge_keys=["id"])

        result = engine.read_delta(delta_path)
        ids = sorted([r["id"] for r in result.collect()])
        # id=1 deleted+reinserted, id=2 deleted+reinserted, id=3 remains, id=4 new
        assert 3 in ids
        assert 4 in ids


# =====================================================================
# Transform tests
# =====================================================================


class TestTransforms:
    def test_add_column(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.add_column(sample_df, "upper_name", "upper(name)")
        assert "upper_name" in engine.get_columns(result)

    def test_drop_columns(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.drop_columns(sample_df, ["date_str"])
        assert "date_str" not in engine.get_columns(result)
        assert "name" in engine.get_columns(result)

    def test_drop_nonexistent_is_noop(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.drop_columns(sample_df, ["no_such_col"])
        assert engine.count_rows(result) == 3

    def test_rename_column(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.rename_column(sample_df, "name", "full_name")
        assert "full_name" in engine.get_columns(result)
        assert "name" not in engine.get_columns(result)

    def test_filter_rows(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.filter_rows(sample_df, "id > 1")
        assert engine.count_rows(result) == 2

    def test_cast_column(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.cast_column(sample_df, "id", "string")
        schema = engine.get_schema(result)
        assert "string" in schema["id"].lower()

    def test_cast_date_with_format(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.cast_column(sample_df, "date_str", "date", fmt="yyyy-MM-dd")
        schema = engine.get_schema(result)
        assert "date" in schema["date_str"].lower()


class TestDeduplicate:
    def test_row_number(self, engine: SparkEngine, spark: SparkSession, _data_dir: Path) -> None:
        df = spark.read.parquet(str(_data_dir / "dedup"))
        result = engine.deduplicate(
            df, partition_columns=["id"], order_columns=["score"], order="first"
        )
        assert engine.count_rows(result) == 2
        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["score"] == 20  # desc order → highest first

    def test_rank(self, engine: SparkEngine, spark: SparkSession, _data_dir: Path) -> None:
        df = spark.read.parquet(str(_data_dir / "rank"))
        result = engine.deduplicate_by_rank(
            df, partition_columns=["id"], order_columns=["score"], order="first"
        )
        # rank keeps ties — id=1 has two rows with score=20? No: desc order means
        # score=20 has rank 1 (one row), score=10 has rank 2 (two rows)
        # So for id=1: only the row with score=20 survives
        rows_1 = [r for r in result.collect() if r["id"] == 1]
        assert all(r["score"] == 20 for r in rows_1)


# =====================================================================
# System columns
# =====================================================================


class TestSystemColumns:
    def test_add_system_columns(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.add_system_columns(sample_df)
        cols = engine.get_columns(result)
        assert SystemColumn.CREATED_AT in cols
        assert SystemColumn.UPDATED_AT in cols
        assert SystemColumn.UPDATED_BY in cols

    def test_default_author(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.add_system_columns(sample_df)
        row = result.select(SystemColumn.UPDATED_BY).first()
        assert row[0] == DEFAULT_AUTHOR

    def test_custom_author(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.add_system_columns(sample_df, author="tester")
        row = result.select(SystemColumn.UPDATED_BY).first()
        assert row[0] == "tester"

    def test_remove_system_columns(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        with_sys = engine.add_system_columns(sample_df)
        without = engine.remove_system_columns(with_sys)
        cols = engine.get_columns(without)
        for sys_col in SystemColumn:
            assert sys_col not in cols

    def test_convert_timestamp_ntz(self, engine: SparkEngine, spark: SparkSession, _data_dir: Path) -> None:
        df = spark.read.parquet(str(_data_dir / "timestamp"))
        df = df.withColumn("ts", sf.to_timestamp("ts"))
        # Just ensure no error
        result = engine.convert_timestamp_ntz_to_timestamp(df)
        assert engine.count_rows(result) == 1


# =====================================================================
# Metrics / inspection
# =====================================================================


class TestMetrics:
    def test_count_rows(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        assert engine.count_rows(sample_df) == 3

    def test_is_empty(self, engine: SparkEngine, spark: SparkSession, sample_df: DataFrame) -> None:
        assert engine.is_empty(sample_df) is False
        empty = sample_df.limit(0)
        assert engine.is_empty(empty) is True

    def test_get_columns(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        assert engine.get_columns(sample_df) == ["id", "name", "date_str"]

    def test_get_schema(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        schema = engine.get_schema(sample_df)
        assert "id" in schema
        assert "name" in schema

    def test_get_max_values(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        result = engine.get_max_values(sample_df, ["id"])
        assert result["id"] == 3

    def test_get_count_and_max(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        count, maxes = engine.get_count_and_max_values(sample_df, ["id"])
        assert count == 3
        assert maxes["id"] == 3


# =====================================================================
# Table operations
# =====================================================================


class TestTableOps:
    def test_table_exists_false(self, engine: SparkEngine, delta_path: str) -> None:
        assert engine.table_exists_by_path(delta_path) is False

    def test_table_exists_true(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        sample_df.write.format("delta").save(delta_path)
        assert engine.table_exists_by_path(delta_path) is True

    def test_get_table_history(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        sample_df.write.format("delta").save(delta_path)
        history = engine.get_history_by_path(delta_path, limit=1)
        assert len(history) == 1
        assert "operation" in history[0]

    def test_history_empty_when_no_table(self, engine: SparkEngine, delta_path: str) -> None:
        assert engine.get_history_by_path(delta_path) == []

    def test_history_with_start_time_filters(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        from datetime import datetime, timezone

        sample_df.write.format("delta").save(delta_path)
        # All entries should have timestamp >= a very old date
        old = datetime(2000, 1, 1, tzinfo=timezone.utc)
        history = engine.get_history_by_path(delta_path, limit=10, start_time=old)
        assert len(history) >= 1
        # A future start_time should yield nothing
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        history = engine.get_history_by_path(delta_path, limit=10, start_time=future)
        assert len(history) == 0

    def test_history_start_time_none_returns_all(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        sample_df.write.format("delta").save(delta_path)
        # start_time=None should behave like no filter
        h1 = engine.get_history_by_path(delta_path, limit=10)
        h2 = engine.get_history_by_path(delta_path, limit=10, start_time=None)
        assert len(h1) == len(h2)


class TestWriteMetrics:
    def test_metrics_after_write(
        self, engine: SparkEngine, sample_df: DataFrame, delta_path: str
    ) -> None:
        engine.write_to_path(sample_df, delta_path, "overwrite", "delta")
        history = engine.get_history_by_path(delta_path, limit=1)
        writer = DeltaWriter(engine)
        metrics = writer._parse_write_metrics(history)
        assert metrics["rows_written"] == 3
        assert metrics["files_added"] >= 1

    def test_metrics_no_table(self, engine: SparkEngine, delta_path: str) -> None:
        writer = DeltaWriter(engine)
        metrics = writer._parse_write_metrics([])
        assert metrics["rows_written"] == 0


# =====================================================================
# Advanced branch tests (mock-driven)
# =====================================================================


class TestSparkEngineAdvanced:
    def test_apply_options_applies_all_pairs(self) -> None:
        class StubReader:
            def __init__(self) -> None:
                self.calls: list[tuple[str, Any]] = []

            def option(self, key: str, value: Any) -> "StubReader":
                self.calls.append((key, value))
                return self

        reader = StubReader()
        out = SparkEngine._apply_options(reader, {"a": 1, "b": "x"})
        assert out is reader
        assert ("a", 1) in reader.calls
        assert ("b", "x") in reader.calls

    def test_apply_options_none_is_noop(self) -> None:
        obj = object()
        assert SparkEngine._apply_options(obj, None) is obj

    def test_read_database_builds_dbtable_for_table_and_query(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        fake_reader = MagicMock()
        fake_reader.load.return_value = sample_df

        with patch.object(engine, "_apply_options", return_value=fake_reader) as apply_opts:
            out = engine.read_database(table="public.users", options={"url": "jdbc://x"})
            assert engine.count_rows(out) == 3
            merged = apply_opts.call_args.args[1]
            assert merged["dbtable"] == "public.users"

            out2 = engine.read_database(query="  SELECT 1 ", options={"url": "jdbc://x"})
            assert engine.count_rows(out2) == 3
            merged2 = apply_opts.call_args.args[1]
            assert merged2["dbtable"] == "(SELECT 1) AS q"

    def test_execute_sql_with_and_without_parameters(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        with patch.object(engine.spark, "sql", return_value=sample_df) as spark_sql:
            out = engine.execute_sql("SELECT * FROM t", parameters={"x": 1})
            assert engine.count_rows(out) == 3
            spark_sql.assert_any_call("SELECT * FROM t", {"x": 1})

            out2 = engine.execute_sql("SELECT * FROM t")
            assert engine.count_rows(out2) == 3
            spark_sql.assert_any_call("SELECT * FROM t")

    def test_build_merge_condition_col_single_key(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        from pyspark.sql import Column

        cond = SparkEngine._build_merge_condition_col(sample_df, "db.tbl", ["id"])
        assert isinstance(cond, Column)

    def test_build_merge_condition_col_multi_key(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        from pyspark.sql import Column

        cond = SparkEngine._build_merge_condition_col(sample_df, "cat.db.tbl", ["id", "name"])
        assert isinstance(cond, Column)
        # String representation should reference both keys
        cond_str = str(cond)
        assert "id" in cond_str
        assert "name" in cond_str

    def test_merge_into_upsert_passes_column_condition(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        from pyspark.sql import Column

        captured: Dict[str, Any] = {}

        def fake_merge_into(table: str, cond: object) -> MagicMock:
            captured["table"] = table
            captured["cond"] = cond
            writer = MagicMock()
            writer.whenMatched.return_value = writer
            writer.update.return_value = writer
            writer.whenNotMatched.return_value = writer
            writer.insertAll.return_value = writer
            writer.merge.return_value = None
            return writer

        with patch.object(sample_df, "mergeInto", side_effect=fake_merge_into, create=True):
            engine._merge_into_upsert(sample_df, "cat.db.tbl", ["id"])

        assert captured["table"] == "cat.db.tbl"
        assert isinstance(captured["cond"], Column), (
            f"mergeInto condition must be a Column, got {type(captured['cond'])}"
        )

    def test_merge_into_overwrite_passes_column_condition(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        from pyspark.sql import Column

        captured: Dict[str, Any] = {}

        def fake_merge_into(table: str, cond: object) -> MagicMock:
            captured["table"] = table
            captured["cond"] = cond
            writer = MagicMock()
            writer.whenMatched.return_value = writer
            writer.delete.return_value = writer
            writer.merge.return_value = None
            return writer

        with patch.object(engine, "write_to_table"):
            # Patch mergeInto on the DataFrame class so keys_df (derived via .select) also uses it
            with patch.object(DataFrame, "mergeInto", side_effect=fake_merge_into, create=True):
                engine._merge_into_overwrite(sample_df, "cat.db.tbl", ["id"], fmt="delta")

        assert captured["table"] == "cat.db.tbl"
        assert isinstance(captured["cond"], Column), (
            f"mergeInto condition must be a Column, got {type(captured['cond'])}"
        )

    def test_merge_into_upsert_update_map_values_are_columns(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        from pyspark.sql import Column

        captured: Dict[str, Any] = {}

        def fake_when_matched(*args: object, **kwargs: object) -> MagicMock:
            writer = MagicMock()

            def fake_update(update_map: dict) -> MagicMock:
                captured["update_map"] = update_map
                return writer

            writer.update.side_effect = fake_update
            writer.whenNotMatched.return_value = writer
            writer.insertAll.return_value = writer
            writer.merge.return_value = None
            return writer

        mock_writer = MagicMock()
        mock_writer.whenMatched.side_effect = fake_when_matched

        with patch.object(sample_df, "mergeInto", return_value=mock_writer, create=True):
            engine._merge_into_upsert(sample_df, "cat.db.tbl", ["id"])

        assert captured.get("update_map"), "update_map was not passed to .update()"
        for col_val in captured["update_map"].values():
            assert isinstance(col_val, Column), (
                f"update map value must be Column, got {type(col_val)}"
            )

    def test_merge_to_table_dispatches_by_supports_merge_into(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        with patch.object(SparkEngine, "_supports_merge_into", new_callable=PropertyMock, return_value=True):
            with patch.object(engine, "_merge_into_upsert") as merge_into, patch.object(engine, "_merge_sql_upsert") as merge_sql:
                engine.merge_to_table(sample_df, "catalog.db.tbl", ["id"], fmt="delta")
                merge_into.assert_called_once()
                merge_sql.assert_not_called()

        with patch.object(SparkEngine, "_supports_merge_into", new_callable=PropertyMock, return_value=False):
            with patch.object(engine, "_merge_into_upsert") as merge_into, patch.object(engine, "_merge_sql_upsert") as merge_sql:
                engine.merge_to_table(sample_df, "catalog.db.tbl", ["id"], fmt="delta")
                merge_sql.assert_called_once()
                merge_into.assert_not_called()

    def test_merge_overwrite_to_table_dispatches_by_supports_merge_into(self, engine: SparkEngine, sample_df: DataFrame) -> None:
        with patch.object(SparkEngine, "_supports_merge_into", new_callable=PropertyMock, return_value=True):
            with patch.object(engine, "_merge_into_overwrite") as merge_into_overwrite, patch.object(engine, "write_to_table") as write_to_table:
                engine.merge_overwrite_to_table(sample_df, "catalog.db.tbl", ["id"], fmt="delta")
                merge_into_overwrite.assert_called_once()
                write_to_table.assert_not_called()

        with patch.object(SparkEngine, "_supports_merge_into", new_callable=PropertyMock, return_value=False):
            with patch.object(engine, "_merge_sql_overwrite") as merge_sql_overwrite, patch.object(engine, "write_to_table") as write_to_table:
                engine.merge_overwrite_to_table(sample_df, "catalog.db.tbl", ["id"], fmt="delta")
                merge_sql_overwrite.assert_called_once()
                write_to_table.assert_not_called()

    def test_table_exists_by_path_platform_branches(self, engine: SparkEngine) -> None:
        platform = MagicMock()
        engine.set_platform(platform)

        platform.folder_exists.return_value = True
        assert engine.table_exists_by_path("s3://x/tbl/", fmt="iceberg") is True
        platform.folder_exists.assert_called_with("s3://x/tbl/metadata")

        platform.folder_exists.return_value = True
        assert engine.table_exists_by_path("s3://x/delta/", fmt="delta") is True
        platform.folder_exists.assert_called_with("s3://x/delta/_delta_log")

        platform.folder_exists.return_value = True
        assert engine.table_exists_by_path("/generic/path", fmt="parquet") is True
        platform.folder_exists.assert_called_with("/generic/path")

    def test_table_exists_by_path_without_platform_delta_branches(self, engine: SparkEngine) -> None:
        with patch.object(engine.spark, "sql") as spark_sql:
            spark_sql.return_value = MagicMock()
            assert engine.table_exists_by_path("/tmp/delta", fmt="delta") is True

            spark_sql.side_effect = RuntimeError("not found")
            assert engine.table_exists_by_path("/tmp/delta", fmt="delta") is False

    def test_table_exists_by_name_exception_returns_false(self, engine: SparkEngine) -> None:
        with patch.object(engine.spark.catalog, "tableExists", side_effect=RuntimeError("boom")):
            assert engine.table_exists_by_name("catalog.db.tbl") is False

    def test_history_dispatch_unsupported_formats_return_empty(self, engine: SparkEngine) -> None:
        assert engine.get_history_by_path("/tmp/x", fmt="parquet") == []
        assert engine.get_history_by_name("db.tbl", fmt="parquet") == []

    def test_compact_by_name_iceberg_option_toggles(self, engine: SparkEngine) -> None:
        with patch.object(engine, "_iceberg_rewrite_data_files") as data_files, \
             patch.object(engine, "_iceberg_rewrite_position_delete_files") as pos_delete, \
             patch.object(engine, "_iceberg_rewrite_manifests") as manifests:
            engine.compact_by_name(
                "catalog.db.tbl",
                fmt="iceberg",
                options={
                    "rewrite_data_files": True,
                    "rewrite_position_delete_files": False,
                    "rewrite_manifests": True,
                },
            )
            data_files.assert_called_once_with("catalog.db.tbl")
            pos_delete.assert_not_called()
            manifests.assert_called_once_with("catalog.db.tbl")

    def test_cleanup_by_name_iceberg_option_toggles(self, engine: SparkEngine) -> None:
        with patch.object(engine, "_iceberg_expire_snapshots") as expire, \
             patch.object(engine, "_iceberg_remove_orphan_files") as orphan:
            engine.cleanup_by_name(
                "catalog.db.tbl",
                retention_hours=24,
                fmt="iceberg",
                options={
                    "expire_snapshots": False,
                    "remove_orphan_files": True,
                },
            )
            expire.assert_not_called()
            orphan.assert_called_once_with("catalog.db.tbl", 24)

    def test_extract_catalog(self) -> None:
        assert SparkEngine._extract_catalog("my_catalog.db.table") == "my_catalog"


class TestSparkToHiveType:
    """Test SparkEngine._spark_type_to_hive with native PySpark type objects."""

    def test_scalars(self) -> None:
        from pyspark.sql import types as T
        assert SparkEngine._spark_type_to_hive(T.LongType()) == "BIGINT"
        assert SparkEngine._spark_type_to_hive(T.IntegerType()) == "INT"
        assert SparkEngine._spark_type_to_hive(T.ShortType()) == "SMALLINT"
        assert SparkEngine._spark_type_to_hive(T.ByteType()) == "TINYINT"
        assert SparkEngine._spark_type_to_hive(T.FloatType()) == "FLOAT"
        assert SparkEngine._spark_type_to_hive(T.DoubleType()) == "DOUBLE"
        assert SparkEngine._spark_type_to_hive(T.BooleanType()) == "BOOLEAN"
        assert SparkEngine._spark_type_to_hive(T.StringType()) == "STRING"
        assert SparkEngine._spark_type_to_hive(T.BinaryType()) == "BINARY"
        assert SparkEngine._spark_type_to_hive(T.DateType()) == "DATE"
        assert SparkEngine._spark_type_to_hive(T.TimestampType()) == "TIMESTAMP"
        assert SparkEngine._spark_type_to_hive(T.TimestampNTZType()) == "TIMESTAMP"

    def test_decimal(self) -> None:
        from pyspark.sql import types as T
        assert SparkEngine._spark_type_to_hive(T.DecimalType(10, 2)) == "DECIMAL(10,2)"
        assert SparkEngine._spark_type_to_hive(T.DecimalType(38, 18)) == "DECIMAL(38,18)"

    def test_array(self) -> None:
        from pyspark.sql import types as T
        assert SparkEngine._spark_type_to_hive(T.ArrayType(T.StringType())) == "ARRAY<STRING>"

    def test_nested_array(self) -> None:
        from pyspark.sql import types as T
        assert SparkEngine._spark_type_to_hive(
            T.ArrayType(T.ArrayType(T.IntegerType()))
        ) == "ARRAY<ARRAY<INT>>"

    def test_map(self) -> None:
        from pyspark.sql import types as T
        assert SparkEngine._spark_type_to_hive(
            T.MapType(T.StringType(), T.LongType())
        ) == "MAP<STRING,BIGINT>"

    def test_struct(self) -> None:
        from pyspark.sql import types as T
        dt = T.StructType([
            T.StructField("a", T.IntegerType()),
            T.StructField("b", T.StringType()),
        ])
        assert SparkEngine._spark_type_to_hive(dt) == "STRUCT<a:INT,b:STRING>"

    def test_get_hive_schema(self, spark: SparkSession) -> None:
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("id", T.LongType()),
            T.StructField("name", T.StringType()),
            T.StructField("tags", T.ArrayType(T.StringType())),
        ])
        df = spark.createDataFrame([], schema)
        engine = SparkEngine(spark)
        result = engine.get_hive_schema(df)
        assert result == {"id": "BIGINT", "name": "STRING", "tags": "ARRAY<STRING>"}
