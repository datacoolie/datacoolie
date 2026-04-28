"""Tests for DeltaWriter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.constants import (
    DataFlowStatus,
    Format,
    LoadType,
)
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import Connection, Destination, PartitionColumn
from datacoolie.platforms.aws_platform import AWSPlatform

from datacoolie.destinations.delta_writer import DeltaWriter

from tests.unit.destinations.support import MockEngine, _make_dataflow, engine


# ============================================================================
# DeltaWriter tests
# ============================================================================


class TestDeltaWriter:
    def test_write_overwrite(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.OVERWRITE.value)
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._written) == 1
        assert engine._written[0]["mode"] == "overwrite"

    def test_write_populates_operation_details(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.OVERWRITE.value)
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        # operation_details is the history list returned by the engine
        assert info.operation_details == [{"version": 0}]

    def test_write_append(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.APPEND.value)
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert engine._written[0]["mode"] == "append"

    def test_write_merge_upsert_new_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(False)
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=["order_id"],
        )
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        # Falls back to overwrite for new table
        assert len(engine._written) == 1
        assert engine._written[0]["mode"] == "overwrite"

    def test_write_merge_upsert_existing_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=["order_id"],
        )
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._merged) == 1

    def test_write_merge_overwrite_new_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(False)
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["order_id"],
        )
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._written) == 1

    def test_write_merge_overwrite_existing_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["order_id"],
        )
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._merge_overwritten) == 1

    def test_write_wrong_format_raises(self, engine: MockEngine) -> None:
        df = _make_dataflow(dest_format=Format.PARQUET.value)
        writer = DeltaWriter(engine)
        with pytest.raises(DestinationError, match="only supports Delta"):
            writer.write({"data": 1}, df)

    def test_write_no_path_succeeds_with_table_name(self, engine: MockEngine) -> None:
        df = _make_dataflow(has_path=False)
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._written) == 1

    def test_write_merge_no_keys_raises(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.MERGE_UPSERT.value, merge_keys=[])
        engine.set_table_exists(True)
        writer = DeltaWriter(engine)
        with pytest.raises(DestinationError, match="requires merge_keys"):
            writer.write({"data": 1}, df)

    def test_write_scd2_raises_not_implemented(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.SCD2.value)
        writer = DeltaWriter(engine)
        with pytest.raises(DestinationError):
            writer.write({"data": 1}, df)

    def test_write_with_partition_columns(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.OVERWRITE.value,
            partition_cols=[PartitionColumn(column="year")],
        )
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value

    def test_compact_internal(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        df = _make_dataflow()
        writer = DeltaWriter(engine)
        info = writer.run_maintenance(df, do_cleanup=False)
        assert len(engine._compacted) == 1

    def test_cleanup_internal(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        df = _make_dataflow()
        writer = DeltaWriter(engine)
        info = writer.run_maintenance(df, do_compact=False, retention_hours=72)
        assert len(engine._cleaned) == 1
        assert engine._cleaned[0]["hours"] == 72

    def test_parse_write_metrics(self, engine: MockEngine) -> None:
        history = [{"operationMetrics": {
            "numOutputRows": "10",
            "numTargetRowsInserted": "8",
            "numTargetRowsUpdated": "2",
            "numTargetFilesAdded": "3",
            "numRemovedFiles": "1",
            "numAddedBytes": "1024",
            "numRemovedBytes": "512",
        }}]
        writer = DeltaWriter(engine)
        m = writer._parse_write_metrics(history)
        assert m["rows_written"] == 10
        assert m["rows_inserted"] == 8
        assert m["rows_updated"] == 2
        assert m["files_added"] == 3
        assert m["files_removed"] == 1
        assert m["bytes_added"] == 1024
        assert m["bytes_removed"] == 512

    def test_parse_write_metrics_empty_history(self, engine: MockEngine) -> None:
        writer = DeltaWriter(engine)
        m = writer._parse_write_metrics([])
        assert m["rows_written"] == 0
        assert m["files_added"] == 0

    def test_parse_maintenance_metrics(self, engine: MockEngine) -> None:
        history = [
            {"operation": "OPTIMIZE", "operationMetrics": {
                "numAddedFiles": "5",
                "numRemovedFiles": "10",
                "numAddedBytes": "2048",
                "numRemovedBytes": "4096",
            }},
            {"operation": "VACUUM END", "operationMetrics": {
                "numDeletedFiles": "3",
            }},
        ]
        writer = DeltaWriter(engine)
        m = writer._parse_maintenance_metrics(history)
        assert m["compact"]["files_added"] == 5
        assert m["compact"]["files_removed"] == 10
        assert m["compact"]["bytes_added"] == 2048
        assert m["cleanup"]["files_removed"] == 3

    def test_parse_maintenance_metrics_accumulates_same_operation(self, engine: MockEngine) -> None:
        history = [
            {"operation": "OPTIMIZE", "operationMetrics": {
                "numAddedFiles": "2",
                "numRemovedFiles": "1",
                "numAddedBytes": "100",
                "numRemovedBytes": "50",
            }},
            {"operation": "OPTIMIZE", "operationMetrics": {
                "numAddedFiles": "3",
                "numDeletedFiles": "4",
                "numAddedBytes": "200",
                "sizeOfDataToDelete": "80",
            }},
        ]
        writer = DeltaWriter(engine)
        m = writer._parse_maintenance_metrics(history)
        assert m["compact"]["files_added"] == 5
        assert m["compact"]["files_removed"] == 5
        assert m["compact"]["bytes_added"] == 300
        assert m["compact"]["bytes_removed"] == 130


# ============================================================================
# Post-write catalog tests
# ============================================================================


def _make_aws_engine() -> tuple[MockEngine, MagicMock]:
    """Create a MockEngine with an AWSPlatform mock attached."""
    engine = MockEngine()
    mock_platform = MagicMock(spec=AWSPlatform)
    engine._platform = mock_platform
    engine.generate_symlink_manifest = MagicMock()
    # Mock read and get_schema for _build_schema_ddl / _build_partition_ddl
    engine.read = MagicMock(return_value={})
    engine.get_schema = MagicMock(return_value={"id": "bigint", "name": "string", "amount": "double"})
    engine.get_hive_schema = MagicMock(return_value={"id": "BIGINT", "name": "STRING", "amount": "DOUBLE"})
    return engine, mock_platform


def _make_glue_dataflow(
    *,
    generate_manifest: bool = False,
    register_symlink_table: bool = False,
    partition_cols: list | None = None,
):
    """Build a DataFlow with flat Glue/Athena config."""
    conn = Connection(
        name="aws-delta",
        format="delta",
        database="mydb",
        configure={
            "base_path": "s3://data-bucket/silver/",
            "athena_output_location": "s3://bucket/athena-results/",
            "generate_manifest": generate_manifest,
            "register_symlink_table": register_symlink_table,
            "symlink_database_prefix": "symlink_",
        },
    )
    from datacoolie.core.models import DataFlow, Source, Transform

    return DataFlow(
        dataflow_id="test-glue-df",
        name="test-glue",
        source=Source(connection=Connection(name="src", format="delta")),
        destination=Destination(
            connection=conn,
            table="orders",
            load_type=LoadType.OVERWRITE.value,
            partition_columns=partition_cols or [],
        ),
        transform=Transform(),
    )


class TestPostWriteCatalog:
    def test_no_op_when_not_aws_platform(self) -> None:
        engine = MockEngine()
        engine._platform = MagicMock()  # Not an AWSPlatform
        engine.generate_symlink_manifest = MagicMock()
        df = _make_glue_dataflow()
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        engine.generate_symlink_manifest.assert_not_called()

    def test_no_op_when_no_athena_output_location(self) -> None:
        engine, platform = _make_aws_engine()
        df = _make_dataflow()  # No athena_output_location
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        platform.register_delta_table.assert_not_called()

    def test_always_registers_native_delta_table(self) -> None:
        engine, platform = _make_aws_engine()
        df = _make_glue_dataflow()
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        platform.register_delta_table.assert_called_once()
        call_args = platform.register_delta_table.call_args
        assert call_args[0][0] == "orders"  # table_name

    def test_no_symlink_when_flags_false(self) -> None:
        engine, platform = _make_aws_engine()
        df = _make_glue_dataflow(generate_manifest=False, register_symlink_table=False)
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        # Native is always registered
        platform.register_delta_table.assert_called_once()
        # Symlink is NOT generated
        engine.generate_symlink_manifest.assert_not_called()
        platform.register_symlink_table.assert_not_called()

    def test_generates_manifest_only_when_generate_manifest_true(self) -> None:
        engine, platform = _make_aws_engine()
        df = _make_glue_dataflow(generate_manifest=True)
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        engine.generate_symlink_manifest.assert_called_once()
        # Only manifest, no symlink table registration
        platform.register_symlink_table.assert_not_called()

    def test_register_symlink_implies_generate(self) -> None:
        engine, platform = _make_aws_engine()
        df = _make_glue_dataflow(register_symlink_table=True)
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        engine.generate_symlink_manifest.assert_called_once()
        platform.register_symlink_table.assert_called_once()
        call_kwargs = platform.register_symlink_table.call_args
        assert call_kwargs[1]["database"] == "symlink_mydb"

    def test_symlink_table_with_partitions(self) -> None:
        engine, platform = _make_aws_engine()
        engine.get_schema.return_value = {
            "id": "bigint", "name": "string", "year": "string",
        }
        df = _make_glue_dataflow(
            register_symlink_table=True,
            partition_cols=[PartitionColumn(column="year")],
        )
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        call_kwargs = platform.register_symlink_table.call_args
        assert "PARTITIONED BY" in call_kwargs[1]["partition_ddl"]
        # Schema DDL should NOT include the partition column
        assert "year" not in call_kwargs[1]["schema_ddl"]

    def test_native_and_symlink_together(self) -> None:
        engine, platform = _make_aws_engine()
        df = _make_glue_dataflow(register_symlink_table=True)
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)
        platform.register_delta_table.assert_called_once()
        platform.register_symlink_table.assert_called_once()
        engine.generate_symlink_manifest.assert_called_once()

    def test_symlink_manifest_failure_skips_table_registration(self) -> None:
        engine, platform = _make_aws_engine()
        engine.generate_symlink_manifest.side_effect = RuntimeError("boom")
        df = _make_glue_dataflow(register_symlink_table=True)
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)  # should not raise
        platform.register_symlink_table.assert_not_called()
        # Native should still have been registered before the symlink attempt
        platform.register_delta_table.assert_called_once()

    def test_native_failure_logs_warning(self) -> None:
        engine, platform = _make_aws_engine()
        platform.register_delta_table.side_effect = RuntimeError("athena down")
        df = _make_glue_dataflow()
        writer = DeltaWriter(engine)
        writer._post_write_catalog(df)  # should not raise


class TestToHiveTypeDelegation:
    """Verify DeltaWriter delegates type conversion to the engine."""

    def test_build_schema_ddl_uses_engine_get_hive_schema(self) -> None:
        engine, _ = _make_aws_engine()
        engine.get_hive_schema = MagicMock(return_value={"id": "BIGINT", "name": "STRING"})
        df = _make_glue_dataflow()
        writer = DeltaWriter(engine)
        ddl = writer._build_schema_ddl(df)
        assert "`id` BIGINT" in ddl
        assert "`name` STRING" in ddl


class TestPostMaintenanceCatalog:
    def test_registers_native_after_maintenance(self) -> None:
        engine, platform = _make_aws_engine()
        engine.set_table_exists(True)
        df = _make_glue_dataflow()
        writer = DeltaWriter(engine)
        writer.run_maintenance(df, do_compact=True, do_cleanup=False)
        platform.register_delta_table.assert_called_once()

    def test_regenerates_symlink_after_maintenance(self) -> None:
        engine, platform = _make_aws_engine()
        engine.set_table_exists(True)
        df = _make_glue_dataflow(register_symlink_table=True)
        writer = DeltaWriter(engine)
        writer.run_maintenance(df, do_compact=True, do_cleanup=False)
        engine.generate_symlink_manifest.assert_called_once()
        platform.register_symlink_table.assert_called_once()

    def test_no_symlink_after_maintenance_when_not_configured(self) -> None:
        engine, platform = _make_aws_engine()
        engine.set_table_exists(True)
        df = _make_glue_dataflow()  # no generate_manifest or register_symlink_table
        writer = DeltaWriter(engine)
        writer.run_maintenance(df, do_compact=True, do_cleanup=False)
        # Native still registered
        platform.register_delta_table.assert_called_once()
        # Symlink NOT generated
        engine.generate_symlink_manifest.assert_not_called()
        platform.register_symlink_table.assert_not_called()
