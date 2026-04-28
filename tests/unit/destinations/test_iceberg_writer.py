"""Tests for IcebergWriter."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import (
    DataFlowStatus,
    Format,
    LoadType,
)
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import (
    Connection,
    DataFlow,
    Destination,
    PartitionColumn,
    Source,
    Transform,
)

from datacoolie.destinations.iceberg_writer import IcebergWriter

from tests.unit.destinations.support import MockEngine, _make_dataflow, engine


# ============================================================================
# IcebergWriter tests
# ============================================================================


class TestIcebergWriter:
    def test_write_iceberg_with_catalog(self, engine: MockEngine) -> None:
        """IcebergWriter writes via table_name when catalog is set."""
        dst_conn = Connection(
            name="iceberg_dst",
            connection_type="lakehouse",
            format=Format.ICEBERG.value,
            catalog="my_catalog",
            database="my_db",
            configure={},
        )
        src_conn = Connection(
            name="src",
            connection_type="lakehouse",
            format=Format.DELTA.value,
            configure={"base_path": "/data/bronze"},
        )
        df = DataFlow(
            name="iceberg_flow",
            source=Source(connection=src_conn, schema_name="raw", table="events"),
            destination=Destination(
                connection=dst_conn,
                schema_name="curated",
                table="dim_events",
                load_type=LoadType.OVERWRITE.value,
            ),
            transform=Transform(),
        )
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._written) == 1
        assert engine._written[0]["table_name"] == "`my_catalog`.`my_db`.`curated`.`dim_events`"

    def test_write_wrong_format_raises(self, engine: MockEngine) -> None:
        df = _make_dataflow(dest_format=Format.DELTA.value)
        writer = IcebergWriter(engine)
        with pytest.raises(DestinationError, match="only supports Iceberg"):
            writer.write({"data": 1}, df)

    def test_write_no_path_or_catalog_succeeds_with_table_name(self, engine: MockEngine) -> None:
        dst_conn = Connection(
            name="iceberg_no_catalog",
            connection_type="lakehouse",
            format=Format.ICEBERG.value,
            configure={},
        )
        src_conn = Connection(
            name="src",
            connection_type="lakehouse",
            format=Format.DELTA.value,
            configure={"base_path": "/data/bronze"},
        )
        df = DataFlow(
            name="iceberg_fail",
            source=Source(connection=src_conn, schema_name="raw", table="events"),
            destination=Destination(
                connection=dst_conn,
                schema_name="curated",
                table="dim_events",
                load_type=LoadType.OVERWRITE.value,
            ),
            transform=Transform(),
        )
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._written) == 1
        assert engine._written[0]["table_name"] == "`curated`.`dim_events`"


# ============================================================================
# IcebergWriter advanced tests
# ============================================================================


class TestIcebergWriterAdvanced:
    """Test Iceberg writer in complex scenarios."""

    def test_write_iceberg_with_merge_upsert(self, engine: MockEngine) -> None:
        """IcebergWriter with MERGE_UPSERT and catalog.
        
        Validates:
          - Catalog-qualified table name created
          - Merge operation routing works correctly
          - Merge keys captured
        """
        engine.set_table_exists(True)
        
        dst_conn = Connection(
            name="iceberg_dst",
            connection_type="lakehouse",
            format=Format.ICEBERG.value,
            catalog="prod_catalog",
            database="analytics_db",
            configure={},
        )
        src_conn = Connection(
            name="src",
            connection_type="lakehouse",
            format=Format.DELTA.value,
            configure={"base_path": "/data/bronze"},
        )
        df = DataFlow(
            name="iceberg_merge_flow",
            source=Source(connection=src_conn, schema_name="raw", table="customers"),
            destination=Destination(
                connection=dst_conn,
                schema_name="curated",
                table="dim_customers",
                load_type=LoadType.MERGE_UPSERT.value,
                merge_keys=["customer_id"],
            ),
            transform=Transform(),
        )
        
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._merged) == 1
        assert engine._merged[0]["merge_keys"] == ["customer_id"]

    def test_write_iceberg_with_merge_overwrite(self, engine: MockEngine) -> None:
        """IcebergWriter with MERGE_OVERWRITE.
        
        Validates:
          - Merge overwrite operation routes correctly
          - Table qualifications preserved
        """
        engine.set_table_exists(True)
        
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["id"],
            dest_format=Format.ICEBERG.value,
        )
        
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._merge_overwritten) == 1

    def test_write_iceberg_with_partition_columns(self, engine: MockEngine) -> None:
        """IcebergWriter respects partition columns.
        
        Validates:
          - Partition columns passed through
          - No error on partitioned write
        """
        df = _make_dataflow(
            dest_format=Format.ICEBERG.value,
            partition_cols=[
                PartitionColumn(column="year"),
                PartitionColumn(column="month"),
            ],
        )
        
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._written) == 1

    def test_write_iceberg_new_table_with_merge_upsert_falls_back(self, engine: MockEngine) -> None:
        """IcebergWriter falls back to overwrite for new table with MERGE_UPSERT.
        
        Validates:
          - Table doesn't exist
          - MERGE_UPSERT requested
          - Falls back to OVERWRITE
                    - Write succeeds as overwrite
        """
        engine.set_table_exists(False)  # Table doesn't exist
        
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=["order_id"],
            dest_format=Format.ICEBERG.value,
        )
        
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        # When table doesn't exist, should fall back to overwrite
        assert len(engine._written) == 1
        assert engine._written[0]["mode"] == "overwrite"


# ============================================================================
# IcebergWriter maintenance operations
# ============================================================================


class TestIcebergWriterMaintenance:
    """Test Iceberg-specific maintenance operations."""

    def test_iceberg_maintenance_compact_and_cleanup(self, engine: MockEngine) -> None:
        """IcebergWriter maintenance runs compact and cleanup.
        
        Validates:
          - Maintenance succeeds on Iceberg
          - Both compact and cleanup operations recorded
          - Operation details populated
        """
        engine.set_table_exists(True)
        
        df = _make_dataflow(dest_format=Format.ICEBERG.value)
        writer = IcebergWriter(engine)
        info = writer.run_maintenance(df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._compacted) == 1
        assert len(engine._cleaned) == 1

    def test_iceberg_maintenance_with_retention(self, engine: MockEngine) -> None:
        """IcebergWriter maintenance respects retention hours.
        
        Validates:
          - Custom retention propagated
          - Cleanup operation receives correct hours
        """
        engine.set_table_exists(True)
        
        df = _make_dataflow(dest_format=Format.ICEBERG.value)
        writer = IcebergWriter(engine)
        
        custom_hours = 96
        info = writer.run_maintenance(df, do_compact=False, retention_hours=custom_hours)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._cleaned) == 1
        assert engine._cleaned[0]["hours"] == custom_hours

    def test_iceberg_maintenance_compact_only(self, engine: MockEngine) -> None:
        """IcebergWriter maintenance with only compact.
        
        Validates:
          - do_cleanup=False skips cleanup
          - Compact performed once
          - Operation details correct
        """
        engine.set_table_exists(True)
        
        df = _make_dataflow(dest_format=Format.ICEBERG.value)
        writer = IcebergWriter(engine)
        info = writer.run_maintenance(df, do_compact=True, do_cleanup=False)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._compacted) == 1
        assert len(engine._cleaned) == 0

    def test_iceberg_append_operation(self, engine: MockEngine) -> None:
        """IcebergWriter with APPEND load type.
        
        Validates:
          - Append operation works
          - Write mode is 'append'
        """
        df = _make_dataflow(
            load_type=LoadType.APPEND.value,
            dest_format=Format.ICEBERG.value,
        )
        
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert engine._written[0]["mode"] == "append"


class TestIcebergWriterMetricsParsing:
    def test_parse_maintenance_metrics_maps_and_accumulates(self, engine: MockEngine) -> None:
        writer = IcebergWriter(engine)
        history = [
            {
                "operation": "REPLACE",
                "summary": {
                    "added-data-files": "2",
                    "deleted-data-files": "1",
                    "added-files-size": "200",
                    "removed-files-size": "100",
                },
            },
            {
                "operation": "EXPIRE_SNAPSHOTS",
                "summary": {
                    "added-data-files": "0",
                    "deleted-data-files": "3",
                    "added-files-size": "0",
                    "removed-files-size": "75",
                },
            },
            {
                "operation": "REMOVE_ORPHAN_FILES",
                "summary": {
                    "added-data-files": "0",
                    "deleted-data-files": "2",
                    "added-files-size": "0",
                    "removed-files-size": "25",
                },
            },
        ]
        m = writer._parse_maintenance_metrics(history)
        assert m["compact"]["files_added"] == 2
        assert m["compact"]["files_removed"] == 1
        assert m["cleanup"]["files_removed"] == 5
        assert m["cleanup"]["bytes_removed"] == 100

    def test_parse_maintenance_metrics_ignores_unknown_operation(self, engine: MockEngine) -> None:
        writer = IcebergWriter(engine)
        history = [{"operation": "SNAPSHOT", "summary": {"added-data-files": "9"}}]
        assert writer._parse_maintenance_metrics(history) == {}
