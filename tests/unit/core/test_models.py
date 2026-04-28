"""Tests for datacoolie.core.models."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from datacoolie.core.constants import DataFlowStatus, ExecutionType, Format, LoadType
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import (
    AdditionalColumn,
    Connection,
    DataCoolieRunConfig,
    DataFlow,
    DataFlowRuntimeInfo,
    Destination,
    DestinationRuntimeInfo,
    JobRuntimeInfo,
    PartitionColumn,
    RuntimeInfo,
    SchemaHint,
    Source,
    SourceRuntimeInfo,
    Transform,
    TransformRuntimeInfo,
)
from datacoolie.utils.helpers import name_to_uuid


# ============================================================================
# Supporting models
# ============================================================================


class TestSchemaHint:
    """Verify SchemaHint model validation and construction."""
    def test_valid(self) -> None:
        sh = SchemaHint(column_name="amount", data_type="DECIMAL", precision=18, scale=2)
        assert sh.column_name == "amount"
        assert sh.data_type == "DECIMAL"
        assert sh.precision == 18
        assert sh.scale == 2
        assert sh.is_active is True

    def test_empty_column_name_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            SchemaHint(column_name="", data_type="STRING")

    def test_empty_data_type_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            SchemaHint(column_name="col", data_type="")

    def test_from_dict(self) -> None:
        sh = SchemaHint(**{"column_name": "dt", "data_type": "DATE", "format": "yyyy-MM-dd"})
        assert sh.format == "yyyy-MM-dd"


class TestPartitionColumn:
    """Verify PartitionColumn model for table partitioning."""
    def test_valid(self) -> None:
        pc = PartitionColumn(column="year", expression="year(order_date)")
        assert pc.column == "year"
        assert pc.expression == "year(order_date)"

    def test_no_expression(self) -> None:
        pc = PartitionColumn(column="region")
        assert pc.expression is None

    def test_empty_column_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            PartitionColumn(column="")


class TestAdditionalColumn:
    """Verify AdditionalColumn model for computed columns."""
    def test_valid(self) -> None:
        ac = AdditionalColumn(column="src", expression="'erp'")
        assert ac.column == "src"
        assert ac.expression == "'erp'"

    def test_empty_expression_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            AdditionalColumn(column="src", expression="")


# ============================================================================
# Connection
# ============================================================================


class TestConnection:
    """Verify Connection model construction, validation, and computed properties."""
    def test_minimal(self) -> None:
        conn = Connection(name="test_conn")
        assert conn.name == "test_conn"
        assert conn.connection_type == "file"
        assert conn.format == Format.PARQUET.value
        assert conn.is_active is True
        assert conn.connection_id == name_to_uuid("test_conn")

    def test_connection_id_default_is_none_in_field_definition(self) -> None:
        assert Connection.model_fields["connection_id"].default is None

    def test_connection_id_respects_explicit_value(self) -> None:
        conn = Connection(name="test_conn", connection_id="manual-id")
        assert conn.connection_id == "manual-id"

    def test_computed_properties(self, file_connection: Connection) -> None:
        assert file_connection.base_path == "abfss://bronze@storage.dfs.core.windows.net"
        assert file_connection.read_options == {"inferSchema": "false"}
        assert file_connection.write_options == {"optimizeWrite": "true"}
        assert file_connection.use_schema_hint is True

    def test_db_connection_properties(self, db_connection: Connection) -> None:
        assert db_connection.host == "erp-server.example.com"
        assert db_connection.port == 1433
        assert db_connection.database == "ERP"
        assert db_connection.username == "etl_reader"
        assert db_connection.password == "s3cret!"

    def test_format_normalised(self) -> None:
        conn = Connection(name="c", format="DELTA")
        assert conn.format == "delta"

    def test_config_from_json_string(self) -> None:
        conn = Connection(name="c", configure='{"base_path": "/data"}')
        assert conn.base_path == "/data"

    def test_empty_name_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            Connection(name="")

    def test_none_base_path(self) -> None:
        conn = Connection(name="bare")
        assert conn.base_path is None

    def test_none_port(self) -> None:
        conn = Connection(name="bare")
        assert conn.port is None

    def test_secrets_ref_default_is_none(self) -> None:
        conn = Connection(name="bare")
        assert conn.secrets_ref is None

    def test_secrets_ref_accepts_dict(self) -> None:
        conn = Connection(name="c", secrets_ref={"my-scope": ["password", "api_key"]})
        assert conn.secrets_ref == {"my-scope": ["password", "api_key"]}

    def test_secrets_ref_accepts_json_string(self) -> None:
        conn = Connection(name="c", secrets_ref='{"my-scope": ["api_key"]}')
        assert conn.secrets_ref == {"my-scope": ["api_key"]}

    def test_secrets_ref_empty_string_gives_none(self) -> None:
        conn = Connection(name="c", secrets_ref="")
        assert conn.secrets_ref is None

    def test_secrets_ref_empty_dict_gives_none(self) -> None:
        conn = Connection(name="c", secrets_ref={})
        assert conn.secrets_ref is None

    def test_secrets_ref_invalid_json_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            Connection(name="c", secrets_ref="not{valid")

    def test_secrets_ref_invalid_type_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            Connection(name="c", secrets_ref=12345)

    def test_secrets_ref_duplicate_field_across_sources_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="'username'.*appears in both"):
            Connection(
                name="c",
                secrets_ref={
                    "prod-scope": ["username", "password"],
                    "dev-scope": ["username"],
                },
            )


# ============================================================================
# Source
# ============================================================================


class TestSource:
    """Verify Source model construction and table name formatting."""
    def test_construction(self, sample_source: Source) -> None:
        assert sample_source.table == "orders"
        assert sample_source.schema_name == "sales"
        assert sample_source.watermark_columns == ["modified_at"]

    def test_full_table_name(self, sample_source: Source) -> None:
        # no database on file connection → `schema`.`table`
        assert sample_source.full_table_name == "`sales`.`orders`"

    def test_full_table_name_with_database(self, db_connection: Connection) -> None:
        src = Source(connection=db_connection, schema_name="dbo", table="users")
        assert src.full_table_name == "`ERP`.`dbo`.`users`"

    def test_full_table_name_no_table(self, file_connection: Connection) -> None:
        src = Source(connection=file_connection)
        assert src.full_table_name is None

    def test_path(self, sample_source: Source) -> None:
        assert sample_source.path == "abfss://bronze@storage.dfs.core.windows.net/sales/orders"

    def test_path_no_base(self, db_connection: Connection) -> None:
        src = Source(connection=db_connection, table="t")
        assert src.path is None

    def test_read_options_merged(self, file_connection: Connection) -> None:
        src = Source(
            connection=file_connection,
            table="t",
            configure={"read_options": {"maxFilesPerTrigger": "50"}},
        )
        opts = src.read_options
        assert opts["inferSchema"] == "false"  # from connection
        assert opts["maxFilesPerTrigger"] == "50"  # from source

    def test_watermark_from_csv_string(self, file_connection: Connection) -> None:
        src = Source(connection=file_connection, watermark_columns="col_a,col_b")
        assert src.watermark_columns == ["col_a", "col_b"]


# ============================================================================
# Destination
# ============================================================================


class TestDestination:
    """Verify Destination model construction, write options, and partitioning."""
    def test_construction(self, sample_destination: Destination) -> None:
        assert sample_destination.table == "dim_orders"
        assert sample_destination.load_type == LoadType.MERGE_UPSERT.value
        assert sample_destination.merge_keys == ["order_id"]
        assert len(sample_destination.partition_columns) == 1

    def test_path(self, sample_destination: Destination) -> None:
        assert sample_destination.path == "abfss://silver@storage.dfs.core.windows.net/curated/dim_orders"

    def test_full_table_name(self, sample_destination: Destination) -> None:
        # No database on connection → `schema`.`table`
        assert sample_destination.full_table_name == "`curated`.`dim_orders`"

    def test_load_type_normalised(self, dest_connection: Connection) -> None:
        dest = Destination(connection=dest_connection, table="t", load_type="MERGE_UPSERT")
        assert dest.load_type == "merge_upsert"

    def test_empty_table_raises(self, dest_connection: Connection) -> None:
        with pytest.raises(ConfigurationError):
            Destination(connection=dest_connection, table="")

    def test_write_options_merged(self, dest_connection: Connection) -> None:
        dest = Destination(
            connection=dest_connection,
            table="t",
            configure={"write_options": {"targetFileSize": "128MB"}},
        )
        opts = dest.write_options
        assert opts["optimizeWrite"] == "true"  # from connection
        assert opts["targetFileSize"] == "128MB"  # from dest

    def test_partition_column_names(self, sample_destination: Destination) -> None:
        assert sample_destination.partition_column_names == ["year"]

    def test_partition_from_string(self, dest_connection: Connection) -> None:
        dest = Destination(
            connection=dest_connection,
            table="t",
            partition_columns=["region"],  # type: ignore[arg-type]
        )
        assert len(dest.partition_columns) == 1
        assert dest.partition_columns[0].column == "region"

    def test_merge_keys_from_csv(self, dest_connection: Connection) -> None:
        dest = Destination(connection=dest_connection, table="t", merge_keys="id,name")
        assert dest.merge_keys == ["id", "name"]

    def test_table_normalised_to_lowercase(self, dest_connection: Connection) -> None:
        dest = Destination(connection=dest_connection, table="MyTable")
        assert dest.table == "mytable"

    def test_schema_name_normalised_to_lowercase(self, dest_connection: Connection) -> None:
        dest = Destination(connection=dest_connection, table="t", schema_name="MySchema")
        assert dest.schema_name == "myschema"

    def test_schema_name_none_remains_none(self, dest_connection: Connection) -> None:
        dest = Destination(connection=dest_connection, table="t", schema_name=None)
        assert dest.schema_name is None


# ============================================================================
# Transform
# ============================================================================


class TestTransform:
    """Verify Transform model for deduplication, schema hints, and computed columns."""
    def test_construction(self, sample_transform: Transform) -> None:
        assert sample_transform.deduplicate_columns == ["order_id", "modified_at"]
        assert len(sample_transform.additional_columns) == 1
        assert len(sample_transform.schema_hints) == 2

    def test_deduplicate_column_names_fallback(self) -> None:
        t = Transform()
        assert t.deduplicate_column_names(merge_keys=["id"]) == ["id"]

    def test_deduplicate_column_names_explicit(self, sample_transform: Transform) -> None:
        assert sample_transform.deduplicate_column_names(merge_keys=["xyz"]) == ["order_id", "modified_at"]

    def test_schema_hints_dict(self, sample_transform: Transform) -> None:
        d = sample_transform.schema_hints_dict
        assert "order_date" in d
        assert d["order_date"].data_type == "DATE"

    def test_from_dicts(self) -> None:
        t = Transform(
            additional_columns=[{"column": "src", "expression": "'x'"}],
            schema_hints=[{"column_name": "c", "data_type": "STRING"}],
        )
        assert len(t.additional_columns) == 1
        assert isinstance(t.additional_columns[0], AdditionalColumn)
        assert isinstance(t.schema_hints[0], SchemaHint)

    def test_empty(self) -> None:
        t = Transform()
        assert t.deduplicate_columns == []
        assert t.additional_columns == []
        assert t.schema_hints == []


# ============================================================================
# DataFlow
# ============================================================================


class TestDataFlow:
    """Verify DataFlow model construction and validation."""
    def test_construction(self, sample_dataflow: DataFlow) -> None:
        assert sample_dataflow.name == "orders_bronze_to_silver"
        assert sample_dataflow.stage == "bronze2silver"
        assert sample_dataflow.group_number == 1
        assert sample_dataflow.execution_order == 0
        assert sample_dataflow.dataflow_id == name_to_uuid("orders_bronze_to_silver")

    def test_dataflow_id_is_none_when_name_not_provided(self, sample_source: Source, sample_destination: Destination) -> None:
        df = DataFlow(source=sample_source, destination=sample_destination)
        assert df.name is None
        assert df.dataflow_id is None

    def test_dataflow_id_respects_explicit_value(self, sample_source: Source, sample_destination: Destination) -> None:
        df = DataFlow(dataflow_id="df-manual", source=sample_source, destination=sample_destination)
        assert df.dataflow_id == "df-manual"

    def test_proxied_properties(self, sample_dataflow: DataFlow) -> None:
        assert sample_dataflow.load_type == LoadType.MERGE_UPSERT.value
        assert sample_dataflow.merge_keys == ["order_id"]
        assert sample_dataflow.partition_column_names == ["year"]

    def test_processing_mode_normalised(
        self, sample_source: Source, sample_destination: Destination
    ) -> None:
        df = DataFlow(
            source=sample_source,
            destination=sample_destination,
            processing_mode="BATCH",
        )
        assert df.processing_mode == "batch"

    def test_default_transform(self, sample_source: Source, sample_destination: Destination) -> None:
        df = DataFlow(source=sample_source, destination=sample_destination)
        assert isinstance(df.transform, Transform)

    def test_properties_from_json_string(
        self, sample_source: Source, sample_destination: Destination
    ) -> None:
        df = DataFlow(
            source=sample_source,
            destination=sample_destination,
            configure='{"key": "value"}',
        )
        assert df.configure == {"key": "value"}


# ============================================================================
# ETLRunConfig
# ============================================================================


class TestDataCoolieRunConfig:
    """Verify DataCoolieRunConfig defaults and validation."""
    def test_defaults(self, default_run_config: DataCoolieRunConfig) -> None:
        assert default_run_config.job_id  # auto-generated UUID
        assert default_run_config.job_num == 1
        assert default_run_config.job_index == 0
        assert default_run_config.max_workers == 8
        assert default_run_config.stop_on_error is False

    def test_multi_job(self, multi_job_config: DataCoolieRunConfig) -> None:
        assert multi_job_config.job_num == 3
        assert multi_job_config.job_index == 1
        assert multi_job_config.max_workers == 8

    def test_job_num_less_than_1_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="job_num"):
            DataCoolieRunConfig(job_num=0)

    def test_job_index_negative_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="job_index"):
            DataCoolieRunConfig(job_index=-1)

    def test_job_index_ge_job_num_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="job_index"):
            DataCoolieRunConfig(job_num=2, job_index=2)

    def test_max_workers_0_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="max_workers"):
            DataCoolieRunConfig(max_workers=0)

    def test_negative_retry_count_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="retry_count"):
            DataCoolieRunConfig(retry_count=-1)

    def test_negative_retry_delay_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="retry_delay"):
            DataCoolieRunConfig(retry_delay=-1.0)

    def test_negative_retention_hours_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="retention_hours"):
            DataCoolieRunConfig(retention_hours=-1)


# ============================================================================
# RuntimeInfo models
# ============================================================================


class TestRuntimeInfo:
    """Verify base RuntimeInfo with status and duration."""
    def test_defaults(self) -> None:
        ri = RuntimeInfo()
        assert ri.status == DataFlowStatus.PENDING.value
        assert ri.duration_seconds is None

    def test_duration(self) -> None:
        start = datetime(2026, 2, 12, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 12, 10, 0, 30, tzinfo=timezone.utc)
        ri = RuntimeInfo(start_time=start, end_time=end, status="succeeded")
        assert ri.duration_seconds == 30.0


class TestSourceRuntimeInfo:
    """Verify SourceRuntimeInfo tracking row counts and watermarks."""
    def test_defaults(self) -> None:
        sri = SourceRuntimeInfo()
        assert sri.rows_read == 0
        assert sri.source_action == {}
        assert sri.watermark_before is None


class TestTransformRuntimeInfo:
    """Verify TransformRuntimeInfo tracking applied transformers."""
    def test_defaults(self) -> None:
        tri = TransformRuntimeInfo()
        assert tri.transformers_applied == []


class TestDestinationRuntimeInfo:
    """Verify DestinationRuntimeInfo tracking rows written/inserted/updated."""
    def test_defaults(self) -> None:
        dri = DestinationRuntimeInfo()
        assert dri.rows_written == 0
        assert dri.rows_inserted == 0
        assert dri.rows_updated == 0
        assert dri.rows_deleted == 0


class TestDataFlowRuntimeInfo:
    """Verify DataFlowRuntimeInfo aggregating source, transform, destination info."""
    def test_proxy_properties(self) -> None:
        info = DataFlowRuntimeInfo(
            source=SourceRuntimeInfo(rows_read=100),
            destination=DestinationRuntimeInfo(
                rows_written=90, rows_inserted=80, rows_updated=10
            ),
        )
        assert info.rows_read == 100
        assert info.rows_written == 90
        assert info.rows_inserted == 80
        assert info.rows_updated == 10

    def test_success_state(self) -> None:
        info = DataFlowRuntimeInfo(status=DataFlowStatus.SUCCEEDED.value)
        assert info.is_success is True
        assert info.is_failed is False

    def test_failed_state(self) -> None:
        info = DataFlowRuntimeInfo(status=DataFlowStatus.FAILED.value)
        assert info.is_failed is True
        assert info.is_success is False


class TestDestinationRuntimeInfoBytesSaved:
    """Verify bytes_saved calculation for compaction metrics."""
    def test_bytes_saved(self) -> None:
        dri = DestinationRuntimeInfo(bytes_added=100, bytes_removed=500)
        assert dri.bytes_saved == 400

    def test_bytes_saved_no_negative(self) -> None:
        dri = DestinationRuntimeInfo(bytes_added=500, bytes_removed=100)
        assert dri.bytes_saved == 0


class TestJobRuntimeInfo:
    """Verify JobRuntimeInfo summarizing overall job execution."""
    def test_defaults(self) -> None:
        log = JobRuntimeInfo()
        assert log.total_dataflows == 0
        assert log.total_succeeded == 0
        assert log.job_id  # auto-generated
        assert log.duration_seconds is None

    def test_duration(self) -> None:
        start = datetime(2026, 2, 12, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 12, 10, 5, 0, tzinfo=timezone.utc)
        log = JobRuntimeInfo(start_time=start, end_time=end)
        assert log.duration_seconds == 300.0


# ============================================================================
# Extended Connection Tests
# ============================================================================


class TestConnectionExtended:
    """Verify Connection advanced properties: catalog, database, partitioning."""
    def test_populate_catalog_and_database_from_configure(self) -> None:
        conn = Connection(name="c", configure={"catalog": "cat1", "database": "db1"})
        assert conn.catalog == "cat1"
        assert conn.database == "db1"

    def test_date_folder_and_hive_partitioning(self) -> None:
        conn = Connection(
            name="c",
            configure={"date_folder_partitions": "year={year}/month={month}", "use_hive_partitioning": "true"},
        )
        assert conn.date_folder_partitions == "year={year}/month={month}"
        assert conn.use_hive_partitioning is True

    def test_date_backward_from_flat_and_nested(self) -> None:
        conn = Connection(
            name="c",
            configure={"backward_days": "7", "backward": {"months": 2, "closing_day": 10}},
        )
        assert conn.date_backward == {"days": 7, "months": 2, "closing_day": 10}

    def test_date_backward_none_when_not_set(self) -> None:
        conn = Connection(name="c", configure={})
        assert conn.date_backward is None

    def test_generate_manifest_default_false(self) -> None:
        conn = Connection(name="c", configure={})
        assert conn.generate_manifest is False

    def test_generate_manifest_true_from_configure(self) -> None:
        conn = Connection(name="c", configure={"generate_manifest": True})
        assert conn.generate_manifest is True

    def test_generate_manifest_truthy_string(self) -> None:
        conn = Connection(name="c", configure={"generate_manifest": "true"})
        assert conn.generate_manifest is True

    def test_athena_output_location_none_when_absent(self) -> None:
        conn = Connection(name="c", configure={})
        assert conn.athena_output_location is None

    def test_athena_output_location_returns_value(self) -> None:
        conn = Connection(name="c", configure={"athena_output_location": "s3://bucket/results/"})
        assert conn.athena_output_location == "s3://bucket/results/"

    def test_athena_output_location_empty_string_returns_none(self) -> None:
        conn = Connection(name="c", configure={"athena_output_location": ""})
        assert conn.athena_output_location is None

    def test_register_symlink_table_default_false(self) -> None:
        conn = Connection(name="c", configure={})
        assert conn.register_symlink_table is False

    def test_register_symlink_table_true_from_configure(self) -> None:
        conn = Connection(name="c", configure={"register_symlink_table": True})
        assert conn.register_symlink_table is True

    def test_symlink_database_prefix_default(self) -> None:
        conn = Connection(name="c", configure={})
        assert conn.symlink_database_prefix == "symlink_"

    def test_symlink_database_prefix_custom(self) -> None:
        conn = Connection(name="c", configure={"symlink_database_prefix": "sl_"})
        assert conn.symlink_database_prefix == "sl_"


# ============================================================================
# Extended Source Tests
# ============================================================================


class TestSourceExtended:
    """Verify Source advanced properties: namespace, date backward."""
    def test_namespace_none_when_no_parts(self, file_connection: Connection) -> None:
        src = Source(connection=file_connection)
        assert src.namespace is None

    def test_namespace_with_catalog_database_schema(self, db_connection: Connection) -> None:
        src = Source(connection=db_connection, schema_name="dbo", table="t")
        assert src.namespace == "`ERP`.`dbo`"

    def test_date_backward_source_overrides_connection(self, file_connection: Connection) -> None:
        file_connection.configure["backward_days"] = 1
        src = Source(connection=file_connection, table="t", configure={"backward_days": 3})
        assert src.date_backward == {"days": 3}

    def test_date_backward_falls_back_to_connection(self, file_connection: Connection) -> None:
        file_connection.configure["backward_days"] = 2
        src = Source(connection=file_connection, table="t")
        assert src.date_backward == {"days": 2}

    def test_source_configure_from_json(self, file_connection: Connection) -> None:
        src = Source(connection=file_connection, table="t", configure='{"read_options": {"header": "true"}}')
        assert src.read_options["header"] == "true"


# ============================================================================
# Extended Destination Tests
# ============================================================================


class TestDestinationExtended:
    """Verify Destination advanced properties: partition lifting, extended merge keys."""
    def test_partition_columns_lifted_from_configure(self, dest_connection: Connection) -> None:
        dest = Destination(
            connection=dest_connection,
            table="t",
            configure={"partition_columns": [{"column": "region"}]},
        )
        assert dest.partition_column_names == ["region"]
        assert "partition_columns" not in dest.configure

    def test_namespace_none_when_no_parts(self, dest_connection: Connection) -> None:
        dest = Destination(connection=dest_connection, table="t")
        assert dest.namespace is None

    def test_merge_keys_extended_adds_partition_columns(self, dest_connection: Connection) -> None:
        dest = Destination(
            connection=dest_connection,
            table="t",
            merge_keys=["id"],
            partition_columns=[PartitionColumn(column="year"), PartitionColumn(column="id")],
        )
        assert dest.merge_keys_extended == ["id", "year"]

    def test_partition_columns_accept_partition_column_object(self, dest_connection: Connection) -> None:
        pc = PartitionColumn(column="dt")
        dest = Destination(connection=dest_connection, table="t", partition_columns=[pc])
        assert dest.partition_columns[0] is pc


# ============================================================================
# Extended Transform Tests
# ============================================================================


class TestTransformExtended:
    """Verify Transform advanced properties: timestamp_ntz, rank deduplication."""
    def test_convert_timestamp_ntz_default_and_override(self) -> None:
        t1 = Transform()
        assert t1.convert_timestamp_ntz is True
        t2 = Transform(configure={"convert_timestamp_ntz": False})
        assert t2.convert_timestamp_ntz is False

    def test_deduplicate_by_rank_default_and_override(self) -> None:
        t1 = Transform()
        assert t1.deduplicate_by_rank is False
        t2 = Transform(configure={"deduplicate_by_rank": True})
        assert t2.deduplicate_by_rank is True

    def test_transform_configure_from_json(self) -> None:
        t = Transform(configure='{"deduplicate_by_rank": true}')
        assert t.deduplicate_by_rank is True

    def test_transform_coercion_single_items(self) -> None:
        t = Transform(
            additional_columns={"column": "source", "expression": "'x'"},
            schema_hints={"column_name": "amount", "data_type": "DECIMAL"},
            latest_data_columns="updated_at",
            deduplicate_columns="id",
        )
        assert [a.column for a in t.additional_columns] == ["source"]
        assert list(t.schema_hints_dict) == ["amount"]
        assert t.latest_data_columns == ["updated_at"]
        assert t.deduplicate_columns == ["id"]


# ============================================================================
# Extended DataFlow Tests
# ============================================================================


class TestDataFlowExtended:
    """Verify DataFlow derived properties and proxies."""
    def test_dataflow_id_is_derived_from_name(self, sample_source: Source, sample_destination: Destination) -> None:
        df = DataFlow(name="df_name", source=sample_source, destination=sample_destination)
        assert isinstance(df.dataflow_id, str)
        assert df.dataflow_id

    def test_deduplicate_columns_proxy_fallback(self, sample_source: Source, sample_destination: Destination) -> None:
        df = DataFlow(source=sample_source, destination=sample_destination, transform=Transform(), configure={})
        # no explicit dedup cols on transform -> falls back to merge keys
        assert df.deduplicate_columns == df.merge_keys

    def test_order_columns_fallback_to_watermark(self, file_connection: Connection, sample_destination: Destination) -> None:
        src = Source(connection=file_connection, table="orders", watermark_columns=["wm_col"])
        df = DataFlow(source=src, destination=sample_destination, transform=Transform())
        assert df.order_columns == ["wm_col"]

    def test_dataflow_configure_from_json(self, sample_source: Source, sample_destination: Destination) -> None:
        df = DataFlow(source=sample_source, destination=sample_destination, configure='{"x": 1}')
        assert df.configure == {"x": 1}


# ============================================================================
# Run Config and Runtime Extended
# ============================================================================


class TestDataCoolieRunConfigExtended:
    """Verify DataCoolieRunConfig edge cases."""
    def test_empty_job_id_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="job_id"):
            DataCoolieRunConfig(job_id="")


class TestRuntimeInfoExtended:
    """Verify RuntimeInfo edge cases."""
    def test_dataflow_rows_deleted_proxy(self) -> None:
        info = DataFlowRuntimeInfo(destination=DestinationRuntimeInfo(rows_deleted=3))
        assert info.rows_deleted == 3


# ============================================================================
# Internal Validators and Edge Case Coverage
# ============================================================================


class TestModelInternalBranches:
    """Test internal validator branches for complete coverage."""
    def test_connection_internal_validators_branches(self) -> None:
        # _derive_connection_id_from_name: branch where id is already provided
        values = {"name": "conn", "connection_id": "fixed-id"}
        out = Connection._derive_connection_id_from_name(values.copy())
        assert out["connection_id"] == "fixed-id"

        # _normalise_format non-str branch
        assert Connection._normalise_format(123) == 123

        # _parse_secrets_ref with non-list source values uses 'continue' branch
        parsed = Connection._parse_secrets_ref({"scope_a": "not-a-list"})
        assert parsed == {"scope_a": "not-a-list"}

    def test_source_with_catalog_and_nested_backward(self) -> None:
        conn = Connection(name="c", catalog="cat", database="db", configure={"base_path": "/data"})
        src = Source(
            connection=conn,
            schema_name="sch",
            table="tbl",
            configure={"backward": {"months": 1}},
        )
        assert src.full_table_name == "`cat`.`db`.`sch`.`tbl`"
        assert src.namespace == "`cat`.`db`.`sch`"
        assert src.path == "data/sch/tbl"
        assert src.date_backward == {"months": 1}

    def test_destination_internal_validators_branches(self, dest_connection: Connection) -> None:
        # schema_name/load_type non-str return branch
        assert Destination._normalise_schema_name(10) == 10
        assert Destination._normalise_load_type(10) == 10

        # partition columns edge branches
        assert Destination._coerce_partition_columns(None) == []
        assert Destination._coerce_partition_columns([123]) == [123]

        # model validator non-dict passthrough branch
        assert Destination._lift_partition_columns_from_configure("not-a-dict") == "not-a-dict"

        # destination with no base_path covers path None branch
        no_base = Connection(name="nobase", configure={})
        dest = Destination(connection=no_base, table="t")
        assert dest.path is None

    def test_destination_with_catalog_and_database(self) -> None:
        conn = Connection(name="c", catalog="cat", database="db", configure={"base_path": "/x"})
        dest = Destination(connection=conn, schema_name="sch", table="tbl")
        assert dest.full_table_name == "`cat`.`db`.`sch`.`tbl`"
        assert dest.namespace == "`cat`.`db`.`sch`"
        assert dest.path == "x/sch/tbl"

    def test_source_full_table_without_schema_and_source_path_without_schema(self) -> None:
        conn = Connection(name="c", catalog="cat", database="db", configure={"base_path": "/data"})
        src = Source(connection=conn, table="tbl")
        assert src.full_table_name == "`cat`.`db`.`tbl`"
        assert src.path == "data/tbl"

    def test_source_path_without_table_stops_after_schema(self, file_connection: Connection) -> None:
        src = Source(connection=file_connection, schema_name="sch", table=None)
        assert src.path is None

    def test_destination_full_table_and_path_without_schema(self) -> None:
        conn = Connection(name="c", catalog="cat", database="db", configure={"base_path": "/x"})
        dest = Destination(connection=conn, table="tbl")
        assert dest.full_table_name == "`cat`.`db`.`tbl`"
        assert dest.path == "x/tbl"

    def test_transform_internal_branches(self) -> None:
        assert Transform._coerce_dedup(None) == []
        assert Transform._coerce_additional([]) == []
        assert Transform._coerce_hints([]) == []

    def test_dataflow_internal_branches(self, sample_source: Source, sample_destination: Destination) -> None:
        # _derive_dataflow_id_from_name branch with pre-supplied dataflow_id
        values = {"name": "df", "dataflow_id": "id-1"}
        out = DataFlow._derive_dataflow_id_from_name(values.copy())
        assert out["dataflow_id"] == "id-1"

        # processing mode non-str branch
        assert DataFlow._normalise_mode(10) == 10

        df = DataFlow(source=sample_source, destination=sample_destination)
        assert df.partition_columns == sample_destination.partition_columns

    def test_run_config_validate_constraints_direct(self) -> None:
        # Force explicit execution of the job_id empty branch in _validate_constraints
        cfg = DataCoolieRunConfig.model_construct(
            job_id="",
            job_num=1,
            job_index=0,
            max_workers=1,
            stop_on_error=False,
            retry_count=0,
            retry_delay=0.0,
            dry_run=False,
            retention_hours=0,
        )
        with pytest.raises(ConfigurationError, match="job_id"):
            DataCoolieRunConfig._validate_constraints(cfg)


# ============================================================================
# Fixture Validation
# ============================================================================


class TestFixtureDiversity:
    """Validate richer shared fixtures in tests/conftest.py."""
    """Validate richer shared fixtures in tests/conftest.py."""

    def test_api_connection_fixture_shape(self, api_connection: Connection) -> None:
        assert api_connection.connection_type == "api"
        assert api_connection.format == "api"
        assert api_connection.configure["base_url"] == "https://api.example.com"
        assert api_connection.secrets_ref == {"shared-secrets": ["auth_token"]}
        assert api_connection.connection_id == name_to_uuid("public_api")

    def test_file_connection_with_backward_fixture(self, file_connection_with_backward: Connection) -> None:
        assert file_connection_with_backward.date_backward == {"days": 3}
        assert file_connection_with_backward.use_hive_partitioning is True
        assert file_connection_with_backward.date_folder_partitions == "year={year}/month={month}/day={day}"

    def test_dataflow_no_name_fixture_uses_nullable_id(self, sample_dataflow_no_name: DataFlow) -> None:
        assert sample_dataflow_no_name.name is None
        assert sample_dataflow_no_name.dataflow_id is None


class TestConnectionTypeFormatValidation:
    """Tests for _validate_connection_type_format model validator on Connection."""

    # -- auto-derive connection_type ------------------------------------------

    def test_auto_derive_delta_to_lakehouse(self) -> None:
        conn = Connection(name="x", format="delta", configure={})
        assert conn.connection_type == "lakehouse"

    def test_auto_derive_iceberg_to_lakehouse(self) -> None:
        conn = Connection(name="x", format="iceberg", configure={})
        assert conn.connection_type == "lakehouse"

    def test_auto_derive_parquet_to_file(self) -> None:
        conn = Connection(name="x", format="parquet", configure={})
        assert conn.connection_type == "file"

    def test_auto_derive_csv_to_file(self) -> None:
        conn = Connection(name="x", format="csv", configure={})
        assert conn.connection_type == "file"

    def test_auto_derive_json_to_file(self) -> None:
        conn = Connection(name="x", format="json", configure={})
        assert conn.connection_type == "file"

    def test_auto_derive_sql_to_database(self) -> None:
        conn = Connection(name="x", format="sql", configure={})
        assert conn.connection_type == "database"

    def test_auto_derive_api_to_api(self) -> None:
        conn = Connection(name="x", format="api", configure={})
        assert conn.connection_type == "api"

    def test_auto_derive_function_to_function(self) -> None:
        conn = Connection(name="x", format="function", configure={})
        assert conn.connection_type == "function"

    # -- explicit valid pairs pass -------------------------------------------

    def test_explicit_lakehouse_delta_valid(self) -> None:
        conn = Connection(name="x", connection_type="lakehouse", format="delta", configure={})
        assert conn.connection_type == "lakehouse"
        assert conn.format == "delta"

    def test_explicit_file_parquet_valid(self) -> None:
        conn = Connection(name="x", connection_type="file", format="parquet", configure={})
        assert conn.connection_type == "file"

    def test_explicit_database_sql_valid(self) -> None:
        conn = Connection(name="x", connection_type="database", format="sql", configure={})
        assert conn.connection_type == "database"

    def test_explicit_api_api_valid(self) -> None:
        conn = Connection(name="x", connection_type="api", format="api", configure={})
        assert conn.connection_type == "api"

    # -- explicit invalid pairs raise ----------------------------------------

    def test_explicit_file_delta_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="delta"):
            Connection(name="x", connection_type="file", format="delta", configure={})

    def test_explicit_file_iceberg_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="iceberg"):
            Connection(name="x", connection_type="file", format="iceberg", configure={})

    def test_explicit_lakehouse_parquet_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="parquet"):
            Connection(name="x", connection_type="lakehouse", format="parquet", configure={})

    def test_explicit_database_delta_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="delta"):
            Connection(name="x", connection_type="database", format="delta", configure={})

    # -- streaming raises for any format -------------------------------------

    def test_streaming_any_format_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="streaming"):
            Connection(name="x", connection_type="streaming", format="parquet", configure={})

    # -- unknown connection_type raises --------------------------------------

    def test_unknown_connection_type_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="Unknown connection_type"):
            Connection(name="x", connection_type="nosuchtype", format="parquet", configure={})
