"""Shared pytest fixtures for the datacoolie test suite."""

from __future__ import annotations

import logging

import pytest

from datacoolie.core.constants import Format, LoadType
from datacoolie.core.models import (
    AdditionalColumn,
    Connection,
    DataFlow,
    Destination,
    DataCoolieRunConfig,
    PartitionColumn,
    SchemaHint,
    Source,
    Transform,
)
from tests.integration.cloud_config import (
    pytest_add_cloud_options as _add_cloud_options,
    pytest_configure_cloud as _configure_cloud,
    pytest_gate_cloud_items as _gate_cloud_items,
)


def pytest_addoption(parser: pytest.Parser) -> None:
    _add_cloud_options(parser)


def pytest_configure(config: pytest.Config) -> None:
    _configure_cloud(config)


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    _gate_cloud_items(config, items)


# ---------------------------------------------------------------------------
# Wire pytest caplog into the datacoolie logger hierarchy
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _caplog_datacoolie(caplog: pytest.LogCaptureFixture):
    """Ensure caplog captures records from the ``datacoolie`` logger.

    ``datacoolie`` sets ``propagate = False`` to avoid duplicate console
    output, which prevents records from reaching the root logger where
    pytest's caplog handler lives.  This fixture temporarily attaches
    caplog's handler directly to the ``datacoolie`` logger.
    """
    dc_logger = logging.getLogger("datacoolie")
    dc_logger.addHandler(caplog.handler)
    yield
    dc_logger.removeHandler(caplog.handler)


# ---------------------------------------------------------------------------
# Connection fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def file_connection() -> Connection:
    """A file-based Delta connection."""
    return Connection(
        name="bronze_adls",
        connection_type="lakehouse",
        format=Format.DELTA.value,
        configure={
            "base_path": "abfss://bronze@storage.dfs.core.windows.net/",
            "read_options": {"inferSchema": "false"},
            "write_options": {"optimizeWrite": "true"},
            "use_schema_hint": True,
        },
    )


@pytest.fixture()
def db_connection() -> Connection:
    """A JDBC database connection."""
    return Connection(
        name="source_erp",
        connection_type="database",
        format=Format.SQL.value,
        database="ERP",
        configure={
            "host": "erp-server.example.com",
            "port": 1433,
            "username": "etl_reader",
            "password": "s3cret!",
            "read_options": {"fetchsize": "10000"},
        },
    )


@pytest.fixture()
def dest_connection() -> Connection:
    """A file-based Delta destination connection."""
    return Connection(
        name="silver_lakehouse",
        connection_type="lakehouse",
        format=Format.DELTA.value,
        configure={
            "base_path": "abfss://silver@storage.dfs.core.windows.net/",
            "write_options": {"optimizeWrite": "true"},
        },
    )


@pytest.fixture()
def api_connection() -> Connection:
    """An API connection with auth and pagination settings."""
    return Connection(
        name="public_api",
        connection_type="api",
        format="api",
        configure={
            "base_url": "https://api.example.com",
            "auth_type": "bearer",
            "auth_token": "token-ref",
            "read_options": {"timeout": 30},
        },
        secrets_ref={"shared-secrets": ["auth_token"]},
    )


@pytest.fixture()
def file_connection_with_backward() -> Connection:
    """A file connection including backward date-folder options."""
    return Connection(
        name="bronze_with_backward",
        connection_type="file",
        format=Format.PARQUET.value,
        configure={
            "base_path": "abfss://bronze@storage.dfs.core.windows.net/",
            "backward_days": 3,
            "use_hive_partitioning": True,
            "date_folder_partitions": "year={year}/month={month}/day={day}",
        },
    )


# ---------------------------------------------------------------------------
# Source / Destination / Transform fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_source(file_connection: Connection) -> Source:
    return Source(
        connection=file_connection,
        schema_name="sales",
        table="orders",
        watermark_columns=["modified_at"],
    )


@pytest.fixture()
def sample_destination(dest_connection: Connection) -> Destination:
    return Destination(
        connection=dest_connection,
        schema_name="curated",
        table="dim_orders",
        load_type=LoadType.MERGE_UPSERT.value,
        merge_keys=["order_id"],
        partition_columns=[PartitionColumn(column="year", expression="year(order_date)")],
    )


@pytest.fixture()
def sample_transform() -> Transform:
    return Transform(
        deduplicate_columns=["order_id", "modified_at"],
        latest_data_columns=["modified_at"],
        additional_columns=[
            AdditionalColumn(column="etl_source", expression="'erp'"),
        ],
        schema_hints=[
            SchemaHint(column_name="order_date", data_type="DATE", format="yyyy-MM-dd"),
            SchemaHint(column_name="amount", data_type="DECIMAL", precision=18, scale=2),
        ],
    )


# ---------------------------------------------------------------------------
# DataFlow fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_dataflow(
    sample_source: Source,
    sample_destination: Destination,
    sample_transform: Transform,
) -> DataFlow:
    """A fully populated DataFlow."""
    return DataFlow(
        name="orders_bronze_to_silver",
        stage="bronze2silver",
        group_number=1,
        execution_order=0,
        source=sample_source,
        destination=sample_destination,
        transform=sample_transform,
    )


@pytest.fixture()
def sample_dataflow_no_name(
    sample_source: Source,
    sample_destination: Destination,
) -> DataFlow:
    """A DataFlow without name to exercise nullable id semantics."""
    return DataFlow(
        source=sample_source,
        destination=sample_destination,
    )


# ---------------------------------------------------------------------------
# ETLRunConfig fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def default_run_config() -> DataCoolieRunConfig:
    return DataCoolieRunConfig()


@pytest.fixture()
def multi_job_config() -> DataCoolieRunConfig:
    return DataCoolieRunConfig(job_num=3, job_index=1, max_workers=8)
