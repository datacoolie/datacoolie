"""Shared test helpers for logging unit tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

from datacoolie.core.constants import DataFlowStatus, ExecutionType, Format, LoadType
from datacoolie.core.models import (
    Connection,
    DataCoolieRunConfig,
    DataFlow,
    DataFlowRuntimeInfo,
    Destination,
    DestinationRuntimeInfo,
    Source,
    SourceRuntimeInfo,
)
from datacoolie.logging.base import LogConfig
from datacoolie.logging.etl_logger import ETLLogger
from datacoolie.platforms.local_platform import LocalPlatform


def make_connection() -> Connection:
    return Connection(name="test", format=Format.DELTA.value, configure={"base_path": "/data"})


def make_dataflow(df_id: str = "df-1", stage: str = "bronze") -> DataFlow:
    conn = make_connection()
    return DataFlow(
        dataflow_id=df_id,
        stage=stage,
        source=Source(connection=conn, table="src_table"),
        destination=Destination(connection=conn, table="dst_table", load_type=LoadType.APPEND.value),
    )


def make_runtime(
    df_id: str = "df-1",
    status: str = DataFlowStatus.SUCCEEDED.value,
    rows_read: int = 100,
    rows_written: int = 100,
) -> DataFlowRuntimeInfo:
    return DataFlowRuntimeInfo(
        dataflow_id=df_id,
        status=status,
        source=SourceRuntimeInfo(rows_read=rows_read),
        destination=DestinationRuntimeInfo(rows_written=rows_written),
        start_time=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 15, 10, 1, 0, tzinfo=timezone.utc),
    )


def make_maintenance_runtime(
    df_id: str = "df-1",
    status: str = DataFlowStatus.SUCCEEDED.value,
) -> DataFlowRuntimeInfo:
    return DataFlowRuntimeInfo(
        dataflow_id=df_id,
        status=status,
        operation_type=ExecutionType.MAINTENANCE.value,
        destination=DestinationRuntimeInfo(
            status=status,
            operation_type=ExecutionType.MAINTENANCE.value,
            files_added=1,
            files_removed=3,
            bytes_added=100,
            bytes_removed=500,
        ),
        start_time=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 15, 10, 0, 30, tzinfo=timezone.utc),
    )


def make_logger(**overrides: Any) -> tuple[ETLLogger, MagicMock]:
    platform = MagicMock()
    defaults = {"output_path": "/logs"}
    defaults.update(overrides)
    cfg = LogConfig(**defaults)
    logger = ETLLogger(cfg, platform)
    logger.set_run_config(DataCoolieRunConfig(job_id="j1"))
    return logger, platform


def make_real_logger(tmp_path, **overrides: Any) -> tuple[ETLLogger, LocalPlatform]:
    platform = LocalPlatform(base_path=str(tmp_path))
    defaults = {"output_path": "logs"}
    defaults.update(overrides)
    cfg = LogConfig(**defaults)
    logger = ETLLogger(cfg, platform)
    logger.set_run_config(DataCoolieRunConfig(job_id="j1"))
    return logger, platform
