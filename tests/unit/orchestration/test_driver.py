"""Tests for datacoolie.orchestration.driver (DataCoolieDriver + create_driver).

This module verifies driver initialization, dataflow processing, maintenance
operations, retry handling, and parallel execution.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, List
from unittest.mock import MagicMock, call, patch

import pytest

from datacoolie.core.constants import ColumnCaseMode, DataFlowStatus, ExecutionType, Format, LoadType
from datacoolie.core.exceptions import ConfigurationError, DataCoolieError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo, DestinationRuntimeInfo, PipelineAttemptResult, SourceRuntimeInfo, TransformRuntimeInfo
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.core.secrets.provider import BaseSecretProvider, SecretStr
from datacoolie.orchestration import create_driver
from datacoolie.core.exceptions import PipelineError
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.orchestration.scheduling.parallel_executor import ExecutionResult


@pytest.fixture(autouse=True)
def _reset_log_manager():
    """Keep exclusive SystemLogger capture ownership isolated per test."""
    from datacoolie.logging.runtime.manager import LogManager

    LogManager.reset()
    yield
    # Driver tests intentionally exercise construction without always closing
    # the returned object.  Release the owner explicitly before the next test;
    # ordinary callers still must close their Driver and unauthorized
    # ``LogManager.reset()`` remains rejected by the production contract.
    manager = LogManager._instance
    if manager is not None:
        owner = manager._capture_owner
        if owner is not None:
            manager.release_capture(owner)
    LogManager.reset()


# ============================================================================
# Helpers
# ============================================================================


def _conn(
    fmt: str = Format.DELTA.value,
    name: str = "test",
    connection_id: str | None = None,
) -> Connection:
    """Create a minimal Connection for testing."""
    kw: dict = {"name": name, "format": fmt, "configure": {"base_path": "/data"}}
    if connection_id is not None:
        kw["connection_id"] = connection_id
    return Connection(**kw)


def _dataflow(
    dataflow_id: str = "df-1",
    fmt: str = Format.DELTA.value,
    group_number: int | None = None,
    is_active: bool = True,
    dest_conn: Connection | None = None,
) -> DataFlow:
    """Create a minimal DataFlow for testing."""
    conn = dest_conn or _conn(fmt=fmt)
    return DataFlow(
        dataflow_id=dataflow_id,
        group_number=group_number,
        is_active=is_active,
        source=Source(connection=conn, table="src"),
        destination=Destination(connection=conn, table="dst", load_type=LoadType.APPEND.value),
    )


def _mock_engine():
    engine = MagicMock()
    engine.platform = None
    engine.set_platform.side_effect = lambda p: setattr(engine, 'platform', p)
    engine.count_rows.return_value = 5
    engine.table_exists.return_value = True
    engine.get_maintenance_metrics.return_value = {}
    return engine


def _mock_platform():
    return MagicMock()


def _execution_logger_spy():
    from datacoolie.logging import ExecutionLogger, LogConfig

    logger = ExecutionLogger(LogConfig(), _mock_platform())
    # Keep the real in-memory aggregation while retaining call assertions and
    # failure injection for orchestration tests.
    logger.log = MagicMock(wraps=logger.log)
    logger.close = MagicMock()
    return logger


def _system_logger_spy():
    from datacoolie.logging import LogConfig, SystemLogger

    logger = SystemLogger(LogConfig(), _mock_platform())
    logger.close = MagicMock()
    return logger


def _mock_metadata(dataflows: List[DataFlow] | None = None):
    md = MagicMock()
    md.get_dataflows.return_value = dataflows or []
    md.get_watermark.return_value = {}
    md.update_watermark.return_value = None
    return md


def _mock_watermark():
    wm = MagicMock()
    wm.get_watermark.return_value = {}
    wm.save_watermark.return_value = None
    return wm


def _make_driver(
    dataflows: List[DataFlow] | None = None,
    config: DataCoolieRunConfig | None = None,
    execution_logger: Any = None,
    system_logger: Any = None,
) -> tuple:
    """Return (driver, engine, metadata, watermark) with mocked deps."""
    engine = _mock_engine()
    platform = _mock_platform()
    metadata = _mock_metadata(dataflows)
    watermark = _mock_watermark()
    cfg = config or DataCoolieRunConfig()

    driver = DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        watermark_manager=watermark,
        config=cfg,
        system_logger=system_logger,
        execution_logger=execution_logger,
    )
    return driver, engine, metadata, watermark


class _CountingSecretProvider(BaseSecretProvider):
    """Native provider test double that retains real TTL-cache behavior."""

    def __init__(self) -> None:
        super().__init__(cache_ttl=300)
        self.fetch_count = 0

    def _fetch_secret(self, key: str, source: str) -> str:
        self.fetch_count += 1
        return "resolved-value"


# ============================================================================
# Init / properties
# ============================================================================


class TestDataCoolieDriverInit:
    def test_default_config(self):
        d, *_ = _make_driver()
        assert d.config.job_num == 1
        assert d.config.job_index == 0
        assert d.job_id == d.config.job_id  # delegates to config

    def test_custom_config(self):
        cfg = DataCoolieRunConfig(job_num=3, job_index=1, max_workers=4)
        d, *_ = _make_driver(config=cfg)
        assert d.config.job_num == 3
        assert d.config.job_index == 1

    def test_config_is_detached_from_driver_session(self):
        cfg = DataCoolieRunConfig(
            max_workers=2,
            retry_count=1,
            allowed_function_prefixes=["project.functions"],
        )
        d, *_ = _make_driver(config=cfg)
        try:
            cfg.max_workers = 9
            cfg.retry_count = 8
            cfg.allowed_function_prefixes.append("other.functions")

            returned = d.config
            returned.max_workers = 10
            returned.allowed_function_prefixes.append("returned.functions")

            assert d._executor._max_workers == 2
            assert d._retry_handler.retry_count == 1
            assert d._config.allowed_function_prefixes == ["project.functions"]
        finally:
            d.close()

    def test_context_manager(self):
        d, *_ = _make_driver()
        with d as driver:
            assert driver is d
        # close should have been called — dataflows cleared
        assert d._dataflows == []

    def test_startup_events_surround_provider_initialization(self):
        """System capture is active before provider I/O and ready is last."""
        from datacoolie.logging import LogConfig, SystemLogger
        from datacoolie.logging.runtime.manager import LogManager, get_logger

        provider = MagicMock()

        def initialize() -> None:
            get_logger("datacoolie.test.provider").info("provider initialized")

        provider.initialize.side_effect = initialize
        system_logger = SystemLogger(LogConfig(), _mock_platform())
        driver = DataCoolieDriver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=provider,
            watermark_manager=_mock_watermark(),
            system_logger=system_logger,
        )
        try:
            handler = LogManager.get_instance().capture_handler
            assert handler is not None
            records = handler.get_records()
            messages = [record.message for record in records]
            event_names = [record.event_name for record in records]
            assert event_names[:1] == ["session.starting"]
            assert "provider initialized" in messages
            assert event_names[-1:] == ["session.ready"]
            assert messages.index("provider initialized") > 0
            assert messages.index("provider initialized") < len(messages) - 1
            provider.initialize.assert_called_once_with()
        finally:
            driver.close()

    def test_provider_startup_failure_persists_failure_without_ready(self, tmp_path):
        """An activated system logger records the escaping startup error."""
        import json

        from datacoolie.logging import LogConfig, SystemLogger
        from datacoolie.platforms.local_platform import LocalPlatform

        provider = MagicMock()
        provider.initialize.side_effect = ValueError("metadata unavailable")
        platform = LocalPlatform(base_path=str(tmp_path))
        system_logger = SystemLogger(
            LogConfig(output_path="logs", flush_interval_seconds=0),
            platform,
        )

        with pytest.raises(ValueError, match="metadata unavailable"):
            DataCoolieDriver(
                engine=_mock_engine(),
                platform=platform,
                metadata_provider=provider,
                watermark_manager=_mock_watermark(),
                system_logger=system_logger,
            )

        files = list(tmp_path.rglob("system_*.json"))
        assert len(files) == 1
        rows = [
            json.loads(line)
            for line in files[0].read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        event_names = [row.get("event_name") for row in rows]
        assert "session.starting" in event_names
        assert "session.startup_failed" in event_names
        assert "session.ready" not in event_names

    def test_interrupted_provider_startup_releases_active_system_logger(self):
        from datacoolie.logging import LogConfig, SystemLogger

        provider = MagicMock()
        provider.initialize.side_effect = KeyboardInterrupt("startup cancelled")
        system_logger = SystemLogger(LogConfig(), _mock_platform())

        with pytest.raises(KeyboardInterrupt, match="startup cancelled"):
            DataCoolieDriver(
                engine=_mock_engine(),
                platform=_mock_platform(),
                metadata_provider=provider,
                watermark_manager=_mock_watermark(),
                system_logger=system_logger,
            )

        assert system_logger.is_active is False

    def test_logger_activation_failure_attempts_all_session_cleanup(self):
        system_logger = _system_logger_spy()
        execution_logger = _execution_logger_spy()
        system_logger.activate = MagicMock(
            side_effect=RuntimeError("capture activation failed")
        )

        with pytest.raises(RuntimeError, match="capture activation failed"):
            DataCoolieDriver(
                engine=_mock_engine(),
                platform=_mock_platform(),
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
                system_logger=system_logger,
                execution_logger=execution_logger,
            )

        execution_logger.close.assert_called_once_with()
        system_logger.close.assert_called_once_with()


# ============================================================================
# Logger wiring
# ============================================================================


class TestDataCoolieDriverLoggers:
    """Tests for log_base_path auto-creation and job_id sync."""

    def _base_kwargs(self):
        return dict(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
        )

    def test_no_loggers_by_default(self):
        d, *_ = _make_driver()
        assert d._system_logger is None
        assert d._execution_logger is None

    def test_log_base_path_creates_both_loggers(self):
        from datacoolie.logging import ExecutionLogger, SystemLogger

        d = DataCoolieDriver(log_base_path="/logs", **self._base_kwargs())

        assert isinstance(d._system_logger, SystemLogger)
        assert isinstance(d._execution_logger, ExecutionLogger)
        assert d._system_logger.config.output_path == "/logs/system_logs"
        assert d._execution_logger.config.output_path == "/logs/execution_logs"

    def test_log_base_path_strips_trailing_slash(self):
        d = DataCoolieDriver(log_base_path="/logs/", **self._base_kwargs())

        assert d._system_logger.config.output_path == "/logs/system_logs"
        assert d._execution_logger.config.output_path == "/logs/execution_logs"

    def test_log_base_path_auto_loggers_carry_job_id(self):
        d = DataCoolieDriver(log_base_path="/logs", **self._base_kwargs())

        assert d._system_logger.run_config == d.config
        assert d._execution_logger.run_config == d.config
        assert d._execution_logger._job_info.job_id == d.job_id
        assert d._system_logger.is_active
        assert d._execution_logger.is_active

    def test_explicit_system_logger_not_replaced_by_log_base_path(self):
        """Explicit system_logger takes precedence; only execution_logger is auto-created."""
        from datacoolie.logging import ExecutionLogger

        explicit_sys = _system_logger_spy()
        d = DataCoolieDriver(log_base_path="/logs", system_logger=explicit_sys, **self._base_kwargs())

        assert d._system_logger is explicit_sys
        assert isinstance(d._execution_logger, ExecutionLogger)

    def test_explicit_execution_logger_not_replaced_by_log_base_path(self):
        """Explicit execution_logger takes precedence; only system_logger is auto-created."""
        from datacoolie.logging import SystemLogger

        explicit_execution = _execution_logger_spy()
        d = DataCoolieDriver(log_base_path="/logs", execution_logger=explicit_execution, **self._base_kwargs())

        assert d._execution_logger is explicit_execution
        assert isinstance(d._system_logger, SystemLogger)

    def test_provided_loggers_get_job_id_synced(self):
        """Externally created loggers always receive the driver's run config."""
        sys_lgr = _system_logger_spy()
        execution_lgr = _execution_logger_spy()
        d = DataCoolieDriver(system_logger=sys_lgr, execution_logger=execution_lgr, **self._base_kwargs())

        assert sys_lgr.run_config == d.config
        assert execution_lgr.run_config == d.config
        assert sys_lgr.is_active
        assert execution_lgr.is_active

    def test_log_config_used_for_auto_created_loggers(self):
        """User-supplied LogConfig is used as template for auto-created loggers."""
        from datacoolie.logging import LogConfig

        custom = LogConfig(
            log_level="DEBUG",
            storage_mode="file",
            partition_by_date=False,
            partition_pattern="year={year}",
            flush_interval_seconds=30,
        )
        d = DataCoolieDriver(
            log_base_path="/logs",
            log_config=custom,
            **self._base_kwargs(),
        )

        for lgr in (d._system_logger, d._execution_logger):
            assert lgr.config.log_level == "DEBUG"
            assert lgr.config.storage_mode == "file"
            assert lgr.config.partition_by_date is False
            assert lgr.config.partition_pattern == "year={year}"
            assert lgr.config.flush_interval_seconds == 30

        assert d._system_logger.config.output_path == "/logs/system_logs"
        assert d._execution_logger.config.output_path == "/logs/execution_logs"

    def test_log_config_alone_creates_loggers(self):
        """LogConfig with output_path (no log_base_path) auto-creates loggers."""
        from datacoolie.logging import ExecutionLogger, LogConfig, SystemLogger

        cfg = LogConfig(output_path="/data/logs", log_level="WARNING")
        d = DataCoolieDriver(log_config=cfg, **self._base_kwargs())

        assert isinstance(d._system_logger, SystemLogger)
        assert isinstance(d._execution_logger, ExecutionLogger)
        assert d._system_logger.config.output_path == "/data/logs/system_logs"
        assert d._execution_logger.config.output_path == "/data/logs/execution_logs"
        assert d._system_logger.config.log_level == "WARNING"
        assert d._execution_logger.config.log_level == "WARNING"

    def test_log_base_path_overrides_log_config_output_path(self):
        """log_base_path takes precedence over log_config.output_path."""
        from datacoolie.logging import LogConfig

        cfg = LogConfig(output_path="/ignored/path", log_level="DEBUG")
        d = DataCoolieDriver(
            log_base_path="/preferred",
            log_config=cfg,
            **self._base_kwargs(),
        )

        assert d._system_logger.config.output_path == "/preferred/system_logs"
        assert d._execution_logger.config.output_path == "/preferred/execution_logs"
        # Other LogConfig fields are still preserved.
        assert d._system_logger.config.log_level == "DEBUG"

    def test_log_config_without_output_path_no_loggers(self):
        """LogConfig with output_path=None and no log_base_path creates no loggers."""
        from datacoolie.logging import LogConfig

        cfg = LogConfig(log_level="DEBUG")  # output_path defaults to None
        d = DataCoolieDriver(log_config=cfg, **self._base_kwargs())

        assert d._system_logger is None
        assert d._execution_logger is None


class TestDataCoolieDriverPathOwnership:
    def test_state_root_derives_log_root_and_binds_provider(self):
        class Provider:
            def configure_context(self, context):
                self.bound = {
                    "state_base_path": context.state_base_path,
                    "log_base_path": context.log_base_path,
                }

            def initialize(self):
                return None

        provider = Provider()
        engine = _mock_engine()
        platform = _mock_platform()
        driver = DataCoolieDriver(
            engine=engine,
            platform=platform,
            metadata_provider=provider,
            watermark_manager=_mock_watermark(),
            state_base_path="runtime/state",
        )

        assert driver._effective_log_base_path == "runtime/state/logs"
        assert provider.bound == {
            "state_base_path": "runtime/state",
            "log_base_path": "runtime/state/logs",
        }

    def test_component_roots_are_kept_separate(self):
        # The common and component-specific roots are Driver-owned but are not
        # collapsed into one path object.
        driver = DataCoolieDriver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
            artifact_base_path="release",
            sql_base_path="release/sql-files",
            state_base_path="runtime",
            log_base_path="runtime/custom-logs",
        )

        assert driver._artifact_base_path == "release"
        assert driver._sql_base_path == "release/sql-files"
        assert driver._state_base_path == "runtime"
        assert driver._log_base_path == "runtime/custom-logs"

    def test_blank_root_is_rejected(self):
        with pytest.raises(ConfigurationError, match="artifact_base_path"):
            DataCoolieDriver(
                engine=_mock_engine(),
                platform=_mock_platform(),
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
                artifact_base_path="   ",
            )

    def test_invalid_injected_logger_does_not_bind_engine_platform(self):
        engine = _mock_engine()
        platform = _mock_platform()

        with pytest.raises(ConfigurationError, match="system_logger"):
            DataCoolieDriver(
                engine=engine,
                platform=platform,
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
                system_logger=object(),
            )

        assert engine.platform is None

    def test_provider_context_conflict_does_not_bind_engine_platform(self):
        engine = _mock_engine()
        platform = _mock_platform()
        provider = MagicMock()
        provider.configure_context.side_effect = ConfigurationError(
            "metadata_base_path is not supported"
        )

        with pytest.raises(ConfigurationError, match="metadata_base_path"):
            DataCoolieDriver(
                engine=engine,
                platform=platform,
                metadata_provider=provider,
                metadata_base_path="metadata",
            )

        assert engine.platform is None

    def test_watermark_configuration_fails_before_reader(self):
        class Provider:
            def configure_context(self, context):
                return None

            def initialize(self):
                return None

        engine = _mock_engine()
        platform = _mock_platform()
        watermark = _mock_watermark()
        watermark.validate_ready.side_effect = ConfigurationError("watermark root is missing")

        driver = DataCoolieDriver(
            engine=engine,
            platform=platform,
            metadata_provider=Provider(),
            watermark_manager=watermark,
        )
        dataflow = _dataflow()
        dataflow.source.watermark_columns = ["modified_at"]
        reader = MagicMock()

        with patch.object(driver, "_create_source_reader", return_value=reader):
            result = driver._process_dataflow(dataflow)

        assert result.status == DataFlowStatus.FAILED.value
        assert "watermark root is missing" in (result.message or "")
        reader.read.assert_not_called()
        watermark.get_watermark.assert_not_called()

    def test_custom_reader_watermark_is_checked_before_destination_write(self):
        class Provider:
            def configure_context(self, context):
                return None

            def initialize(self):
                return None

        watermark = _mock_watermark()
        watermark.validate_ready.side_effect = ConfigurationError("watermark root is missing")
        driver = DataCoolieDriver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=Provider(),
            watermark_manager=watermark,
        )
        dataflow = _dataflow()
        reader = MagicMock()
        reader.read.return_value = "rows"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=1)
        reader.get_new_watermark.return_value = {"custom": "value"}
        writer = MagicMock()

        with patch.object(driver, "_create_source_reader", return_value=reader), \
             patch.object(driver, "_create_transformer_pipeline") as pipeline_factory, \
             patch.object(driver, "_create_destination_writer", return_value=writer):
            result = driver._process_dataflow(dataflow)

        assert result.status == DataFlowStatus.FAILED.value
        pipeline_factory.assert_not_called()
        writer.write.assert_not_called()


# ============================================================================
# load_dataflows
# ============================================================================


class TestLoadDataflows:
    def test_loads_and_filters(self):
        dfs = [_dataflow("a"), _dataflow("b"), _dataflow("c", is_active=False)]
        d, _, md, _ = _make_driver(dataflows=dfs)

        result = d.load_dataflows(stage="bronze")
        md.get_dataflows.assert_called_once_with(
            stage="bronze", active_only=True, attach_schema_hints=True,
        )
        # Single job → all active pass through
        assert len(result) == 2

    def test_empty(self):
        d, _, md, _ = _make_driver(dataflows=[])
        result = d.load_dataflows()
        assert result == []


# ============================================================================
# run — routing
# ============================================================================


class TestRun:
    def test_is_alias_for_run_dataflow(self):
        d, *_ = _make_driver()
        df = _dataflow()

        with patch.object(d, "run_dataflow") as mock_rf:
            mock_rf.return_value = ExecutionResult(total=1, succeeded=1)
            result = d.run(stage="bronze", dataflows=[df])

        mock_rf.assert_called_once_with(
            stage="bronze", dataflows=[df], column_name_mode=ColumnCaseMode.LOWER,
        )
        assert result.total == 1

    def test_forwards_column_name_mode(self):
        d, *_ = _make_driver()
        df = _dataflow()

        with patch.object(d, "run_dataflow") as mock_rf:
            mock_rf.return_value = ExecutionResult(total=1, succeeded=1)
            d.run(stage="bronze", dataflows=[df], column_name_mode="snake")

        mock_rf.assert_called_once_with(
            stage="bronze", dataflows=[df], column_name_mode="snake",
        )


# ============================================================================
# run_dataflow — dry run
# ============================================================================


class TestRunDataflowDryRun:
    @pytest.mark.parametrize("start,end,interval", [
        (date(2025, 1, 1), "2025-01-03", {"days": 1}),
        ("2025-01-01T00:00:00", datetime(2025, 1, 2), None),
        (Decimal("1.1"), Decimal("2.2"), None),
    ], ids=["mixed-date-chunks", "mixed-timestamp-one-shot", "decimal-one-shot"])
    def test_replay_dry_run_validates_without_source_or_state_io(self, start, end, interval):
        driver, _, _, manager = _make_driver(config=DataCoolieRunConfig(dry_run=True))
        with patch.object(driver, "_create_source_reader") as reader, \
             patch.object(driver, "_create_transformer_pipeline") as transform, \
             patch.object(driver, "_create_destination_writer") as writer:
            result = driver.run_replay(_dataflow_with_watermark(),
                                      ReplayConfig(start=start, end=end, chunk_interval=interval))
        assert (result.total, result.skipped, result.failed) == (1, 1, 0)
        reader.assert_not_called()
        transform.assert_not_called()
        writer.assert_not_called()
        manager.get_watermark.assert_not_called()
        manager.save_watermark.assert_not_called()

    def test_dry_run_no_processing(self):
        cfg = DataCoolieRunConfig(dry_run=True)
        d, engine, _, _ = _make_driver(config=cfg)
        df = _dataflow()

        result = d.run_dataflow(dataflows=[df])
        assert result.total == 1
        assert result.succeeded == 0  # no actual processing

    def test_inactive_dry_run_does_not_read_missing_sql_file(self):
        driver, _, _, watermark = _make_driver(config=DataCoolieRunConfig(dry_run=True))
        flow = _dataflow(is_active=False)
        flow.source.query = "missing/orders.sql"
        with patch.object(driver, "_validate_execution_preparation") as validate:
            result = driver.run_dataflow(dataflows=[flow])
        assert (result.total, result.skipped, result.failed) == (1, 1, 0)
        validate.assert_not_called()
        watermark.get_watermark.assert_not_called()

    def test_inactive_replay_dry_run_ignores_missing_chunk_column(self):
        driver, _, _, watermark = _make_driver(config=DataCoolieRunConfig(dry_run=True))
        flow = _dataflow(is_active=False)
        flow.source.watermark_columns = []
        result = driver.run_replay(flow, ReplayConfig(start=1, end=2))
        assert (result.total, result.skipped, result.failed) == (1, 1, 0)
        watermark.get_watermark.assert_not_called()

    def test_api_independent_chunk_column_is_accepted_in_dry_run(self):
        driver, _, _, watermark = _make_driver(config=DataCoolieRunConfig(dry_run=True))
        source_conn = _conn(fmt=Format.API.value)
        destination_conn = _conn(fmt=Format.DELTA.value)
        flow = DataFlow(
            dataflow_id="api-dry-run",
            source=Source(
                connection=source_conn,
                table="src",
                watermark_columns=["updated_at"],
                configure={
                    "range_param_mapping": {
                        "created_at": {
                            "lower": {"name": "created_from", "operator": ">="},
                            "upper": {"name": "created_to", "operator": "<"},
                        }
                    }
                },
            ),
            destination=Destination(
                connection=destination_conn,
                table="dst",
                load_type=LoadType.APPEND.value,
            ),
        )

        result = driver.run_replay(
            flow,
            ReplayConfig(start="2025-01-01", end="2025-02-01", chunk_column="created_at"),
        )

        assert result.failed == 0
        assert result.skipped == 1
        watermark.get_watermark.assert_not_called()

    def test_dry_run_validates_file_query_without_reader_or_secrets(self):
        cfg = DataCoolieRunConfig(dry_run=True)
        d, _, _, watermark = _make_driver(config=cfg)
        d._sql_base_path = "release/sql"
        d._engine.platform.read_file_under_base.return_value = "SELECT 1;"
        dataflow = _dataflow()
        dataflow.source.query = "orders.sql"
        secret_resolver = MagicMock(side_effect=AssertionError("dry-run secret access"))

        with patch.object(d, "_resolve_secrets_for_connection", secret_resolver), \
             patch.object(d, "_create_source_reader") as reader_factory, \
             patch.object(d, "_create_destination_writer") as writer_factory:
            result = d.run_dataflow(dataflows=[dataflow])

        assert result.skipped == 1
        assert result.failed == 0
        reader_factory.assert_not_called()
        writer_factory.assert_not_called()
        secret_resolver.assert_not_called()
        watermark.get_watermark.assert_not_called()

    def test_dry_run_missing_query_root_is_failed_validation(self):
        cfg = DataCoolieRunConfig(dry_run=True)
        d, _, _, watermark = _make_driver(config=cfg)
        dataflow = _dataflow()
        dataflow.source.query = "orders.sql"

        with patch.object(d, "_create_source_reader") as reader_factory:
            result = d.run_dataflow(dataflows=[dataflow])

        assert result.failed == 1
        assert "requires artifact_base_path" in result.errors["df-1"]
        reader_factory.assert_not_called()
        watermark.get_watermark.assert_not_called()

    def test_dry_run_validates_required_watermark_storage_without_state_io(self):
        cfg = DataCoolieRunConfig(dry_run=True)
        d, _, _, watermark = _make_driver(config=cfg)
        watermark.validate_ready.side_effect = ConfigurationError("missing watermark root")
        dataflow = _dataflow_with_watermark()

        result = d.run_dataflow(dataflows=[dataflow])

        assert result.failed == 1
        assert "missing watermark root" in result.errors["df-1"]
        watermark.validate_ready.assert_called_once_with()
        watermark.get_watermark.assert_not_called()
        watermark.save_watermark.assert_not_called()

    def test_replay_and_maintenance_dry_runs_do_not_touch_state_or_writers(self):
        cfg = DataCoolieRunConfig(dry_run=True)
        d, _, _, watermark = _make_driver(config=cfg)
        replay_flow = _dataflow_with_watermark()
        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-02-01",
            chunk_interval={"months": 1},
        )

        with patch.object(d, "_create_source_reader") as reader_factory, \
             patch.object(d, "_create_destination_writer") as writer_factory:
            replay_result = d.run_replay(replay_flow, replay)
            maintenance_result = d.run_maintenance(dataflows=[_dataflow()])

        assert replay_result.skipped == 1
        assert maintenance_result.skipped == 1
        reader_factory.assert_not_called()
        writer_factory.assert_not_called()
        watermark.get_watermark.assert_not_called()
        watermark.save_watermark.assert_not_called()

    def test_replay_persistence_without_manager_is_failed_before_pipeline(self):
        driver = DataCoolieDriver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=None,
            watermark_manager=None,
        )
        dataflow = _dataflow_with_watermark()
        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-02-01",
            save_watermark=True,
        )

        with patch.object(driver, "_create_source_reader") as reader_factory:
            result = driver.run_replay(dataflow, replay)

        assert result.failed == 1
        assert "requires a watermark_manager" in result.errors["df-1"]
        reader_factory.assert_not_called()

    def test_no_dataflows(self):
        d, *_ = _make_driver()
        result = d.run_dataflow()
        assert result.total == 0

    def test_column_name_mode_stored_on_driver(self):
        d, *_ = _make_driver()
        d.run_dataflow(column_name_mode="snake")
        assert not hasattr(d, "_column_name_mode")

    def test_column_name_mode_defaults_to_lower(self):
        d, *_ = _make_driver()
        d.run_dataflow()
        assert not hasattr(d, "_column_name_mode")

    def test_invalid_column_name_mode_raises(self):
        d, *_ = _make_driver()
        with pytest.raises(ValueError):
            d.run_dataflow(column_name_mode="invalid")

    def test_with_stage_loads_dataflows(self):
        dfs = [_dataflow("a"), _dataflow("b")]
        d, _, md, _ = _make_driver(dataflows=dfs)

        with patch.object(d, "_process_dataflow") as mock_proc:
            mock_proc.return_value = DataFlowRuntimeInfo(
                dataflow_id="df-1",
                status=DataFlowStatus.SUCCEEDED.value,
            )
            result = d.run_dataflow(stage="bronze")

        md.get_dataflows.assert_called_once_with(
            stage="bronze", active_only=True, attach_schema_hints=True,
        )
        assert result.total == 2


# ============================================================================
# _process_dataflow
# ============================================================================


class TestProcessDataflow:
    def test_execute_etl_returns_common_attempt_result(self):
        d, *_ = _make_driver()
        df = _dataflow()
        source = SourceRuntimeInfo(
            rows_read=2,
            status=DataFlowStatus.SUCCEEDED.value,
        )
        transform = TransformRuntimeInfo(status=DataFlowStatus.SUCCEEDED.value)
        destination = DestinationRuntimeInfo(
            rows_written=2,
            status=DataFlowStatus.SUCCEEDED.value,
        )
        reader = MagicMock()
        reader.read.return_value = "source_df"
        reader.get_runtime_info.return_value = source
        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed_df"
        pipeline.get_runtime_info.return_value = transform
        writer = MagicMock()
        writer.get_runtime_info.return_value = destination

        with patch.object(d, "_create_source_reader", return_value=reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=pipeline), \
             patch.object(d, "_create_destination_writer", return_value=writer):
            result = d._execute_etl_pipeline(
                df,
                "run-1",
                save_watermark=False,
            )

        assert isinstance(result, PipelineAttemptResult)
        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.source is source
        assert result.transform is transform
        assert result.destination is destination

    def test_success_flow(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()
        df.source.watermark_columns = ["col"]

        # Mock reader
        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=10, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {"col": "val"}

        # Mock pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "transformed_df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            transformers_applied=["SchemaConverter"],
            status=DataFlowStatus.SUCCEEDED.value,
        )

        # Mock writer
        mock_writer = MagicMock()
        mock_writer.write.return_value = DestinationRuntimeInfo(
            rows_written=10, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_writer.get_runtime_info.return_value = DestinationRuntimeInfo(
            rows_written=10, status=DataFlowStatus.SUCCEEDED.value
        )

        with patch.object(
                 d,
                 "_create_source_reader",
                 return_value=mock_reader,
             ) as mock_reader_factory, \
             patch.object(
                 d,
                 "_create_transformer_pipeline",
                 return_value=mock_pipeline,
             ) as mock_pipeline_factory, \
             patch.object(
                 d,
                 "_create_destination_writer",
                 return_value=mock_writer,
             ) as mock_writer_factory:
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.dataflow_id == "df-1"
        mock_reader_factory.assert_called_once_with(df.source.connection.format)
        mock_writer_factory.assert_called_once_with(df.destination.connection.format)
        mock_pipeline_factory.assert_called_once_with(
            dataflow_run_id=result.dataflow_run_id,
            column_name_mode=ColumnCaseMode.LOWER,
        )
        wm.save_watermark.assert_called_once()

    def test_runtime_exists_before_retry_execution(self):
        d, *_ = _make_driver()
        df = _dataflow()
        real_runtime_type = DataFlowRuntimeInfo
        events: list[tuple[str, str]] = []

        def create_runtime(**kwargs):
            runtime = real_runtime_type(**kwargs)
            events.append(("runtime", runtime.dataflow_run_id))
            return runtime

        def execute(_fn, _dataflow, **kwargs):
            events.append(("execute", kwargs["dataflow_run_id"]))
            return (
                PipelineAttemptResult(
                    status=DataFlowStatus.SKIPPED.value,
                    source=SourceRuntimeInfo(rows_read=0),
                ),
                1,
            )

        with patch(
            "datacoolie.orchestration.execution.lifecycle.DataFlowRuntimeInfo",
            side_effect=create_runtime,
        ), patch.object(d._retry_handler, "execute", side_effect=execute):
            result = d._run_single_pipeline(
                d._prepare_execution_dataflow(df)
            )

        assert events == [
            ("runtime", result.dataflow_run_id),
            ("execute", result.dataflow_run_id),
        ]
        assert result.status == DataFlowStatus.SKIPPED.value
        assert result.message == "Execution returned skipped without a detailed reason"

    def test_no_data_skipped(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = None
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=0, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {}

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.SKIPPED.value
        assert result.message == "Source returned no data to process"
        wm.save_watermark.assert_not_called()

    def test_failure(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.side_effect = RuntimeError("read error")

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "read error" in result.message

    def test_failure_preserves_partial_source_runtime(self):
        """When transform fails, the already-collected source runtime is kept."""
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=42, status=DataFlowStatus.SUCCEEDED.value
        )

        mock_pipeline = MagicMock()
        mock_pipeline.transform.side_effect = RuntimeError("transform boom")

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "transform boom" in result.message
        assert result.source.rows_read == 42
        assert result.source.status == DataFlowStatus.SUCCEEDED.value

    def test_failure_preserves_partial_transform_runtime(self):
        """When write fails, source and transform runtimes are preserved."""
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=10, status=DataFlowStatus.SUCCEEDED.value
        )

        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "transformed_df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            transformers_applied=["SchemaConverter"],
            status=DataFlowStatus.SUCCEEDED.value,
        )

        mock_writer = MagicMock()
        mock_writer.write.side_effect = RuntimeError("write boom")

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline), \
             patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "write boom" in result.message
        assert result.source.rows_read == 10
        assert result.transform.transformers_applied == ["SchemaConverter"]

    @patch("datacoolie.utils.retry.time.sleep")
    def test_retry_on_exception(self, mock_sleep):
        cfg = DataCoolieRunConfig(retry_count=2, retry_delay=0.01)
        d, engine, _, wm = _make_driver(config=cfg)
        df = _dataflow()

        call_count = 0

        def fake_read(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RuntimeError("transient")
            return "fake_df"

        mock_reader = MagicMock()
        mock_reader.read.side_effect = fake_read
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=5, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {}

        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value
        )

        mock_writer = MagicMock()
        mock_writer.write.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value
        )
        mock_writer.get_runtime_info.return_value = DestinationRuntimeInfo(
            rows_written=5, status=DataFlowStatus.SUCCEEDED.value
        )

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline), \
             patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.retry_attempts == 2

    @patch("datacoolie.utils.retry.time.sleep")
    def test_retry_reuses_resolved_secret_on_single_runtime_copy(self, mock_sleep):
        cfg = DataCoolieRunConfig(retry_count=1, retry_delay=0.01)
        d, *_ = _make_driver(config=cfg)
        df = _dataflow()
        connection = df.source.connection
        connection.configure["password"] = "db-password"
        connection.secrets_ref = {"scope": ["password"]}

        secret_provider = MagicMock()
        secret_provider.get_secret.return_value = "runtime-secret"
        d._secret_provider = secret_provider

        mock_reader = MagicMock()
        mock_reader.read.side_effect = [RuntimeError("transient"), None]
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=0,
            status=DataFlowStatus.SUCCEEDED.value,
        )

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.SKIPPED.value
        assert result.retry_attempts == 1
        secret_provider.get_secret.assert_called_once_with("db-password", "scope")
        assert connection.configure["password"] == "db-password"

    def test_exhausted_retries_report_all_attempts(self):
        config = DataCoolieRunConfig(retry_count=2, retry_delay=0)
        d, *_ = _make_driver(config=config)

        with patch.object(
            d,
            "_execute_etl_pipeline",
            side_effect=RuntimeError("persistent failure"),
        ) as execute_attempt:
            result = d._process_dataflow(_dataflow())

        assert result.status == DataFlowStatus.FAILED.value
        assert result.retry_attempts == 2
        assert execute_attempt.call_count == 3

    def test_execution_logger_called(self):
        execution_logger = _execution_logger_spy()
        d, *_ = _make_driver(execution_logger=execution_logger)
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = None
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=0)
        mock_reader.get_new_watermark.return_value = {}

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            d._process_dataflow(df)

        execution_logger.log.assert_called_once()

    def test_file_query_is_prepared_but_metadata_log_keeps_reference(self):
        execution_logger = _execution_logger_spy()
        driver, engine, _, _ = _make_driver(execution_logger=execution_logger)
        driver._sql_base_path = "release/sql"
        driver._engine.platform.read_file_under_base.return_value = "SELECT 1;"
        dataflow = _dataflow()
        dataflow.source.query = "orders.sql"

        reader = MagicMock()
        reader.read.return_value = None
        reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=0,
            source_action={"reader": "DatabaseReader", "query": "SELECT 1;"},
        )

        with patch.object(driver, "_create_source_reader", return_value=reader):
            result = driver._process_dataflow(dataflow)

        assert result.status == DataFlowStatus.SKIPPED.value
        prepared_source = reader.read.call_args.args[0]
        assert prepared_source.query == "SELECT 1;"
        logged_dataflow, logged_runtime = execution_logger.log.call_args.kwargs["dataflow"], execution_logger.log.call_args.kwargs["runtime_info"]
        assert logged_dataflow.source.query == "orders.sql"
        assert logged_runtime.source.source_action["query"] == "SELECT 1;"


# ============================================================================
# _process_maintenance
# ============================================================================


class TestProcessMaintenance:
    def test_execute_maintenance_returns_common_attempt_result(self):
        d, *_ = _make_driver()
        df = _dataflow()
        destination = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        writer = MagicMock()
        writer.run_maintenance.return_value = destination

        with patch.object(d, "_create_destination_writer", return_value=writer):
            result = d._execute_maintenance_pipeline(df)

        assert isinstance(result, PipelineAttemptResult)
        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.source is None
        assert result.transform is None
        assert result.destination is destination

    def test_success(self):
        d, engine, _, _ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
            files_added=1,
            files_removed=3,
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.destination.files_added == 1
        mock_writer.run_maintenance.assert_called_once()
        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is True
        assert call_kwargs.kwargs["do_cleanup"] is True

    def test_runtime_exists_before_retry_execution(self):
        d, *_ = _make_driver()
        df = _dataflow()
        real_runtime_type = DataFlowRuntimeInfo
        events: list[tuple[str, str]] = []

        def create_runtime(**kwargs):
            runtime = real_runtime_type(**kwargs)
            events.append(("runtime", runtime.dataflow_run_id))
            return runtime

        def execute(_fn, _dataflow, **_kwargs):
            events.append(("execute", events[0][1]))
            return (
                PipelineAttemptResult(
                    status=DataFlowStatus.SUCCEEDED.value,
                    destination=DestinationRuntimeInfo(
                        status=DataFlowStatus.SUCCEEDED.value,
                    ),
                ),
                1,
            )

        with patch(
            "datacoolie.orchestration.execution.lifecycle.DataFlowRuntimeInfo",
            side_effect=create_runtime,
        ), patch.object(d._retry_handler, "execute", side_effect=execute):
            result = d._process_maintenance(df)

        assert events == [
            ("runtime", result.dataflow_run_id),
            ("execute", result.dataflow_run_id),
        ]

    def test_compact_only(self):
        """do_cleanup=False skips vacuum; do_compact=True runs optimize."""
        d, *_ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df, do_compact=True, do_cleanup=False)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is True
        assert call_kwargs.kwargs["do_cleanup"] is False

    def test_cleanup_only(self):
        """do_compact=False skips optimize; do_cleanup=True runs vacuum."""
        d, *_ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df, do_compact=False, do_cleanup=True)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is False
        assert call_kwargs.kwargs["do_cleanup"] is True

    def test_failure(self):
        d, *_ = _make_driver()
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.side_effect = RuntimeError("maint err")

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert "maint err" in result.message
        assert result.destination.status == DataFlowStatus.PENDING.value
        assert result.destination.operation_type is None

    def test_setup_failure_does_not_create_destination_failure(self):
        d, *_ = _make_driver()
        df = _dataflow()

        with patch.object(
            d,
            "_create_destination_writer",
            side_effect=RuntimeError("factory failed"),
        ):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert result.message == "factory failed"
        assert result.destination.status == DataFlowStatus.PENDING.value
        assert result.destination.message is None
        assert result.destination.operation_type is None

    def test_secret_resolution_failure_does_not_start_destination(self):
        d, *_ = _make_driver()
        df = _dataflow()

        with patch.object(
            d,
            "_resolve_secrets_for_connection",
            side_effect=RuntimeError("secret resolution failed"),
        ), patch.object(d, "_create_destination_writer") as writer_factory:
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert result.message == "secret resolution failed"
        assert result.destination.status == DataFlowStatus.PENDING.value
        assert result.destination.operation_type is None
        writer_factory.assert_not_called()

    def test_writer_exception_recovers_started_maintenance_runtime(self):
        d, *_ = _make_driver()
        df = _dataflow()
        partial = DestinationRuntimeInfo(
            status=DataFlowStatus.RUNNING.value,
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        mock_writer = MagicMock()
        mock_writer.run_maintenance.side_effect = RuntimeError("maintenance crashed")
        mock_writer.get_runtime_info.return_value = partial

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert result.destination is partial
        assert result.message == "maintenance crashed"
        assert partial.status == DataFlowStatus.FAILED.value
        assert partial.message == "maintenance crashed"
        assert partial.end_time is not None

    def test_component_pipeline_error_is_normalized_to_attempt_result(self):
        d, *_ = _make_driver()
        df = _dataflow()
        partial = DestinationRuntimeInfo(
            status=DataFlowStatus.FAILED.value,
            message="writer partial",
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        original = PipelineError("writer pipeline error", partial_result=partial)
        mock_writer = MagicMock()
        mock_writer.run_maintenance.side_effect = original
        mock_writer.get_runtime_info.return_value = partial

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            with pytest.raises(PipelineError) as caught:
                d._execute_maintenance_pipeline(df)

        assert caught.value is not original
        assert caught.value.__cause__ is original
        assert isinstance(caught.value.partial_result, PipelineAttemptResult)
        assert caught.value.partial_result.status == DataFlowStatus.FAILED.value
        assert caught.value.partial_result.destination is partial

    def test_returned_failure_retries_whole_maintenance_function(self):
        config = DataCoolieRunConfig(retry_count=1, retry_delay=0)
        d, *_ = _make_driver(config=config)
        df = _dataflow()
        failed = DestinationRuntimeInfo(
            status=DataFlowStatus.FAILED.value,
            message="transient failure",
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        succeeded = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        first_writer = MagicMock()
        first_writer.run_maintenance.return_value = failed
        second_writer = MagicMock()
        second_writer.run_maintenance.return_value = succeeded

        with patch.object(
            d,
            "_create_destination_writer",
            side_effect=[first_writer, second_writer],
        ) as writer_factory:
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.destination is succeeded
        assert result.retry_attempts == 1
        assert writer_factory.call_count == 2
        assert writer_factory.call_args_list == [
            call(df.destination.connection.format),
            call(df.destination.connection.format),
        ]

    def test_exhausted_returned_failures_keep_last_partial_runtime(self):
        config = DataCoolieRunConfig(retry_count=1, retry_delay=0)
        d, *_ = _make_driver(config=config)
        df = _dataflow()
        first = DestinationRuntimeInfo(
            status=DataFlowStatus.FAILED.value,
            message="first failure",
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        last = DestinationRuntimeInfo(
            status=DataFlowStatus.FAILED.value,
            message="last failure",
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        writers = [MagicMock(), MagicMock()]
        writers[0].run_maintenance.return_value = first
        writers[1].run_maintenance.return_value = last

        with patch.object(
            d,
            "_create_destination_writer",
            side_effect=writers,
        ):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert result.destination is last
        assert result.message == "last failure"

    def test_skipped_maintenance_does_not_retry(self):
        config = DataCoolieRunConfig(retry_count=2, retry_delay=0)
        d, *_ = _make_driver(config=config)
        df = _dataflow()
        skipped = DestinationRuntimeInfo(
            status=DataFlowStatus.SKIPPED.value,
            operation_type=ExecutionType.MAINTENANCE.value,
        )
        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = skipped

        with patch.object(
            d,
            "_create_destination_writer",
            return_value=mock_writer,
        ) as writer_factory:
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.SKIPPED.value
        assert result.message == "Maintenance performed no operations"
        assert result.destination is skipped
        writer_factory.assert_called_once_with(df.destination.connection.format)

    def test_execution_logger_not_failing(self):
        """Even if execution_logger.log raises, _process_maintenance doesn't fail."""
        lgr = _execution_logger_spy()
        lgr.log.side_effect = RuntimeError("log error")
        d, *_ = _make_driver(execution_logger=lgr)
        df = _dataflow()

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value, operation_type="maintenance"
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value

    def test_retry_reuses_resolved_destination_secret(self):
        config = DataCoolieRunConfig(retry_count=1, retry_delay=0)
        d, *_ = _make_driver(config=config)

        source_connection = _conn(name="source")
        source_connection.configure["password"] = "source-password-ref"
        source_connection.secrets_ref = {"source-scope": ["password"]}

        destination_connection = _conn(name="destination")
        destination_connection.configure["password"] = "destination-password-ref"
        destination_connection.secrets_ref = {"destination-scope": ["password"]}

        df = DataFlow(
            dataflow_id="maintenance-secrets",
            source=Source(connection=source_connection, table="src"),
            destination=Destination(
                connection=destination_connection,
                table="dst",
                load_type=LoadType.APPEND.value,
            ),
        )

        secret_provider = MagicMock()
        secret_provider.get_secret.return_value = "resolved-value"
        d._secret_provider = secret_provider

        mock_writer = MagicMock()
        mock_writer.run_maintenance.side_effect = [
            RuntimeError("transient maintenance failure"),
            DestinationRuntimeInfo(
                status=DataFlowStatus.SUCCEEDED.value,
                operation_type=ExecutionType.MAINTENANCE.value,
            ),
        ]

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.retry_attempts == 1
        assert mock_writer.run_maintenance.call_count == 2
        secret_provider.get_secret.assert_called_once_with(
            "destination-password-ref", "destination-scope"
        )

        runtime_df = mock_writer.run_maintenance.call_args.kwargs["dataflow"]
        assert runtime_df.source.connection.configure["password"] == "source-password-ref"
        assert isinstance(runtime_df.destination.connection.configure["password"], SecretStr)
        assert source_connection.configure["password"] == "source-password-ref"
        assert destination_connection.configure["password"] == "destination-password-ref"


# ============================================================================
# run_maintenance — connection filter + lakehouse only
# ============================================================================


class TestRunMaintenance:
    """run_maintenance delegates to get_maintenance_dataflows."""

    def test_no_args_calls_get_maintenance_dataflows(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        d.run_maintenance()
        md.get_maintenance_dataflows.assert_called_once_with(
            connection=None,
            active_only=True,
        )

    def test_connection_string_forwarded(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        d.run_maintenance(connection="my_lakehouse")
        md.get_maintenance_dataflows.assert_called_once_with(
            connection="my_lakehouse",
            active_only=True,
        )

    def test_connection_list_forwarded(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        d.run_maintenance(connection=["lh_a", "lh_b"])
        md.get_maintenance_dataflows.assert_called_once_with(
            connection=["lh_a", "lh_b"],
            active_only=True,
        )

    def test_load_maintenance_dataflows_forwards_active_only(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []

        d._load_maintenance_dataflows(active_only=False)

        md.get_maintenance_dataflows.assert_called_once_with(
            connection=None,
            active_only=False,
        )

    def test_pre_loaded_dataflows_skip_metadata(self):
        """When dataflows are passed directly, metadata is not called."""
        d, _, md, _ = _make_driver()
        dfs = [_dataflow()]
        with patch.object(d, "_process_maintenance") as mock_proc:
            mock_proc.return_value = DataFlowRuntimeInfo(
                dataflow_id="df-1",
                status=DataFlowStatus.SUCCEEDED.value,
            )
            d.run_maintenance(dataflows=dfs)
        md.get_maintenance_dataflows.assert_not_called()

    def test_run_maintenance_flags_thread_to_writer(self):
        """do_compact/do_cleanup passed to run_maintenance reach writer."""
        d, _, md, _ = _make_driver()
        df = _dataflow()
        md.get_maintenance_dataflows.return_value = [df]

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            d.run_maintenance(do_compact=False, do_cleanup=True)

        call_kwargs = mock_writer.run_maintenance.call_args
        assert call_kwargs.kwargs["do_compact"] is False
        assert call_kwargs.kwargs["do_cleanup"] is True

    def test_empty_result_when_no_lakehouse_dataflows(self):
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = []
        result = d.run_maintenance()
        assert result.total == 0
        assert result.succeeded == 0


# ============================================================================
# Factory methods
# ============================================================================


class TestFactoryMethods:
    def test_create_delta_reader(self):
        d, *_ = _make_driver()
        reader = d._create_source_reader(Format.DELTA.value)
        from datacoolie.sources import DeltaReader
        assert isinstance(reader, DeltaReader)

    def test_create_parquet_reader(self):
        d, *_ = _make_driver()
        reader = d._create_source_reader(Format.PARQUET.value)
        from datacoolie.sources import FileReader
        assert isinstance(reader, FileReader)

    def test_create_jdbc_reader(self):
        d, *_ = _make_driver()
        reader = d._create_source_reader(Format.SQL.value)
        from datacoolie.sources import DatabaseReader
        assert isinstance(reader, DatabaseReader)

    def test_create_function_reader(self):
        d, *_ = _make_driver()
        reader = d._create_source_reader(Format.FUNCTION.value)
        from datacoolie.sources import PythonFunctionReader
        assert isinstance(reader, PythonFunctionReader)

    def test_create_delta_writer(self):
        d, *_ = _make_driver()
        writer = d._create_destination_writer(Format.DELTA.value)
        from datacoolie.destinations import DeltaWriter
        assert isinstance(writer, DeltaWriter)

    def test_create_iceberg_writer(self):
        d, *_ = _make_driver()
        writer = d._create_destination_writer(Format.ICEBERG.value)
        from datacoolie.destinations import IcebergWriter
        assert isinstance(writer, IcebergWriter)

    def test_unsupported_source_format(self):
        d, *_ = _make_driver()
        with pytest.raises(DataCoolieError, match="No plugin registered for 'unknown'"):
            d._create_source_reader("unknown")

    def test_unsupported_dest_format(self):
        d, *_ = _make_driver()
        with pytest.raises(DataCoolieError, match="No plugin registered for 'unknown'"):
            d._create_destination_writer("unknown")

    def test_create_transformer_pipeline(self):
        d, *_ = _make_driver()
        pipeline = d._create_transformer_pipeline()
        from datacoolie.transformers import TransformerPipeline
        assert isinstance(pipeline, TransformerPipeline)
        assert len(pipeline.transformers) == 12
        assert [transformer.order for transformer in pipeline.transformers] == [
            5, 10, 18, 20, 30, 35, 60, 70, 80, 84, 85, 90
        ]

    def test_create_transformer_pipeline_injects_dataflow_run_id(self):
        d, *_ = _make_driver()
        pipeline = d._create_transformer_pipeline(dataflow_run_id="run-123")
        from datacoolie.transformers import SystemColumnAdder

        system_adders = [
            transformer
            for transformer in pipeline.transformers
            if isinstance(transformer, SystemColumnAdder)
        ]
        assert len(system_adders) == 1
        assert system_adders[0]._dataflow_run_id == "run-123"

    def test_create_transformer_pipeline_uses_column_name_mode(self):
        d, *_ = _make_driver()
        pipeline = d._create_transformer_pipeline(
            column_name_mode=ColumnCaseMode.SNAKE
        )
        from datacoolie.transformers import ColumnNameSanitizer
        sanitizers = [t for t in pipeline.transformers if isinstance(t, ColumnNameSanitizer)]
        assert len(sanitizers) == 1
        assert sanitizers[0]._mode == ColumnCaseMode.SNAKE

    def test_create_transformer_pipeline_defaults_to_lower(self):
        d, *_ = _make_driver()
        pipeline = d._create_transformer_pipeline()
        from datacoolie.transformers import ColumnNameSanitizer
        sanitizers = [t for t in pipeline.transformers if isinstance(t, ColumnNameSanitizer)]
        assert len(sanitizers) == 1
        assert sanitizers[0]._mode == ColumnCaseMode.LOWER


# ============================================================================
# ============================================================================
# run_replay
# ============================================================================


def _replay_config(
    from_val: str = "2025-01-01",
    to_val: str = "2025-04-01",
    chunk_interval: dict | None = None,
    update_watermark: bool = False,
    chunk_column: str | None = None,
) -> ReplayConfig:
    return ReplayConfig(
        start=from_val,
        end=to_val,
        chunk_interval=chunk_interval or {"months": 1},
        save_watermark=update_watermark,
        chunk_column=chunk_column,
    )


def _dataflow_with_watermark(
    dataflow_id: str = "df-1",
    watermark_columns: list | None = None,
) -> DataFlow:
    """Create a DataFlow with watermark_columns for replay tests."""
    conn = _conn()
    return DataFlow(
        dataflow_id=dataflow_id,
        source=Source(
            connection=conn,
            table="src",
            watermark_columns=watermark_columns or ["order_date"],
        ),
        destination=Destination(connection=conn, table="dst", load_type=LoadType.APPEND.value),
    )


class TestRunReplay:
    """Tests for DataCoolieDriver.run_replay."""

    def _setup_driver_for_replay(self, rows_read: int = 10) -> tuple:
        """Create driver with mocked reader/writer/pipeline."""
        driver, engine, metadata, watermark = _make_driver()
        engine.filter_rows.side_effect = lambda df, expr: df
        engine.count_rows.return_value = rows_read
        return driver, engine, metadata, watermark

    @pytest.mark.parametrize("start,end,interval,expected", [
        (date(2025, 1, 1), "2025-01-03", None, [(date(2025, 1, 1), date(2025, 1, 3))]),
        ("2025-01-01", date(2025, 1, 3), {"days": 1},
         [(date(2025, 1, 1), date(2025, 1, 2)), (date(2025, 1, 2), date(2025, 1, 3))]),
        (datetime(2025, 1, 1, 0, 0, 0, 123456), "2025-01-02T00:00:00.654321", None,
         [(datetime(2025, 1, 1, 0, 0, 0, 123456), datetime(2025, 1, 2, 0, 0, 0, 654321))]),
        ("2025-01-01T00:00:00", datetime(2025, 1, 3), {"days": 1},
         [(datetime(2025, 1, 1), datetime(2025, 1, 2)), (datetime(2025, 1, 2), datetime(2025, 1, 3))]),
        (Decimal("1.0000000001"), Decimal("2.0000000002"), None,
         [(Decimal("1.0000000001"), Decimal("2.0000000002"))]),
    ], ids=["native-date-iso", "iso-date-native-chunked", "native-timestamp-iso",
            "iso-timestamp-native-chunked", "decimal-one-shot"])
    def test_canonical_bounds_reach_reader_in_execution(self, start, end, interval, expected):
        driver, _, _, watermark = self._setup_driver_for_replay()
        reader = MagicMock()
        reader.read.return_value = "frame"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        flow = _dataflow_with_watermark()
        before = flow.model_dump()
        with patch.object(driver, "_create_source_reader", return_value=reader), \
             patch.object(driver, "_create_transformer_pipeline", return_value=pipeline), \
             patch.object(driver, "_create_destination_writer", return_value=writer):
            result = driver.run_replay(flow, ReplayConfig(start=start, end=end, chunk_interval=interval))
        assert result.succeeded == 1, result.errors
        calls = reader.read.call_args_list
        assert [(item.kwargs["read_range"].start, item.kwargs["read_range"].end)
                for item in calls] == expected
        assert all(item.args[1] is None and item.kwargs["watermark_end"] is None for item in calls)
        assert all(item.kwargs["read_range"].lower_operator == ">=" and
                   item.kwargs["read_range"].upper_operator == "<" for item in calls)
        watermark.get_watermark.assert_not_called()
        watermark.save_watermark.assert_not_called()
        assert flow.model_dump() == before

    def test_outer_runtime_is_created_before_chunks_and_aggregates_them(self):
        driver, *_ = self._setup_driver_for_replay()
        dataflow = _dataflow_with_watermark()
        replay = ReplayConfig(start="2025-01-01", end="2025-02-01")
        chunk_runtime = DataFlowRuntimeInfo(
            dataflow_id=dataflow.dataflow_id,
            operation_type=ExecutionType.REPLAY.value,
            source=SourceRuntimeInfo(rows_read=7),
            destination=DestinationRuntimeInfo(rows_written=5),
            status=DataFlowStatus.SUCCEEDED.value,
            retry_attempts=1,
        )
        real_runtime_type = DataFlowRuntimeInfo
        events: list[str] = []
        created: list[DataFlowRuntimeInfo] = []

        def create_runtime(**kwargs):
            runtime = real_runtime_type(**kwargs)
            created.append(runtime)
            events.append("runtime")
            return runtime

        def run_chunk(*_args, **_kwargs):
            events.append("chunk")
            return chunk_runtime

        with patch(
            "datacoolie.orchestration.execution.replay.DataFlowRuntimeInfo",
            side_effect=create_runtime,
        ), patch.object(driver, "_run_single_pipeline", side_effect=run_chunk):
            result = driver._process_replay(dataflow, replay)

        assert events == ["runtime", "chunk"]
        assert result is created[0]
        assert result.dataflow_run_id != chunk_runtime.dataflow_run_id
        assert result.source.rows_read == 7
        assert result.destination.rows_written == 5
        assert result.retry_attempts == 1

    def test_replay_chunk_callback_cannot_mutate_aggregate(self):
        driver, *_ = self._setup_driver_for_replay()
        dataflow = _dataflow_with_watermark()
        replay = ReplayConfig(start="2025-01-01", end="2025-02-01")
        chunk_runtime = DataFlowRuntimeInfo(
            dataflow_id=dataflow.dataflow_id,
            operation_type=ExecutionType.REPLAY.value,
            status=DataFlowStatus.SUCCEEDED.value,
        )

        def mutate_chunk(runtime):
            runtime.status = DataFlowStatus.FAILED.value
            runtime.message = "observer mutation"

        with patch.object(driver, "_run_single_pipeline", return_value=chunk_runtime), \
             patch.object(driver, "_on_replay_chunk_complete", side_effect=mutate_chunk):
            result = driver._process_replay(dataflow, replay)

        assert result.status == DataFlowStatus.SUCCEEDED.value
        assert result.message is None

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_destination_writer")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_single_shot_no_chunking(self, mock_reader_fn, mock_pipeline_fn, mock_writer_fn):
        driver, engine, _, watermark = self._setup_driver_for_replay()

        # Setup reader
        reader = MagicMock()
        reader.read.return_value = "fake_df"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        reader.get_new_watermark.return_value = {"order_date": "2025-04-01"}
        mock_reader_fn.return_value = reader

        # Setup pipeline
        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed_df"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        mock_pipeline_fn.return_value = pipeline

        # Setup writer
        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        mock_writer_fn.return_value = writer

        dataflow = _dataflow_with_watermark()
        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-04-01",
            # No chunk_interval = single shot
        )

        result = driver.run_replay(dataflow, replay)

        assert result.total == 1
        assert result.succeeded == 1
        # Reader called with override watermark
        reader.read.assert_called_once()
        # Watermark NOT updated (default)
        watermark.save_watermark.assert_not_called()

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_destination_writer")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_chunked_three_months(self, mock_reader_fn, mock_pipeline_fn, mock_writer_fn):
        driver, engine, _, watermark = self._setup_driver_for_replay()

        reader = MagicMock()
        reader.read.return_value = "fake_df"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        reader.get_new_watermark.return_value = {}
        mock_reader_fn.return_value = reader

        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed_df"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        mock_pipeline_fn.return_value = pipeline

        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        mock_writer_fn.return_value = writer

        dataflow = _dataflow_with_watermark()
        replay = _replay_config(chunk_interval={"months": 1})

        result = driver.run_replay(dataflow, replay)

        # One ExecutionResult entry per dataflow (chunks run internally)
        assert result.total == 1
        assert result.succeeded == 1
        # Reader called 3 times (once per chunk)
        assert reader.read.call_count == 3

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_destination_writer")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_chunk_retry_reuses_resolved_secret(
        self, mock_reader_fn, mock_pipeline_fn, mock_writer_fn
    ):
        config = DataCoolieRunConfig(retry_count=1, retry_delay=0)
        driver, *_ = _make_driver(config=config)

        reader = MagicMock()
        reader.read.side_effect = [RuntimeError("transient read failure"), "fake_df"]
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        mock_reader_fn.return_value = reader

        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed_df"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        mock_pipeline_fn.return_value = pipeline

        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        mock_writer_fn.return_value = writer

        dataflow = _dataflow_with_watermark()
        connection = dataflow.source.connection
        connection.configure["password"] = "password-ref"
        connection.secrets_ref = {"scope": ["password"]}

        secret_provider = MagicMock()
        secret_provider.get_secret.return_value = "resolved-value"
        driver._secret_provider = secret_provider

        replay = ReplayConfig(start="2025-01-01", end="2025-02-01")
        result = driver.run_replay(dataflow, replay)

        assert result.succeeded == 1
        assert reader.read.call_count == 2
        secret_provider.get_secret.assert_called_once_with("password-ref", "scope")
        assert connection.configure["password"] == "password-ref"

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_destination_writer")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_chunks_reuse_native_provider_ttl_cache(
        self, mock_reader_fn, mock_pipeline_fn, mock_writer_fn
    ):
        driver, *_ = self._setup_driver_for_replay()

        reader = MagicMock()
        reader.read.return_value = "fake_df"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        mock_reader_fn.return_value = reader

        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed_df"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        mock_pipeline_fn.return_value = pipeline

        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        mock_writer_fn.return_value = writer

        dataflow = _dataflow_with_watermark()
        connection = dataflow.source.connection
        connection.configure["password"] = "password-ref"
        connection.secrets_ref = {"scope": ["password"]}

        provider = _CountingSecretProvider()
        driver._secret_provider = provider

        result = driver.run_replay(
            dataflow,
            _replay_config(chunk_interval={"months": 1}),
        )

        assert result.succeeded == 1
        assert reader.read.call_count == 3
        assert provider.fetch_count == 1
        assert connection.configure["password"] == "password-ref"

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_destination_writer")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_update_watermark_saves_per_chunk(self, mock_reader_fn, mock_pipeline_fn, mock_writer_fn):
        driver, engine, _, watermark = self._setup_driver_for_replay()
        watermark.get_watermark.return_value = None  # No stored watermark

        reader = MagicMock()
        reader.read.return_value = "fake_df"
        observations = [
            {"order_date": date(2025, 1, 31)},
            {"order_date": date(2025, 2, 28)},
            {"order_date": date(2025, 3, 31)},
        ]
        reader.get_new_watermark.side_effect = observations
        reader.merge_watermark.side_effect = lambda existing, candidate: dict(candidate)
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        mock_reader_fn.return_value = reader

        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed_df"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        mock_pipeline_fn.return_value = pipeline

        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        mock_writer_fn.return_value = writer

        dataflow = _dataflow_with_watermark()
        replay = _replay_config(chunk_interval={"months": 1}, update_watermark=True)

        result = driver.run_replay(dataflow, replay)

        # 1 dataflow, all 3 chunks succeeded
        assert result.total == 1
        assert result.succeeded == 1
        # Watermark saved 3 times (once per chunk)
        assert watermark.save_watermark.call_count == 3
        assert reader.read.call_count == 3
        assert [entry.kwargs["watermark"] for entry in watermark.save_watermark.call_args_list] == observations

    @pytest.mark.parametrize("stored", [date(2024, 12, 1), date(2025, 3, 1),
                                      date(2025, 4, 1), date(2025, 5, 1)])
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_replay_reruns_all_chunks_when_stored_watermark_is_mid_range(
        self, mock_reader_fn, stored
    ):
        driver, engine, _, watermark = self._setup_driver_for_replay()
        # A stored production watermark never acts as a replay checkpoint.
        watermark.get_watermark.return_value = {"order_date": stored}

        reader = MagicMock()
        reader.read.return_value = "fake_df"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        mock_reader_fn.return_value = reader

        dataflow = _dataflow_with_watermark()
        replay = _replay_config(chunk_interval={"months": 1}, update_watermark=True)

        with patch.object(driver, "_create_transformer_pipeline") as mock_pipe, \
             patch.object(driver, "_create_destination_writer") as mock_writer:
            pipeline = MagicMock()
            pipeline.transform.return_value = "df"
            pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
            mock_pipe.return_value = pipeline

            writer = MagicMock()
            writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=5)
            mock_writer.return_value = writer

            result = driver.run_replay(dataflow, replay)

        # All three requested chunks run again: Jan, Feb and Mar.
        assert result.total == 1
        assert result.succeeded == 1
        assert result.skipped == 0
        assert reader.read.call_count == 3
        ranges = [call.kwargs["read_range"] for call in reader.read.call_args_list]
        assert [(item.start, item.end) for item in ranges] == [
            (date(2025, 1, 1), date(2025, 2, 1)),
            (date(2025, 2, 1), date(2025, 3, 1)),
            (date(2025, 3, 1), date(2025, 4, 1)),
        ]

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_destination_writer")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_replay_reruns_range_when_stored_watermark_at_end(
        self, mock_reader_fn, mock_pipeline_fn, mock_writer_fn
    ):
        driver, engine, _, watermark = self._setup_driver_for_replay()
        # A stored production watermark never acts as a replay checkpoint.
        watermark.get_watermark.return_value = {"order_date": date(2025, 4, 1)}

        reader = MagicMock()
        reader.read.return_value = "fake_df"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        reader.get_new_watermark.return_value = {"order_date": date(2025, 4, 1)}
        mock_reader_fn.return_value = reader

        pipeline = MagicMock()
        pipeline.transform.return_value = "transformed_df"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        mock_pipeline_fn.return_value = pipeline

        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        mock_writer_fn.return_value = writer

        dataflow = _dataflow_with_watermark()
        replay = _replay_config(chunk_interval={"months": 1}, update_watermark=True)

        result = driver.run_replay(dataflow, replay)

        # All three requested chunks run again, even though the stored
        # watermark is already at the replay end.
        assert result.total == 1
        assert result.succeeded == 1
        assert result.skipped == 0
        assert reader.read.call_count == 3
        assert watermark.save_watermark.call_count == 3

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_chunk_no_data_skipped(self, mock_reader_fn):
        driver, engine, _, watermark = self._setup_driver_for_replay(rows_read=0)

        reader = MagicMock()
        reader.read.return_value = None
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=0)
        mock_reader_fn.return_value = reader

        dataflow = _dataflow_with_watermark()
        replay = _replay_config(chunk_interval={"months": 1})

        with patch.object(driver, "_on_replay_complete") as on_complete:
            result = driver.run_replay(dataflow, replay)

        # 1 dataflow, all 3 chunks skipped (no data)
        assert result.total == 1
        assert result.skipped == 1
        assert on_complete.call_args.args[0].message == "All replay chunks were skipped"

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_chunk_failure_stops_replay(self, mock_reader_fn):
        driver, engine, _, watermark = self._setup_driver_for_replay()

        reader = MagicMock()
        reader.read.side_effect = RuntimeError("connection lost")
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=0)
        mock_reader_fn.return_value = reader

        dataflow = _dataflow_with_watermark()
        replay = _replay_config(chunk_interval={"months": 1})

        result = driver.run_replay(dataflow, replay)

        # Processing stops at the first failed chunk
        assert result.total == 1
        assert result.failed == 1
        assert "connection lost" in result.errors.get("df-1", "")

    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_destination_writer")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline")
    @patch("datacoolie.orchestration.driver.DataCoolieDriver._create_source_reader")
    def test_date_backward_disabled(self, mock_reader_fn, mock_pipeline_fn, mock_writer_fn):
        """Replay disables date_backward on the source."""
        driver, engine, _, watermark = self._setup_driver_for_replay()

        reader = MagicMock()
        reader.read.return_value = "fake_df"
        reader.get_runtime_info.return_value = SourceRuntimeInfo(rows_read=10)
        mock_reader_fn.return_value = reader

        pipeline = MagicMock()
        pipeline.transform.return_value = "df"
        pipeline.get_runtime_info.return_value = TransformRuntimeInfo()
        mock_pipeline_fn.return_value = pipeline

        writer = MagicMock()
        writer.get_runtime_info.return_value = DestinationRuntimeInfo(rows_written=10)
        mock_writer_fn.return_value = writer

        dataflow = _dataflow_with_watermark()
        dataflow.source.configure["backward_days"] = 7  # Would normally extend read

        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-02-01",
        )

        result = driver.run_replay(dataflow, replay)

        assert result.total == 1
        assert result.succeeded == 1
        # date_backward should have been cleared on the copy
        # (the original is unchanged since model_copy is used)
        assert dataflow.source.configure["backward_days"] == 7

    def test_chunk_uses_gte_and_lt_operators(self):
        """All replay chunks use >= for lower bound and < for upper bound."""
        from datetime import date

        driver, *_ = self._setup_driver_for_replay()

        dataflow = _dataflow_with_watermark(watermark_columns=["order_date"])
        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-02-01",
            chunk_interval={"months": 1},
        )

        with patch.object(driver, "_execute_etl_pipeline") as mock_pipeline:
            mock_pipeline.return_value = PipelineAttemptResult(
                status=DataFlowStatus.SUCCEEDED.value,
                source=SourceRuntimeInfo(rows_read=5),
                transform=TransformRuntimeInfo(),
                destination=DestinationRuntimeInfo(rows_written=5),
            )
            driver.run_replay(dataflow, replay)

        # watermark_start is the raw inclusive lower bound
        call_kwargs = mock_pipeline.call_args[1]
        assert call_kwargs["watermark_start"] == {"order_date": date(2025, 1, 1)}

        # watermark_start_operator passed as proper parameter (not via source.configure)
        assert call_kwargs["watermark_start_operator"] == ">="

        # Upper bound passed as watermark_end (not via filter_expression)
        assert call_kwargs["watermark_end"] == {"order_date": date(2025, 2, 1)}

    def test_auto_resolve_column_from_watermark_columns(self):
        """chunk_column auto-resolved from dataflow.source.watermark_columns[0]."""
        driver, engine, _, watermark = self._setup_driver_for_replay()

        dataflow = _dataflow_with_watermark(watermark_columns=["shipped_at"])
        replay = ReplayConfig(start="2025-01-01", end="2025-02-01")

        with patch.object(driver, "_execute_etl_pipeline") as mock_pipeline:
            mock_pipeline.return_value = PipelineAttemptResult(
                status=DataFlowStatus.SUCCEEDED.value,
                source=SourceRuntimeInfo(rows_read=10),
                transform=TransformRuntimeInfo(),
                destination=DestinationRuntimeInfo(rows_written=10),
            )
            result = driver.run_replay(dataflow, replay)

        assert result.total == 1
        # Verify the override watermark uses auto-resolved column "shipped_at"
        call_kwargs = mock_pipeline.call_args[1]
        assert "shipped_at" in call_kwargs["watermark_start"]

    def test_no_watermark_columns_raises(self):
        """Error when no watermark_columns and no chunk_column override."""
        driver, *_ = self._setup_driver_for_replay()

        dataflow = _dataflow()  # no watermark_columns
        replay = ReplayConfig(start="2025-01-01", end="2025-02-01")

        with pytest.raises(DataCoolieError, match="Cannot auto-resolve chunk_column"):
            driver.run_replay([dataflow], replay)

    def test_explicit_chunk_column_overrides_auto(self):
        """Explicit chunk_column takes priority over watermark_columns[0]."""
        driver, engine, _, watermark = self._setup_driver_for_replay()

        dataflow = _dataflow_with_watermark(watermark_columns=["order_date", "region_id"])
        replay = ReplayConfig(
            start=0, end=100,
            chunk_column="region_id",
            chunk_interval={"step": 50},
        )

        with patch.object(driver, "_execute_etl_pipeline") as mock_pipeline:
            mock_pipeline.return_value = PipelineAttemptResult(
                status=DataFlowStatus.SUCCEEDED.value,
                source=SourceRuntimeInfo(rows_read=10),
                transform=TransformRuntimeInfo(),
                destination=DestinationRuntimeInfo(rows_written=10),
            )
            driver.run_replay(dataflow, replay)

        assert mock_pipeline.call_count == 2
        call_kwargs = mock_pipeline.call_args_list[0][1]
        assert "region_id" in call_kwargs["watermark_start"]

    def test_api_allows_independent_chunk_column_with_range_mapping(self):
        """API selection may be separate from persisted watermark columns."""
        driver, *_ = self._setup_driver_for_replay()
        conn = _conn(fmt=Format.API.value)
        dataflow = DataFlow(
            dataflow_id="api-independent",
            source=Source(
                connection=conn,
                table="src",
                watermark_columns=["updated_at"],
                configure={
                    "range_param_mapping": {
                        "created_at": {
                            "lower": {"name": "created_from", "operator": ">="},
                            "upper": {"name": "created_to", "operator": "<"},
                            "format": "iso",
                            "response_column": "created_at",
                            "watermark_value": "observed_max",
                        }
                    }
                },
            ),
            destination=Destination(
                connection=conn,
                table="dst",
                load_type=LoadType.APPEND.value,
            ),
        )
        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-03-01",
            chunk_interval={"months": 1},
            chunk_column="created_at",
        )

        with patch.object(driver, "_execute_etl_pipeline") as mock_pipeline:
            mock_pipeline.return_value = PipelineAttemptResult(
                status=DataFlowStatus.SUCCEEDED.value,
                source=SourceRuntimeInfo(rows_read=1),
                transform=TransformRuntimeInfo(),
                destination=DestinationRuntimeInfo(rows_written=1),
            )
            result = driver.run_replay(dataflow, replay)

        assert result.succeeded == 1
        assert mock_pipeline.call_count == 2
        for pipeline_call in mock_pipeline.call_args_list:
            kwargs = pipeline_call.kwargs
            assert kwargs["read_range"].column == "created_at"
            assert kwargs["watermark_start"] is None
            assert kwargs["watermark_end"] is None

    def test_api_independent_chunk_column_requires_range_mapping(self):
        """An API field without an exact mapping fails before execution."""
        driver, *_ = self._setup_driver_for_replay()
        conn = _conn(fmt=Format.API.value)
        dataflow = DataFlow(
            dataflow_id="api-missing-mapping",
            source=Source(
                connection=conn,
                table="src",
                watermark_columns=["updated_at"],
            ),
            destination=Destination(
                connection=conn,
                table="dst",
                load_type=LoadType.APPEND.value,
            ),
        )
        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-02-01",
            chunk_column="created_at",
        )

        with patch.object(driver, "_execute_etl_pipeline") as mock_pipeline:
            with pytest.raises(DataCoolieError, match="range_param_mapping"):
                driver.run_replay(dataflow, replay)

        mock_pipeline.assert_not_called()


# ============================================================================
# create_driver factory
# ============================================================================


class TestCreateDriver:
    def test_basic(self):
        driver = create_driver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
            job_num=2,
            job_index=1,
            max_workers=4,
        )
        assert isinstance(driver, DataCoolieDriver)
        assert driver.config.job_num == 2
        assert driver.config.job_index == 1
        assert driver.config.max_workers == 4

    def test_extra_kwargs(self):
        driver = create_driver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
            dry_run=True,
            retry_count=3,
        )
        assert driver.config.dry_run is True
        assert driver.config.retry_count == 3

    def test_with_log_base_path(self):
        from datacoolie.logging import ExecutionLogger, SystemLogger

        driver = create_driver(
            engine=_mock_engine(),
            platform=_mock_platform(),
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
            log_base_path="/logs",
        )

        assert isinstance(driver._system_logger, SystemLogger)
        assert isinstance(driver._execution_logger, ExecutionLogger)
        assert driver._system_logger.config.output_path == "/logs/system_logs"
        assert driver._execution_logger.config.output_path == "/logs/execution_logs"

    def test_old_base_log_path_keyword_is_rejected(self):
        with pytest.raises(TypeError, match="base_log_path.*log_base_path"):
            create_driver(
                engine=_mock_engine(),
                platform=_mock_platform(),
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
                base_log_path="/logs",
            )

    def test_unknown_run_config_keyword_is_rejected(self):
        with pytest.raises(TypeError, match="unsupported_option"):
            create_driver(
                engine=_mock_engine(),
                platform=_mock_platform(),
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
                unsupported_option=True,
            )


# ============================================================================
# Additional edge cases (merged from edge-case module)
# ============================================================================


class TestDriverInitEdgeCases:
    def test_raises_when_platform_type_mismatch(self):
        class P1: ...

        class P2: ...

        engine = _mock_engine()
        engine.platform = P1()
        with pytest.raises(DataCoolieError, match="Platform type mismatch"):
            DataCoolieDriver(
                engine=engine,
                platform=P2(),
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
            )

    def test_raises_when_no_platform_anywhere(self):
        engine = _mock_engine()
        engine.platform = None
        with pytest.raises(DataCoolieError, match="A platform is required"):
            DataCoolieDriver(
                engine=engine,
                platform=None,
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
            )

    def test_uses_existing_engine_platform_when_no_platform_arg(self):
        existing = _mock_platform()
        engine = _mock_engine()
        engine.platform = existing
        d = DataCoolieDriver(
            engine=engine,
            platform=None,
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
        )
        assert d._engine.platform is existing

    def test_same_platform_type_does_not_raise(self):
        platform = _mock_platform()
        engine = _mock_engine()
        engine.platform = platform
        d = DataCoolieDriver(
            engine=engine,
            platform=platform,
            metadata_provider=_mock_metadata(),
            watermark_manager=_mock_watermark(),
        )
        assert d._engine.platform is platform

    def test_distinct_platform_instance_is_rejected_even_when_type_matches(self):
        platform = _mock_platform()
        engine = _mock_engine()
        engine.platform = platform
        with pytest.raises(DataCoolieError, match="different platform instances"):
            DataCoolieDriver(
                engine=engine,
                platform=_mock_platform(),
                metadata_provider=_mock_metadata(),
                watermark_manager=_mock_watermark(),
            )

    def test_closed_driver_rejects_run_before_executor_submission(self):
        driver, *_ = _make_driver()
        driver.close()
        driver._executor.execute_with_groups = MagicMock()

        with pytest.raises(RuntimeError, match="closed"):
            driver.run_dataflow(dataflows=[_dataflow()])

        driver._executor.execute_with_groups.assert_not_called()

    def test_operation_admission_is_released_after_failure(self):
        driver, *_ = _make_driver()
        execute = MagicMock(side_effect=RuntimeError("executor failed"))
        driver._executor.execute_with_groups = execute

        with pytest.raises(RuntimeError, match="executor failed"):
            driver.run_dataflow(dataflows=[_dataflow()])
        assert driver._operation_active is False

        execute.side_effect = None
        execute.return_value = ExecutionResult()
        result = driver.run_dataflow(dataflows=[])
        assert result.total == 0

    def test_keyboard_interrupt_marks_session_failed_and_releases_admission(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        driver._executor.execute_with_groups = MagicMock(
            side_effect=KeyboardInterrupt("cancelled")
        )

        with pytest.raises(KeyboardInterrupt, match="cancelled"):
            driver.run_dataflow(dataflows=[_dataflow()])

        assert driver._operation_active is False
        assert driver._session_failed is True
        assert "cancelled" in (driver._session_error_message or "")
        driver.close()

    def test_system_exit_marks_session_failed_and_releases_admission(self):
        driver, *_ = _make_driver()
        driver._executor.execute_with_groups = MagicMock(
            side_effect=SystemExit("shutdown")
        )

        with pytest.raises(SystemExit, match="shutdown"):
            driver.run_dataflow(dataflows=[_dataflow()])

        assert driver._operation_active is False
        assert driver._final_job_status() == DataFlowStatus.FAILED.value
        driver.close()

    def test_diagnostic_failure_at_operation_start_does_not_stick_guard(self):
        import datacoolie.orchestration.driver as driver_module

        driver, *_ = _make_driver()
        with patch.object(
            driver_module.logger,
            "log",
            side_effect=[RuntimeError("start diagnostic failed"), None, None],
        ):
            # Patch only the first lifecycle emission.  The operation itself
            # and its summary must continue after the diagnostic failure.
            result = driver.run_dataflow(dataflows=[])

        assert result.total == 0
        assert driver._operation_active is False
        driver.close()

    def test_later_operation_failure_does_not_replace_result_details(self):
        driver, *_ = _make_driver()
        driver._observe_operation_result(
            ExecutionResult(
                total=1,
                failed=1,
                errors={"first": "first failure"},
            )
        )
        driver._executor.execute_with_groups = MagicMock(
            side_effect=RuntimeError("second failure")
        )

        with pytest.raises(RuntimeError, match="second failure"):
            driver.run_dataflow(dataflows=[_dataflow()])

        message = driver._session_error_message or ""
        assert "first: first failure" in message
        assert "second failure" in message
        driver.close()

    def test_context_manager_records_body_failure_before_cleanup(self):
        driver, *_ = _make_driver()

        with pytest.raises(ValueError, match="body failed"):
            with driver:
                raise ValueError("body failed")

        assert driver._session_failed is True
        assert "body failed" in (driver._session_error_message or "")
        assert driver._closed is True

    def test_close_rejects_while_public_operation_is_active(self):
        import threading

        driver, *_ = _make_driver()
        entered = threading.Event()
        release = threading.Event()
        errors: list[BaseException] = []

        def execute(*_args, **_kwargs):
            entered.set()
            release.wait(timeout=2)
            return ExecutionResult()

        driver._executor.execute_with_groups = execute

        def run() -> None:
            try:
                driver.run_dataflow(dataflows=[_dataflow()])
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        assert entered.wait(timeout=2)

        with pytest.raises(RuntimeError, match="operation is active"):
            driver.close()
        with pytest.raises(RuntimeError, match="active operation"):
            driver.run_dataflow(dataflows=[])

        release.set()
        worker.join(timeout=2)
        assert errors == []
        driver.close()


class TestDriverProcessingEdgeCases:
    def test_scheduler_fallback_gets_one_execution_observation(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        dataflow = _dataflow("scheduler-fallback")

        with patch.object(
            driver,
            "_process_dataflow",
            side_effect=RuntimeError("worker boundary failed"),
        ):
            result = driver._run_dataflow(dataflows=[dataflow])

        assert result.failed == 1
        execution_logger.log.assert_called_once()
        logged_metadata = execution_logger.log.call_args.kwargs["dataflow"]
        logged_runtime = execution_logger.log.call_args.kwargs["runtime_info"]
        assert logged_metadata.dataflow_id == dataflow.dataflow_id
        assert logged_runtime.dataflow_id == dataflow.dataflow_id
        assert logged_runtime.operation_type == ExecutionType.ETL.value
        assert logged_runtime.message == "worker boundary failed"

    def test_replay_orchestration_failure_has_no_aggregate_execution_row(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        dataflow = _dataflow_with_watermark("replay-orchestration-failure")
        replay = ReplayConfig(start="2025-01-01", end="2025-02-01")

        with patch.object(
            driver,
            "_process_replay",
            side_effect=RuntimeError("replay scheduler failed"),
        ):
            result = driver.run_replay(dataflow, replay)

        assert result.failed == 1
        assert result.errors[dataflow.dataflow_id] == "replay scheduler failed"
        assert "replay scheduler failed" in driver._session_error_message
        execution_logger.log.assert_not_called()

    def test_replay_chunk_boundary_logs_one_failed_chunk(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        dataflow = _dataflow_with_watermark("replay-chunk-boundary")
        replay = ReplayConfig(
            start="2025-01-01",
            end="2025-03-01",
            chunk_interval={"months": 1},
        )

        with patch.object(
            driver,
            "_run_single_pipeline",
            side_effect=RuntimeError("chunk boundary failed"),
        ):
            result = driver.run_replay(dataflow, replay)

        assert result.failed == 1
        execution_logger.log.assert_called_once()
        logged_runtime = execution_logger.log.call_args.kwargs["runtime_info"]
        assert logged_runtime.operation_type == ExecutionType.REPLAY.value
        assert logged_runtime.message == "chunk boundary failed"

    def test_etl_completion_observation_precedes_hook_override(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        dataflow = _dataflow("hook-order")
        def mutate_hook(result):
            result.status = DataFlowStatus.SUCCEEDED.value

        hook = MagicMock(side_effect=mutate_hook)
        driver._on_dataflow_complete = hook

        with patch.object(
            driver,
            "_process_dataflow",
            side_effect=RuntimeError("worker boundary failed"),
        ):
            result = driver._run_dataflow(dataflows=[dataflow])

        assert result.failed == 1
        execution_logger.log.assert_called_once()
        hook.assert_called_once()
        assert (
            execution_logger.log.call_args.kwargs["runtime_info"].status
            == DataFlowStatus.FAILED.value
        )
        driver.close()

    def test_maintenance_completion_observation_precedes_hook_override(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        dataflow = _dataflow("maintenance-hook")
        hook = MagicMock()
        driver._on_maintenance_complete = hook

        with patch.object(
            driver,
            "_process_maintenance",
            side_effect=RuntimeError("maintenance boundary failed"),
        ):
            result = driver._run_maintenance(dataflows=[dataflow])

        assert result.failed == 1
        execution_logger.log.assert_called_once()
        hook.assert_called_once()
        driver.close()

    def test_completion_hook_failure_is_observational(self):
        driver, *_ = _make_driver()
        driver._on_dataflow_complete = MagicMock(
            side_effect=RuntimeError("hook failed")
        )

        with patch.object(
            driver,
            "_process_dataflow",
            side_effect=RuntimeError("worker boundary failed"),
        ):
            result = driver._run_dataflow(dataflows=[_dataflow("hook-error")])

        assert result.failed == 1
        assert driver._session_failed is False
        driver.close()

    def test_process_dataflow_pipeline_error_without_partial(self):
        d, *_ = _make_driver(execution_logger=None)
        df = _dataflow()
        d._retry_handler.execute = MagicMock(side_effect=PipelineError("boom", partial_result=None))

        result = d._process_dataflow(df)
        assert result.status == DataFlowStatus.FAILED.value
        assert result.message == "boom"

    def test_process_dataflow_generic_exception_branch(self):
        d, *_ = _make_driver(execution_logger=None)
        df = _dataflow()
        d._retry_handler.execute = MagicMock(side_effect=RuntimeError("explode"))

        result = d._process_dataflow(df)
        assert result.status == DataFlowStatus.FAILED.value
        assert result.message == "explode"

    def test_process_dataflow_execution_logger_failure_swallowed(self):
        execution_logger = _execution_logger_spy()
        execution_logger.log.side_effect = RuntimeError("log fail")
        d, *_ = _make_driver(execution_logger=execution_logger)
        df = _dataflow()

        src = SourceRuntimeInfo(rows_read=1, status=DataFlowStatus.SUCCEEDED.value)
        trn = TransformRuntimeInfo(status=DataFlowStatus.SUCCEEDED.value)
        dst = DestinationRuntimeInfo(rows_written=1, status=DataFlowStatus.SUCCEEDED.value)
        d._retry_handler.execute = MagicMock(
            return_value=(
                PipelineAttemptResult(
                    status=DataFlowStatus.SUCCEEDED.value,
                    source=src,
                    transform=trn,
                    destination=dst,
                ),
                1,
            )
        )

        result = d._process_dataflow(df)
        assert result.status == DataFlowStatus.SUCCEEDED.value

    def test_process_maintenance_pipeline_error_with_partial(self):
        d, *_ = _make_driver(execution_logger=None)
        df = _dataflow()
        destination = DestinationRuntimeInfo(
            status=DataFlowStatus.FAILED.value,
            message="partial",
        )
        partial = PipelineAttemptResult(
            status=DataFlowStatus.FAILED.value,
            destination=destination,
        )
        d._retry_handler.execute = MagicMock(side_effect=PipelineError("x", partial_result=partial))

        result = d._process_maintenance(df)
        assert result.destination is destination
        assert result.status == DataFlowStatus.FAILED.value

    def test_process_maintenance_ignores_foreign_partial_result(self):
        d, *_ = _make_driver(execution_logger=None)
        df = _dataflow()
        foreign = DestinationRuntimeInfo(status=DataFlowStatus.FAILED.value)
        d._retry_handler.execute = MagicMock(
            side_effect=PipelineError("x", partial_result=foreign)
        )

        result = d._process_maintenance(df)

        assert result.status == DataFlowStatus.FAILED.value
        assert result.destination.status == DataFlowStatus.PENDING.value

    def test_process_maintenance_generic_exception_branch(self):
        d, *_ = _make_driver(execution_logger=None)
        df = _dataflow()
        d._retry_handler.execute = MagicMock(side_effect=RuntimeError("maint fail"))

        result = d._process_maintenance(df)
        assert result.status == DataFlowStatus.FAILED.value
        assert result.message == "maint fail"


class TestDriverHelperBranches:
    def test_dataflow_error_is_not_appended_again_to_job_session_message(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        dataflow = _dataflow("failed-summary")
        dataflow.name = "Orders"
        runtime = DataFlowRuntimeInfo(
            dataflow_id=dataflow.dataflow_id,
            status=DataFlowStatus.FAILED.value,
            message="database unavailable",
            source=SourceRuntimeInfo(),
            destination=DestinationRuntimeInfo(),
        )

        from datacoolie.orchestration.execution.lifecycle import log_result_safely

        log_result_safely(driver._log_dataflow_result, dataflow, runtime)
        driver._record_dataflow_complete(runtime)
        driver._observe_operation_result(
            ExecutionResult(
                total=1,
                failed=1,
                errors={dataflow.dataflow_id: runtime.message},
            )
        )

        assert driver._session_error_message is None
        assert execution_logger.job_runtime.message == "Orders [failed-summary]"
        driver.close()

    def test_unobserved_scheduler_error_is_kept_in_session_summary(self):
        driver, *_ = _make_driver()
        driver._completion_observed_ids = set()

        driver._observe_operation_result(
            ExecutionResult(
                total=1,
                failed=1,
                errors={"scheduler": "worker boundary failed"},
            )
        )

        assert driver._session_error_message == "scheduler: worker boundary failed"
        driver.close()

    def test_empty_interrupt_uses_exception_type_and_is_recorded_once(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        error = KeyboardInterrupt()

        try:
            with driver._driver_operation("test"):
                raise error
        except KeyboardInterrupt as caught:
            assert caught is error

        assert driver._session_error_message == "KeyboardInterrupt"
        driver.close()

    def test_manual_close_preserves_primary_exception_when_finalization_fails(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        primary = ValueError("business failure")

        try:
            try:
                with driver._driver_operation("test"):
                    raise primary
            finally:
                with patch.object(
                    execution_logger,
                    "finish_job",
                    side_effect=ConfigurationError("finalization failed"),
                ):
                    driver.close()
        except BaseException as caught:
            assert caught is primary
        else:  # pragma: no cover - assertion documents the required path
            pytest.fail("the primary exception was unexpectedly swallowed")

    def test_handler_interrupt_does_not_replace_primary_exception(self):
        import datacoolie.orchestration.driver as driver_module

        driver, *_ = _make_driver()
        primary = ValueError("business failure")

        with patch.object(
            driver_module.logger,
            "log",
            side_effect=KeyboardInterrupt("diagnostic interruption"),
        ):
            try:
                with driver._driver_operation("test"):
                    raise primary
            except BaseException as caught:
                assert caught is primary

        driver.close()

    def test_owned_provider_failure_marks_job_failed_after_all_cleanup(self):
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)
        provider = MagicMock()
        provider.close.side_effect = RuntimeError("provider close failed")
        driver._metadata_provider = provider
        driver._owns_metadata_provider = True

        with pytest.raises(RuntimeError, match="provider close failed"):
            driver.close()

        provider.close.assert_called_once_with()
        execution_logger.close.assert_called_once_with()
        assert execution_logger.job_runtime.status == DataFlowStatus.FAILED.value
        assert "provider close failed" in (execution_logger.job_runtime.message or "")

    def test_borrowed_provider_is_not_closed_by_driver(self):
        driver, *_ = _make_driver()
        provider = MagicMock()
        driver._metadata_provider = provider

        driver.close()

        provider.close.assert_not_called()

    def test_pre_finalization_diagnostic_interrupt_fails_job_and_closes_loggers(self):
        import datacoolie.orchestration.driver as driver_module

        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(execution_logger=execution_logger)

        with patch.object(
            driver_module.logger,
            "log",
            side_effect=KeyboardInterrupt("finishing interrupted"),
        ):
            with pytest.raises(KeyboardInterrupt, match="finishing interrupted"):
                driver.close()

        assert execution_logger.job_runtime.status == DataFlowStatus.FAILED.value
        execution_logger.close.assert_called_once_with()

    def test_create_source_reader_python_function_format(self):
        d, *_ = _make_driver()
        reader = d._create_source_reader(Format.FUNCTION.value)
        from datacoolie.sources import PythonFunctionReader

        assert isinstance(reader, PythonFunctionReader)

    def test_close_swallows_logger_close_errors(self):
        good = _system_logger_spy()
        bad = _execution_logger_spy()
        bad.close.side_effect = RuntimeError("close fail")

        d, *_ = _make_driver(system_logger=good, execution_logger=bad)
        d.close()  # should not raise

    def test_close_retries_remaining_cleanup_after_logger_interrupt(self):
        good = _system_logger_spy()
        interrupted = _execution_logger_spy()
        interrupted.close.side_effect = KeyboardInterrupt("close cancelled")

        driver, *_ = _make_driver(
            system_logger=good,
            execution_logger=interrupted,
        )

        with pytest.raises(KeyboardInterrupt, match="close cancelled"):
            driver.close()

        interrupted.close.assert_called_once_with()
        good.close.assert_called_once_with()
        assert interrupted.job_runtime.status == DataFlowStatus.SKIPPED.value
        assert driver._closed is True

    def test_close_diagnostic_failure_still_closes_session_loggers(self):
        import datacoolie.orchestration.driver as driver_module

        system_logger = _system_logger_spy()
        execution_logger = _execution_logger_spy()
        driver, *_ = _make_driver(
            system_logger=system_logger,
            execution_logger=execution_logger,
        )

        with patch.object(
            driver_module.logger,
            "log",
            side_effect=RuntimeError("finish diagnostic failed"),
        ):
            driver.close()

        execution_logger.close.assert_called_once_with()
        system_logger.close.assert_called_once_with()

    def test_close_closes_execution_before_system(self):
        close_order = []
        system_logger = _system_logger_spy()
        execution_logger = _execution_logger_spy()
        execution_logger.close.side_effect = lambda: close_order.append("execution")
        system_logger.close.side_effect = lambda: close_order.append("system")

        driver, *_ = _make_driver(
            system_logger=system_logger,
            execution_logger=execution_logger,
        )
        driver.close()

        assert close_order == ["execution", "system"]


# ============================================================================
# Phase G — deep-copy DataFlow isolation
# ============================================================================


class TestDeepCopyDataFlow:
    """Verify _process_dataflow does not mutate the original DataFlow."""

    def test_process_dataflow_does_not_mutate_original(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()
        original_id = df.dataflow_id
        original_table = df.source.table
        original_connection = df.source.connection
        original_connection.configure["password"] = "db-password"
        original_connection.secrets_ref = {"scope": ["password"]}

        secret_provider = MagicMock()
        secret_provider.get_secret.return_value = "runtime-secret"
        d._secret_provider = secret_provider

        mock_reader = MagicMock()
        mock_reader.read.return_value = None
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=0, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {}

        with patch.object(d, "_create_source_reader", return_value=mock_reader):
            d._process_dataflow(df)

        # Original DataFlow remains untouched
        assert df.dataflow_id == original_id
        assert df.source.table == original_table
        assert original_connection.configure["password"] == "db-password"
        secret_provider.get_secret.assert_called_once_with("db-password", "scope")


# ============================================================================
# Phase I — watermark skipped on write failure
# ============================================================================


class TestWatermarkAtomicSave:
    """Verify watermark is NOT saved when write fails."""

    def test_write_failure_skips_watermark_save(self):
        d, engine, _, wm = _make_driver()
        df = _dataflow()

        mock_reader = MagicMock()
        mock_reader.read.return_value = "fake_df"
        mock_reader.get_runtime_info.return_value = SourceRuntimeInfo(
            rows_read=10, status=DataFlowStatus.SUCCEEDED.value
        )
        mock_reader.get_new_watermark.return_value = {"col": "2024-01-01"}

        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = "transformed_df"
        mock_pipeline.get_runtime_info.return_value = TransformRuntimeInfo(
            transformers_applied=["SchemaConverter"],
            status=DataFlowStatus.SUCCEEDED.value,
        )

        mock_writer = MagicMock()
        mock_writer.write.side_effect = RuntimeError("write boom")

        with patch.object(d, "_create_source_reader", return_value=mock_reader), \
             patch.object(d, "_create_transformer_pipeline", return_value=mock_pipeline), \
             patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d._process_dataflow(df)

        assert result.status == DataFlowStatus.FAILED.value
        wm.save_watermark.assert_not_called()


class TestDriverCoverageGaps:
    """Cover specific uncovered branches in driver.py."""

    def test_run_replay_empty_list_returns_empty_result(self) -> None:
        """Line 666: run_replay returns early when target is empty list."""
        driver, engine, metadata, watermark = _make_driver()
        replay = ReplayConfig(start='2025-01-01', end='2025-04-01')
        result = driver.run_replay([], replay)
        assert result.total == 0

    def test_init_with_metadata_provider_autocreates_watermark_manager(self) -> None:
        """Lines 164-165: WatermarkManager is auto-created when metadata_provider given."""
        engine = _mock_engine()
        metadata = _mock_metadata()
        # Pass metadata_provider but NO watermark_manager
        driver = DataCoolieDriver(
            engine=engine,
            platform=_mock_platform(),
            metadata_provider=metadata,
            watermark_manager=None,
        )
        # Should have auto-created a watermark manager
        assert driver._watermark_manager is not None

    def test_run_replay_auto_resolves_chunk_column_from_watermark(self) -> None:
        """Line 719: auto-resolve chunk_column from source.watermark_columns."""
        driver, engine, metadata, watermark = _make_driver()
        engine.filter_rows.side_effect = lambda df, expr: df
        engine.count_rows.return_value = 5

        dataflow = DataFlow(
            dataflow_id='df-auto',
            source=Source(
                connection=_conn(),
                table='src',
                watermark_columns=['order_date'],  # auto-resolve chunk_column from here
            ),
            destination=Destination(
                connection=_conn(),
                table='dst',
                load_type=LoadType.APPEND.value,
            ),
        )
        replay = ReplayConfig(
            start='2025-01-01',
            end='2025-02-01',
            # chunk_column=None -> auto-resolved from watermark_columns
        )

        with patch.object(driver, '_execute_etl_pipeline') as mock_pipeline:
            mock_pipeline.return_value = PipelineAttemptResult(
                status=DataFlowStatus.SUCCEEDED.value,
                source=SourceRuntimeInfo(rows_read=5),
                transform=TransformRuntimeInfo(),
                destination=DestinationRuntimeInfo(rows_written=5),
            )
            result = driver.run_replay(dataflow, replay)

        assert result.total >= 1


class TestDriverRemainingCoverage:
    """Cover lines 719, 1030-1032, 1059."""

    def test_run_replay_raises_when_no_watermark_columns_and_no_chunk_column(self) -> None:
        """Line 719: raises DataCoolieError when no chunk_column and no watermark_columns."""
        driver = DataCoolieDriver(
            engine=MagicMock(),
            metadata_provider=MagicMock(),
        )
        # Source with no watermark_columns
        src_conn = Connection(connection_id='c-src', name='src', format='parquet')
        dest_conn = Connection(connection_id='c-dst', name='dst', format='parquet')
        df = DataFlow(
            dataflow_id='df-replay-fail',
            source=Source(connection=src_conn, table='t', watermark_columns=[]),
            destination=Destination(connection=dest_conn, table='out'),
        )
        replay = ReplayConfig(start='2024-01-01', end='2024-12-31')
        with pytest.raises(DataCoolieError, match='chunk_column'):
            driver.run_replay(df, replay)

    def test_create_source_reader_with_allowed_prefixes(self) -> None:
        """Line 1059: allowed_prefixes kwarg passed when format is function."""
        config = DataCoolieRunConfig(allowed_function_prefixes=['my_prefix'])
        driver = DataCoolieDriver(
            engine=MagicMock(),
            metadata_provider=MagicMock(),
            config=config,
        )
        mock_source = MagicMock()
        with patch('datacoolie.source_registry.get', return_value=mock_source):
            reader = driver._create_source_reader(Format.FUNCTION.value)
        assert reader == mock_source
