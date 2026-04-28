"""Core ETLLogger behavior tests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.constants import DataFlowStatus, ExecutionType, LogPurpose
from datacoolie.core.models import DataCoolieRunConfig, DataFlowRuntimeInfo, DestinationRuntimeInfo
from datacoolie.logging.base import LogConfig, LogManager
from datacoolie.logging.etl_logger import (
    ETLLogger,
    _build_dataflow_schema,
    _build_job_summary_schema,
    create_etl_logger,
)

from tests.unit.logging.support import (
    make_dataflow,
    make_logger,
    make_maintenance_runtime,
    make_runtime,
)


class TestETLLoggerCore:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_log_etl_entry_has_operation_type_etl(self):
        logger, _ = make_logger()
        logger.log(make_dataflow(), make_runtime())
        assert len(logger._runtime_logs) == 1
        entry = logger._runtime_logs[0]
        assert entry["_type"] == "dataflow_run_log"
        # ETL runs may keep operation_type unset; only maintenance is explicit.
        assert entry["operation_type"] is None
        assert entry["source_rows_read"] == 100
        assert entry["destination_rows_written"] == 100
        logger.close()

    def test_log_maintenance_entry_has_maintenance_metrics(self):
        logger, _ = make_logger()
        logger.log(make_dataflow(), make_maintenance_runtime())
        entry = logger._runtime_logs[0]
        assert entry["operation_type"] == ExecutionType.MAINTENANCE.value
        assert entry["destination_files_added"] == 1
        assert entry["destination_bytes_removed"] == 500
        logger.close()

    def test_log_after_close_is_ignored(self):
        logger, _ = make_logger()
        logger.close()
        logger.log(make_dataflow(), make_runtime())
        assert logger._runtime_logs == []

    def test_set_run_config_updates_job_info(self):
        logger, _ = make_logger()
        rc = DataCoolieRunConfig(job_id="new-job", retry_count=3, max_workers=4)
        logger.set_run_config(rc)
        assert logger._job_info.job_id == "new-job"
        assert logger._job_info.retry_count == 3
        assert logger._job_info.max_workers == 4
        logger.close()

    def test_set_component_names_applies_to_job_info(self):
        logger, _ = make_logger()
        logger.set_component_names(
            engine_name="spark",
            platform_name="local",
            metadata_provider_name="sqlite",
            watermark_manager_name="wmgr",
        )
        assert logger._job_info.engine_name == "spark"
        assert logger._job_info.platform_name == "local"
        assert logger._job_info.metadata_provider_name == "sqlite"
        assert logger._job_info.watermark_manager_name == "wmgr"
        logger.close()


class TestJobSummaryAndFlattening:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_job_summary_aggregates_counts_and_rows(self):
        logger, _ = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a", rows_read=50, rows_written=50))
        logger.log(make_dataflow("b"), make_runtime("b", status=DataFlowStatus.FAILED.value, rows_read=20, rows_written=0))
        logger.log(make_dataflow("c"), make_runtime("c", status=DataFlowStatus.SKIPPED.value, rows_read=0, rows_written=0))

        summary = logger._build_job_summary()
        assert summary["total_dataflows"] == 3
        assert summary["total_succeeded"] == 1
        assert summary["total_failed"] == 1
        assert summary["total_skipped"] == 1
        assert summary["total_rows_read"] == 70
        assert summary["total_rows_written"] == 50
        assert "runtime_logs" not in summary
        logger.close()

    @pytest.mark.parametrize(
        "statuses,expected",
        [
            ([DataFlowStatus.SUCCEEDED.value], DataFlowStatus.SUCCEEDED.value),
            ([DataFlowStatus.SKIPPED.value], DataFlowStatus.SKIPPED.value),
            ([DataFlowStatus.SUCCEEDED.value, DataFlowStatus.FAILED.value], DataFlowStatus.FAILED.value),
        ],
    )
    def test_job_summary_status_derivation(self, statuses, expected):
        logger, _ = make_logger()
        for i, status in enumerate(statuses):
            logger.log(make_dataflow(f"df-{i}"), make_runtime(f"df-{i}", status=status))
        summary = logger._build_job_summary()
        assert summary["status"] == expected
        logger.close()

    def test_flatten_runtime_maintenance_serializes_operation_details(self):
        runtime = DataFlowRuntimeInfo(
            dataflow_id="df-1",
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type=ExecutionType.MAINTENANCE.value,
            destination=DestinationRuntimeInfo(
                status=DataFlowStatus.SUCCEEDED.value,
                operation_type="maintenance",
                operation_details=[{"operation": "OPTIMIZE", "removed": 3}],
            ),
        )
        flat = ETLLogger._flatten_dataflow_runtime(runtime)
        details = json.loads(flat["destination_operation_details"])
        assert details[0]["operation"] == "OPTIMIZE"
        assert details[0]["removed"] == 3

    def test_partition_path_uses_debug_json_folder(self):
        logger, _ = make_logger()
        path = logger._partition_path(LogPurpose.DEBUG.value, "job_run_log", make_runtime().start_time)
        assert "debug_json" in path
        logger.close()


class TestLifecycleAndFactory:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_cleanup_clears_state_and_reapplies_config(self):
        logger, _ = make_logger()
        logger.set_component_names(engine_name="spark")
        logger.log(make_dataflow(), make_runtime())
        assert len(logger._runtime_logs) == 1

        logger._cleanup()
        assert logger._runtime_logs == []
        assert logger._job_info.job_id == "j1"
        assert logger._job_info.engine_name == "spark"

    def test_factory_returns_logger(self):
        logger = create_etl_logger(output_path="/logs", platform=MagicMock())
        assert isinstance(logger, ETLLogger)
        assert logger.config.output_path == "/logs"
        logger.close()

    def test_flush_noop_when_missing_requirements(self):
        logger = ETLLogger(LogConfig(output_path=None), platform=MagicMock())
        logger.log(make_dataflow(), make_runtime())
        logger.flush()  # no-op
        logger.close()


# ============================================================================
# Additional edge cases (merged from test_etl_logger_edge_cases.py)
# ============================================================================


class TestSchemaBuilderImportErrorPaths:
    def test_build_dataflow_schema_returns_none_when_pyarrow_missing(self):
        real_import = __import__

        def _import(name, *args, **kwargs):
            if name == "pyarrow":
                raise ImportError("missing")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_import):
            assert _build_dataflow_schema() is None

    def test_build_job_summary_schema_returns_none_when_pyarrow_missing(self):
        real_import = __import__

        def _import(name, *args, **kwargs):
            if name == "pyarrow":
                raise ImportError("missing")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_import):
            assert _build_job_summary_schema() is None


class TestAggregationEdgeCases:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_update_job_runtime_skipped_and_maintenance_rows_not_counted(self):
        logger, _ = make_logger()

        logger.log(make_dataflow("m"), make_maintenance_runtime("m", status=DataFlowStatus.SKIPPED.value))
        summary = logger._build_job_summary()

        assert summary["total_skipped"] == 1
        assert summary["total_rows_read"] == 0
        assert summary["total_rows_written"] == 0
        assert summary["status"] == DataFlowStatus.SKIPPED.value
        logger.close()

    def test_update_job_runtime_unknown_status_still_updates_rows(self):
        logger, _ = make_logger()
        rt = make_runtime("u")
        rt.status = "running"
        logger.log(make_dataflow("u"), rt)
        summary = logger._build_job_summary()
        assert summary["total_dataflows"] == 1
        assert summary["total_rows_read"] == 100
        assert summary["total_rows_written"] == 100
        logger.close()

    def test_build_job_summary_keeps_pending_when_no_counts(self):
        logger, _ = make_logger()
        summary = logger._build_job_summary()
        assert summary["status"] == DataFlowStatus.PENDING.value
        logger.close()
