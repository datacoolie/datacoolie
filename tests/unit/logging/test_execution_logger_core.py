"""Core ExecutionLogger projection, terminal boundary and aggregation tests."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import DataFlowStatus, ExecutionType
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.core.models.runtime import DataFlowRuntimeInfo, DestinationRuntimeInfo
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.runtime.manager import LogManager
from datacoolie.logging.execution_logger import ExecutionLogger, create_execution_logger

from tests.unit.logging.support import (
    make_dataflow,
    make_logger,
    make_maintenance_runtime,
    make_runtime,
    make_transform_dataflow,
)


class TestExecutionLoggerProjection:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_build_entry_preserves_metadata_and_actual_action_query(self):
        logger, _ = make_logger()
        dataflow = make_dataflow()
        dataflow.source.query = "sql/orders/incremental.sql"
        runtime = make_runtime()
        runtime.source.source_action = {
            "query": "SELECT * FROM orders WHERE id > 10",
        }

        entry = logger._build_entry(dataflow, runtime)

        assert entry["source_query"] == "sql/orders/incremental.sql"
        assert json.loads(entry["source_action"])["query"].startswith("SELECT")
        assert entry["source_rows_read"] == 100
        assert entry["destination_rows_written"] == 100
        logger.close()

    def test_build_entry_projects_typed_transform_metadata(self):
        logger, _ = make_logger()
        dataflow = make_transform_dataflow()
        entry = logger._build_entry(dataflow, make_runtime(dataflow.dataflow_id))

        assert json.loads(entry["transform_select_columns"]) == ["customer_id", "email"]
        assert json.loads(entry["transform_rename_columns"]) == {"email": "contact_email"}
        assert json.loads(entry["transform_value_rules"])[0]["mapping"] == {
            "A": "active",
            "I": "inactive",
        }
        assert json.loads(entry["transform_hash_columns"])[0]["target_column"] == "business_hash"
        assert json.loads(entry["transform_masking_rules"])[0]["value"] == "[PRIVATE]"
        assert "transform_missing_column_policy" not in entry
        logger.close()

    def test_terminal_boundary_rejects_running_and_malformed_input(self):
        logger, _ = make_logger()
        with pytest.raises(ConfigurationError, match="terminal"):
            logger.log(make_dataflow(), make_runtime(status=DataFlowStatus.RUNNING.value))
        with pytest.raises(ConfigurationError):
            logger.log(make_dataflow(), object())
        logger.close()

    def test_log_updates_job_aggregation_but_does_not_invent_terminal_status(self):
        logger, _ = make_logger(output_path=None)
        logger.activate()
        logger.log(make_dataflow("a"), make_runtime("a", rows_read=50, rows_written=50))
        logger.log(
            make_dataflow("b"),
            make_runtime("b", status=DataFlowStatus.FAILED.value, rows_read=20, rows_written=0),
        )
        logger.log(
            make_dataflow("c"),
            make_runtime("c", status=DataFlowStatus.SKIPPED.value, rows_read=0, rows_written=0),
        )

        summary = logger._build_job_summary()
        assert summary["status"] == DataFlowStatus.RUNNING.value
        assert summary["total_dataflows"] == 3
        assert summary["total_succeeded"] == 1
        assert summary["total_failed"] == 1
        assert summary["total_skipped"] == 1
        assert summary["total_rows_read"] == 70
        assert summary["total_rows_written"] == 50
        logger.finish_job(DataFlowStatus.FAILED.value)
        assert logger.job_runtime.status == DataFlowStatus.FAILED.value
        logger.close()

    def test_failed_job_summary_contains_distinct_dataflow_labels(self):
        logger, _ = make_logger(output_path=None)
        logger.activate()

        first = make_dataflow("orders")
        first.name = "Orders"
        second = make_dataflow("customers")
        second.name = "Customers"
        logger.log(first, make_runtime("orders", status=DataFlowStatus.FAILED.value))
        logger.log(second, make_runtime("customers", status=DataFlowStatus.FAILED.value))

        assert logger.job_runtime.message == "Orders [orders]; Customers [customers]"
        logger.close()

    def test_maintenance_rows_do_not_count_as_business_rows(self):
        logger, _ = make_logger(output_path=None)
        logger.activate()
        logger.log(make_dataflow(), make_maintenance_runtime())
        summary = logger._build_job_summary()

        assert summary["total_dataflows"] == 1
        assert summary["total_succeeded"] == 1
        assert summary["total_rows_read"] == 0
        assert summary["total_rows_written"] == 0
        assert summary["total_files_added"] == 1
        assert summary["total_bytes_removed"] == 500
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

    def test_run_attributes_are_serialized_once_in_job_runtime(self):
        logger, _ = make_logger(output_path=None)
        logger.set_run_config(
            DataCoolieRunConfig(
                job_id="j1",
                run_attributes={"factory_job_id": "glue-7", "nested": {"attempt": 2}},
            )
        )
        logger.activate()
        summary = logger._build_job_summary()
        assert summary["run_attributes"] == '{"factory_job_id":"glue-7","nested":{"attempt":2}}'
        logger.close()

    def test_finish_job_is_idempotent_but_conflicts_are_rejected(self):
        logger, _ = make_logger(output_path=None)
        logger.activate()
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        first_end = logger.job_runtime.end_time
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        assert logger.job_runtime.end_time == first_end
        with pytest.raises(ConfigurationError, match="finalized"):
            logger.finish_job(DataFlowStatus.FAILED.value)
        logger.close()

    def test_message_is_bounded_and_marks_truncation(self):
        logger, _ = make_logger(output_path=None)
        logger.activate()
        runtime = make_runtime(status=DataFlowStatus.FAILED.value)
        dataflow = make_dataflow()
        dataflow.name = "x" * 20_000
        logger.log(dataflow, runtime)
        summary = logger._build_job_summary()
        assert len(summary["message"].encode("utf-8")) <= 16 * 1024
        assert summary["message_truncated"] is True
        logger.close()

    def test_append_failure_keeps_aggregate_dirty_for_a_later_checkpoint(self):
        logger, _platform = make_logger()
        logger.activate()
        writer = logger._dataflow_writer
        assert writer is not None
        writer.append = MagicMock(side_effect=RuntimeError("detail sink unavailable"))

        with pytest.raises(RuntimeError, match="detail sink unavailable"):
            logger.log(make_dataflow("accepted"), make_runtime("accepted"))

        summary = logger._build_job_summary()
        assert summary["total_dataflows"] == 1
        assert summary["total_succeeded"] == 1
        assert logger._job_summary_dirty is True
        assert logger._job_summary_revision == 1

        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

    def test_flatten_runtime_serializes_operation_details(self):
        runtime = DataFlowRuntimeInfo(
            dataflow_id="df-1",
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type=ExecutionType.MAINTENANCE.value,
            destination=DestinationRuntimeInfo(
                status=DataFlowStatus.SUCCEEDED.value,
                operation_type=ExecutionType.MAINTENANCE.value,
                operation_details=[{"operation": "OPTIMIZE", "removed": 3}],
            ),
        )
        flat = ExecutionLogger._flatten_dataflow_runtime(runtime)
        assert json.loads(flat["destination_operation_details"])[0]["removed"] == 3


class TestExecutionLoggerLifecycle:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_factory_and_no_output_mode(self):
        logger = create_execution_logger(output_path="/logs", platform=MagicMock())
        assert isinstance(logger, ExecutionLogger)
        assert logger.config.output_path == "/logs"
        logger.close()

        logger = ExecutionLogger(LogConfig(output_path=None), platform=MagicMock())
        logger.activate()
        logger.log(make_dataflow(), make_runtime())
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()
        assert logger.terminal_outcomes == ()

    def test_factory_validates_override_without_mutating_input(self):
        configured = LogConfig()

        with pytest.raises(ValueError, match="output_path"):
            create_execution_logger(output_path=" ", config=configured)

        assert configured.output_path is None

    def test_concurrent_terminal_observations_update_exact_counters(self):
        logger = ExecutionLogger(LogConfig(output_path=None, flush_interval_seconds=0), platform=None)
        logger.activate()
        dataflow = make_dataflow("concurrent")
        runtime = make_runtime("concurrent", rows_read=1, rows_written=1)
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(lambda _: logger.log(dataflow, runtime), range(300)))

        summary = logger._build_job_summary()
        assert summary["total_dataflows"] == 300
        assert summary["total_succeeded"] == 300
        assert summary["total_rows_read"] == 300
        assert summary["total_rows_written"] == 300
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()
