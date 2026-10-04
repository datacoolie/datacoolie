"""Loss accounting and immutable snapshot retry are distinct boundaries."""

from dataclasses import fields
from unittest.mock import patch

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import FlushResult
from datacoolie.logging.execution_logger import (
    ExecutionLogger, _JOB_METRIC_EXCLUSIONS, _METRIC_TO_TOTAL,
)
from datacoolie.logging.persistence.serialization import serialize_metrics
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.orchestration.execution.runtime import ExecutionMetrics
from tests.unit.logging.support import make_dataflow, make_real_logger, make_runtime


def test_metric_inventory_maps_every_counter_or_explicitly_excludes_it():
    assert {f.name for f in fields(ExecutionMetrics)} == set(_METRIC_TO_TOTAL) | _JOB_METRIC_EXCLUSIONS
    assert _JOB_METRIC_EXCLUSIONS == {"bytes_saved"}
    assert set(serialize_metrics(ExecutionMetrics())) == {f.name for f in fields(ExecutionMetrics)}


def test_metric_producer_ownership_matches_inventory():
    runtime = make_runtime()
    runtime.final_attempt.source = SourceRuntimeInfo(rows_read=11)
    values = {field.name: index + 1 for index, field in enumerate(fields(ExecutionMetrics)) if field.name != "rows_read"}
    runtime.final_attempt.destination = DestinationRuntimeInfo(**{name: value for name, value in values.items() if name != "bytes_saved"})
    values["bytes_saved"] = max(0, values["bytes_removed"] - values["bytes_added"])
    metrics = ExecutionMetrics.from_attempt(runtime.final_attempt)
    assert metrics.rows_read == 11
    assert {name: getattr(metrics, name) for name in values} == values


@pytest.mark.parametrize("fault", ["projection", "encoding", "admission"])
def test_detail_rejection_counts_once_and_keeps_business_totals(tmp_path, fault):
    logger, _ = make_real_logger(tmp_path, flush_interval_seconds=0)
    logger.activate()
    target = logger if fault == "projection" else logger._dataflow_writer
    name = "_build_entry" if fault == "projection" else "append"
    options = {"return_value": False} if fault == "admission" else {"side_effect": ValueError("failure")}
    with patch.object(target, name, **options):
        logger.log(make_dataflow(), make_runtime(rows_read=7))
    assert logger.job_runtime.total_dataflows == 1
    assert logger.job_runtime.total_rows_read == 7
    assert logger.job_runtime.log_records_dropped == 1
    assert logger.job_runtime.log_bytes_dropped == 0
    logger.close()


def test_sink_disabled_does_not_project_or_count_loss():
    logger = ExecutionLogger(LogConfig(flush_interval_seconds=0))
    logger.activate()
    with patch.object(logger, "_build_entry", side_effect=AssertionError("must not project")):
        logger.log(make_dataflow(), make_runtime())
    assert logger.job_runtime.total_dataflows == 1
    assert logger.job_runtime.log_records_dropped == 0
    logger.close()


def test_invalid_metric_contract_does_not_partially_mutate_summary(tmp_path):
    logger, _ = make_real_logger(tmp_path, flush_interval_seconds=0)
    logger.activate()
    runtime = make_runtime()
    runtime.final_attempt.destination.rows_written = "invalid"
    with pytest.raises(ConfigurationError):
        logger.log(make_dataflow(), runtime)
    assert logger.job_runtime.total_dataflows == 0
    assert logger.job_runtime.total_rows_read == 0
    assert logger.job_runtime.log_records_dropped == 0
    logger.close()


@pytest.mark.parametrize("reject", ["projection", "admission"])
def test_rejected_latest_summary_still_retries_old_payload_without_observation_loss(tmp_path, reject):
    logger, platform = make_real_logger(tmp_path, flush_interval_seconds=0)
    logger.activate()
    logger.log(make_dataflow(), make_runtime())
    with patch.object(platform, "upload_file", side_effect=OSError("upload failed")):
        with pytest.raises(OSError):
            logger._flush_job_snapshot(force=True)
    writer = logger._job_writer
    assert writer.pending
    target, name = (logger, "_job_record") if reject == "projection" else (writer, "replace")
    options = {"side_effect": ValueError("projection failure")} if reject == "projection" else {"return_value": False}
    with patch.object(target, name, **options):
        assert logger._flush_job_snapshot(force=True) is FlushResult.WRITTEN
        assert logger._job_summary_dirty
    assert logger.job_runtime.log_records_dropped == 0
    logger.close()
