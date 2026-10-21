"""Current producer observations remain truthful on the wire."""
import hashlib
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.execution_logger import ExecutionLogger
from datacoolie.logging.persistence.projection import build_dataflow_entry
from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo, DataFlowRuntimeInfo
from datacoolie.orchestration.runtime import JobRuntimeInfo
from datacoolie.sources.runtime import SourceRuntimeInfo


STAMP = datetime(2026, 10, 11, 10, tzinfo=timezone.utc)
SCHEMA = Path(__file__).parents[2] / "fixtures/logging/v7/schema.json"


def test_current_fixture_streams_are_pinned_deterministic_and_schema_valid(tmp_path):
    from tests.fixtures.logging.v7.generator import generate
    generate(tmp_path)
    fixture_root = SCHEMA.parent
    manifest = json.loads((fixture_root / "manifest.json").read_text())
    assert manifest["schema_version"] == 7
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()), format_checker=FormatChecker())
    assert (tmp_path / "manifest.json").read_bytes() == (fixture_root / "manifest.json").read_bytes()
    kinds = set()
    for filename, stream in manifest["streams"].items():
        payload = (tmp_path / filename).read_bytes()
        assert payload == (fixture_root / filename).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == stream["sha256"]
        records = [json.loads(line) for line in payload.splitlines()]
        assert len(records) == stream["rows"]
        for record in records:
            validator.validate(record)
            assert record["log_schema_version"] == 7
            kinds.add(record["_type"])
    assert kinds == {"dataflow_run_log", "job_run_log", "system_log"}


def _flow():
    connection = Connection(name="v7", format="delta", configure={"base_path": "/data"})
    return DataFlow(dataflow_id="observed-v7", source=Source(connection=connection, table="input"),
                    destination=Destination(connection=connection, table="output", load_type="append"))


def _run(*, source=None, destination=None, status="succeeded", operation="etl", attempts=True):
    attempt = AttemptRuntimeInfo(status=status, source=source, destination=destination,
                                 start_time=STAMP, end_time=STAMP)
    return DataFlowRuntimeInfo(dataflow_id="observed-v7", status=status, operation_type=operation,
                               start_time=STAMP, end_time=STAMP, attempts=[attempt] if attempts else [])


def _entry(runtime, **options):
    return build_dataflow_entry(_flow(), runtime,
                               job_info=JobRuntimeInfo(job_id="v7-job"), **options)


@pytest.mark.parametrize("count", [0, 100, 2**80])
def test_measured_integers_and_unknown_fields_are_schema_valid(count):
    entry = _entry(_run(source=SourceRuntimeInfo(rows_read=count, status="succeeded"),
                        destination=DestinationRuntimeInfo(rows_written=count, status="succeeded")))
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()), format_checker=FormatChecker())
    validator.validate(entry)
    decoded = json.loads(json.dumps(entry))
    assert decoded["log_schema_version"] == 7
    assert decoded["metrics"]["rows_read"] == count
    assert decoded["metrics"]["rows_written"] == count
    assert decoded["metrics"]["files_added"] is None
    assert "metric_eligibility" not in decoded


def test_empty_source_and_unentered_pipeline_never_invent_destination_zero():
    entry = _entry(_run(source=SourceRuntimeInfo(rows_read=0, status="succeeded"), status="skipped"))
    assert entry["metrics"]["rows_read"] == 0
    assert entry["metrics"]["rows_written"] is None
    assert "metric_eligibility" not in entry
    unentered = _entry(_run(status="skipped", attempts=False))
    assert all(value is None for value in unentered["metrics"].values())
    assert "metric_eligibility" not in unentered


def test_failed_source_and_preflight_failure_keep_missing_measurements_null():
    failed_read = _entry(_run(source=SourceRuntimeInfo(status="failed"), status="failed"))
    assert failed_read["metrics"]["rows_read"] is None
    assert "metric_eligibility" not in failed_read
    preflight = _entry(_run(status="failed", attempts=False))
    assert all(value is None for value in preflight["metrics"].values())
    assert "metric_eligibility" not in preflight


def test_entered_destination_without_action_context_keeps_measurements_unknown():
    entry = _entry(_run(destination=DestinationRuntimeInfo(status="failed"), status="failed"))
    assert entry["metrics"]["rows_written"] is None
    assert "metric_eligibility" not in entry
    maintenance = _entry(_run(destination=DestinationRuntimeInfo(
        status="succeeded", operation_type="maintenance"), operation="maintenance"))
    assert maintenance["metrics"]["files_added"] is None
    assert "metric_eligibility" not in maintenance


def test_observations_survive_optional_diagnostic_omission():
    phase = DestinationRuntimeInfo(status="succeeded", operation_type="maintenance",
                                   operation_details=[{"reason": "x" * 5000}])
    entry = _entry(_run(destination=phase, operation="maintenance"), diagnostic_max_bytes=128)
    assert "operation_details" not in entry["attempts"][0]["destination"]
    assert entry["metrics"]["files_removed"] is None
    assert "metric_eligibility" not in entry
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()), format_checker=FormatChecker())
    validator.validate(entry)


def test_job_preserves_raw_maintenance_rows_but_excludes_them_from_ingestion():
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    logger = ExecutionLogger(LogConfig(output_path=None))
    logger.set_run_config(DataCoolieRunConfig(job_id="v7-job"))
    logger.activate()
    try:
        etl = _run(destination=DestinationRuntimeInfo(rows_written=100, status="succeeded"))
        maintenance = _run(destination=DestinationRuntimeInfo(rows_written=5, status="succeeded"),
                           operation="maintenance")
        logger.log(_flow(), etl)
        logger.log(_flow(), maintenance)
        assert _entry(maintenance)["metrics"]["rows_written"] == 5
        assert logger.job_runtime.total_rows_written == 100
    finally:
        logger.close()


def test_job_observed_totals_ignore_missing_contributions():
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    logger = ExecutionLogger(LogConfig(output_path=None))
    logger.set_run_config(DataCoolieRunConfig(job_id="v7-job"))
    logger.activate()
    try:
        logger.log(
            _flow(),
            _run(destination=DestinationRuntimeInfo(rows_written=100, status="succeeded")),
        )
        logger.log(
            _flow(),
            _run(destination=DestinationRuntimeInfo(status="succeeded")),
        )
        assert logger.job_runtime.total_rows_written == 100
    finally:
        logger.close()


@pytest.mark.parametrize("entered", [False, True])
def test_replay_reduces_final_attempt_observations_once(entered):
    from datacoolie.core.models.run_config import ReplayConfig
    from datacoolie.orchestration.execution.replay import process_replay
    from datacoolie.orchestration.preparation import PreparedDataFlow
    flow = _flow()
    flow.source.watermark_columns = ["id"]
    known = _run(destination=DestinationRuntimeInfo(status="succeeded", rows_written=100))
    known.attempts.insert(0, AttemptRuntimeInfo(status="failed",
        destination=DestinationRuntimeInfo(status="failed", rows_written=999)))
    other = _run(destination=DestinationRuntimeInfo(status="succeeded") if entered else None)
    chunks = iter([known, other])
    result = process_replay(flow, ReplayConfig(start=1, end=3, chunk_interval={"step": 1}),
        column_name_mode="lower", prepare_execution_dataflow=lambda item, **kwargs:
            PreparedDataFlow(metadata=item.model_copy(deep=True), execution=item.model_copy(deep=True)),
        validate_watermark_storage=lambda *args, **kwargs: None, watermark_manager=None,
        run_single_pipeline=lambda *args, **kwargs: next(chunks),
        log_result=lambda *args: None, on_chunk_complete=lambda *args: None)
    assert result.metrics.rows_written == 100
    assert result.retry_attempts == 1
    assert len(result.chunk_run_ids) == 2
