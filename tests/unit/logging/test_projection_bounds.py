"""Projection must enforce bounds before metadata/codec copies are made."""

import json
from datetime import datetime, timezone
from decimal import Decimal
import tracemalloc
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging.persistence.projection import build_dataflow_entry, flatten_dataflow_runtime
from datacoolie.orchestration.runtime import JobRuntimeInfo
from datacoolie.sources.contracts import SourceReadRequest
from tests.unit.logging.support import make_dataflow, make_real_logger, make_runtime


@pytest.mark.parametrize("field", ["configure", "query", "watermark"])
def test_huge_authoritative_input_is_rejected_before_copy(field):
    huge = "🧭" * 500_000
    dataflow, runtime = make_dataflow(), make_runtime()
    if field == "configure":
        dataflow.configure = {"huge": huge}
    elif field == "query":
        dataflow.source.query = huge
    else:
        runtime.read_request = SourceReadRequest(reference_time=datetime.now(timezone.utc), watermark_start={"huge": huge})
    tracemalloc.start()
    try:
        with pytest.raises(ValueError):
            build_dataflow_entry(dataflow, runtime, job_info=JobRuntimeInfo(), max_record_bytes=4096)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 512_000


def test_metadata_screen_precedes_model_dump():
    dataflow = make_dataflow()
    dataflow.transform.additional_columns = []
    dataflow.configure = {"huge": "a" * 1_000_000}
    with patch.object(dataflow, "model_dump", side_effect=AssertionError("eager copy")):
        with pytest.raises(ValueError):
            build_dataflow_entry(dataflow, make_runtime(), job_info=JobRuntimeInfo(), max_record_bytes=1024)


def test_custom_dataclass_metadata_never_calls_unbounded_string_hook():
    @dataclass
    class CustomValue:
        field: int = 1

        def __str__(self):
            raise AssertionError("custom hook must not run")

    dataflow = make_dataflow()
    dataflow.configure = {"hook": CustomValue()}
    with pytest.raises(TypeError, match="unsupported authoritative"):
        build_dataflow_entry(dataflow, make_runtime(), job_info=JobRuntimeInfo(), max_record_bytes=4096)


@pytest.mark.parametrize("kind", ["bytes", "decimal", "datetime"])
def test_supported_scalar_subclasses_use_builtin_representation(kind):
    class BytesHook(bytes):
        def __str__(self):
            raise AssertionError("must not call subclass hook")

    class DecimalHook(Decimal):
        def __str__(self):
            raise AssertionError("must not call subclass hook")

    class DateTimeHook(datetime):
        def isoformat(self, *args, **kwargs):
            raise AssertionError("must not call subclass hook")

    values = {"bytes": BytesHook(b"safe"), "decimal": DecimalHook("1E+900000"), "datetime": DateTimeHook(2026, 10, 4)}
    dataflow = make_dataflow()
    dataflow.configure = {"safe": values[kind]}
    record = build_dataflow_entry(dataflow, make_runtime(), job_info=JobRuntimeInfo(), max_record_bytes=4096)
    expected = {"bytes": "b'safe'", "decimal": "1E+900000", "datetime": "2026-10-04T00:00:00"}
    assert json.loads(record["configure"])["safe"] == expected[kind]


def test_root_and_attempt_messages_share_one_budget():
    runtime = make_runtime()
    runtime.message = "🧭" * 2000
    runtime.attempts *= 20
    for attempt in runtime.attempts:
        attempt.message = "retry" * 2000
        attempt.source.message = "source" * 2000
    record = flatten_dataflow_runtime(runtime, diagnostic_max_bytes=128)
    assert len(json.dumps(record["message"], ensure_ascii=False).encode()) <= 128
    diagnostic_values = [record["message"]]
    for attempt in record["attempts"]:
        if "message" in attempt:
            diagnostic_values.append(attempt["message"])
        for phase in ("source", "transform", "destination"):
            value = attempt[phase]
            if value is not None:
                diagnostic_values.extend(value[key] for key in ("message", "source_action", "transformers_applied", "operation_details") if key in value)
    assert sum(len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()) for value in diagnostic_values) <= 128
    assert record["diagnostic_issues"][-1] == {"field": "", "reason": "omitted"}
    assert len(record["diagnostic_issues"]) == 16


def test_oversized_run_attributes_do_not_prevent_business_accounting(tmp_path):
    logger, _ = make_real_logger(tmp_path, flush_interval_seconds=0, max_record_bytes=4096)
    logger.set_run_config(DataCoolieRunConfig(run_attributes={"huge": "a" * 1_000_000}))
    logger.activate()
    logger.log(make_dataflow(), make_runtime())
    assert logger.job_runtime.total_succeeded == 1
    assert logger.job_runtime.log_records_dropped == 0
    assert logger._job_summary_dirty
    assert not list(tmp_path.rglob("job_*.json"))
    logger.close()
