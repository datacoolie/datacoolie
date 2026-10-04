"""Producer, schema, typed-value, and current-producer checks for logging v6."""

from __future__ import annotations

import hashlib
import json
import warnings
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from datacoolie.core.exceptions import WatermarkError
from datacoolie.watermark.base import WatermarkSerializer
from datacoolie.logging.persistence.serialization import serialize_source_runtime
from datacoolie.sources.runtime import SourceRuntimeInfo
from tests.fixtures.logging.v6.generator import generate

FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "logging" / "v6"


def _manifest() -> dict:
    return json.loads((FIXTURE_ROOT / "manifest.json").read_text(encoding="utf-8"))


def _rows(filename: str) -> list[dict]:
    return [
        json.loads(line)
        for line in (FIXTURE_ROOT / filename).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _validator() -> Draft202012Validator:
    schema = json.loads((FIXTURE_ROOT / _manifest()["schema_file"]).read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def test_checked_in_producer_fixtures_match_manifest_and_schema():
    manifest = _manifest()
    assert manifest["schema_version"] == 6
    assert manifest["schema_file"] == "schema.json"
    schema = json.loads((FIXTURE_ROOT / manifest["schema_file"]).read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    for filename, stream in manifest["streams"].items():
        path = FIXTURE_ROOT / filename
        payload = path.read_bytes()
        assert hashlib.sha256(payload).hexdigest() == stream["sha256"]
        rows = _rows(filename)
        assert len(rows) == stream["rows"]
        for row in rows:
            errors = sorted(validator.iter_errors(row), key=lambda error: list(error.path))
            assert not errors, [(list(error.path), error.message) for error in errors]


def test_fixture_generator_is_deterministic_and_uses_actual_current_producer(tmp_path):
    generate(tmp_path)
    manifest = _manifest()
    expected_files = set(manifest["streams"]) | {"manifest.json"}
    assert {path.name for path in tmp_path.iterdir()} == expected_files
    for filename in expected_files:
        assert (tmp_path / filename).read_bytes() == (
            FIXTURE_ROOT / filename
        ).read_bytes()


def test_v6_schema_enforces_identity_terminal_status_and_root_nested_keys():
    validator = _validator()
    row = _rows("dataflow_snapshot.json")[0]

    missing_id = dict(row)
    missing_id.pop("dataflow_run_id")
    assert list(validator.iter_errors(missing_id))

    empty_id = dict(row, dataflow_run_id="")
    assert list(validator.iter_errors(empty_id))

    for status in ("pending", "running"):
        assert list(validator.iter_errors(dict(row, status=status)))

    root_metadata = dict(row)
    root_metadata["read_request"] = {
        "watermark_start": {"__date_folder_partition__": "2026-10-04"},
        "watermark_end": None,
        "watermark_start_operator": None,
        "watermark_end_operator": None,
        "read_range": None,
        "reference_time": "2026-10-04T12:00:00+00:00",
    }
    assert not list(validator.iter_errors(root_metadata))

    nested_metadata = dict(root_metadata)
    nested_metadata["read_request"] = dict(root_metadata["read_request"])
    nested_metadata["read_request"]["watermark_start"] = {
        "": {"__date_folder_partition__": "2026-10-04"}
    }
    assert list(validator.iter_errors(nested_metadata))

    empty_keys = dict(root_metadata)
    empty_keys["read_request"] = dict(root_metadata["read_request"])
    empty_keys["read_request"]["watermark_start"] = {
        "": {"": {"__date__": "2026-10-04"}}
    }
    assert not list(validator.iter_errors(empty_keys))

    newline_keys = dict(root_metadata)
    newline_keys["read_request"] = dict(root_metadata["read_request"])
    newline_keys["read_request"]["watermark_start"] = {
        "__a\nb__": 1,
    }
    assert list(validator.iter_errors(newline_keys))

    newline_suffix = dict(root_metadata)
    newline_suffix["read_request"] = dict(root_metadata["read_request"])
    newline_suffix["read_request"]["watermark_start"] = {
        "__a__\n": 1,
    }
    assert not list(validator.iter_errors(newline_suffix))


def test_v6_diagnostic_issue_inventory_is_bounded_and_root_owned():
    validator = _validator()
    row = _rows("dataflow_snapshot.json")[0]
    valid_issues = [
        {"field": "/attempts/0/message", "reason": "truncated"},
        {"field": "", "reason": "omitted"},
    ]
    valid = dict(row, diagnostic_issues=valid_issues)
    assert not list(validator.iter_errors(valid))
    assert list(validator.iter_errors(dict(valid, diagnostic_issues=valid_issues * 9)))
    assert list(validator.iter_errors(dict(valid, diagnostic_issues=[
        {"field": "x" * 193, "reason": "invalid"}
    ])))
    assert list(validator.iter_errors(dict(valid, diagnostic_issues=[
        {"field": "/message", "reason": "nodes"}
    ])))

    job = _rows("job_snapshot.json")[0]
    assert list(validator.iter_errors(dict(job, diagnostic_issues=valid_issues)))


def test_additional_terminal_fixtures_cover_execution_boundaries():
    rows = {row["dataflow_run_id"]: row for row in _rows("dataflow_part_00000002.json")}
    retry = rows["fixture-v6-retry-success-run"]
    assert [attempt["status"] for attempt in retry["attempts"]] == ["failed", "succeeded"]
    assert retry["retry_attempts"] == 1
    empty = rows["fixture-v6-transformed-empty-run"]
    assert empty["attempts"][-1]["destination"]["rows_written"] == 0
    assert empty["attempts"][-1]["source"]["rows_read"] > 0
    assert rows["fixture-v6-capture-failure-run"]["status"] == "succeeded"
    assert rows["fixture-v6-adapter-raise-run"]["status"] == "failed"
    assert rows["fixture-v6-sparse-metrics-run"]["metrics"]["bytes_added"] is None
    assert rows["inactive-run"]["attempts"] == []
    assert rows["preparation-run"]["attempts"] == []
    assert {rows[run_id]["status"] for run_id in ("chunk-success", "chunk-skipped", "chunk-failed")} == {
        "succeeded", "skipped", "failed"
    }
    job = _rows("job_bounded_snapshot.json")[0]
    assert job["total_dataflows"] == 1
    assert job["total_failed"] == 1
    assert job["log_records_dropped"] == 1
    assert job["log_bytes_dropped"] > 0


def test_malformed_diagnostic_omission_survives_warning_as_error():
    source = SourceRuntimeInfo(rows_read=2, source_action={"invalid": float("nan")})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        encoded = serialize_source_runtime(source)
    assert encoded["rows_read"] == 2
    assert "source_action" not in encoded


def test_typed_watermark_values_and_scalar_range_bounds_decode_exactly():
    row = _rows("dataflow_snapshot.json")[0]
    request = row["read_request"]
    assert request["watermark_start"] is None
    assert request["watermark_end"] is None
    assert request["reference_time"] == "2026-10-04T12:00:00+00:00"

    read_range = request["read_range"]
    assert read_range["start"] == {
        "__datetime__": "2026-10-01T00:00:00+00:00"
    }
    assert read_range["end"] == {
        "__datetime__": "2026-10-04T00:00:00+00:00"
    }
    assert WatermarkSerializer.deserialize(
        json.dumps({"value": read_range["start"]})
    )["value"] == datetime(2026, 10, 1, tzinfo=timezone.utc)

    source_selection = row["attempts"][0]["source"]["selection"]
    assert source_selection["lower"] == {"event_ts": read_range["start"]}
    assert source_selection["upper"] == {"event_ts": read_range["end"]}
    assert source_selection["upper_origin"] == "explicit"

    watermark = row["attempts"][0]["watermark"]
    expected = {
        "event_date": date(2026, 10, 4),
        "event_time": time(12, 34, 56, 789000),
        "event_ts": datetime(2026, 10, 4, 12, 34, 56, tzinfo=timezone.utc),
        "amount": Decimal("12.50"),
        "payload": b"v6\x00\xff",
        "nested": {"observed": date(2026, 10, 4)},
        "items": [date(2026, 10, 4), Decimal("0.25")],
    }
    for field in (
        "loaded_watermark",
        "candidate",
        "watermark_to_save",
        "saved_watermark",
    ):
        encoded = watermark[field]["values"] if field == "candidate" else watermark[field]
        decoded = WatermarkSerializer.deserialize(json.dumps(encoded))
        assert decoded == expected
    assert watermark["candidate"]["kind"] == "observed_max"
    assert watermark["merge_base"] == {"event_date": {"__date__": "2026-10-04"}}

    skipped = _rows("dataflow_snapshot.json")[1]
    expected_start = WatermarkSerializer.deserialize(
        json.dumps(skipped["read_request"]["watermark_start"])
    )
    expected_end = WatermarkSerializer.deserialize(
        json.dumps(skipped["read_request"]["watermark_end"])
    )
    assert expected_start["event_date"] == date(2026, 10, 5)
    assert expected_start["event_ts"] == datetime(
        2026, 10, 5, 12, 34, 56, tzinfo=timezone.utc
    )
    assert expected_start["event_time"] == time(12, 34, 56, 789000)
    assert expected_start["amount"] == Decimal("12.50")
    assert expected_start["payload"] == b"v6\x00\xff"
    assert expected_end["event_date"] == date(2026, 10, 6)


def test_invalid_typed_tags_are_rejected_by_schema_or_codec():
    validator = _validator()
    row = _rows("dataflow_snapshot.json")[0]
    invalid_time = dict(row)
    invalid_time["read_request"] = dict(row["read_request"])
    invalid_time["read_request"]["read_range"] = {
        "column": "event_ts",
        "start": {"__time__": "25:00:00"},
        "end": {"__time__": "12:00:00"},
        "lower_operator": ">=",
        "upper_operator": "<",
    }
    assert list(validator.iter_errors(invalid_time))

    for value in (
        {"__date__": "2026-02-30"},
        {"__datetime__": "not-a-date"},
        {"__time__": "25:00:00"},
    ):
        with pytest.raises(WatermarkError):
            WatermarkSerializer.deserialize(json.dumps({"value": value}))


def test_v6_runtime_is_native_allowlisted_and_preserves_optional_nulls():
    success, skipped, empty = _rows("dataflow_snapshot.json")
    failed = _rows("dataflow_part_00000001.json")[0]
    assert isinstance(success["attempts"], list)
    assert isinstance(success["read_request"], dict)
    assert isinstance(success["metrics"], dict)
    assert success["attempts"][0]["source"]["source_action"] == {
        "query": "SELECT * FROM fixture",
        "observed": "3.14",
    }
    assert isinstance(skipped["read_request"], dict)
    assert skipped["read_request"]["read_range"] is None
    assert skipped["attempts"][0]["transform"] is None
    assert skipped["attempts"][0]["watermark"] is None
    assert empty["attempts"] == []
    assert failed["attempts"][1]["source"]["rows_read"] is None
    assert failed["attempts"][1]["destination"]["rows_written"] is None
    assert failed["metrics"]["rows_read"] is None
    assert failed["metrics"]["rows_written"] is None
    assert failed["metrics"]["bytes_added"] is None

    forbidden_root = {
        "overhead_duration_seconds",
        "source_start_time",
        "source_rows_read",
        "source_watermark_before",
        "source_read_range",
        "transform_start_time",
        "destination_start_time",
        "watermark_checkpoint_status",
    }
    assert not forbidden_root & set(success)

    def all_keys(value):
        if isinstance(value, dict):
            return set(value) | {
                key for child in value.values() for key in all_keys(child)
            }
        if isinstance(value, list):
            return {key for child in value for key in all_keys(child)}
        return set()

    assert not {"__dataclass__", "frame", "frames"} & all_keys(success)
