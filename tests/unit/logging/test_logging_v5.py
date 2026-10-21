"""Historical schema, hashes, and exact-value checks for logging v5."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

from jsonschema import Draft202012Validator

from datacoolie.watermark.base import WatermarkSerializer

FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "logging" / "v5"


def _manifest() -> dict:
    return json.loads((FIXTURE_ROOT / "manifest.json").read_text(encoding="utf-8"))


def _rows(filename: str) -> list[dict]:
    return [
        json.loads(line)
        for line in (FIXTURE_ROOT / filename).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_checked_in_producer_fixtures_match_manifest_and_schema():
    manifest = _manifest()
    assert manifest["schema_version"] == 5
    assert manifest["schema_file"] == "schema.json"
    schema = json.loads((FIXTURE_ROOT / manifest["schema_file"]).read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for filename, stream in manifest["streams"].items():
        path = FIXTURE_ROOT / filename
        payload = path.read_bytes()
        assert hashlib.sha256(payload).hexdigest() == stream["sha256"]
        rows = _rows(filename)
        assert len(rows) == stream["rows"]
        for row in rows:
            errors = sorted(validator.iter_errors(row), key=lambda error: list(error.path))
            assert not errors, [(list(error.path), error.message) for error in errors]


def test_additional_terminal_fixtures_cover_execution_boundaries():
    rows = {row["dataflow_run_id"]: row for row in _rows("dataflow_part_00000002.json")}
    retry = rows["fixture-v5-retry-success-run"]
    assert [attempt["status"] for attempt in retry["attempts"]] == ["failed", "succeeded"]
    assert retry["retry_attempts"] == 1
    empty = rows["fixture-v5-transformed-empty-run"]
    assert empty["attempts"][-1]["destination"]["rows_written"] == 0
    assert empty["attempts"][-1]["source"]["rows_read"] > 0
    assert rows["fixture-v5-capture-failure-run"]["status"] == "succeeded"
    assert rows["fixture-v5-adapter-raise-run"]["status"] == "failed"
    assert rows["fixture-v5-sparse-metrics-run"]["metrics"]["bytes_added"] is None
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
        "payload": b"v5\x00\xff",
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
    assert expected_start["payload"] == b"v5\x00\xff"
    assert expected_end["event_date"] == date(2026, 10, 6)


def test_v5_runtime_is_native_allowlisted_and_preserves_optional_nulls():
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
