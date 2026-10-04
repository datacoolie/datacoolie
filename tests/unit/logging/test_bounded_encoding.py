"""Focused checks for bounded JSON encoding and writer admission."""

from __future__ import annotations


import json
import tracemalloc
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.persistence.bounded_encoder import (
    BoundedEncodingError,
    bounded_json_dumps,
    bounded_json_encode,
    bounded_json_size,
)
from datacoolie.logging.persistence.writer import JsonLogWriter, encode_json_line
from datacoolie.utils.converters import custom_json_encoder, json_default


def test_integer_subclass_hooks_do_not_change_native_json_or_allocate():
    class IntHook(int):
        def __str__(self):
            raise AssertionError("integer hook must not run")

        def __repr__(self):
            raise AssertionError("integer hook must not run")

    assert bounded_json_encode({"metric": IntHook(7)}, max_bytes=64) == b'{"metric": 7}'


def test_bounded_encoder_preserves_normal_writer_bytes():
    record = {
        "id": 7,
        "message": 'Unicode é and escaped \\" text',
        "attempts": [{"status": "failed", "duration": 1.25}],
        "when": datetime(2024, 1, 15, 10, 0, 0),
    }
    expected = (
        json.dumps(
            record,
            default=custom_json_encoder,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")

    assert encode_json_line(record) == expected
    assert (
        bounded_json_encode(
            record,
            max_bytes=len(expected),
            default=custom_json_encoder,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            newline=True,
        )
        == expected
    )
    assert bounded_json_size(
        record,
        default=custom_json_encoder,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        newline=True,
    ) == len(expected)


def test_bounded_dumps_preserves_metadata_json_default_options():
    value = {
        "z": Decimal("3.14"),
        "é": datetime(2024, 1, 15, 10, 0, 0),
        "bytes": b"abc",
    }
    expected = json.dumps(value, default=json_default, sort_keys=True)

    assert bounded_json_dumps(
        value,
        max_bytes=len(expected.encode("utf-8")),
        default=json_default,
        sort_keys=True,
    ) == expected


def test_record_limit_includes_newline_and_reports_exact_size():
    record = {"message": "x"}
    payload = encode_json_line(record)

    assert bounded_json_encode(
        record,
        max_bytes=len(payload),
        default=custom_json_encoder,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        newline=True,
    ) == payload
    with pytest.raises(BoundedEncodingError) as error:
        bounded_json_encode(
            record,
            max_bytes=len(payload) - 1,
            default=custom_json_encoder,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            newline=True,
        )
    assert error.value.known_size == len(payload)


def test_oversized_string_is_counted_without_scaling_output_memory():
    value = {"message": "x" * 8_000_000}
    tracemalloc.start()
    try:
        with pytest.raises(BoundedEncodingError) as error:
            bounded_json_encode(
                value,
                max_bytes=1024,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                newline=True,
            )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert error.value.known_size == len(value["message"]) + len('{"message":""}\n')
    # The source string is caller-owned and created before tracing.  The
    # encoder should retain only bounded slices while it counts the value.
    assert peak < 512 * 1024


def test_oversized_container_reports_exact_size_without_payload_materialization():
    value = {"items": list(range(100_000))}
    expected = len(
        (
            json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
    )

    with pytest.raises(BoundedEncodingError) as error:
        bounded_json_encode(
            value,
            max_bytes=1024,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            newline=True,
        )
    assert error.value.known_size == expected


def test_cycle_has_no_fabricated_known_size():
    value: dict[str, object] = {}
    value["self"] = value

    with pytest.raises(ValueError, match="Circular reference detected"):
        bounded_json_encode(value, max_bytes=1024)


class _CapturingPlatform:
    def __init__(self) -> None:
        self.payloads: list[bytes] = []

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        self.payloads.append(Path(local_path).read_bytes())


def _writer(tmp_path, platform, *, max_record_bytes: int) -> JsonLogWriter:
    return JsonLogWriter(
        platform,
        "logs/job.json",
        LogConfig(
            flush_interval_seconds=0,
            spool_directory=str(tmp_path),
            buffer_memory_bytes=4096,
            spool_max_bytes=4096,
        ),
        max_record_bytes=max_record_bytes,
    )


def test_append_rejection_preserves_prior_payload_and_stats(tmp_path):
    platform = _CapturingPlatform()
    writer = _writer(tmp_path, platform, max_record_bytes=64)
    assert writer.append({"id": "old"}) is True
    before = writer.stats

    assert writer.append({"message": "x" * 1_000}) is False
    after = writer.stats
    assert after.pending_records == before.pending_records
    assert after.pending_bytes == before.pending_bytes
    assert after.dropped_records == before.dropped_records + 1
    assert after.dropped_bytes > 0

    writer.flush(force=True)
    assert [json.loads(line)["id"] for line in platform.payloads[0].splitlines()] == [
        "old"
    ]
    writer.close()


def test_replace_rejection_preserves_prior_immutable_snapshot(tmp_path):
    platform = _CapturingPlatform()
    writer = _writer(tmp_path, platform, max_record_bytes=64)
    assert writer.replace({"id": "old"}) is True
    before = writer.stats

    assert writer.replace({"message": "x" * 1_000}) is False
    after = writer.stats
    assert after.pending_records == before.pending_records
    assert after.pending_bytes == before.pending_bytes
    assert after.dropped_records == before.dropped_records + 1
    assert writer.pending is True

    writer.flush(force=True)
    assert [json.loads(line)["id"] for line in platform.payloads[0].splitlines()] == [
        "old"
    ]
    writer.close()
