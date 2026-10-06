"""Mutation guards for persisted A9 CSV write-option precedence."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"
FEATURES_ROOT = SIM_ROOT / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from write_basic.validate_a9_csv_options import (  # noqa: E402
    EXPECTED_BUSINESS_ROWS,
    EXPECTED_HEADER,
    EXPECTED_SCHEMA,
    assert_a9_csv_metadata,
    assert_a9_csv_profile,
    assert_csv_options_output,
)


METADATA_PATH = SIM_ROOT / "metadata" / "file" / "write_basic" / "a9_csv_options.json"


def _valid_bytes() -> bytes:
    header = "|".join(EXPECTED_HEADER)
    return (
        f'{header}\n'
        'A-001|"café ""quoted"", comma"|source.parquet|source.parquet|2024-01-01T00:00:00Z|'
        '2024-01-01T00:00:00Z|2024-01-01T00:00:00Z|datacoolie|run-1\n'
        'A-002|"東京 🚲 | pipe"|source.parquet|source.parquet|2024-01-01T00:00:00Z|'
        '2024-01-01T00:00:00Z|2024-01-01T00:00:00Z|datacoolie|run-1\n'
    ).encode("utf-8")


def test_a9_csv_fixture_declares_connection_defaults_and_destination_override() -> None:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    assert_a9_csv_metadata(metadata)


def test_csv_options_oracle_accepts_native_rows_and_schema() -> None:
    raw = _valid_bytes()
    rows = [
        {
            "id": "A-001",
            "label": 'café "quoted", comma',
            **{column: "metadata" for column in EXPECTED_HEADER[2:]},
        },
        {
            "id": "A-002",
            "label": "東京 🚲 | pipe",
            **{column: "metadata" for column in EXPECTED_HEADER[2:]},
        },
    ]
    assert_csv_options_output(raw, rows, EXPECTED_SCHEMA)


@pytest.mark.parametrize("mutation", ["wrong_separator", "missing_header", "wrong_business_value"])
def test_csv_options_bytes_oracle_rejects_persistence_mutants(mutation: str) -> None:
    raw = _valid_bytes()
    rows = [
        {
            "id": row["id"],
            "label": row["label"],
            **{column: "metadata" for column in EXPECTED_HEADER[2:]},
        }
        for row in EXPECTED_BUSINESS_ROWS
    ]
    if mutation == "wrong_separator":
        raw = raw.replace(b"|", b";", 1)
    elif mutation == "missing_header":
        raw = raw.split(b"\n", 1)[1]
    else:
        rows[1]["label"] = "wrong"

    with pytest.raises(AssertionError, match="a9-csv-options"):
        assert_csv_options_output(raw, rows, EXPECTED_SCHEMA)


def test_csv_options_oracle_rejects_non_string_native_schema() -> None:
    rows = [
        {
            "id": row["id"],
            "label": row["label"],
            **{column: "metadata" for column in EXPECTED_HEADER[2:]},
        }
        for row in EXPECTED_BUSINESS_ROWS
    ]
    schema = copy.deepcopy(EXPECTED_SCHEMA)
    schema["id"] = "Int64"
    with pytest.raises(AssertionError, match="a9-csv-options-schema"):
        assert_csv_options_output(_valid_bytes(), rows, schema)


def test_csv_options_profile_rejects_shared_delta_default() -> None:
    with pytest.raises(AssertionError, match="a9-csv-options-profile"):
        assert_a9_csv_profile({"profile": {"destination_format": "delta"}})
