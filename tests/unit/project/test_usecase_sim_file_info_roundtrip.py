"""Contract and oracle tests for the A9 file-info roundtrip case."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"
FEATURES_ROOT = SIM_ROOT / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from read.validate_a9_file_info_roundtrip import (  # noqa: E402
    EXPECTED_SCHEMA,
    HISTORICAL_SOURCE_PATH,
    SOURCE_ROWS,
    assert_native_flat_output,
    assert_roundtrip_read_output,
)


def test_authored_roundtrip_fixture_declares_read_phase_and_native_seed_path() -> None:
    from runner.metadata_sources import recipe_document
    metadata = recipe_document("read", "a9_file_info_roundtrip")
    connections = {item["name"]: item for item in metadata["connections"]}
    flows = {item["name"]: item for item in metadata["dataflows"]}

    assert set(flows) == {"a9-file-info-roundtrip-read"}
    assert flows["a9-file-info-roundtrip-read"]["stage"] == "a9_file_info_roundtrip_read"
    assert connections["a9_file_info_flat_source"]["format"] == "csv"
    assert connections["a9_file_info_flat_source"]["configure"]["base_path"] == (
        "__CASE_ROOT__/input/flat"
    )
    assert connections["a9_file_info_roundtrip_destination"]["format"] == "delta"


def _native_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            **{name: [row[name] for row in SOURCE_ROWS] for name in ("id", "value")},
            "__file_path": [HISTORICAL_SOURCE_PATH] * 2,
            "__file_name": ["source.csv"] * 2,
            "__file_modification_time": ["2026-01-01T00:00:00Z"] * 2,
            "__created_at": ["created-a", "created-b"],
        }
    )


def test_native_flat_oracle_requires_rows_schema_and_file_metadata() -> None:
    assert_native_flat_output(
        _native_rows(),
        SOURCE_ROWS,
        expected_source_path=HISTORICAL_SOURCE_PATH,
    )


@pytest.mark.parametrize("mutation", ["business", "path", "missing_metadata"])
def test_native_flat_oracle_rejects_roundtrip_mutations(mutation: str) -> None:
    frame = _native_rows()
    if mutation == "business":
        frame = frame.with_columns(pl.lit("wrong").alias("value"))
    elif mutation == "path":
        frame = frame.with_columns(pl.lit("C:/stale/path.csv").alias("__file_path"))
    else:
        frame = frame.drop("__file_modification_time")

    with pytest.raises(AssertionError, match="a9-file-info-flat"):
        assert_native_flat_output(
            frame,
            SOURCE_ROWS,
            expected_source_path=HISTORICAL_SOURCE_PATH,
        )


def test_roundtrip_delta_oracle_accepts_current_file_metadata() -> None:
    mtime = datetime(2026, 1, 2, tzinfo=timezone.utc)
    rows = [
        {
            **row,
            "__file_path": "C:/case/output/flat/own_output.csv",
            "__file_name": "own_output.csv",
            "__file_modification_time": mtime,
            "__created_at": "destination-created",
        }
        for row in SOURCE_ROWS
    ]
    assert_roundtrip_read_output(
        rows,
        {**EXPECTED_SCHEMA, "__file_path": "String", "__file_name": "String"},
        SOURCE_ROWS,
        expected_file_path="C:/case/output/flat/own_output.csv",
        expected_file_name="own_output.csv",
        expected_file_mtime=mtime,
    )


def test_roundtrip_delta_oracle_rejects_stale_file_metadata() -> None:
    mtime = datetime(2026, 1, 2, tzinfo=timezone.utc)
    rows = [
        {
            **row,
            "__file_path": HISTORICAL_SOURCE_PATH,
            "__file_name": "source.csv",
            "__file_modification_time": mtime,
            "__created_at": "destination-created",
        }
        for row in SOURCE_ROWS
    ]

    with pytest.raises(AssertionError, match="current file path metadata is stale"):
        assert_roundtrip_read_output(
            rows,
            {**EXPECTED_SCHEMA, "__file_path": "String", "__file_name": "String"},
            SOURCE_ROWS,
            expected_file_path="C:/case/output/flat/own_output.csv",
            expected_file_name="own_output.csv",
            expected_file_mtime=mtime,
        )
