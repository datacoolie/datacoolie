"""Mutation guards for the A9 local transformer fixture and oracles."""

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

from transform.validate_a9_local import (  # noqa: E402
    EXPECTED_SCHEMA,
    EXPECTED_TIMESTAMP_SCHEMA,
    EXPECTED_TIMESTAMP_TRUNCATE_ROWS,
    EXPECTED_UPPERCASE_ROWS,
    MAP_NULL_FLOW,
    PARTIAL_KEEP_START_FLOW,
    TIMESTAMP_TRUNCATE_FLOW,
    UPPERCASE_FLOW,
    assert_a9_transform_metadata,
    assert_map_null_output,
    assert_partial_keep_start_output,
    assert_timestamp_truncate_output,
    assert_uppercase_output,
)


METADATA_PATH = SIM_ROOT / "metadata" / "file" / "transform" / "a9_local.json"


def _rows(rows: list[dict]) -> list[dict]:
    return copy.deepcopy(rows)


def test_a9_transform_fixture_declares_four_independent_primary_flows() -> None:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    assert_a9_transform_metadata(metadata)
    assert {flow["stage"] for flow in metadata["dataflows"]} == {
        "a9_transform_uppercase",
        "a9_transform_map_null",
        "a9_transform_partial_keep_start",
        "a9_transform_timestamp_truncate_hour",
    }


def test_uppercase_oracle_rejects_lowercase_unicode_mutant() -> None:
    mutated = _rows(EXPECTED_UPPERCASE_ROWS)
    mutated[0]["mixed_text"] = "hello Đặng"
    with pytest.raises(AssertionError, match=UPPERCASE_FLOW):
        assert_uppercase_output(mutated, EXPECTED_SCHEMA, label=UPPERCASE_FLOW)


def test_uppercase_oracle_rejects_null_preservation_mutant() -> None:
    mutated = _rows(EXPECTED_UPPERCASE_ROWS)
    mutated[2]["mixed_text"] = "NULL"
    with pytest.raises(AssertionError, match=UPPERCASE_FLOW):
        assert_uppercase_output(mutated, EXPECTED_SCHEMA, label=UPPERCASE_FLOW)


def test_map_null_oracle_rejects_unknown_value_retention() -> None:
    from transform.validate_a9_local import EXPECTED_MAP_NULL_ROWS

    mutated = _rows(EXPECTED_MAP_NULL_ROWS)
    mutated[1]["status"] = "unknown"
    with pytest.raises(AssertionError, match=MAP_NULL_FLOW):
        assert_map_null_output(mutated, EXPECTED_SCHEMA, label=MAP_NULL_FLOW)


def test_map_null_oracle_rejects_input_null_to_value_mutant() -> None:
    from transform.validate_a9_local import EXPECTED_MAP_NULL_ROWS

    mutated = _rows(EXPECTED_MAP_NULL_ROWS)
    mutated[2]["status"] = "active"
    with pytest.raises(AssertionError, match=MAP_NULL_FLOW):
        assert_map_null_output(mutated, EXPECTED_SCHEMA, label=MAP_NULL_FLOW)


def test_partial_keep_start_oracle_rejects_prefix_mutant() -> None:
    from transform.validate_a9_local import EXPECTED_PARTIAL_KEEP_START_ROWS

    mutated = _rows(EXPECTED_PARTIAL_KEEP_START_ROWS)
    mutated[0]["phone"] = "*89"
    with pytest.raises(AssertionError, match=PARTIAL_KEEP_START_FLOW):
        assert_partial_keep_start_output(
            mutated,
            EXPECTED_SCHEMA,
            label=PARTIAL_KEEP_START_FLOW,
        )


def test_partial_keep_start_oracle_rejects_short_and_null_mutants() -> None:
    from transform.validate_a9_local import EXPECTED_PARTIAL_KEEP_START_ROWS

    for index, value in ((1, "xy"), (2, "#")):
        mutated = _rows(EXPECTED_PARTIAL_KEEP_START_ROWS)
        mutated[index]["phone"] = value
        with pytest.raises(AssertionError, match=PARTIAL_KEEP_START_FLOW):
            assert_partial_keep_start_output(
                mutated,
                EXPECTED_SCHEMA,
                label=PARTIAL_KEEP_START_FLOW,
            )


def test_timestamp_truncate_oracle_rejects_no_truncation_and_null_mutants() -> None:
    for index, value in (
        (0, EXPECTED_TIMESTAMP_TRUNCATE_ROWS[0]["event_time"].replace(minute=34)),
        (2, EXPECTED_TIMESTAMP_TRUNCATE_ROWS[0]["event_time"]),
    ):
        mutated = _rows(EXPECTED_TIMESTAMP_TRUNCATE_ROWS)
        mutated[index]["event_time"] = value
        with pytest.raises(AssertionError, match=TIMESTAMP_TRUNCATE_FLOW):
            assert_timestamp_truncate_output(
                mutated,
                EXPECTED_TIMESTAMP_SCHEMA,
                label=TIMESTAMP_TRUNCATE_FLOW,
            )


def test_timestamp_truncate_oracle_rejects_date_type_mutant() -> None:
    mutated_schema = dict(EXPECTED_SCHEMA)
    mutated_schema["event_time"] = "Date"
    with pytest.raises(AssertionError, match=TIMESTAMP_TRUNCATE_FLOW):
        assert_timestamp_truncate_output(
            EXPECTED_TIMESTAMP_TRUNCATE_ROWS,
            mutated_schema,
            label=TIMESTAMP_TRUNCATE_FLOW,
        )
