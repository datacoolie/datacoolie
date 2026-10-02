"""Independent datatype observation comparisons.

No production resolver or engine schema mapper is imported here.  A mismatch
must remain visible instead of being normalised into the expected answer.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from tests.support.data_types.observations import (
    GENERATED_SYSTEM_FIELDS,
    FrameObservation,
)


class ObservationMismatch(AssertionError):
    """Raised when persisted observations disagree."""


def _diff(label: str, expected: Any, actual: Any) -> ObservationMismatch:
    return ObservationMismatch(
        f"{label} mismatch:\nexpected={expected!r}\nactual={actual!r}"
    )


_DECIMAL_TYPE = re.compile(r"^decimal\((\d+),(\d+)\)$")


def _expand_field_types(expected: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    """Expand compact persisted-contract type declarations.

    Matrix contracts repeat the same nullable field shape many times.  The
    fixture stores the semantic type by field name; this helper expands that
    representation into the exact Arrow observation descriptors.
    """

    result: list[dict[str, Any]] = []
    for name, raw_type in expected.get("business_field_types", {}).items():
        if not isinstance(raw_type, str):
            raise AssertionError(f"Invalid expected datatype for {name!r}: {raw_type!r}")
        decimal_match = _DECIMAL_TYPE.fullmatch(raw_type)
        if decimal_match:
            precision, scale = map(int, decimal_match.groups())
            result.append(
                {
                    "name": name,
                    "kind": "decimal",
                    "precision": precision,
                    "scale": scale,
                    "nullable": True,
                }
            )
            continue
        if raw_type == "date":
            result.append(
                {"name": name, "kind": "date", "unit": "day", "nullable": True}
            )
            continue
        if raw_type == "timestamp_ntz":
            result.append(
                {
                    "name": name,
                    "kind": "timestamp",
                    "unit": "us",
                    "timezone": None,
                    "nullable": True,
                }
            )
            continue
        if raw_type == "timestamp":
            result.append(
                {
                    "name": name,
                    "kind": "timestamp",
                    "unit": "us",
                    "timezone": "UTC",
                    "nullable": True,
                }
            )
            continue
        result.append({"name": name, "kind": raw_type, "nullable": True})
    return tuple(result)


def _ignore_nullability(fields: Any) -> tuple[dict[str, Any], ...]:
    """Normalize writer-owned nullability differences for system columns."""

    return tuple(
        {key: value for key, value in field.items() if key != "nullable"}
        for field in fields
    )


def compare_observations(
    left: FrameObservation,
    right: FrameObservation,
) -> None:
    """Compare business schema/values and normalized system-column schema.

    File metadata columns are framework-owned and some writers disagree only
    on their nullability.  Their names and semantic kinds still must match;
    nullability is intentionally excluded from that operational comparison.
    """

    if left.case_id != right.case_id:
        raise _diff("case_id", left.case_id, right.case_id)
    if left.output_format != right.output_format:
        raise _diff("output_format", left.output_format, right.output_format)
    left_business_fields = tuple(
        field
        for field in left.fields
        if field.get("name") not in GENERATED_SYSTEM_FIELDS
    )
    right_business_fields = tuple(
        field
        for field in right.fields
        if field.get("name") not in GENERATED_SYSTEM_FIELDS
    )
    if left_business_fields != right_business_fields:
        raise _diff("business_fields", left_business_fields, right_business_fields)
    left_generated_fields = tuple(
        field for field in left.fields if field.get("name") in GENERATED_SYSTEM_FIELDS
    )
    right_generated_fields = tuple(
        field for field in right.fields if field.get("name") in GENERATED_SYSTEM_FIELDS
    )
    if _ignore_nullability(left_generated_fields) != _ignore_nullability(
        right_generated_fields
    ):
        raise _diff("generated_fields", left_generated_fields, right_generated_fields)
    if left.row_count != right.row_count:
        raise _diff("row_count", left.row_count, right.row_count)
    if left.business_rows != right.business_rows:
        raise _diff("rows", left.business_rows, right.business_rows)


def assert_observation_matches(
    observation: FrameObservation,
    expected: Mapping[str, Any],
) -> None:
    """Check one observation against an independently authored expectation."""

    format_contract = expected.get("formats", {}).get(observation.output_format)
    if isinstance(format_contract, Mapping):
        expected = {**expected, **format_contract}
    expected_fields = tuple(expected.get("fields", ()))
    compact_business_fields = bool(expected.get("business_field_types"))
    if not expected_fields and compact_business_fields:
        expected_fields = _expand_field_types(expected)
    if expected_fields and not compact_business_fields:
        actual_fields = observation.fields
    else:
        expected_fields = tuple(expected.get("business_fields", ()))
        if compact_business_fields:
            expected_fields = _expand_field_types(expected)
        actual_fields = tuple(
            field
            for field in observation.fields
            if field.get("name") not in GENERATED_SYSTEM_FIELDS
        )
        expected_generated = tuple(expected.get("generated_fields", ()))
        if expected_generated:
            actual_generated = tuple(
                field
                for field in observation.fields
                if field.get("name") in GENERATED_SYSTEM_FIELDS
            )
            if _ignore_nullability(actual_generated) != _ignore_nullability(
                expected_generated
            ):
                raise _diff("generated_fields", expected_generated, actual_generated)
    if actual_fields != expected_fields:
        raise _diff("fields", expected_fields, actual_fields)
    expected_rows = tuple(expected.get("rows", ()))
    if observation.business_rows != expected_rows:
        raise _diff("rows", expected_rows, observation.business_rows)
    expected_count = int(expected.get("row_count", len(expected_rows)))
    if observation.row_count != expected_count:
        raise _diff("row_count", expected_count, observation.row_count)
