"""Validate one persisted output for every focused transformer use case."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


USECASE_SIM = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = USECASE_SIM / "data" / "output" / "parquet"
SOURCE_BUSINESS_COLUMNS = {
    "record_id",
    "email",
    "status",
    "phone",
    "formatted_phone",
    "age",
    "birth_date",
    "country_code",
    "customer_id",
    "notes",
    "secret",
    "drop_me",
    "nullable_label",
    "ordered_text",
    "whitespace_text",
    "optional_phone",
}


def _read_case(output_root: Path, table_name: str) -> tuple[pa.Schema, list[dict]]:
    path = output_root / table_name
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise AssertionError(f"{table_name}: no Parquet output files found under {path}")
    table = pq.read_table([str(file) for file in files])
    rows = sorted(table.to_pylist(), key=lambda row: row["record_id"])
    assert len(rows) == 3, f"{table_name}: expected 3 rows, got {len(rows)}"
    return table.schema, rows


def _assert_values(
    output_root: Path,
    table_name: str,
    column: str,
    expected: list[object],
) -> None:
    _, rows = _read_case(output_root, table_name)
    actual = [row[column] for row in rows]
    assert actual == expected, (
        f"{table_name}.{column}: expected {expected!r}, got {actual!r}"
    )


def _business_columns(schema: pa.Schema) -> set[str]:
    return {name for name in schema.names if not name.startswith("__")}


def _assert_business_columns(
    output_root: Path,
    table_name: str,
    expected: set[str],
) -> None:
    schema, _ = _read_case(output_root, table_name)
    actual = _business_columns(schema)
    assert actual == expected, (
        f"{table_name}: expected business columns {sorted(expected)!r}, "
        f"got {sorted(actual)!r}"
    )


def _validate_value_rules(output_root: Path) -> None:
    _assert_values(
        output_root,
        "tf_value_trim",
        "whitespace_text",
        ["\talpha\u00a0", "beta", "\u00a0gamma\u00a0"],
    )
    _assert_values(
        output_root,
        "tf_value_case",
        "email",
        [" alice@example.com ", "bob@example.com", "carol@example.com "],
    )
    _assert_values(
        output_root,
        "tf_value_regex_replace",
        "formatted_phone",
        ["0912345678", "0987654321", "1234"],
    )
    _assert_values(
        output_root,
        "tf_value_regex_literal",
        "formatted_phone",
        [r"0912$1\tail345$1\tail678", "0987 654 321", r"12$1\tail34"],
    )
    _assert_values(
        output_root,
        "tf_value_empty_to_null",
        "notes",
        [None, "keep me", "note"],
    )
    _assert_values(
        output_root,
        "tf_value_fill_null",
        "nullable_label",
        ["unknown", "known", "vip"],
    )
    _assert_values(
        output_root,
        "tf_value_map",
        "status",
        ["active", "inactive", "X"],
    )
    _assert_values(
        output_root,
        "tf_value_stable_order",
        "notes",
        ["ordered", "keep me", "note"],
    )

    schema, rows = _read_case(output_root, "tf_value_before_cast")
    assert pa.types.is_int64(schema.field("formatted_phone").type)
    assert [row["formatted_phone"] for row in rows] == [912345678, 987654321, 1234]


def _validate_schema_and_hash(output_root: Path) -> None:
    schema, rows = _read_case(output_root, "tf_schema_cast")
    customer_id_type = schema.field("customer_id").type
    assert pa.types.is_string(customer_id_type) or pa.types.is_large_string(
        customer_id_type
    )
    assert [row["customer_id"] for row in rows] == ["123", "456", "789"]

    _assert_business_columns(
        output_root, "tf_schema_missing_warning", SOURCE_BUSINESS_COLUMNS
    )
    _assert_values(
        output_root,
        "tf_hash_sha256",
        "identity_hash",
        [
            "37b8936f5d03e9f3658997ed1e6dd919cdf5071319de953091488d0355c74f41",
            "000f2632bd4aca14e54298cb424b115352fd5b0298331f669c594afb88f1065f",
            "5c998958d3e2eed3f2b82e5b66f9ac871d0a24421bd3b583c1f79338122c9a0a",
        ],
    )


def _validate_masking(output_root: Path) -> None:
    _assert_values(
        output_root,
        "tf_mask_redact",
        "secret",
        ["[REDACTED]", "[REDACTED]", "[REDACTED]"],
    )
    _assert_values(output_root, "tf_mask_nullify", "drop_me", [None, None, None])
    _assert_values(
        output_root, "tf_mask_partial", "phone", ["*5678", "*4321", "*"]
    )
    _assert_values(
        output_root, "tf_mask_partial", "optional_phone", [None, "", "*"]
    )
    _assert_values(output_root, "tf_mask_numeric_bucket", "age", [20, 30, 40])
    _assert_values(
        output_root,
        "tf_mask_date_truncate",
        "birth_date",
        [date(1990, 1, 1), date(1985, 1, 1), date(1978, 1, 1)],
    )


def _validate_projection(output_root: Path) -> None:
    _assert_business_columns(
        output_root, "tf_projection_select", {"record_id", "email"}
    )
    _assert_business_columns(
        output_root,
        "tf_projection_drop",
        SOURCE_BUSINESS_COLUMNS - {"notes", "drop_me"},
    )
    renamed = (
        SOURCE_BUSINESS_COLUMNS - {"phone", "customer_id"}
    ) | {"masked_phone", "business_customer_id"}
    _assert_business_columns(output_root, "tf_projection_batch_rename", renamed)
    _assert_values(
        output_root,
        "tf_projection_batch_rename",
        "masked_phone",
        ["0912345678", "0987654321", "1234"],
    )
    _assert_values(
        output_root,
        "tf_projection_batch_rename",
        "business_customer_id",
        [123, 456, 789],
    )


def _validate_missing_column_policy(output_root: Path) -> None:
    for table_name in (
        "tf_missing_value_rule",
        "tf_missing_hash",
        "tf_missing_masking",
    ):
        _assert_business_columns(output_root, table_name, SOURCE_BUSINESS_COLUMNS)

    schema, _ = _read_case(output_root, "tf_missing_hash")
    assert "skipped_hash" not in schema.names
    _assert_business_columns(output_root, "tf_missing_projection", {"record_id"})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate single-case transformer scenario outputs"
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    _validate_value_rules(args.output_root)
    _validate_schema_and_hash(args.output_root)
    _validate_masking(args.output_root)
    _validate_projection(args.output_root)
    _validate_missing_column_policy(args.output_root)
    print(
        "Transformer feature output validation passed: "
        "24 single-case tables, 72 reconciled rows"
    )


if __name__ == "__main__":
    main()
