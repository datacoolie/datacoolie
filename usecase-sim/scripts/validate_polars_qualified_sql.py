"""Reconcile persisted outputs from focused Polars qualified-SQL scenarios."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = USECASE_SIM_DIR / "data" / "output" / "qualified_sql" / "results"

EXPECTED_OUTPUTS = {
    "delta": (
        "delta_name_4",
        "delta_name_3",
        "delta_name_2",
        "delta_name_1",
        "delta_include",
        "delta_exclude",
        "delta_lazy_reuse",
    ),
    "iceberg": (
        "iceberg_default_root",
        "iceberg_logical_prefix",
        "iceberg_name_1",
        "iceberg_include",
        "iceberg_exclude",
        "iceberg_lazy_reuse",
    ),
}

EXPECTED_BUSINESS_COLUMNS = frozenset({"matched_rows", "total_amount"})


def _read_output(output_root: Path, output_name: str) -> tuple[pa.Schema, dict]:
    path = output_root / output_name
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise AssertionError(
            f"{output_name}: no Parquet output files found under {path}"
        )
    table = pq.read_table([str(file) for file in files])
    rows = table.to_pylist()
    if len(rows) != 1:
        raise AssertionError(f"{output_name}: expected 1 result row, got {len(rows)}")
    return table.schema, rows[0]


def _assert_schema(output_name: str, schema: pa.Schema) -> None:
    business_fields = {
        field.name: field.type for field in schema if not field.name.startswith("__")
    }
    if set(business_fields) != EXPECTED_BUSINESS_COLUMNS:
        raise AssertionError(
            f"{output_name}: business columns differ; "
            f"expected={sorted(EXPECTED_BUSINESS_COLUMNS)!r}, "
            f"actual={sorted(business_fields)!r}"
        )
    matched_rows_type = business_fields["matched_rows"]
    if not pa.types.is_integer(matched_rows_type):
        raise AssertionError(
            f"{output_name}.matched_rows: expected integer, got {matched_rows_type}"
        )
    total_amount_type = business_fields["total_amount"]
    if total_amount_type != pa.float64():
        raise AssertionError(
            f"{output_name}.total_amount: expected double, got {total_amount_type}"
        )


def validate_suite(suite: str, *, output_root: Path = DEFAULT_OUTPUT_ROOT) -> None:
    """Validate persisted query-result schema, row count, and measures."""

    expected_outputs = EXPECTED_OUTPUTS[suite]
    for output_name in expected_outputs:
        schema, row = _read_output(output_root, output_name)
        _assert_schema(output_name, schema)
        if row["matched_rows"] != 3:
            raise AssertionError(
                f"{output_name}: expected matched_rows=3, got {row['matched_rows']!r}"
            )
        if row["total_amount"] != 60.0:
            raise AssertionError(
                f"{output_name}: expected total_amount=60.0, "
                f"got {row['total_amount']!r}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, choices=sorted(EXPECTED_OUTPUTS))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    validate_suite(args.suite, output_root=args.output_root)
    print(
        f"Qualified-SQL {args.suite} validation passed: "
        f"{len(EXPECTED_OUTPUTS[args.suite])} independent query outputs"
    )


if __name__ == "__main__":
    main()
