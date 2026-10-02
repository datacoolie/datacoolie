"""Validate the small result produced by an artifact-backed runtime scenario."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = USECASE_SIM_DIR / ".runtime" / "data" / "output"


def _resolve_output(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (USECASE_SIM_DIR.parent / path).resolve()
    else:
        path = path.resolve()
    try:
        path.relative_to(DEFAULT_ROOT.resolve())
    except ValueError as exc:
        raise AssertionError(
            f"Artifact fixture output must stay below {DEFAULT_ROOT}: {path}"
        ) from exc
    return path


def validate(output: str, *, minimum_rows: int = 1) -> None:
    root = _resolve_output(output)
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise AssertionError(f"No parquet files found below {root}")
    table = pq.read_table([str(path) for path in files])
    if table.num_rows < minimum_rows:
        raise AssertionError(
            f"Expected at least {minimum_rows} rows below {root}, got {table.num_rows}"
        )
    expected = {"order_id", "amount", "category"}
    actual = set(table.column_names)
    if not expected.issubset(actual):
        raise AssertionError(f"Missing query result columns: expected {expected}, got {actual}")
    if not pa.types.is_integer(table.schema.field("order_id").type):
        raise AssertionError("order_id must remain an integer column")
    order_ids = table.column("order_id").to_pylist()
    if order_ids != sorted(order_ids):
        raise AssertionError(f"Query ordering was not preserved: {order_ids}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--minimum-rows", type=int, default=1)
    args = parser.parse_args()
    validate(args.output, minimum_rows=args.minimum_rows)
    print(f"validated artifact output: {_resolve_output(args.output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
