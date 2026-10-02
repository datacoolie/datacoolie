"""Validate the API incremental failure/recovery/continuation fixture."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from deltalake import DeltaTable


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent
RUNTIME_DATA_DIR = USECASE_SIM_DIR / ".runtime" / "data"


def _under(path: Path, root: Path, label: str) -> Path:
    resolved = path.expanduser()
    if not resolved.is_absolute():
        resolved = (USECASE_SIM_DIR.parent / resolved).resolve()
    else:
        resolved = resolved.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise AssertionError(f"{label} must stay below {root}: {resolved}") from exc
    return resolved


def _watermarks(state_root: Path) -> list[dict[str, object]]:
    values: list[dict[str, object]] = []
    for path in sorted(state_root.rglob("watermark_value.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise AssertionError(f"Watermark file must contain an object: {path}")
        values.append(payload)
    return values


def validate(
    output: str,
    state_root: str,
    *,
    expected_rows: int,
    continuation: bool,
) -> None:
    output_path = _under(Path(output), RUNTIME_DATA_DIR / "output", "output")
    state_path = _under(Path(state_root), RUNTIME_DATA_DIR, "state_root")
    table = DeltaTable(str(output_path)).to_pyarrow_table()
    if table.num_rows != expected_rows:
        raise AssertionError(
            f"Expected {expected_rows} rows after API recovery, got {table.num_rows}"
        )

    ids = table.column("order_id").to_pylist()
    expected_ids = set(range(1001, 1026)) | {1026, 1027}
    if continuation:
        expected_ids.add(1028)
    if set(ids) != expected_ids:
        raise AssertionError(f"Unexpected API order IDs: {sorted(set(ids))}")
    if continuation and ids.count(1028) != 1:
        raise AssertionError(
            "Continuation should select the late row 1028 once"
            f"; counts={ids.count(1028)}"
        )

    values = _watermarks(state_path)
    if not values:
        raise AssertionError(f"No persisted watermark found below {state_path}")
    expected_text = (
        "2024-02-02T09:30:00" if continuation else "2024-02-01T09:30:00"
    )
    expected = datetime.fromisoformat(expected_text)
    observed = [
        datetime.fromisoformat(str(value["modified_at"]))
        for value in values
        if value.get("modified_at") is not None
    ]
    if expected not in observed:
        raise AssertionError(f"Expected saved modified_at={expected!s}, got {observed}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--continuation", action="store_true")
    args = parser.parse_args()
    validate(
        args.output,
        args.state_root,
        expected_rows=args.expected_rows,
        continuation=args.continuation,
    )
    print(
        "validated API incremental recovery: "
        f"rows={args.expected_rows} continuation={args.continuation} "
        "saved modified_at="
        f"{'2024-02-02T09:30:00' if args.continuation else '2024-02-01T09:30:00'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
