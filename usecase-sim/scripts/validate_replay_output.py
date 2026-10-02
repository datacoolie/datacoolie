"""Validate one bounded replay run and its optional persisted watermark.

The validator is intentionally scenario-facing: it checks the rows selected by
the requested half-open window and the source observation saved by
``--replay-save-watermark``.  It does not infer progress from a stored
watermark, because replay chunks are always executed from the requested range.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from deltalake import DeltaTable


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent
RUNTIME_DATA_DIR = USECASE_SIM_DIR / ".runtime" / "data"
DEFAULT_OUTPUT = RUNTIME_DATA_DIR / "output" / "delta" / "orders_replay"


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


def _instant(value: Any) -> datetime:
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc).replace(
            tzinfo=None
        )
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, dict):
        if set(value) == {"__datetime__"}:
            return _instant(datetime.fromisoformat(str(value["__datetime__"])))
        if set(value) == {"__date__"}:
            return _instant(date.fromisoformat(str(value["__date__"])))
    if isinstance(value, str):
        return _instant(datetime.fromisoformat(value.replace(" ", "T")))
    raise AssertionError(f"Expected a temporal value, got {value!r}")


def _watermark_values(state_root: Path) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for path in sorted(state_root.rglob("watermark_value.json")):
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            continue
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise AssertionError(f"Watermark file must contain an object: {path}")
        values.append(value)
    return values


def validate(
    output: str,
    *,
    start: str,
    end: str,
    expected_rows: int,
    state_root: str | None = None,
    expected_watermark_column: str = "modified_at",
    expected_watermark: str | None = None,
) -> None:
    output_path = _under(Path(output), RUNTIME_DATA_DIR / "output", "output")
    table = DeltaTable(str(output_path)).to_pyarrow_table()
    if table.num_rows != expected_rows:
        raise AssertionError(
            f"Expected exactly {expected_rows} replay rows, got {table.num_rows}"
        )
    required = {"order_id", "modified_at"}
    missing = required.difference(table.column_names)
    if missing:
        raise AssertionError(f"Replay output is missing columns: {sorted(missing)}")

    lower = _instant(start)
    upper = _instant(end)
    modified = [_instant(value) for value in table.column("modified_at").to_pylist()]
    if not modified:
        raise AssertionError("Replay output must contain at least one row")
    if any(value < lower or value >= upper for value in modified):
        raise AssertionError(
            f"Replay rows escaped the half-open range [{start}, {end})"
        )
    expected_ids = {1001, 1002, 1003, 1004, 1005, 1006}
    actual_ids = set(table.column("order_id").to_pylist())
    if actual_ids != expected_ids:
        raise AssertionError(f"Unexpected replay order ids: {sorted(actual_ids)}")

    if state_root is None:
        return
    state_path = _under(Path(state_root), RUNTIME_DATA_DIR, "state_root")
    values = _watermark_values(state_path)
    if not values:
        raise AssertionError(f"No persisted watermark found below {state_path}")
    if expected_watermark is not None:
        expected = _instant(expected_watermark)
        candidates = [
            _instant(value[expected_watermark_column])
            for value in values
            if value.get(expected_watermark_column) is not None
        ]
        if expected not in candidates:
            raise AssertionError(
                f"Expected saved {expected_watermark_column}={expected_watermark}, got {candidates}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--state-root")
    parser.add_argument("--expected-watermark-column", default="modified_at")
    parser.add_argument("--expected-watermark")
    args = parser.parse_args()
    validate(
        args.output,
        start=args.start,
        end=args.end,
        expected_rows=args.expected_rows,
        state_root=args.state_root,
        expected_watermark_column=args.expected_watermark_column,
        expected_watermark=args.expected_watermark,
    )
    print(f"validated replay output: {Path(args.output)}")
    if args.state_root:
        print(f"validated replay watermark: {args.state_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
