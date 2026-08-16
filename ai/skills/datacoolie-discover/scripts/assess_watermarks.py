"""Create a deterministic, scratch-only watermark candidate shortlist."""
from __future__ import annotations

import argparse
import csv
import io
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from _artifact_io import atomic_write_json, atomic_write_text
from _observation_contract import KEY_FIELDS, read_observations
from _watermark_signals import suggest_roles

ASSESSMENT_HEADER = [
    "source",
    "object_type",
    "catalog",
    "schema",
    "object",
    "source_operation",
    "column",
    "row_estimate",
    "suggested_roles",
    "reason",
]
OBJECT_FIELDS = KEY_FIELDS[:-1]


def _object_row_estimate(rows: Iterable[Mapping[str, Any]]) -> str:
    values = {str(row.get("row_estimate", "")).strip() for row in rows}
    values.discard("")
    if len(values) > 1:
        raise ValueError(f"Object has contradictory row estimates: {sorted(values)}")
    return next(iter(values), "")


def build_assessment(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        object_key = tuple(str(row.get(field, "")).strip() for field in OBJECT_FIELDS)
        grouped[object_key].append(row)

    result: list[dict[str, str]] = []
    for object_key in sorted(grouped):
        object_rows = sorted(grouped[object_key], key=lambda row: (
            str(row.get("ordinal", "")), str(row.get("column", "")),
        ))
        suggestions = []
        for row in object_rows:
            roles, reason = suggest_roles(row)
            if roles:
                suggestions.append((row, roles, reason))

        row_estimate = _object_row_estimate(object_rows)
        if not suggestions:
            base = dict(zip(OBJECT_FIELDS, object_key))
            result.append({
                **base,
                "column": "",
                "row_estimate": row_estimate,
                "suggested_roles": "",
                "reason": "no name/type shortlist; assess fallback and human decision",
            })
            continue

        for row, roles, reason in suggestions:
            result.append({
                **{field: str(row.get(field, "")) for field in OBJECT_FIELDS},
                "column": str(row.get("column", "")),
                "row_estimate": row_estimate,
                "suggested_roles": roles,
                "reason": reason,
            })
    return result


def build_object_summary(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Build one compact scratch summary per discovered object."""
    grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        object_key = tuple(str(row.get(field, "")).strip() for field in OBJECT_FIELDS)
        grouped[object_key].append(row)

    objects: list[dict[str, Any]] = []
    for object_key in sorted(grouped):
        object_rows = sorted(grouped[object_key], key=lambda row: (
            int(str(row.get("ordinal", "") or 0)), str(row.get("column", "")),
        ))
        signals = []
        for row in object_rows:
            roles, reason = suggest_roles(row)
            if roles:
                signals.append({
                    "column": str(row.get("column", "")),
                    "suggested_roles": roles,
                    "reason": reason,
                })
        objects.append({
            "match": dict(zip(OBJECT_FIELDS, object_key)),
            "column_count": len(object_rows),
            "row_estimate": _object_row_estimate(object_rows),
            "keys": [
                {"column": str(row["column"]), "key": str(row["key"])}
                for row in object_rows if row.get("key")
            ],
            "references": [
                {"column": str(row["column"]), "reference": str(row["reference"])}
                for row in object_rows if row.get("reference")
            ],
            "signals": signals,
        })
    return {"schema_version": 1, "objects": objects}


def write_assessment(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    materialized = [{field: str(row.get(field, "")) for field in ASSESSMENT_HEADER} for row in rows]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=ASSESSMENT_HEADER)
    writer.writeheader()
    writer.writerows(materialized)
    atomic_write_text(path, buffer.getvalue())
    return len(materialized)


def assess(input_path: Path, output_path: Path, summary_output: Path | None = None) -> int:
    rows = read_observations(input_path)
    if summary_output is not None:
        atomic_write_json(summary_output, build_object_summary(rows))
    return write_assessment(output_path, build_assessment(rows))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Shortlist watermark candidates for semantic review.", allow_abbrev=False,
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--summary-output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        count = assess(args.input, args.output, args.summary_output)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    print(f"Wrote {count} watermark assessment row(s) to {args.output}")


if __name__ == "__main__":
    main()
