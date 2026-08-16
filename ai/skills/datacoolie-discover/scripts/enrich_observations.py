"""Merge small, explicit annotations into generated discovery observations."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from _observation_contract import KEY_FIELDS, atomic_write_observations, observation_key, read_observations

MUTABLE_FIELDS = {
    "key",
    "reference",
    "watermark_candidate",
}


def load_annotations(path: Path) -> dict[tuple[str, ...], tuple[dict[str, str], str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Annotations must be a JSON array")
    result: dict[tuple[str, ...], tuple[dict[str, str], str]] = {}
    for index, item in enumerate(payload):
        if not isinstance(item, dict) or not {"match", "set"}.issubset(item):
            raise ValueError(f"Annotation {index} requires 'match' and 'set'")
        unsupported_item_fields = set(item) - {"match", "set", "append_notes"}
        if unsupported_item_fields:
            raise ValueError(
                f"Annotation {index} has unsupported fields: {sorted(unsupported_item_fields)}"
            )
        match, updates = item["match"], item["set"]
        if not isinstance(match, dict) or set(match) != set(KEY_FIELDS):
            raise ValueError(f"Annotation {index} match must contain the stable key fields")
        if not isinstance(updates, dict) or not updates:
            raise ValueError(f"Annotation {index} set must be a non-empty object")
        unsupported = set(updates) - MUTABLE_FIELDS
        if unsupported:
            raise ValueError(f"Annotation {index} cannot set fields: {sorted(unsupported)}")
        append_notes = str(item.get("append_notes", "")).strip()
        if "append_notes" in item and not append_notes:
            raise ValueError(f"Annotation {index} append_notes cannot be empty")
        key = observation_key(match)
        if key in result:
            raise ValueError(f"Duplicate annotation key: {key}")
        result[key] = (
            {field: str(value) for field, value in updates.items()},
            append_notes,
        )
    return result


def enrich(input_path: Path, annotations_path: Path, output_path: Path) -> int:
    annotations = load_annotations(annotations_path)
    rows = read_observations(input_path)

    seen: set[tuple[str, ...]] = set()
    applied = 0
    for row in rows:
        key = observation_key(row)
        if key in seen:
            raise ValueError(f"Duplicate observation key: {key}")
        seen.add(key)
        if key in annotations:
            updates, append_notes = annotations[key]
            row.update(updates)
            if append_notes:
                row["notes"] = (
                    f"{row['notes']}; {append_notes}" if row["notes"] else append_notes
                )
            applied += 1

    unmatched = set(annotations) - seen
    if unmatched:
        raise ValueError(f"Annotations reference unknown observation keys: {sorted(unmatched)}")

    atomic_write_observations(output_path, rows)
    return applied


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge annotations into discovery observations.", allow_abbrev=False,
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--annotations", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        applied = enrich(args.input, args.annotations, args.output)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    print(f"Applied {applied} annotation(s) to {args.output}")


if __name__ == "__main__":
    main()
