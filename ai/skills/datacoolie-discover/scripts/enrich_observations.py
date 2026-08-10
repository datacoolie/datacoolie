"""Merge small, explicit annotations into generated discovery observations."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from _observation_contract import KEY_FIELDS, atomic_write_observations, observation_key, read_observations

MUTABLE_FIELDS = {
    "declared_key",
    "declared_reference",
    "watermark_candidate",
    "evidence_class",
}


def load_annotations(path: Path) -> dict[tuple[str, ...], tuple[dict[str, str], dict[str, str]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Annotations must be a JSON array")
    result: dict[tuple[str, ...], tuple[dict[str, str], dict[str, str]]] = {}
    for index, item in enumerate(payload):
        if not isinstance(item, dict) or set(item) != {"match", "set", "evidence"}:
            raise ValueError(f"Annotation {index} requires exactly 'match', 'set', and 'evidence'")
        match, updates, evidence = item["match"], item["set"], item["evidence"]
        if not isinstance(match, dict) or set(match) != set(KEY_FIELDS):
            raise ValueError(f"Annotation {index} match must contain the stable key fields")
        if not isinstance(updates, dict) or not updates:
            raise ValueError(f"Annotation {index} set must be a non-empty object")
        unsupported = set(updates) - MUTABLE_FIELDS
        if unsupported:
            raise ValueError(f"Annotation {index} cannot set fields: {sorted(unsupported)}")
        if not isinstance(evidence, dict) or set(evidence) != {"method", "observed_at", "notes"}:
            raise ValueError(
                f"Annotation {index} evidence requires method, observed_at, and notes"
            )
        normalized_evidence = {field: str(value).strip() for field, value in evidence.items()}
        if not normalized_evidence["method"] or not normalized_evidence["notes"]:
            raise ValueError(f"Annotation {index} evidence method and notes cannot be empty")
        try:
            parsed_at = datetime.fromisoformat(
                normalized_evidence["observed_at"].replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(f"Annotation {index} observed_at is not ISO-8601") from exc
        if parsed_at.tzinfo is None:
            raise ValueError(f"Annotation {index} observed_at requires a timezone")
        key = observation_key(match)
        if key in result:
            raise ValueError(f"Duplicate annotation key: {key}")
        result[key] = (
            {field: str(value) for field, value in updates.items()},
            normalized_evidence,
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
            updates, evidence = annotations[key]
            row.update(updates)
            row["method"] = f"{row['method']} | annotation:{evidence['method']}"
            provenance_note = (
                f"annotation[{evidence['observed_at']} via {evidence['method']}]: "
                f"{evidence['notes']}"
            )
            row["notes"] = f"{row['notes']}; {provenance_note}" if row["notes"] else provenance_note
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
