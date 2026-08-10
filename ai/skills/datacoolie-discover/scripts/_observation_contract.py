"""Canonical discovery observation construction, validation, and CSV I/O."""
from __future__ import annotations

import csv
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, TextIO

CSV_HEADER = [
    "source",
    "object_type",
    "catalog",
    "schema",
    "object",
    "operation",
    "column",
    "native_type",
    "data_type",
    "format",
    "precision",
    "scale",
    "nullable",
    "ordinal",
    "declared_key",
    "declared_reference",
    "row_estimate",
    "watermark_candidate",
    "observed_at",
    "method",
    "evidence_class",
    "notes",
]

KEY_FIELDS = (
    "source", "object_type", "catalog", "schema", "object", "operation", "column",
)
WATERMARK_VALUES = {"", "declared", "observed", "inferred"}
EVIDENCE_VALUES = {"declared", "observed", "inferred", "unresolved"}

_TIMESTAMP_NAMES = {
    "last_modified",
    "last_modified_at",
    "last_updated",
    "last_updated_at",
    "modified_at",
    "updated_at",
}
_SEQUENCE_NAMES = {
    "change_version",
    "row_version",
    "rowversion",
    "sequence_number",
}


def utc_observed_at() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def infer_watermark_candidate(column: str, data_type: str) -> str:
    name = column.strip().lower()
    normalized_type = data_type.strip().lower()
    temporal = any(token in normalized_type for token in ("date", "time", "timestamp"))
    sequence = any(
        token in normalized_type
        for token in ("int", "long", "number", "numeric", "decimal", "binary", "byte")
    )
    if name in _TIMESTAMP_NAMES and temporal:
        return "inferred"
    if name in _SEQUENCE_NAMES and sequence:
        return "inferred"
    return ""


def observation_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(row.get(field, "")).strip() for field in KEY_FIELDS)


def make_observation(
    *,
    source: Any,
    object_type: Any,
    object: Any,
    column: Any,
    native_type: Any,
    data_type: Any,
    observed_at: Any,
    method: Any,
    evidence_class: Any,
    catalog: Any = "",
    schema: Any = "",
    operation: Any = "",
    format: Any = "",
    precision: Any = "",
    scale: Any = "",
    nullable: Any = "",
    ordinal: Any = "",
    declared_key: Any = "",
    declared_reference: Any = "",
    row_estimate: Any = "",
    watermark_candidate: Any | None = None,
    notes: Any = "",
) -> dict[str, str]:
    """Create one normalized observation without adapter-specific positional rows."""
    values = locals()
    result = {
        field: "" if values[field] is None else str(values[field])
        for field in CSV_HEADER
    }
    if watermark_candidate is None:
        result["watermark_candidate"] = infer_watermark_candidate(
            result["column"], result["data_type"],
        )
    validate_observation(result)
    return result


def _valid_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return value.endswith("Z") or "+" in value[10:] or "-" in value[10:]


def validate_observation(row: Mapping[str, Any]) -> None:
    missing = [field for field in CSV_HEADER if field not in row]
    extra = [field for field in row if field not in CSV_HEADER]
    if missing or extra:
        raise ValueError(f"Observation fields mismatch; missing={missing}, extra={extra}")
    for field in ("source", "object_type", "object", "column", "observed_at", "method"):
        if not str(row[field]).strip():
            raise ValueError(f"Observation requires non-empty {field}")
    if str(row["watermark_candidate"]) not in WATERMARK_VALUES:
        raise ValueError("Invalid watermark_candidate value")
    if str(row["evidence_class"]) not in EVIDENCE_VALUES:
        raise ValueError("Invalid evidence_class value")
    if not _valid_timestamp(str(row["observed_at"])):
        raise ValueError("observed_at must be an ISO-8601 timestamp with timezone")
    if str(row["ordinal"]):
        try:
            if int(str(row["ordinal"])) < 1:
                raise ValueError
        except ValueError as exc:
            raise ValueError("ordinal must be a positive integer or empty") from exc


def validate_observations(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()
    for index, row in enumerate(rows, start=1):
        validate_observation(row)
        item = {field: str(row[field]) for field in CSV_HEADER}
        key = observation_key(item)
        if key in seen:
            raise ValueError(f"Duplicate observation key at data row {index}: {key}")
        seen.add(key)
        normalized.append(item)
    return normalized


def read_observations(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != CSV_HEADER:
            raise ValueError(f"{path} does not match the discovery observation header")
        rows = list(reader)
        if any(None in row or any(value is None for value in row.values()) for row in rows):
            raise ValueError(f"{path} contains a malformed CSV row")
        return validate_observations(rows)


def write_observations(handle: TextIO, rows: Iterable[Mapping[str, Any]]) -> int:
    validated = validate_observations(rows)
    writer = csv.DictWriter(handle, fieldnames=CSV_HEADER, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(validated)
    return len(validated)


def atomic_write_observations(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    validated = validate_observations(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
            writer.writeheader()
            writer.writerows(validated)
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise
    return len(validated)
