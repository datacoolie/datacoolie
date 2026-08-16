"""Canonical discovery observation construction, validation, and CSV I/O."""
from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping, TextIO

CSV_HEADER = [
    "source",
    "object_type",
    "catalog",
    "schema",
    "object",
    "source_operation",
    "column",
    "native_type",
    "data_type",
    "format",
    "precision",
    "scale",
    "nullable",
    "ordinal",
    "key",
    "reference",
    "row_estimate",
    "watermark_candidate",
    "notes",
]

KEY_FIELDS = (
    "source", "object_type", "catalog", "schema", "object", "source_operation", "column",
)
WATERMARK_ROLES = (
    "change",
    "insert",
    "update",
    "delete",
    "append",
    "auxiliary",
    "backward",
)
_WATERMARK_ROLE_INDEX = {role: index for index, role in enumerate(WATERMARK_ROLES)}


def observation_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(row.get(field, "")).strip() for field in KEY_FIELDS)


def canonicalize_watermark_candidate(value: Any) -> str:
    """Return unique watermark roles in canonical order."""
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    roles = [role.strip() for role in text.split("|")]
    if any(not role for role in roles):
        raise ValueError("watermark_candidate contains an empty role")
    unknown = sorted(set(roles) - set(WATERMARK_ROLES))
    if unknown:
        raise ValueError(f"Unknown watermark_candidate roles: {unknown}")
    if len(roles) != len(set(roles)):
        raise ValueError("watermark_candidate roles must be unique")
    return "|".join(sorted(roles, key=_WATERMARK_ROLE_INDEX.__getitem__))


def make_observation(
    *,
    source: Any,
    object_type: Any,
    object: Any,
    column: Any,
    native_type: Any,
    data_type: Any,
    catalog: Any = "",
    schema: Any = "",
    source_operation: Any = "",
    format: Any = "",
    precision: Any = "",
    scale: Any = "",
    nullable: Any = "",
    ordinal: Any = "",
    key: Any = "",
    reference: Any = "",
    row_estimate: Any = "",
    watermark_candidate: Any = "",
    notes: Any = "",
) -> dict[str, str]:
    """Create one normalized observation without adapter-specific positional rows."""
    values = locals()
    result = {
        field: "" if values[field] is None else str(values[field])
        for field in CSV_HEADER
    }
    validate_observation(result)
    return result


def validate_observation(row: Mapping[str, Any]) -> None:
    missing = [field for field in CSV_HEADER if field not in row]
    extra = [field for field in row if field not in CSV_HEADER]
    if missing or extra:
        raise ValueError(f"Observation fields mismatch; missing={missing}, extra={extra}")
    for field in ("source", "object_type", "object", "column"):
        if not str(row[field]).strip():
            raise ValueError(f"Observation requires non-empty {field}")
    key = str(row["key"])
    unique_name = key.removeprefix("unique:") if key.startswith("unique:") else ""
    if key and key != "primary" and not (
        unique_name and unique_name == unique_name.strip()
    ):
        raise ValueError("key must be empty, primary, or unique:<constraint-name>")
    reference = str(row["reference"])
    if reference.lstrip().startswith("→"):
        raise ValueError("reference must not contain display decoration")
    watermark = str(row["watermark_candidate"])
    if watermark != canonicalize_watermark_candidate(watermark):
        raise ValueError("watermark_candidate roles are not in canonical order")
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
