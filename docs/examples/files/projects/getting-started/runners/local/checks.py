"""Small, project-owned guards used by the local onboarding runners."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


class GuardError(RuntimeError):
    """Raised when a tutorial preflight or output invariant is not met."""


def resolve_root(project_root: Path, value: str) -> Path:
    """Resolve a runner option relative to the project root."""

    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else project_root / candidate


def read_fixture(path: Path, required_headers: Iterable[str]) -> list[dict[str, str]]:
    """Read one non-empty CSV fixture and enforce its business headers."""

    if not path.is_file():
        raise GuardError(f"Input fixture is missing: {path}")
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = set(reader.fieldnames or ())
            missing = sorted(set(required_headers) - headers)
            if missing:
                raise GuardError(
                    f"Input fixture {path} is missing required headers: {', '.join(missing)}"
                )
            rows = [
                {key: (value or "").strip() for key, value in row.items()}
                for row in reader
                if any((value or "").strip() for value in row.values())
            ]
    except UnicodeDecodeError as exc:
        raise GuardError(f"Input fixture is not UTF-8 CSV: {path}") from exc
    if not rows:
        raise GuardError(f"Input fixture has no business rows: {path}")
    return rows


def parse_int(value: str, *, column: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise GuardError(f"Column {column!r} contains a non-integer value: {value!r}") from exc


def parse_amount(value: str) -> Decimal:
    try:
        return Decimal(value).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise GuardError(f"Amount is not a two-decimal value: {value!r}") from exc


def parse_timestamp(value: Any, *, label: str = "timestamp") -> datetime:
    """Parse ISO timestamps and normalize them to UTC-aware datetimes."""

    if isinstance(value, dict):
        value = value.get("__datetime__")
    if not isinstance(value, str) or not value.strip():
        raise GuardError(f"{label} is missing or not an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise GuardError(f"{label} is not an ISO timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def result_counts(result: Any) -> dict[str, Any]:
    """Return stable, JSON-friendly counters from an ExecutionResult."""

    return {
        "total": int(getattr(result, "total", 0)),
        "succeeded": int(getattr(result, "succeeded", 0)),
        "failed": int(getattr(result, "failed", 0)),
        "skipped": int(getattr(result, "skipped", 0)),
        "pending": int(getattr(result, "pending", 0)),
        "errors": {
            str(key): str(value)
            for key, value in dict(getattr(result, "errors", {}) or {}).items()
        },
    }


def require_named_dataflows(
    dataflows: list[Any], *, expected_name: str, stage: str
) -> list[Any]:
    """Require one active dataflow with the lesson's exact authored name."""

    names = [str(getattr(dataflow, "name", "") or "") for dataflow in dataflows]
    if names != [expected_name]:
        raise GuardError(
            f"Stage {stage!r} selected {names!r}; expected [{expected_name!r}]"
        )
    return dataflows


def require_terminal_result(result: Any, *, lesson: str, allow_skip: bool = False) -> dict[str, Any]:
    """Require exactly one selected flow and no failed or pending work."""

    counts = result_counts(result)
    if counts["total"] != 1:
        raise GuardError(
            f"Lesson {lesson!r} selected {counts['total']} dataflows; expected exactly one"
        )
    if counts["failed"] or counts["pending"]:
        raise GuardError(f"Lesson {lesson!r} did not complete: {counts}")
    if counts["skipped"] and not allow_skip:
        raise GuardError(f"Lesson {lesson!r} was skipped unexpectedly: {counts}")
    if not counts["succeeded"] and not counts["skipped"]:
        raise GuardError(f"Lesson {lesson!r} has no terminal success: {counts}")
    return counts


def latest_orders(rows: list[dict[str, str]]) -> dict[int, dict[str, str]]:
    """Return one newest row per order ID, retaining the first exact tie."""

    latest: dict[int, tuple[datetime, dict[str, str]]] = {}
    for row in rows:
        order_id = parse_int(row["order_id"], column="order_id")
        observed = parse_timestamp(row["updated_at"], label="updated_at")
        previous = latest.get(order_id)
        if previous is None or observed > previous[0]:
            latest[order_id] = (observed, row)
    return {order_id: row for order_id, (_, row) in latest.items()}


def read_updated_at_watermark(runtime_root: Path) -> datetime:
    """Read the persisted orders watermark from the explicit runtime root."""

    watermark_root = runtime_root / "watermarks"
    candidates = sorted(watermark_root.rglob("watermark_value.json")) if watermark_root.is_dir() else []
    values: list[datetime] = []
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise GuardError(f"Cannot read persisted watermark: {path}") from exc
        if not isinstance(payload, dict) or "updated_at" not in payload:
            continue
        values.append(parse_timestamp(payload["updated_at"], label=f"watermark {path}"))
    if not values:
        raise GuardError(f"No persisted updated_at watermark found below {watermark_root}")
    return max(values)


def require_no_newer_rows(
    rows: list[dict[str, str]],
    *,
    runtime_root: Path,
    output_path: Path,
) -> datetime:
    """Prove a skipped orders read is a safe no-change continuation."""

    if not output_path.is_dir() or not (output_path / "_delta_log").is_dir():
        raise GuardError(f"Skipped Bronze run has no persisted Delta output: {output_path}")
    watermark = read_updated_at_watermark(runtime_root)
    newer = [
        row["updated_at"]
        for row in rows
        if parse_timestamp(row["updated_at"], label="updated_at") > watermark
    ]
    if newer:
        raise GuardError(
            "Bronze was skipped while input contains rows newer than the persisted "
            f"watermark {watermark.isoformat()}: {newer}"
        )
    return watermark


def require_delta_path(path: Path, *, label: str) -> None:
    """Require a materialized Delta directory before downstream use."""

    if not path.is_dir() or not (path / "_delta_log").is_dir():
        raise GuardError(f"{label} Delta output is missing: {path}")
