"""Typed allowlist serializers for execution runtime values.

The runtime models deliberately do not flow through ``dataclasses.asdict`` or
``getattr(..., default=None)``.  A missing contract member is a programming
error; an existing nullable member whose value is ``None`` remains JSON null.
Diagnostic members may be normalized with one shared
:class:`DiagnosticBudget` across a complete dataflow projection.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Any

from datacoolie.logging.persistence.diagnostics import (
    DiagnosticBudget,
    OMITTED,
    ensure_json_size,
    join_pointer,
)
from datacoolie.core.models.metrics import METRIC_FIELDS
from datacoolie.watermark.base import WatermarkSerializer


_METRIC_FIELDS = METRIC_FIELDS


def _member(value: Any, name: str) -> Any:
    """Read a required typed member without converting absence to ``None``."""

    return getattr(value, name)


def _enum_value(value: Any) -> Any:
    """Persist enum members by their stable wire value."""

    return value.value if isinstance(value, Enum) else value


def _timestamp(value: Any) -> Any:
    """Keep runtime timestamps as ISO strings, while preserving nulls."""

    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _effective_budget(budget: DiagnosticBudget | None) -> DiagnosticBudget:
    return budget if budget is not None else DiagnosticBudget()


def _diagnostic(
    budget: DiagnosticBudget,
    value: Any,
    *,
    path: str,
) -> Any:
    return budget.normalize(value, path=path)


def _set_diagnostic(
    result: dict[str, Any],
    field_name: str,
    value: Any,
    *,
    budget: DiagnosticBudget,
    path: str,
) -> None:
    encoded = _diagnostic(
        budget,
        value,
        path=join_pointer(path, field_name),
    )
    if encoded is not OMITTED:
        result[field_name] = encoded


def _native_value(
    value: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> Any:
    """Normalize a diagnostic value without an eager full JSON round-trip."""

    effective = _effective_budget(budget)
    encoded = effective.normalize(value, path=path)
    if encoded is OMITTED:
        raise TypeError("diagnostic value was omitted")
    return encoded


def _optional_native_value(
    value: Any,
    *,
    field_name: str,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> Any:
    """Return a bounded diagnostic value or the omission sentinel."""

    effective = _effective_budget(budget)
    encoded = effective.normalize(value, path=join_pointer(path, field_name))
    return encoded


def serialize_watermark_value(
    value: Any,
    *,
    max_bytes: int | None = None,
) -> Any:
    """Encode one authoritative watermark-shaped value with the tagged codec.

    ``max_bytes`` is an optional preflight bound for callers enforcing a total
    record cap.  It rejects before ``WatermarkSerializer.serialize`` allocates
    its full JSON string; it never truncates an authoritative value.
    """

    if value is None:
        return None
    if isinstance(value, dict):
        if max_bytes is not None:
            ensure_json_size(value, max_bytes)
        return json.loads(WatermarkSerializer.serialize(value))
    wrapped = {"value": value}
    if max_bytes is not None:
        ensure_json_size(wrapped, max_bytes)
    # The wrapper is only an implementation detail for scalar bounds.  It
    # must not look like a tagged watermark key: the canonical serializer
    # rejects reserved ``__...__`` mapping keys before encoding.
    encoded = json.loads(WatermarkSerializer.serialize(wrapped))
    return encoded["value"]


def serialize_read_range(
    read_range: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize the public read-range shape and encode only its bounds."""

    if read_range is None:
        return None
    return {
        "column": _member(read_range, "column"),
        "start": serialize_watermark_value(_member(read_range, "start")),
        "end": serialize_watermark_value(_member(read_range, "end")),
        "lower_operator": _member(read_range, "lower_operator"),
        "upper_operator": _member(read_range, "upper_operator"),
    }


def serialize_read_request(
    request: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize the caller-owned request, retaining its original bounds."""

    if request is None:
        return None
    effective = _effective_budget(budget)
    return {
        "watermark_start": serialize_watermark_value(
            _member(request, "watermark_start")
        ),
        "watermark_end": serialize_watermark_value(
            _member(request, "watermark_end")
        ),
        "watermark_start_operator": _member(request, "watermark_start_operator"),
        "watermark_end_operator": _member(request, "watermark_end_operator"),
        "read_range": serialize_read_range(
            _member(request, "read_range"),
            budget=effective,
            path=join_pointer(path, "read_range"),
        ),
        "reference_time": _timestamp(_member(request, "reference_time")),
    }


def serialize_selection(
    selection: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize source-resolved selection evidence."""

    if selection is None:
        return None
    return {
        "mode": _enum_value(_member(selection, "mode")),
        "lower": serialize_watermark_value(_member(selection, "lower")),
        "upper": serialize_watermark_value(_member(selection, "upper")),
        "lower_operator": _member(selection, "lower_operator"),
        "upper_operator": _member(selection, "upper_operator"),
        "combine_operator": _member(selection, "combine_operator"),
        "upper_origin": _enum_value(_member(selection, "upper_origin")),
    }


def serialize_source_runtime(
    source: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize the source phase allowlist."""

    if source is None:
        return None
    effective = _effective_budget(budget)
    result: dict[str, Any] = {
        "start_time": _timestamp(_member(source, "start_time")),
        "end_time": _timestamp(_member(source, "end_time")),
        "duration_seconds": _member(source, "duration_seconds"),
        "status": _enum_value(_member(source, "status")),
    }
    _set_diagnostic(
        result,
        "message",
        _member(source, "message"),
        budget=effective,
        path=path,
    )
    result["rows_read"] = _member(source, "rows_read")
    result["selection"] = serialize_selection(
        _member(source, "selection"),
        budget=effective,
        path=join_pointer(path, "selection"),
    )
    _set_diagnostic(
        result,
        "source_action",
        _member(source, "source_action"),
        budget=effective,
        path=path,
    )
    return result


def serialize_transform_runtime(
    transform: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize the transformer phase allowlist."""

    if transform is None:
        return None
    effective = _effective_budget(budget)
    result: dict[str, Any] = {
        "start_time": _timestamp(_member(transform, "start_time")),
        "end_time": _timestamp(_member(transform, "end_time")),
        "duration_seconds": _member(transform, "duration_seconds"),
        "status": _enum_value(_member(transform, "status")),
    }
    _set_diagnostic(
        result,
        "message",
        _member(transform, "message"),
        budget=effective,
        path=path,
    )
    _set_diagnostic(
        result,
        "transformers_applied",
        _member(transform, "transformers_applied"),
        budget=effective,
        path=path,
    )
    return result


def serialize_destination_runtime(
    destination: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize the destination/maintenance phase allowlist."""

    if destination is None:
        return None
    effective = _effective_budget(budget)
    result: dict[str, Any] = {
        "start_time": _timestamp(_member(destination, "start_time")),
        "end_time": _timestamp(_member(destination, "end_time")),
        "duration_seconds": _member(destination, "duration_seconds"),
        "status": _enum_value(_member(destination, "status")),
        "operation_type": _enum_value(_member(destination, "operation_type")),
        "rows_written": _member(destination, "rows_written"),
        "rows_inserted": _member(destination, "rows_inserted"),
        "rows_updated": _member(destination, "rows_updated"),
        "rows_deleted": _member(destination, "rows_deleted"),
        "files_added": _member(destination, "files_added"),
        "files_removed": _member(destination, "files_removed"),
        "bytes_added": _member(destination, "bytes_added"),
        "bytes_removed": _member(destination, "bytes_removed"),
        "bytes_saved": _member(destination, "bytes_saved"),
    }
    _set_diagnostic(
        result,
        "message",
        _member(destination, "message"),
        budget=effective,
        path=path,
    )
    _set_diagnostic(
        result,
        "operation_details",
        _member(destination, "operation_details"),
        budget=effective,
        path=path,
    )
    return result


def serialize_candidate(
    candidate: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize a typed watermark candidate."""

    if candidate is None:
        return None
    return {
        "values": serialize_watermark_value(_member(candidate, "values")),
        "kind": _enum_value(_member(candidate, "kind")),
    }


def serialize_watermark_runtime(
    watermark: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any] | None:
    """Serialize the watermark phase without checkpoint compatibility fields."""

    if watermark is None:
        return None
    return {
        "load_status": _enum_value(_member(watermark, "load_status")),
        "loaded_watermark": serialize_watermark_value(
            _member(watermark, "loaded_watermark")
        ),
        "merge_base": serialize_watermark_value(_member(watermark, "merge_base")),
        "candidate": serialize_candidate(
            _member(watermark, "candidate"),
            budget=budget,
            path=join_pointer(path, "candidate"),
        ),
        "watermark_to_save": serialize_watermark_value(
            _member(watermark, "watermark_to_save")
        ),
        "save_status": _enum_value(_member(watermark, "save_status")),
        "saved_watermark": serialize_watermark_value(
            _member(watermark, "saved_watermark")
        ),
    }


def serialize_metrics(metrics: Any) -> dict[str, Any] | None:
    """Serialize the nullable execution-metric fields in contract order."""

    if metrics is None:
        return None
    return {name: _member(metrics, name) for name in _METRIC_FIELDS}


def serialize_attempt(
    attempt: Any,
    *,
    budget: DiagnosticBudget | None = None,
    path: str = "",
) -> dict[str, Any]:
    """Serialize one finalized attempt, including only nested contract fields."""

    effective = _effective_budget(budget)
    result: dict[str, Any] = {
        "attempt_number": _member(attempt, "attempt_number"),
        "start_time": _timestamp(_member(attempt, "start_time")),
        "end_time": _timestamp(_member(attempt, "end_time")),
        "duration_seconds": _member(attempt, "duration_seconds"),
        "status": _enum_value(_member(attempt, "status")),
    }
    _set_diagnostic(
        result,
        "message",
        _member(attempt, "message"),
        budget=effective,
        path=path,
    )
    result["source"] = serialize_source_runtime(
        _member(attempt, "source"),
        budget=effective,
        path=join_pointer(path, "source"),
    )
    result["transform"] = serialize_transform_runtime(
        _member(attempt, "transform"),
        budget=effective,
        path=join_pointer(path, "transform"),
    )
    result["destination"] = serialize_destination_runtime(
        _member(attempt, "destination"),
        budget=effective,
        path=join_pointer(path, "destination"),
    )
    result["watermark"] = serialize_watermark_runtime(
        _member(attempt, "watermark"),
        budget=effective,
        path=join_pointer(path, "watermark"),
    )
    return result


__all__ = [
    "DiagnosticBudget",
    "ensure_json_size",
    "serialize_attempt",
    "serialize_candidate",
    "serialize_destination_runtime",
    "serialize_metrics",
    "serialize_read_range",
    "serialize_read_request",
    "serialize_selection",
    "serialize_source_runtime",
    "serialize_transform_runtime",
    "serialize_watermark_runtime",
    "serialize_watermark_value",
]
