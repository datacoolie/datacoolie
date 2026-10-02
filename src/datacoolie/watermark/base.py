"""Watermark serialization and abstract manager.

``WatermarkSerializer`` converts ``Dict[str, Any]`` watermarks to/from JSON
using the ``__datetime__`` sentinel pattern for ``datetime`` round-tripping.

``BaseWatermarkManager`` is the ABC that :class:`WatermarkManager` implements.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
from collections.abc import Mapping
from abc import ABC, abstractmethod
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from numbers import Number
from typing import Any, Dict, Optional

from datacoolie.core.constants import (
    BINARY_PATTERN,
    DATE_PATTERN,
    DATETIME_PATTERN,
    DATE_FOLDER_PARTITION_KEY,
    DECIMAL_PATTERN,
    TIME_PATTERN,
)
from datacoolie.core.exceptions import WatermarkError


# ============================================================================
# WatermarkSerializer — datetime-safe JSON round-trip
# ============================================================================


class WatermarkSerializer:
    """Serialize / deserialize watermark dictionaries to / from JSON.

    ``datetime`` values are stored as::

        {"__datetime__": "2026-02-09T10:30:00+00:00"}

    and restored on deserialization.
    """

    @staticmethod
    def serialize(watermark: Dict[str, Any]) -> str:
        """Convert a watermark dict to a JSON string.

        ``datetime`` values are encoded with the ``__datetime__`` pattern.

        Args:
            watermark: Watermark key-value pairs.

        Returns:
            JSON string.
        """
        if not isinstance(watermark, dict):
            raise TypeError("Watermark must be a dict")

        tagged_names = {
            DATETIME_PATTERN,
            DATE_PATTERN,
            TIME_PATTERN,
            DECIMAL_PATTERN,
            BINARY_PATTERN,
        }

        def _validate(value: Any, path: str) -> None:
            if isinstance(value, float) and not math.isfinite(value):
                raise TypeError(
                    f"Watermark value at {path or '<root>'} must be finite"
                )
            if isinstance(value, (tuple, set, frozenset)):
                raise TypeError(
                    f"Unsupported watermark container at {path or '<root>'}: "
                    f"{type(value).__name__}"
                )
            if isinstance(value, dict):
                for key, nested in value.items():
                    if not isinstance(key, str):
                        raise TypeError(
                            f"Watermark mapping key at {path or '<root>'} "
                            "must be a string"
                        )
                    is_internal_date_folder_key = (
                        not path and key == DATE_FOLDER_PARTITION_KEY
                    )
                    if key in tagged_names or (
                        key.startswith("__")
                        and key.endswith("__")
                        and not is_internal_date_folder_key
                    ):
                        raise TypeError(
                            f"Reserved watermark tag key at "
                            f"{path + '.' if path else ''}{key!r}"
                        )
                    _validate(nested, f"{path}.{key}" if path else key)
            elif isinstance(value, list):
                for index, nested in enumerate(value):
                    _validate(nested, f"{path}[{index}]")

        _validate(watermark, "")

        def _encode(obj: Any) -> Any:
            if isinstance(obj, datetime):  # must precede date — datetime subclasses date
                return {DATETIME_PATTERN: obj.isoformat()}
            if isinstance(obj, date):
                return {DATE_PATTERN: obj.isoformat()}
            if isinstance(obj, time):
                return {TIME_PATTERN: obj.isoformat()}
            if isinstance(obj, Decimal):
                if not obj.is_finite():
                    raise TypeError("Watermark Decimal values must be finite")
                return {DECIMAL_PATTERN: str(obj)}
            if isinstance(obj, bytes):
                return {BINARY_PATTERN: base64.b64encode(obj).decode("ascii")}
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

        return json.dumps(watermark, default=_encode, sort_keys=True)

    @staticmethod
    def deserialize(json_str: Optional[str]) -> Dict[str, Any]:
        """Parse a JSON string back into a watermark dict.

        Restores ``__datetime__`` patterns to ``datetime`` objects.
        Empty / null inputs return ``{}``.  A non-empty malformed value is
        corruption and raises :class:`WatermarkError`; it must not be treated
        as an absent first-run checkpoint.

        Args:
            json_str: JSON string (may be ``None``, empty, ``"null"``).

        Returns:
            Watermark dictionary.
        """
        if json_str is None:
            return {}
        if not isinstance(json_str, str):
            raise WatermarkError("Invalid watermark JSON")
        if not json_str or json_str.strip() in ("", "{}", "null", "None"):
            return {}

        def _reject_nonfinite(token: str) -> Any:
            raise ValueError(f"non-finite JSON constant {token!r}")

        try:
            raw = json.loads(json_str, parse_constant=_reject_nonfinite)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise WatermarkError("Invalid watermark JSON") from exc

        if not isinstance(raw, dict):
            raise WatermarkError("Watermark JSON must contain an object")

        return WatermarkSerializer._restore_datetimes(raw)

    @staticmethod
    def _restore_datetimes(data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively restore supported tagged scalar values."""
        def _restore(value: Any, key: str) -> Any:
            if isinstance(value, dict):
                tagged_keys = {
                    DATETIME_PATTERN,
                    DATE_PATTERN,
                    TIME_PATTERN,
                    DECIMAL_PATTERN,
                    BINARY_PATTERN,
                } & value.keys()
                unknown_tag_keys = {
                    key
                    for key in value
                    if isinstance(key, str)
                    and key.startswith("__")
                    and key.endswith("__")
                    and key not in tagged_keys
                }
                if unknown_tag_keys:
                    raise WatermarkError(f"Invalid watermark tag for {key!r}")
                if tagged_keys and (len(tagged_keys) != 1 or len(value) != 1):
                    raise WatermarkError(
                        f"Invalid watermark tag for {key!r}"
                    )
                if DATETIME_PATTERN in value:
                    try:
                        return datetime.fromisoformat(value[DATETIME_PATTERN])
                    except (binascii.Error, ValueError, TypeError):
                        raise WatermarkError(
                            f"Invalid datetime watermark value for {key!r}"
                        ) from None
                if DATE_PATTERN in value:
                    try:
                        return date.fromisoformat(value[DATE_PATTERN])
                    except (ValueError, TypeError):
                        raise WatermarkError(
                            f"Invalid date watermark value for {key!r}"
                        ) from None
                if TIME_PATTERN in value:
                    try:
                        return time.fromisoformat(value[TIME_PATTERN])
                    except (ValueError, TypeError):
                        raise WatermarkError(
                            f"Invalid time watermark value for {key!r}"
                        ) from None
                if DECIMAL_PATTERN in value:
                    try:
                        decimal = Decimal(value[DECIMAL_PATTERN])
                    except (InvalidOperation, TypeError, ValueError):
                        raise WatermarkError(
                            f"Invalid decimal watermark value for {key!r}"
                        ) from None
                    if not decimal.is_finite():
                        raise WatermarkError(
                            f"Invalid decimal watermark value for {key!r}"
                        )
                    return decimal
                if BINARY_PATTERN in value:
                    try:
                        encoded = value[BINARY_PATTERN]
                        if not isinstance(encoded, str):
                            raise TypeError
                        return base64.b64decode(encoded, validate=True)
                    except (ValueError, TypeError):
                        raise WatermarkError(
                            f"Invalid binary watermark value for {key!r}"
                        ) from None
                return {nested_key: _restore(nested_value, nested_key) for nested_key, nested_value in value.items()}
            if isinstance(value, list):
                return [_restore(item, key) for item in value]
            return value

        result: Dict[str, Any] = {}
        for key, value in data.items():
            result[key] = _restore(value, key)
        return result


# ============================================================================
# Module-level convenience functions
# ============================================================================


def serialize_watermark(watermark: Dict[str, Any]) -> str:
    """Convenience wrapper for :meth:`WatermarkSerializer.serialize`."""
    return WatermarkSerializer.serialize(watermark)


def deserialize_watermark(json_str: str) -> Dict[str, Any]:
    """Convenience wrapper for :meth:`WatermarkSerializer.deserialize`."""
    return WatermarkSerializer.deserialize(json_str)


def is_watermark_empty(watermark: Optional[Dict[str, Any]]) -> bool:
    """Return ``True`` if the watermark is ``None``, empty, or all-``None`` values."""
    if watermark is None:
        return True
    if not watermark:
        return True
    return all(v is None for v in watermark.values())


def _temporal_value(value: Any) -> Any:
    """Normalize a source-declared temporal observation for comparison."""

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            if "T" not in text and " " not in text:
                return date.fromisoformat(text)
            parsed = datetime.fromisoformat(text.replace(" ", "T"))
            return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed
        except ValueError as exc:
            raise WatermarkError(
                f"Ordered temporal watermark value is not parseable: {value!r}"
            ) from exc
    raise WatermarkError(
        f"Ordered temporal watermark value has unsupported type: {type(value).__name__}"
    )


def _numeric_family(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (Decimal, Number)):
        raise WatermarkError(
            f"Ordered numeric watermark value has unsupported type: {type(value).__name__}"
        )
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise WatermarkError("Ordered Decimal watermark values must be finite")
        return "decimal"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise WatermarkError("Ordered float watermark values must be finite")
        return "float"
    return "integer"


def _ordered_max(existing: Any, candidate: Any, *, kind: Optional[str] = None, key: str = "") -> Any:
    """Merge one source-authorized ordered value without narrowing it."""

    if candidate is None:
        return existing
    if existing is None:
        return candidate

    if kind == "temporal":
        left = _temporal_value(existing)
        right = _temporal_value(candidate)
        if type(left) is not type(right):
            raise WatermarkError(
                f"Incompatible ordered temporal watermark types for {key!r}: "
                f"{type(existing).__name__} and {type(candidate).__name__}"
            )
        try:
            return candidate if right > left else existing
        except TypeError as exc:
            raise WatermarkError(
                f"Incompatible ordered temporal watermark values for {key!r}"
            ) from exc

    if kind == "numeric":
        left_family = _numeric_family(existing)
        right_family = _numeric_family(candidate)
        if {left_family, right_family} - {"integer", "decimal"} and left_family != right_family:
            raise WatermarkError(
                f"Incompatible ordered numeric watermark types for {key!r}: "
                f"{type(existing).__name__} and {type(candidate).__name__}"
            )
        try:
            return candidate if candidate > existing else existing
        except (TypeError, ValueError) as exc:
            raise WatermarkError(
                f"Incompatible ordered numeric watermark values for {key!r}"
            ) from exc

    # Without an explicit source-owned semantic kind the value is opaque.
    # Native Python types do not make ordering safe: a custom cursor may be a
    # numeric token or a date-like object whose meaning is replacement, not
    # observed maximum. Only the reader that produced the value may authorize
    # monotonic ordering through ``ordered_keys``.
    return candidate


def merge_watermark_values(
    existing: Optional[Mapping[str, Any]],
    candidate: Optional[Mapping[str, Any]],
    *,
    ordered_keys: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Merge a source observation into stored state by key.

    ``ordered_keys`` is the source-owned semantic map (``numeric`` or
    ``temporal``). Keys without a declared meaning are opaque and replace their
    own value. Keys absent from the candidate remain untouched so a replay that
    observes only one watermark column cannot erase the others.
    """

    result: Dict[str, Any] = dict(existing or {})
    for key, value in (candidate or {}).items():
        if not isinstance(key, str):
            raise WatermarkError("Watermark keys must be strings")
        if value is None and key in result:
            continue
        result[key] = _ordered_max(
            result.get(key),
            value,
            kind=(ordered_keys or {}).get(key),
            key=key,
        )
    return result


# ============================================================================
# BaseWatermarkManager — abstract manager
# ============================================================================


class BaseWatermarkManager(ABC):
    """Abstract watermark manager.

    Concrete implementations decide *where* watermarks are stored (file,
    database, API).  The serializer is an internal concern — callers
    always pass and receive ``Dict[str, Any]``.
    """

    def validate_ready(self) -> None:
        """Validate configuration without reading or writing watermark state."""

    @abstractmethod
    def get_watermark(self, dataflow_id: str) -> Optional[Dict[str, Any]]:
        """Return the current watermark for *dataflow_id*, or ``None``."""

    @abstractmethod
    def save_watermark(
        self,
        dataflow_id: str,
        watermark: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        """Persist a watermark for *dataflow_id*."""

    def serialize(self, watermark: Dict[str, Any]) -> str:
        """Serialize a dict watermark to JSON."""
        return WatermarkSerializer.serialize(watermark)

    def deserialize(self, json_str: str) -> Dict[str, Any]:
        """Deserialize a JSON string to a watermark dict."""
        return WatermarkSerializer.deserialize(json_str)

    def merge_watermark(
        self,
        dataflow_id: str,
        candidate: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Return a monotonic per-key merge with the stored watermark."""

        return merge_watermark_values(self.get_watermark(dataflow_id), candidate)
