"""Watermark serialization and abstract manager.

``WatermarkSerializer`` converts ``Dict[str, Any]`` watermarks to/from JSON
using the ``__datetime__`` sentinel pattern for ``datetime`` round-tripping.

``BaseWatermarkManager`` is the ABC that :class:`WatermarkManager` implements.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import date, datetime, time, timezone
from typing import Any, Dict, Optional

from datacoolie.core.constants import DATE_PATTERN, DATETIME_PATTERN, TIME_PATTERN


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
        def _encode(obj: Any) -> Any:
            if isinstance(obj, datetime):  # must precede date — datetime subclasses date
                return {DATETIME_PATTERN: obj.isoformat()}
            if isinstance(obj, date):
                return {DATE_PATTERN: obj.isoformat()}
            if isinstance(obj, time):
                return {TIME_PATTERN: obj.isoformat()}
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

        return json.dumps(watermark, default=_encode, sort_keys=True)

    @staticmethod
    def deserialize(json_str: str) -> Dict[str, Any]:
        """Parse a JSON string back into a watermark dict.

        Restores ``__datetime__`` patterns to ``datetime`` objects.
        Empty / null inputs return ``{}``.

        Args:
            json_str: JSON string (may be ``None``, empty, ``"null"``).

        Returns:
            Watermark dictionary.
        """
        if not json_str or json_str.strip() in ("", "{}", "null", "None"):
            return {}

        try:
            raw = json.loads(json_str)
        except json.JSONDecodeError:
            return {}

        if not isinstance(raw, dict):
            return {}

        return WatermarkSerializer._restore_datetimes(raw)

    @staticmethod
    def _restore_datetimes(data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively restore ``__datetime__``, ``__date__``, and ``__time__`` sentinels."""
        result: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict):
                if DATETIME_PATTERN in value:
                    try:
                        result[key] = datetime.fromisoformat(value[DATETIME_PATTERN])
                    except (ValueError, TypeError):
                        result[key] = value[DATETIME_PATTERN]
                elif DATE_PATTERN in value:
                    try:
                        result[key] = date.fromisoformat(value[DATE_PATTERN])
                    except (ValueError, TypeError):
                        result[key] = value[DATE_PATTERN]
                elif TIME_PATTERN in value:
                    try:
                        result[key] = time.fromisoformat(value[TIME_PATTERN])
                    except (ValueError, TypeError):
                        result[key] = value[TIME_PATTERN]
                else:
                    result[key] = WatermarkSerializer._restore_datetimes(value)
            else:
                result[key] = value
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


# ============================================================================
# BaseWatermarkManager — abstract manager
# ============================================================================


class BaseWatermarkManager(ABC):
    """Abstract watermark manager.

    Concrete implementations decide *where* watermarks are stored (file,
    database, API).  The serializer is an internal concern — callers
    always pass and receive ``Dict[str, Any]``.
    """

    def __init__(self) -> None:
        self._serializer = WatermarkSerializer()

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
        return self._serializer.serialize(watermark)

    def deserialize(self, json_str: str) -> Dict[str, Any]:
        """Deserialize a JSON string to a watermark dict."""
        return self._serializer.deserialize(json_str)
