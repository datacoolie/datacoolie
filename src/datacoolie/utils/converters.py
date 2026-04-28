"""Type conversion utilities for the DataCoolie framework."""

from __future__ import annotations

import json
import re as _re
from datetime import date, datetime, time, timezone
from typing import Any


def parse_json(
    value: str | dict[str, Any] | None,
    *,
    raise_on_error: bool = True,
) -> dict[str, Any]:
    """Parse a JSON string to a dictionary.

    If *value* is already a ``dict``, it is returned as-is.
    ``None`` or empty strings yield an empty ``dict``.

    Args:
        value: JSON string, dictionary, or ``None``.
        raise_on_error: When ``True`` (default), raise on invalid JSON.

    Returns:
        Parsed dictionary.

    Raises:
        ValueError: If the JSON is invalid and *raise_on_error* is ``True``.
    """
    if isinstance(value, dict):
        return value

    if value is None or (isinstance(value, str) and not value.strip()):
        return {}

    if isinstance(value, str):
        try:
            result = json.loads(value)
            if not isinstance(result, dict):
                if raise_on_error:
                    raise ValueError(f"Expected JSON object, got {type(result).__name__}: {value}")
                return {}
            return result
        except json.JSONDecodeError as exc:
            if raise_on_error:
                raise ValueError(f"Invalid JSON format: {value}") from exc
            return {}

    if raise_on_error:
        raise ValueError(f"Expected str or dict, got {type(value).__name__}: {value}")
    return {}


_TRUTH_MAP: dict[str, bool] = {
    "true": True,
    "yes": True,
    "1": True,
    "on": True,
    "t": True,
    "y": True,
    "false": False,
    "no": False,
    "0": False,
    "off": False,
    "f": False,
    "n": False,
    "": False,
}


def convert_to_bool(value: str | bool | int | None) -> bool:
    """Convert various types to ``bool``.

    Supports string representations like ``"true"``, ``"yes"``, ``"1"``,
    ``"on"`` and their negatives.

    Args:
        value: Value to convert.

    Returns:
        Boolean result.

    Raises:
        ValueError: If the value cannot be interpreted as a boolean.
    """
    if value is None:
        return False

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        raise ValueError(f"Invalid boolean integer: {value}")

    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in _TRUTH_MAP:
            return _TRUTH_MAP[normalised]

    raise ValueError(f"Invalid boolean value: {value}")


def convert_to_int(value: str | int | float | None) -> int | None:
    """Convert various types to ``int``.

    Args:
        value: Value to convert.

    Returns:
        Integer value, or ``None`` when the input is ``None``.

    Raises:
        ValueError: If the value cannot be converted.
    """
    if value is None:
        return None

    if isinstance(value, int) and not isinstance(value, bool):
        return value

    if isinstance(value, float):
        if value != int(value):
            raise ValueError(f"Cannot convert non-whole float to integer: {value}")
        return int(value)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            try:
                return int(float(stripped))
            except ValueError:
                raise ValueError(f"Invalid integer string: {value}") from None

    raise ValueError(f"Cannot convert to integer: {value}")


# ---------------------------------------------------------------------------
# JSON serialization converters
# ---------------------------------------------------------------------------


def custom_json_encoder(obj: Any) -> Any:
    """JSON encoder callback for ``datetime`` / ``date`` / ``time`` objects."""
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def json_default(obj: Any) -> Any:
    """JSON serializer for ``json.dumps(default=...)``.  Never raises.

    * ``datetime`` / ``date`` → ISO-8601 string
    * everything else → ``str(obj)``
    """
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    return str(obj)


def as_json(value: Any) -> str | None:
    """Return ``json.dumps(value)`` when *value* is truthy, else ``None``."""
    return json.dumps(value, default=json_default) if value else None


# ---------------------------------------------------------------------------
# String format converters
# ---------------------------------------------------------------------------

_RE_CAMEL_BOUNDARY = _re.compile(r"([a-z0-9])([A-Z])")
_RE_UPPER_RUN = _re.compile(r"([A-Z]+)([A-Z][a-z])")
_RE_NON_ALNUM = _re.compile(r"[^a-z0-9]+")


def to_snake_case(name: str) -> str:
    """Convert an arbitrary column name to ``snake_case``.

    Column names that already start with an underscore (e.g. system
    columns such as ``__created_at``) are returned unchanged.

    Examples::

        "MyColumn"     → "my_column"
        "order-date"   → "order_date"
        "Column Name"  → "column_name"
        "HTTPStatus"   → "http_status"
        "123"          → "_123"
        "col@#name"    → "col_name"
        "__created_at" → "__created_at"  # unchanged
    """
    if name.startswith("_"):
        return name
    s = _RE_CAMEL_BOUNDARY.sub(r"\1_\2", name)
    s = _RE_UPPER_RUN.sub(r"\1_\2", s)
    s = s.lower()
    s = _RE_NON_ALNUM.sub("_", s)
    s = s.strip("_")
    if s and s[0].isdigit():
        s = f"_{s}"
    return s


def to_lower_case(name: str) -> str:
    """Convert an arbitrary column name to lowercased form.

    Column names that already start with an underscore (e.g. system
    columns such as ``__created_at``) are returned unchanged.

    Examples::

        "MyColumn"     → "mycolumn"
        "HTTPStatus"   → "httpstatus"
        "order-date"   → "order_date"
        "Column Name"  → "column_name"
        "123"          → "_123"
        "col@#name"    → "col_name"
        "__created_at" → "__created_at"  # unchanged
    """
    if name.startswith("_"):
        return name
    s = name.lower()
    s = _RE_NON_ALNUM.sub("_", s)
    s = s.strip("_")
    if s and s[0].isdigit():
        s = f"_{s}"
    return s


# ---------------------------------------------------------------------------
# Collection converters
# ---------------------------------------------------------------------------


def convert_string_to_list(
    value: str | list[str] | None,
    separator: str = ",",
) -> list[str]:
    """Convert a delimited ``str`` to a list of strings.

    Already-``list`` values pass through. ``None`` yields ``[]``.

    Args:
        value: Delimited string, list, or ``None``.
        separator: Delimiter character (default ``","``).

    Returns:
        List of non-empty, stripped strings.
    """
    if not value:
        return []
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    return [part.strip() for part in value.split(separator) if part.strip()]
