"""Bounded normalization for logging-only diagnostic values.

Diagnostic values are useful context, but they must not make a terminal
observation disappear or allocate an unbounded JSON copy.  This module keeps
the policy independent from the structured projection so one budget can be
shared by a whole persisted record.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar


DEFAULT_DIAGNOSTIC_MAX_BYTES = 65_536
DEFAULT_DIAGNOSTIC_MAX_ISSUES = 16
DEFAULT_DIAGNOSTIC_MAX_PATH_BYTES = 192
DEFAULT_DIAGNOSTIC_MAX_DEPTH = 16
DEFAULT_DIAGNOSTIC_MAX_NODES = 4_096


class _Omitted:
    """Private sentinel used when a diagnostic value cannot be retained."""


OMITTED = _Omitted()


class SerializationLimitError(ValueError):
    """Raised before an authoritative value is handed to a full encoder."""


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _utf8_prefix(value: str, max_bytes: int) -> tuple[str, bool]:
    """Return a valid-UTF-8 prefix without encoding the whole string."""

    if max_bytes <= 0:
        return "", bool(value)
    # Most log messages are ASCII.  Consume in modest chunks so a very large
    # caller-owned string never gets copied as one large temporary bytestring.
    result: list[str] = []
    used = 0
    index = 0
    length = str.__len__(value)
    chunk_size = 1024
    while index < length:
        chunk = str.__getitem__(value, slice(index, index + chunk_size))
        chunk_bytes = len(str.encode(chunk, "utf-8"))
        if used + chunk_bytes <= max_bytes:
            result.append(chunk)
            used += chunk_bytes
            index += len(chunk)
            continue
        # The final chunk can contain a multi-byte code point.  Add characters
        # one at a time only until the encoded-byte allowance is exhausted.
        for character in str.__iter__(chunk):
            character_bytes = len(str.encode(character, "utf-8"))
            if used + character_bytes > max_bytes:
                return "".join(result), True
            result.append(character)
            used += character_bytes
        index += len(chunk)
    return "".join(result), False


def _pointer(value: str | None) -> str:
    if not value:
        return ""
    return value if value.startswith("/") else f"/{value}"


def _escape_pointer_segment(value: object) -> str:
    return str(value).replace("~", "~0").replace("/", "~1")


def join_pointer(parent: str | None, segment: object) -> str:
    """Join one JSON-pointer segment and keep callers on pointer syntax."""

    base = _pointer(parent)
    return f"{base}/{_escape_pointer_segment(segment)}"


def _bounded_join_pointer(
    parent: str | None,
    segment: str,
    max_bytes: int,
) -> str:
    """Join a pointer without materializing an unbounded key segment."""

    base = _pointer(parent)
    available = max(0, max_bytes - _bounded_utf8_length(base, max_bytes) - 1)
    escaped: list[str] = []
    used = 0
    for character in segment:
        escaped_character = _escape_pointer_segment(character)
        encoded = escaped_character.encode("utf-8")
        if used + len(encoded) > available:
            break
        escaped.append(escaped_character)
        used += len(encoded)
    return f"{base}/{''.join(escaped)}"


def bounded_pointer(value: str | None, max_bytes: int = DEFAULT_DIAGNOSTIC_MAX_PATH_BYTES) -> str:
    """Bound a JSON-pointer path by UTF-8 bytes without splitting a code point."""

    max_bytes = _positive_int(max_bytes, "max_path_bytes")
    path = _pointer(value)
    encoded_length = _bounded_utf8_length(path, max_bytes)
    if encoded_length <= max_bytes:
        return path
    prefix, _ = _utf8_prefix(path, max_bytes)
    # A pointer escape is an indivisible ``~0`` or ``~1`` pair.  Trimming a
    # terminal tilde keeps a detached/captured marker syntactically valid.
    while prefix.endswith("~"):
        prefix = prefix[:-1]
    return prefix


def _bounded_utf8_length(value: str, limit: int) -> int:
    """Return length up to ``limit + 1`` without copying a large value."""

    used = 0
    for index in range(0, str.__len__(value), 1024):
        chunk = str.__getitem__(value, slice(index, index + 1024))
        used += len(str.encode(chunk, "utf-8"))
        if used > limit:
            return limit + 1
    return used


def _safe_json_size(value: Any) -> int:
    """Encode an already bounded value with the canonical diagnostic options."""

    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )


def _bounded_json_string_size(value: str, limit: int) -> int:
    """Count a JSON string without materializing its encoded copy.

    Diagnostic mapping keys are retained only when the complete key fits in
    the remaining allowance.  Counting escaped characters one at a time
    avoids turning a multi-megabyte key into an equally large temporary JSON
    string merely to discover that the containing object must be omitted.
    """

    total = 2  # JSON string delimiters.
    for character in str.__iter__(value):
        codepoint = ord(character)
        if character in {"\"", "\\"}:
            total += 2
        elif codepoint == 0x08 or codepoint == 0x09 or codepoint == 0x0A:
            total += 2
        elif codepoint == 0x0C or codepoint == 0x0D:
            total += 2
        elif codepoint < 0x20:
            total += 6
        else:
            # Encoding one code point is bounded even when the source string
            # itself is very large.  Lone surrogates remain invalid UTF-8.
            total += len(character.encode("utf-8"))
        if total > limit:
            return limit + 1
    return total


def _bytes_json_size(value: bytes | bytearray) -> int:
    """Return the compact JSON size of ``str(value)`` without converting it."""

    # ``bytes.__repr__`` selects double quotes only when that escapes fewer
    # quote characters.  Matching that choice lets us reject an oversized
    # value before calling ``str`` while preserving the established repr
    # shape for values that fit.
    single_quotes = 0
    double_quotes = 0
    iterator = (
        bytes.__iter__(value)
        if isinstance(value, bytes)
        else bytearray.__iter__(value)
    )
    for item in iterator:
        single_quotes += item == 0x27
        double_quotes += item == 0x22
    quote = 0x22 if single_quotes and not double_quotes else 0x27

    # Prefix/suffix for bytes and bytearray reprs, plus the outer JSON quotes.
    quote_cost = 2 if quote == 0x27 else 4
    total = (
        (1 if isinstance(value, bytes) else len("bytearray(b"))
        + quote_cost
        + (0 if isinstance(value, bytes) else 1)
        + 2
    )
    iterator = (
        bytes.__iter__(value)
        if isinstance(value, bytes)
        else bytearray.__iter__(value)
    )
    for item in iterator:
        if isinstance(value, bytearray) and item == 0x27:
            # bytearray repr escapes single quotes even when it selects a
            # double-quoted bytes payload.
            total += 3
        elif item == quote:
            # Backslash plus the selected quote; JSON escapes both when the
            # selected quote is double, and only the backslash otherwise.
            total += 4 if quote == 0x22 else 3
        elif item == 0x5C:
            total += 4
        elif 0x20 <= item <= 0x7E:
            total += 2 if item == 0x22 else 1
        elif item in {0x09, 0x0A, 0x0D}:
            total += 3
        else:
            total += 5
    return total


def _decimal_text_length(value: Decimal) -> int | None:
    """Bound the ASCII length of ``str(value)`` after a storage guard."""

    try:
        parts = Decimal.as_tuple(value)
        digits_length = len(parts.digits)
        exponent = parts.exponent
        adjusted = Decimal.adjusted(value)
    except (ValueError, OverflowError):
        return None
    sign_length = 1 if parts.sign else 0

    # Decimal uses scientific notation for positive exponents and for values
    # whose adjusted exponent is below -6.  Otherwise it uses fixed notation.
    if exponent > 0 or adjusted < -6:
        mantissa_length = 1 if digits_length == 1 else digits_length + 1
        return sign_length + mantissa_length + 2 + len(str(abs(adjusted)))
    position = digits_length + exponent
    if position <= 0:
        return sign_length + 2 - position + digits_length
    if position >= digits_length:
        return sign_length + position
    return sign_length + digits_length + 1


def _json_scalar(value: Any, *, max_bytes: int = DEFAULT_DIAGNOSTIC_MAX_BYTES) -> Any:
    """Return supported generic-JSON scalar parity, or ``OMITTED``.

    ``json_default`` historically represented dates, decimals, bytes and
    SecretStr values as strings.  Keep those representations for diagnostics
    while refusing arbitrary object stringification, which can execute an
    unbounded or side-effecting ``__str__`` implementation.
    """

    if value is None or isinstance(value, bool):
        return value
    max_bytes = max(0, max_bytes)
    if isinstance(value, str):
        return str.__str__(value)
    if isinstance(value, int) and not isinstance(value, bool):
        # Avoid converting a pathological integer to a multi-megabyte string.
        if int.bit_length(value) > (max_bytes + 32) * 4:
            return OMITTED
        return int.__int__(value)
    if isinstance(value, float):
        scalar = float.__float__(value)
        return scalar if math.isfinite(scalar) else OMITTED
    if isinstance(value, datetime):
        return datetime.isoformat(value)
    if isinstance(value, date):
        return date.isoformat(value)
    if isinstance(value, time):
        return time.isoformat(value)
    if isinstance(value, Decimal):
        if not Decimal.is_finite(value):
            return OMITTED
        # Decimal.__sizeof__ reflects coefficient storage.  Check it before
        # as_tuple()/str(), since a caller can provide millions of digits with
        # a small adjusted exponent (for example ``0.111...``).
        if Decimal.__sizeof__(value) > max(max_bytes, 256):
            return OMITTED
        text_length = _decimal_text_length(value)
        if text_length is None or text_length + 2 > max_bytes:
            return OMITTED
        return Decimal.__str__(value)
    if isinstance(value, (bytes, bytearray)):
        # ``json_default`` uses ``str(bytes)``; keep that exact public shape.
        if _bytes_json_size(value) > max_bytes:
            return OMITTED
        return (
            bytes.__str__(value)
            if isinstance(value, bytes)
            else bytearray.__str__(value)
        )
    if type(value).__name__ == "SecretStr":
        return "***"
    if isinstance(value, Enum):
        # Avoid an overridden ``__str__`` while retaining the generic enum
        # representation used by ``json_default``.
        return _json_scalar(Enum.__str__(value), max_bytes=max_bytes)
    return OMITTED


@dataclass
class DiagnosticBudget:
    """Shared encoded-byte and traversal allowance for one persisted record.

    A budget is intentionally stateful.  Call :meth:`normalize` in stable
    projection order and pass the same instance to every serializer involved
    in a record.  ``issues`` is a detached list suitable for the optional
    ``diagnostic_issues`` wire field.
    """

    max_bytes: int = DEFAULT_DIAGNOSTIC_MAX_BYTES
    max_issues: int = DEFAULT_DIAGNOSTIC_MAX_ISSUES
    max_path_bytes: int = DEFAULT_DIAGNOSTIC_MAX_PATH_BYTES
    max_depth: int = DEFAULT_DIAGNOSTIC_MAX_DEPTH
    max_nodes: int = DEFAULT_DIAGNOSTIC_MAX_NODES
    _used_bytes: int = field(default=0, init=False, repr=False)
    _nodes: int = field(default=0, init=False, repr=False)
    _issues: list[dict[str, str]] = field(default_factory=list, init=False, repr=False)
    _overflow_issue_added: bool = field(default=False, init=False, repr=False)

    _OVERFLOW_MARKER: ClassVar[dict[str, str]] = {"field": "", "reason": "omitted"}

    def __post_init__(self) -> None:
        self.max_bytes = _positive_int(self.max_bytes, "diagnostic_max_bytes")
        self.max_issues = _positive_int(self.max_issues, "diagnostic_max_issues")
        self.max_path_bytes = _positive_int(self.max_path_bytes, "diagnostic_max_path_bytes")
        self.max_depth = _positive_int(self.max_depth, "diagnostic_max_depth")
        self.max_nodes = _positive_int(self.max_nodes, "diagnostic_max_nodes")

    @property
    def used_bytes(self) -> int:
        return self._used_bytes

    @property
    def remaining_bytes(self) -> int:
        return max(0, self.max_bytes - self._used_bytes)

    @property
    def nodes(self) -> int:
        return self._nodes

    @property
    def issues(self) -> list[dict[str, str]]:
        return [dict(issue) for issue in self._issues]

    @property
    def diagnostic_issues(self) -> list[dict[str, str]]:
        """Alias used by projection/capture code when assembling a record."""

        return self.issues

    def add_issue(self, field: str | None, reason: str) -> None:
        """Append one bounded marker, retaining a final overflow marker."""

        if self.max_issues == 1:
            if not self._overflow_issue_added:
                self._issues.append(dict(self._OVERFLOW_MARKER))
                self._overflow_issue_added = True
            return
        marker = {
            "field": bounded_pointer(field, self.max_path_bytes),
            "reason": (
                "truncated"
                if reason == "truncated"
                else "invalid"
                if reason == "invalid"
                else "omitted"
            ),
        }
        if len(self._issues) < self.max_issues - 1:
            self._issues.append(marker)
            return
        if not self._overflow_issue_added:
            self._issues.append(dict(self._OVERFLOW_MARKER))
            self._overflow_issue_added = True

    def normalize(self, value: Any, *, path: str | None = None) -> Any:
        """Normalize one diagnostic value and account for its encoded bytes."""

        issue_path = bounded_pointer(path, self.max_path_bytes)
        if self.remaining_bytes <= 0:
            self.add_issue(issue_path, "omitted")
            return OMITTED

        local_issues: list[tuple[str, str]] = []
        normalized, size, reason = self._normalize(
            value,
            path=issue_path,
            depth=0,
            active_ids=set(),
            limit=self.remaining_bytes,
            local_issues=local_issues,
        )
        if normalized is OMITTED:
            self.add_issue(issue_path, reason or "invalid")
            return OMITTED
        self._used_bytes += size
        for issue_field, issue_reason in local_issues:
            self.add_issue(issue_field, issue_reason)
        return normalized

    def _normalize(
        self,
        value: Any,
        *,
        path: str,
        depth: int,
        active_ids: set[int],
        limit: int,
        local_issues: list[tuple[str, str]],
    ) -> tuple[Any, int, str | None]:
        if self._nodes >= self.max_nodes:
            return OMITTED, 0, "nodes"
        self._nodes += 1
        if depth > self.max_depth:
            return OMITTED, 0, "depth"

        scalar = _json_scalar(value, max_bytes=limit)
        if scalar is not OMITTED:
            if isinstance(scalar, str):
                # JSON quotes/escapes consume part of the allowance, so leave
                # a small margin and then verify the exact encoded size.
                try:
                    prefix, truncated = _utf8_prefix(scalar, max(0, limit))
                    low, high = 0, len(prefix)
                    best = ""
                    best_size = 2
                    while low <= high:
                        midpoint = (low + high) // 2
                        candidate = prefix[:midpoint]
                        size = _safe_json_size(candidate)
                        if size <= limit:
                            best = candidate
                            best_size = size
                            low = midpoint + 1
                        else:
                            high = midpoint - 1
                    if not best and limit < 2:
                        return OMITTED, 0, "omitted"
                except (TypeError, ValueError, UnicodeError):
                    return OMITTED, 0, "invalid"
                if truncated or len(best) < len(scalar):
                    local_issues.append((path, "truncated"))
                return best, best_size, None
            try:
                size = _safe_json_size(scalar)
            except (TypeError, ValueError):
                return OMITTED, 0, "invalid"
            return (scalar, size, None) if size <= limit else (OMITTED, 0, "omitted")

        if not isinstance(value, (Mapping, list, tuple)):
            return OMITTED, 0, "invalid"
        identity = id(value)
        if identity in active_ids:
            return OMITTED, 0, "cycle"
        active_ids.add(identity)
        try:
            if isinstance(value, Mapping):
                result: dict[str, Any] = {}
                total_size = 2
                for key, nested in value.items():
                    if not isinstance(key, (str, int, float, bool)) and key is not None:
                        return OMITTED, 0, "invalid"
                    key_scalar = _json_scalar(key, max_bytes=limit)
                    if key_scalar is OMITTED:
                        return OMITTED, 0, "invalid"
                    if isinstance(key_scalar, str):
                        key_text = key_scalar
                    elif key_scalar is None:
                        key_text = "null"
                    elif isinstance(key_scalar, bool):
                        key_text = "true" if key_scalar else "false"
                    else:
                        key_text = str(key_scalar)
                    key_path = _bounded_join_pointer(
                        path,
                        key_text,
                        self.max_path_bytes,
                    )
                    try:
                        key_size = _bounded_json_string_size(key_text, max(0, limit - total_size))
                    except UnicodeError:
                        return OMITTED, 0, "invalid"
                    if key_size > max(0, limit - total_size):
                        return OMITTED, 0, "omitted"
                    child_limit = max(0, limit - total_size - key_size - 2)
                    issue_count = len(local_issues)
                    child, _, child_reason = self._normalize(
                        nested,
                        path=key_path,
                        depth=depth + 1,
                        active_ids=active_ids,
                        limit=child_limit,
                        local_issues=local_issues,
                    )
                    if child is OMITTED:
                        return OMITTED, 0, child_reason or "omitted"
                    # A nested string that needed truncation makes the
                    # containing diagnostic object oversized.  Keep the
                    # field-level contract simple: strings can be truncated
                    # when they are the diagnostic value; containers are
                    # omitted as a whole when any child exceeds its bound.
                    if len(local_issues) != issue_count:
                        return OMITTED, 0, "omitted"
                    result[key_text] = child
                    total_size += key_size + 1 + _safe_json_size(child)
                    if len(result) > 1:
                        total_size += 1
                    if total_size > limit:
                        return OMITTED, 0, "omitted"
                size = _safe_json_size(result)
                return (result, size, None) if size <= limit else (OMITTED, 0, "omitted")

            result_list: list[Any] = []
            total_size = 2
            for index, nested in enumerate(value):
                child_limit = max(0, limit - total_size - 1)
                issue_count = len(local_issues)
                child, _, child_reason = self._normalize(
                    nested,
                    path=join_pointer(path, index),
                    depth=depth + 1,
                    active_ids=active_ids,
                    limit=child_limit,
                    local_issues=local_issues,
                )
                if child is OMITTED:
                    return OMITTED, 0, child_reason or "omitted"
                if len(local_issues) != issue_count:
                    return OMITTED, 0, "omitted"
                result_list.append(child)
                total_size += _safe_json_size(child)
                if len(result_list) > 1:
                    total_size += 1
                if total_size > limit:
                    return OMITTED, 0, "omitted"
            size = _safe_json_size(result_list)
            return (result_list, size, None) if size <= limit else (OMITTED, 0, "omitted")
        finally:
            active_ids.remove(identity)


def normalize_diagnostic(
    value: Any,
    *,
    budget: DiagnosticBudget,
    path: str | None = None,
) -> Any:
    """Functional spelling for callers that do not need a budget method."""

    return budget.normalize(value, path=path)


def normalize_issue_entries(
    values: Any,
    *,
    max_issues: int = DEFAULT_DIAGNOSTIC_MAX_ISSUES,
    max_path_bytes: int = DEFAULT_DIAGNOSTIC_MAX_PATH_BYTES,
) -> list[dict[str, str]]:
    """Validate detached issue markers loaded from capture memory/files."""

    if not isinstance(values, list):
        return []
    budget = DiagnosticBudget(
        max_bytes=1,
        max_issues=max_issues,
        max_path_bytes=max_path_bytes,
    )
    for item in values:
        if not isinstance(item, Mapping):
            continue
        field = item.get("field")
        reason = item.get("reason")
        if not isinstance(field, str) or not isinstance(reason, str):
            continue
        budget.add_issue(field, reason)
    return budget.issues


def _estimate_json_size(
    value: Any,
    *,
    limit: int,
    depth: int,
    nodes: list[int],
    active_ids: set[int],
) -> int:
    """Conservatively screen an authoritative value before full encoding."""

    nodes[0] += 1
    if nodes[0] > DEFAULT_DIAGNOSTIC_MAX_NODES:
        raise SerializationLimitError("value exceeds bounded traversal nodes")
    if depth > DEFAULT_DIAGNOSTIC_MAX_DEPTH:
        raise SerializationLimitError("value exceeds bounded traversal depth")
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float):
            value = float.__float__(value)
        if isinstance(value, float) and not math.isfinite(value):
            raise TypeError("non-finite value")
        if isinstance(value, int) and int.bit_length(value) > (limit + 32) * 4:
            raise SerializationLimitError("value exceeds encoded size limit")
        try:
            return _safe_json_size(value)
        except (TypeError, ValueError) as exc:
            raise TypeError("unsupported scalar") from exc
    if isinstance(value, str):
        value = str.__str__(value)
        if _bounded_json_string_size(value, max(0, limit)) > limit:
            raise SerializationLimitError("value exceeds encoded size limit")
        return _bounded_json_string_size(value, max(0, limit))
    if isinstance(value, datetime):
        text = datetime.isoformat(value)
        return _estimate_json_size(
            text,
            limit=max(0, limit - 18),
            depth=depth,
            nodes=nodes,
            active_ids=active_ids,
        ) + 18
    if isinstance(value, date):
        text = date.isoformat(value)
        return _estimate_json_size(
            text,
            limit=max(0, limit - 14),
            depth=depth,
            nodes=nodes,
            active_ids=active_ids,
        ) + 14
    if isinstance(value, time):
        text = time.isoformat(value)
        return _estimate_json_size(
            text,
            limit=max(0, limit - 14),
            depth=depth,
            nodes=nodes,
            active_ids=active_ids,
        ) + 14
    if isinstance(value, Decimal):
        if not Decimal.is_finite(value):
            raise SerializationLimitError("value exceeds encoded size limit")
        if Decimal.__sizeof__(value) > max(limit, 256):
            raise SerializationLimitError("value exceeds encoded size limit")
        text_length = _decimal_text_length(value)
        if text_length is None or text_length + 19 > limit:
            raise SerializationLimitError("value exceeds encoded size limit")
        text = Decimal.__str__(value)
        return _estimate_json_size(
            text,
            limit=max(0, limit - 17),
            depth=depth,
            nodes=nodes,
            active_ids=active_ids,
        ) + 17
    if isinstance(value, bytes):
        # Watermark encoding expands bytes with base64 by roughly 4/3; leave a
        # conservative margin before WatermarkSerializer allocates it.
        value_length = bytes.__len__(value)
        if value_length > max(0, (limit - 18) * 3 // 4):
            raise SerializationLimitError("value exceeds encoded size limit")
        encoded_length = 4 * ((value_length + 2) // 3)
        return min(limit + 1, encoded_length + 18)
    if not isinstance(value, (Mapping, list, tuple)):
        raise TypeError(f"unsupported value type: {type(value).__name__}")
    identity = id(value)
    if identity in active_ids:
        raise SerializationLimitError("cyclic value")
    active_ids.add(identity)
    try:
        total = 2
        if isinstance(value, Mapping):
            emitted = 0
            for key, nested in value.items():
                if not isinstance(key, str):
                    raise TypeError("mapping keys must be strings")
                key = str.__str__(key)
                key_limit = max(0, limit - total)
                key_size = _bounded_json_string_size(key, key_limit)
                if key_size > key_limit:
                    raise SerializationLimitError("value exceeds encoded size limit")
                # Include the key separator and a conservative comma margin.
                if emitted:
                    total += 2  # ``", "`` in WatermarkSerializer's output.
                total += key_size + 2  # ``": "``
                total += _estimate_json_size(
                    nested,
                    limit=max(0, limit - total),
                    depth=depth + 1,
                    nodes=nodes,
                    active_ids=active_ids,
                )
                if total > limit:
                    raise SerializationLimitError("value exceeds encoded size limit")
                emitted += 1
        else:
            for index, nested in enumerate(value):
                if index:
                    total += 2  # ``, `` in WatermarkSerializer's output.
                total += _estimate_json_size(
                    nested,
                    limit=max(0, limit - total),
                    depth=depth + 1,
                    nodes=nodes,
                    active_ids=active_ids,
                )
                if total > limit:
                    raise SerializationLimitError("value exceeds encoded size limit")
        return total
    finally:
        active_ids.remove(identity)


def ensure_json_size(value: Any, max_bytes: int) -> None:
    """Reject an oversized authoritative value before a full JSON copy."""

    max_bytes = _positive_int(max_bytes, "max_record_bytes")
    estimate = _estimate_json_size(
        value,
        limit=max_bytes,
        depth=0,
        nodes=[0],
        active_ids=set(),
    )
    if estimate > max_bytes:
        raise SerializationLimitError("value exceeds encoded size limit")


__all__ = [
    "DEFAULT_DIAGNOSTIC_MAX_BYTES",
    "DEFAULT_DIAGNOSTIC_MAX_DEPTH",
    "DEFAULT_DIAGNOSTIC_MAX_ISSUES",
    "DEFAULT_DIAGNOSTIC_MAX_NODES",
    "DEFAULT_DIAGNOSTIC_MAX_PATH_BYTES",
    "DiagnosticBudget",
    "OMITTED",
    "SerializationLimitError",
    "bounded_pointer",
    "ensure_json_size",
    "join_pointer",
    "normalize_diagnostic",
    "normalize_issue_entries",
]
