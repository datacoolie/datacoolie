"""Bounded, streaming JSON encoding used by logging admission paths.

``json.dumps`` materialises the complete JSON string before a caller can
inspect its size.  That is unsafe for a logging boundary because a caller can
hand the logger a very large authored value.  This module first walks the
value and counts its UTF-8 representation in bounded chunks.  Only a value
whose complete representation fits the requested limit is rendered.

The encoder intentionally mirrors the compact subset used by the logging
writer while retaining the useful ``json.dumps`` options needed by metadata
projection (the generic ``default`` callback, ``ensure_ascii``, separators and
``sort_keys``).
"""

from __future__ import annotations

import json
from json.encoder import encode_basestring, encode_basestring_ascii
from typing import Any, Callable, Iterable, Optional


_STRING_CHUNK_CHARS = 8192
_DEFAULT_SEPARATORS = (", ", ": ")


class BoundedEncodingError(ValueError):
    """Raised when a complete JSON representation exceeds its byte limit.

    ``known_size`` is populated only after the complete value has been
    counted.  It is therefore an exact encoded byte count, including the
    optional JSON Lines newline, rather than a lower bound from a partial
    traversal.  A render-time failure caused by a mutable/custom value keeps
    it as ``None``.
    """

    def __init__(
        self,
        message: str,
        *,
        known_size: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.known_size = known_size
        # ``size`` is a small compatibility convenience for callers that use
        # size terminology for admission counters.
        self.size = known_size
        self.encoded_size = known_size
        self.max_bytes = max_bytes


# The longer spelling is useful to callers that want to distinguish this from
# other bounded diagnostics errors while keeping one exception type.
BoundedJsonEncodingError = BoundedEncodingError


def _validate_max_bytes(max_bytes: Optional[int]) -> None:
    if max_bytes is None:
        return
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer or None")


def _normalise_separators(
    separators: Optional[tuple[str, str]],
) -> tuple[str, str]:
    if separators is None:
        return _DEFAULT_SEPARATORS
    if (
        not isinstance(separators, tuple)
        or len(separators) != 2
        or not all(isinstance(item, str) for item in separators)
    ):
        raise TypeError("separators must be a tuple of two strings")
    return separators


def _utf8_size(value: str) -> int:
    """Count a bounded text fragment without creating a full byte copy."""

    return len(value.encode("utf-8"))


def _quoted_fragments(value: str, *, ensure_ascii: bool) -> Iterable[str]:
    """Yield JSON string fragments whose source slices stay bounded."""

    encode_string = encode_basestring_ascii if ensure_ascii else encode_basestring
    yield '"'
    for start in range(0, len(value), _STRING_CHUNK_CHARS):
        # ``encode_basestring*`` returns a quoted string.  Encoding one small
        # slice at a time avoids the large temporary produced for a whole
        # diagnostic or metadata string.
        encoded = encode_string(value[start : start + _STRING_CHUNK_CHARS])
        yield encoded[1:-1]
    yield '"'


class _Sink:
    """Small sink abstraction shared by text and byte render passes."""

    def __init__(self, max_bytes: Optional[int]) -> None:
        self.max_bytes = max_bytes
        self.size = 0

    def write(self, value: str) -> None:
        raise NotImplementedError

    def _check(self, fragment_bytes: int) -> None:
        next_size = self.size + fragment_bytes
        if self.max_bytes is not None and next_size > self.max_bytes:
            raise BoundedEncodingError(
                "encoded JSON exceeded max_bytes during rendering",
                max_bytes=self.max_bytes,
            )
        self.size = next_size


class _TextSink(_Sink):
    def __init__(self, max_bytes: Optional[int]) -> None:
        super().__init__(max_bytes)
        self.parts: list[str] = []

    def write(self, value: str) -> None:
        self._check(_utf8_size(value))
        self.parts.append(value)

    def finish(self) -> str:
        return "".join(self.parts)


class _BytesSink(_Sink):
    def __init__(self, max_bytes: Optional[int]) -> None:
        super().__init__(max_bytes)
        self.value = bytearray()

    def write(self, value: str) -> None:
        encoded = value.encode("utf-8")
        self._check(len(encoded))
        self.value.extend(encoded)

    def finish(self) -> bytes:
        return bytes(self.value)


class _JsonWalker:
    """Measure and render a JSON value without whole-value string copies."""

    def __init__(
        self,
        *,
        default: Optional[Callable[[Any], Any]],
        ensure_ascii: bool,
        allow_nan: bool,
        separators: tuple[str, str],
        sort_keys: bool,
        skipkeys: bool,
        check_circular: bool,
    ) -> None:
        self.default = default
        self.ensure_ascii = ensure_ascii
        self.allow_nan = allow_nan
        self.item_separator, self.key_separator = separators
        self._item_separator_size = _utf8_size(self.item_separator)
        self._key_separator_size = _utf8_size(self.key_separator)
        self.sort_keys = sort_keys
        self.skipkeys = skipkeys
        self.check_circular = check_circular
        self._active: set[int] = set()
        self._default_active: set[int] = set()
        self._measuring = False

    def measure(self, value: Any) -> int:
        # Key ordering cannot change a JSON byte count.  Avoid sorting a
        # potentially huge mapping during the bounded preflight; the render
        # pass sorts only after the count proves that at most max_bytes of
        # output can be retained.
        self._measuring = True
        try:
            return self._measure(value)
        finally:
            self._measuring = False

    def render(self, value: Any, sink: _Sink) -> None:
        self._render(value, sink)

    def _measure(self, value: Any) -> int:
        if value is None:
            return 4
        if value is True:
            return 4
        if value is False:
            return 5
        if isinstance(value, str):
            return sum(_utf8_size(fragment) for fragment in _quoted_fragments(value, ensure_ascii=self.ensure_ascii))
        if isinstance(value, (int, float)):
            return _utf8_size(self._number_text(value))
        if isinstance(value, (list, tuple)):
            return self._measure_sequence(value)
        if isinstance(value, dict):
            return self._measure_mapping(value)
        return self._measure_default(value)

    def _measure_sequence(self, value: list[Any] | tuple[Any, ...]) -> int:
        self._enter_container(value)
        try:
            total = 2
            for index, item in enumerate(value):
                if index:
                    total += self._item_separator_size
                total += self._measure(item)
            return total
        finally:
            self._leave_container(value)

    def _measure_mapping(self, value: dict[Any, Any]) -> int:
        self._enter_container(value)
        try:
            items = (
                sorted(value.items())
                if self.sort_keys and not self._measuring
                else value.items()
            )
            total = 2
            emitted = 0
            for key, item in items:
                key_text = self._key_text(key)
                if key_text is None:
                    continue
                if emitted:
                    total += self._item_separator_size
                total += sum(
                    _utf8_size(fragment)
                    for fragment in _quoted_fragments(
                        key_text,
                        ensure_ascii=self.ensure_ascii,
                    )
                )
                total += self._key_separator_size
                total += self._measure(item)
                emitted += 1
            return total
        finally:
            self._leave_container(value)

    def _measure_default(self, value: Any) -> int:
        if self.default is None:
            raise TypeError(
                f"Object of type {type(value).__name__} is not JSON serializable"
            )
        object_id = id(value)
        if object_id in self._default_active:
            raise ValueError("Circular reference detected")
        self._default_active.add(object_id)
        try:
            return self._measure(self.default(value))
        finally:
            self._default_active.remove(object_id)

    def _render(self, value: Any, sink: _Sink) -> None:
        if value is None:
            sink.write("null")
            return
        if value is True:
            sink.write("true")
            return
        if value is False:
            sink.write("false")
            return
        if isinstance(value, str):
            self._write_string(value, sink)
            return
        if isinstance(value, (int, float)):
            self._write_text(self._number_text(value), sink)
            return
        if isinstance(value, (list, tuple)):
            self._render_sequence(value, sink)
            return
        if isinstance(value, dict):
            self._render_mapping(value, sink)
            return
        self._render_default(value, sink)

    def _render_sequence(self, value: list[Any] | tuple[Any, ...], sink: _Sink) -> None:
        self._enter_container(value)
        try:
            sink.write("[")
            for index, item in enumerate(value):
                if index:
                    sink.write(self.item_separator)
                self._render(item, sink)
            sink.write("]")
        finally:
            self._leave_container(value)

    def _render_mapping(self, value: dict[Any, Any], sink: _Sink) -> None:
        self._enter_container(value)
        try:
            sink.write("{")
            items = sorted(value.items()) if self.sort_keys else value.items()
            emitted = 0
            for key, item in items:
                key_text = self._key_text(key)
                if key_text is None:
                    continue
                if emitted:
                    sink.write(self.item_separator)
                self._write_string(key_text, sink)
                sink.write(self.key_separator)
                self._render(item, sink)
                emitted += 1
            sink.write("}")
        finally:
            self._leave_container(value)

    def _render_default(self, value: Any, sink: _Sink) -> None:
        if self.default is None:
            raise TypeError(
                f"Object of type {type(value).__name__} is not JSON serializable"
            )
        object_id = id(value)
        if object_id in self._default_active:
            raise ValueError("Circular reference detected")
        self._default_active.add(object_id)
        try:
            self._render(self.default(value), sink)
        finally:
            self._default_active.remove(object_id)

    def _write_string(self, value: str, sink: _Sink) -> None:
        for fragment in _quoted_fragments(value, ensure_ascii=self.ensure_ascii):
            sink.write(fragment)

    def _write_text(self, value: str, sink: _Sink) -> None:
        for start in range(0, len(value), _STRING_CHUNK_CHARS):
            sink.write(value[start : start + _STRING_CHUNK_CHARS])

    def _number_text(self, value: int | float) -> str:
        if isinstance(value, int) and not isinstance(value, bool):
            # ``json.dumps`` uses the integer's decimal spelling.  Avoiding a
            # scalar JSONEncoder call for every item materially reduces the
            # cost of counting a large list while keeping the same output.
            return int.__repr__(value)
        # Calling json.dumps for one scalar preserves Python's exact float
        # spelling and allow_nan semantics without materialising a container.
        return json.dumps(
            value,
            ensure_ascii=self.ensure_ascii,
            allow_nan=self.allow_nan,
            separators=(",", ":"),
        )

    def _key_text(self, value: Any) -> Optional[str]:
        if isinstance(value, str):
            return value
        if value is None:
            return "null"
        if value is True:
            return "true"
        if value is False:
            return "false"
        if isinstance(value, (int, float)):
            return self._number_text(value)
        if self.skipkeys:
            return None
        raise TypeError(f"keys must be str, int, float, bool or None, not {type(value).__name__}")

    def _enter_container(self, value: Any) -> None:
        if not self.check_circular:
            return
        object_id = id(value)
        if object_id in self._active:
            raise ValueError("Circular reference detected")
        self._active.add(object_id)

    def _leave_container(self, value: Any) -> None:
        if self.check_circular:
            self._active.remove(id(value))


def _measure_and_check(
    value: Any,
    *,
    max_bytes: Optional[int],
    newline: bool,
    default: Optional[Callable[[Any], Any]],
    ensure_ascii: bool,
    allow_nan: bool,
    separators: Optional[tuple[str, str]],
    sort_keys: bool,
    skipkeys: bool,
    check_circular: bool,
) -> tuple[_JsonWalker, int, tuple[str, str]]:
    _validate_max_bytes(max_bytes)
    normalized = _normalise_separators(separators)
    walker = _JsonWalker(
        default=default,
        ensure_ascii=ensure_ascii,
        allow_nan=allow_nan,
        separators=normalized,
        sort_keys=sort_keys,
        skipkeys=skipkeys,
        check_circular=check_circular,
    )
    size = walker.measure(value)
    if newline:
        size += 1
    if max_bytes is not None and size > max_bytes:
        raise BoundedEncodingError(
            f"encoded JSON size {size} exceeds max_bytes {max_bytes}",
            known_size=size,
            max_bytes=max_bytes,
        )
    return walker, size, normalized


def bounded_json_size(
    value: Any,
    *,
    max_bytes: Optional[int] = None,
    default: Optional[Callable[[Any], Any]] = None,
    ensure_ascii: bool = True,
    allow_nan: bool = True,
    separators: Optional[tuple[str, str]] = None,
    sort_keys: bool = False,
    skipkeys: bool = False,
    check_circular: bool = True,
    newline: bool = False,
) -> int:
    """Return the exact UTF-8 JSON size, optionally including a newline."""

    _, size, _ = _measure_and_check(
        value,
        max_bytes=max_bytes,
        newline=newline,
        default=default,
        ensure_ascii=ensure_ascii,
        allow_nan=allow_nan,
        separators=separators,
        sort_keys=sort_keys,
        skipkeys=skipkeys,
        check_circular=check_circular,
    )
    return size


def bounded_json_dumps(
    value: Any,
    max_bytes: Optional[int] = None,
    *,
    default: Optional[Callable[[Any], Any]] = None,
    ensure_ascii: bool = True,
    allow_nan: bool = True,
    separators: Optional[tuple[str, str]] = None,
    sort_keys: bool = False,
    skipkeys: bool = False,
    check_circular: bool = True,
) -> str:
    """Return JSON text only when its complete UTF-8 size fits ``max_bytes``."""

    walker, _, _ = _measure_and_check(
        value,
        max_bytes=max_bytes,
        newline=False,
        default=default,
        ensure_ascii=ensure_ascii,
        allow_nan=allow_nan,
        separators=separators,
        sort_keys=sort_keys,
        skipkeys=skipkeys,
        check_circular=check_circular,
    )
    sink = _TextSink(max_bytes)
    walker.render(value, sink)
    return sink.finish()


def bounded_json_encode(
    value: Any,
    max_bytes: Optional[int] = None,
    *,
    default: Optional[Callable[[Any], Any]] = None,
    ensure_ascii: bool = True,
    allow_nan: bool = True,
    separators: Optional[tuple[str, str]] = None,
    sort_keys: bool = False,
    skipkeys: bool = False,
    check_circular: bool = True,
    newline: bool = False,
    include_newline: Optional[bool] = None,
) -> bytes:
    """Return UTF-8 JSON bytes, optionally with one JSON Lines newline.

    The newline participates in the limit.  ``include_newline`` is accepted as
    a descriptive alias for callers that use JSON Lines terminology.
    """

    if include_newline is not None:
        newline = include_newline
    walker, _, _ = _measure_and_check(
        value,
        max_bytes=max_bytes,
        newline=newline,
        default=default,
        ensure_ascii=ensure_ascii,
        allow_nan=allow_nan,
        separators=separators,
        sort_keys=sort_keys,
        skipkeys=skipkeys,
        check_circular=check_circular,
    )
    sink = _BytesSink(max_bytes)
    walker.render(value, sink)
    if newline:
        sink.write("\n")
    return sink.finish()


__all__ = [
    "BoundedEncodingError",
    "BoundedJsonEncodingError",
    "bounded_json_dumps",
    "bounded_json_encode",
    "bounded_json_size",
]
