"""Canonical range binding and response filtering helpers for API sources."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from datacoolie.core.exceptions import SourceError
from datacoolie.utils.chunking import validate_iso_fraction_precision

_BOUND_OPERATORS = frozenset({">", ">=", "<", "<="})
_LOCATIONS = frozenset({"params", "body"})
_FORMATS = frozenset(
    {
        "iso",
        "date",
        "timestamp",
        "timestamp_ms",
        "datetime",
        "datetime_ms",
        # ``integer`` is deliberately separate from the temporal formats:
        # the value is kept as an int so httpx encodes it as a native number.
        "integer",
        "int",
        "native_integer",
        "number",
    }
)
_WATERMARK_KINDS = frozenset({"observed_max", "request_end"})
_REMOVED_SOURCE_KEYS = frozenset(
    {
        "watermark_param_mapping",
        "watermark_to_param",
        "watermark_param_location",
        "watermark_param_format",
        "watermark_range_to_exclusive_offset",
    }
)
_BINDING_KEYS = frozenset(
    {"lower", "upper", "format", "response_column", "watermark_value"}
)
_PARAM_BINDING_KEYS = frozenset({"location", "name", "operator"})


@dataclass(frozen=True)
class APIParamBinding:
    """One configured HTTP parameter carrying one side of a range."""

    location: str
    name: str
    operator: str


@dataclass(frozen=True)
class APIRangeBinding:
    """Normalized per-field range configuration."""

    field: str
    lower: Optional[APIParamBinding]
    upper: Optional[APIParamBinding]
    format: str
    response_column: Optional[str]
    watermark_value: str


@dataclass(frozen=True)
class NormalizedAPIRangeMapping:
    """Normalized canonical API bindings."""

    fields: Dict[str, APIRangeBinding]


@dataclass(frozen=True)
class ReadRangeSpec:
    """Duck typed view of the shared ``SourceReadRange`` contract."""

    column: str
    start: Any
    end: Any
    lower_operator: str
    upper_operator: str


@dataclass(frozen=True)
class ResidualFilter:
    """A response-side filter for one active API binding."""

    field: str
    response_column: Optional[str]
    lower: Any
    upper: Any
    lower_operator: str
    upper_operator: str


def normalize_api_range_mapping(src_cfg: Mapping[str, Any]) -> NormalizedAPIRangeMapping:
    """Validate and normalize the canonical per-field range mapping.

    Runtime readers can receive metadata without a schema marker, so removed
    source keys and unsupported binding properties are rejected here before
    credentials or an HTTP client are created.
    """

    removed_keys = sorted(_REMOVED_SOURCE_KEYS.intersection(src_cfg), key=str)
    if removed_keys:
        raise SourceError(
            "API source.configure contains removed API range keys; use only "
            "source.configure.range_param_mapping.",
            details={
                "keys": removed_keys,
                "paths": [f"source.configure.{key}" for key in removed_keys],
            },
        )

    if "range_param_mapping" not in src_cfg:
        return NormalizedAPIRangeMapping(fields={})
    raw_mapping = src_cfg["range_param_mapping"]
    if not isinstance(raw_mapping, Mapping) or not raw_mapping:
        raise SourceError("API range_param_mapping must be a non-empty mapping.")

    fields: Dict[str, APIRangeBinding] = {}
    for field, raw in raw_mapping.items():
        if not isinstance(field, str) or not field:
            raise SourceError("API range_param_mapping field names must be non-empty strings.")
        if not isinstance(raw, Mapping):
            raise SourceError(
                f"API range_param_mapping[{field!r}] must be an object with lower/upper bindings."
            )
        unknown = sorted(set(raw).difference(_BINDING_KEYS), key=str)
        if unknown:
            raise SourceError(
                f"Unknown API range_param_mapping[{field!r}] properties: {unknown!r}.",
                details={"field": field, "properties": unknown},
            )
        fmt = raw.get("format", "iso")
        if not isinstance(fmt, str):
            raise SourceError(
                f"API range format for field {field!r} must be a string."
            )
        fmt = fmt.lower()
        if fmt not in _FORMATS:
            raise SourceError(
                f"Unsupported API range format {fmt!r} for field {field!r}.",
                details={"field": field, "format": fmt},
            )
        response_column = raw.get("response_column", field)
        if response_column is not None and (not isinstance(response_column, str) or not response_column):
            raise SourceError(
                f"API range response_column for {field!r} must be a string or null."
            )
        kind = raw.get("watermark_value", "observed_max")
        if not isinstance(kind, str):
            raise SourceError(
                f"API watermark_value for field {field!r} must be a string."
            )
        kind = kind.lower()
        if kind not in _WATERMARK_KINDS:
            raise SourceError(
                f"Unsupported API watermark_value {kind!r} for field {field!r}; "
                "expected 'observed_max' or 'request_end'."
            )
        lower = _normalize_param_binding(raw.get("lower"), field, "lower", ">=", "params")
        upper = _normalize_param_binding(raw.get("upper"), field, "upper", "<", "params")
        if lower is None and upper is None:
            raise SourceError(
                f"API range_param_mapping[{field!r}] must define a lower or upper binding."
            )
        if kind == "request_end" and upper is None:
            raise SourceError(
                f"API request_end binding for {field!r} requires an upper parameter binding."
            )
        fields[field] = APIRangeBinding(
            field=field,
            lower=lower,
            upper=upper,
            format=fmt,
            response_column=response_column,
            watermark_value=kind,
        )
    _reject_duplicate_parameter_names(fields)
    return NormalizedAPIRangeMapping(fields=fields)


def _normalize_param_binding(
    raw: Any,
    field: str,
    side: str,
    default_operator: str,
    default_location: str,
) -> Optional[APIParamBinding]:
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = {"name": raw}
    if not isinstance(raw, Mapping):
        raise SourceError(
            f"API range {side} binding for {field!r} must be an object or parameter name."
        )
    unknown = sorted(set(raw).difference(_PARAM_BINDING_KEYS), key=str)
    if unknown:
        raise SourceError(
            f"Unknown API range {side} binding properties for field {field!r}: {unknown!r}.",
            details={"field": field, "side": side, "properties": unknown},
        )
    name = raw.get("name")
    location = str(raw.get("location", default_location)).lower()
    operator = str(raw.get("operator", default_operator))
    if not isinstance(name, str) or not name:
        raise SourceError(f"API range {side} binding for {field!r} requires a non-empty name.")
    if location not in _LOCATIONS:
        raise SourceError(
            f"API range {side} binding for {field!r} has unsupported location {location!r}."
        )
    if operator not in _BOUND_OPERATORS:
        raise SourceError(
            f"API range {side} binding for {field!r} has unsupported operator {operator!r}."
        )
    return APIParamBinding(location=location, name=name, operator=operator)


def _reject_duplicate_parameter_names(fields: Mapping[str, APIRangeBinding]) -> None:
    """Reject ambiguous reuse of a canonical binding's parameter name."""
    seen: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for field, binding in fields.items():
        for side, param in (("lower", binding.lower), ("upper", binding.upper)):
            if param is None:
                continue
            key = (param.location, param.name)
            previous = seen.get(key)
            if previous is not None:
                raise SourceError(
                    "API range parameter names must be unique per location; shared bounds are ambiguous.",
                    details={"location": param.location, "name": param.name, "first": previous, "second": (field, side)},
                )
            seen[key] = (field, side)


def coerce_read_range(value: Any) -> Optional[ReadRangeSpec]:
    """Read a ``SourceReadRange`` value without importing shared models.

    A mapping is accepted for plugin callers and tests, while the normal path
    uses the immutable object supplied by the source contract.
    """
    if value is None:
        return None
    if isinstance(value, Mapping):
        get = value.get
        column = get("column", get("selection_column"))
        start = get("start")
        end = get("end")
        lower_operator = get("lower_operator", get("start_operator", ">="))
        upper_operator = get("upper_operator", get("end_operator", "<"))
    else:
        column = getattr(value, "column", getattr(value, "selection_column", None))
        start = getattr(value, "start", None)
        end = getattr(value, "end", None)
        lower_operator = getattr(
            value,
            "lower_operator",
            getattr(value, "start_operator", ">="),
        )
        upper_operator = getattr(
            value,
            "upper_operator",
            getattr(value, "end_operator", "<"),
        )
    if not isinstance(column, str) or not column:
        raise SourceError("read_range requires a non-empty selection column.")
    if start is None or end is None:
        raise SourceError("read_range requires both start and end bounds.")
    lower_operator = str(lower_operator)
    upper_operator = str(upper_operator)
    if lower_operator not in _BOUND_OPERATORS or upper_operator not in _BOUND_OPERATORS:
        raise SourceError(
            "read_range lower_operator and upper_operator must be one of >, >=, <, <=."
        )
    try:
        increasing = compare_values(start, end, "<")
    except SourceError as exc:
        raise SourceError("read_range bounds must have a comparable type.") from exc
    if not increasing:
        raise SourceError("read_range requires start to be less than end.")
    return ReadRangeSpec(column, start, end, lower_operator, upper_operator)


def format_bound(value: Any, fmt: str) -> Any:
    """Serialize a canonical wire bound without losing precision.

    Bounds are compiled from caller-owned values, so silently dropping
    date/time precision would change the selected interval.  Keep the strict
    encoder as the one wire-format boundary for incremental and bounded reads.
    """
    normalized = str(fmt).lower()
    if normalized in {"integer", "int", "native_integer", "number"}:
        if isinstance(value, bool) or type(value) is not int:
            raise SourceError(
                f"API integer range bounds require an int, got {type(value).__name__}."
            )
        return value
    if normalized == "iso":
        if isinstance(value, str):
            try:
                validate_iso_fraction_precision(value)
            except ValueError as exc:
                raise SourceError(str(exc)) from exc
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                # An opaque string is already represented losslessly on the
                # wire.  Preserve the historical pass-through for ISO.
                return value
        if type(value) is date:
            value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        if isinstance(value, datetime):
            return value.isoformat()
        raise SourceError(
            f"API ISO range bounds require a date, datetime, or ISO string, "
            f"got {type(value).__name__}."
        )

    wire_datetime = _coerce_bound_datetime(value)
    if normalized == "date":
        if wire_datetime.time() != datetime.min.time():
            raise SourceError(
                "API date range format cannot represent a non-midnight datetime "
                "without loss of precision."
            )
        return wire_datetime.date().isoformat()
    if normalized == "datetime":
        if wire_datetime.microsecond:
            raise SourceError(
                "API datetime range format cannot represent subsecond precision "
                "without loss."
            )
        return wire_datetime.replace(tzinfo=None).isoformat()
    if normalized == "datetime_ms":
        if wire_datetime.microsecond % 1000:
            raise SourceError(
                "API datetime_ms range format cannot represent submillisecond "
                "precision without loss."
            )
        naive = wire_datetime.replace(tzinfo=None)
        return naive.strftime("%Y-%m-%dT%H:%M:%S.") + f"{naive.microsecond // 1000:03d}"
    if normalized in {"timestamp", "timestamp_ms"}:
        epoch_microseconds = _epoch_microseconds(wire_datetime)
        # Integer arithmetic is deliberate: datetime.timestamp() goes through
        # a binary float and loses exactness for negative or far-date values.
        if normalized == "timestamp":
            return _format_epoch_seconds(epoch_microseconds)
        if epoch_microseconds % 1_000:
            raise SourceError(
                "API timestamp_ms range format cannot represent submillisecond "
                "precision without loss."
            )
        return str(epoch_microseconds // 1_000)
    raise SourceError(f"Unsupported API range wire format {normalized!r}.")


def _coerce_bound_datetime(value: Any) -> datetime:
    """Return a temporal bound as a datetime without changing its wall time."""

    if type(value) is date:
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            validate_iso_fraction_precision(value)
            return datetime.fromisoformat(value)
        except ValueError as exc:
            if "fractional precision beyond six" in str(exc):
                raise SourceError(str(exc)) from exc
            raise SourceError(
                f"API temporal range bound must be a date, datetime, or ISO string; "
                f"got {value!r}."
            ) from exc
    raise SourceError(
        f"API temporal range bound requires a date, datetime, or ISO string, "
        f"got {type(value).__name__}."
    )


def _epoch_microseconds(value: datetime) -> int:
    """Compute UTC epoch microseconds using integer datetime arithmetic."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value.astimezone(timezone.utc) - epoch
    return (
        delta.days * 86_400 * 1_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def _format_epoch_seconds(epoch_microseconds: int) -> str:
    """Format exact Unix seconds, retaining a fractional microsecond part."""

    if epoch_microseconds == 0:
        return "0"
    sign = "-" if epoch_microseconds < 0 else ""
    magnitude = abs(epoch_microseconds)
    whole, fraction = divmod(magnitude, 1_000_000)
    if fraction == 0:
        return f"{sign}{whole}"
    return f"{sign}{whole}.{fraction:06d}".rstrip("0")


def validate_operator_coverage(
    *,
    requested_lower: Optional[str],
    actual_lower: Optional[str],
    requested_upper: Optional[str],
    actual_upper: Optional[str],
    field: str,
) -> None:
    """Reject endpoint operators that cannot return a requested boundary."""
    if requested_lower and actual_lower:
        if requested_lower == ">=" and actual_lower == ">":
            raise SourceError(
                f"API binding for {field!r} cannot provide the inclusive lower boundary requested by the read."
            )
    if requested_upper and actual_upper:
        if requested_upper == "<=" and actual_upper == "<":
            raise SourceError(
                f"API binding for {field!r} cannot provide the inclusive upper boundary requested by the read."
            )


def compare_values(left: Any, right: Any, operator: str) -> bool:
    """Compare common API row values without narrowing integer wire values."""
    left, right = _coerce_pair(left, right)
    try:
        if operator == ">":
            return left > right
        if operator == ">=":
            return left >= right
        if operator == "<":
            return left < right
        if operator == "<=":
            return left <= right
    except TypeError as exc:
        raise SourceError(
            f"API response field values cannot be compared with range bound: {exc}"
        ) from exc
    raise SourceError(f"Unsupported API comparison operator {operator!r}.")


def filter_records_by_ranges(
    records: Iterable[Mapping[str, Any]],
    filters: Iterable[ResidualFilter],
) -> List[Dict[str, Any]]:
    """Apply residual filters to all records returned across all pages.

    Multiple active fields retain the existing source semantics: a row is
    retained when it satisfies any field's predicate.  A missing response
    field is an error when a residual comparison is required because silently
    dropping that row would turn an endpoint contract failure into data loss.
    """
    filters = list(filters)
    if not filters:
        return [dict(record) for record in records]
    output: List[Dict[str, Any]] = []
    for record in records:
        for item in filters:
            if item.response_column is None:
                # The compiler only allows this when the endpoint operator is
                # exact, so no response-side comparison is necessary.
                output.append(dict(record))
                break
            if item.response_column not in record:
                raise SourceError(
                    f"API response is missing residual range field {item.response_column!r} "
                    f"for active binding {item.field!r}."
                )
            value = record[item.response_column]
            if value is None:
                continue
            if item.lower is not None and not compare_values(value, item.lower, item.lower_operator):
                continue
            if item.upper is not None and not compare_values(value, item.upper, item.upper_operator):
                continue
            output.append(dict(record))
            break
    return output


def _coerce_pair(left: Any, right: Any) -> Tuple[Any, Any]:
    if type(left) is type(right):
        return left, right
    if type(left) is date and isinstance(right, datetime):
        return datetime.combine(left, datetime.min.time(), tzinfo=right.tzinfo), right
    if type(right) is date and isinstance(left, datetime):
        return left, datetime.combine(right, datetime.min.time(), tzinfo=left.tzinfo)
    if isinstance(left, datetime) and isinstance(right, str):
        try:
            return left, datetime.fromisoformat(right)
        except ValueError:
            return left, right
    if isinstance(right, datetime) and isinstance(left, str):
        try:
            return datetime.fromisoformat(left), right
        except ValueError:
            return left, right
    if type(left) is date and isinstance(right, str):
        try:
            parsed = datetime.fromisoformat(right)
            if parsed.time() == datetime.min.time():
                return left, parsed.date()
            return datetime.combine(left, datetime.min.time(), tzinfo=parsed.tzinfo), parsed
        except ValueError:
            return left, right
    if type(right) is date and isinstance(left, str):
        try:
            parsed = datetime.fromisoformat(left)
            if parsed.time() == datetime.min.time():
                return parsed.date(), right
            return parsed, datetime.combine(right, datetime.min.time(), tzinfo=parsed.tzinfo)
        except ValueError:
            return left, right
    return left, right


__all__ = [
    "APIParamBinding",
    "APIRangeBinding",
    "NormalizedAPIRangeMapping",
    "ReadRangeSpec",
    "ResidualFilter",
    "coerce_read_range",
    "compare_values",
    "filter_records_by_ranges",
    "format_bound",
    "normalize_api_range_mapping",
    "validate_operator_coverage",
]
