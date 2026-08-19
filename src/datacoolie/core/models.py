"""Domain models for the DataCoolie framework.

Metadata models (:class:`Connection`, :class:`Source`, :class:`Destination`,
:class:`Transform`, :class:`DataFlow`, :class:`DataCoolieRunConfig`) use
stdlib ``@dataclass`` plus a small compatibility layer that preserves the
constructor-time coercion, validation, dump, and deep-copy behavior the
framework expects when loading metadata from loose dictionaries and JSON
strings.

Runtime containers (:class:`RuntimeInfo` hierarchy) use plain stdlib
``@dataclass`` — they are constructed internally by the framework and
never deserialised from external input.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from datetime import datetime
from types import UnionType
from typing import Any, ClassVar, Dict, List, Optional, TypeVar, Union, get_args, get_origin, get_type_hints

from datacoolie.core.constants import (
    CONNECTION_TYPE_FORMATS,
    DEFAULT_MAX_WORKERS,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETENTION_HOURS,
    DataFlowStatus,
    DatabaseAuthType,
    Format,
    ConnectionType,
    LoadType,
    ProcessingMode,
)
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.converters import convert_to_bool, json_default, parse_json
from datacoolie.utils.helpers import (
    ensure_list,
    generate_unique_id,
    name_to_uuid,
    utc_now,
)
from datacoolie.utils.path_utils import build_path, normalize_path


# ============================================================================
# Shared helpers
# ============================================================================


@dataclass(frozen=True)
class _CompatFieldInfo:
    """Small subset of field metadata used by tests and compatibility helpers."""

    default: Any = MISSING
    default_factory: Callable[[], Any] | None = None


class _ClassProperty:
    """Descriptor implementing a minimal read-only class property."""

    def __init__(self, func: Callable[[type], Any]) -> None:
        self._func = func

    def __get__(self, instance: object, owner: type | None = None) -> Any:
        if owner is None:
            owner = type(instance)
        return self._func(owner)


def _build_default(dc_field: Any) -> Any:
    """Return the declared default value for a dataclass field."""

    if dc_field.default_factory is not MISSING:
        return dc_field.default_factory()
    if dc_field.default is not MISSING:
        return copy.deepcopy(dc_field.default)
    raise ConfigurationError(f"Missing required field: {dc_field.name}")


def _to_field_info(dc_field: Any) -> _CompatFieldInfo:
    """Convert a dataclass field into the lightweight compatibility shape."""

    default_factory = None
    if dc_field.default_factory is not MISSING:
        default_factory = dc_field.default_factory
    return _CompatFieldInfo(
        default=None if dc_field.default is MISSING else dc_field.default,
        default_factory=default_factory,
    )


def _parse_json_object(value: Any) -> Dict[str, Any]:
    """Parse a dict-like JSON field and wrap parsing failures consistently."""

    try:
        return parse_json(value, raise_on_error=True)
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc


def _model_dump_value(value: Any) -> Any:
    """Recursively serialise model values to plain Python containers."""

    if isinstance(value, CompatModel):
        return value.model_dump()
    if is_dataclass(value) and not isinstance(value, type):
        return {dc_field.name: _model_dump_value(getattr(value, dc_field.name)) for dc_field in fields(value)}
    if isinstance(value, list):
        return [_model_dump_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_model_dump_value(item) for item in value)
    if isinstance(value, dict):
        return {key: _model_dump_value(item) for key, item in value.items()}
    return value


def _coerce_annotation_value(
    annotation: Any,
    value: Any,
    field_path: str | None = None,
) -> Any:
    """Coerce nested model annotations from mappings into model instances."""

    if value is None:
        return None

    origin = get_origin(annotation)
    if origin in (list, List):
        args = get_args(annotation)
        if args and isinstance(value, list):
            inner = args[0]
            result = []
            for index, item in enumerate(value):
                item_path = f"{field_path}[{index}]" if field_path else None
                try:
                    result.append(_coerce_annotation_value(inner, item, item_path))
                except ConfigurationError as exc:
                    details = dict(exc.details)
                    if item_path:
                        details["field"] = item_path
                    raise ConfigurationError(exc.message, details=details) from exc
            return result
        return value

    if origin in (dict, Dict):
        return value

    if origin in (Union, UnionType):
        for arg in get_args(annotation):
            if arg is type(None):
                continue
            coerced = _coerce_annotation_value(arg, value, field_path)
            if coerced is not value:
                return coerced
        return value

    if isinstance(annotation, type) and issubclass(annotation, CompatModel) and isinstance(value, Mapping):
        try:
            return annotation(**dict(value))
        except ConfigurationError as exc:
            details = dict(exc.details)
            if field_path:
                details["field"] = field_path
            raise ConfigurationError(exc.message, details=details) from exc

    return value


class CompatModel:
    """Small compatibility layer for the subset of BaseModel behavior we use."""

    forbid_unknown_fields: ClassVar[bool] = False
    field_path_prefix: ClassVar[str | None] = None
    model_fields_set: set[str]

    def __init__(self, **kwargs: Any) -> None:
        cls = type(self)
        dc_fields = fields(cls)
        declared_names = {dc_field.name for dc_field in dc_fields}
        unknown_fields = set(kwargs).difference(declared_names)
        if unknown_fields and cls.forbid_unknown_fields:
            raise ConfigurationError(
                f"Unknown field(s) for {cls.__name__}",
                details={"fields": sorted(unknown_fields)},
            )
        provided_fields = set(kwargs) & declared_names
        type_hints = get_type_hints(cls)

        for dc_field in dc_fields:
            if dc_field.name in kwargs:
                value = kwargs[dc_field.name]
            else:
                value = _build_default(dc_field)
            annotation = type_hints.get(dc_field.name, Any)
            prefix = cls.field_path_prefix
            field_path = f"{prefix}.{dc_field.name}" if prefix else dc_field.name
            setattr(
                self,
                dc_field.name,
                _coerce_annotation_value(annotation, value, field_path),
            )

        self.model_fields_set = provided_fields
        post_init = getattr(self, "__post_init__", None)
        if callable(post_init):
            post_init()

    @_ClassProperty
    def model_fields(cls: type["CompatModel"]) -> Dict[str, _CompatFieldInfo]:
        return {dc_field.name: _to_field_info(dc_field) for dc_field in fields(cls)}

    @classmethod
    def model_construct(cls, **values: Any) -> "CompatModel":
        obj = cls.__new__(cls)
        dc_fields = fields(cls)
        declared_names = {dc_field.name for dc_field in dc_fields}
        for dc_field in dc_fields:
            if dc_field.name in values:
                value = values[dc_field.name]
            else:
                value = _build_default(dc_field)
            setattr(obj, dc_field.name, value)
        obj.model_fields_set = set(values) & declared_names
        return obj

    def model_copy(self, *, deep: bool = False) -> "CompatModel":
        return copy.deepcopy(self) if deep else copy.copy(self)

    def model_dump(self) -> Dict[str, Any]:
        return {dc_field.name: _model_dump_value(getattr(self, dc_field.name)) for dc_field in fields(self)}

    def model_dump_json(self) -> str:
        return json.dumps(self.model_dump(), default=json_default)


def build_qualified_name(
    catalog: str | None,
    database: str | None,
    schema_name: str | None,
    table: str | None,
) -> str | None:
    """Build a backtick-quoted, dot-separated qualified name.

    Returns ``None`` when *table* is ``None``.  Pass ``table=None`` to
    get just the namespace (catalog.database.schema).
    """
    parts: list[str] = []
    if catalog:
        parts.append(f"`{catalog}`")
    if database:
        parts.append(f"`{database}`")
    if schema_name:
        parts.append(f"`{schema_name}`")
    if table is None:
        return ".".join(parts) if parts else None
    parts.append(f"`{table}`")
    return ".".join(parts)


def parse_backward_config(configure: Dict[str, Any]) -> Dict[str, Any] | None:
    """Parse backward look-back offset from a ``configure`` dict.

    Reads ``backward_days``, ``backward_months``, ``backward_hours``,
    ``backward_years``, ``backward_closing_day`` as top-level keys, plus
    a nested ``backward`` dict.  Returns ``None`` when no backward config
    is present.
    """
    backward: Dict[str, Any] = {}
    for unit in ("days", "months", "hours", "years", "closing_day"):
        key = f"backward_{unit}"
        if key in configure:
            backward[unit] = int(configure[key])
    nested = configure.get("backward")
    if isinstance(nested, dict):
        backward.update(nested)
    return backward if backward else None


# ============================================================================
# Supporting models
# ============================================================================


@dataclass(init=False)
class SchemaHint(CompatModel):
    """Column-level type hint for schema conversion."""

    column_name: str
    data_type: str
    format: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default_value: Optional[str] = None
    ordinal_position: Optional[int] = 0
    is_active: bool = True

    @classmethod
    def _must_be_non_empty(cls, v: Any, field_name: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError(f"{field_name} must be a non-empty string")
        return v

    def __post_init__(self) -> None:
        self.column_name = self._must_be_non_empty(self.column_name, "column_name")
        self.data_type = self._must_be_non_empty(self.data_type, "data_type")


@dataclass(init=False)
class PartitionColumn(CompatModel):
    """Partition column definition.

    ``expression`` is an optional SQL expression used to derive the partition
    value (e.g. ``"year(event_date)"``).
    """

    column: str
    expression: Optional[str] = None

    @classmethod
    def _must_be_non_empty(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError("column must be a non-empty string")
        return v

    def __post_init__(self) -> None:
        self.column = self._must_be_non_empty(self.column)


@dataclass(init=False)
class AdditionalColumn(CompatModel):
    """Computed column added during the transform phase."""

    column: str
    expression: str

    @classmethod
    def _must_be_non_empty(cls, v: Any, field_name: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError(f"{field_name} must be a non-empty string")
        return v

    def __post_init__(self) -> None:
        self.column = self._must_be_non_empty(self.column, "column")
        self.expression = self._must_be_non_empty(self.expression, "expression")


_MAX_PORTABLE_REGEX_LENGTH = 4096
_REGEX_ESCAPED_LITERALS = frozenset(r".^$*+?{}[]\|()-")
_REGEX_CONTROL_ESCAPES = frozenset("nrtf")
_REGEX_UNSUPPORTED_ESCAPES = frozenset("dDsSwWbBAZGpPkK")


def _validate_portable_regex(
    pattern: str,
    *,
    field_path: str = "value_rules.pattern",
) -> str:
    """Validate the portable-regex invariant owned by ``ValueRule``."""
    if not isinstance(pattern, str):
        raise ConfigurationError(
            "regex_replace rule requires a string pattern",
            details={"field": field_path},
        )
    if len(pattern) > _MAX_PORTABLE_REGEX_LENGTH:
        raise ConfigurationError(
            f"Portable regex patterns must not exceed {_MAX_PORTABLE_REGEX_LENGTH} characters",
            details={"field": field_path, "length": len(pattern)},
        )

    group_stack: list[dict[str, bool]] = []
    in_class = False
    escaped = False
    index = 0

    while index < len(pattern):
        char = pattern[index]

        if escaped:
            if char.isdigit() or char in _REGEX_UNSUPPORTED_ESCAPES:
                _portable_regex_error(field_path, index - 1, f"unsupported escape \\{char}")
            if char not in _REGEX_ESCAPED_LITERALS and char not in _REGEX_CONTROL_ESCAPES:
                _portable_regex_error(field_path, index - 1, f"unsupported escape \\{char}")
            escaped = False
            index += 1
            continue

        if char == "\\":
            escaped = True
            index += 1
            continue

        if in_class:
            if char == "]":
                in_class = False
            elif pattern[index : index + 2] in {"&&", "--", "~~"}:
                _portable_regex_error(
                    field_path,
                    index,
                    "character-class set operations are unsupported",
                )
            index += 1
            continue

        if char == "[":
            in_class = True
            index += 1
            continue

        if char == "(":
            if group_stack:
                group_stack[-1]["nested"] = True
            if pattern[index : index + 3] == "(?:":
                index += 3
            elif pattern[index : index + 2] == "(?":
                _portable_regex_error(
                    field_path,
                    index,
                    "lookaround, named groups, and inline flags are unsupported",
                )
            else:
                index += 1
            group_stack.append(
                {"nested": False, "quantified": False, "alternation": False}
            )
            continue

        if char == ")":
            if not group_stack:
                _portable_regex_error(field_path, index, "unbalanced closing group")
            group = group_stack.pop()
            next_index = index + 1
            is_quantified = next_index < len(pattern) and pattern[next_index] in "*+?{"
            if is_quantified and any(group.values()):
                _portable_regex_error(
                    field_path,
                    next_index,
                    "quantified groups containing nesting, quantifiers, or alternation are unsupported",
                )
            index += 1
            continue

        if group_stack and char in "*+?{":
            group_stack[-1]["quantified"] = True
        elif group_stack and char == "|":
            group_stack[-1]["alternation"] = True

        if char in "*+?" and index + 1 < len(pattern) and pattern[index + 1] == "+":
            _portable_regex_error(field_path, index, "possessive quantifiers are unsupported")

        index += 1

    if escaped:
        _portable_regex_error(field_path, len(pattern) - 1, "trailing escape")
    if in_class:
        _portable_regex_error(field_path, len(pattern) - 1, "unclosed character class")
    if group_stack:
        _portable_regex_error(field_path, len(pattern) - 1, "unclosed group")

    try:
        re.compile(pattern)
    except re.error as exc:
        raise ConfigurationError(
            "Invalid portable regex pattern",
            details={"field": field_path, "reason": str(exc)},
        ) from exc
    return pattern


def _portable_regex_error(field_path: str, index: int, reason: str) -> None:
    raise ConfigurationError(
        "Unsupported portable regex pattern",
        details={"field": field_path, "index": max(index, 0), "reason": reason},
    )


@dataclass(init=False)
class ValueRule(CompatModel):
    """Typed, engine-portable value normalization rule."""

    forbid_unknown_fields: ClassVar[bool] = True

    operation: str
    columns: List[str] = field(default_factory=list)
    order: int = 100
    mode: Optional[str] = None
    pattern: Optional[str] = None
    replacement: str = ""
    value: Any = None
    mapping: Dict[str, str] = field(default_factory=dict)
    on_unmapped: str = "keep"

    def __post_init__(self) -> None:
        self.operation = str(self.operation).strip().lower()
        self.columns = ensure_list(self.columns)
        self.mode = self.mode.strip().lower() if isinstance(self.mode, str) else self.mode
        self.on_unmapped = str(self.on_unmapped).strip().lower()
        supported = {"trim", "case", "regex_replace", "empty_to_null", "fill_null", "map"}
        if self.operation not in supported:
            raise ConfigurationError(
                f"Unsupported value rule operation: {self.operation!r}",
                details={"supported": sorted(supported)},
            )
        _validate_column_list(self.columns, "value_rules.columns")
        if not isinstance(self.order, int) or isinstance(self.order, bool) or self.order < 0:
            raise ConfigurationError("value_rules.order must be a non-negative integer")
        if self.operation == "case" and self.mode not in {"lower", "upper"}:
            raise ConfigurationError("case rule requires mode 'lower' or 'upper'")
        if self.operation == "regex_replace" and not isinstance(self.pattern, str):
            raise ConfigurationError("regex_replace rule requires a string pattern")
        if self.operation == "regex_replace":
            self.pattern = _validate_portable_regex(self.pattern or "")
        if not isinstance(self.replacement, str):
            raise ConfigurationError("value_rules.replacement must be a string")
        if self.operation == "fill_null" and (
            self.value is None or isinstance(self.value, (dict, list, tuple, set))
        ):
            raise ConfigurationError("fill_null.value must be a non-null JSON scalar")
        if self.operation == "map":
            if not isinstance(self.mapping, dict) or not self.mapping or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in self.mapping.items()
            ):
                raise ConfigurationError("map.mapping must be a non-empty string-to-string object")
            if self.on_unmapped not in {"keep", "null"}:
                raise ConfigurationError("map.on_unmapped supports only 'keep' or 'null'")


@dataclass(init=False)
class MaskingRule(CompatModel):
    """Typed, irreversible column masking rule."""

    forbid_unknown_fields: ClassVar[bool] = True

    method: str
    columns: List[str] = field(default_factory=list)
    value: Any = None
    keep_start: int = 0
    keep_end: int = 0
    mask_char: str = "*"
    bucket_size: Optional[float] = None
    unit: Optional[str] = None

    def __post_init__(self) -> None:
        self.method = str(self.method).strip().lower()
        self.columns = ensure_list(self.columns)
        self.unit = self.unit.strip().lower() if isinstance(self.unit, str) else self.unit
        supported = {"redact", "nullify", "partial", "numeric_bucket", "date_truncate"}
        if self.method not in supported:
            raise ConfigurationError(
                f"Unsupported masking method: {self.method!r}",
                details={"supported": sorted(supported)},
            )
        _validate_column_list(self.columns, "masking_rules.columns")
        if self.method == "redact" and (
            self.value is None or isinstance(self.value, (dict, list, tuple, set))
        ):
            raise ConfigurationError("redact.value must be a non-null JSON scalar")
        if self.method == "partial":
            if (
                not isinstance(self.keep_start, int)
                or isinstance(self.keep_start, bool)
                or not isinstance(self.keep_end, int)
                or isinstance(self.keep_end, bool)
                or self.keep_start < 0
                or self.keep_end < 0
            ):
                raise ConfigurationError("partial keep_start and keep_end must be non-negative")
            if not isinstance(self.mask_char, str) or len(self.mask_char) != 1:
                raise ConfigurationError("partial.mask_char must contain exactly one character")
        if self.method == "numeric_bucket" and (
            not isinstance(self.bucket_size, (int, float))
            or isinstance(self.bucket_size, bool)
            or self.bucket_size <= 0
        ):
            raise ConfigurationError("numeric_bucket.bucket_size must be greater than zero")
        if self.method == "date_truncate" and self.unit not in {"year", "month", "day", "hour"}:
            raise ConfigurationError("date_truncate.unit must be year, month, day, or hour")


@dataclass(init=False)
class HashColumn(CompatModel):
    """Stable hash column generated from an ordered list of scalar columns."""

    forbid_unknown_fields: ClassVar[bool] = True

    target_column: str
    columns: List[str] = field(default_factory=list)
    algorithm: str = "sha256"
    serialization: str = "dc_hash_v1"

    def __post_init__(self) -> None:
        if not isinstance(self.target_column, str) or not self.target_column.strip():
            raise ConfigurationError("hash_columns.target_column must be a non-empty string")
        self.columns = ensure_list(self.columns)
        _validate_column_list(self.columns, "hash_columns.columns")
        self.algorithm = str(self.algorithm).strip().lower()
        if self.algorithm != "sha256":
            raise ConfigurationError("hash_columns.algorithm currently supports only 'sha256'")
        self.serialization = str(self.serialization).strip().lower()
        if self.serialization != "dc_hash_v1":
            raise ConfigurationError(
                "hash_columns.serialization currently supports only 'dc_hash_v1'"
            )


def _validate_column_list(columns: List[str], field_name: str) -> None:
    if not columns or not all(isinstance(column, str) and column.strip() for column in columns):
        raise ConfigurationError(f"{field_name} must contain non-empty strings")
    lowered = [column.lower() for column in columns]
    if len(lowered) != len(set(lowered)):
        raise ConfigurationError(f"{field_name} must not contain duplicate columns")


_ModelT = TypeVar("_ModelT", bound=CompatModel)


def _coerce_model_list(
    value: Any,
    model_type: type[_ModelT],
    field_path: str,
) -> List[_ModelT]:
    """Coerce a typed metadata collection with indexed error context."""
    if value is None or (isinstance(value, (list, tuple)) and not value):
        return []
    if isinstance(value, (Mapping, model_type)):
        items = [value]
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        raise ConfigurationError(
            f"{field_path} must be a list of objects",
            details={"field": field_path, "value_type": type(value).__name__},
        )

    result: List[_ModelT] = []
    for index, item in enumerate(items):
        item_path = f"{field_path}[{index}]"
        if isinstance(item, model_type):
            result.append(item)
            continue
        if not isinstance(item, Mapping):
            raise ConfigurationError(
                f"{item_path} must be an object",
                details={"field": item_path, "value_type": type(item).__name__},
            )
        try:
            result.append(model_type(**dict(item)))
        except ConfigurationError as exc:
            raise ConfigurationError(
                exc.message,
                details={**exc.details, "field": item_path},
            ) from exc
    return result


# ============================================================================
# Connection
# ============================================================================


@dataclass(init=False)
class Connection(CompatModel):
    """Endpoint configuration for a data source or destination.

    The ``configure`` JSON field stores type-specific settings (host, port,
    read_options, write_options, etc.).  Frequently-used values are
    surfaced as computed properties.
    """

    name: str
    connection_id: Optional[str] = None
    workspace_id: Optional[str] = None
    connection_type: str = ConnectionType.FILE.value
    format: str = Format.PARQUET.value
    catalog: Optional[str] = None
    database: Optional[str] = None
    configure: Dict[str, Any] = field(default_factory=dict)
    secrets_ref: Optional[Dict[str, List[str]]] = None
    is_active: bool = True

    @classmethod
    def _derive_connection_id_from_name(cls, values: Any) -> Any:
        if isinstance(values, dict) and not values.get("connection_id"):
            name = values.get("name")
            if name:
                values["connection_id"] = name_to_uuid(str(name))
        return values

    @classmethod
    def _name_non_empty(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError("Connection.name must be a non-empty string")
        return v

    @classmethod
    def _normalise_format(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    def _validate_connection_type_format(self) -> "Connection":
        """Validate and auto-derive the connection_type/format relationship.

        * If ``connection_type`` was explicitly provided, validate ``format``
          is in its allowed set.
        * If ``connection_type`` was NOT explicitly provided, auto-derive it
          from ``CONNECTION_TYPE_FORMATS``.
        """
        explicit_ct = "connection_type" in self.model_fields_set

        if explicit_ct:
            allowed = CONNECTION_TYPE_FORMATS.get(self.connection_type)
            if allowed is None:
                valid = ", ".join(sorted(CONNECTION_TYPE_FORMATS))
                raise ConfigurationError(
                    f"Unknown connection_type '{self.connection_type}'. "
                    f"Valid types: {valid}"
                )
            if self.format not in allowed:
                allowed_str = (
                    ", ".join(sorted(allowed)) if allowed
                    else "none (streaming is not yet supported)"
                )
                raise ConfigurationError(
                    f"Format '{self.format}' is not valid for "
                    f"connection_type '{self.connection_type}'. "
                    f"Allowed: {allowed_str}"
                )
        else:
            for ct, fmts in CONNECTION_TYPE_FORMATS.items():
                if self.format in fmts:
                    self.connection_type = ct
                    break

        return self

    def _validate_database_auth(self) -> "Connection":
        """Validate database auth_type requirements.

        * ``service_principal`` requires ``username``, ``password``, ``tenant_id``.
        * ``access_token`` requires ``token``.
        * Fabric SQL endpoint host rejects ``password`` auth_type.
        """
        if self.connection_type != ConnectionType.DATABASE.value:
            return self
        auth = self.configure.get("auth_type")
        if not auth:
            return self  # no auth_type = implicit password (backward compat)

        if auth == DatabaseAuthType.SERVICE_PRINCIPAL:
            missing = [
                f for f in ("username", "password", "tenant_id")
                if not self.configure.get(f)
            ]
            if missing:
                raise ConfigurationError(
                    f"auth_type 'service_principal' requires configure fields: "
                    f"{', '.join(missing)} on connection '{self.name}'"
                )

        elif auth == DatabaseAuthType.ACCESS_TOKEN:
            if not self.configure.get("token"):
                raise ConfigurationError(
                    f"auth_type 'access_token' requires 'token' in configure "
                    f"on connection '{self.name}'"
                )

        # Fabric SQL endpoint only supports Entra ID auth
        host = self.configure.get("host", "")
        if (
            host
            and ".fabric.microsoft.com" in host
            and auth == DatabaseAuthType.PASSWORD
        ):
            raise ConfigurationError(
                f"Fabric SQL endpoint ({host}) does not support password auth. "
                f"Use auth_type 'service_principal', 'managed_identity', or "
                f"'access_token' on connection '{self.name}'"
            )

        return self

    @classmethod
    def _parse_secrets_ref(cls, v: Any) -> Optional[Dict[str, List[str]]]:
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        if isinstance(v, (str, dict)):
            result = _parse_json_object(v)
            if not result:
                return None
            # Guard: a configure field must appear under exactly one source.
            # Listing the same field under two sources is ambiguous — after the
            # first source resolves it the vault key is gone and the second
            # source would look up the real value as a key.
            seen: dict[str, str] = {}  # field → first source that claimed it
            for source, fields_for_source in result.items():
                if not isinstance(fields_for_source, list):
                    continue
                for field_name in fields_for_source:
                    if field_name in seen:
                        raise ConfigurationError(
                            f"Field '{field_name}' appears in both secrets_ref sources "
                            f"'{seen[field_name]}' and '{source}'. "
                            f"Each configure field must be listed under exactly one source."
                        )
                    seen[field_name] = source
            return result
        raise ConfigurationError(f"secrets_ref must be a str or dict, got {type(v).__name__}")

    @classmethod
    def _parse_json_field(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    def _populate_database_from_configure(self) -> "Connection":
        """Back-compat: lift ``database`` and ``catalog`` from ``configure`` when not set."""
        if not self.catalog and "catalog" in self.configure:
            self.catalog = self.configure["catalog"]
        if not self.database and "database" in self.configure:
            self.database = self.configure["database"]
        return self

    def __post_init__(self) -> None:
        values = self._derive_connection_id_from_name(
            {"connection_id": self.connection_id, "name": self.name}
        )
        self.connection_id = values.get("connection_id")
        self.name = self._name_non_empty(self.name)
        self.format = self._normalise_format(self.format)
        self.secrets_ref = self._parse_secrets_ref(self.secrets_ref)
        self.configure = self._parse_json_field(self.configure)
        self._validate_connection_type_format()
        self._validate_database_auth()
        self._populate_database_from_configure()

    def refresh_from_configure(self) -> None:
        """Unconditionally sync ``database`` and ``catalog`` from ``configure``.

        Unlike the model validator (which only sets empty fields at
        construction time), this always overwrites — call after secret
        resolution when ``configure`` values have been resolved from vault
        keys to real values.
        """
        if "database" in self.configure:
            v = self.configure["database"]
            self.database = object.__getattribute__(v, "_value") if type(v).__name__ == "SecretStr" else v
        if "catalog" in self.configure:
            v = self.configure["catalog"]
            self.catalog = object.__getattribute__(v, "_value") if type(v).__name__ == "SecretStr" else v

    # -- computed properties ------------------------------------------------

    @property
    def base_path(self) -> Optional[str]:
        """Base storage path (e.g. ``abfss://container@storage/``)."""
        return normalize_path(self.configure.get("base_path")) or None

    @property
    def host(self) -> Optional[str]:
        return self.configure.get("host")

    @property
    def port(self) -> Optional[int]:
        raw = self.configure.get("port")
        if raw is None:
            return None
        return int(raw)

    @property
    def username(self) -> Optional[str]:
        return self.configure.get("username")

    @property
    def password(self) -> Optional[str]:
        return self.configure.get("password")

    @property
    def database_type(self) -> Optional[str]:
        """Database type (mysql, mssql, postgresql, oracle, sqlite)."""
        return self.configure.get("database_type")

    @property
    def auth_type(self) -> Optional[str]:
        """Database authentication type (password, service_principal, managed_identity, access_token)."""
        return self.configure.get("auth_type")

    @property
    def tenant_id(self) -> Optional[str]:
        """Azure AD tenant ID for service_principal auth."""
        return self.configure.get("tenant_id")

    @property
    def token(self) -> Optional[str]:
        """Pre-fetched access token for access_token auth."""
        return self.configure.get("token")

    @property
    def url(self) -> Optional[str]:
        """Explicit URL / connection string from configure."""
        return self.configure.get("url")

    @property
    def driver(self) -> Optional[str]:
        """JDBC driver class name."""
        return self.configure.get("driver")

    @property
    def read_options(self) -> Dict[str, Any]:
        return dict(self.configure.get("read_options", {}))

    @property
    def write_options(self) -> Dict[str, Any]:
        return dict(self.configure.get("write_options", {}))

    @property
    def use_schema_hint(self) -> bool:
        return convert_to_bool(self.configure.get("use_schema_hint", True))

    @property
    def use_hive_partitioning(self) -> bool:
        return convert_to_bool(self.configure.get("use_hive_partitioning", False))

    @property
    def athena_output_location(self) -> Optional[str]:
        """S3 path for Athena DDL query results.

        When set, the writer always registers a native Delta table via
        Athena DDL (``DROP + CREATE EXTERNAL TABLE ... TBLPROPERTIES
        ('table_type'='DELTA')``) after every write and maintenance.
        """
        return self.configure.get("athena_output_location") or None

    @property
    def generate_manifest(self) -> bool:
        """Generate ``_symlink_format_manifest/`` after writes and maintenance."""
        return convert_to_bool(self.configure.get("generate_manifest", False))

    @property
    def register_symlink_table(self) -> bool:
        """Register a ``SymlinkTextInputFormat`` table in Glue after writes.

        Implies :attr:`generate_manifest`.
        """
        return convert_to_bool(self.configure.get("register_symlink_table", False))

    @property
    def symlink_database_prefix(self) -> str:
        """Prefix for symlink Glue database name.  Default ``"symlink_"``."""
        return self.configure.get("symlink_database_prefix", "symlink_")

    @property
    def date_folder_partitions(self) -> Optional[str]:
        return self.configure.get("date_folder_partitions")

    @property
    def date_backward(self) -> Optional[Dict[str, Any]]:
        """Backward look-back offset for date-folder partition discovery.

        Reads ``backward_days``, ``backward_months``, ``backward_hours`` as
        top-level keys from ``config``, or a nested ``backward`` dict.

        **Strategies:**

        *Fixed offset* — subtract days / months / hours from watermark::

            config:
              backward_days: 7
              # or
              backward: {days: 7, months: 1}

        *Closing-day* — monthly period boundary based on current date::

            config:
              backward: {closing_day: 10}
        """
        return parse_backward_config(self.configure)


# ============================================================================
# Source / Destination / Transform
# ============================================================================


@dataclass(init=False)
class Source(CompatModel):
    """Read-side pipeline configuration."""

    connection: Connection
    schema_name: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = None
    python_function: Optional[str] = None
    watermark_columns: List[str] = field(default_factory=list)
    filter_expression: Optional[str] = None
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _coerce_list(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    def __post_init__(self) -> None:
        self.watermark_columns = self._coerce_list(self.watermark_columns)
        self.configure = self._parse_configure(self.configure)

    # -- computed properties ------------------------------------------------

    @property
    def full_table_name(self) -> Optional[str]:
        if not self.table:
            return None
        return build_qualified_name(
            self.connection.catalog,
            self.connection.database,
            self.schema_name,
            self.table,
        )

    @property
    def namespace(self) -> Optional[str]:
        """Namespace without the table: ``catalog.database.schema``."""
        return build_qualified_name(
            self.connection.catalog,
            self.connection.database,
            self.schema_name,
            None,
        )

    @property
    def path(self) -> Optional[str]:
        bp = self.connection.base_path
        if not bp or not self.table:
            return None
        return build_path(bp, self.schema_name, self.table)

    @property
    def read_options(self) -> Dict[str, Any]:
        """Merged read options: connection defaults + source overrides."""
        opts = dict(self.connection.read_options)
        opts.update(self.configure.get("read_options", {}))
        return opts

    @property
    def date_backward(self) -> Optional[Dict[str, Any]]:
        """Backward look-back offset, source-level overrides connection-level.

        Reads from ``configure`` (same keys as
        :attr:`Connection.date_backward`).  If no source-level config
        is present, falls back to the connection's value.

        Example (YAML / source configure)::

            configure:
              backward_days: 7         # overrides connection setting
              # or
              backward: {months: 1}
              # or closing-day strategy
              backward: {closing_day: 10}
        """
        return parse_backward_config(self.configure) or self.connection.date_backward


@dataclass(init=False)
class Destination(CompatModel):
    """Write-side pipeline configuration."""

    connection: Connection
    table: str
    schema_name: Optional[str] = None
    load_type: str = LoadType.APPEND.value
    merge_keys: List[str] = field(default_factory=list)
    partition_columns: List[PartitionColumn] = field(default_factory=list)
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _normalise_schema_name(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, str):
            stripped = v.strip()
            # Temporarily disabled: return stripped.lower() if stripped else None
            return stripped if stripped else None
        return v

    @classmethod
    def _normalise_table(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError("Destination.table must be a non-empty string")
        # Temporarily disabled: return v.strip().lower()
        return v.strip()

    @classmethod
    def _normalise_load_type(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @classmethod
    def _coerce_merge_keys(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_partition_columns(cls, v: Any) -> List[PartitionColumn]:
        if not v:
            return []
        result: list[PartitionColumn] = []
        items = v if isinstance(v, list) else [v]
        for item in items:
            if isinstance(item, dict):
                result.append(PartitionColumn(**item))
            elif isinstance(item, PartitionColumn):
                result.append(item)
            elif isinstance(item, str):
                result.append(PartitionColumn(column=item))
            else:
                result.append(item)
        return result

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    @classmethod
    def _lift_partition_columns_from_configure(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        cfg = values.get("configure")
        if isinstance(cfg, dict) and not values.get("partition_columns"):
            pc = cfg.pop("partition_columns", None)
            if pc:
                values["partition_columns"] = pc
        return values

    def __post_init__(self) -> None:
        self.configure = self._parse_configure(self.configure)
        if "partition_columns" not in self.model_fields_set and not self.partition_columns:
            lifted = self._lift_partition_columns_from_configure(
                {
                    "configure": self.configure,
                    "partition_columns": self.partition_columns,
                }
            )
            if isinstance(lifted, dict):
                self.configure = lifted.get("configure", self.configure)
                self.partition_columns = lifted.get("partition_columns", self.partition_columns)
        self.schema_name = self._normalise_schema_name(self.schema_name)
        self.table = self._normalise_table(self.table)
        self.load_type = self._normalise_load_type(self.load_type)
        self.merge_keys = self._coerce_merge_keys(self.merge_keys)
        self.partition_columns = self._coerce_partition_columns(self.partition_columns)

    # -- computed properties ------------------------------------------------

    @property
    def full_table_name(self) -> str:
        return build_qualified_name(
            self.connection.catalog,
            self.connection.database,
            self.schema_name,
            self.table,
        )

    @property
    def namespace(self) -> Optional[str]:
        """Namespace without the table: ``catalog.database.schema``."""
        return build_qualified_name(
            self.connection.catalog,
            self.connection.database,
            self.schema_name,
            None,
        )

    @property
    def path(self) -> Optional[str]:
        bp = self.connection.base_path
        if not bp or not self.table:
            return None
        return build_path(bp, self.schema_name, self.table)

    @property
    def destination_key(self) -> str:
        """Stable identity for this destination as a physical object.

        Two destinations that resolve to the same physical object share
        the same key.  Useful for orchestration concerns like
        deduplicating fan-in writes or scheduling maintenance at most
        once per object.

        Identity priority:

        1. Fully-qualified table name when ``catalog`` or ``database`` is
           set on the connection — this matches how Databricks Unity
           Catalog, Fabric Lakehouse, and AWS Glue address tables.
        2. Storage path otherwise — covers unregistered Delta tables
           (local dev / tests).

        Results are prefixed (``"table:"`` / ``"path:"``) to prevent a
        path string from colliding with a qualified name, and lowercased
        for case-insensitive equivalence.

        Raises:
            ConfigurationError: When the destination has neither a
                catalog/database registration nor a storage path.
        """
        conn = self.connection
        if conn.catalog or conn.database:
            return f"table:{self.full_table_name.lower()}"
        if self.path:
            return f"path:{self.path.rstrip('/').lower()}"
        raise ConfigurationError(
            f"Destination '{self.table}' has no catalog/database registration "
            "and no storage path — cannot compute a destination identity"
        )

    @property
    def write_options(self) -> Dict[str, Any]:
        """Merged write options: connection defaults + destination overrides."""
        opts = dict(self.connection.write_options)
        opts.update(self.configure.get("write_options", {}))
        return opts

    @property
    def partition_column_names(self) -> List[str]:
        return [pc.column for pc in self.partition_columns if pc.column]

    @property
    def merge_keys_extended(self) -> List[str]:
        """Return merge keys extended with partition columns."""
        keys = list(self.merge_keys)
        for col in self.partition_column_names:
            if col not in keys:
                keys.append(col)
        return keys

    @property
    def scd2_effective_column(self) -> Optional[str]:
        """SQL expression used as ``__valid_from`` for SCD2 loads.

        Read from ``destination.configure["scd2_effective_column"]``.
        Returns ``None`` when not set (non-SCD2 destinations).
        """
        return self.configure.get("scd2_effective_column") or None

    @property
    def replace_by_watermark(self) -> bool:
        """Whether merge_overwrite should use range-based window replace.

        When ``True``, the strategy deletes all target rows within the
        watermark window (watermark_effective → new_watermark) instead of
        doing key-based delete.  This handles source-side deletions.

        Requires ``date_backward`` on the source to ensure the read window
        covers the delete scope.
        """
        return bool(self.configure.get("replace_by_watermark", False))


@dataclass(init=False)
class Transform(CompatModel):
    """Transformation rules applied between source read and destination write."""

    field_path_prefix: ClassVar[str] = "transform"

    deduplicate_columns: List[str] = field(default_factory=list)
    latest_data_columns: List[str] = field(default_factory=list)
    filter_expression: Optional[str] = None
    additional_columns: List[AdditionalColumn] = field(default_factory=list)
    schema_hints: List[SchemaHint] = field(default_factory=list)
    select_columns: List[str] = field(default_factory=list)
    drop_columns: List[str] = field(default_factory=list)
    rename_columns: Dict[str, str] = field(default_factory=dict)
    value_rules: List[ValueRule] = field(default_factory=list)
    hash_columns: List[HashColumn] = field(default_factory=list)
    masking_rules: List[MaskingRule] = field(default_factory=list)
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _coerce_list(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_dedup(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_additional(cls, v: Any) -> List[AdditionalColumn]:
        return _coerce_model_list(v, AdditionalColumn, "transform.additional_columns")

    @classmethod
    def _coerce_hints(cls, v: Any) -> List[SchemaHint]:
        return _coerce_model_list(v, SchemaHint, "transform.schema_hints")

    @classmethod
    def _coerce_value_rules(cls, v: Any) -> List[ValueRule]:
        return _coerce_model_list(v, ValueRule, "transform.value_rules")

    @classmethod
    def _coerce_masking_rules(cls, v: Any) -> List[MaskingRule]:
        return _coerce_model_list(v, MaskingRule, "transform.masking_rules")

    @classmethod
    def _coerce_hash_columns(cls, v: Any) -> List[HashColumn]:
        return _coerce_model_list(v, HashColumn, "transform.hash_columns")

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    def __post_init__(self) -> None:
        self.latest_data_columns = self._coerce_list(self.latest_data_columns)
        self.deduplicate_columns = self._coerce_dedup(self.deduplicate_columns)
        self.additional_columns = self._coerce_additional(self.additional_columns)
        self.schema_hints = self._coerce_hints(self.schema_hints)
        self.select_columns = self._coerce_list(self.select_columns)
        self.drop_columns = self._coerce_list(self.drop_columns)
        self.value_rules = self._coerce_value_rules(self.value_rules)
        self.hash_columns = self._coerce_hash_columns(self.hash_columns)
        self.masking_rules = self._coerce_masking_rules(self.masking_rules)
        self.configure = self._parse_configure(self.configure)
        _ = self.missing_column_policy
        self._validate_projection()
        self._validate_hash_targets()
        self._validate_masking_targets()

    def _validate_projection(self) -> None:
        if self.select_columns and self.drop_columns:
            raise ConfigurationError("select_columns and drop_columns are mutually exclusive")
        if self.select_columns:
            _validate_column_list(self.select_columns, "select_columns")
        if self.drop_columns:
            _validate_column_list(self.drop_columns, "drop_columns")
        if not isinstance(self.rename_columns, dict) or not all(
            isinstance(source, str) and source.strip()
            and isinstance(target, str) and target.strip()
            for source, target in self.rename_columns.items()
        ):
            raise ConfigurationError("rename_columns must be a string-to-string object")
        sources = {source.lower() for source in self.rename_columns}
        targets = [target.lower() for target in self.rename_columns.values()]
        if len(targets) != len(set(targets)):
            raise ConfigurationError("rename_columns must not contain duplicate targets")
        if sources.intersection(targets):
            raise ConfigurationError("rename_columns chains, cycles, and no-op renames are not supported")

    def _validate_masking_targets(self) -> None:
        seen: set[str] = set()
        for rule in self.masking_rules:
            overlap = seen.intersection(column.lower() for column in rule.columns)
            if overlap:
                raise ConfigurationError(
                    "A column may appear in only one masking rule",
                    details={"columns": sorted(overlap)},
                )
            seen.update(column.lower() for column in rule.columns)

    def _validate_hash_targets(self) -> None:
        targets = [definition.target_column.lower() for definition in self.hash_columns]
        if len(targets) != len(set(targets)):
            raise ConfigurationError("hash_columns must not contain duplicate target columns")

    @property
    def missing_column_policy(self) -> str:
        policy = str(self.configure.get("missing_column_policy", "error")).strip().lower()
        if policy not in {"error", "ignore"}:
            raise ConfigurationError("missing_column_policy must be 'error' or 'ignore'")
        return policy

    def deduplicate_column_names(self, merge_keys: List[str] | None = None) -> List[str]:
        """Return dedup columns, falling back to *merge_keys*."""
        if self.deduplicate_columns:
            return self.deduplicate_columns
        return merge_keys or []

    @property
    def convert_timestamp_ntz(self) -> bool:
        """Whether to convert ``timestamp_ntz`` columns to ``timestamp``.

        Reads ``convert_timestamp_ntz`` from :attr:`configure`.
        Defaults to ``True``.

        Example (YAML / metadata)::

            transform:
              configure:
                convert_timestamp_ntz: false
        """
        return convert_to_bool(self.configure.get("convert_timestamp_ntz", True))

    @property
    def deduplicate_by_rank(self) -> bool:
        """Whether to use RANK-based deduplication instead of ROW_NUMBER.

        Reads ``deduplicate_by_rank`` from :attr:`configure`.
        Defaults to ``False``.

        Example (YAML / metadata)::

            transform:
              configure:
                deduplicate_by_rank: true
        """
        return convert_to_bool(self.configure.get("deduplicate_by_rank", False))

    @property
    def schema_hints_dict(self) -> Dict[str, SchemaHint]:
        return {h.column_name: h for h in self.schema_hints}


# ============================================================================
# DataFlow — complete pipeline definition
# ============================================================================


@dataclass(init=False)
class DataFlow(CompatModel):
    """Complete ETL pipeline configuration.

    Composes :class:`Source`, :class:`Destination`, and :class:`Transform`.
    """

    source: Source
    destination: Destination
    dataflow_id: Optional[str] = None
    workspace_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    stage: Optional[str] = None
    group_number: Optional[int] = None
    execution_order: Optional[int] = None
    processing_mode: str = ProcessingMode.BATCH.value
    is_active: bool = True
    transform: Transform = field(default_factory=Transform)
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _derive_dataflow_id_from_name(cls, values: Any) -> Any:
        if isinstance(values, dict) and not values.get("dataflow_id"):
            name = values.get("name")
            if name:
                values["dataflow_id"] = name_to_uuid(str(name))
        return values

    @classmethod
    def _normalise_mode(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    def __post_init__(self) -> None:
        values = self._derive_dataflow_id_from_name(
            {"dataflow_id": self.dataflow_id, "name": self.name}
        )
        self.dataflow_id = values.get("dataflow_id")
        self.processing_mode = self._normalise_mode(self.processing_mode)
        self.configure = self._parse_configure(self.configure)
        # Mutable runtime state — set by driver after source read.
        self._watermark_window: Optional[Dict[str, tuple]] = None
        self.validate()

    @property
    def watermark_window(self) -> Optional[Dict[str, tuple]]:
        """Watermark window for range-based replace.

        Set by the driver after source read.  Maps column names to
        ``(lower_bound, upper_bound)`` tuples.
        """
        return self._watermark_window

    # -- validation ---------------------------------------------------------

    def validate(self) -> None:
        """Validate metadata configuration; raise on invalid combinations.

        Called automatically in ``__post_init__`` to fail fast at
        metadata load time.
        """
        if self.destination.replace_by_watermark:
            if not self.source.date_backward:
                raise ConfigurationError(
                    "replace_by_watermark requires date_backward on the source "
                    "to ensure the read window covers the delete scope",
                    details={"dataflow": self.name or self.dataflow_id},
                )
            if self.load_type != LoadType.MERGE_OVERWRITE.value:
                raise ConfigurationError(
                    f"replace_by_watermark is only supported with merge_overwrite load_type, "
                    f"got {self.load_type!r}",
                    details={"dataflow": self.name or self.dataflow_id},
                )

    def apply_watermark_window(self, source_runtime: "SourceRuntimeInfo") -> None:
        """Set :attr:`watermark_window` from source runtime info.

        Computes the (watermark_effective, watermark_after) window per
        column when ``replace_by_watermark`` is active and both bounds
        are available.  Otherwise leaves the window as ``None``.
        """
        if (
            self.destination.replace_by_watermark
            and source_runtime.watermark_effective
            and source_runtime.watermark_after
        ):
            self._watermark_window = {
                col: (source_runtime.watermark_effective[col], source_runtime.watermark_after[col])
                for col in source_runtime.watermark_effective
                if col in source_runtime.watermark_after
            }

    # -- convenience proxies ------------------------------------------------

    @property
    def load_type(self) -> str:
        return self.destination.load_type

    @property
    def merge_keys(self) -> List[str]:
        return self.destination.merge_keys

    @property
    def partition_columns(self) -> List[PartitionColumn]:
        return self.destination.partition_columns

    @property
    def partition_column_names(self) -> List[str]:
        return self.destination.partition_column_names

    @property
    def deduplicate_columns(self) -> List[str]:
        return self.transform.deduplicate_column_names(self.merge_keys)

    @property
    def order_columns(self) -> List[str]:
        """Columns used to order rows during deduplication.

        Returns ``transform.latest_data_columns`` when set, otherwise
        falls back to ``source.watermark_columns``.
        """
        return self.transform.latest_data_columns or self.source.watermark_columns


# ============================================================================
# ReplayConfig — bounded replay / init with chunking
# ============================================================================


@dataclass
class ReplayConfig:
    """Configuration for replaying a bounded time range in chunks.

    Used by :meth:`DataCoolieDriver.run_replay` to reprocess historical
    data without corrupting the production watermark.

    The range uses the left-closed, right-open ``[start, end)`` convention:
    *start* is **inclusive**, *end* is **exclusive**.  This aligns chunks
    to whole calendar units (days, weeks, months, etc.) and is the
    industry-standard interval convention used by Python’s ``range()``,
    Spark partition pruning, and PostgreSQL range types.

    Example::

        # Replay all of Q1 2025 in monthly chunks:
        ReplayConfig(
            start="2025-01-01",  # inclusive
            end="2025-04-01",    # exclusive (first day NOT included)
            chunk_interval={"months": 1},
        )
        # Produces chunks: [Jan 1, Feb 1), [Feb 1, Mar 1), [Mar 1, Apr 1)

    The chunk column is auto-resolved from
    ``dataflow.source.watermark_columns[0]`` at runtime.  Override with
    ``chunk_column`` for multi-column watermarks where the first column
    is not the one to chunk on.

    Type detection is automatic:

    * ``str`` parseable to date/datetime → time-based chunking
    * ``datetime`` / ``date`` objects → time-based chunking
    * ``int`` → integer-based chunking

    Args:
        start: Inclusive lower bound of the replay range.
        end: Exclusive upper bound of the replay range.
        chunk_interval: Chunking interval.  Time-based keys (``months``,
            ``days``, ``hours``, ``minutes``, ``weeks``, ``years``) use
            ``relativedelta``; ``step`` key is for integer watermarks.
            ``None`` disables chunking (single-shot replay).
        save_watermark: When ``True`` (init mode), save the watermark
            after each successful chunk — enables crash-resume.
            When ``False`` (backfill mode), the stored watermark is
            never touched.
        chunk_column: Override auto-resolved chunk column.  Only needed
            for multi-column watermarks where the first column is not
            the desired chunking dimension.
    """

    start: Any
    end: Any
    chunk_interval: Optional[Dict[str, int]] = None
    save_watermark: bool = False
    chunk_column: Optional[str] = None

    def __post_init__(self) -> None:
        if self.start is None:
            raise ConfigurationError("ReplayConfig.start must not be None")
        if self.end is None:
            raise ConfigurationError("ReplayConfig.end must not be None")


# ============================================================================
# DataCoolieRunConfig — execution parameters
# ============================================================================


@dataclass(init=False)
class DataCoolieRunConfig(CompatModel):
    """Validated execution parameters for a DataCoolie run."""

    job_id: str = field(default_factory=generate_unique_id)
    job_num: int = 1
    job_index: int = 0
    max_workers: int = DEFAULT_MAX_WORKERS
    stop_on_error: bool = False
    retry_count: int = DEFAULT_RETRY_COUNT
    retry_delay: float = DEFAULT_RETRY_DELAY
    dry_run: bool = False
    retention_hours: int = DEFAULT_RETENTION_HOURS
    allowed_function_prefixes: List[str] = field(default_factory=list)

    def _validate_constraints(self) -> "DataCoolieRunConfig":
        if not self.job_id:
            raise ConfigurationError("DataCoolieRunConfig.job_id must be a non-empty string")
        if self.job_num < 1:
            raise ConfigurationError("DataCoolieRunConfig.job_num must be at least 1")
        if self.job_index < 0:
            raise ConfigurationError("DataCoolieRunConfig.job_index must be non-negative")
        if self.job_index >= self.job_num:
            raise ConfigurationError(
                f"DataCoolieRunConfig.job_index ({self.job_index}) must be less than job_num ({self.job_num})"
            )
        if self.max_workers < 1:
            raise ConfigurationError("DataCoolieRunConfig.max_workers must be at least 1")
        if self.retry_count < 0:
            raise ConfigurationError("DataCoolieRunConfig.retry_count must be non-negative")
        if self.retry_delay < 0:
            raise ConfigurationError("DataCoolieRunConfig.retry_delay must be non-negative")
        if self.retention_hours < 0:
            raise ConfigurationError("DataCoolieRunConfig.retention_hours must be non-negative")
        return self

    def __post_init__(self) -> None:
        self._validate_constraints()


# ============================================================================
# Runtime information models
# ============================================================================


@dataclass
class RuntimeInfo:
    """Base timing / status model for execution tracking."""

    start_time: datetime = field(default_factory=utc_now)
    end_time: Optional[datetime] = None
    status: str = DataFlowStatus.PENDING.value
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class SourceRuntimeInfo(RuntimeInfo):
    """Runtime metrics for source reading."""

    rows_read: int = 0
    source_action: Dict[str, Any] = field(default_factory=dict)
    watermark_before: Optional[Dict[str, Any]] = None
    watermark_after: Optional[Dict[str, Any]] = None
    watermark_effective: Optional[Dict[str, Any]] = None


@dataclass
class TransformRuntimeInfo(RuntimeInfo):
    """Runtime metrics for transformation."""

    transformers_applied: List[str] = field(default_factory=list)


@dataclass
class DestinationRuntimeInfo(RuntimeInfo):
    """Runtime metrics for destination writing or maintenance."""

    operation_type: Optional[str] = None  # e.g. "merge", "overwrite", "append", "maintenance", etc.
    rows_written: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    files_added: int = 0
    files_removed: int = 0
    bytes_added: int = 0
    bytes_removed: int = 0
    operation_details: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def bytes_saved(self) -> int:
        return max(0, self.bytes_removed - self.bytes_added)


@dataclass
class PipelineAttemptResult:
    """Terminal phase results from one retryable pipeline attempt."""

    status: str
    source: Optional[SourceRuntimeInfo] = None
    transform: Optional[TransformRuntimeInfo] = None
    destination: Optional[DestinationRuntimeInfo] = None


@dataclass
class DataFlowRuntimeInfo(RuntimeInfo):
    """Mutable orchestration record for one dataflow execution."""

    dataflow_run_id: str = field(default_factory=generate_unique_id)
    dataflow_id: Optional[str] = None
    operation_type: Optional[str] = None  # e.g. "etl", "maintenance"
    source: SourceRuntimeInfo = field(default_factory=SourceRuntimeInfo)
    transform: TransformRuntimeInfo = field(default_factory=TransformRuntimeInfo)
    destination: DestinationRuntimeInfo = field(default_factory=DestinationRuntimeInfo)
    retry_attempts: int = 0

    @property
    def rows_read(self) -> int:
        return self.source.rows_read

    @property
    def rows_written(self) -> int:
        return self.destination.rows_written

    @property
    def rows_inserted(self) -> int:
        return self.destination.rows_inserted

    @property
    def rows_updated(self) -> int:
        return self.destination.rows_updated

    @property
    def rows_deleted(self) -> int:
        return self.destination.rows_deleted

    @property
    def is_success(self) -> bool:
        return self.status == DataFlowStatus.SUCCEEDED.value

    @property
    def is_failed(self) -> bool:
        return self.status == DataFlowStatus.FAILED.value


# ============================================================================
# Job-level aggregation
# ============================================================================


@dataclass
class JobRuntimeInfo(RuntimeInfo):
    """Aggregated metrics for an entire DataCoolie job run."""

    job_id: str = field(default_factory=generate_unique_id)
    job_num: int = 1
    job_index: int = 0
    workspace_id: Optional[str] = None
    stages: Optional[str | List[str]] = None  # single stage or list of stages

    # Component names (set by driver from type(obj).__name__)
    engine_name: Optional[str] = None
    platform_name: Optional[str] = None
    metadata_provider_name: Optional[str] = None
    watermark_manager_name: Optional[str] = None

    # RunConfig attributes
    max_workers: int = DEFAULT_MAX_WORKERS
    stop_on_error: bool = False
    retry_count: int = DEFAULT_RETRY_COUNT
    retry_delay: float = DEFAULT_RETRY_DELAY
    dry_run: bool = False
    retention_hours: int = DEFAULT_RETENTION_HOURS

    total_dataflows: int = 0
    total_succeeded: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    total_running: int = 0
    total_pending: int = 0

    total_rows_read: int = 0
    total_rows_written: int = 0
    total_rows_inserted: int = 0
    total_rows_updated: int = 0
    total_rows_deleted: int = 0

    total_files_added: int = 0
    total_files_removed: int = 0

    total_bytes_added: int = 0
    total_bytes_removed: int = 0

    operation_types: Optional[str | List[str]] = None
