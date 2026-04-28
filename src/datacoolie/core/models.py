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
from collections.abc import Callable, Mapping
from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from datetime import datetime
from types import UnionType
from typing import Any, Dict, List, Optional, Union, get_args, get_origin, get_type_hints

from datacoolie.core.constants import (
    CONNECTION_TYPE_FORMATS,
    DEFAULT_MAX_WORKERS,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETENTION_HOURS,
    DataFlowStatus,
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


def _coerce_annotation_value(annotation: Any, value: Any) -> Any:
    """Coerce nested model annotations from mappings into model instances."""

    if value is None:
        return None

    origin = get_origin(annotation)
    if origin in (list, List):
        args = get_args(annotation)
        if args and isinstance(value, list):
            inner = args[0]
            return [_coerce_annotation_value(inner, item) for item in value]
        return value

    if origin in (dict, Dict):
        return value

    if origin in (Union, UnionType):
        for arg in get_args(annotation):
            if arg is type(None):
                continue
            coerced = _coerce_annotation_value(arg, value)
            if coerced is not value:
                return coerced
        return value

    if isinstance(annotation, type) and issubclass(annotation, CompatModel) and isinstance(value, Mapping):
        return annotation(**dict(value))

    return value


class CompatModel:
    """Small compatibility layer for the subset of BaseModel behavior we use."""

    model_fields_set: set[str]

    def __init__(self, **kwargs: Any) -> None:
        cls = type(self)
        dc_fields = fields(cls)
        declared_names = {dc_field.name for dc_field in dc_fields}
        provided_fields = set(kwargs) & declared_names
        type_hints = get_type_hints(cls)

        for dc_field in dc_fields:
            if dc_field.name in kwargs:
                value = kwargs[dc_field.name]
            else:
                value = _build_default(dc_field)
            annotation = type_hints.get(dc_field.name, Any)
            setattr(self, dc_field.name, _coerce_annotation_value(annotation, value))

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
        self._populate_database_from_configure()

    def refresh_from_configure(self) -> None:
        """Unconditionally sync ``database`` and ``catalog`` from ``configure``.

        Unlike the model validator (which only sets empty fields at
        construction time), this always overwrites — call after secret
        resolution when ``configure`` values have been resolved from vault
        keys to real values.
        """
        if "database" in self.configure:
            self.database = self.configure["database"]
        if "catalog" in self.configure:
            self.catalog = self.configure["catalog"]

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
            return stripped.lower() if stripped else None
        return v

    @classmethod
    def _normalise_table(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError("Destination.table must be a non-empty string")
        return v.strip().lower()

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


@dataclass(init=False)
class Transform(CompatModel):
    """Transformation rules applied between source read and destination write."""

    deduplicate_columns: List[str] = field(default_factory=list)
    latest_data_columns: List[str] = field(default_factory=list)
    additional_columns: List[AdditionalColumn] = field(default_factory=list)
    schema_hints: List[SchemaHint] = field(default_factory=list)
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _coerce_list(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_dedup(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_additional(cls, v: Any) -> List[AdditionalColumn]:
        if not v:
            return []
        items = v if isinstance(v, list) else [v]
        return [
            AdditionalColumn(**item) if isinstance(item, dict) else item
            for item in items
        ]

    @classmethod
    def _coerce_hints(cls, v: Any) -> List[SchemaHint]:
        if not v:
            return []
        items = v if isinstance(v, list) else [v]
        return [
            SchemaHint(**item) if isinstance(item, dict) else item
            for item in items
        ]

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    def __post_init__(self) -> None:
        self.latest_data_columns = self._coerce_list(self.latest_data_columns)
        self.deduplicate_columns = self._coerce_dedup(self.deduplicate_columns)
        self.additional_columns = self._coerce_additional(self.additional_columns)
        self.schema_hints = self._coerce_hints(self.schema_hints)
        self.configure = self._parse_configure(self.configure)

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
class DataFlowRuntimeInfo(RuntimeInfo):
    """Composite runtime info for a complete dataflow execution."""

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

    total_rows_read: int = 0
    total_rows_written: int = 0
    total_rows_inserted: int = 0
    total_rows_updated: int = 0
    total_rows_deleted: int = 0

    total_files_added: int = 0
    total_files_removed: int = 0

    total_bytes_added: int = 0
    total_bytes_removed: int = 0
