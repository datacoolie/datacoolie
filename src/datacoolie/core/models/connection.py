"""Connection model and connection-scoped configuration helpers."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import (
    CONNECTION_TYPE_FORMATS,
    DatabaseAuthType,
    Format,
    ConnectionType,
)
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.converters import convert_to_bool
from datacoolie.utils.identity import is_usable_identifier, name_to_uuid
from datacoolie.utils.path_utils import normalize_path

from datacoolie.core.models.base import CompatModel, _parse_json_object


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
        if isinstance(values, dict) and not is_usable_identifier(
            values.get("connection_id")
        ):
            name = values.get("name")
            if is_usable_identifier(name):
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
                    ", ".join(sorted(allowed))
                    if allowed
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
                f
                for f in ("username", "password", "tenant_id")
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
        raise ConfigurationError(
            f"secrets_ref must be a str or dict, got {type(v).__name__}"
        )

    @classmethod
    def _parse_json_field(cls, v: Any) -> Dict[str, Any]:
        return copy.deepcopy(_parse_json_object(v))

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
            self.database = (
                object.__getattribute__(v, "_value")
                if type(v).__name__ == "SecretStr"
                else v
            )
        if "catalog" in self.configure:
            v = self.configure["catalog"]
            self.catalog = (
                object.__getattribute__(v, "_value")
                if type(v).__name__ == "SecretStr"
                else v
            )

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
    def schema_hint_type_system(self) -> Optional[str]:
        """Raw source convention used when interpreting transform hints.

        The model preserves the authored value.  Normalization and validation
        belong to the transform resolver when a schema hint is actually used.
        """
        return self.configure.get("schema_hint_type_system")

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
    def merge_options(self) -> Dict[str, Any]:
        """Return options intended for merge/upsert operations."""
        return dict(self.configure.get("merge_options", {}))

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
