"""Pure destination target resolution.

Destination routing is owned by the destination layer.  Engines receive the
resolved handle, while maintenance and orchestration use the same projection
to calculate a physical identity.  This module performs no I/O or catalog
lookup.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

from datacoolie.core.constants import CONNECTION_TYPE_FORMATS, ConnectionType, Format
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.path_utils import normalize_path


AddressingMode = Literal["table", "path"]


class _ConnectionContract(Protocol):
    connection_type: str
    format: str
    catalog: str | None
    database: str | None
    connection_id: str | None
    name: str

    @property
    def athena_output_location(self) -> str | None: ...


class _DestinationContract(Protocol):
    connection: _ConnectionContract
    table: str
    full_table_name: str
    path: str | None


class _EngineTargetContract(Protocol):
    def preferred_target_addressing(
        self, *, fmt: str, table_name: str | None, path: str | None
    ) -> AddressingMode | None: ...

    def target_name_identity(
        self, *, fmt: str, table_name: str
    ) -> tuple[str, str] | None: ...


@dataclass(frozen=True, slots=True)
class ResolvedDestination:
    """Effective table/path handles and their deterministic identity."""

    table_name: str | None
    path: str | None
    format: str
    addressing: AddressingMode
    identity: str | None
    storage_path: str | None = None


def resolve_destination_target(
    destination: _DestinationContract,
    *,
    engine: _EngineTargetContract | None = None,
    require_target: bool = True,
    require_identity: bool = False,
) -> ResolvedDestination:
    """Resolve the primary handle used by destination engine operations.

    Forced-path destination policy precedes the engine's pure preference.
    Engine-aware results expose only the operative handle, retaining the
    auxiliary path in storage_path. Unknown named equality is None, or an
    error when require_identity is requested. Engine-free calls preserve the
    legacy declared projection for inspection, not runtime physical equality.
    """

    try:
        connection = destination.connection
        fmt = str(connection.format or "").strip().lower()
        connection_type = str(connection.connection_type or "").strip().lower()
        table_name = destination.full_table_name or None
        path = normalize_path(destination.path) or None
        catalog_scoped = bool(connection.catalog or connection.database)
        athena_path = bool(connection.athena_output_location)
    except Exception as exc:  # pragma: no cover - malformed foreign contract
        raise ConfigurationError("Cannot resolve destination target") from exc

    file_formats = CONNECTION_TYPE_FORMATS.get(ConnectionType.FILE.value, frozenset())
    path_only = (
        connection_type == ConnectionType.FILE.value
        and fmt in file_formats
    ) or (
        bool(path)
        and fmt == Format.DELTA.value
        and (athena_path or not catalog_scoped)
    ) or (
        bool(path)
        and fmt == Format.ICEBERG.value
        and not catalog_scoped
    )

    addressing: AddressingMode = "path" if path_only else "table"
    if engine is not None and not path_only:
        try:
            preference = engine.preferred_target_addressing(
                fmt=fmt, table_name=table_name, path=path
            )
        except Exception as exc:
            raise ConfigurationError("Cannot resolve engine target preference") from exc
        if preference not in ("table", "path"):
            raise ConfigurationError(f"Invalid or missing engine target preference: {preference!r}")
        addressing = preference

    if addressing == "path":
        if not path:
            if require_target:
                raise ConfigurationError(
                    f"Destination '{destination.table}' cannot compute a destination "
                    "identity: storage path is required"
                )
            target = "<missing-path>"
        else:
            target = path
        table_name = None
    else:
        if not table_name:
            if require_target:
                raise ConfigurationError(
                    f"Destination '{destination.table}' cannot compute a destination "
                    "identity: table name is required"
                )
            target = "<missing-table>"
        else:
            target = table_name

    if addressing == "path":
        identity = f"{connection_type}:{fmt}:path:{target}"
    elif engine is not None:
        try:
            native = engine.target_name_identity(fmt=fmt, table_name=target)
        except Exception as exc:
            raise ConfigurationError("Cannot resolve engine target identity") from exc
        if native is None:
            identity = None
        elif (
            not isinstance(native, tuple) or len(native) != 2
            or any(not isinstance(value, str) or not value.strip() for value in native)
        ):
            raise ConfigurationError(f"Invalid engine target identity: {native!r}")
        else:
            backend_scope, canonical_name = native
            scope = ""
            if connection_type == ConnectionType.DATABASE.value:
                scope = f"{connection.connection_id or connection.name}:"
            identity = f"{connection_type}:{fmt}:table:{scope}{backend_scope}:{canonical_name}"
    else:
        # Named targets are not globally unique without their backend scope.
        # connection_id is stable for a configured connection; name is the
        # deterministic fallback for lightweight test/plugin contracts.
        if connection_type == ConnectionType.DATABASE.value:
            scope = connection.connection_id or connection.name
            identity = f"{connection_type}:{fmt}:table:{scope}:{target.casefold()}"
        else:
            identity = f"{connection_type}:{fmt}:table:{target.casefold()}"

    if require_identity and (identity is None or not (path if addressing == "path" else table_name)):
        raise ConfigurationError(f"Unqualified destination identity for '{destination.table}' ({fmt})")

    return ResolvedDestination(
        table_name=table_name,
        path=path if engine is None or addressing == "path" else None,
        format=fmt,
        addressing=addressing,
        identity=identity,
        storage_path=path,
    )


__all__ = ["AddressingMode", "ResolvedDestination", "resolve_destination_target"]
