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


@dataclass(frozen=True, slots=True)
class ResolvedDestination:
    """Effective table/path handles and their deterministic identity."""

    table_name: str | None
    path: str | None
    format: str
    addressing: AddressingMode
    identity: str


def resolve_destination_target(
    destination: _DestinationContract,
    *,
    require_target: bool = True,
) -> ResolvedDestination:
    """Resolve the primary handle used by destination engine operations.

    File connections are path-addressed.  Delta uses a path when it has no
    catalog/database scope or when Athena explicitly requires a path.  Iceberg
    is catalog-addressed when a catalog/database is configured and otherwise
    falls back to a path.  Physical paths are identity-preserving; named
    targets include a connection scope because two backends can expose the
    same schema/table spelling independently.
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

    if path_only:
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
        addressing: AddressingMode = "path"
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
        addressing = "table"

    if addressing == "path":
        identity = f"{connection_type}:{fmt}:path:{target}"
    else:
        # Named targets are not globally unique without their backend scope.
        # connection_id is stable for a configured connection; name is the
        # deterministic fallback for lightweight test/plugin contracts.
        if connection_type == ConnectionType.DATABASE.value:
            scope = connection.connection_id or connection.name
            identity = f"{connection_type}:{fmt}:table:{scope}:{target.casefold()}"
        else:
            identity = f"{connection_type}:{fmt}:table:{target.casefold()}"

    return ResolvedDestination(
        table_name=table_name,
        path=path,
        format=fmt,
        addressing=addressing,
        identity=identity,
    )


__all__ = ["AddressingMode", "ResolvedDestination", "resolve_destination_target"]
