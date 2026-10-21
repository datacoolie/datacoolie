"""Runtime maintenance values; no destination, catalog, or session imports."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any
from uuid import UUID

from datacoolie.core.exceptions import ConfigurationError

COMPACT_ACTIONS = ("rewrite_data_files", "rewrite_position_delete_files", "rewrite_manifests")
CLEANUP_ACTIONS = ("expire_snapshots", "remove_orphan_files")
ICEBERG_ACTIONS = frozenset((*COMPACT_ACTIONS, *CLEANUP_ACTIONS))


def default_options(options):
    """An empty action mapping carries the same intent as omitted options."""
    if options is not None and not isinstance(options, Mapping):
        raise ConfigurationError("Maintenance options must be a boolean mapping")
    return None if isinstance(options, Mapping) and not options else options


def selected_actions(*, do_compact: bool, do_cleanup: bool,
                     compact_options: Mapping[str, bool] | None = None,
                     cleanup_options: Mapping[str, bool] | None = None) -> tuple[str, ...]:
    """Validate the entire request before returning ordered effective actions."""
    selected = []
    for enabled, options, known in ((do_compact, compact_options, COMPACT_ACTIONS),
                                    (do_cleanup, cleanup_options, CLEANUP_ACTIONS)):
        if type(enabled) is not bool:
            raise ConfigurationError("Maintenance group flags must be boolean")
        if options is not None and not isinstance(options, Mapping):
            raise ConfigurationError("Maintenance options must be a boolean mapping")
        options = {} if options is None else options
        if any(key not in known for key in options):
            raise ConfigurationError("Unknown maintenance action option")
        if any(type(value) is not bool for value in options.values()):
            raise ConfigurationError("Maintenance action options must be boolean")
        if not enabled and options:
            raise ConfigurationError("Options require their maintenance group to be enabled")
        if enabled:
            selected.extend(action for action in known if options.get(action, True))
    return tuple(selected)


@dataclass(frozen=True, slots=True)
class RestMaintenanceDeployment:
    """Operator-attested single backend; sentinel checks only routing continuity.

    deployment_identity is the trusted deployment control-plane identity (not
    a connection label). Its backend instance must change when storage/catalog
    ownership changes. Callers own that attestation; no endpoint inference.
    """

    deployment_identity: tuple[str, ...]
    routes: tuple[str, ...]
    warehouse: str
    sentinel_identifier: tuple[str, ...]
    sentinel_uuid: str
    catalog_label: str
    prefix: str = ""

    def __post_init__(self) -> None:
        for values in (self.deployment_identity, self.routes, self.sentinel_identifier):
            if not isinstance(values, tuple) or not values or any(not isinstance(x, str) or not x.strip() for x in values):
                raise ConfigurationError("REST maintenance requires immutable verified deployment evidence")
        if len(self.sentinel_identifier) < 2 or not self.catalog_label or not self.warehouse:
            raise ConfigurationError("REST maintenance requires a catalog label, namespace sentinel and warehouse")
        try:
            UUID(self.sentinel_uuid)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ConfigurationError("REST sentinel requires a native table UUID") from exc

    @property
    def scope(self) -> str:
        # Routes and catalog labels are aliases, not deployment identity.
        evidence = (self.deployment_identity, self.warehouse, self.prefix)
        return "iceberg-rest:" + hashlib.sha256(json.dumps(evidence).encode()).hexdigest()

    def validate_properties(self, properties: Mapping[str, Any]) -> None:
        if properties.get("uri") not in self.routes:
            raise ConfigurationError("REST operative route has no verified deployment binding")
        if properties.get("prefix", "") != self.prefix:
            raise ConfigurationError("REST effective prefix differs from verified deployment")
        warehouse = properties.get("warehouse")
        if warehouse is not None and warehouse != self.warehouse:
            raise ConfigurationError("REST requested warehouse differs from verified deployment")
        if any(value and (str(key).lower() in {"token", "credential", "oauth2-server-uri", "scope", "auth", "auth-manager", "rest.sigv4-enabled"}
                         or str(key).lower().startswith(("header.", "rest.auth", "tenant")))
               for key, value in properties.items()):
            raise ConfigurationError("Authenticated or tenant-routed REST maintenance is unqualified")
        if isinstance(properties.get("ssl"), Mapping) and properties["ssl"].get("client"):
            raise ConfigurationError("REST client certificate maintenance is unqualified")

    def validate_location(self, location: str) -> None:
        if not location.startswith(self.warehouse.rstrip("/") + "/"):
            raise ConfigurationError("Native table location differs from verified deployment warehouse")


@dataclass(frozen=True, slots=True)
class PreparedMaintenanceTarget:
    """Operation-scoped opaque native binding, never a metadata/log value."""

    format: str
    table_name: str | None
    path: str | None
    identity: str | None
    missing: bool = False
    expected_uuid: str | None = None
    capabilities: frozenset[str] = frozenset()
    binding: Any = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class MaintenanceTableAddress:
    """Declared catalog boundary and native namespace, without name guessing."""

    catalog: str | None
    namespace: tuple[str, ...]
    name: str


def native_identifier(table_name: str, *, catalog_label: str,
                      table_address: MaintenanceTableAddress | None = None) -> tuple[str, ...]:
    """Resolve only explicit boundaries or an unambiguous namespace/table pair."""
    if table_address is not None:
        if not isinstance(table_address, MaintenanceTableAddress):
            raise ConfigurationError("REST maintenance requires a structured table address")
        if table_address.catalog is not None and table_address.catalog != catalog_label:
            raise ConfigurationError("REST maintenance does not support the requested catalog")
        if not isinstance(table_address.namespace, tuple) or not table_address.namespace:
            raise ConfigurationError("REST maintenance requires an explicit namespace and table")
        parts = (*table_address.namespace, table_address.name)
        if any(not isinstance(part, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", part) for part in parts):
            raise ConfigurationError("REST maintenance requires simple identifier components")
        return parts
    raw = table_name.split(".")
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*|`[A-Za-z_][A-Za-z_0-9]*`", part) for part in raw):
        raise ConfigurationError("REST maintenance requires simple identifier components")
    parts = tuple(part.strip("`") for part in raw)
    if len(parts) != 2:
        raise ConfigurationError("REST maintenance requires separate catalog and namespace for multipart names")
    return parts
