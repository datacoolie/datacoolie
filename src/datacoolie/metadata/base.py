"""Abstract base class for metadata providers and in-memory metadata cache.

``BaseMetadataProvider`` uses a **Template Method** pattern: public ``get_*``
methods call protected ``_fetch_*`` abstract methods, wrapping them with an
optional in-memory cache layer (``MetadataCache``).

Concrete providers — ``FileProvider``, ``DatabaseProvider``, ``APIProvider`` —
implement only the ``_fetch_*`` and watermark methods.
"""

from __future__ import annotations

import copy
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from contextlib import contextmanager
from functools import wraps
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from datacoolie.core.constants import CONNECTION_TYPE_FORMATS, ConnectionType
from datacoolie.core.exceptions import ConfigurationError, MetadataError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.transform import SchemaHint
from datacoolie.metadata.contracts.context import MetadataProviderStartupContext
from datacoolie.utils.collections import ensure_list
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.metadata.resolution.schema_hints import (
    SchemaHintKey,
    normalize_grouped_hints,
    normalized_key,
    select_schema_hints,
)
from datacoolie.metadata.contracts.identity import (
    connection_identity_error,
    dataflow_identity_error,
)
from datacoolie.utils.component_paths import (
    ComponentPath,
    ComponentPathError,
    normalize_component_paths,
)

logger = get_logger(__name__)


# ============================================================================
# MetadataCache — simple in-memory store
# ============================================================================


class MetadataCache:
    """In-memory cache for metadata objects.

    Stores connections (by id and name), dataflows (by id), and schema
    hints (by composite key).  The cache is optional and purely additive —
    it never performs I/O.
    """

    __slots__ = (
        "_connections",
        "_connections_by_name",
        "_dataflows",
        "_schema_hints",
        "_lock",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._connections: Dict[str, Connection] = {}
        self._connections_by_name: Dict[str, List[Connection]] = {}
        self._dataflows: Dict[str, DataFlow] = {}
        self._schema_hints: Dict[SchemaHintKey, List[SchemaHint]] = {}

    # -- connections -------------------------------------------------------

    def get_connection(self, connection_id: str) -> Optional[Connection]:
        """Return a cached connection by *connection_id*, or ``None``."""
        with self._lock:
            return self._connections.get(connection_id)

    def get_connection_by_name(self, name: str) -> Optional[Connection]:
        """Return a cached connection by *name*, or ``None``."""
        with self._lock:
            matches = self._connections_by_name.get(name, [])
            if len(matches) > 1:
                raise MetadataError(
                    f"Connection name is ambiguous: {name}; use connection_id"
                )
            return matches[0] if matches else None

    # -- dataflows ---------------------------------------------------------

    def get_dataflow(self, dataflow_id: str) -> Optional[DataFlow]:
        """Return a cached dataflow by *dataflow_id*, or ``None``."""
        with self._lock:
            return self._dataflows.get(dataflow_id)

    # -- schema hints ------------------------------------------------------

    def get_schema_hints(
        self,
        connection_id: str,
        schema_name: Optional[str],
        table_name: str,
    ) -> Optional[List[SchemaHint]]:
        """Return cached schema hints for the composite key, or ``None``."""
        with self._lock:
            return select_schema_hints(
                self._schema_hints,
                connection_id,
                schema_name,
                table_name,
            )

    def publish_snapshot(
        self,
        connections: List[Connection],
        dataflows: List[DataFlow],
        schema_hints: Dict[SchemaHintKey, List[SchemaHint]],
    ) -> None:
        """Publish one complete metadata snapshot atomically."""
        connections_by_id = {
            connection.connection_id: connection for connection in connections
        }
        connections_by_name: Dict[str, List[Connection]] = {}
        for connection in connections:
            connections_by_name.setdefault(connection.name, []).append(connection)
        dataflows_by_id = {dataflow.dataflow_id: dataflow for dataflow in dataflows}
        grouped_hints = normalize_grouped_hints(schema_hints)
        with self._lock:
            self._connections = connections_by_id
            self._connections_by_name = connections_by_name
            self._dataflows = dataflows_by_id
            self._schema_hints = grouped_hints

    def get_all_connections(self) -> List[Connection]:
        """Return a snapshot of all cached connections."""
        with self._lock:
            return list(self._connections.values())

    def get_all_dataflows(self) -> List[DataFlow]:
        """Return a snapshot of all cached dataflows."""
        with self._lock:
            return list(self._dataflows.values())
        
    def get_all_schema_hints(self) -> Dict[SchemaHintKey, List[SchemaHint]]:
        """Return a shallow copy of the full schema-hint map in one lock acquire.

        Callers that need to perform many hint lookups (e.g. attaching
        hints to every dataflow in a batch) should use this function
        instead of calling :meth:`get_schema_hints` in a loop — one
        lock acquire vs N.
        """
        with self._lock:
            return dict(self._schema_hints)

    # -- housekeeping ------------------------------------------------------

    def clear(self) -> None:
        """Remove all cached entries."""
        with self._lock:
            self._connections.clear()
            self._connections_by_name.clear()
            self._dataflows.clear()
            self._schema_hints.clear()


# ============================================================================
# BaseMetadataProvider — abstract provider
# ============================================================================


def _metadata_read(method):
    """Serialize a public metadata read with provider initialization."""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lifecycle_lock:
            self.initialize()
            return method(self, *args, **kwargs)

    return wrapper


def _normalize_sql_roots(
    value: str | Sequence[str] | None,
    *,
    artifact_base_path: str | None = None,
    allow_deferred_artifact: bool = False,
) -> tuple[ComponentPath, ...] | None:
    """Normalize SQL roots and translate path errors to provider errors."""

    try:
        return normalize_component_paths(
            value,
            name="sql_base_path",
            artifact_base_path=artifact_base_path,
            allow_empty=False,
            allow_deferred_artifact=allow_deferred_artifact,
        )
    except ComponentPathError as exc:
        raise ConfigurationError(str(exc)) from exc


def _serialize_sql_roots(
    roots: tuple[ComponentPath, ...] | None,
) -> str | tuple[str, ...] | None:
    """Return the public scalar-or-tuple representation of SQL roots."""

    if roots is None:
        return None
    values = tuple(root.base_path for root in roots)
    return values[0] if len(values) == 1 else values


def _sql_root_signature(roots: tuple[ComponentPath, ...] | None) -> frozenset[tuple[str, str]] | None:
    """Compare roots by deterministic prefix mapping, independent of order."""

    if roots is None:
        return None
    return frozenset((root.prefix, root.base_path) for root in roots)


class BaseMetadataProvider(ABC):
    """Abstract metadata provider with optional caching.

    Subclasses implement the ``_fetch_*`` methods (I/O layer) and the
    watermark methods.  This base class wraps them with cache-aside logic:
    check cache → miss → fetch → store → return.

    ``sql_base_path`` is provider-owned configuration for SQL references in
    the metadata.  The provider retains the roots; Driver preparation reads
    the selected file through its execution platform.

    **Context manager** — ``with provider: ...`` calls :meth:`close` on exit.
    """

    def __init__(
        self,
        *,
        enable_cache: bool = True,
        sql_base_path: str | Sequence[str] | None = None,
    ) -> None:
        self._cache: Optional[MetadataCache] = MetadataCache() if enable_cache else None
        self._sql_base_paths = _normalize_sql_roots(
            sql_base_path,
            allow_deferred_artifact=True,
        )
        # Construction only records configuration.  Driver or a standalone
        # metadata read owns the explicit startup boundary.
        self._lifecycle_lock: threading.RLock = threading.RLock()
        self._initialized: bool = False
        self._closed: bool = False
        self._in_initialization: bool = False
        # Runtime operations (for example watermark reads/writes) use the
        # same lifecycle lock as startup and close.  The depth marker lets
        # ``close`` reject same-thread re-entry; an RLock alone would allow a
        # callback to close a resource while the operation still owns it.
        self._runtime_operation_depth: int = 0

    @property
    def is_initialized(self) -> bool:
        """Whether this provider completed its startup contract."""
        with self._lifecycle_lock:
            return self._initialized and not self._closed

    @property
    def is_ready(self) -> bool:
        """Alias for :attr:`is_initialized` used by startup diagnostics."""
        return self.is_initialized

    @property
    def is_closed(self) -> bool:
        """Whether :meth:`close` has already been called."""
        with self._lifecycle_lock:
            return self._closed

    @property
    def sql_base_path(self) -> str | tuple[str, ...] | None:
        """Return the provider-declared SQL root or immutable root tuple."""

        with self._lifecycle_lock:
            return _serialize_sql_roots(self._sql_base_paths)

    def resolve_sql_base_path(
        self,
        context: MetadataProviderStartupContext,
    ) -> str | tuple[str, ...] | None:
        """Resolve provider roots against one Driver startup context.

        The method only normalizes configuration.  It never reads SQL or
        stores Driver fallback values on the provider.
        """

        with self._lifecycle_lock:
            self._ensure_open()
            roots = self._effective_sql_roots(context)
            return _serialize_sql_roots(roots)

    def configure_context(self, context: MetadataProviderStartupContext) -> None:
        """Apply optional runtime defaults before provider initialization."""
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError(f"{type(self).__name__} is closed")
            if self._in_initialization:
                raise RuntimeError("Cannot configure metadata provider during initialization")
            self._validate_sql_context(context)
            self._configure_context(context)

    def _validate_sql_context(self, context: MetadataProviderStartupContext) -> None:
        """Validate SQL roots before a concrete provider mutates its state."""

        self._effective_sql_roots(context)

    def _effective_sql_roots(
        self,
        context: MetadataProviderStartupContext,
    ) -> tuple[ComponentPath, ...] | None:
        """Return provider roots or Driver fallback after conflict checking."""

        context_roots = _normalize_sql_roots(
            context.sql_base_path,
            artifact_base_path=context.artifact_base_path,
        )
        if self._sql_base_paths is None:
            return context_roots

        provider_roots = _normalize_sql_roots(
            self.sql_base_path,
            artifact_base_path=context.artifact_base_path,
        )
        if (
            context_roots is not None
            and _sql_root_signature(provider_roots)
            != _sql_root_signature(context_roots)
        ):
            raise ConfigurationError(
                f"{type(self).__name__} sql_base_path conflicts with Driver sql_base_path"
            )
        return provider_roots

    def _configure_context(self, context: MetadataProviderStartupContext) -> None:
        """Hook for providers that consume startup context.

        ``metadata_base_path`` is a file-provider concern.  Providers backed
        by an API or database must not silently accept a path which they
        cannot use, so the shared default rejects it explicitly.  Providers
        with their own path semantics can override this hook.
        """
        if context.metadata_base_path is not None:
            raise ConfigurationError(
                f"{type(self).__name__} does not support metadata_base_path"
            )

    def validate_watermark_storage(self) -> None:
        """Validate provider-owned watermark configuration without I/O."""
        with self._lifecycle_lock:
            self._ensure_open()

    def _initialize_metadata(self) -> None:
        """Prepare provider-local metadata state before the bulk load.

        Concrete providers may override this hook to resolve a deferred
        metadata location (for example a FileProvider artifact directory).
        The hook must not publish a ready snapshot; :meth:`initialize`
        publishes readiness only after the complete scope has loaded.
        """

    def _cleanup_failed_initialization(self) -> None:
        """Best-effort hook used after a failed startup attempt."""

    def _ensure_open(self) -> None:
        """Reject metadata access after :meth:`close` has released resources.

        This guard intentionally does not acquire ``_lifecycle_lock``. Provider
        bulk loaders may fan out transport I/O to worker threads while the
        caller owns that lock; workers must not wait on a lock held by the
        thread waiting for their results.
        """
        if self._closed:
            raise RuntimeError(f"{type(self).__name__} is closed")

    @contextmanager
    def _runtime_operation(self) -> Iterator[None]:
        """Guard one public runtime operation against lifecycle changes.

        The context deliberately does not initialize metadata.  Watermark
        stores and similar runtime services may be used standalone, but their
        resource acquisition and I/O must be serialized with ``close`` and
        provider cleanup.  Private backend workers must not call this helper:
        API bulk workers run while the initialization owner holds the same
        lifecycle lock.
        """
        with self._lifecycle_lock:
            self._ensure_open()
            self._runtime_operation_depth += 1
            try:
                yield
            finally:
                self._runtime_operation_depth -= 1

    def _validate_initialized_scope(
        self,
        connections: List[Connection],
        dataflows: List[DataFlow],
        hints: Dict[Tuple[str, Optional[str], str], List[SchemaHint]],
    ) -> None:
        """Validate identity invariants shared by all provider backends."""
        seen_ids: set[str] = set()
        connections_by_id: Dict[str, Connection] = {}
        connections_by_name: Dict[str, List[Connection]] = {}
        for connection in connections:
            if connection.connection_id in seen_ids:
                raise MetadataError(
                    f"Duplicate connection_id in metadata: {connection.connection_id}"
                )
            seen_ids.add(connection.connection_id)
            connections_by_id[connection.connection_id] = connection
            connections_by_name.setdefault(connection.name, []).append(connection)

        seen_dataflow_ids: set[str] = set()
        for dataflow in dataflows:
            identity_error = dataflow_identity_error(
                dataflow.dataflow_id,
                dataflow.name,
                label=str(dataflow.dataflow_id or dataflow.name or "?"),
            )
            if identity_error:
                raise MetadataError(identity_error)
            if dataflow.dataflow_id in seen_dataflow_ids:
                raise MetadataError(
                    f"Duplicate dataflow_id in metadata: {dataflow.dataflow_id}"
                )
            seen_dataflow_ids.add(dataflow.dataflow_id)

            for role, connection in (
                ("source", dataflow.source.connection),
                ("destination", dataflow.destination.connection),
            ):
                identity_error = connection_identity_error(
                    connection,
                    connections_by_id,
                    connections_by_name,
                    context=f"Dataflow {dataflow.dataflow_id} {role}",
                )
                if identity_error:
                    raise MetadataError(identity_error)
                if connection.connection_id not in connections_by_id:
                    connections_by_id[connection.connection_id] = connection
                    connections_by_name.setdefault(connection.name, []).append(connection)
                    seen_ids.add(connection.connection_id)

        for connection_id, _schema_name, _table_name in hints:
            if connection_id not in seen_ids:
                raise MetadataError(
                    f"Schema hint references unknown connection: {connection_id}"
                )

        # Duplicate-column and key normalization are centralized in
        # schema_hints.py so every backend applies the same rules once per
        # initialized snapshot.
        normalize_grouped_hints(hints)

    def initialize(self) -> None:
        """Start the provider and validate its complete configured scope.

        Construction only configures a provider.  This method is the shared
        startup boundary used by :class:`DataCoolieDriver`: it is idempotent,
        serializes concurrent callers, and leaves the provider retryable when
        a load fails.  Cache-enabled providers retain the loaded snapshot;
        cache-disabled providers still perform the full load for validation
        but discard the result.
        """
        started = time.perf_counter()
        counts: Optional[tuple[int, int, int]] = None
        initialized = False
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError(f"{type(self).__name__} is closed")
            if self._in_initialization:
                raise RuntimeError(
                    "Recursive metadata initialization; provider startup hooks must "
                    "use private fetch methods"
                )
            if self._initialized:
                return
            try:
                self._in_initialization = True
                self._initialize_metadata()
                connections, dataflows, hints = self._bulk_load()
                self._validate_initialized_scope(connections, dataflows, hints)
                if self._cache is not None:
                    self._cache.publish_snapshot(connections, dataflows, hints)
                self._initialized = True
                initialized = True
                counts = (len(connections), len(dataflows), len(hints))
            except Exception:
                self._initialized = False
                if self._cache is not None:
                    self._cache.clear()
                try:
                    self._cleanup_failed_initialization()
                except Exception:
                    logger.debug(
                        "%s failed cleanup after initialization error",
                        type(self).__name__,
                        exc_info=True,
                    )
                raise
            finally:
                self._in_initialization = False

        if initialized:
            connection_count, dataflow_count, hint_count = counts or (0, 0, 0)
            logger.debug(
                "%s metadata initialized in %.3fs (connections=%d, dataflows=%d, schema_hints=%d)",
                type(self).__name__,
                time.perf_counter() - started,
                connection_count,
                dataflow_count,
                hint_count,
                extra={"event_name": LogEvent.METADATA_INITIALIZED.value},
            )

    def _bulk_load(
        self,
    ) -> Tuple[
        List[Connection],
        List[DataFlow],
        Dict[Tuple[str, Optional[str], str], List[SchemaHint]],
    ]:
        """Default bulk-load: serial calls to the per-resource fetchers.

        Always loads the full (active + inactive) set — callers filter
        downstream.  Subclasses (``APIProvider``, ``DatabaseProvider``,
        ``FileProvider``) override this with an optimised implementation
        (e.g. concurrent paginated GETs or a single ``IN (...)`` SELECT).
        """
        connections = self._fetch_connections(active_only=False)
        dataflows = self._fetch_dataflows(active_only=False)
        hints = self._bulk_fetch_schema_hints(connections=connections, dataflows=dataflows)
        return connections, dataflows, hints

    def _bulk_fetch_schema_hints(
        self,
        *,
        connections: List[Connection],
        dataflows: List[DataFlow],
    ) -> Dict[Tuple[str, Optional[str], str], List[SchemaHint]]:
        """Default fallback: walk dataflows and fetch hints per-(conn, table).

        Used by the base implementation of :meth:`_bulk_load` when a
        subclass does not override it.  Most concrete providers should
        override this with a bulk query.
        """
        grouped: Dict[Tuple[str, Optional[str], str], List[SchemaHint]] = {}
        seen: set = set()
        for df in dataflows:
            src = df.source.connection
            if not src.use_schema_hint or df.source.table is None or not src.connection_id:
                continue
            key = normalized_key(
                src.connection_id,
                df.source.schema_name,
                df.source.table,
            )
            if key in seen:
                continue
            seen.add(key)
            hints = self._fetch_schema_hints(
                connection_id=src.connection_id,
                table_name=df.source.table,
                schema_name=df.source.schema_name,
            )
            if hints:
                grouped[key] = hints
        return grouped

    # ------------------------------------------------------------------
    # Internal helpers — shared by get_dataflows / get_maintenance_dataflows
    # ------------------------------------------------------------------

    def _load_dataflows_for_read(
        self,
        *,
        active_only: bool,
        stages: Optional[List[str]] = None,
    ) -> List[DataFlow]:
        """Return dataflows from the initialized snapshot or the live fetcher."""
        if self._cache is None:
            return copy.deepcopy(
                self._fetch_dataflows(stages=stages, active_only=active_only)
            )
        dataflows = self._cache.get_all_dataflows()
        if active_only:
            dataflows = [df for df in dataflows if df.is_active]
        if stages is not None:
            stage_set = set(stages)
            dataflows = [df for df in dataflows if df.stage in stage_set]
        # Cache objects are provider-owned mutable snapshots.  Detach once at
        # the public boundary so schema-hint enrichment and caller edits do
        # not mutate future reads or the cache's identity maps.
        return copy.deepcopy(dataflows)

    def _attach_hints_if_requested(
        self,
        dataflows: List[DataFlow],
        *,
        attach_schema_hints: bool,
    ) -> None:
        """Attach schema hints to *dataflows* when requested.

        When the cache is fully bulk-loaded, a single snapshot of the
        hint map is taken (one lock acquire) and used for all lookups.
        Cache-disabled providers use the same public lookup path one
        dataflow at a time; they do not have a second prefetch lifecycle.
        """
        if not attach_schema_hints:
            return
        if self._cache is not None:
            snapshot = self._cache.get_all_schema_hints()
            for df in dataflows:
                if df.transform.schema_hints:
                    continue
                src = df.source.connection
                if not src.use_schema_hint or df.source.table is None:
                    continue
                hints = select_schema_hints(
                    snapshot,
                    src.connection_id,
                    df.source.schema_name,
                    df.source.table,
                )
                if hints:
                    df.transform.schema_hints = copy.deepcopy(hints)
            return
        for df in dataflows:
            self._attach_schema_hints(df)

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the cache and invalidate readiness when caching is enabled."""
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError(f"{type(self).__name__} is closed")
            if self._cache is None:
                return
            if self._in_initialization:
                raise RuntimeError("Cannot clear metadata cache during initialization")
            self._cache.clear()
            self._initialized = False

    # ------------------------------------------------------------------
    # Connections — public API with caching
    # ------------------------------------------------------------------

    @_metadata_read
    def get_connections(self, *, active_only: bool = True) -> List[Connection]:
        """Return all connections, optionally filtered to active ones.

        Cache-enabled providers serve this from the published snapshot;
        cache-disabled providers delegate to the provider fetcher.
        """
        self._ensure_open()
        if self._cache is not None:
            cached = self._cache.get_all_connections()
            if active_only:
                cached = [c for c in cached if c.is_active]
            return copy.deepcopy(cached)
        return copy.deepcopy(self._fetch_connections(active_only=active_only))

    @_metadata_read
    def get_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        """Return a single connection by *connection_id*.

        When the cache has been bulk-loaded and the id is not in it,
        returns ``None`` without falling through to a remote fetch
        (negative cache — bulk-load owns the full workspace).
        """
        self._ensure_open()
        if self._cache is None:
            return copy.deepcopy(self._fetch_connection_by_id(connection_id))
        return copy.deepcopy(self._cache.get_connection(connection_id))

    @_metadata_read
    def get_connection_by_name(self, name: str) -> Optional[Connection]:
        """Return a single connection by *name*.

        Negative-cache behaviour mirrors :meth:`get_connection_by_id`.
        """
        self._ensure_open()
        if self._cache is None:
            return copy.deepcopy(self._fetch_connection_by_name(name))
        return copy.deepcopy(self._cache.get_connection_by_name(name))

    # ------------------------------------------------------------------
    # Dataflows — public API with caching + schema hint attachment
    # ------------------------------------------------------------------

    @_metadata_read
    def get_dataflows(
        self,
        *,
        stage: Optional[Union[str, List[str]]] = None,
        active_only: bool = True,
        attach_schema_hints: bool = True,
    ) -> List[DataFlow]:
        """Return dataflows, optionally filtered by *stage*.

        *stage* may be a single name, a comma-separated string
        (``"bronze2silver,silver2gold"``), or a list of names.
        When *attach_schema_hints* is ``True`` (default), schema hints
        are fetched and attached to each dataflow's ``transform.schema_hints``.

        The first public read initializes the complete configured scope.
        """
        self._ensure_open()
        stages = self._normalise_stages(stage)
        dataflows = self._load_dataflows_for_read(active_only=active_only, stages=stages)
        self._attach_hints_if_requested(dataflows, attach_schema_hints=attach_schema_hints)
        return dataflows

    @_metadata_read
    def get_maintenance_dataflows(
        self,
        *,
        connection: Optional[Union[str, List[str]]] = None,
        active_only: bool = True,
        attach_schema_hints: bool = False,
    ) -> List[DataFlow]:
        """Return lakehouse-only dataflows for maintenance.

        Only destinations whose format is in
        ``CONNECTION_TYPE_FORMATS[ConnectionType.LAKEHOUSE.value]``
        (Delta Lake and Iceberg) are included.

        Args:
            connection: Filter by destination connection id or name.
                Accepts a single value, a comma-separated string,
                or a list.
            active_only: Skip inactive dataflows.
            attach_schema_hints: Attach schema hints from metadata.
        """
        self._ensure_open()
        dataflows = self._load_dataflows_for_read(active_only=active_only)

        lakehouse_formats = CONNECTION_TYPE_FORMATS[ConnectionType.LAKEHOUSE.value]
        dataflows = [
            df for df in dataflows
            if df.destination.connection.format in lakehouse_formats
        ]
        if connection is not None:
            wanted = {v.strip() for v in ensure_list(connection) if v}
            dataflows = [
                df for df in dataflows
                if df.destination.connection.connection_id in wanted
                or df.destination.connection.name in wanted
            ]

        self._attach_hints_if_requested(dataflows, attach_schema_hints=attach_schema_hints)
        return dataflows

    @_metadata_read
    def get_dataflow_by_id(
        self,
        dataflow_id: str,
        *,
        attach_schema_hints: bool = True,
    ) -> Optional[DataFlow]:
        """Return a single dataflow by *dataflow_id*.

        When the cache has been bulk-loaded and the id is not in it,
        returns ``None`` without falling through to a remote fetch
        (negative cache).
        """
        self._ensure_open()
        if self._cache is not None:
            df = self._cache.get_dataflow(dataflow_id)
        else:
            df = self._fetch_dataflow_by_id(dataflow_id)
        df = copy.deepcopy(df)
        if df is not None:
            if attach_schema_hints:
                self._attach_schema_hints(df)
        return df

    # ------------------------------------------------------------------
    # Schema hints
    # ------------------------------------------------------------------

    @_metadata_read
    def get_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        """Return schema hints for a given connection + table."""
        self._ensure_open()
        if self._cache is not None:
            hints = self._cache.get_schema_hints(
                connection_id,
                schema_name,
                table_name,
            ) or []
        else:
            hints = self._fetch_schema_hints(
                connection_id=connection_id,
                table_name=table_name,
                schema_name=schema_name,
            )
        return copy.deepcopy(hints)

    def _attach_schema_hints(self, dataflow: DataFlow) -> None:
        """Populate ``dataflow.transform.schema_hints`` from the provider.

        Only fetches when the source connection has
        ``use_schema_hint == True`` and no hints are already attached.
        """
        if dataflow.transform.schema_hints:
            return  # already attached
        src_conn = dataflow.source.connection
        if not src_conn.use_schema_hint:
            return
        table = dataflow.source.table
        schema = dataflow.source.schema_name
        if table is None:
            # Query-based sources (SQL query, python function) have no table
            # name, so schema hints cannot be looked up by table.
            return
        hints = self.get_schema_hints(
            connection_id=src_conn.connection_id,
            table_name=table,
            schema_name=schema,
        )
        if hints:
            dataflow.transform.schema_hints = hints

    # ------------------------------------------------------------------
    # Watermark — abstract; each provider stores watermarks differently
    # ------------------------------------------------------------------

    @abstractmethod
    def get_watermark(self, dataflow_id: str) -> Optional[str]:
        """Return the raw serialised watermark string for *dataflow_id*, or ``None``."""

    @abstractmethod
    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        """Persist a serialised watermark for *dataflow_id*."""

    # ------------------------------------------------------------------
    # Stage normalisation helper
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_stages(
        stage: Optional[Union[str, List[str]]],
    ) -> Optional[List[str]]:
        """Normalise *stage* to a list or ``None``.

        Accepts any of:

        * ``None`` – no filter (returns ``None``).
        * ``"bronze2silver"`` – single name string.
        * ``"bronze2silver,silver2gold"`` – comma-separated string.
        * ``["bronze2silver", "silver2gold"]`` – already a list.
        """
        if stage is None:
            return None
        stages = ensure_list(stage)   # handles all three str cases + list
        return stages if stages else None

    # ------------------------------------------------------------------
    # Abstract fetch methods – subclass I/O layer
    # ------------------------------------------------------------------

    @abstractmethod
    def _fetch_connections(self, *, active_only: bool = True) -> List[Connection]:
        """Fetch all connections from the underlying store."""

    @abstractmethod
    def _fetch_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        """Fetch a single connection by ID."""

    @abstractmethod
    def _fetch_connection_by_name(self, name: str) -> Optional[Connection]:
        """Fetch a single connection by name."""

    @abstractmethod
    def _fetch_dataflows(
        self,
        *,
        stages: Optional[List[str]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]:
        """Fetch dataflows from the underlying store.

        *stages* is already normalised to a list (or ``None``) by
        :meth:`get_dataflows` via :meth:`_normalise_stages`.
        """

    @abstractmethod
    def _fetch_dataflow_by_id(self, dataflow_id: str) -> Optional[DataFlow]:
        """Fetch a single dataflow by ID."""

    @abstractmethod
    def _fetch_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        """Fetch schema hints for a connection + table."""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the provider and release owned resources exactly once."""
        with self._lifecycle_lock:
            if self._closed:
                return
            if self._in_initialization:
                raise RuntimeError("Cannot close metadata provider during initialization")
            if self._runtime_operation_depth:
                raise RuntimeError("Cannot close metadata provider during a runtime operation")
            self._closed = True
            self._initialized = False
            if self._cache is not None:
                self._cache.clear()
            self._close_resources()

    def _close_resources(self) -> None:
        """Release provider-owned resources under the lifecycle lock."""

    def __enter__(self) -> "BaseMetadataProvider":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
