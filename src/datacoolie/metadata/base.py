"""Abstract base class for metadata providers and in-memory metadata cache.

``BaseMetadataProvider`` uses a **Template Method** pattern: public ``get_*``
methods call protected ``_fetch_*`` abstract methods, wrapping them with an
optional in-memory cache layer (``MetadataCache``).

Concrete providers — ``FileProvider``, ``DatabaseProvider``, ``APIClient`` —
implement only the ``_fetch_*`` and watermark methods.
"""

from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

from datacoolie.core.constants import CONNECTION_TYPE_FORMATS, ConnectionType
from datacoolie.core.models import Connection, DataFlow, SchemaHint
from datacoolie.utils.helpers import ensure_list

logger = logging.getLogger(__name__)


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
        "_loaded",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._connections: Dict[str, Connection] = {}
        self._connections_by_name: Dict[str, Connection] = {}
        self._dataflows: Dict[str, DataFlow] = {}
        self._schema_hints: Dict[Tuple[str, Optional[str], str], List[SchemaHint]] = {}
        # Set to True once a provider has bulk-loaded the entire workspace
        # metadata into the cache; subsequent reads can be served entirely
        # from memory without further I/O.
        self._loaded: bool = False

    # -- connections -------------------------------------------------------

    def get_connection(self, connection_id: str) -> Optional[Connection]:
        """Return a cached connection by *connection_id*, or ``None``."""
        with self._lock:
            return self._connections.get(connection_id)

    def get_connection_by_name(self, name: str) -> Optional[Connection]:
        """Return a cached connection by *name*, or ``None``."""
        with self._lock:
            return self._connections_by_name.get(name)

    def set_connection(self, connection: Connection) -> None:
        """Store a single connection in the cache."""
        with self._lock:
            self._connections[connection.connection_id] = connection
            self._connections_by_name[connection.name] = connection

    def set_connections(self, connections: List[Connection]) -> None:
        """Bulk-store connections, replacing any previous entries."""
        with self._lock:
            self._connections.clear()
            self._connections_by_name.clear()
            for conn in connections:
                self._connections[conn.connection_id] = conn
                self._connections_by_name[conn.name] = conn

    # -- dataflows ---------------------------------------------------------

    def get_dataflow(self, dataflow_id: str) -> Optional[DataFlow]:
        """Return a cached dataflow by *dataflow_id*, or ``None``."""
        with self._lock:
            return self._dataflows.get(dataflow_id)

    def set_dataflow(self, dataflow: DataFlow) -> None:
        """Store a single dataflow in the cache."""
        with self._lock:
            self._dataflows[dataflow.dataflow_id] = dataflow

    def set_dataflows(self, dataflows: List[DataFlow]) -> None:
        """Bulk-store dataflows, replacing any previous entries."""
        with self._lock:
            self._dataflows.clear()
            for df in dataflows:
                self._dataflows[df.dataflow_id] = df

    # -- schema hints ------------------------------------------------------

    def get_schema_hints(
        self,
        connection_id: str,
        schema_name: Optional[str],
        table_name: str,
    ) -> Optional[List[SchemaHint]]:
        """Return cached schema hints for the composite key, or ``None``."""
        with self._lock:
            return self._schema_hints.get((connection_id, schema_name, table_name))

    def set_schema_hints(
        self,
        connection_id: str,
        schema_name: Optional[str],
        table_name: str,
        hints: List[SchemaHint],
    ) -> None:
        """Store schema hints under the composite key."""
        with self._lock:
            self._schema_hints[(connection_id, schema_name, table_name)] = hints

    def set_all_schema_hints(
        self,
        grouped: Dict[Tuple[str, Optional[str], str], List[SchemaHint]],
    ) -> None:
        """Bulk-store the entire workspace schema-hint map, replacing prior entries."""
        with self._lock:
            self._schema_hints.clear()
            self._schema_hints.update(grouped)

    # -- bulk-load state ---------------------------------------------------

    def is_loaded(self) -> bool:
        """Return ``True`` once a full workspace bulk-load has populated the cache."""
        with self._lock:
            return self._loaded

    def mark_loaded(self) -> None:
        """Mark the cache as fully populated by a bulk-load."""
        with self._lock:
            self._loaded = True

    def get_all_connections(self) -> List[Connection]:
        """Return a snapshot of all cached connections."""
        with self._lock:
            return list(self._connections.values())

    def get_all_dataflows(self) -> List[DataFlow]:
        """Return a snapshot of all cached dataflows."""
        with self._lock:
            return list(self._dataflows.values())
        
    def get_all_schema_hints(
        self,
    ) -> Dict[Tuple[str, Optional[str], str], List[SchemaHint]]:
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
            self._loaded = False


# ============================================================================
# BaseMetadataProvider — abstract provider
# ============================================================================


class BaseMetadataProvider(ABC):
    """Abstract metadata provider with optional caching.

    Subclasses implement the ``_fetch_*`` methods (I/O layer) and the
    watermark methods.  This base class wraps them with cache-aside logic:
    check cache → miss → fetch → store → return.

    **Context manager** — ``with provider: ...`` calls :meth:`close` on exit.
    """

    def __init__(
        self,
        *,
        enable_cache: bool = True,
        auto_prefetch: bool = True,
        eager_prefetch: bool = False,
    ) -> None:
        self._cache: Optional[MetadataCache] = MetadataCache() if enable_cache else None
        # When True (and caching is enabled), the first call to
        # ``get_dataflows`` triggers ``prefetch_all()`` so that all
        # subsequent reads are served from memory.
        self._auto_prefetch: bool = auto_prefetch
        # When True, concrete providers trigger ``prefetch_all()`` at the
        # end of their own ``__init__`` via :meth:`_maybe_eager_prefetch`
        # so the cache is populated before the first ``get_*`` call.
        # Recommended for short-lived batch runners; leave ``False`` for
        # long-lived multi-tenant services where most providers may
        # never be used.
        self._eager_prefetch: bool = eager_prefetch
        # Guards ``prefetch_all`` so concurrent callers cannot trigger
        # duplicate bulk-loads (double-checked locking pattern).
        self._prefetch_lock: threading.Lock = threading.Lock()

    def _maybe_eager_prefetch(self) -> None:
        """Trigger an eager bulk-load when ``eager_prefetch`` is enabled.

        Concrete providers should call this at the **end** of their
        ``__init__`` (after all I/O state — engine / httpx client /
        loaded JSON — is ready), not in :class:`BaseMetadataProvider`
        itself, because ``_bulk_load`` depends on subclass state.
        """
        if self._eager_prefetch and self._cache is not None:
            self.prefetch_all()

    # ------------------------------------------------------------------
    # Bulk pre-load — load entire workspace metadata into the cache
    # ------------------------------------------------------------------

    def prefetch_all(self) -> None:
        """Bulk-load connections, dataflows and schema hints into the cache.

        After this call returns, ``get_connections``, ``get_dataflows``,
        ``get_schema_hints`` and the per-id getters are served entirely
        from the in-memory cache — no further I/O is performed by the
        provider for those reads.

        Workspace scoping (database / API providers) and the single-file
        nature of the file provider make it safe to load the full set of
        metadata up-front.

        The bulk-load always loads the full set (active **and** inactive)
        so that subsequent calls with any ``active_only`` flag can be
        filtered correctly from cache.

        Idempotent and thread-safe: concurrent callers race on a lock
        and only one bulk-load is performed.  No-op when caching is
        disabled or when the cache is already loaded.

        Subclasses implement :meth:`_bulk_load` to provide the actual
        I/O.
        """
        if self._cache is None or self._cache.is_loaded():
            return
        with self._prefetch_lock:
            # Double-check inside the lock: another thread may have
            # completed the bulk-load while we were waiting.
            if self._cache.is_loaded():
                return
            started = time.monotonic()
            # Always load the full set — partial loads would silently
            # corrupt later ``active_only=False`` reads from the cache.
            connections, dataflows, hints = self._bulk_load()
            self._cache.set_connections(connections)
            self._cache.set_dataflows(dataflows)
            self._cache.set_all_schema_hints(hints)
            self._cache.mark_loaded()
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "%s.prefetch_all: connections=%d dataflows=%d schema_hint_keys=%d elapsed_ms=%d",
                type(self).__name__,
                len(connections),
                len(dataflows),
                len(hints),
                elapsed_ms,
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
        downstream.  Subclasses (``APIClient``, ``DatabaseProvider``,
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
            key = (src.connection_id, df.source.schema_name, df.source.table)
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

    def _auto_prefetch_if_needed(self, *, attach_schema_hints: bool) -> None:
        """Trigger a one-time bulk pre-load on first use when appropriate.

        Only fires when the caller wants schema hints attached — that's
        the dominant cost path.  Callers that explicitly opt out of
        hints also opt out of the implicit pre-load (preserves the
        lightweight-read contract used by tests).
        """
        if (
            attach_schema_hints
            and self._cache is not None
            and self._auto_prefetch
            and not self._cache.is_loaded()
        ):
            self.prefetch_all()

    def _load_dataflows_for_read(
        self,
        *,
        active_only: bool,
        stages: Optional[List[str]] = None,
    ) -> List[DataFlow]:
        """Return dataflows from the bulk cache, or fetch + cache them.

        When the cache has been bulk-loaded, filtering is done in memory.
        Otherwise the per-resource fetch is delegated to the subclass
        and the results are seeded into the cache so that subsequent
        :meth:`get_dataflow_by_id` lookups can hit the cache.  Seeding
        with a filtered subset is safe because ``is_loaded()`` gates
        bulk reads and ``set_dataflows`` replaces on every full fetch.
        """
        if self._cache is not None and self._cache.is_loaded():
            dataflows = self._cache.get_all_dataflows()
            if active_only:
                dataflows = [df for df in dataflows if df.is_active]
            if stages is not None:
                stage_set = set(stages)
                dataflows = [df for df in dataflows if df.stage in stage_set]
            return dataflows
        dataflows = self._fetch_dataflows(stages=stages, active_only=active_only)
        if self._cache is not None:
            self._cache.set_dataflows(dataflows)
        return dataflows

    def _attach_hints_if_requested(
        self,
        dataflows: List[DataFlow],
        *,
        attach_schema_hints: bool,
    ) -> None:
        """Attach schema hints to *dataflows* when requested.

        When the cache is fully bulk-loaded, a single snapshot of the
        hint map is taken (one lock acquire) and used for all lookups;
        otherwise the subclass bulk-prefetch hook is called and each
        dataflow falls back to the per-dataflow attach path.
        """
        if not attach_schema_hints:
            return
        if self._cache is not None and self._cache.is_loaded():
            snapshot = self._cache.get_all_schema_hints()
            for df in dataflows:
                if df.transform.schema_hints:
                    continue
                src = df.source.connection
                if not src.use_schema_hint or df.source.table is None:
                    continue
                hints = snapshot.get(
                    (src.connection_id, df.source.schema_name, df.source.table)
                )
                if hints:
                    df.transform.schema_hints = hints
            return
        self._prefetch_schema_hints(dataflows)
        for df in dataflows:
            self._attach_schema_hints(df)

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the in-memory cache (no-op when caching is disabled)."""
        if self._cache is not None:
            self._cache.clear()

    # ------------------------------------------------------------------
    # Connections — public API with caching
    # ------------------------------------------------------------------

    def get_connections(self, *, active_only: bool = True) -> List[Connection]:
        """Return all connections, optionally filtered to active ones.

        When the cache has been bulk-loaded via :meth:`prefetch_all`,
        this is served entirely from memory.  Otherwise the fetched
        list is seeded into the cache so that subsequent
        :meth:`get_connection_by_id` / ``_by_name`` lookups hit the
        cache.  Seeding a filtered subset is safe because
        ``is_loaded()`` gates bulk reads and ``set_connections``
        replaces on every full fetch.
        """
        if self._cache is not None and self._cache.is_loaded():
            cached = self._cache.get_all_connections()
            if active_only:
                return [c for c in cached if c.is_active]
            return cached
        connections = self._fetch_connections(active_only=active_only)
        if self._cache is not None:
            self._cache.set_connections(connections)
        return connections

    def get_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        """Return a single connection by *connection_id*.

        When the cache has been bulk-loaded and the id is not in it,
        returns ``None`` without falling through to a remote fetch
        (negative cache — bulk-load owns the full workspace).
        """
        if self._cache is not None:
            cached = self._cache.get_connection(connection_id)
            if cached is not None:
                return cached
            if self._cache.is_loaded():
                return None
        conn = self._fetch_connection_by_id(connection_id)
        if conn is not None and self._cache is not None:
            self._cache.set_connection(conn)
        return conn

    def get_connection_by_name(self, name: str) -> Optional[Connection]:
        """Return a single connection by *name*.

        Negative-cache behaviour mirrors :meth:`get_connection_by_id`.
        """
        if self._cache is not None:
            cached = self._cache.get_connection_by_name(name)
            if cached is not None:
                return cached
            if self._cache.is_loaded():
                return None
        conn = self._fetch_connection_by_name(name)
        if conn is not None and self._cache is not None:
            self._cache.set_connection(conn)
        return conn

    # ------------------------------------------------------------------
    # Dataflows — public API with caching + schema hint attachment
    # ------------------------------------------------------------------

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

        On the first call (when caching is enabled and ``auto_prefetch``
        is on), the entire workspace metadata is bulk-loaded into the
        cache via :meth:`prefetch_all`; subsequent calls are served
        entirely from memory.
        """
        self._auto_prefetch_if_needed(attach_schema_hints=attach_schema_hints)
        stages = self._normalise_stages(stage)
        dataflows = self._load_dataflows_for_read(active_only=active_only, stages=stages)
        self._attach_hints_if_requested(dataflows, attach_schema_hints=attach_schema_hints)
        return dataflows

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
        self._auto_prefetch_if_needed(attach_schema_hints=attach_schema_hints)
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
        if self._cache is not None:
            cached = self._cache.get_dataflow(dataflow_id)
            if cached is not None:
                if attach_schema_hints:
                    self._attach_schema_hints(cached)
                return cached
            if self._cache.is_loaded():
                return None
        df = self._fetch_dataflow_by_id(dataflow_id)
        if df is not None:
            if self._cache is not None:
                self._cache.set_dataflow(df)
            if attach_schema_hints:
                self._attach_schema_hints(df)
        return df

    # ------------------------------------------------------------------
    # Schema hints
    # ------------------------------------------------------------------

    def get_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        """Return schema hints for a given connection + table."""
        if self._cache is not None:
            cached = self._cache.get_schema_hints(connection_id, schema_name, table_name)
            if cached is not None:
                return cached
            # When the cache has been fully bulk-loaded, a missing key
            # means there are genuinely no hints for this (connection,
            # schema, table) — do NOT fall through to a remote fetch.
            if self._cache.is_loaded():
                return []
        hints = self._fetch_schema_hints(
            connection_id=connection_id,
            table_name=table_name,
            schema_name=schema_name,
        )
        if self._cache is not None:
            self._cache.set_schema_hints(connection_id, schema_name, table_name, hints)
        return hints

    def _prefetch_schema_hints(self, dataflows: List[DataFlow]) -> None:
        """Optional bulk-prefetch hook called before per-dataflow attach.

        Default is a no-op; providers with remote I/O (e.g. ``APIClient``)
        override this to warm the cache in one or few round trips so the
        subsequent per-dataflow ``_attach_schema_hints`` calls become
        cache lookups rather than N+1 network requests.

        When the cache is fully bulk-loaded the per-dataflow attach loop
        is already guaranteed to hit the cache, so this hook is skipped
        in that case.
        """
        return

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
        """Release resources.  Base implementation clears the cache."""
        self.clear_cache()

    def __enter__(self) -> "BaseMetadataProvider":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
