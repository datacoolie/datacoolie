"""Database-backed metadata provider — SQLAlchemy 2.0.

``DatabaseProvider`` reads connections, dataflows, schema hints and
watermarks from a relational database using SQLAlchemy 2.0 ORM sessions.

The database schema follows the ``dc_framework_*`` table convention
described in the architecture document (03-metadata-schema-design).

All queries are **workspace-scoped** and honour **soft-delete**
(``deleted_at IS NULL``).
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    create_engine,
    func,
    or_,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from datacoolie.core.exceptions import MetadataError, WatermarkError
from datacoolie.core.models import (
    Connection,
    DataFlow,
    Destination,
    SchemaHint,
    Source,
    Transform,
)
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.orchestration.retry_handler import RetryHandler
from datacoolie.utils.converters import parse_json
from datacoolie.utils.helpers import ensure_list, generate_unique_id, utc_now


# ============================================================================
# Table definitions — lightweight SQLAlchemy Core tables (no ORM mapping)
# ============================================================================

_meta = MetaData()

# Retry budget for watermark upserts under concurrent load. MySQL InnoDB can
# raise transient deadlock (1213) / lock-wait-timeout (1205) errors when many
# parallel workers upsert into ``dc_framework_watermarks``. PostgreSQL can
# raise serialization failures (40001). Retries are short and bounded.
_WATERMARK_MAX_RETRIES = 3
_WATERMARK_RETRY_DELAY = 0.1  # seconds; multiplied by (attempt + 1)

_connections_table = Table(
    "dc_framework_connections",
    _meta,
    Column("connection_id", String(36), primary_key=True),
    Column("workspace_id", String(36), nullable=False),
    Column("name", String(100), nullable=False),
    Column("description", Text, nullable=True),
    Column("connection_type", String(50), nullable=False),
    Column("format", String(50), nullable=False),
    Column("catalog", String(200), nullable=True),
    Column("database", String(200), nullable=True),
    Column("configure", Text, nullable=True),
    Column("secrets_ref", Text, nullable=True),
    Column("is_active", Boolean, default=True),
    Column("version", Integer, default=1),
    Column("created_at", DateTime, nullable=True),
    Column("updated_at", DateTime, nullable=True),
    Column("deleted_at", DateTime, nullable=True),
    Column("created_by", String(100), nullable=True),
    Column("updated_by", String(100), nullable=True),
)

_dataflows_table = Table(
    "dc_framework_dataflows",
    _meta,
    Column("dataflow_id", String(36), primary_key=True),
    Column("workspace_id", String(36), nullable=False),
    Column("name", String(200), nullable=False),
    Column("description", Text, nullable=True),
    Column("stage", String(100), nullable=True),
    Column("group_number", Integer, nullable=True),
    Column("execution_order", Integer, nullable=True),
    Column("processing_mode", String(20), nullable=True),
    # source
    Column("source_connection_id", String(36), nullable=False),
    Column("source_schema", String(100), nullable=True),
    Column("source_table", String(200), nullable=True),
    Column("source_query", Text, nullable=True),
    Column("source_python_function", String(500), nullable=True),
    Column("source_watermark_columns", Text, nullable=True),
    Column("source_configure", Text, nullable=True),
    # transform
    Column("transform", Text, nullable=True),
    # destination
    Column("destination_connection_id", String(36), nullable=False),
    Column("destination_schema", String(100), nullable=True),
    Column("destination_table", String(200), nullable=False),
    Column("destination_load_type", String(50), nullable=False),
    Column("destination_merge_keys", Text, nullable=True),
    Column("destination_configure", Text, nullable=True),
    # configuration
    Column("configure", Text, nullable=True),
    # audit
    Column("is_active", Boolean, default=True),
    Column("version", Integer, default=1),
    Column("created_at", DateTime, nullable=True),
    Column("updated_at", DateTime, nullable=True),
    Column("deleted_at", DateTime, nullable=True),
    Column("created_by", String(100), nullable=True),
    Column("updated_by", String(100), nullable=True),
)

_watermarks_table = Table(
    "dc_framework_watermarks",
    _meta,
    Column("watermark_id", String(36), primary_key=True),
    Column("dataflow_id", String(36), nullable=False, unique=True),
    Column("current_value", Text, nullable=True),
    Column("previous_value", Text, nullable=True),
    Column("job_id", String(100), nullable=True),
    Column("dataflow_run_id", String(100), nullable=True),
    Column("updated_at", DateTime, nullable=True),
)

_schema_hints_table = Table(
    "dc_framework_schema_hints",
    _meta,
    Column("schema_hint_id", String(36), primary_key=True),
    Column("connection_id", String(36), nullable=False),
    Column("dataflow_id", String(36), nullable=True),
    Column("schema_name", String(100), nullable=True),
    Column("table_name", String(200), nullable=True),
    Column("column_name", String(200), nullable=False),
    Column("data_type", String(50), nullable=False),
    Column("format", String(200), nullable=True),
    Column("precision", Integer, nullable=True),
    Column("scale", Integer, nullable=True),
    Column("default_value", String(200), nullable=True),
    Column("ordinal_position", Integer, nullable=True),
    Column("is_active", Boolean, default=True),
    Column("created_at", DateTime, nullable=True),
    Column("updated_at", DateTime, nullable=True),
    Column("deleted_at", DateTime, nullable=True),
)


# ============================================================================
# DatabaseProvider
# ============================================================================


class DatabaseProvider(BaseMetadataProvider):
    """Metadata provider backed by a relational database (SQLAlchemy 2.0).

    Supports any SQLAlchemy-compatible backend (PostgreSQL, SQLite,
    SQL Server, MySQL, etc.).

    Args:
        connection_string: SQLAlchemy database URL.
        workspace_id: Workspace scope for all queries.
        engine: Optional pre-built ``Engine`` (overrides *connection_string*
            and all pool parameters).
        enable_cache: Enable the in-memory metadata cache.
        pool_size: Number of persistent connections kept in the pool.
        max_overflow: Extra connections allowed beyond *pool_size* under
            load.  Combined with *pool_size* this caps total concurrent
            connections at ``pool_size + max_overflow``.
        pool_pre_ping: When ``True`` (default), validate each connection
            before checkout with a lightweight ping.  Prevents stale-
            connection errors common with serverless / auto-paused
            databases.
        warm_up_retries: Number of extra connection attempts on the very first
            session open.  Defaults to ``1`` so that serverless / auto-paused
            databases (e.g. Azure SQL Serverless, Aurora Serverless) get one
            automatic retry while they resume from cold start.  Set to ``0``
            to disable.
        warm_up_delay: Seconds to wait between warm-up retry attempts.
    """

    def __init__(
        self,
        *,
        connection_string: Optional[str] = None,
        workspace_id: str,
        engine: Optional[Engine] = None,
        enable_cache: bool = True,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_pre_ping: bool = True,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
        warm_up_retries: int = 1,
        warm_up_delay: float = 15.0,
        eager_prefetch: bool = False,
    ) -> None:
        super().__init__(enable_cache=enable_cache, eager_prefetch=eager_prefetch)
        if engine is not None:
            self._engine = engine
        elif connection_string:
            pool_kwargs: Dict[str, Any] = {"pool_pre_ping": pool_pre_ping}
            # SQLite uses SingletonThreadPool which does not accept
            # pool_size / max_overflow.  Only pass them for real
            # server-backed databases.
            if not connection_string.startswith("sqlite"):
                pool_kwargs["pool_size"] = pool_size
                pool_kwargs["max_overflow"] = max_overflow
                pool_kwargs["pool_timeout"] = pool_timeout
                pool_kwargs["pool_recycle"] = pool_recycle
            self._engine = create_engine(connection_string, **pool_kwargs)
        else:
            raise MetadataError("Either 'connection_string' or 'engine' is required")
        self._workspace_id = workspace_id
        self._retry_handler = RetryHandler(
            retry_count=warm_up_retries,
            retry_delay=warm_up_delay,
        )
        self._maybe_eager_prefetch()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def _supports_row_locks(self) -> bool:
        """``True`` when the backend supports ``SELECT … FOR UPDATE``."""
        return self._engine.dialect.name != "sqlite"

    def _session(self) -> Session:
        """Open a new database session, retrying on connection failure.

        The retry is intentionally shallow (default: 1 extra attempt with a
        short delay) to handle serverless databases that need a moment to
        resume from an auto-paused state.
        """
        return self._retry_handler.execute(Session, self._engine)[0]

    @staticmethod
    def _soft_delete_filter(table: Table) -> Any:
        """Return a ``deleted_at IS NULL`` filter clause."""
        col = table.c.get("deleted_at")
        if col is not None:
            return col.is_(None)
        return True  # table has no soft delete column

    def _workspace_filter(self, table: Table) -> Any:
        """Return a ``workspace_id = ?`` filter clause."""
        return table.c.workspace_id == self._workspace_id

    # ------------------------------------------------------------------
    # Row → Model mappers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_connection(row: Any) -> Connection:
        """Map a database row to a ``Connection`` model."""
        return Connection(
            connection_id=row.connection_id,
            workspace_id=row.workspace_id,
            name=row.name,
            connection_type=row.connection_type,
            format=row.format,
            catalog=getattr(row, "catalog", None),
            database=getattr(row, "database", None),
            configure=parse_json(row.configure),
            secrets_ref=row.secrets_ref,
            is_active=bool(row.is_active) if row.is_active is not None else True,
        )

    def _row_to_dataflow(self, row: Any, src_conn: Connection, dest_conn: Connection) -> DataFlow:
        """Map a database row to a ``DataFlow`` model."""
        # Source
        source = Source(
            connection=src_conn,
            schema_name=row.source_schema,
            table=row.source_table,
            query=row.source_query,
            python_function=row.source_python_function,
            watermark_columns=ensure_list(row.source_watermark_columns),
            configure=parse_json(row.source_configure),
        )

        # Destination
        dest_configure = parse_json(row.destination_configure)
        partition_columns = dest_configure.pop("partition_by", None) or dest_configure.pop("partition_columns", [])
        destination = Destination(
            connection=dest_conn,
            schema_name=row.destination_schema,
            table=row.destination_table,
            load_type=row.destination_load_type,
            merge_keys=ensure_list(row.destination_merge_keys),
            partition_columns=partition_columns,
            configure=dest_configure,
        )

        # Transform
        transform_raw = parse_json(row.transform)
        transform = Transform(**transform_raw) if transform_raw else Transform()

        return DataFlow(
            dataflow_id=row.dataflow_id,
            workspace_id=row.workspace_id,
            name=row.name,
            description=row.description,
            stage=row.stage,
            group_number=row.group_number,
            execution_order=row.execution_order,
            processing_mode=row.processing_mode or "batch",
            is_active=bool(row.is_active) if row.is_active is not None else True,
            source=source,
            destination=destination,
            transform=transform,
            configure=parse_json(row.configure),
        )

    @staticmethod
    def _row_to_schema_hint(row: Any) -> SchemaHint:
        """Map a database row to a ``SchemaHint`` model."""
        return SchemaHint(
            column_name=row.column_name,
            data_type=row.data_type,
            format=getattr(row, "format", None),
            precision=row.precision,
            scale=row.scale,
            default_value=row.default_value,
            ordinal_position=row.ordinal_position or 0,
            is_active=bool(row.is_active) if getattr(row, "is_active", None) is not None else True,
        )

    # ------------------------------------------------------------------
    # Connection lookup helper (used by dataflow builder)
    # ------------------------------------------------------------------

    def _get_connection_by_id_from_db(self, session: Session, connection_id: str) -> Optional[Connection]:
        """Fetch a single connection by ID within an existing session.

        Always applies the workspace filter for defence-in-depth and
        consistency with the other connection queries.
        """
        t = _connections_table
        stmt = select(t).where(
            and_(
                t.c.connection_id == connection_id,
                self._workspace_filter(t),
                self._soft_delete_filter(t),
            )
        )
        row = session.execute(stmt).first()
        return self._row_to_connection(row) if row else None

    # ------------------------------------------------------------------
    # Abstract method implementations — connections
    # ------------------------------------------------------------------

    def _fetch_connections(self, *, active_only: bool = True) -> List[Connection]:
        t = _connections_table
        clauses = [self._workspace_filter(t), self._soft_delete_filter(t)]
        if active_only:
            clauses.append(t.c.is_active == True)  # noqa: E712
        stmt = select(t).where(and_(*clauses))
        with self._session() as session:
            rows = session.execute(stmt).all()
            return [self._row_to_connection(r) for r in rows]

    def _fetch_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        t = _connections_table
        stmt = select(t).where(
            and_(
                t.c.connection_id == connection_id,
                self._workspace_filter(t),
                self._soft_delete_filter(t),
            )
        )
        with self._session() as session:
            row = session.execute(stmt).first()
            return self._row_to_connection(row) if row else None

    def _fetch_connection_by_name(self, name: str) -> Optional[Connection]:
        t = _connections_table
        stmt = select(t).where(
            and_(
                t.c.name == name,
                self._workspace_filter(t),
                self._soft_delete_filter(t),
            )
        )
        with self._session() as session:
            row = session.execute(stmt).first()
            return self._row_to_connection(row) if row else None

    # ------------------------------------------------------------------
    # Abstract method implementations — dataflows
    # ------------------------------------------------------------------

    def _fetch_dataflows(
        self,
        *,
        stages: Optional[List[str]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]:
        t = _dataflows_table
        clauses = [self._workspace_filter(t), self._soft_delete_filter(t)]
        if active_only:
            clauses.append(t.c.is_active == True)  # noqa: E712
        if stages is not None:
            clauses.append(t.c.stage.in_(stages))
        stmt = select(t).where(and_(*clauses))

        with self._session() as session:
            rows = session.execute(stmt).all()
            if not rows:
                return []

            # Batch-load all referenced connections in a single query
            # instead of issuing one SELECT per unique connection id.
            needed_ids: set = set()
            for row in rows:
                needed_ids.add(row.source_connection_id)
                needed_ids.add(row.destination_connection_id)

            ct = _connections_table
            conn_stmt = select(ct).where(
                and_(
                    ct.c.connection_id.in_(list(needed_ids)),
                    self._workspace_filter(ct),
                    self._soft_delete_filter(ct),
                )
            )
            conn_rows = session.execute(conn_stmt).all()
            conn_by_id: Dict[str, Connection] = {
                r.connection_id: self._row_to_connection(r) for r in conn_rows
            }

            dataflows: List[DataFlow] = []
            for row in rows:
                src_conn = conn_by_id.get(row.source_connection_id)
                dest_conn = conn_by_id.get(row.destination_connection_id)
                if src_conn is None or dest_conn is None:
                    continue  # skip orphaned dataflows
                dataflows.append(self._row_to_dataflow(row, src_conn, dest_conn))
            return dataflows

    def _fetch_dataflow_by_id(self, dataflow_id: str) -> Optional[DataFlow]:
        t = _dataflows_table
        stmt = select(t).where(
            and_(
                t.c.dataflow_id == dataflow_id,
                self._workspace_filter(t),
                self._soft_delete_filter(t),
            )
        )
        with self._session() as session:
            row = session.execute(stmt).first()
            if row is None:
                return None
            src_conn = self._get_connection_by_id_from_db(session, row.source_connection_id)
            dest_conn = self._get_connection_by_id_from_db(session, row.destination_connection_id)
            if src_conn is None or dest_conn is None:
                return None
            return self._row_to_dataflow(row, src_conn, dest_conn)

    # ------------------------------------------------------------------
    # Abstract method implementations — schema hints
    # ------------------------------------------------------------------

    def _fetch_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        t = _schema_hints_table
        clauses = [
            t.c.connection_id == connection_id,
            func.lower(t.c.table_name) == table_name.lower(),
            self._soft_delete_filter(t),
        ]
        if schema_name is not None:
            clauses.append(func.lower(t.c.schema_name) == schema_name.lower())
        stmt = select(t).where(and_(*clauses)).order_by(t.c.ordinal_position)
        with self._session() as session:
            rows = session.execute(stmt).all()
            return [self._row_to_schema_hint(r) for r in rows]

    # ------------------------------------------------------------------
    # Bulk pre-load — single-pass workspace fetch
    # ------------------------------------------------------------------

    def _bulk_load(
        self,
    ) -> Tuple[
        List[Connection],
        List[DataFlow],
        Dict[Tuple[str, Optional[str], str], List[SchemaHint]],
    ]:
        """Bulk-load the entire workspace via three queries in one session.

        - 1 SELECT for connections (workspace-scoped).
        - 1 SELECT for dataflows (workspace-scoped); src/dest connections
          resolved from the in-memory map built from the first query
          (no N+1).
        - 1 SELECT for schema hints filtered by
          ``connection_id IN (workspace_connection_ids)``.
        """
        ct = _connections_table
        dt = _dataflows_table
        ht = _schema_hints_table

        conn_clauses = [self._workspace_filter(ct), self._soft_delete_filter(ct)]
        df_clauses = [self._workspace_filter(dt), self._soft_delete_filter(dt)]

        with self._session() as session:
            conn_rows = session.execute(select(ct).where(and_(*conn_clauses))).all()
            connections = [self._row_to_connection(r) for r in conn_rows]
            conn_by_id: Dict[str, Connection] = {c.connection_id: c for c in connections}

            df_rows = session.execute(select(dt).where(and_(*df_clauses))).all()
            dataflows: List[DataFlow] = []
            for row in df_rows:
                src_conn = conn_by_id.get(row.source_connection_id)
                dest_conn = conn_by_id.get(row.destination_connection_id)
                if src_conn is None or dest_conn is None:
                    continue  # skip orphaned dataflows
                dataflows.append(self._row_to_dataflow(row, src_conn, dest_conn))

            grouped: Dict[Tuple[str, Optional[str], str], List[SchemaHint]] = {}
            if conn_by_id:
                hint_stmt = (
                    select(ht)
                    .where(
                        and_(
                            ht.c.connection_id.in_(list(conn_by_id.keys())),
                            self._soft_delete_filter(ht),
                        )
                    )
                    .order_by(ht.c.ordinal_position)
                )
                hint_rows = session.execute(hint_stmt).all()
                for r in hint_rows:
                    cid = r.connection_id
                    table = getattr(r, "table_name", None)
                    if not cid or not table:
                        continue
                    schema = getattr(r, "schema_name", None) or None  # normalise '' -> None
                    grouped.setdefault((cid, schema, table), []).append(
                        self._row_to_schema_hint(r)
                    )
        return connections, dataflows, grouped

    # ------------------------------------------------------------------
    # Abstract method implementations — watermarks
    # ------------------------------------------------------------------

    def get_watermark(self, dataflow_id: str) -> Optional[str]:
        """Return the raw serialised watermark string for *dataflow_id*, or ``None``."""
        t = _watermarks_table
        stmt = select(t).where(t.c.dataflow_id == dataflow_id)
        try:
            with self._session() as session:
                row = session.execute(stmt).first()
                return row.current_value if row and row.current_value else None
        except Exception as exc:
            raise WatermarkError(
                f"Failed to read watermark for dataflow {dataflow_id}"
            ) from exc

    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        """Upsert a watermark with atomic ``previous_value`` rotation.

        Strategy (avoids ``SELECT … FOR UPDATE`` gap locks which cause
        deadlocks on MySQL InnoDB under parallel load):

        1. Issue a single ``UPDATE`` that rotates ``previous_value`` from
           the current value in one statement (SQL evaluates all RHS
           expressions from pre-update values).
        2. If ``rowcount == 0``, the row does not yet exist → ``INSERT``.
        3. If the insert races with another worker and raises
           :class:`IntegrityError`, rollback and re-run the ``UPDATE``.

        Transient MySQL deadlocks / lock-wait timeouts (``OperationalError``)
        are retried up to ``_WATERMARK_MAX_RETRIES`` times.
        """
        t = _watermarks_table
        last_exc: Optional[Exception] = None
        for attempt in range(_WATERMARK_MAX_RETRIES + 1):
            now = utc_now()
            try:
                with self._session() as session:
                    update_stmt = (
                        t.update()
                        .where(t.c.dataflow_id == dataflow_id)
                        .values(
                            previous_value=t.c.current_value,
                            current_value=watermark_value,
                            job_id=job_id,
                            dataflow_run_id=dataflow_run_id,
                            updated_at=now,
                        )
                    )
                    result = session.execute(update_stmt)
                    if result.rowcount == 0:
                        try:
                            session.execute(
                                t.insert().values(
                                    watermark_id=generate_unique_id(),
                                    dataflow_id=dataflow_id,
                                    current_value=watermark_value,
                                    previous_value=None,
                                    job_id=job_id,
                                    dataflow_run_id=dataflow_run_id,
                                    updated_at=now,
                                )
                            )
                        except IntegrityError:
                            # Concurrent insert won the race — rotate via
                            # UPDATE now that the row exists.
                            session.rollback()
                            session.execute(update_stmt)
                    session.commit()
                return
            except OperationalError as exc:
                # Deadlock (MySQL 1213) / lock wait timeout (1205) /
                # serialization failure (PG 40001). Retry with backoff.
                last_exc = exc
                if attempt >= _WATERMARK_MAX_RETRIES:
                    break
                time.sleep(_WATERMARK_RETRY_DELAY * (attempt + 1))
            except Exception as exc:
                raise WatermarkError(
                    f"Failed to update watermark for dataflow {dataflow_id}"
                ) from exc
        raise WatermarkError(
            f"Failed to update watermark for dataflow {dataflow_id}"
        ) from last_exc

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Dispose the engine and clear cache."""
        super().close()
        if hasattr(self, "_engine") and self._engine is not None:
            self._engine.dispose()

    # ------------------------------------------------------------------
    # Schema management — utility for testing / setup
    # ------------------------------------------------------------------

    def create_tables(self) -> None:
        """Create all ``dc_framework_*`` tables (useful for testing)."""
        _meta.create_all(self._engine)

    def drop_tables(self) -> None:
        """Drop all ``dc_framework_*`` tables (useful for testing)."""
        _meta.drop_all(self._engine)
