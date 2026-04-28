"""Database source reader.

Reads data from relational databases via :meth:`engine.read_database`,
supporting both table and query modes.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import DatabaseType
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Source
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.sources.base import BaseSourceReader

logger = get_logger(__name__)


class DatabaseReader(BaseSourceReader[DF]):
    """Source reader for relational databases.

    Uses :meth:`engine.read_database` which delegates to the
    engine-specific external database reader (JDBC in Spark,
    connectorx/ADBC in Polars, etc.).

    Supports:
    * Table mode — read entire table (or schema.table).
    * Query mode — arbitrary SQL query executed on the source database.
    * Watermark-based incremental reads pushed down to SQL.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        super().__init__(engine)

    # ------------------------------------------------------------------
    # Core reading
    # ------------------------------------------------------------------

    def _read_internal(
        self,
        source: Source,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read data from a database.

        Steps:
            1. Build watermark WHERE clause and push down to SQL.
            2. Read via JDBC or SQL query.
            3. Calculate count and new watermark.
            4. Return ``None`` if zero rows.
        """
        df = self._read_data(source, watermark=watermark)

        context = f"Table: {source.full_table_name or 'query'} (type: {source.connection.database_type})"
        return self._finalize_read(df, source.watermark_columns, "DatabaseReader", context)

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
        *,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Read from a database via :meth:`engine.read_database`.

        When *watermark* is provided the reader pushes filters to SQL:

        * **Table mode** — builds
          ``SELECT * FROM <table> WHERE col1 > v1 OR col2 > v2``.
        * **Query mode** — wraps the user query as a subquery:
          ``SELECT * FROM (<user_query>) _q WHERE col1 > v1 OR col2 > v2``.

        On first run (*watermark* is ``None`` or empty) both modes
        execute the original query/table as-is — no placeholders required.

        Raises:
            SourceError: If neither query nor table is specified.
        """
        options = self._build_options(source, configure)

        wm_cols = source.watermark_columns or []
        active_wm: Dict[str, Any] = {}
        if watermark and wm_cols:
            active_wm = {c: watermark[c] for c in wm_cols if watermark.get(c) is not None}

        db_type = source.connection.database_type or ""
        where_clause = self._build_where_clause(active_wm, db_type=db_type) if active_wm else ""

        if source.query:
            logger.debug("DatabaseReader: executing query on source database")
            query = source.query
            if where_clause:
                query = f"SELECT * FROM ({query}) t1 WHERE {where_clause}"
            self._set_source_action({"reader": type(self).__name__, "query": query})
            return self._engine.read_database(query=query, options=options)

        if not source.table:
            raise SourceError(
                "DatabaseReader requires source.query or source.table",
                details={"connection_name": source.connection.name, "schema": source.schema_name},
            )

        # For JDBC, use plain table reference (database is already in the URI).
        # Avoid ``Source.full_table_name`` which uses backtick quoting and
        # prepends catalog/database — that format is for lakehouse engines
        # (Spark + Hive metastore).  JDBC engines (PostgreSQL, SQLite, etc.)
        # expect unquoted ``schema.table`` identifiers.  The two quoting
        # paths are intentionally separate; see ``Source.full_table_name``.
        table = f"{source.schema_name}.{source.table}" if source.schema_name else source.table
        if where_clause:
            query = f"SELECT * FROM {table} WHERE {where_clause}"
            logger.debug("DatabaseReader: reading table %s with watermark query", table)
            self._set_source_action({"reader": type(self).__name__, "table": table, "query": query})
            return self._engine.read_database(query=query, options=options)

        logger.debug("DatabaseReader: reading table %s via read_database", table)
        self._set_source_action({"reader": type(self).__name__, "table": table})
        return self._engine.read_database(table=table, options=options)

    # ------------------------------------------------------------------
    # Watermark SQL helpers
    # ------------------------------------------------------------------

    # Allowed pattern for SQL column identifiers — letters, digits,
    # underscores, and dots (for schema.column).  Rejects anything that
    # could be used for SQL injection (spaces, semicolons, quotes, etc.).
    _SAFE_COLUMN_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")

    @staticmethod
    def _escape_value(value: Any, db_type: str = "") -> str:
        """Return a SQL-safe literal for *value*.

        Strings, dates, and datetimes are single-quoted with internal
        single quotes escaped (``'`` → ``''``).  Numeric types are bare.
        Oracle requires TO_TIMESTAMP() for datetime values.
        """
        if isinstance(value, datetime):
            if db_type == DatabaseType.ORACLE:
                ts = value.strftime("%Y-%m-%d %H:%M:%S.%f")
                return f"TO_TIMESTAMP('{ts}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
            return f"'{value.isoformat()}'"
        if isinstance(value, date):
            if db_type == DatabaseType.ORACLE:
                return f"TO_DATE('{value.isoformat()}', 'YYYY-MM-DD')"
            return f"'{value.isoformat()}'"
        if isinstance(value, (int, float)):
            return str(value)
        # Default: treat as string — escape internal quotes and wrap
        safe = str(value).replace("'", "''")
        return f"'{safe}'"

    @staticmethod
    def _build_where_clause(watermark: Dict[str, Any], db_type: str = "") -> str:
        """Build ``col1 > v1 OR col2 > v2 …`` from watermark values.

        Column names are validated against :data:`_SAFE_COLUMN_RE` to
        prevent SQL injection via malicious metadata.
        """
        conditions = []
        for col, val in watermark.items():
            if not DatabaseReader._SAFE_COLUMN_RE.match(col):
                raise SourceError(
                    f"Invalid watermark column name: {col!r}. "
                    f"Column names must match [a-zA-Z_][a-zA-Z0-9_.]*",
                    details={"column": col},
                )
            escaped = DatabaseReader._escape_value(val, db_type=db_type)
            conditions.append(f"{col} > {escaped}")
        return " OR ".join(conditions)

    # ------------------------------------------------------------------
    # JDBC helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_options(source: Source, configure: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Build a flat options dict combining connection settings,
        ``source.read_options``, and any caller-supplied ``configure``.

        Framework-level keys (``database_type``, ``host``, ``port``,
        ``database``, ``user``, ``password``, ``driver``, ``url``) are
        passed through so each engine can build its own URL or
        connection string.  If ``url`` contains ``{user}``,
        ``{password}``, etc. placeholders they are resolved here.
        """
        conn = source.connection
        opts: Dict[str, Any] = {}

        if conn.database_type:
            opts["database_type"] = conn.database_type
        if conn.host:
            opts["host"] = conn.host
        if conn.port:
            opts["port"] = conn.port
        # Prefer configure["database"] (resolved by secrets_ref) over the model field
        # which is set at init time and not updated when secrets are resolved.
        db = conn.configure.get("database") or conn.database
        if db:
            opts["database"] = db
        if conn.username:
            opts["user"] = conn.username
        if conn.password:
            opts["password"] = conn.password
        if conn.driver:
            opts["driver"] = conn.driver

        # Explicit URL — resolve placeholders like {user}, {password}, etc.
        if conn.url:
            url = conn.url
            replacements = {
                "user": opts.get("user", ""),
                "password": opts.get("password", ""),
                "host": opts.get("host", ""),
                "port": str(opts.get("port", "")),
                "database": opts.get("database", ""),
            }
            try:
                url = url.format(**replacements)
            except KeyError:
                pass  # unknown placeholders are left as-is
            opts["url"] = url

        # Pass through any extra configure keys not explicitly handled above
        # (e.g. encrypt, trustServerCertificate for MSSQL).
        _handled_keys = {"database_type", "host", "port", "database", "username", "password", "driver", "url"}
        for k, v in conn.configure.items():
            if k not in _handled_keys and k not in opts:
                opts[k] = v

        opts.update(source.read_options)
        if configure:
            opts.update(configure)
        return opts
