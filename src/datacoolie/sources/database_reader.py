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
from datacoolie.core.secret_provider import unwrap_secret
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
        watermark_start: Optional[Dict[str, Any]] = None,
        *,
        watermark_end: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read data from a database.

        Steps:
            1. Build watermark WHERE clause and push down to SQL.
            2. Read via JDBC or SQL query.
            3. Calculate count and new watermark.
            4. Return ``None`` if zero rows.
        """
        df = self._read_data(source, watermark_start=watermark_start, watermark_end=watermark_end)

        context = f"Table: {source.full_table_name or 'query'} (type: {source.connection.database_type})"
        return self._finalize_read(df, source.watermark_columns, type(self).__name__, context)

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
        *,
        watermark_start: Optional[Dict[str, Any]] = None,
        watermark_end: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Read from a database via :meth:`engine.read_database`.

        When *watermark* is provided the reader pushes filters to SQL:

        * **Table mode** — builds
          ``SELECT * FROM <table> WHERE col1 > v1 OR col2 > v2``.
        * **Query mode** — wraps the user query as a subquery:
          ``SELECT * FROM (<user_query>) _q WHERE col1 > v1 OR col2 > v2``.

        When *watermark_end* is provided, builds windowed conditions:
          ``(col1 > lower1 AND col1 < upper1) OR (col2 > lower2 AND col2 < upper2)``

        On first run (*watermark* is ``None`` or empty) both modes
        execute the original query/table as-is — no placeholders required.

        Raises:
            SourceError: If neither query nor table is specified.
        """
        options = self._build_options(source, configure)

        wm_cols = source.watermark_columns or []
        active_wm: Dict[str, Any] = {}
        if watermark_start and wm_cols:
            active_wm = {c: watermark_start[c] for c in wm_cols if watermark_start.get(c) is not None}

        active_upper: Dict[str, Any] = {}
        if watermark_end and wm_cols:
            active_upper = {c: watermark_end[c] for c in wm_cols if watermark_end.get(c) is not None}

        db_type = source.connection.database_type or ""

        where_clause = self._build_window_where_clause(
            active_wm, active_upper, db_type=db_type,
            lower_op=self._watermark_start_operator,
            upper_op=self._watermark_end_operator,
        )

        # Append user-supplied filter_expression to the WHERE clause.
        if source.filter_expression:
            filter_clause = f"({source.filter_expression})"
            where_clause = f"{where_clause} AND {filter_clause}" if where_clause else filter_clause

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
            logger.debug("%s: reading table %s with query", type(self).__name__, table)
            self._set_source_action({"reader": type(self).__name__, "table": table, "query": query})
            return self._engine.read_database(query=query, options=options)

        logger.debug("%s: reading table %s via read_database", type(self).__name__, table)
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
    def _build_window_where_clause(
        lower: Dict[str, Any],
        upper: Dict[str, Any],
        db_type: str = "",
        lower_op: str = ">",
        upper_op: str = "<",
    ) -> str:
        """Build per-column windowed WHERE clause OR'd across columns.

        Produces: ``(col1 > l1 AND col1 < u1) OR (col2 > l2 AND col2 < u2)``

        Per-column edge cases:
        - lower has col, upper has col → ``(col > lower AND col < upper)``
        - lower has col, upper missing → ``col > lower``
        - lower missing, upper has col → ``col < upper``
        - both missing → skip column
        """
        if lower_op not in (">", ">="):
            lower_op = ">"
        if upper_op not in ("<", "<="):
            upper_op = "<"

        all_cols = set(lower) | set(upper)
        conditions = []
        for col in all_cols:
            if not DatabaseReader._SAFE_COLUMN_RE.match(col):
                raise SourceError(
                    f"Invalid watermark column name: {col!r}. "
                    f"Column names must match [a-zA-Z_][a-zA-Z0-9_.]*",
                    details={"column": col},
                )
            lower_val = lower.get(col)
            upper_val = upper.get(col)
            if lower_val is None and upper_val is None:
                continue
            parts = []
            if lower_val is not None:
                escaped = DatabaseReader._escape_value(lower_val, db_type=db_type)
                parts.append(f"{col} {lower_op} {escaped}")
            if upper_val is not None:
                escaped = DatabaseReader._escape_value(upper_val, db_type=db_type)
                parts.append(f"{col} {upper_op} {escaped}")
            if len(parts) == 1:
                conditions.append(parts[0])
            else:
                conditions.append(f"({' AND '.join(parts)})")
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
            opts["database"] = unwrap_secret(db)
        if conn.username:
            opts["user"] = unwrap_secret(conn.username)
        if conn.password:
            opts["password"] = unwrap_secret(conn.password)
        if conn.driver:
            opts["driver"] = conn.driver

        # Explicit URL — resolve placeholders like {user}, {password}, etc.
        if conn.url:
            url = unwrap_secret(conn.url)
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
                opts[k] = unwrap_secret(v)

        opts.update(source.read_options)
        if configure:
            opts.update(configure)
        return opts
