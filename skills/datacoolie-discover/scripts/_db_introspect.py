"""Database introspection via SQLAlchemy INFORMATION_SCHEMA queries.

Supports: PostgreSQL, MySQL, MSSQL, Oracle, SQLite + generic fallback.
"""

from __future__ import annotations

import sys

from _types import Column, SourceResult, Table


def introspect_db(connection_str: str, schema_filter: str | None = None,
                  engine=None) -> SourceResult:
    """Extract schema from database using SQLAlchemy."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        print("ERROR: sqlalchemy is required for database introspection.", file=sys.stderr)
        print("Install with: pip install sqlalchemy", file=sys.stderr)
        sys.exit(1)

    if engine is None:
        engine = create_engine(connection_str, echo=False)
    dialect = engine.dialect.name

    result = SourceResult(
        source_type="db",
        dialect=dialect,
        database=engine.url.database,
        host=engine.url.host,
        port=engine.url.port,
    )

    with engine.connect() as conn:
        tables = _get_tables(conn, dialect, schema_filter)
        for tbl in tables:
            columns_raw = _get_columns(conn, dialect, tbl["schema"], tbl["name"])
            pk_cols = _get_primary_keys(conn, dialect, tbl["schema"], tbl["name"])
            fk_list = _get_foreign_keys(conn, dialect, tbl["schema"], tbl["name"])
            idx_list = _get_indexes(conn, dialect, tbl["schema"], tbl["name"])

            # Build FK column → reference mapping for quick lookup
            fk_col_refs: dict[str, str] = {}
            for fk in fk_list:
                for local_col, ref_col in zip(fk.columns, fk.references_columns):
                    fk_col_refs[local_col] = f"{fk.references_table}.{ref_col}"

            columns = [
                Column(
                    name=c["name"],
                    data_type=c["data_type"],
                    ordinal=c.get("ordinal", 0),
                    nullable=c.get("nullable", True),
                    is_primary_key=c["name"] in pk_cols,
                    is_foreign_key=c["name"] in fk_col_refs,
                    references=fk_col_refs.get(c["name"]),
                )
                for c in columns_raw
                if "error" not in c
            ]

            table = Table(
                table_name=tbl["name"],
                schema=tbl["schema"],
                columns=columns,
                primary_key=pk_cols,
                foreign_keys=fk_list,
                indexes=idx_list,
                row_estimate=_get_row_estimate(conn, dialect, tbl["schema"], tbl["name"]),
            )
            result.tables.append(table)

    result.schema_count = len(set(t.schema for t in result.tables if t.schema))
    result.total_columns = sum(len(t.columns) for t in result.tables)
    result.estimated_total_rows = sum(t.row_estimate or 0 for t in result.tables)
    engine.dispose()
    return result


def _get_tables(conn, dialect: str, schema_filter: str | None) -> list[dict]:
    """Get list of user tables from catalog."""
    from sqlalchemy import text

    system_schemas = {
        "information_schema", "pg_catalog", "pg_toast",
        "sys", "INFORMATION_SCHEMA", "guest",
        "mysql", "performance_schema",
    }

    if dialect in ("postgresql", "mssql", "mysql"):
        query = text("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
        """)
        rows = conn.execute(query).fetchall()
        tables = [
            {"schema": r[0], "name": r[1]}
            for r in rows
            if r[0] not in system_schemas
        ]
    elif dialect == "sqlite":
        query = text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        rows = conn.execute(query).fetchall()
        tables = [{"schema": "main", "name": r[0]} for r in rows]
    elif dialect == "oracle":
        # Default to the current session user; --schema overrides to a specific owner.
        if schema_filter:
            query = text("""
                SELECT owner, table_name
                FROM all_tables
                WHERE owner = :owner
                ORDER BY table_name
            """)
            rows = conn.execute(query, {"owner": schema_filter.upper()}).fetchall()
        else:
            query = text("""
                SELECT owner, table_name
                FROM all_tables
                WHERE owner = SYS_CONTEXT('USERENV', 'SESSION_USER')
                ORDER BY table_name
            """)
            rows = conn.execute(query).fetchall()
        tables = [{"schema": r[0], "name": r[1]} for r in rows]
    else:
        query = text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
        rows = conn.execute(query).fetchall()
        tables = [{"schema": r[0], "name": r[1]} for r in rows if r[0] not in system_schemas]

    if schema_filter:
        tables = [t for t in tables if t["schema"] == schema_filter]

    return tables


def _get_columns(conn, dialect: str, schema: str, table: str) -> list[dict]:
    """Get column definitions from catalog."""
    from sqlalchemy import text

    if dialect == "sqlite":
        query = text(f"PRAGMA table_info('{table}')")
        rows = conn.execute(query).fetchall()
        return [
            {"name": r[1], "data_type": r[2] or "TEXT", "nullable": not bool(r[3]), "ordinal": r[0] + 1}
            for r in rows
        ]
    elif dialect == "oracle":
        query = text("""
            SELECT column_name, data_type, nullable, column_id
            FROM all_tab_columns
            WHERE owner = :schema AND table_name = :table
            ORDER BY column_id
        """)
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        return [
            {"name": r[0], "data_type": r[1], "nullable": r[2] == "Y", "ordinal": r[3]}
            for r in rows
        ]
    else:
        query = text("""
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """)
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        return [
            {"name": r[0], "data_type": r[1], "nullable": r[2] == "YES", "ordinal": r[3]}
            for r in rows
        ]


def _get_primary_keys(conn, dialect: str, schema: str, table: str) -> list[str]:
    """Get primary key columns."""
    from sqlalchemy import text

    if dialect == "sqlite":
        query = text(f"PRAGMA table_info('{table}')")
        rows = conn.execute(query).fetchall()
        return [r[1] for r in rows if r[5] > 0]
    elif dialect == "oracle":
        query = text("""
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
            WHERE cons.constraint_type = 'P' AND cons.owner = :schema AND cons.table_name = :table
            ORDER BY cols.position
        """)
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        return [r[0] for r in rows]
    elif dialect == "postgresql":
        query = text("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary AND n.nspname = :schema AND c.relname = :table
        """)
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        return [r[0] for r in rows]
    elif dialect == "mssql":
        # MSSQL PK constraint names are custom (e.g. PK_Address_AddressID),
        # not the MySQL-style 'PRIMARY' literal — must join on constraint_type.
        query = text("""
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA    = kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA    = :schema
              AND tc.TABLE_NAME      = :table
            ORDER BY kcu.ORDINAL_POSITION
        """)
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        return [r[0] for r in rows]
    else:
        # MySQL: PK constraint is always named 'PRIMARY'
        query = text("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = :schema AND table_name = :table
              AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
        """)
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        return [r[0] for r in rows]


def _get_row_estimate(conn, dialect: str, schema: str, table: str) -> int | None:
    """Get row count estimate (stats-based where possible)."""
    from sqlalchemy import text

    try:
        if dialect == "postgresql":
            query = text("""
                SELECT reltuples::bigint
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema AND c.relname = :table
            """)
            row = conn.execute(query, {"schema": schema, "table": table}).fetchone()
            return int(row[0]) if row and row[0] >= 0 else None
        elif dialect == "mssql":
            query = text("""
                SELECT SUM(p.rows) AS row_count
                FROM sys.partitions p
                JOIN sys.tables t ON p.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = :schema AND t.name = :table AND p.index_id IN (0,1)
            """)
            row = conn.execute(query, {"schema": schema, "table": table}).fetchone()
            return int(row[0]) if row and row[0] else None
        elif dialect == "mysql":
            query = text("""
                SELECT table_rows
                FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :table
            """)
            row = conn.execute(query, {"schema": schema, "table": table}).fetchone()
            return int(row[0]) if row and row[0] else None
        elif dialect == "oracle":
            # ALL_TABLES.NUM_ROWS is populated after DBMS_STATS.GATHER_SCHEMA_STATS
            query = text("""
                SELECT num_rows
                FROM all_tables
                WHERE owner = :schema AND table_name = :table
            """)
            row = conn.execute(query, {"schema": schema, "table": table}).fetchone()
            return int(row[0]) if row and row[0] is not None else None
        else:
            return None
    except Exception:
        return None


def _get_foreign_keys(conn, dialect: str, schema: str, table: str) -> list:
    """Get foreign key constraints — returns list[ForeignKey]."""
    from sqlalchemy import text
    from _types import ForeignKey

    try:
        if dialect == "sqlite":
            rows = conn.execute(text(f"PRAGMA foreign_key_list('{table}')")).fetchall()
            grouped: dict[int, dict] = {}
            for r in rows:
                fk_id = r[0]
                if fk_id not in grouped:
                    grouped[fk_id] = {"ref_table": r[2], "columns": [], "ref_columns": []}
                grouped[fk_id]["columns"].append(r[3])
                grouped[fk_id]["ref_columns"].append(r[4] or r[3])
            return [
                ForeignKey(columns=v["columns"], references_table=v["ref_table"], references_columns=v["ref_columns"])
                for v in grouped.values()
            ]

        elif dialect == "postgresql":
            # Use pg_constraint for reliable multi-column FK unpacking
            query = text("""
                SELECT con.conname AS constraint_name,
                       attr.attname AS column_name,
                       nsp_r.nspname AS r_schema,
                       cls_r.relname AS r_table,
                       attr_r.attname AS r_column
                FROM pg_constraint con
                JOIN pg_class     cls   ON cls.oid   = con.conrelid
                JOIN pg_namespace nsp   ON nsp.oid   = cls.relnamespace
                JOIN pg_class     cls_r ON cls_r.oid = con.confrelid
                JOIN pg_namespace nsp_r ON nsp_r.oid = cls_r.relnamespace,
                unnest(con.conkey, con.confkey) AS pairs(fk_col, ref_col)
                JOIN pg_attribute attr   ON attr.attrelid   = con.conrelid  AND attr.attnum   = pairs.fk_col
                JOIN pg_attribute attr_r ON attr_r.attrelid = con.confrelid AND attr_r.attnum = pairs.ref_col
                WHERE con.contype = 'f'
                  AND nsp.nspname = :schema
                  AND cls.relname = :table
                ORDER BY con.conname, pairs.fk_col
            """)

        elif dialect == "mysql":
            query = text("""
                SELECT constraint_name,
                       column_name,
                       referenced_table_schema AS r_schema,
                       referenced_table_name   AS r_table,
                       referenced_column_name  AS r_column
                FROM information_schema.key_column_usage
                WHERE table_schema = :schema
                  AND table_name   = :table
                  AND referenced_table_name IS NOT NULL
                ORDER BY constraint_name, ordinal_position
            """)

        elif dialect == "mssql":
            query = text("""
                SELECT fk.name        AS constraint_name,
                       col.name       AS column_name,
                       SCHEMA_NAME(rt.schema_id) AS r_schema,
                       rt.name        AS r_table,
                       rcol.name      AS r_column
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc
                     ON fk.object_id = fkc.constraint_object_id
                JOIN sys.tables  t   ON t.object_id  = fkc.parent_object_id
                JOIN sys.schemas s   ON s.schema_id  = t.schema_id
                JOIN sys.columns col
                     ON col.object_id = fkc.parent_object_id
                    AND col.column_id = fkc.parent_column_id
                JOIN sys.tables  rt  ON rt.object_id  = fkc.referenced_object_id
                JOIN sys.columns rcol
                     ON rcol.object_id = fkc.referenced_object_id
                    AND rcol.column_id = fkc.referenced_column_id
                WHERE s.name = :schema AND t.name = :table
                ORDER BY fk.name, fkc.constraint_column_id
            """)

        elif dialect == "oracle":
            query = text("""
                SELECT ac.constraint_name,
                       acc.column_name,
                       rc.owner        AS r_schema,
                       rc.table_name   AS r_table,
                       rcc.column_name AS r_column
                FROM all_constraints ac
                JOIN all_cons_columns acc
                     ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
                JOIN all_constraints rc
                     ON ac.r_owner = rc.owner AND ac.r_constraint_name = rc.constraint_name
                JOIN all_cons_columns rcc
                     ON rc.owner = rcc.owner
                    AND rc.constraint_name = rcc.constraint_name
                    AND acc.position = rcc.position
                WHERE ac.constraint_type = 'R'
                  AND ac.owner = :schema AND ac.table_name = :table
                ORDER BY ac.constraint_name, acc.position
            """)

        else:
            return []

        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        # Group rows → one ForeignKey per constraint_name
        grouped_fk: dict[str, dict] = {}
        for r in rows:
            cname = r[0]
            if cname not in grouped_fk:
                r_schema = r[2] or ""
                r_table = r[3]
                grouped_fk[cname] = {
                    "ref_table": f"{r_schema}.{r_table}" if r_schema else r_table,
                    "columns": [],
                    "ref_columns": [],
                }
            grouped_fk[cname]["columns"].append(r[1])
            grouped_fk[cname]["ref_columns"].append(r[4])
        return [
            ForeignKey(columns=v["columns"], references_table=v["ref_table"], references_columns=v["ref_columns"])
            for v in grouped_fk.values()
        ]
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def _get_indexes(conn, dialect: str, schema: str, table: str) -> list:
    """Get non-PK indexes — returns list[Index]."""
    from sqlalchemy import text
    from _types import Index

    try:
        if dialect == "sqlite":
            idx_rows = conn.execute(text(f"PRAGMA index_list('{table}')")).fetchall()
            indexes = []
            for idx_row in idx_rows:
                idx_name = idx_row[1]
                is_unique = bool(idx_row[2])
                col_rows = conn.execute(text(f"PRAGMA index_info('{idx_name}')")).fetchall()
                cols = [r[2] for r in col_rows]
                indexes.append(Index(name=idx_name, columns=cols, unique=is_unique))
            return indexes

        elif dialect == "postgresql":
            query = text("""
                SELECT i.relname AS idx_name,
                       a.attname AS col_name,
                       ix.indisunique AS is_unique
                FROM pg_index ix
                JOIN pg_class     i ON i.oid = ix.indexrelid
                JOIN pg_class     t ON t.oid = ix.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE n.nspname = :schema AND t.relname = :table
                  AND NOT ix.indisprimary
                ORDER BY i.relname, a.attnum
            """)

        elif dialect == "mysql":
            query = text("""
                SELECT index_name,
                       column_name,
                       NOT non_unique AS is_unique
                FROM information_schema.statistics
                WHERE table_schema = :schema AND table_name = :table
                  AND index_name <> 'PRIMARY'
                ORDER BY index_name, seq_in_index
            """)

        elif dialect == "mssql":
            query = text("""
                SELECT i.name    AS idx_name,
                       c.name    AS col_name,
                       i.is_unique
                FROM sys.indexes i
                JOIN sys.index_columns ic
                     ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c
                     ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.tables  t ON t.object_id = i.object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = :schema AND t.name = :table
                  AND i.is_primary_key = 0
                  AND i.type > 0
                ORDER BY i.name, ic.key_ordinal
            """)

        elif dialect == "oracle":
            query = text("""
                SELECT ai.index_name,
                       aic.column_name,
                       CASE WHEN ai.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique
                FROM all_indexes ai
                JOIN all_ind_columns aic
                     ON ai.owner = aic.index_owner AND ai.index_name = aic.index_name
                WHERE ai.table_owner = :schema AND ai.table_name = :table
                ORDER BY ai.index_name, aic.column_position
            """)

        else:
            return []

        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        # Group rows → one Index per idx_name
        grouped_idx: dict[str, dict] = {}
        for r in rows:
            idx_name = r[0]
            if idx_name not in grouped_idx:
                grouped_idx[idx_name] = {"columns": [], "unique": bool(r[2])}
            grouped_idx[idx_name]["columns"].append(r[1])
        return [
            Index(name=k, columns=v["columns"], unique=v["unique"])
            for k, v in grouped_idx.items()
        ]
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return []
