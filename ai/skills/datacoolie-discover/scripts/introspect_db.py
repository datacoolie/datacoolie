"""introspect_db.py — Database schema introspection via SQLAlchemy.

Connects to any SQLAlchemy-supported database, extracts table/column metadata,
and outputs a standardised CSV matching the datacoolie discover schema contract.

Usage:
    python introspect_db.py --url "postgresql://user:pass@host/db" --source erp
    python introspect_db.py --url "mysql+pymysql://..." --source crm --schemas sales,hr
    python introspect_db.py --url "sqlite:///local.db" --source app --output schema.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import (
    BigInteger, Boolean, Date, DateTime, Float, Integer, LargeBinary,
    Numeric, SmallInteger, String, Text, Time,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CSV_HEADER = [
    "source", "schema", "table", "column", "type", "format",
    "precision", "scale", "nullable", "pk", "fk",
    "ordinal_position", "row_estimate", "notes",
]

SYSTEM_SCHEMAS: dict[str, set[str]] = {
    "postgresql": {"information_schema", "pg_catalog", "pg_toast"},
    "redshift":   {"information_schema", "pg_catalog", "pg_toast"},
    "mysql":      {"information_schema", "mysql", "performance_schema", "sys"},
    "mariadb":    {"information_schema", "mysql", "performance_schema", "sys"},
    "mssql":      {"sys", "INFORMATION_SCHEMA", "guest"},
    "oracle":     {"SYS", "SYSTEM", "DBSNMP", "OUTLN"},
    "snowflake":  {"INFORMATION_SCHEMA"},
}

# Row-estimate queries per dialect.  Use :schema / :table as bind params.
# SQLite needs special handling (table name can't be a bind param).
ROW_ESTIMATE_QUERIES: dict[str, str] = {
    "postgresql": (
        "SELECT reltuples::bigint FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = :schema AND c.relname = :table"
    ),
    "redshift": (
        'SELECT "rows" FROM svv_table_info '
        'WHERE "schema" = :schema AND "table" = :table'
    ),
    "mysql": (
        "SELECT table_rows FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_name = :table"
    ),
    "mariadb": (
        "SELECT table_rows FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_name = :table"
    ),
    "mssql": (
        "SELECT SUM(p.rows) FROM sys.partitions p "
        "JOIN sys.tables t ON p.object_id = t.object_id "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = :schema AND t.name = :table AND p.index_id IN (0,1)"
    ),
    "oracle": (
        "SELECT num_rows FROM all_tables "
        "WHERE owner = :schema AND table_name = :table"
    ),
    "snowflake": (
        "SELECT row_count FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_name = :table"
    ),
    "bigquery": (
        "SELECT row_count FROM __TABLES__ WHERE table_id = :table"
    ),
}

# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

def map_type(sa_type: Any) -> tuple[str, str, str, str]:
    """Map a SQLAlchemy column type to (canonical, format, precision, scale).

    Returns strings (empty string when not applicable) so CSV output is clean.
    """
    type_obj = sa_type
    type_name = type(type_obj).__name__.upper()
    # Also grab the raw compiled string for format hints
    try:
        raw_str = str(type_obj)
    except Exception:
        raw_str = type_name

    precision = ""
    scale = ""
    fmt = ""

    # --- Numeric types ---
    if isinstance(type_obj, Boolean):
        return "boolean", "", "", ""
    if isinstance(type_obj, SmallInteger):
        return "short", "", "", ""
    if isinstance(type_obj, BigInteger):
        return "long", "", "", ""
    if isinstance(type_obj, Integer):
        return "integer", "", "", ""
    if isinstance(type_obj, Numeric):
        canonical = "decimal"
        p = getattr(type_obj, "precision", None)
        s = getattr(type_obj, "scale", None)
        if p is not None:
            precision = str(p)
            canonical = f"decimal({p},{s or 0})"
        if s is not None:
            scale = str(s)
        if getattr(type_obj, "asdecimal", True) is False:
            # Numeric with asdecimal=False → float behaviour
            return "double", "", "", ""
        return canonical, "", precision, scale
    if isinstance(type_obj, Float):
        p = getattr(type_obj, "precision", None)
        if p is not None and p > 24:
            return "double", "", "", ""
        return "float", "", "", ""

    # --- String types ---
    if isinstance(type_obj, (String, Text)):
        length = getattr(type_obj, "length", None)
        if length is not None:
            precision = str(length)
        # Detect special sub-types by raw name
        upper = type_name
        if "UUID" in upper or "UNIQUEIDENTIFIER" in upper:
            return "string", "", precision, ""
        if "JSON" in upper:
            return "string", "json", precision, ""
        if "XML" in upper:
            return "string", "xml", precision, ""
        return "string", "", precision, ""

    # --- Date/time types ---
    if isinstance(type_obj, Date):
        return "date", "", "", ""
    if isinstance(type_obj, DateTime):
        tz = getattr(type_obj, "timezone", False)
        return ("timestamp_tz" if tz else "timestamp"), "", "", ""
    if isinstance(type_obj, Time):
        return "time", "", "", ""

    # --- Binary ---
    if isinstance(type_obj, LargeBinary):
        length = getattr(type_obj, "length", None)
        if length is not None:
            precision = str(length)
        return "binary", "", precision, ""

    # --- Fallback: use dialect-specific type name for best-effort mapping ---
    upper = type_name
    raw_upper = raw_str.upper()

    # Bit (MSSQL boolean)
    if upper == "BIT":
        return "boolean", "", "", ""
    # Tinyint
    if "TINYINT" in upper:
        return "byte", "", "", ""
    # Array
    if "ARRAY" in upper:
        return "array", raw_str, "", ""
    # JSON / JSONB
    if upper in ("JSON", "JSONB", "VARIANT"):
        return "string", "json", "", ""
    # UUID
    if upper in ("UUID", "UNIQUEIDENTIFIER"):
        return "string", "uuid", "", ""
    # Bytea / BLOB / RAW
    if upper in ("BYTEA", "BLOB", "RAW", "IMAGE"):
        return "binary", "", "", ""
    # CLOB / NCLOB / LONG
    if upper in ("CLOB", "NCLOB", "LONG"):
        return "string", "", "", ""
    # STRUCT / OBJECT / MAP
    if upper in ("STRUCT", "OBJECT", "MAP", "RECORD"):
        return "struct", raw_str, "", ""
    # TIMESTAMP WITH TIME ZONE variants
    if "TIMESTAMP" in upper and ("TZ" in upper or "TIME ZONE" in upper):
        return "timestamp_tz", "", "", ""
    if "TIMESTAMP" in upper or "DATETIME" in upper:
        return "timestamp", "", "", ""
    if "DATE" in upper:
        return "date", "", "", ""
    if "TIME" in upper:
        return "time", "", "", ""
    # NUMBER / NUMERIC with precision from raw string
    m = re.search(r"NUMBER\((\d+),\s*(\d+)\)", raw_upper)
    if m:
        p, s = m.group(1), m.group(2)
        if s == "0":
            if int(p) <= 10:
                return "integer", "", "", ""
            return "long", "", "", ""
        return f"decimal({p},{s})", "", p, s
    # FLOAT / REAL / DOUBLE
    if "DOUBLE" in upper or "FLOAT8" in upper:
        return "double", "", "", ""
    if "FLOAT" in upper or "REAL" in upper:
        return "float", "", "", ""
    if "INT" in upper:
        if "BIG" in upper:
            return "long", "", "", ""
        if "SMALL" in upper:
            return "short", "", "", ""
        if "TINY" in upper:
            return "byte", "", "", ""
        return "integer", "", "", ""
    # String-like fallback
    if "CHAR" in upper or "TEXT" in upper or "STRING" in upper or "VARCHAR" in upper:
        return "string", "", "", ""

    # Unknown type — pass through as-is with format hint
    return raw_str.lower(), raw_str, "", ""


# ---------------------------------------------------------------------------
# Foreign key helpers
# ---------------------------------------------------------------------------

def _build_fk_map(fk_list: list[dict]) -> dict[str, str]:
    """Build {column_name: '→ referred_schema.table.column'} from inspector FK list."""
    fk_map: dict[str, str] = {}
    for fk in fk_list:
        ref_schema = fk.get("referred_schema") or ""
        ref_table = fk.get("referred_table", "")
        constrained = fk.get("constrained_columns", [])
        referred = fk.get("referred_columns", [])
        for local_col, ref_col in zip(constrained, referred):
            if ref_schema:
                fk_map[local_col] = f"→ {ref_schema}.{ref_table}.{ref_col}"
            else:
                fk_map[local_col] = f"→ {ref_table}.{ref_col}"
    return fk_map


# ---------------------------------------------------------------------------
# Row estimates
# ---------------------------------------------------------------------------

def _get_row_estimate(conn, dialect_name: str, schema: str, table: str) -> str:
    """Return row estimate as string, or empty string if unavailable."""
    if dialect_name == "sqlite":
        try:
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"'))
            row = result.fetchone()
            return str(row[0]) if row else ""
        except Exception:
            return ""

    query_tpl = ROW_ESTIMATE_QUERIES.get(dialect_name)
    if not query_tpl:
        return ""

    try:
        params: dict[str, str] = {"table": table}
        if ":schema" in query_tpl:
            params["schema"] = schema
        result = conn.execute(text(query_tpl), params)
        row = result.fetchone()
        if row and row[0] is not None:
            return str(int(row[0]))
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Credential masking
# ---------------------------------------------------------------------------

_URL_PASSWORD_RE = re.compile(r"://([^:]+):([^@]+)@")


def _mask_url(url: str) -> str:
    """Replace password in a connection URL with '***'."""
    return _URL_PASSWORD_RE.sub(r"://\1:***@", url)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _resolve_dialect(engine: Engine) -> str:
    """Return a normalised dialect name string."""
    name = engine.dialect.name.lower()
    # Normalise common aliases
    if name.startswith("postgres"):
        return "postgresql"
    return name


def _get_system_schemas(dialect: str) -> set[str]:
    """Return system schemas to exclude for the given dialect."""
    return SYSTEM_SCHEMAS.get(dialect, set())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Introspect a database and output a standardised schema CSV.",
    )
    parser.add_argument("--url", required=True, help="SQLAlchemy connection URL")
    parser.add_argument("--source", required=True, help="Source name for CSV output")
    parser.add_argument(
        "--schemas", default=None,
        help="Comma-separated schema filter (default: all non-system)",
    )
    parser.add_argument(
        "--tables", default=None,
        help="Comma-separated table filter (default: all)",
    )
    parser.add_argument(
        "--output", default=None,
        help="Output CSV file path (default: stdout)",
    )
    return parser.parse_args(argv)


def introspect(
    url: str,
    source: str,
    schema_filter: list[str] | None = None,
    table_filter: list[str] | None = None,
    output_path: str | None = None,
) -> None:
    """Run introspection and write CSV output."""
    try:
        engine = create_engine(url)
    except Exception as exc:
        print(f"ERROR: Cannot create engine for {_mask_url(url)}: {exc}", file=sys.stderr)
        sys.exit(1)

    dialect = _resolve_dialect(engine)
    system_schemas = _get_system_schemas(dialect)

    try:
        insp = inspect(engine)
    except Exception as exc:
        print(f"ERROR: Cannot connect to {_mask_url(url)}: {exc}", file=sys.stderr)
        sys.exit(1)

    # Resolve schemas
    if schema_filter:
        schemas = schema_filter
    else:
        try:
            all_schemas = insp.get_schema_names()
        except Exception:
            all_schemas = [None]  # type: ignore[list-item]
        schemas = [s for s in all_schemas if s not in system_schemas]
        if not schemas:
            schemas = [None]  # type: ignore[list-item]

    # Prepare output
    out_file = open(output_path, "w", newline="", encoding="utf-8") if output_path else sys.stdout
    try:
        writer = csv.writer(out_file, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(CSV_HEADER)

        with engine.connect() as conn:
            for schema_name in schemas:
                try:
                    all_tables = insp.get_table_names(schema=schema_name)
                except Exception as exc:
                    print(
                        f"WARNING: Cannot list tables in schema '{schema_name}': {exc}",
                        file=sys.stderr,
                    )
                    continue

                tables = all_tables
                if table_filter:
                    table_set = set(table_filter)
                    tables = [t for t in all_tables if t in table_set]

                for table_name in tables:
                    try:
                        _introspect_table(
                            writer, conn, insp, dialect,
                            source, schema_name, table_name,
                        )
                    except Exception as exc:
                        print(
                            f"WARNING: Skipping {schema_name}.{table_name}: {exc}",
                            file=sys.stderr,
                        )
    finally:
        if output_path and out_file is not sys.stdout:
            out_file.close()


def _introspect_table(
    writer: csv.writer,
    conn,
    insp,
    dialect: str,
    source: str,
    schema_name: str | None,
    table_name: str,
) -> None:
    """Introspect a single table and write rows to CSV."""
    columns = insp.get_columns(table_name, schema=schema_name)

    # Primary keys
    pk_info = insp.get_pk_constraint(table_name, schema=schema_name)
    pk_cols = set(pk_info.get("constrained_columns", []) if pk_info else [])

    # Foreign keys
    fk_list = insp.get_foreign_keys(table_name, schema=schema_name)
    fk_map = _build_fk_map(fk_list)

    # Row estimate
    row_est = _get_row_estimate(conn, dialect, schema_name or "", table_name)

    schema_str = schema_name or ""

    for ordinal, col in enumerate(columns, start=1):
        canonical, fmt, prec, scl = map_type(col["type"])
        is_pk = "true" if col["name"] in pk_cols else ""
        nullable = "true" if col.get("nullable", True) else "false"

        writer.writerow([
            source,
            schema_str,
            table_name,
            col["name"],
            canonical,
            fmt,
            prec,
            scl,
            nullable,
            is_pk,
            fk_map.get(col["name"], ""),
            ordinal,
            row_est,
            "",
        ])


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    schema_filter = [s.strip() for s in args.schemas.split(",")] if args.schemas else None
    table_filter = [t.strip() for t in args.tables.split(",")] if args.tables else None
    introspect(
        url=args.url,
        source=args.source,
        schema_filter=schema_filter,
        table_filter=table_filter,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
