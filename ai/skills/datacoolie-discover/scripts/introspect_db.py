"""introspect_db.py — Database schema introspection via SQLAlchemy.

Connects to any SQLAlchemy-supported database, extracts table/column metadata,
and outputs a standardised CSV matching the datacoolie discover schema contract.

Usage:
    python introspect_db.py --url-env DATACOOLIE_DISCOVERY_URL --source erp
    python introspect_db.py --odbc-connstr-env DATACOOLIE_ODBC_CONNSTR --source fabric_sql
    python introspect_db.py --url-env LOCAL_SQLITE_URL --source app --output observations.csv
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote_plus, urlencode, urlsplit, urlunsplit

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from _observation_contract import (
    CSV_HEADER as CSV_HEADER,  # noqa: F401 - public cross-probe contract
    atomic_write_observations,
    make_observation,
    utc_observed_at,
    write_observations,
)
from _probe_status import PARTIAL_EXIT_CODE, write_probe_status
from sqlalchemy.types import (
    BigInteger, Boolean, Date, DateTime, Float, Integer, LargeBinary,
    Numeric, SmallInteger, String, Text, Time,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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
_CONNSTR_SECRET_RE = re.compile(
    r"(?i)((?:^|;)\s*(?:PWD|Password|ClientSecret|Client Secret|Secret|AccessToken|Token)\s*=\s*)[^;]*"
)
_SENSITIVE_QUERY_KEYS = {
    "password",
    "pwd",
    "client_secret",
    "clientsecret",
    "access_token",
    "token",
}


def _mask_url(url: str) -> str:
    """Replace secrets in a connection URL or encoded ODBC string with '***'."""
    masked = _URL_PASSWORD_RE.sub(r"://\1:***@", url)
    try:
        parts = urlsplit(masked)
        if parts.query:
            pairs = parse_qsl(parts.query, keep_blank_values=True)
            changed = False
            masked_pairs: list[tuple[str, str]] = []
            for key, value in pairs:
                key_lower = key.lower()
                if key_lower == "odbc_connect":
                    value = _mask_connection_string(value)
                    changed = True
                elif key_lower in _SENSITIVE_QUERY_KEYS:
                    value = "***"
                    changed = True
                masked_pairs.append((key, value))
            if changed:
                masked = urlunsplit(
                    (
                        parts.scheme,
                        parts.netloc,
                        parts.path,
                        urlencode(masked_pairs, safe="*"),
                        parts.fragment,
                    )
                )
    except Exception:
        pass
    return _mask_connection_string(masked)


def _mask_connection_string(connstr: str) -> str:
    """Mask common secret fields in ODBC-style connection strings."""
    return _CONNSTR_SECRET_RE.sub(lambda m: f"{m.group(1)}***", connstr)


def _build_odbc_url(connstr: str, dialect: str = "mssql+pyodbc") -> str:
    """Build a SQLAlchemy URL from a raw ODBC connection string."""
    if not dialect:
        raise ValueError("--dialect cannot be empty when using ODBC connection strings")
    if not connstr.strip():
        raise ValueError("ODBC connection string cannot be empty")
    return f"{dialect}:///?odbc_connect={quote_plus(connstr)}"


def _read_env_value(name: str, label: str) -> str:
    if not name:
        raise ValueError(f"{label} environment variable name cannot be empty")
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"Environment variable {name!r} is not set or is empty")
    return value


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
        allow_abbrev=False,
        description="Introspect a database and output a standardised schema CSV.",
    )
    connection = parser.add_argument_group("connection")
    connection.add_argument(
        "--url-env",
        default=None,
        help="Environment variable containing the SQLAlchemy connection URL",
    )
    connection.add_argument(
        "--odbc-connstr-env",
        default=None,
        help="Environment variable containing a raw ODBC connection string",
    )
    connection.add_argument(
        "--dialect",
        default="mssql+pyodbc",
        help="SQLAlchemy dialect used with ODBC connection strings (default: mssql+pyodbc)",
    )
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
        "--max-objects", type=int, default=1000,
        help="Maximum tables and views to inspect before returning partial status",
    )
    parser.add_argument(
        "--output", default=None,
        help="Output CSV file path (default: stdout)",
    )
    parser.add_argument("--status-output", default=None, help="Optional probe status JSON path")
    return parser.parse_args(argv)


def resolve_connection_url(args: argparse.Namespace) -> str:
    """Resolve connection input into a SQLAlchemy URL."""
    connection_inputs = [args.url_env, args.odbc_connstr_env]
    provided = [value for value in connection_inputs if value]
    if len(provided) != 1:
        raise ValueError(
            "Provide exactly one environment-backed connection source: "
            "--url-env or --odbc-connstr-env"
        )

    if args.url_env:
        return _read_env_value(args.url_env, "--url-env")
    return _build_odbc_url(
        _read_env_value(args.odbc_connstr_env, "--odbc-connstr-env"),
        args.dialect,
    )


def introspect(
    url: str,
    source: str,
    schema_filter: list[str] | None = None,
    table_filter: list[str] | None = None,
    output_path: str | None = None,
    max_objects: int = 1000,
) -> tuple[int, list[str]]:
    """Run introspection and write canonical observations."""
    try:
        engine = create_engine(url)
    except Exception as exc:
        print(
            f"ERROR: Cannot create engine for {_mask_url(url)} ({type(exc).__name__})",
            file=sys.stderr,
        )
        sys.exit(1)

    dialect = _resolve_dialect(engine)
    system_schemas = _get_system_schemas(dialect)
    rows: list[dict[str, str]] = []
    issues: list[str] = []
    inspected_objects = 0
    limit_reached = False
    observed_at = utc_observed_at()

    try:
        insp = inspect(engine)
    except Exception as exc:
        print(
            f"ERROR: Cannot connect to {_mask_url(url)} ({type(exc).__name__})",
            file=sys.stderr,
        )
        sys.exit(1)

    # Resolve schemas
    if schema_filter:
        schemas = schema_filter
    else:
        try:
            all_schemas = insp.get_schema_names()
        except Exception as exc:
            all_schemas = [None]  # type: ignore[list-item]
            issue = f"Cannot list schemas ({type(exc).__name__}); using default schema"
            issues.append(issue)
            print(f"WARNING: {issue}", file=sys.stderr)
        schemas = [s for s in all_schemas if s not in system_schemas]
        if not schemas:
            schemas = [None]  # type: ignore[list-item]

    try:
        with engine.connect() as conn:
            for schema_index, schema_name in enumerate(schemas):
                try:
                    all_tables = insp.get_table_names(schema=schema_name)
                except Exception as exc:
                    issue = (
                        f"Cannot list tables in schema '{schema_name}' "
                        f"({type(exc).__name__})"
                    )
                    issues.append(issue)
                    print(f"WARNING: {issue}", file=sys.stderr)
                    continue

                try:
                    all_views = insp.get_view_names(schema=schema_name)
                except Exception as exc:
                    all_views = []
                    issue = (
                        f"Cannot list views in schema '{schema_name}' "
                        f"({type(exc).__name__})"
                    )
                    issues.append(issue)
                    print(f"WARNING: {issue}", file=sys.stderr)

                objects = [(name, "table") for name in all_tables]
                objects.extend((name, "view") for name in all_views)
                if table_filter:
                    table_set = set(table_filter)
                    objects = [item for item in objects if item[0] in table_set]
                objects.sort(key=lambda item: (item[1], item[0]))

                remaining = max_objects - inspected_objects
                if len(objects) > remaining:
                    objects = objects[:remaining]
                    limit_reached = True

                for table_name, object_type in objects:
                    inspected_objects += 1
                    try:
                        rows.extend(_introspect_table(
                            conn,
                            insp,
                            dialect,
                            source,
                            schema_name,
                            table_name,
                            observed_at,
                            object_type=object_type,
                        ))
                    except Exception as exc:
                        issue = (
                            f"Skipping {schema_name}.{table_name} "
                            f"({type(exc).__name__})"
                        )
                        issues.append(issue)
                        print(f"WARNING: {issue}", file=sys.stderr)
                has_uninspected_schemas = schema_index < len(schemas) - 1
                if limit_reached or (inspected_objects >= max_objects and has_uninspected_schemas):
                    issue = f"Stopped after --max-objects={max_objects}; source scope is partial"
                    issues.append(issue)
                    print(f"WARNING: {issue}", file=sys.stderr)
                    break
    except Exception as exc:
        print(
            f"ERROR: Database catalog probe failed for {_mask_url(url)} "
            f"({type(exc).__name__})",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    finally:
        engine.dispose()
    if output_path:
        atomic_write_observations(Path(output_path), rows)
    else:
        write_observations(sys.stdout, rows)
    return len(rows), issues


def _introspect_table(
    conn,
    insp,
    dialect: str,
    source: str,
    schema_name: str | None,
    table_name: str,
    observed_at: str,
    object_type: str = "table",
) -> list[dict[str, str]]:
    """Introspect a single table or view into canonical observations."""
    columns = insp.get_columns(table_name, schema=schema_name)

    pk_info = (
        insp.get_pk_constraint(table_name, schema=schema_name)
        if object_type == "table" else {}
    )
    pk_cols = set(pk_info.get("constrained_columns", []) if pk_info else [])

    fk_list = (
        insp.get_foreign_keys(table_name, schema=schema_name)
        if object_type == "table" else []
    )
    fk_map = _build_fk_map(fk_list)

    unique_map: dict[str, str] = {}
    if object_type == "table":
        try:
            for constraint in insp.get_unique_constraints(table_name, schema=schema_name):
                label = f"unique:{constraint.get('name') or 'unnamed'}"
                for column_name in constraint.get("column_names", []):
                    unique_map[column_name] = label
        except Exception:
            pass

    row_est = (
        _get_row_estimate(conn, dialect, schema_name or "", table_name)
        if object_type == "table" else ""
    )

    schema_str = schema_name or ""

    rows = []
    for ordinal, col in enumerate(columns, start=1):
        canonical, fmt, prec, scl = map_type(col["type"])
        is_pk = "true" if col["name"] in pk_cols else ""
        nullable = "true" if col.get("nullable", True) else "false"

        rows.append(make_observation(
            source=source,
            object_type=object_type,
            schema=schema_str,
            object=table_name,
            column=col["name"],
            native_type=str(col["type"]),
            data_type=canonical,
            format=fmt,
            precision=prec,
            scale=scl,
            nullable=nullable,
            ordinal=ordinal,
            declared_key="primary" if is_pk else unique_map.get(col["name"], ""),
            declared_reference=fk_map.get(col["name"], ""),
            row_estimate=row_est,
            observed_at=observed_at,
            method=f"{dialect}:catalog",
            evidence_class="observed",
        ))
    return rows


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.max_objects < 1 or args.max_objects > 100000:
        raise SystemExit("ERROR: --max-objects must be between 1 and 100000")
    try:
        url = resolve_connection_url(args)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    schema_filter = [s.strip() for s in args.schemas.split(",")] if args.schemas else None
    table_filter = [t.strip() for t in args.tables.split(",")] if args.tables else None
    row_count, issues = introspect(
        url=url,
        source=args.source,
        schema_filter=schema_filter,
        table_filter=table_filter,
        output_path=args.output,
        max_objects=args.max_objects,
    )
    status = write_probe_status(
        Path(args.status_output) if args.status_output else None,
        source=args.source,
        probe="database-catalog",
        row_count=row_count,
        issues=issues,
    )
    if status == "partial":
        raise SystemExit(PARTIAL_EXIT_CODE)


if __name__ == "__main__":
    main()
