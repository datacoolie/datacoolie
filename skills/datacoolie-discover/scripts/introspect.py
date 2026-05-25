"""Introspect data sources and extract DDL-level schema information.

Thin CLI dispatcher — delegates to:
    _db_introspect.py   — Database via SQLAlchemy
    _file_introspect.py — File directory scan
    _api_introspect.py  — OpenAPI spec parse

Usage:
    python introspect.py --source-type db --connection "postgresql://user@host/db"
    python introspect.py --source-type file --path ./data/input/
    python introspect.py --source-type api --spec-url https://api.example.com/openapi.json
    python introspect.py --source-type lakehouse --catalog glue --database my_db
    python introspect.py --source-type file --path ./data/ --format csv --output-dir .datacoolie/discover/

Options:
    --source-type   db, file, api, or lakehouse (required)
    --connection    Connection string (db mode, or env var name prefixed with $)
    --path          Directory path (file mode)
    --spec-url      OpenAPI spec URL or local file (api mode)
    --catalog       Catalog type: glue, unity, fabric (lakehouse mode)
    --database      Database/schema name (lakehouse mode)
    --schema        Schema filter for db mode (default: all non-system schemas)
    --output        Output file path (default: stdout)
    --output-dir    Write structured CSV asset files to directory
    --format        Output format: text, json, csv (default: text)

Exit codes:
    0 = success
    1 = connection/introspection error
    2 = input error

Security:
    - Read-only operations only
    - Connection strings from env vars (prefix with $) or direct input
    - Never logs credentials
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from _types import SourceResult


# ---------------------------------------------------------------------------
# Output formatting (legacy dict-based, for backwards compat)
# ---------------------------------------------------------------------------


def _result_to_legacy_dict(result: SourceResult) -> dict:
    """Convert SourceResult to the legacy dict format for text/json output."""
    data: dict = {"source_type": result.source_type}

    if result.source_type == "api":
        data["spec_url"] = result.spec_url
        data["endpoints"] = [
            {
                "path": ep.path,
                "method": ep.method,
                "summary": ep.summary,
                "parameters": [
                    {"name": p.name, "in": p.location, "required": p.required}
                    for p in ep.parameters
                ],
                "response_schema": ep.response_schema,
            }
            for ep in result.endpoints
        ]
    else:
        if result.path:
            data["path"] = result.path
        if result.catalog:
            data["catalog"] = result.catalog
        if result.database:
            data["database"] = result.database
        data["tables"] = [
            {
                "schema": tbl.schema,
                "table_name": tbl.table_name,
                "columns": [
                    {
                        "name": c.name,
                        "data_type": c.data_type,
                        "nullable": c.nullable,
                        "ordinal": c.ordinal,
                    }
                    for c in tbl.columns
                ],
                "primary_keys": tbl.primary_key,
                "row_estimate": tbl.row_estimate,
                **({"file_count": tbl.file_count} if tbl.file_count else {}),
                **({"format": tbl.file_format} if tbl.file_format else {}),
                **({"representative_file": tbl.representative_file} if tbl.representative_file else {}),
            }
            for tbl in result.tables
        ]

    if result.note:
        data["note"] = result.note

    return data


def format_text(data: dict) -> str:
    """Format introspection results as readable text."""
    lines = []
    source_type = data.get("source_type", "unknown")
    lines.append(f"# Discovery Results — Source Type: {source_type}")
    lines.append("")

    if source_type in ("db", "file", "lakehouse"):
        tables = data.get("tables", [])
        lines.append(f"**Tables found:** {len(tables)}")
        lines.append("")

        for tbl in tables:
            schema_prefix = f"{tbl['schema']}." if tbl.get("schema") else ""
            name = tbl.get("table_name", "unknown")
            lines.append(f"## {schema_prefix}{name}")

            if tbl.get("row_estimate") is not None:
                lines.append(f"  Row estimate: ~{tbl['row_estimate']:,}")
            if tbl.get("file_count"):
                lines.append(f"  Files: {tbl['file_count']} ({tbl.get('format', '')})")
            if tbl.get("primary_keys"):
                lines.append(f"  PK: {', '.join(tbl['primary_keys'])}")

            lines.append("")
            lines.append("  | # | Column | Type | Nullable |")
            lines.append("  |---|--------|------|----------|")
            for col in tbl.get("columns", []):
                if "error" in col:
                    lines.append(f"  | - | ERROR | {col['error']} | - |")
                else:
                    nullable = "✓" if col.get("nullable", True) else "✗"
                    lines.append(f"  | {col.get('ordinal', '-')} | {col['name']} | {col['data_type']} | {nullable} |")
            lines.append("")

    elif source_type == "api":
        endpoints = data.get("endpoints", [])
        lines.append(f"**Endpoints found:** {len(endpoints)}")
        lines.append("")
        for ep in endpoints:
            lines.append(f"## {ep['method']} {ep['path']}")
            if ep.get("summary"):
                lines.append(f"  {ep['summary']}")
            if ep.get("parameters"):
                params = ", ".join(f"{p['name']} ({p['in']})" for p in ep["parameters"])
                lines.append(f"  Params: {params}")
            if ep.get("response_schema"):
                lines.append(f"  Response: {json.dumps(ep['response_schema'], indent=4)}")
            lines.append("")

    if data.get("note"):
        lines.append(f"> {data['note']}")

    return "\n".join(lines)


def format_json(data: dict) -> str:
    """Format as JSON."""
    return json.dumps(data, indent=2, default=str)


# ---------------------------------------------------------------------------
# Lakehouse introspection
# ---------------------------------------------------------------------------


def _dispatch_lakehouse(args) -> SourceResult:
    """Dispatch lakehouse introspection to _lakehouse_introspect module."""
    from _lakehouse_introspect import introspect_lakehouse
    return introspect_lakehouse(
        catalog=args.catalog,
        database=args.database,
        region=args.region,
        warehouse_id=getattr(args, "warehouse_id", None),
        workspace_id=getattr(args, "workspace_id", None),
        workspace_name=getattr(args, "workspace_name", None),
        lakehouse_name=getattr(args, "lakehouse_name", None),
        schema_filter=args.schema,
        catalog_uri=getattr(args, "catalog_uri", None),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def resolve_connection(raw: str) -> str:
    """Resolve connection string — supports $ENV_VAR syntax."""
    if raw.startswith("$"):
        env_name = raw[1:]
        value = os.environ.get(env_name)
        if not value:
            print(f"ERROR: Environment variable '{env_name}' not set.", file=sys.stderr)
            sys.exit(2)
        return value
    return raw


def main():
    parser = argparse.ArgumentParser(
        description="Discover data source schema for DataCoolie ETL projects.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source-type", required=True, choices=["db", "file", "api", "lakehouse"],
                        help="Type of data source to introspect")
    parser.add_argument("--connection", help="Connection string (db mode). Prefix with $ for env var.")
    parser.add_argument("--path", help="Directory path (file mode). Supports local and cloud paths (s3://, abfs://, gs://)")
    parser.add_argument("--storage-options", help="JSON dict of fsspec storage options, e.g. '{\"key\": \"...\", \"secret\": \"...\"}' (file mode)")
    parser.add_argument("--max-files", type=int, default=1000,
                        help="Max files to scan in file mode (default: 1000)")
    parser.add_argument("--spec-url", help="OpenAPI spec URL or file path (api mode)")
    parser.add_argument("--catalog", help="Catalog type: glue, unity, fabric (lakehouse mode)")
    parser.add_argument("--database", help="Database name (db/lakehouse mode)")
    parser.add_argument("--schema", help="Schema filter (db mode)")
    parser.add_argument("--output", help="Output file path (default: stdout)")
    parser.add_argument("--output-dir", help="Write CSV catalog files to this directory")
    parser.add_argument("--format", choices=["text", "json", "csv"], default="text",
                        help="Output format (default: text)")

    # Auth args (db mode — alternative to --connection)
    auth_group = parser.add_argument_group("auth", "Token-based database auth (alternative to --connection)")
    auth_group.add_argument("--auth", choices=["rds_iam", "azure_ad", "oauth2"],
                            help="Token auth mode for database connections")
    auth_group.add_argument("--dialect", choices=["postgresql", "mysql", "mssql", "oracle"],
                            help="Database dialect (used with --auth)")
    auth_group.add_argument("--host", help="Database host (used with --auth)")
    auth_group.add_argument("--port", type=int, help="Database port (used with --auth)")
    auth_group.add_argument("--user", help="Database user (used with --auth). Prefix with $ for env var.")
    auth_group.add_argument("--password", help="Database password. Prefix with $ for env var.")
    auth_group.add_argument("--region", help="AWS region (for rds_iam auth)")
    auth_group.add_argument("--token-url", help="OAuth2 token endpoint URL")
    auth_group.add_argument("--client-id", help="OAuth2 / Azure AD client ID. Prefix with $ for env var.")
    auth_group.add_argument("--client-secret", help="OAuth2 client secret. Prefix with $ for env var.")
    auth_group.add_argument("--driver", help="ODBC driver name (mssql dialect, default: ODBC Driver 18 for SQL Server)")

    # API args
    api_group = parser.add_argument_group("api", "API introspection options")
    api_group.add_argument("--api-type", choices=["openapi", "graphql", "odata"], default="openapi",
                           help="API type (default: openapi)")
    api_group.add_argument("--api-auth", choices=["api_key", "bearer", "basic"],
                           help="API auth mode")
    api_group.add_argument("--api-key", help="API key value. Prefix with $ for env var.")
    api_group.add_argument("--api-key-header", default="X-API-Key",
                           help="Header name for API key (default: X-API-Key)")
    api_group.add_argument("--api-token", help="Bearer/basic token. Prefix with $ for env var.")
    api_group.add_argument("--sample-call", action="store_true",
                           help="Make sample GET calls to infer response schema from live data")

    # Lakehouse args
    lh_group = parser.add_argument_group("lakehouse", "Lakehouse catalog options")
    lh_group.add_argument("--catalog-uri", help="Catalog URI (Iceberg REST endpoint, Hive thrift://host:port)")
    lh_group.add_argument("--workspace-id", help="Fabric workspace GUID (for fabric catalog)")
    lh_group.add_argument("--workspace-name", help="Fabric workspace display name (resolved to ID if --workspace-id not set)")
    lh_group.add_argument("--lakehouse-name", help="Fabric lakehouse display name (resolved to ID if --database is not a GUID)")
    lh_group.add_argument("--warehouse-id", help="Databricks warehouse ID (for Unity Catalog)")

    args = parser.parse_args()

    # Dispatch by source type
    result: SourceResult

    if args.source_type == "db":
        if args.auth:
            # Token-based auth — build connection from individual args
            if not args.dialect or not args.host:
                print("ERROR: --dialect and --host required when using --auth.", file=sys.stderr)
                sys.exit(2)
            from _auth import build_connection_string, generate_azure_ad_token, register_mssql_token_hook
            conn_str = build_connection_string(
                dialect=args.dialect,
                host=args.host,
                port=args.port,
                database=args.database,
                user=args.user,
                password=args.password,
                auth=args.auth,
                region=args.region,
                token_url=args.token_url,
                client_id=args.client_id,
                client_secret=args.client_secret,
                driver=args.driver,
            )
            from _db_introspect import introspect_db
            # For MSSQL + azure_ad, we need the token hook on the engine
            if args.dialect == "mssql" and args.auth == "azure_ad":
                from sqlalchemy import create_engine
                token = generate_azure_ad_token()
                engine = create_engine(conn_str, echo=False)
                register_mssql_token_hook(engine, token)
                result = introspect_db(conn_str, schema_filter=args.schema, engine=engine)
            else:
                result = introspect_db(conn_str, schema_filter=args.schema)
        elif args.connection:
            conn_str = resolve_connection(args.connection)
            from _db_introspect import introspect_db
            result = introspect_db(conn_str, schema_filter=args.schema)
        else:
            print("ERROR: --connection or --auth required for db source type.", file=sys.stderr)
            sys.exit(2)

    elif args.source_type == "file":
        if not args.path:
            print("ERROR: --path required for file source type.", file=sys.stderr)
            sys.exit(2)
        storage_opts: dict | None = None
        if args.storage_options:
            try:
                storage_opts = json.loads(args.storage_options)
            except json.JSONDecodeError as exc:
                print(f"ERROR: --storage-options is not valid JSON: {exc}", file=sys.stderr)
                sys.exit(2)
        from _file_introspect import introspect_files
        result = introspect_files(args.path, storage_options=storage_opts, max_files=args.max_files)

    elif args.source_type == "api":
        if not args.spec_url:
            print("ERROR: --spec-url required for api source type.", file=sys.stderr)
            sys.exit(2)
        from _api_introspect import introspect_api
        result = introspect_api(
            args.spec_url,
            api_type=getattr(args, "api_type", "openapi") or "openapi",
            auth_mode=getattr(args, "api_auth", None),
            api_key=getattr(args, "api_key", None),
            api_key_header=getattr(args, "api_key_header", "X-API-Key") or "X-API-Key",
            token=getattr(args, "api_token", None),
            sample_call=getattr(args, "sample_call", False),
        )

    elif args.source_type == "lakehouse":
        if not args.catalog or not args.database:
            print("ERROR: --catalog and --database required for lakehouse source type.", file=sys.stderr)
            sys.exit(2)
        result = _dispatch_lakehouse(args)

    # Write CSV catalog if --output-dir specified
    if args.output_dir:
        from _assets import write_assets
        write_assets(result, Path(args.output_dir))

    # Format output for stdout/file
    if args.format == "csv":
        from _assets import format_csv
        output = format_csv(result)
    elif args.format == "json":
        data = _result_to_legacy_dict(result)
        output = format_json(data)
    else:
        data = _result_to_legacy_dict(result)
        output = format_text(data)

    # Write output
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
