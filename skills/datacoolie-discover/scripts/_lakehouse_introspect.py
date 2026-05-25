"""Lakehouse / data catalog introspection — extract table metadata from catalogs.

Supports: AWS Glue, Databricks Unity Catalog, Microsoft Fabric (OneLake REST),
          Apache Iceberg (pyiceberg), Delta Lake (deltalake), Hive Metastore.
"""

from __future__ import annotations

import sys

from _types import Column, SourceResult, Table


def introspect_lakehouse(catalog: str, database: str,
                         region: str | None = None,
                         warehouse_id: str | None = None,
                         workspace_id: str | None = None,
                         workspace_name: str | None = None,
                         lakehouse_name: str | None = None,
                         schema_filter: str | None = None,
                         catalog_uri: str | None = None) -> SourceResult:
    """Dispatch to the appropriate catalog backend."""
    dispatchers = {
        "glue": _introspect_glue,
        "unity": _introspect_unity,
        "fabric": _introspect_fabric,
        "iceberg": _introspect_iceberg,
        "delta": _introspect_delta,
        "hive": _introspect_hive,
    }

    handler = dispatchers.get(catalog)
    if not handler:
        print(f"ERROR: Unknown catalog type '{catalog}'. Supported: {', '.join(dispatchers.keys())}", file=sys.stderr)
        sys.exit(1)

    return handler(database=database, region=region, warehouse_id=warehouse_id,
                   workspace_id=workspace_id, workspace_name=workspace_name,
                   lakehouse_name=lakehouse_name, schema_filter=schema_filter,
                   catalog_uri=catalog_uri)


# ---------------------------------------------------------------------------
# AWS Glue Data Catalog
# ---------------------------------------------------------------------------

def _introspect_glue(database: str, region: str | None = None, **_kwargs) -> SourceResult:
    """Introspect AWS Glue Data Catalog tables."""
    try:
        import boto3
    except ImportError:
        print("ERROR: boto3 required for Glue catalog — pip install boto3", file=sys.stderr)
        sys.exit(1)

    session = boto3.Session(region_name=region)
    client = session.client("glue")

    result = SourceResult(
        source_type="lakehouse",
        catalog="glue",
        database=database,
    )

    # Paginate through tables
    paginator = client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for tbl in page.get("TableList", []):
            columns = []
            storage_desc = tbl.get("StorageDescriptor", {})
            for i, col in enumerate(storage_desc.get("Columns", []), start=1):
                columns.append(Column(
                    name=col["Name"],
                    data_type=col.get("Type", "string"),
                    ordinal=i,
                    nullable=True,
                ))
            # Add partition keys as columns
            for pk in tbl.get("PartitionKeys", []):
                columns.append(Column(
                    name=pk["Name"],
                    data_type=pk.get("Type", "string"),
                    ordinal=len(columns) + 1,
                    nullable=True,
                ))

            table = Table(
                table_name=tbl["Name"],
                schema=database,
                columns=columns,
                location=storage_desc.get("Location"),
                file_format=storage_desc.get("InputFormat", "").rsplit(".", 1)[-1] if storage_desc.get("InputFormat") else None,
                partition_keys=[pk["Name"] for pk in tbl.get("PartitionKeys", [])],
                properties=tbl.get("Parameters", {}),
            )
            result.tables.append(table)

    result.total_columns = sum(len(t.columns) for t in result.tables)
    return result


# ---------------------------------------------------------------------------
# Databricks Unity Catalog
# ---------------------------------------------------------------------------

def _introspect_unity(database: str, schema_filter: str | None = None,
                      warehouse_id: str | None = None, **_kwargs) -> SourceResult:
    """Introspect Databricks Unity Catalog via databricks-sdk."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("ERROR: databricks-sdk required for Unity Catalog — pip install databricks-sdk", file=sys.stderr)
        sys.exit(1)

    w = WorkspaceClient()
    result = SourceResult(
        source_type="lakehouse",
        catalog="unity",
        database=database,
    )

    # List schemas in catalog
    schemas = list(w.schemas.list(catalog_name=database))
    if schema_filter:
        schemas = [s for s in schemas if s.name == schema_filter]

    for schema in schemas:
        tables_iter = w.tables.list(catalog_name=database, schema=schema.name)
        for tbl in tables_iter:
            columns = []
            if tbl.columns:
                for i, col in enumerate(tbl.columns, start=1):
                    columns.append(Column(
                        name=col.name,
                        data_type=col.type_text or "string",
                        ordinal=i,
                        nullable=col.nullable if col.nullable is not None else True,
                    ))

            table = Table(
                table_name=tbl.name,
                schema=schema.name,
                columns=columns,
                location=tbl.storage_location,
                file_format=tbl.data_source_format.value if tbl.data_source_format else None,
            )
            result.tables.append(table)

    result.total_columns = sum(len(t.columns) for t in result.tables)
    return result


# ---------------------------------------------------------------------------
# Microsoft Fabric (OneLake REST API)
# ---------------------------------------------------------------------------

def _introspect_fabric(database: str, workspace_id: str | None = None,
                       workspace_name: str | None = None, lakehouse_name: str | None = None,
                       **_kwargs) -> SourceResult:
    """Introspect Microsoft Fabric lakehouse tables via REST API.

    Accepts either GUIDs (workspace_id / database-as-lakehouse-id)
    or display names (workspace_name / lakehouse_name) — resolves IDs automatically.
    """
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError:
        print("ERROR: azure-identity required for Fabric — pip install azure-identity", file=sys.stderr)
        sys.exit(1)

    import json
    import urllib.request

    credential = DefaultAzureCredential()
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    auth_headers = {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}

    # Resolve workspace_id from display name if not provided
    if not workspace_id:
        workspace_id = _fabric_resolve_workspace(workspace_name or database, auth_headers)

    # Resolve lakehouse ID from display name if database looks like a name (not UUID)
    lakehouse_id = database
    if workspace_id and (lakehouse_name or not _is_uuid(database)):
        lakehouse_id = _fabric_resolve_lakehouse(workspace_id, lakehouse_name or database, auth_headers)

    result = SourceResult(
        source_type="lakehouse",
        catalog="fabric",
        database=lakehouse_name or database,
    )

    if not workspace_id or not lakehouse_id:
        print("WARN: Cannot resolve Fabric workspace/lakehouse IDs. "
              "Set FABRIC_WORKSPACE_ID + FABRIC_LAKEHOUSE_ID or check display names.",
              file=sys.stderr)
        return result

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
    req = urllib.request.Request(url, headers=auth_headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"ERROR: Fabric API call failed: {e}", file=sys.stderr)
        sys.exit(1)

    for tbl_data in data.get("data", []):
        table = Table(
            table_name=tbl_data.get("name", "unknown"),
            schema="dbo",
            columns=[],  # Fabric tables API doesn't return columns — use SQL endpoint
            location=tbl_data.get("location"),
            file_format=tbl_data.get("format", "delta"),
        )
        result.tables.append(table)

    result.total_columns = sum(len(t.columns) for t in result.tables)
    return result


def _is_uuid(s: str) -> bool:
    """Return True if s looks like a GUID/UUID."""
    import re
    return bool(re.match(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        s.strip().lower(),
    ))


def _fabric_api_get(url: str, headers: dict) -> dict:
    """GET a Fabric REST endpoint, return parsed JSON."""
    import json
    import urllib.request
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"WARN: Fabric API GET {url} failed: {e}", file=sys.stderr)
        return {}


def _fabric_resolve_workspace(name: str, headers: dict) -> str | None:
    """Resolve Fabric workspace display name to GUID."""
    data = _fabric_api_get("https://api.fabric.microsoft.com/v1/workspaces", headers)
    for ws in data.get("value", []):
        if ws.get("displayName") == name:
            return ws["id"]
    print(f"WARN: Fabric workspace '{name}' not found.", file=sys.stderr)
    return None


def _fabric_resolve_lakehouse(workspace_id: str, name: str, headers: dict) -> str | None:
    """Resolve Fabric lakehouse display name to GUID within a workspace."""
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    data = _fabric_api_get(url, headers)
    for lh in data.get("value", []):
        if lh.get("displayName") == name:
            return lh["id"]
    print(f"WARN: Fabric lakehouse '{name}' not found in workspace {workspace_id}.", file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# Apache Iceberg (pyiceberg)
# ---------------------------------------------------------------------------

def _introspect_iceberg(database: str, catalog_uri: str | None = None, **_kwargs) -> SourceResult:
    """Introspect Iceberg tables via pyiceberg REST catalog."""
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError:
        print("ERROR: pyiceberg required for Iceberg catalog — pip install pyiceberg", file=sys.stderr)
        sys.exit(1)

    result = SourceResult(
        source_type="lakehouse",
        catalog="iceberg",
        database=database,
    )

    # Load catalog (uses env vars or provided URI)
    catalog_config = {"type": "rest"}
    if catalog_uri:
        catalog_config["uri"] = catalog_uri
    catalog = load_catalog("default", **catalog_config)

    # List tables in namespace
    try:
        table_ids = catalog.list_tables(database)
    except Exception as e:
        print(f"ERROR: Cannot list Iceberg tables in '{database}': {e}", file=sys.stderr)
        sys.exit(1)

    for table_id in table_ids:
        tbl = catalog.load_table(table_id)
        columns = []
        for i, field in enumerate(tbl.schema().fields, start=1):
            columns.append(Column(
                name=field.name,
                data_type=str(field.field_type),
                ordinal=i,
                nullable=not field.required,
            ))

        table = Table(
            table_name=table_id[1] if len(table_id) > 1 else str(table_id),
            schema=database,
            columns=columns,
            location=tbl.metadata.location if hasattr(tbl, "metadata") else None,
            file_format="iceberg",
            properties=dict(tbl.properties) if hasattr(tbl, "properties") else {},
        )
        result.tables.append(table)

    result.total_columns = sum(len(t.columns) for t in result.tables)
    return result


# ---------------------------------------------------------------------------
# Delta Lake (deltalake)
# ---------------------------------------------------------------------------

def _introspect_delta(database: str, **_kwargs) -> SourceResult:
    """Introspect Delta Lake tables from a directory of delta tables.

    'database' is the root path containing delta table directories.
    """
    try:
        from deltalake import DeltaTable
    except ImportError:
        print("ERROR: deltalake required for Delta catalog — pip install deltalake", file=sys.stderr)
        sys.exit(1)

    from pathlib import Path

    result = SourceResult(
        source_type="lakehouse",
        catalog="delta",
        database=database,
    )

    root = Path(database)
    if not root.exists():
        print(f"ERROR: Delta root path does not exist: {database}", file=sys.stderr)
        sys.exit(1)

    # Find directories containing _delta_log/
    for delta_log in root.rglob("_delta_log"):
        table_dir = delta_log.parent
        try:
            dt = DeltaTable(str(table_dir))
            schema = dt.schema()
            columns = []
            for i, field in enumerate(schema.fields, start=1):
                columns.append(Column(
                    name=field.name,
                    data_type=str(field.type),
                    ordinal=i,
                    nullable=field.nullable,
                ))

            table = Table(
                table_name=table_dir.name,
                schema=str(table_dir.parent.relative_to(root)) if table_dir.parent != root else "",
                columns=columns,
                location=str(table_dir),
                file_format="delta",
            )
            result.tables.append(table)
        except Exception as e:
            print(f"WARN: Cannot read delta table at {table_dir}: {e}", file=sys.stderr)
            continue

    result.total_columns = sum(len(t.columns) for t in result.tables)
    return result


# ---------------------------------------------------------------------------
# Hive Metastore
# ---------------------------------------------------------------------------

def _introspect_hive(database: str, catalog_uri: str | None = None, **_kwargs) -> SourceResult:
    """Introspect Hive Metastore via Thrift (PyHive)."""
    try:
        from pyhive import hive
    except ImportError:
        print("ERROR: pyhive required for Hive metastore — pip install pyhive[hive]", file=sys.stderr)
        sys.exit(1)

    result = SourceResult(
        source_type="lakehouse",
        catalog="hive",
        database=database,
    )

    # Parse host:port from catalog_uri
    host = "localhost"
    port = 10000
    if catalog_uri:
        parts = catalog_uri.replace("thrift://", "").split(":")
        host = parts[0]
        if len(parts) > 1:
            port = int(parts[1])

    conn = hive.connect(host=host, port=port, database=database)
    cursor = conn.cursor()

    # List tables
    cursor.execute("SHOW TABLES")
    table_names = [row[0] for row in cursor.fetchall()]

    for tname in table_names:
        # Validate table name contains only safe characters (alphanumeric + underscore)
        if not all(c.isalnum() or c == '_' for c in tname):
            print(f"WARN: Skipping table with unsafe name: {tname}", file=sys.stderr)
            continue
        cursor.execute(f"DESCRIBE `{tname}`")
        columns = []
        for i, row in enumerate(cursor.fetchall(), start=1):
            if row[0] and not row[0].startswith("#"):  # skip partition info headers
                columns.append(Column(
                    name=row[0],
                    data_type=row[1] or "string",
                    ordinal=i,
                    nullable=True,
                ))

        table = Table(
            table_name=tname,
            schema=database,
            columns=columns,
            file_format="hive",
        )
        result.tables.append(table)

    cursor.close()
    conn.close()
    result.total_columns = sum(len(t.columns) for t in result.tables)
    return result
