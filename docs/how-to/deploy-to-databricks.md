---
title: Deploy to Databricks — DataCoolie How-to
description: Run DataCoolie on Databricks with notebook utilities, workspace storage, and Delta Lake or Unity Catalog patterns.
---

# Deploy to Databricks

**Prerequisites** · Databricks workspace with Unity Catalog · cluster or SQL Warehouse · `pip install datacoolie` on the cluster.
**End state** · DataCoolie pipeline running as a Databricks job with `DatabricksPlatform`, DBFS / UC Volumes I/O, and Databricks secrets.

## 1. Cluster library

Install `datacoolie` on the cluster via the Libraries tab, or `%pip install`
it in a notebook.

Add optional dependencies individually only when needed. Common examples:

- `sqlalchemy` for database metadata.
- `httpx` for API metadata.
- `openpyxl` for Excel metadata files.
- `pyiceberg` if your pipeline uses PyIceberg-based operations.

Databricks already provides the Spark runtime, so a large platform bundle is
often unnecessary.

## 2. Notebook / job code

```python
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.platforms.databricks_platform import DatabricksPlatform
from datacoolie.metadata.database_provider import DatabaseProvider
from datacoolie.orchestration.driver import DataCoolieDriver

engine = SparkEngine(spark, platform=DatabricksPlatform())
metadata = DatabaseProvider(connection_string="jdbc:postgresql://…", workspace_id="your-workspace-id")

with DataCoolieDriver(engine=engine, metadata_provider=metadata,
                     base_log_path="/Volumes/main/logs/datacoolie") as driver:
    driver.run(stage="ingest2bronze")
```

## 3. Paths

- Prefer **UC Volumes**: `/Volumes/<catalog>/<schema>/<volume>/...`
- DBFS still works: `dbfs:/Users/…` or `/dbfs/…`
- `DatabricksPlatform` normalises both.

## 4. Qualified table names

Unity Catalog is three-level (`catalog.schema.table`). In DataCoolie metadata,
that maps to `catalog + database + table`, so prefer leaving `schema_name`
empty for Databricks connections. The checked-in usecase-sim metadata follows
that pattern:

```json
{
  "connection_type": "lakehouse",
  "format": "delta",
  "catalog": "workspace",
  "database": "default",
  "configure": {
    "base_path": "/Volumes/workspace/default/datacoolie_sim/delta"
  }
}
```

With a destination table like `orders_appended`, DataCoolie resolves the target
as `workspace.default.orders_appended`. If you set `schema_name`, the generic
qualified-name builder will include it and produce a four-part name, which is
usually not what you want for Unity Catalog.

## 5. Secrets

`DatabricksPlatform._fetch_secret` always uses `dbutils.secrets.get(scope,
key)`; there is no separate native secret backend on Databricks. Put the
secret key name in `configure`, then map the Databricks scope in
`secrets_ref`:

```json
{
  "configure": {
    "password": "sample-db-password"
  },
  "secrets_ref": {
    "datacoolie-scope": ["password"]
  }
}
```

The checked-in `sample_databricks_secrets.ipynb` notebook validates both direct
provider access and `resolve_secrets(...)` resolution without printing raw
secret values.

## 6. Workflow Job Setup

The checked-in usecase-sim assets are notebook-based, so this repo verifies the
Workflow / notebook-task path rather than a raw Jobs API payload. In practice,
wrap the notebook as a Databricks Workflow job with:

- Spark version ≥ 13.3 LTS
- Python 3.11 (Databricks DBR 14+)
- `datacoolie` as a cluster library, plus only the extra Python packages your
  metadata source or table format actually needs

If you later provision jobs through the Databricks Jobs API, mirror the same
notebook path, runtime, and library list used by the Workflow job. There is no
repo-specific Jobs API JSON example checked in today.

## Reference workspace

Use the Databricks platform guide in usecase-sim for the current sample
notebooks, metadata file, and setup notes:

- [`README.md`](https://github.com/datacoolie/datacoolie/blob/main/datacoolie/usecase-sim/platforms/databricks/README.md)
