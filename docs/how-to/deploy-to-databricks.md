---
title: Deploy Python ETL to Databricks | DataCoolie
description: Run DataCoolie ETL on Databricks with managed Spark, Unity Catalog, Delta Lake, UC Volumes, and a clear Spark-versus-Polars deployment choice.
---

# Deploy to Databricks

**Prerequisites** · Databricks workspace with Unity Catalog · serverless jobs,
classic jobs, or all-purpose compute that supports Python/Spark · `datacoolie`
installed in the job environment.
**End state** · DataCoolie pipeline running as a Databricks job with
`DatabricksPlatform`, UC Volumes I/O, and Databricks secrets.

## 1. Choose the engine

Use `SparkEngine` as the default for production Databricks pipelines.
[Databricks configures and manages the Spark context and session](https://docs.databricks.com/aws/en/spark/faq),
and its Delta Lake, Unity Catalog, monitoring, and runtime optimizations are
designed around that execution path. This removes much of the setup cost that
Spark has in a local benchmark.

Use `PolarsEngine` only for a bounded file-oriented job when all of these are
true:

- The working set fits comfortably on one node.
- A representative test shows a material runtime or cost benefit.
- The job does not need Spark-native Delta or Unity Catalog table behavior.
- The team accepts an additional Python dependency and operational path.

The checked-in Polars notebook supports file operations through UC Volume
paths. For Delta tables addressed through Unity Catalog or UC Volumes, use the
Spark sample. See [Polars vs Spark by platform and ETL layer](../blog/posts/2026-05-26-polars-vs-spark-for-etl.md)
for the cross-platform decision matrix.

## 2. Cluster library

Install `datacoolie` on the cluster via the Libraries tab, or `%pip install`
it in a notebook.

Add optional dependencies individually only when needed. Common examples:

- `sqlalchemy` for database metadata.
- `httpx` for API metadata.
- `openpyxl` for Excel metadata files.
- `pyiceberg` if your pipeline uses PyIceberg-based operations.

Databricks already provides the Spark runtime, so a large platform bundle is
often unnecessary.

## 3. Notebook / job code

```python
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.platforms.databricks_platform import DatabricksPlatform
from datacoolie.metadata.database_provider import DatabaseProvider
from datacoolie.orchestration.driver import DataCoolieDriver

engine = SparkEngine(spark_session=spark, platform=DatabricksPlatform())
metadata = DatabaseProvider(
    connection_string="postgresql+psycopg2://user:password@host:5432/metadata",
    workspace_id="your-workspace-id",
)

with DataCoolieDriver(engine=engine, metadata_provider=metadata,
                     base_log_path="/Volumes/main/logs/datacoolie") as driver:
    driver.run(stage="ingest2bronze")
```

## 4. Paths

- Prefer **UC Volumes**: `/Volumes/<catalog>/<schema>/<volume>/...`
- Use external locations or workspace files when a volume is not the right
  boundary.
- Do not start a new pipeline on DBFS root or mounts. Databricks has deprecated
  both and recommends UC Volumes, external locations, or workspace files; see
  [DBFS and Unity Catalog best practices](https://docs.databricks.com/aws/en/dbfs/unity-catalog).

## 5. Qualified table names

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

## 6. Secrets

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

## 7. Workflow Job Setup

The checked-in usecase-sim assets are notebook-based, so this repo verifies the
Workflow / notebook-task path rather than a raw Jobs API payload. In practice,
wrap the notebook as a Databricks Workflow job with:

- a runtime whose Python version satisfies DataCoolie's `>=3.11,<4.0`
- Spark/Delta versions compatible with the job's chosen table format
- `datacoolie` as a cluster library, plus only the extra Python packages your
  metadata source or table format actually needs

If you later provision jobs through the Databricks Jobs API, mirror the same
notebook path, runtime, and library list used by the Workflow job. There is no
repo-specific Jobs API JSON example checked in today.

## Reference workspace

Use the Databricks platform guide in usecase-sim for the current sample
notebooks, metadata file, and setup notes:

- [`README.md`](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/platforms/databricks/README.md)
