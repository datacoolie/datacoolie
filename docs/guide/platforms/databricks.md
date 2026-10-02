---
title: Deploy Python ETL to Databricks | DataCoolie
description: Run a DataCoolie Spark smoke job with Volume-backed control files and a named Unity Catalog Delta table, with explicit serverless and external boundaries.
---

# Deploy to Databricks

Start with Spark + FileProvider and the [platform smoke project](../../examples/runners.md#platform-smoke).
It reads three CSV rows from a Volume and overwrites a **new sandbox** UC table.
These are locally checked setup contracts; no live Databricks run is claimed.

## 1. Select compute and dependencies

Use managed Spark for UC tables, distributed transforms and Delta operations.
Native Polars is an option for bounded file workloads that fit one node; it does
not provide Spark UC table semantics. See [engine selection](../../blog/posts/2026-05-26-polars-vs-spark-for-etl.md).

| Compute | Bootstrap |
|---|---|
| Classic jobs/all-purpose | Install matching `datacoolie` cluster library or notebook `%pip` |
| Serverless notebook/job | Configure its Environment dependencies and environment version |
| External Python 3.11+ | `datacoolie[databricks-external,polars-delta]`; SDK unified authentication |

For classic notebook installation:

```python
%pip install "datacoolie=={{ datacoolie_version }}"
```

Use a matching wheel for an unpublished checkout. Managed compute supplies
Spark/Delta; do not install an extra PySpark runtime. Python must satisfy
`>=3.11,<4.0`. Serverless environment 1 uses Python 3.10 and is unsuitable;
choose a compatible environment (for example environment 2 uses Python 3.11).
Pin and review the chosen [environment version](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/).
Serverless dependencies belong in the [Environment configuration](https://docs.databricks.com/aws/en/compute/serverless/dependencies),
not compute-scoped libraries. Serverless uses Spark Connect and has API/config
restrictions; Python compatibility alone does not qualify every SparkEngine
operation. This guide's local checks do not establish serverless operation parity.
See [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations).

## 2. Grant the execution identity access

Create a sandbox Volume and schema. Grant the job's Run as principal, rather
than only the interactive author, the permissions needed for this recipe:

| Operation | UC privileges |
|---|---|
| Read Volume metadata/input | `USE CATALOG`, `USE SCHEMA`, `READ VOLUME` |
| Persist Volume logs/watermarks | Above plus `WRITE VOLUME` |
| Create the sandbox managed table | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` on output schema |
| Write an existing table | `MODIFY` on table, plus namespace use |
| Verify table output | `SELECT` on table, plus namespace use |

Volume access does not grant table access. See [privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)
and [permission concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/permissions-concepts).
The fixture needs no secrets or external database.

## 3. Build, upload and parameterize

Follow the [shared build recipe](../../examples/runners.md#platform-smoke). Edit
`metadata/environments/databricks.json`: replace the input Volume root and the
output `catalog`/`database`. Leave `schema_name` unset; empty `base_path` clears
the local output root during overlay merge. Validate/inspect `--env databricks`.

Upload `.builds/current/databricks/metadata/` to
`/Volumes/main/default/datacoolie_example/metadata/`. Upload the CSV separately
to `/Volumes/main/default/datacoolie_example/data/input/orders/orders.csv`.
Replace these example namespaces consistently for your workspace.

Import canonical **databricks/run_spark.ipynb**
([source](../../examples/source/runners/databricks/run_spark.ipynb.md) ·
[raw](../../examples/files/runners/databricks/run_spark.ipynb)). It selects
`runtime="databricks"`, uses `spark` and FileProvider. Configure parameter cell
values interactively or the same widget names as Workflow notebook parameters:

| Widget/job parameter | Smoke value |
|---|---|
| `METADATA_PATH` | `/Volumes/main/default/datacoolie_example/metadata/metadata.json` |
| `CONNECTIONS_PATH`, `SCHEMA_HINTS_PATH` | Empty strings; included in built metadata |
| `WATERMARK_BASE_PATH` | `/Volumes/main/default/datacoolie_example/.runtime/watermarks` |
| `LOG_BASE_PATH` | `/Volumes/main/default/datacoolie_example/.runtime/logs` |
| `STAGE` | `platform_smoke` |
| `JOB_NUM`, `JOB_INDEX` | `1`, `0` |

Set the Workflow Run as identity, notebook task, compatible compute/environment
and dependencies explicitly. Apply shared smoke selection/result guards at the
runner's `driver.run` boundary for a downstream barrier; generic runners raise
on failed flows. There is no repo-owned Jobs API provisioning payload.

## 4. Distinguish table addressing from file addressing

These are alternative output connection configurations:

```json
{"connection_type": "lakehouse", "format": "delta", "catalog": "main", "database": "default", "configure": {"base_path": ""}}
```

With destination `orders_platform_smoke`, this resolves to
`main.default.orders_platform_smoke`. DataCoolie uses named table addressing
when catalog/database is set; a Volume `base_path` alongside it is not the
write target. `catalog + database + table` is UC's three-part name; setting
`schema_name` adds another component.

For a **path-only** Delta sandbox, omit catalog/database/schema_name entirely
(or clear inherited namespace fields in an overlay):

```json
{"connection_type": "lakehouse", "format": "delta", "configure": {"base_path": "/Volumes/main/default/datacoolie_example/data/output"}}
```

Its destination is `<base_path>/orders_platform_smoke`. You may store Delta
files in a Volume, but may not register a UC table on those Volume files; table
and Volume locations cannot overlap. See [Volume paths](https://docs.databricks.com/aws/en/volumes/paths).
`dbfs:/Volumes/...` is an accepted alias; canonical paths are `/Volumes/...`.
Raw `s3://`, `abfss://`, `gs://` are native-only platform paths. Workspace Files
are outside the platform's portable contract. Avoid new DBFS root/mount usage;
see [DBFS guidance](https://docs.databricks.com/aws/en/dbfs/unity-catalog).

## 5. Verify the named table

Require selection `orders_platform_smoke` and result counts `(1, 1, 0, 0)` for
`total, succeeded, failed, pending`. Independently read the chosen namespace:

```python
output = spark.table("main.default.orders_platform_smoke").select(
    "order_id", "customer_id", "amount"
).orderBy("order_id")
assert [tuple(row) for row in output.collect()] == [(1, 100, 20), (2, 100, 43), (3, 101, 7)]
assert output.dtypes == [("order_id", "bigint"), ("customer_id", "bigint"), ("amount", "bigint")]
```

Inspect Volume execution/system logs and the Workflow task failure status.
For the path-only branch use `spark.read.format("delta").load(...)` instead.

## Optional providers, secrets and Polars {#6-secrets}

Database metadata is a separate [DatabaseProvider setup](../providers/database.md),
including bootstrap/migrations, credentials, networking and the SQLAlchemy
backend driver (for PostgreSQL, `psycopg2-binary`). It is not needed for this run.
Secret resolution uses native `dbutils.secrets` or external SDK secrets:

```json
{"configure": {"password": "sample-db-password"}, "secrets_ref": {"datacoolie-scope": ["password"]}}
```

Grant the executing principal secret-scope read access separately. Keep actual
values out of notebooks/logs.

For native file-only Polars use [the native Polars scenario notebook](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/platforms/databricks/sample_databricks_polars.ipynb); install the
required engine/format profile and prepare file/path metadata rather than the
smoke project's named UC target.
For external SDK use **databricks/run_polars_sdk.py**
([source](../../examples/source/runners/databricks/run_polars_sdk.py.md) ·
[raw](../../examples/files/runners/databricks/run_polars_sdk.py)); run `--help`.
SDK Files API access to Volume control files does not mount `/Volumes` locally,
submit remote Spark or configure Polars business storage credentials. Business
connections must address storage directly accessible to that process.

## Troubleshooting and next steps

| Symptom | Check |
|---|---|
| Volume reads work but table denied | Output namespace/table grants to Run as identity |
| Output appears as a table instead of Volume files | Inherited catalog/database in effective metadata |
| Serverless dependency/API error | Environment version/dependencies and Spark Connect limitations |
| Zero selected flows | Built metadata root, stage and shard widgets |
| External Polars cannot read `/Volumes` | SDK access is not a local filesystem mount |

Use [operations](../operations/index.md) for replay, sharding, maintenance and
functions. The [Databricks simulator](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/platforms/databricks/README.md)
and [WWI walkthrough](../../examples/wwi-medallion-multicloud.md#environment-matrix)
are larger scenarios requiring separate preparation.
