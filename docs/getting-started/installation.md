---
title: Install DataCoolie for Python ETL
description: Install DataCoolie with the right extras for Polars, Spark, Delta Lake, Iceberg, cloud SDKs, and metadata backends.
---

# Installation

DataCoolie is published to PyPI. The base package stays light;
engines, cloud SDKs, and metadata backends are **opt-in extras** so you only
install what you need.

## Recommended first install

For most new users, start with the smallest setup that can run a real pipeline:

```bash
pip install "datacoolie[polars-delta]"
```

If you already know Spark is your main runtime, install Spark + Delta instead:

```bash
pip install "datacoolie[spark-delta]"
```

Use `datacoolie[all]` for contributor machines or broad local experimentation.
The bare `pip install datacoolie` package is mainly useful for extension work,
API exploration, or environments where another package layer supplies the
runtime dependencies.

## Quick decision table

| You want to do first | Install |
|---|---|
| Fastest first success on one machine | `pip install "datacoolie[polars-delta]"` |
| Run qualified SQL over registered Polars tables | `pip install "datacoolie[polars-sql,polars-delta,polars-iceberg]"` |
| Access OneLake or ADLS from a laptop, Azure Function, or CI | `pip install "datacoolie[fabric-external]"` |
| Access Databricks UC Volumes from a laptop, function, or CI | `pip install "datacoolie[databricks-external]"` |
| Spark/Fabric/Databricks-style local validation | `pip install "datacoolie[spark-delta]"` |
| Access AWS S3, MinIO, or LocalStack | `pip install "datacoolie[aws]"` |
| Read API-backed metadata | `pip install "datacoolie[source-api]"` |
| Read database metadata | `pip install "datacoolie[metadata-db]"` |
| Read Excel files with Polars | `pip install "datacoolie[source-excel-polars]"` |
| Try many engines, platforms, and metadata backends locally | `pip install "datacoolie[all]"` |
| Only inspect APIs or develop extensions | `pip install datacoolie` |

## Pick your extras

```bash
# Minimal (no engine extras; rarely useful on its own)
pip install datacoolie

# Most common: one engine + one table format
pip install "datacoolie[polars-delta]"
pip install "datacoolie[polars-sql,polars-delta,polars-iceberg]"
pip install "datacoolie[spark-delta]"

# Platform SDKs for execution outside their native runtime
pip install "datacoolie[fabric-external]"
pip install "datacoolie[databricks-external]"
pip install "datacoolie[aws]"  # also covers MinIO and LocalStack

# Everything
pip install "datacoolie[all]"
```

## Extras reference

| Extra | Installs | Use when |
|---|---|---|
| `spark` | `pyspark>=3.5` | Spark engine only. Prefer `spark-delta` for a local Spark + Delta setup. |
| `polars` | `polars>=1.0` | Polars engine only. |
| `polars-sql` | `polars>=1.0`, `sqlglot>=30,<31` | Qualified SQL over Polars relations. |
| `polars-hash` | `polars-hash>=0.6` | Optional Polars hashing implementation; compose with an engine profile. |
| `spark-delta` | `pyspark>=3.5`, `delta-spark>=3.0` | Local or CI Spark + Delta Lake. Fabric and Databricks provide these at runtime. |
| `polars-delta` | `polars>=1.0`, `deltalake>=0.15` | Polars + Delta Lake (`delta-rs`). |
| `polars-iceberg` | `polars>=1.0`, `pyiceberg>=0.6` | Polars + Apache Iceberg. Spark Iceberg remains a runtime/catalog/JAR concern. |
| `aws` | `boto3>=1.43.2` | AWS S3 and services, plus S3-compatible MinIO or LocalStack. |
| `fabric-external` | Azure Identity, Data Lake, and Key Vault SDKs | `FabricPlatform` outside a Fabric notebook. Native Fabric supplies `notebookutils`; install the base package there. |
| `databricks-external` | `databricks-sdk>=0.121,<0.122` | `DatabricksPlatform` outside a Databricks notebook or job. Native Databricks supplies `dbutils`; install the base package there. |
| `source-api` | `httpx>=0.24` | API readers or API-backed metadata. |
| `source-excel-polars` | Polars, `fastexcel`, `openpyxl` | Excel sources read through Polars. |
| `source-db-polars` | Polars, `connectorx` | General Polars database reads. |
| `source-db-oracle-polars` | Polars, `oracledb` | Oracle reads through Polars. |
| `source-db-mssql-odbc-polars` | Polars, SQLAlchemy, `pyodbc` | MSSQL reads using ODBC; an OS ODBC driver is also required. |
| `metadata-yaml` | `pyyaml>=6.0,<7.0` | YAML metadata files. |
| `metadata-excel` | `openpyxl>=3.1` | Excel metadata files. |
| `metadata-db` | `sqlalchemy>=2.0,<3.0` | Database metadata provider. |
| `all` | Union of every dependency above | Broad local/contributor environment; not a minimal deployment image. |

Extras are composable. For example, a Polars Delta pipeline that reads an
Oracle source and writes to S3 can use
`datacoolie[polars-delta,source-db-oracle-polars,aws]`. There are deliberately
no separate `fabric-*`, `databricks-*`, or `aws-*` matrix extras.

Native Fabric and Databricks runtimes provide notebook utilities, Spark, and
their cloud connectors. Do not install fake Python packages for `notebookutils`
or `dbutils`; use the base `datacoolie` install in those runtimes and add only
the source or table-format profile your pipeline needs.

## System requirements

| Component | Minimum | Notes |
|---|---|---|
| Python | **3.11+**, below 4.0 | Enforced by package metadata. |
| Java | Compatible with your installed PySpark runtime | Only required when using `SparkEngine`; the simulator image uses Java 17. |
| RAM | — | Size for the engine, workload, partitions, and concurrency. |
| Disk | — | Depends on your lakehouse layout. |

!!! warning "Windows timezones"
    On Windows, Python's `zoneinfo` needs `tzdata` to resolve IANA zones.
    `tzdata` is pulled in automatically via the `sys_platform == 'win32'` marker
    in `pyproject.toml`. If you vendor a custom wheel, install `tzdata` explicitly.

## Verify

```python
import datacoolie

print(datacoolie.__version__)

# Includes import-registered built-ins and discovered installed entry points.
print(datacoolie.engine_registry.list_plugins())
print(datacoolie.platform_registry.list_plugins())
```

Expected output (with `[all]`):

```text
0.1.3
['polars', 'spark']
['aws', 'databricks', 'fabric', 'local']
```

If one of your engines is missing, the extra for it is not installed — see
the table above.

## Common beginner trap

If `import datacoolie` works but your quickstart still cannot create an engine
or read/write a table, you almost always installed the base package without the
engine or table-format extra you need.

## Next

- Most new users: [Quickstart · Polars](quickstart-polars.md)
- Spark-first users: [Quickstart · Spark](quickstart-spark.md)
