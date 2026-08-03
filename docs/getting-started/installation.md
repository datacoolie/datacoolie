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
pip install "datacoolie[polars,deltalake]"
```

If you already know Spark is your main runtime, install Spark + Delta instead:

```bash
pip install "datacoolie[spark,delta-spark]"
```

Use `datacoolie[all]` for contributor machines or broad local experimentation.
The bare `pip install datacoolie` package is mainly useful for extension work,
API exploration, or environments where another package layer supplies the
runtime dependencies.

## Quick decision table

| You want to do first | Install |
|---|---|
| Fastest first success on one machine | `pip install "datacoolie[polars,deltalake]"` |
| Spark/Fabric/Databricks-style local validation | `pip install "datacoolie[spark,delta-spark]"` |
| Try many engines, platforms, and metadata backends locally | `pip install "datacoolie[all]"` |
| Only inspect APIs or develop extensions | `pip install datacoolie` |

## Pick your extras

```bash
# Minimal (no engine extras; rarely useful on its own)
pip install datacoolie

# Most common: one engine + one table format
pip install "datacoolie[polars,deltalake]"
pip install "datacoolie[spark,delta-spark]"

# Everything
pip install "datacoolie[all]"
```

## Extras reference

| Extra | Installs | Use when |
|---|---|---|
| `spark` | `pyspark>=3.5` | You want the Spark engine. |
| `polars` | `polars>=1.0` | You want the Polars engine. |
| `delta-spark` | `delta-spark>=3.0` | Spark + Delta Lake. |
| `deltalake` | `deltalake>=0.15` | Polars + Delta Lake (delta-rs). |
| `iceberg` | `pyiceberg>=0.6` | Apache Iceberg tables (any engine). |
| `boto3` | `boto3>=1.28` | AWS SDK only. |
| `api` | `httpx>=0.24` | `APIReader` and the API metadata provider. |
| `db` | `sqlalchemy>=2.0` | `DatabaseProvider` for metadata stored in an RDBMS. |
| `excel` | `fastexcel`, `openpyxl` | Reading Excel files as sources or metadata. |
| `fabric-spark` / `fabric-polars` | One selected engine bundle | Fabric with only the selected engine. |
| `fabric` | spark + delta-spark + polars + deltalake | Microsoft Fabric notebooks / Spark pools. |
| `databricks-spark` / `databricks-polars` | One selected engine bundle | Databricks with only the selected engine. |
| `databricks` | spark + delta-spark + polars + deltalake | Databricks Runtime. |
| `aws-spark` | `boto3` | AWS/Glue Spark; Spark and Delta are supplied by the Glue runtime. |
| `aws-polars` | polars + Delta + Iceberg + `boto3` | AWS with Polars. |
| `aws` | same as `aws-polars` | Full packaged AWS bundle; Spark/Delta Spark remain runtime-provided. |
| `all` | everything above | Kitchen-sink local dev. |

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
