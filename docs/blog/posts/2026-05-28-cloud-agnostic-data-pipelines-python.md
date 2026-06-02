---
date: 2026-05-28
categories:
  - Tutorial
authors:
  - datacoolie
description: Learn how to build cloud-agnostic data pipelines in Python that run on AWS, Azure Fabric, and Databricks without rewriting code — using metadata-driven ETL.
---

# How to Build Cloud-Agnostic Data Pipelines in Python

Moving a data pipeline from one cloud to another usually means rewriting file I/O, secrets management, and authentication code. Platform lock-in — where pipeline code is tightly coupled to a specific cloud's APIs and paths — isn't a theoretical problem. It's the reason data teams maintain parallel codebases for the same business logic.

This post shows how to build pipelines that run on local machines, AWS Glue, Microsoft Fabric, and Databricks without code changes.

<!-- more -->

## The Problem: Platform-Coupled Pipelines

A typical Spark ETL job on Databricks looks like this:

```python
# Databricks-specific
df = spark.read.format("delta").load("dbfs:/mnt/raw/orders")
df.write.format("delta").mode("overwrite").save("dbfs:/mnt/silver/orders")
```

The same job on AWS Glue:

```python
# AWS-specific
df = spark.read.format("delta").load("s3://my-bucket/raw/orders")
df.write.format("delta").mode("overwrite").save("s3://my-bucket/silver/orders")
```

And on Fabric:

```python
# Fabric-specific
df = spark.read.format("delta").load("abfss://raw@onelake.dfs.fabric.microsoft.com/orders")
df.write.format("delta").mode("overwrite").save("abfss://silver@onelake.dfs.fabric.microsoft.com/orders")
```

Three versions of the same pipeline. Same business logic. Different file paths, different auth, different secrets.

## The Solution: Separate Intent from Execution

DataCoolie solves this by separating **what** the pipeline does from **where** it runs:

```json
{
  "connections": [
    {
      "name": "raw_orders",
      "connection_type": "file",
      "format": "delta",
      "configure": { "base_path": "raw/" }
    },
    {
      "name": "silver_orders",
      "connection_type": "file",
      "format": "delta",
      "configure": { "base_path": "silver/" }
    }
  ],
  "dataflows": [
    {
      "name": "orders_bronze_to_silver",
      "stage": "bronze2silver",
      "source": { "connection_name": "raw_orders", "table": "orders" },
      "destination": {
        "connection_name": "silver_orders",
        "table": "orders",
        "load_type": "full_load"
      }
    }
  ]
}
```

The metadata uses relative paths. The **platform** resolves them at runtime:

```python
# Local development
platform = LocalPlatform()  # paths resolve to ./raw/, ./silver/

# AWS Glue
platform = AWSPlatform(bucket="my-bucket")  # paths resolve to s3://my-bucket/raw/

# Microsoft Fabric
platform = FabricPlatform()  # paths resolve to OneLake ABFSS paths

# Databricks
platform = DatabricksPlatform()  # paths resolve to DBFS or Unity Catalog
```

## Step-by-Step: One Pipeline, Four Platforms

### 1. Define metadata once

Write a `metadata.json` with connections, dataflows, transforms, and load strategies. This file is your pipeline's single source of truth.

### 2. Develop locally with Polars

```python
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
provider = FileProvider(config_path="metadata.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
    driver.run(stage="bronze2silver")
```

Test fast. Iterate fast. No cloud costs.

### 3. Deploy to Fabric with zero changes to metadata

```python
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.platforms.fabric_platform import FabricPlatform

platform = FabricPlatform()
engine = SparkEngine(spark=spark, platform=platform)
provider = FileProvider(config_path="metadata.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
    driver.run(stage="bronze2silver")
```

Same `metadata.json`. Different platform. Done.

### 4. Or deploy to Databricks or AWS Glue

Swap the platform class. The metadata, transforms, load strategies, watermarks, and schema hints all stay the same.

## What Makes This Work

Three abstraction layers make cloud portability real:

1. **[`BasePlatform`](../../concepts/platforms.md)** — abstracts file I/O (read/write/list/delete) and secret retrieval per cloud
2. **[`BaseEngine[DF]`](../../concepts/engines.md)** — abstracts DataFrame operations so Polars and Spark share one API
3. **[Metadata](../../concepts/metadata-model.md)** — externalizes all pipeline configuration so nothing is hardcoded

Secrets, file paths, and auth are the platform's problem. Business logic is the metadata's problem. Your code just wires them together.

## When Cloud-Agnostic Matters

- **Multi-cloud enterprises** running workloads across Azure and AWS
- **Migration projects** moving from Databricks to Fabric (or vice versa)
- **Local-first development** where engineers shouldn't need cloud credentials to iterate
- **Vendor negotiation leverage** — portable pipelines prevent lock-in pricing

## Get Started

```bash
pip install "datacoolie[polars]"
```

Follow the [Polars quickstart](../../getting-started/quickstart-polars.md), then read the platform-specific deployment guides:

- [Deploy to Fabric](../../how-to/deploy-to-fabric.md)
- [Deploy to Databricks](../../how-to/deploy-to-databricks.md)
- [Deploy to AWS Glue](../../how-to/deploy-to-aws-glue.md)
