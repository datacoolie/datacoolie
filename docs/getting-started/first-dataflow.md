---
title: First Dataflow Tutorial — DataCoolie
description: Build your first DataCoolie pipeline end to end by defining metadata, running a stage, and inspecting outputs and logs.
---

# Your first dataflow (tutorial)

A 20-minute walkthrough: ingest CSV → merge into a bronze Delta table →
write a partitioned silver table → inspect the watermark → re-run incrementally.

**Prerequisites**

- Completed the [Polars quickstart](quickstart-polars.md).
- Comfortable reading JSON metadata.

**End state**

- Two ordered stages (`ingest2bronze`, `bronze2silver`) running in one driver invocation.
- A typed, partitioned Delta silver table.
- A watermark file you can inspect and reason about.
- Understanding of how to move to Spark or another cloud without changing metadata.

## 1. Extend the metadata

Replace `metadata/orders.json` from the quickstart with the two-stage version:

```json
{
  "connections": [
    {"name": "local_input",   "connection_type": "file",      "format": "csv",   "configure": {"base_path": "data/input"}},
    {"name": "local_bronze",  "connection_type": "lakehouse", "format": "delta", "configure": {"base_path": "data/output/bronze"}},
    {"name": "local_silver",  "connection_type": "lakehouse", "format": "delta", "configure": {"base_path": "data/output/silver"}}
  ],
  "dataflows": [
    {
      "name": "orders_to_bronze",
      "stage": "ingest2bronze",
      "group_number": 1,
      "execution_order": 10,
      "source":      {"connection": "local_input", "table": "orders", "watermark_columns": ["updated_at"]},
      "destination": {"connection": "local_bronze", "schema_name": "sales", "table": "orders",
                      "load_type": "merge_upsert", "merge_keys": ["order_id"]},
      "transform":   {"deduplicate_columns": ["order_id"], "latest_data_columns": ["updated_at"],
                       "schema_hints": [
        {"column_name": "order_id",    "data_type": "int"},
        {"column_name": "customer_id", "data_type": "int"},
        {"column_name": "amount",      "data_type": "decimal", "precision": 18, "scale": 2},
        {"column_name": "updated_at",  "data_type": "timestamp"}
      ]}
    },
    {
      "name": "orders_daily_totals",
      "stage": "bronze2silver",
      "group_number": 1,
      "execution_order": 20,
      "source":      {"connection": "local_bronze", "schema_name": "sales", "table": "orders"},
      "destination": {"connection": "local_silver", "schema_name": "sales", "table": "orders_daily_totals",
                      "load_type": "overwrite",
                      "partition_columns": [{"column": "order_date", "expression": "date(updated_at)"}]}
    }
  ]
}
```

`partition_columns[].expression` is enough here. `PartitionHandler` materialises
`order_date` before the write, so you do not need a duplicate
`transform.additional_columns` entry.

## 2. Run both stages

`run.py`:

```python
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
metadata = FileProvider(config_path="metadata/orders.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=metadata) as driver:
    result = driver.run(stage=["ingest2bronze", "bronze2silver"])

print(result)
```

Expected console:

```text
ExecutionResult(total=2, succeeded=2, failed=0, skipped=0)
```

Stages are filters, not a DAG scheduler. The explicit `group_number` and
`execution_order` values in the metadata above are what keep bronze ahead of
silver in this single `run()` call. If you prefer not to encode ordering in
metadata, run the two stages in separate `driver.run(...)` calls instead.

## 3. Inspect the watermark

If you do not pass `watermark_base_path` to `FileProvider`, watermark files
default to the metadata directory: `metadata/watermarks/<stage>_<name>_<dataflow_id>/watermark.json`.

```bash
cat metadata/watermarks/*/watermark.json
```

You should see `{"updated_at": {"__datetime__": "2026-04-03T09:15:00+00:00"}}`.

The `__datetime__` sentinel is intentional — it lets DataCoolie round-trip
typed values through plain JSON. See [Concepts · Watermarks](../concepts/watermarks.md).

## 4. Add new input, re-run

Append rows dated later than the watermark. Re-run `python run.py`. The bronze
stage processes only the new rows; the silver stage rewrites its aggregate
(because its load type is `overwrite`).

## 5. Swap engine or platform — zero metadata change

Switching to Spark:

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform

builder = (
  SparkSession.builder.appName("tut")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

platform = LocalPlatform()
engine = SparkEngine(spark_session=spark, platform=platform)
metadata = FileProvider(config_path="metadata/orders.json", platform=platform)
```

Switching to AWS (S3 + Secrets Manager):

```python
from datacoolie.platforms.aws_platform import AWSPlatform
engine.set_platform(AWSPlatform(region="us-east-1"))
```

Then edit `base_path` in your connections to `s3://my-bucket/bronze` and re-run.

## Key ideas this tutorial exercised

| Idea | See |
|---|---|
| Stages are filters, not a DAG scheduler | [Concepts · Orchestration](../concepts/orchestration.md) |
| `schema_hints` drive `SchemaConverter` | [Concepts · Transformers](../concepts/transformers-and-pipeline.md) |
| Partition columns can be SQL expressions | [How-to · Partitioning](../how-to/partitioning-and-sanitization.md) |
| `merge_upsert` vs `overwrite` | [Concepts · Load strategies](../concepts/load-strategies.md) |
| Watermarks are metadata-provider-stored JSON | [Concepts · Watermarks](../concepts/watermarks.md) |
