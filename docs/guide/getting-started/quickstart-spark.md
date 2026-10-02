---
title: Quickstart · Spark
description: Run the canonical typed orders project with a local Delta-enabled Spark session and verify the same business contract.
---

# Quickstart · Spark

This route uses the same downloaded project and metadata as the [Polars
quickstart](quickstart-polars.md). It adds a JVM, a Delta extension and a
Spark session lifecycle. Use a fresh extracted directory so Spark does not
reuse a Delta schema or watermark created by another engine.

## Prerequisites

Install `datacoolie[cli,spark-delta]` as described in
[Installation](installation.md), then check:

```bash
java -version
python -c "import pyspark, delta; print(pyspark.__version__); print(delta.__file__)"
```

The reviewed local candidate uses Python 3.11.9, PySpark 4.1.1,
`delta-spark` 4.2.0 and Java 17.0.12. Delta's helper can resolve Maven
artifacts during the first start. A successful Python import alone does not
prove that the JVM can load those artifacts.

## Run the orders lesson

Extract [getting-started.zip](../../examples/downloads/getting-started.zip),
change into its `getting-started/` directory and run:

```bash
python runners/local/run_spark.py --lesson orders --state-base-path .runtime
```

The runner creates a local Delta-enabled `SparkSession`, uses local execution,
checks that `orders_to_bronze` is the only selected flow, and stops the session
in a `finally` path. It returns a non-zero process status when the required
flow fails or remains pending.

Read the result with a Spark session in the same inspection process rather
than referring to a `spark` variable from the finished runner process:

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .master("local[2]")
    .appName("dc-inspect")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()
try:
    rows = spark.read.format("delta").load("data/output/bronze/orders")
    assert rows.select("order_id").distinct().count() == 3
    assert rows.count() == 3
    rows.select("order_id", "customer_id", "amount").orderBy("order_id").show()
finally:
    spark.stop()
```

The duplicate order is removed and the business columns follow the metadata
schema hints. Spark's DataFrame type display can differ from Polars while the
Delta table and row-level contract remain the same. The Spark runner accepts
the same lesson names. The reviewed Windows / PySpark 4.1.1 candidate passed
fresh no-change, append and Bronze-to-Silver continuation runs; an earlier
transient BlockManager heartbeat error occurred after a previous process. If
that warning recurs, start from a fresh supported Spark workspace and inspect
the final process status rather than treating a partial log as a successful
no-op. Keep a separate workspace when comparing engines.

## Troubleshoot the first JVM start

- `java` is not found: install a supported JDK and make `java` visible on
  `PATH`, or set `JAVA_HOME` for the interpreter that runs the runner.
- Delta classes cannot be resolved: allow the helper to download its Maven
  artifacts or configure the documented local cache; pip installation does
  not contain those JVM jars.
- The runner starts but the flow fails: inspect the selected package path,
  state root and input file before changing metadata. A missing state root is
  a configuration failure, not an empty successful run.

Managed Fabric, Databricks and AWS Glue sessions supply different host
 boundaries. Pass their existing session/platform objects and follow the
[platform recipes](../platforms/index.md) instead of copying this local JVM
bootstrap into a managed notebook.

## Next

- [Use your own data](use-your-own-data.md) — adapt the metadata without carrying hidden orders fields.
- [Multi-stage dataflow](multi-stage-dataflow.md) — enforce the Bronze-to-Silver barrier.
