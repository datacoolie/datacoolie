# Quickstart · Spark

Same shape as the Polars quickstart, but with a Delta-enabled local `SparkSession`.

**Prerequisites**

- Python 3.11+, Java 17
- `pip install "datacoolie[spark,delta-spark]"`

**End state** — same as the [Polars quickstart](quickstart-polars.md): CSV →
Delta table with watermark.

## 1. Metadata & data

Reuse the directory and `metadata/orders.json` from the Polars quickstart (Step 1→3).

## 2. Run it

`run.py`:

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform

builder = (
    SparkSession.builder.appName("dc-quickstart")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()

platform = LocalPlatform()
engine = SparkEngine(spark_session=spark, platform=platform)
metadata = FileProvider(config_path="metadata/orders.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=metadata) as driver:
    result = driver.run(stage="ingest2bronze")

print(f"succeeded={result.succeeded} failed={result.failed}")
```

```bash
python run.py
```

!!! note "Delta-enabled SparkSession"
    Local Spark needs a Delta-enabled session. The example above configures the
    Delta extensions and catalog explicitly. On Fabric, Databricks, EMR, or
    another managed runtime, pass the existing session into
    `SparkEngine(spark_session=..., platform=platform)` and keep the same
    shared `platform` pattern.

## 3. Inspect

```python
spark.read.format("delta").load("data/output/bronze/sales/orders").show()
```

## Differences vs the Polars run

| Aspect | Polars | Spark |
|---|---|---|
| Startup | <1 s | 10–30 s (JVM warm-up) |
| In-memory datetime dtype | `Datetime` | `timestamp` / `timestamp_ntz` |
| Scale ceiling | single node, ~10 GB | cluster, TB+ |
| Delta backend | `delta-rs` | Spark + `delta-spark` |

Both produce **the same Delta table** on disk and record compatible
watermarks — you can mix engines across stages.

If you later add `schema_hints` with timestamp columns, `SchemaConverter`
applies the same no-time-zone normalization hook on both engines. The row
above describes the runtime-native DataFrame dtype, not a different on-disk
result.

## Next

→ [Your first dataflow](first-dataflow.md)
