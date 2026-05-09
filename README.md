<p align="center">
    <img src="https://raw.githubusercontent.com/datacoolie/datacoolie/main/docs/images/banners/datacoolie-banner-dark.png" alt="DataCoolie banner" width="100%">
</p>

# DataCoolie — Metadata-driven ETL Framework

Metadata-driven ETL framework that unifies execution engines (Spark, Polars, and more in the future), remains cloud-agnostic (Fabric, AWS, Databricks, and more in the future), and currently focuses on batch workloads with a roadmap to micro-batch and streaming.

## What problem does it solve?

Data teams often prototype pipelines locally, then rewrite the same pipeline
for Spark and again for each cloud runtime. That duplicates ETL code and makes
operational behavior such as watermarks, schema hints, partitions, load
strategies, and maintenance drift across environments.

DataCoolie solves this by separating pipeline intent from execution details.
You define connections, dataflows, transforms, and operational controls as
metadata, then run the same intent on Polars or Spark and on local, Fabric,
Databricks, or AWS platforms.

## Why it helps

- **Metadata-driven** — pipeline behavior lives in metadata instead of being re-implemented in each job.
- **Right-sized compute** — small and medium jobs can stay on lighter runtimes like Polars or local execution instead of paying Spark or cluster overhead too early.
- **Portable** — the same metadata can move to Spark and cloud platforms when workloads grow.
- **Engine-unified** — the same metadata runs on Spark *and* Polars; swap at runtime.
- **Cloud-agnostic** — `local`, `aws`, `fabric`, `databricks` platforms abstract file I/O and secrets.
- **Lakehouse-native** — first-class Delta Lake and Apache Iceberg via `fmt="delta"` / `fmt="iceberg"`.
- **Operationally complete** — watermarks, schema hints, partitions, load strategies, logging, and maintenance are built in.
- **Plugin everything** — engines, platforms, sources, destinations, transformers, and secret resolvers are all entry-point plugins.

## Start here

If you are evaluating DataCoolie for the first time, use this order:

1. Install the smallest useful runtime: `pip install "datacoolie[polars,deltalake]"`
2. Run the quick start below
3. Then move to the docs for using your own input and building a multi-stage flow

If you already know your runtime will be Spark, swap the install to
`pip install "datacoolie[spark,delta-spark]"` and keep the same metadata pattern.

## Installation

```bash
# Most common first install
pip install "datacoolie[polars,deltalake]"

# Spark-first local validation
pip install "datacoolie[spark,delta-spark]"

# Core only (mainly useful for extension work)
pip install datacoolie

# All engines
pip install datacoolie[all]
```

## Quick Start

Install, then run two short scripts:

1. `prepare_quickstart.py` creates a sample CSV and `metadata.json`.
2. `run_quickstart.py` loads that metadata and runs the pipeline.

```bash
pip install "datacoolie[polars]"
```

### Part 1 — Prepare sample data and metadata

```python
# prepare_quickstart.py
import json
from pathlib import Path

root = Path("dc_quickstart")
(root / "input" / "orders").mkdir(parents=True, exist_ok=True)
(root / "output").mkdir(parents=True, exist_ok=True)

(root / "input/orders/orders.csv").write_text(
    "order_id,customer_id,amount\n1,100,19.99\n2,100,42.50\n3,101,7.25\n"
)

metadata = {
    "connections": [
        {"name": "csv_in", "connection_type": "file", "format": "csv",
         "configure": {"base_path": str(root / "input"),
                "read_options": {"header": "true", "inferSchema": "true"}}},
        {"name": "parquet_out", "connection_type": "file", "format": "parquet",
         "configure": {"base_path": str(root / "output")}},
    ],
    "dataflows": [
        {"name": "orders_csv_to_parquet", "stage": "bronze2silver",
         "processing_mode": "batch",
         "source": {"connection_name": "csv_in", "table": "orders"},
         "destination": {"connection_name": "parquet_out", "table": "orders",
                         "load_type": "full_load"},
         "transform": {}},
    ],
}
metadata_path = root / "metadata.json"
metadata_path.write_text(json.dumps(metadata, indent=2))
print(f"Created {metadata_path}")
```

```bash
python prepare_quickstart.py
```

### Part 2 — Run the pipeline

```python
# run_quickstart.py
from pathlib import Path

from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver

root = Path("dc_quickstart")
metadata_path = root / "metadata.json"

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
provider = FileProvider(config_path=str(metadata_path), platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
    result = driver.run(stage="bronze2silver")
    print(f"Completed: {result.succeeded}/{result.total}")
```

```bash
python run_quickstart.py
```

Swap `PolarsEngine` for `SparkEngine(spark, ...)` or `LocalPlatform()` for
`AwsPlatform` / `FabricPlatform` / `DatabricksPlatform` — the metadata stays
the same.

## What to do next

- Use your own files while keeping the same runner pattern: <https://datacoolie.github.io/datacoolie/getting-started/use-your-own-data/>
- Build a multi-stage bronze→silver tutorial flow: <https://datacoolie.github.io/datacoolie/getting-started/first-dataflow/>
- Learn the metadata model field by field: <https://datacoolie.github.io/datacoolie/how-to/metadata-guide/>

## Testbed & scenarios

See [usecase-sim/README.md](usecase-sim/README.md) for a ready-made integration
testbed that exercises every `{polars,spark} × {file,database,api} × {local,aws}`
combination, plus lakehouse maintenance and a Docker-compose backend stack.

## License

[AGPL-3.0-or-later](LICENSE) — free and open source.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution terms.