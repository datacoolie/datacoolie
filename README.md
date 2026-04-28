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

## Installation

```bash
# Core only
pip install datacoolie

# With Spark support (primary)
pip install datacoolie[spark]

# With Polars support
pip install datacoolie[polars]

# All engines
pip install datacoolie[all]
```

## Quick Start

Install, save the script below as `quickstart.py`, and run it. Part A generates
a sample CSV + `metadata.json`; Part B runs the pipeline.

```bash
pip install "datacoolie[polars]"
```

```python
# quickstart.py
# --- Part A: prepare sample data & metadata (stdlib only) --------------------
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

# --- Part B: run DataCoolie --------------------------------------------------
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
provider = FileProvider(config_path=str(metadata_path), platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
    result = driver.run(stage="bronze2silver")
    print(f"Completed: {result.succeeded}/{result.total}")
```

```bash
python quickstart.py
```

Swap `PolarsEngine` for `SparkEngine(spark, ...)` or `LocalPlatform()` for
`AwsPlatform` / `FabricPlatform` / `DatabricksPlatform` — the metadata stays
the same.

## Testbed & scenarios

See [usecase-sim/README.md](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/README.md) for a ready-made integration
testbed that exercises every `{polars,spark} × {file,database,api} × {local,aws}`
combination, plus lakehouse maintenance and a Docker-compose backend stack.

## License

[AGPL-3.0-or-later](https://github.com/datacoolie/datacoolie/blob/main/LICENSE) — free and open source.

See [CONTRIBUTING.md](https://github.com/datacoolie/datacoolie/blob/main/CONTRIBUTING.md) for contribution terms.