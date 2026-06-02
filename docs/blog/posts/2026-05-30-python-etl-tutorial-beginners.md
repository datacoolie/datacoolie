---
date: 2026-05-30
categories:
  - Tutorial
authors:
  - datacoolie
description: "Learn ETL basics and build your first data pipeline in Python. A beginner-friendly tutorial using DataCoolie's metadata-driven approach — no prior ETL experience needed."
---

# Python ETL Tutorial for Beginners — Build Your First Data Pipeline

If you work with data, you have probably heard the term "ETL" but never had a
clear explanation of what it means or how to build one yourself. This tutorial
starts from zero — no prior ETL experience needed — and walks you through
building a working data pipeline in Python.

By the end you will understand what ETL is, why naive approaches break down at
scale, and how a metadata-driven framework like DataCoolie makes pipelines
portable, repeatable, and easy to maintain.

<!-- more -->

## What Is ETL?

**ETL** stands for **Extract, Transform, Load**. It describes the three steps of
moving data from one place to another:

1. **Extract** — read data from a source (a CSV file, a database, an API).
2. **Transform** — clean, reshape, or enrich the data (rename columns, filter
   rows, compute new fields).
3. **Load** — write the result to a destination (a Parquet file, a data lake
   table, a warehouse).

Almost every data team runs ETL pipelines daily. Order data flows from
transactional databases into analytics tables. Customer records get cleaned and
deduplicated before reporting. Sensor readings land in a lake for machine
learning.

The concept is simple, but real pipelines get complex fast. You need to track
which rows have already been processed (so you do not re-process them), handle
schema changes when columns are added, and make sure a failed run can be
retried without duplicating data. That is where frameworks help.

## A Minimal ETL Pipeline in Raw Python

Before introducing any framework, let us build a pipeline the simplest way
possible. We will read a CSV, add a computed column, and write the result as a
Parquet file.

First, create a sample CSV:

```python
# create_sample.py
from pathlib import Path

data_dir = Path("etl_tutorial")
data_dir.mkdir(exist_ok=True)

csv_content = """order_id,customer_id,product,quantity,unit_price
1,100,Widget A,2,9.99
2,101,Widget B,1,24.50
3,100,Widget C,5,4.75
4,102,Widget A,3,9.99
5,101,Widget B,2,24.50
"""

(data_dir / "orders.csv").write_text(csv_content.strip())
print("Created etl_tutorial/orders.csv")
```

Now the pipeline itself — read, transform, write:

```python
# simple_etl.py
import csv
from pathlib import Path

# --- Extract ---
rows = []
with open("etl_tutorial/orders.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)

# --- Transform ---
for row in rows:
    row["total"] = float(row["quantity"]) * float(row["unit_price"])

# --- Load ---
# (In production you would write Parquet, but let us keep it simple)
output = Path("etl_tutorial/output.csv")
with open(output, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {output}")
```

Run both scripts:

```bash
python create_sample.py
python simple_etl.py
```

This works — but only for toy data. In production, this approach breaks down.

## Why This Breaks Down at Scale

The script above has no answer for these real-world problems:

- **No incremental processing** — it reads the entire file every time, even if
  only 2 new rows were added. At millions of rows, that wastes time and
  compute. Frameworks solve this with
  [watermarks](../../concepts/watermarks.md) — a checkpoint that tracks the
  last-processed position so the next run reads only new data.
- **No schema validation** — if the source adds a column or changes a type,
  the script silently produces wrong output. Frameworks use
  [schema hints](../../concepts/metadata-model.md) to declare expected types and catch
  mismatches early.
- **No idempotency** — if the script crashes halfway, you cannot safely re-run
  it without duplicating rows. Frameworks ensure
  idempotent writes so retries produce identical results.
- **No engine portability** — the script is pure Python. If you need to process
  100 GB, you must rewrite it for Spark or Polars. A framework abstracts the
  [engine](../../concepts/engines.md) so the same pipeline definition runs on
  any runtime.
- **No platform portability** — file paths are hardcoded. Moving to AWS S3 or
  Azure requires rewriting the I/O layer.
  [Platforms](../../concepts/platforms.md) abstract this away.

This is exactly what DataCoolie solves.

## Enter DataCoolie — the Same Pipeline, Declaratively

[DataCoolie](../../index.md) is an open-source, metadata-driven ETL framework
for Python. Instead of writing pipeline logic as code, you declare **what** you
want in a metadata file and let the framework handle **how**.

### Step 1 — Install DataCoolie

```bash
pip install "datacoolie[polars]"
```

This installs DataCoolie with the [Polars engine](../../concepts/engines.md) —
a fast, lightweight DataFrame library that does not require Java or a Spark
cluster.

### Step 2 — Create Sample Data and Metadata

```python
# prepare_tutorial.py
import json
from pathlib import Path

root = Path("dc_tutorial")
(root / "input" / "orders").mkdir(parents=True, exist_ok=True)
(root / "output").mkdir(parents=True, exist_ok=True)

# Sample CSV — same data as before
csv_content = """order_id,customer_id,product,quantity,unit_price
1,100,Widget A,2,9.99
2,101,Widget B,1,24.50
3,100,Widget C,5,4.75
4,102,Widget A,3,9.99
5,101,Widget B,2,24.50"""

(root / "input/orders/orders.csv").write_text(csv_content)

# Pipeline metadata — this replaces all the procedural code
metadata = {
    "connections": [
        {
            "name": "csv_source",
            "connection_type": "file",
            "format": "csv",
            "configure": {
                "base_path": str(root / "input"),
                "read_options": {"header": "true", "inferSchema": "true"},
            },
        },
        {
            "name": "parquet_dest",
            "connection_type": "file",
            "format": "parquet",
            "configure": {"base_path": str(root / "output")},
        },
    ],
    "dataflows": [
        {
            "name": "orders_csv_to_parquet",
            "stage": "bronze2silver",
            "processing_mode": "batch",
            "source": {"connection_name": "csv_source", "table": "orders"},
            "destination": {
                "connection_name": "parquet_dest",
                "table": "orders",
                "load_type": "full_load",
            },
            "transform": {
                "additional_columns": [
                    {
                        "column_name": "total",
                        "expression": "quantity * unit_price",
                    }
                ]
            },
        },
    ],
}

(root / "metadata.json").write_text(json.dumps(metadata, indent=2))
print("Created dc_tutorial/metadata.json and sample data")
```

Let us unpack the metadata fields:

- **connections** — define *where* data lives. Each
  [connection](../../concepts/metadata-model.md) has a name, type, format, and
  backend-specific settings.
- **dataflows** — define *what* to do. Each
  [dataflow](../../concepts/metadata-model.md) links a source connection to a
  destination connection, specifies the
  [load strategy](../../concepts/load-strategies.md) (`full_load` means
  overwrite the entire table), and optionally adds transforms.
- **additional_columns** — a [transform](../../concepts/transformers-and-pipeline.md)
  that adds computed columns as SQL expressions. Here, `quantity * unit_price`
  creates the `total` column — no Python code needed.
- **stage** — a label like `"bronze2silver"` that groups related dataflows. The
  framework runs all dataflows in a stage together.

### Step 3 — Run the Pipeline

```python
# run_tutorial.py
from pathlib import Path

from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver

root = Path("dc_tutorial")

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
provider = FileProvider(config_path=str(root / "metadata.json"), platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
    result = driver.run(stage="bronze2silver")
    print(f"Completed: {result.succeeded}/{result.total}")
```

```bash
python prepare_tutorial.py
python run_tutorial.py
```

You should see:

```
Completed: 1/1
```

That is it — five rows read from CSV, a `total` column computed, and the result
written as Parquet. The pipeline runner is 8 lines of Python; all the logic
lives in metadata.

## What You Gained

Compare the raw-Python version to the DataCoolie version:

| Concern | Raw Python | DataCoolie |
|---------|-----------|------------|
| Read logic | Hardcoded `csv.DictReader` | Declared in connection metadata |
| Transform | Python loop | SQL expression in metadata |
| Write logic | Hardcoded `csv.DictWriter` | Declared in connection + load type |
| Engine switch | Rewrite everything | Change one class: `PolarsEngine` → `SparkEngine` |
| Cloud switch | Rewrite I/O | Change one class: `LocalPlatform` → `FabricPlatform` |
| Incremental loads | Not supported | Add a [watermark](../../concepts/watermarks.md) in metadata |
| Schema validation | Not supported | Add schema hints in metadata |

The metadata file is the single source of truth. When requirements change —
new columns, different load strategy, different cloud — you edit the metadata,
not the code.

## Next Steps

You have just built your first metadata-driven ETL pipeline. Here is where to
go next:

- [Quickstart · Polars](../../getting-started/quickstart-polars.md) — the
  official quickstart with more detail
- [Your first dataflow](../../getting-started/first-dataflow.md) — deeper
  walkthrough of metadata fields
- [Load strategies](../../concepts/load-strategies.md) — learn about append,
  merge, and SCD2
- [Concepts](../../concepts/architecture.md) — understand the full
  architecture
