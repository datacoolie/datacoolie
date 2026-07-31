---
title: Troubleshooting — DataCoolie Operations
description: Diagnose common DataCoolie failures across metadata loading, engines, platforms, plugins, and destination writes.
---

# Troubleshooting

## "`pl.sql_expr` unknown function …"

**Polars** uses a SQL subset. Common offenders:

| Doesn't work in Polars | Use |
|---|---|
| `current_timestamp()` | Literal cast, or framework-added `__updated_at`. |
| `date_format(col, 'yyyy-MM-dd')` | `CAST(col AS DATE)`. |
| `year(col)` | `EXTRACT(YEAR FROM col)`. |

See [Partitioning](../how-to/partitioning-and-sanitization.md#sql-expression-portability).

## Row count mismatch for multi-line JSON

JSONL treats each line as a record; JSON arrays treat each element as a
record. If the source file has *embedded newlines inside string fields*:

- `format: "jsonl"` → one row per file line → **wrong**.
- `format: "json"` → one row per array element → correct.

## Excel `is_active` column loads everything as inactive

The Excel generator leaves `is_active` blank when unset, which is correctly
parsed as `None` → defaults to `True`. If someone hand-typed `FALSE` in the
cell it is parsed as `False`. Fix: blank the cell.

## "dead lock detected" on Delta optimize

Two dataflows writing the same destination are running `OPTIMIZE` in parallel.
DataCoolie deduplicates maintenance by destination automatically — if you're
seeing this, you probably called `run_maintenance()` twice in quick
succession from an external scheduler. Serialise the scheduler side.

## Docker preflight fails on scenarios

Some scenarios (database metadata, API metadata) require Docker. Run:

```powershell
python usecase-sim/scripts/setup_platform.py
```

Then re-run the scenario.

### Running Spark scenarios with Docker

A containerized PySpark environment is available for running Spark-based
scenarios without installing Spark locally:

```powershell
python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest spark
python usecase-sim/runner/run_scenario.py --scenario local_spark_file
```

When the `datacoolie-spark` container is running, `run_scenario.py`
automatically executes Spark scenarios in it. The repository is mounted at
`/datacoolie`.

## Iceberg writes do not appear in the expected catalog

An Iceberg connection must use `"format": "iceberg"` and point at the catalog
that readers query. Local scenarios use the Iceberg REST catalog; AWS scenarios
use a Glue-backed catalog and need the corresponding Iceberg runtime and IAM
permissions. Delta-only symlink options (`generate_manifest` and
`register_symlink_table`) do not register Iceberg tables.

## `WatermarkManager` throws on first run

First run has no persisted watermark — `get_watermark()` returns `None`. If
your source reader crashes on `None`, it's a bug in the reader: it must
treat `None` (or empty dict after decoding) as "no filter, read everything".
