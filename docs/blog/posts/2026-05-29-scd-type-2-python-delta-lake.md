---
date: 2026-05-29
categories:
  - Tutorial
authors:
  - datacoolie
description: "Step-by-step guide to implementing SCD Type 2 (slowly changing dimensions) in Python with Delta Lake using DataCoolie's metadata-driven approach."
---

# Implementing SCD Type 2 in Python with Delta Lake

Slowly Changing Dimension Type 2 (SCD2) is a data warehousing pattern that preserves the full history of dimension changes. When a customer changes their address, SCD2 keeps both the old and new records with effective date ranges — so you can join facts to the correct dimension state at any point in time.

Implementing SCD2 correctly is harder than it looks. This post shows how DataCoolie handles it declaratively with metadata instead of hand-coded merge logic.

<!-- more -->

## Why SCD2 Is Hard to Get Right

A production-grade SCD2 implementation needs to:

1. **Version by effective date** — use a source business-time column to determine when each version became active
2. **Close old records** — set `__valid_to` to the incoming `__valid_from` and mark `__is_current = false`
3. **Insert new records** — with `__valid_from = source_timestamp`, `__valid_to = NULL`, `__is_current = true`
4. **Handle inserts** — brand new keys get a fresh active record on first load
5. **Guard against late arrivals** — skip the close step if the incoming timestamp is not newer than the current version
6. **Be idempotent** — re-running the same batch shouldn't create duplicate versions

Most teams write 50–200 lines of merge SQL or PySpark for each SCD2 table. When you have 20 dimension tables, that's thousands of lines of nearly-identical, error-prone code.

## The DataCoolie Approach: Declare, Don't Code

With DataCoolie, SCD2 is a metadata declaration:

```json
{
  "name": "customers_scd2",
  "stage": "bronze2silver",
  "source": {
    "connection_name": "raw_input",
    "table": "customers"
  },
  "destination": {
    "connection_name": "silver_lake",
    "table": "dim_customers",
    "load_type": "scd2",
    "merge_keys": ["customer_id"],
    "configure": {
      "scd2_effective_column": "updated_at"
    }
  },
  "transform": {
    "schema_hints": [
      { "column_name": "customer_id", "data_type": "integer" },
      { "column_name": "name",        "data_type": "string" },
      { "column_name": "email",       "data_type": "string" },
      { "column_name": "city",        "data_type": "string" },
      { "column_name": "updated_at",  "data_type": "timestamp" }
    ]
  }
}
```

`scd2_effective_column` names a column in your **source data** (here `updated_at`) whose value timestamps when each version became effective. DataCoolie's engine handles the merge logic, framework columns, and late-arrival guarding.

## What DataCoolie Generates Under the Hood

When you declare `"load_type": "scd2"`, the framework:

1. **Adds three SCD2 framework columns** via the `SCD2ColumnAdder` transformer (order 60):
   - `__valid_from` — copied from your `scd2_effective_column` source value
   - `__valid_to` — `NULL` on new rows (means "still current")
   - `__is_current` — `true` on new rows

2. **Executes a two-step Delta Lake MERGE** using `merge_keys`:
   - **Close step** — for every incoming row whose keys match a target row where `__is_current = true` *and* the incoming `__valid_from` is later than the target `__valid_from` (late-arrival guard), set `__valid_to = source.__valid_from` and `__is_current = false`
   - **Append step** — insert all incoming rows as new active versions

3. **First-run fallback** — if the destination table doesn't exist yet, the first run creates it with an initial overwrite, then subsequent runs use the two-step MERGE

4. **Updates the watermark** so the next run processes only new data

## Running It

```python
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
provider = FileProvider(config_path="metadata.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
    result = driver.run(stage="bronze2silver")
    print(f"SCD2 merge: {result.succeeded}/{result.total} dataflows completed")
```

### First run — initial load

All source rows become active records with `is_current = true` and `effective_to = null`.

### Second run — changes detected

If a customer's city changed from "Boston" to "Denver" and `updated_at` advanced:

| customer_id | name | city | __valid_from | __valid_to | __is_current |
|-------------|------|------|--------------|------------|---------------|
| 42 | Alice | Boston | 2026-05-01 | 2026-05-29 | false |
| 42 | Alice | Denver | 2026-05-29 | null | true |

The old record is closed. The new record is inserted. Historical queries can join on `__valid_from <= fact_date < __valid_to`.

## Merge Upsert vs SCD2

DataCoolie also supports `merge_upsert` for dimensions where you don't need history:

| Strategy | When to use |
|----------|-------------|
| `merge_upsert` | Latest state only; overwrites changed rows in place |
| `scd2` | Full history of changes; preserves every version |
| `append` | Insert-only fact tables; no updates |
| `full_load` | Drop and reload; simple but expensive |

All four strategies use the same metadata structure — just change `load_type`.

## Common Pitfalls DataCoolie Prevents

- **Missing merge keys** — validation raises an error before the run starts
- **Missing `scd2_effective_column`** — lint rule `scd2-effective-column-required` warns at authoring time
- **Late-arrival duplicates** — the late-arrival guard skips the close step if the incoming `__valid_from` is not newer than the existing version
- **Schema drift** — `schema_hints` (a list of `{column_name, data_type}` objects) cast source columns to declared types; requires `source.connection.use_schema_hint: true` to take effect
- **Watermark staleness** — automatic watermark tracking means re-runs don't reprocess already-seen data

## Learn More

- [Merge & SCD2 how-to guide](../../how-to/merge-and-scd2.md) — full configuration reference
- [Load strategies concept](../../concepts/load-strategies.md) — how append, overwrite, merge, and SCD2 work internally
- [Metadata guide — destination patterns](../../how-to/metadata-guide/destination-and-load-patterns.md) — field-by-field walkthrough
