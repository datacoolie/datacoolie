---
title: Use your own data after the quickstart
description: Choose full-refresh or incremental semantics and adapt every dependent metadata field safely.
---

# Use your own data after the quickstart

The canonical project contains two deliberately different adaptations. The
`customers` lesson is a complete full refresh. The `orders` lesson remains an
incremental keyed flow. Choose one contract before changing a path or column.

## Option A: a separate full refresh

The customer fixture has `customer_id,name` and no `order_id`, amount or
watermark. From a fresh copy of the project, replace
`data/input/customers/customers.csv` with your file using the same two-column
shape, then run:

```bash
python runners/local/run_polars.py --lesson customers --state-base-path .runtime
```

The `customers_full_refresh` flow writes to `data/output/customers/customers`
with `overwrite`. The runner uses a distinct flow, destination and runtime
identity, so it does not read or replace the orders output. Verify the result
independently:

```python
import polars as pl

customers = pl.read_delta("data/output/customers/customers")
assert customers.select("customer_id", "name").sort("customer_id").rows() == [
    (17, "Bob"),
    (42, "Alice"),
]
```

For a real customer source, update all of these together:

| Contract | Customer example | What to decide for your data |
|---|---|---|
| Source table/path | `customers` | File/table name and file format |
| Business columns | `customer_id`, `name` | Required columns and their schema hints |
| Transform | none | Any trim, cast or select rules |
| Destination identity | `local_customers.customers` | A new sandbox/table, not the orders target |
| Load mode | `overwrite` | Whether replacing the target is safe |
| State | no row watermark | Do not add one unless the refresh is intentionally incremental |

Changing only the input path while retaining an orders deduplication key,
latest-data column or merge key causes a missing-column failure or silently
applies the wrong business rule. The engine and platform can remain the same;
the source and destination contract cannot be assumed to remain the same.

## Option B: continue orders incrementally

Keep all four orders columns, a stable `order_id` key, and a monotonic
`updated_at` value. The metadata uses `merge_upsert` and deduplicates by
`order_id` using the latest timestamp. Add rows with a timestamp later than the
persisted source watermark, then run the orders lesson again:

```bash
python runners/local/run_polars.py --lesson orders --state-base-path .runtime
```

The destination retains earlier IDs and adds the new rows. Check the output
and watermark after every run. A repeated run with no new source rows must not
shrink the target.

## The overwrite and watermark trap

`watermark_columns` filters what the reader selects. `overwrite` replaces the
destination with that selected batch. Combining them is valid only when the
destination is intentionally a snapshot of the current batch. It is unsafe
for a tutorial that promises accumulated history: after the watermark advances,
the next selected batch can contain only the new row.

Use one of these explicit choices:

- Full refresh: no row watermark, a fresh sandbox destination, and deliberate
  overwrite semantics.
- Incremental history: a reliable watermark, stable keys and an append/merge
  strategy whose idempotency contract is tested.

Do not delete or reset a user's existing Delta table to repair a tutorial
transition. Use a new output and state root, or follow a separately documented
schema migration procedure.

## Source snippets and complete files

The [getting-started project files](../../examples/index.md#getting-started-project)
show the complete customer and orders metadata. The focused fields are:

- [connections.json](../../examples/source/projects/getting-started/metadata/connections.json.md)
  owns input and destination roots.
- [dataflows.json](../../examples/source/projects/getting-started/metadata/dataflows.json.md)
  owns keys, watermark, load mode and stages.
- [schema_hints.json](../../examples/source/projects/getting-started/metadata/schema_hints.json.md)
  owns the typed input contract.

The examples are source files, not an instruction to copy an orders runner into
a production project. Keep host setup and credentials in your own runner.

## Next

- [Multi-stage dataflow](multi-stage-dataflow.md) — use the typed orders output as a Bronze input.
- [Destination and load patterns](../metadata/destination-and-load-patterns.md) — compare load contracts.
- [Runtime configuration](../operations/runtime-configuration.md) — keep metadata, state and logs on explicit roots.
