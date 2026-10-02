---
title: Build a multi-stage dataflow
description: Continue the typed orders example from Bronze to partitioned Silver details with an explicit dependency barrier.
---

# Build a multi-stage dataflow

The canonical project contains two orders flows:

```text
orders.csv ── orders_to_bronze ── data/output/bronze/orders
                                      │
                                      └─ orders_to_silver ── data/output/silver/orders
```

Bronze keeps the incremental reader state. Silver reads the persisted Bronze
Delta table and writes typed detail rows partitioned by `order_date`. This
tutorial does not add a daily aggregate and does not migrate an existing Delta
schema.

## Start in a clean sandbox

Extract a new copy of [getting-started.zip](../../examples/downloads/getting-started.zip)
or use a workspace whose Bronze output was created by the typed orders
quickstart. If an older hand-written quickstart created String columns, do not
replace its metadata and expect the existing Delta table to be converted. Use
a new output/state root or follow a migration procedure owned by your platform.

## Run the dependent lesson

From the project root, run:

```bash
python runners/local/run_polars.py --lesson multi-stage --state-base-path .runtime
```

The runner calls Bronze first and verifies its selected flow, terminal status
and persisted output. It calls Silver only after that barrier passes. A
successful fresh run produces three Silver detail rows. After appending the
fourth orders row from the [Polars quickstart](quickstart-polars.md), the same
lesson produces four detail rows and dates for April 1–4.

Inspect the result independently:

```python
import polars as pl

silver = pl.read_delta("data/output/silver/orders")
assert silver.height == 3  # 4 after the append lesson
assert "order_date" in silver.columns
assert silver.select("order_id").unique().height == silver.height
```

The exact business values remain those of Bronze. `order_date` is derived for
partitioning; it is not a metric. Reconcile row count, keys and amount totals
between the two tables before allowing a downstream stage to start.

## Why the barrier is explicit

`execution_order` describes ordering metadata. It is not a quality gate, and a
multi-stage call can still expose an empty selection or a failed upstream
result to the next stage. The runner therefore uses separate Driver calls:

1. Select exactly `orders_to_bronze` and run it.
2. Require the expected terminal result and a readable Bronze output.
3. Select exactly `orders_to_silver` and run it.
4. Read Silver and reconcile its keys and row count.

If the input is missing or Bronze fails, the process exits non-zero and Silver
must not run. If an incremental rerun has no new rows, the runner may accept a
supported no-change result only after checking that the input is present, the
persisted watermark proves no rows are beyond it, and the prior Bronze output
is still valid. A stale output alone is never a dependency proof.

These checks sit on the existing framework contracts: [watermark
serialization](../../reference/concepts/watermarks.md#serialisation-format),
the [Driver](../../reference/concepts/orchestration.md#driver), [transformer
ordering](../../reference/concepts/transformers-and-pipeline.md#the-twelve-built-ins-order-responsibility),
[load strategy selection](../../reference/concepts/load-strategies.md#choosing-a-strategy),
and [watermark path ownership](../../reference/concepts/watermarks.md#storage-ownership-and-path-binding).

## Spark and other hosts

Use `run_spark.py --lesson multi-stage` in a separate Spark workspace after the
local Spark quickstart passes. Managed hosts need their own session, path,
identity and connector setup. The [platform guides](../platforms/index.md)
explain those handoffs; this page establishes the metadata and stage-barrier
contract only.

## Next

- [Run a stage](../operations/run-stage.md) — reusable selection and terminal checks.
- [Runtime configuration](../operations/runtime-configuration.md) — state, logs and path ownership.
- [Metadata guide](../metadata/index.md) — add more stages after the tutorial contract is clear.
