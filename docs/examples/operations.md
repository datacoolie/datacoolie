---
title: Operations examples
description: DataCoolie replay, maintenance, sharding, logging and failure-handling examples.
---

# Operations examples

Operational samples use the same Driver lifecycle as a normal run. Keep one
job identity for a Driver session, pass explicit external `run_attributes`, and
let the Driver decide result status and teardown ownership.

## Incremental project recipe {#incremental-project-recipe}

The [Incremental project](index.md#incremental-project) is the smallest local
stateful recipe. It requires Python 3.11+ and
`datacoolie[polars]==0.2.0`; the [installation guide](../guide/getting-started/installation.md)
documents the matching source-wheel handoff when that release is not
published. For that preview/source-wheel handoff, apply the `polars` extra to
the matching wheel for a minimal install; the guide's generic
`cli,polars-delta` profile already supplies Polars for this recipe. Extract
[incremental.zip](downloads/incremental.zip),
change to the extracted `incremental/` root, and run the project-owned runner:

```bash
python -m pip install "datacoolie[polars]==0.2.0"
python runners/dev/run.py --state-base-path .runtime
```

The archive starts with two CSV rows (`updated_sequence` 1 and 2). The first
run writes two business rows under `data/output/orders/` and saves the source
watermark below `.runtime/watermarks/`. Run the same command again without
changing `data/input/orders/orders.csv`; the persisted watermark makes this a
successful no-change run and the output remains two business rows.

Append one later source row, then run the same command a third time:

```python
from pathlib import Path

source = Path("data/input/orders/orders.csv")
with source.open("a", encoding="utf-8") as handle:
    handle.write("3,5.50,3\n")
```

Read the append output and watermark independently:

```python
import json
from pathlib import Path

import polars as pl

files = sorted(Path("data/output/orders").glob("*.parquet"))
rows = pl.concat([pl.read_parquet(path) for path in files]).select(
    "order_id", "amount", "updated_sequence"
).sort("order_id")
assert rows.rows() == [(1, 19.99, 1), (2, 29.0, 2), (3, 5.5, 3)]
watermark = next(Path(".runtime/watermarks").rglob("watermark_value.json"))
assert json.loads(watermark.read_text(encoding="utf-8"))[
    "updated_sequence"
] == 3
```

This project uses an append destination, so rerunning after a failed write can
repeat committed business rows. To adapt it, append source rows with a value
greater than the stored watermark and update `watermark_columns` and schema
hints together when the key changes. To reset a trial, extract a fresh copy of
the ZIP into a new directory; generated output and `.runtime/` belong to each
extracted copy and are not removed by the recipe.

## Replay {#replay}

**runners/local/replay.py** ([source](source/runners/local/replay.py.md) ·
[raw](files/runners/local/replay.py)) demonstrates a bounded
`[start, end)` interval, optional chunks and an explicit confirmation before a
watermark is saved. The managed-host equivalent is
**runners/databricks/replay_spark.ipynb**
([source](source/runners/databricks/replay_spark.ipynb.md) ·
[raw](files/runners/databricks/replay_spark.ipynb)).

The **Incremental project** ([project-files](index.md#incremental-project) ·
[download](downloads/incremental.zip)) provides the input snapshot used by this
replay recipe. The ZIP contains the project runner but not the operation
wrapper. Obtain the separate raw **runners/local/replay.py** file
([source](source/runners/local/replay.py.md) ·
[raw](files/runners/local/replay.py)) and save it inside the extracted
`incremental/` project root as `replay.py`.

Start this replay lesson from a fresh extraction of `incremental.zip`. The
incremental recipe above deliberately leaves row 3, output files and a saved
watermark in place; reusing that workspace changes the replay input and result
counts.

```bash
python replay.py --metadata-path metadata --watermark-base-path .runtime/watermarks --log-base-path .runtime/logs --working-directory . --start 1 --end 4
```

Run that command from the extracted `incremental/` root. The runner changes to
`--working-directory` before constructing `FileProvider`, so `metadata`, the
watermark root and the log root all resolve inside that project. Relative paths
from a repository checkout such as `docs/examples/files/...` do not remain
valid after that directory change. On the fresh two-row snapshot this writes
two business rows; because `--save-watermark` is absent, it does not persist a
replay watermark.

### Restartable replay and interrupted sessions

**operations/replay_recovery.py** ([source](source/operations/replay_recovery.py.md) ·
[raw](files/operations/replay_recovery.py)) is a small
project-owned wrapper around the same public replay API. Run the requested
range again with the same input snapshot and watermark root after a failed or
interrupted session. Obtain this raw wrapper separately and save it as
`replay_recovery.py` in the extracted project root:

Use a fresh extracted project for the first recovery attempt, then repeat the
same command in that workspace when checking retry behavior. Every repeat runs
the requested chunks again.

```bash
python replay_recovery.py --metadata-path metadata --watermark-base-path .runtime/watermarks --log-base-path .runtime/logs --working-directory . --start 1 --end 4 --chunk-interval-json '{"step": 2}' --save-watermark --confirm-save-watermark --job-id replay-attempt-2
```

With the two-row snapshot and `step=2`, the first recovery run writes the two
requested business rows and saves `updated_sequence=2`. Every requested chunk
runs again on the next invocation; an append destination can therefore contain
two copies of each row after the repeat. `save_watermark` persists the reader's
observations after successful writes but is not a replay checkpoint. The
destination write and watermark write are separate operations, so a hard
termination after the destination commit can still leave output to be written
again. The framework does not promise exactly-once delivery for an append
target; use a supported keyed strategy such as `merge_upsert` when retries must
reconcile rows. Keep each attempt's `job_id` distinct and retain its logs for
diagnosis.

The runner does not inject failures. Failure-boundary checks belong in an
isolated test process so a public project script never carries a production
fault-injection switch.

## Maintenance {#maintenance}

**runners/local/maintenance.py** ([source](source/runners/local/maintenance.py.md) ·
[raw](files/runners/local/maintenance.py)) requires an
explicit `--confirm-maintenance` flag and refuses a no-op request. Maintenance
deduplicates by physical destination before dispatching compact/cleanup work.

## Sharding and failure behavior

`--job-num` and `--job-index` identify a disjoint shard of the selected
dataflows. The caller owns any external stage barrier: wait for every job before
starting a dependent stage. A failed result is returned and a runner should exit
non-zero; preparation failures are reported before business reads are retried.

## Logging source files

The persisted structured records are full JSON objects. Snapshot mode rewrites a
stable job/dataflow projection; batch mode emits valid JSON Lines records in
`.json` files. The console can be colored for modern terminals, but color is
presentation-only.

Use **configuration/logging_modes.py**
([source](source/configuration/logging_modes.py.md) ·
[raw](files/configuration/logging_modes.py)) for the LogConfig sample, and
**operations/replay_recovery.py**
([source](source/operations/replay_recovery.py.md) ·
[raw](files/operations/replay_recovery.py)) for its wrapper source. The
normative contracts are [logging](../guide/operations/logging.md) and
[logging reference](../reference/concepts/logging.md#configuration).

Do not call a logger's private flush method from a runner. Configure the public
`LogConfig`, close the Driver in a context manager or `finally`, and preserve the
structured records for diagnostics.
