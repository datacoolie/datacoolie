---
title: Getting started with DataCoolie
description: Install DataCoolie, run a verified local pipeline, then adapt it to your data and add a dependent stage.
---

# Getting started

This section takes you from an isolated Python environment to a repeatable
local dataflow. The pages use one downloadable project so that the first run,
the next incremental run and the multi-stage example share the same metadata
contract.

## Recommended path

| Step | Goal | Page |
|---|---|---|
| 1 | Create a virtual environment and verify the interpreter | [Installation](installation.md) |
| 2 | Run the typed orders fixture with Polars | [Quickstart · Polars](quickstart-polars.md) |
| 3 | Run the same metadata with a local Delta-enabled Spark session | [Quickstart · Spark](quickstart-spark.md) |
| 4 | Choose a safe full-refresh or incremental adaptation | [Use your own data](use-your-own-data.md) |
| 5 | Continue the orders flow into a dependent Silver stage | [Multi-stage dataflow](multi-stage-dataflow.md) |

Start with Polars when you want the shortest local feedback loop. Use Spark
when Spark is your target runtime. Run each engine in a fresh directory: state
and Delta schemas belong to the engine-specific workspace.

## The canonical project

The [getting-started project](../../examples/index.md#getting-started-project)
contains the complete metadata, schema hints, CSV fixtures and local runners.
Its archive is the reproducible handoff; the snippets on these pages explain
the important fields without creating a second copy of a complete runner.

After extracting the archive, the project has this shape:

```text
getting-started/
├── data/input/orders/orders.csv
├── data/input/customers/customers.csv
├── metadata/
├── runners/local/run_polars.py
├── runners/local/run_spark.py
└── datacoolie.yml
```

The runners accept `orders`, `customers` and `multi-stage` lessons. They keep
runtime state and logs under `.runtime/` and write tutorial output under
`data/output/`. Do not point them at an existing production directory.

## Which lesson should you choose?

- [Quickstart · Polars](quickstart-polars.md) proves the first run, a no-change
  rerun and an appended row with the smallest local dependency set.
- [Quickstart · Spark](quickstart-spark.md) uses the same typed metadata while
  making the JVM, Delta artifact and Spark session boundary explicit.
- [Use your own data](use-your-own-data.md) separates a customer full refresh
  from an orders incremental flow. Renaming an input file is not enough: keys,
  transforms, watermark and destination identity are part of the contract.
- [Multi-stage dataflow](multi-stage-dataflow.md) runs Bronze and Silver as
  separate calls with an output barrier. It teaches partitioned detail rows,
  not an aggregate model or schema migration.

For metadata fields, use the [metadata guide](../metadata/index.md). For
selection, failure handling and runtime roots, use [Run a stage](../operations/run-stage.md)
and [Runtime configuration](../operations/runtime-configuration.md). For a
managed host, continue to the [platform guides](../platforms/index.md) after
the local contract works.

## Agent checklist

An automated runner should record the project root and package identity, select
the expected flow names, pass an explicit state root, check the terminal
result, and read the Delta output independently. A zero exit code from a
process that only prints an `ExecutionResult` is not an output assertion.
