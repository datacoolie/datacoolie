---
title: Get Started with DataCoolie for Python ETL
description: Install DataCoolie, choose your engine, and run your first Polars or Spark ETL pipeline. Three paths from zero to a working dataflow in minutes.
---

# Getting started

This section takes you from zero to a working DataCoolie pipeline. You will
install the framework, run a pre-built quickstart, and then move on to building
your own dataflows with real data.

**What you will learn:**

- How to install DataCoolie with the engine and format extras you need
- How to run a complete ETL pipeline on your laptop in under 5 minutes
- How to swap sample data for your own files and iterate
- How the metadata model drives every step — so you write less code

No prior ETL framework experience is required. If you are comfortable
installing Python packages and running scripts, you have everything you need.

For most new users, the smoothest path is:

1. **[Installation](installation.md)** — install the smallest useful engine + table-format extras.
2. **[Quickstart · Polars](quickstart-polars.md)** — laptop-only, no Docker, no JVM. Fastest way to see a pipeline run.
3. **[Use your own data after the quickstart](use-your-own-data.md)** — keep the same runner and swap the sample input for your own files.
4. **[Your first dataflow](first-dataflow.md)** — move from one stage to an ordered bronze→silver flow.

Choose **[Quickstart · Spark](quickstart-spark.md)** instead of Polars only if Spark is already your target runtime or you want early parity with Fabric, Databricks, or another Spark-first environment.

## Choose your route

| Situation | Start here | Then go to |
|---|---|---|
| I am new and want the first successful run | [Installation](installation.md) | [Quickstart · Polars](quickstart-polars.md) |
| I already know my runtime will be Spark | [Installation](installation.md) | [Quickstart · Spark](quickstart-spark.md) |
| The sample worked and now I need my own files | [Use your own data after the quickstart](use-your-own-data.md) | [Metadata guide for new users](../how-to/metadata-guide/index.md) |
| I want the deeper workflow model before building | [Concepts](../concepts/index.md) | [Metadata guide](../how-to/metadata-guide/index.md) or the quickstarts |
| I need multi-stage orchestration | [Your first dataflow](first-dataflow.md) | [How-to guides](../how-to/index.md) |

!!! tip "Audience"
    This documentation is for both hands-on builders and readers who mainly want
    to understand the workflow. The getting-started pages are written for new
    data engineers, analytics engineers, and adjacent backend teams who want a
    concrete starting point before learning the full model.

    If you do **not** need to run code yet, start with
    [Concepts](../concepts/architecture.md), especially
    [Architecture](../concepts/architecture.md),
    [Metadata model](../concepts/metadata-model.md), and
    [Orchestration](../concepts/orchestration.md).

    If you do want to run the examples, basic Python and command-line familiarity
    will help, and unfamiliar framework terms are defined or linked on first use.
