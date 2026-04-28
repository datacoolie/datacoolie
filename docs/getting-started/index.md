# Getting started

Three paths depending on how much you've already set up:

1. **[Installation](installation.md)** — pick the right `pip install` extras for your engine and cloud.
2. **[Quickstart · Polars](quickstart-polars.md)** — laptop-only, no Docker, no JVM. Fastest way to see a pipeline run.
3. **[Quickstart · Spark](quickstart-spark.md)** — local `SparkSession`, Delta Lake, single machine.
4. **[Your first dataflow](first-dataflow.md)** — end-to-end tutorial: define metadata, run bronze→silver, write a second stage.

!!! tip "Audience"
    This documentation is for both hands-on builders and readers who mainly want
    to understand the workflow. The quickstarts and installation steps are the
    most code-heavy pages. If you do **not** need to run code, start with
    [Concepts](../concepts/architecture.md), especially
    [Architecture](../concepts/architecture.md),
    [Metadata model](../concepts/metadata-model.md), and
    [Orchestration](../concepts/orchestration.md).

    If you do want to run the examples, basic Python and command-line familiarity
    will help, and any unfamiliar framework terms are defined or linked on first
    use.
