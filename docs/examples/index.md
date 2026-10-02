---
title: Examples
description: DataCoolie example projects, configuration, dataflows, runners, operations and plugins.
---

# DataCoolie examples

This library is organized by the folder that owns each example. If you are new
to DataCoolie, start with [Getting started](../guide/getting-started/index.md),
then return here for a focused project recipe; otherwise jump directly to the
feature you need. Recipes link to the maintained project runner and explain
the extracted-project working directory, dependencies, expected output and
adaptation point.

- [Projects](#projects): complete runnable project sources and their build layout.
- [Configuration](#configuration): Driver/provider construction and run options.
- [Dataflows](#dataflows): focused metadata, SQL and transformation patterns.
- [Runners](#runners): host-specific entrypoints that call the Driver.
- [Operations](#operations): replay, recovery and maintenance wrappers.
- [Plugins](#plugins): extension source and packaging examples.

The table actions use one vocabulary. The action describes what the link does;
it does not describe the file's business meaning:

| Action | Meaning | Scope |
|---|---|---|
| **source** | Opens a generated, readable source projection in the docs. Notebook outputs are omitted. | One file |
| **raw** | Opens the exact canonical file content/bytes. It may render inline; it is not a project archive. | One file |
| **download** | Downloads the complete project archive, currently a `.zip`. | One project |
| **project-files** | Jumps to the complete project section in this catalog. It does not download anything. | One project |
| **guide** | Opens usage, adaptation and verification instructions. | Feature or project |

Use **raw** when a tool needs the original bytes and **source** when a person
or agent needs the readable projection. A browser saving a raw response is a
transport detail, not a separate project download. Standalone files do not
have a download action; complete projects use **download** once at the project
root.

Project runners are executable recipes. Focused configuration and metadata
files are snippets or authoring templates unless their page labels a runnable
local contract fixture, such as `provider_fixtures.py`. Managed-host runners
are host contracts. The [WWI medallion walkthrough](wwi-medallion-multicloud.md)
is a case study; it is not a local project fixture.

The Markdown in this page is the authored inventory. During a docs build,
folder trees and the first column of each marked table are rendered with
visible depth prefixes. The links remain ordinary Markdown links so agents and
offline readers can follow them without JavaScript.
Section anchors are stable navigation targets; file identity comes from the
canonical path, not from a displayed tree prefix.

## Projects {#projects}

Complete projects preserve their authored internal layout. Projects contain metadata and any input or function/query companions needed
by the sample. Most include an environment runner; Platform smoke instead
uses the separate canonical host runners linked below. Runtime output folders are intentionally not part of the
public inventory.

<!-- dc-examples-section: projects -->
<!-- dc-generated-examples-tree: projects -->

| File/Folder | Description | Links |
|---|---|---|
| artifact/ | Artifact-relative SQL project with a Polars runner. | [guide](dataflows.md#artifact-project-recipe) · [project-files](#artifact-project) · [download](downloads/artifact.zip) |
| function/ | Python function source project with automatic package handling. | [guide](dataflows.md#function-project-recipe) · [project-files](#function-project) · [download](downloads/function.zip) |
| getting-started/ | Typed orders onboarding project with Polars/Spark runners, customer refresh and Bronze-to-Silver continuation. | [guide](runners.md#getting-started-project) · [project-files](#getting-started-project) · [download](downloads/getting-started.zip) |
| incremental/ | File-source project that persists an integer watermark. | [guide](operations.md#incremental-project-recipe) · [project-files](#incremental-project) · [download](downloads/incremental.zip) |
| platform-smoke/ | Three-row CSV-to-Delta fixture with local/cloud overlays and separate canonical runners. | [guide](runners.md#platform-smoke) · [project-files](#platform-smoke-project) · [download](downloads/platform-smoke.zip) |
| transform/ | Small focused transformer project with a Parquet destination. | [guide](dataflows.md#transform-project-recipe) · [project-files](#transform-project) · [download](downloads/transform.zip) |
<!-- /dc-examples-section -->

### Getting-started project {#getting-started-project}

This project is the canonical source for the [getting-started
guides](../guide/getting-started/index.md). It contains a typed orders input,
a separate customers full-refresh branch and an orders Bronze-to-Silver
partitioned-detail branch. Download **Getting-started project**
([download](downloads/getting-started.zip)) for a complete checkout.

Use `runners/local/run_polars.py` for the shortest local path or
`runners/local/run_spark.py` for a local Delta-enabled Spark session. The
project-owned guards check input preconditions, intended flow selection,
terminal status, persisted state and Delta output. Runtime `.runtime/` and
`data/output/` directories are excluded from the public inventory.

<!-- dc-examples-section: projects/getting-started -->
<!-- dc-generated-examples-tree: projects/getting-started -->

| File/Folder | Description | Links |
|---|---|---|
| datacoolie.yml | Local environment and metadata build contract. | [source](source/projects/getting-started/datacoolie.yml.md) · [raw](files/projects/getting-started/datacoolie.yml) |
| data/ | Input area; generated runtime output is excluded. | — |
| data/input/ | Source fixtures used by the three lessons. | — |
| data/input/customers/ | Customer full-refresh source folder. | — |
| data/input/customers/customers.csv | Two-row customer full-refresh fixture. | [source](source/projects/getting-started/data/input/customers/customers.csv.md) · [raw](files/projects/getting-started/data/input/customers/customers.csv) |
| data/input/orders/ | Orders incremental source folder. | — |
| data/input/orders/orders.csv | Four-row orders fixture with one duplicate order ID. | [source](source/projects/getting-started/data/input/orders/orders.csv.md) · [raw](files/projects/getting-started/data/input/orders/orders.csv) |
| metadata/ | FileProvider metadata root. | — |
| metadata/connections.json | Input, Bronze, Silver and customer destination roots. | [source](source/projects/getting-started/metadata/connections.json.md) · [raw](files/projects/getting-started/metadata/connections.json) |
| metadata/dataflows.json | Orders, customers and dependent Silver flow definitions. | [source](source/projects/getting-started/metadata/dataflows.json.md) · [raw](files/projects/getting-started/metadata/dataflows.json) |
| metadata/schema_hints.json | Signed IDs, Decimal amount, timestamp and customer type hints. | [source](source/projects/getting-started/metadata/schema_hints.json.md) · [raw](files/projects/getting-started/metadata/schema_hints.json) |
| runners/ | Project-owned execution entrypoints. | — |
| runners/local/ | Local Polars and Spark runner environment. | — |
| runners/local/checks.py | Tutorial-owned input, status, state and output guards. | [source](source/projects/getting-started/runners/local/checks.py.md) · [raw](files/projects/getting-started/runners/local/checks.py) |
| runners/local/run_polars.py | Polars runner for all three lessons. | [source](source/projects/getting-started/runners/local/run_polars.py.md) · [raw](files/projects/getting-started/runners/local/run_polars.py) |
| runners/local/run_spark.py | Spark runner with local Delta session lifecycle. | [source](source/projects/getting-started/runners/local/run_spark.py.md) · [raw](files/projects/getting-started/runners/local/run_spark.py) |
<!-- /dc-examples-section -->

### Artifact project {#artifact-project}

This project demonstrates a FileProvider artifact root, artifact-relative SQL
and qualified Polars relations. Open **Artifact SQL project** ([download](downloads/artifact.zip))
when a complete checkout is needed. The tree below is generated from the
canonical source.

The project runner is **runners/dev/run.py** ([source](source/projects/artifact/runners/dev/run.py.md) ·
[raw](files/projects/artifact/runners/dev/run.py));
it registers the Polars relations before executing the artifact-relative query.

<!-- dc-examples-section: projects/artifact -->
<!-- dc-generated-examples-tree: projects/artifact -->

| File/Folder | Description | Links |
|---|---|---|
| datacoolie.yml | Project and environment build contract. | [source](source/projects/artifact/datacoolie.yml.md) · [raw](files/projects/artifact/datacoolie.yml) |
| data/ | Reference fixture area kept outside generated runtime output. | — |
| data/input/ | Reference CSV retained with the project; the runner registers its relations in memory. | — |
| data/input/orders.csv | Reference CSV kept with the project; the runner registers its two SQL relations in memory. | [source](source/projects/artifact/data/input/orders.csv.md) · [raw](files/projects/artifact/data/input/orders.csv) |
| metadata/ | FileProvider metadata root. | — |
| metadata/connections.json | Connection definitions for the fixture. | [source](source/projects/artifact/metadata/connections.json.md) · [raw](files/projects/artifact/metadata/connections.json) |
| metadata/dataflows/ | Section-wrapped dataflow metadata. | — |
| metadata/dataflows/orders_query.json | Dataflow using an artifact-relative SQL reference. | [source](source/projects/artifact/metadata/dataflows/orders_query.json.md) · [raw](files/projects/artifact/metadata/dataflows/orders_query.json) |
| metadata/schema_hints.json | Explicit input schema hints. | [source](source/projects/artifact/metadata/schema_hints.json.md) · [raw](files/projects/artifact/metadata/schema_hints.json) |
| queries/ | Project SQL root referenced by metadata. | — |
| queries/orders.sql | Qualified SQL query joined after runner table registration. | [source](source/projects/artifact/queries/orders.sql.md) · [raw](files/projects/artifact/queries/orders.sql) |
| runners/ | Project-owned execution entrypoints. | — |
| runners/dev/ | Development environment runner. | — |
| runners/dev/run.py | Local artifact runner invocation. | [source](source/projects/artifact/runners/dev/run.py.md) · [raw](files/projects/artifact/runners/dev/run.py) |
<!-- /dc-examples-section -->

### Function project {#function-project}

This project demonstrates a Python function source. Its function root contains
`__init__.py`, so the CLI automatic packaging rule creates a ZIP while the
runner keeps the import prefix explicit. Open **Function project**
([download](downloads/function.zip)) when a complete checkout is needed.

The package entrypoint is **functions/__init__.py**
([source](source/projects/function/functions/__init__.py.md) ·
[raw](files/projects/function/functions/__init__.py)).

<!-- dc-examples-section: projects/function -->
<!-- dc-generated-examples-tree: projects/function -->

| File/Folder | Description | Links |
|---|---|---|
| datacoolie.yml | Project and environment build contract. | [source](source/projects/function/datacoolie.yml.md) · [raw](files/projects/function/datacoolie.yml) |
| data/ | Optional input area for the function example. | — |
| data/input/ | Empty input area created for project layout consistency. | — |
| functions/ | Project-owned Python function package root. | — |
| functions/__init__.py | Makes the configured function root importable. | [source](source/projects/function/functions/__init__.py.md) · [raw](files/projects/function/functions/__init__.py) |
| functions/sources.py | Python source callable referenced by metadata. | [source](source/projects/function/functions/sources.py.md) · [raw](files/projects/function/functions/sources.py) |
| functions/range_source.py | Separate custom-reader extension fixture; bundled with this package, but not selected by its dataflow. | [guide](../extensions/writing-a-source.md#testing) · [source](source/projects/function/functions/range_source.py.md) · [raw](files/projects/function/functions/range_source.py) |
| metadata/ | FileProvider metadata root. | — |
| metadata/connections.json | Connection definitions for the fixture. | [source](source/projects/function/metadata/connections.json.md) · [raw](files/projects/function/metadata/connections.json) |
| metadata/dataflows/ | Dataflow metadata selecting the Python function. | — |
| metadata/dataflows/orders_function.json | Dataflow using a packaged Python source. | [source](source/projects/function/metadata/dataflows/orders_function.json.md) · [raw](files/projects/function/metadata/dataflows/orders_function.json) |
| metadata/schema_hints.json | Explicit input schema hints. | [source](source/projects/function/metadata/schema_hints.json.md) · [raw](files/projects/function/metadata/schema_hints.json) |
| runners/ | Project-owned execution entrypoints. | — |
| runners/dev/ | Development environment runner. | — |
| runners/dev/run.py | Local function project runner. | [source](source/projects/function/runners/dev/run.py.md) · [raw](files/projects/function/runners/dev/run.py) |
<!-- /dc-examples-section -->

### Incremental project {#incremental-project}

This project demonstrates a CSV source, append destination and integer
`updated_sequence` watermark. Run **runners/dev/run.py**
([source](source/projects/incremental/runners/dev/run.py.md) ·
[raw](files/projects/incremental/runners/dev/run.py)) once for two rows, run it
again without changing the input to observe a no-change run, append a row with
`updated_sequence=3`, then run it again with the same runtime root. The last
run appends one row and advances the watermark. Open **Incremental project**
([download](downloads/incremental.zip)) for a complete checkout.

<!-- dc-examples-section: projects/incremental -->
<!-- dc-generated-examples-tree: projects/incremental -->

| File/Folder | Description | Links |
|---|---|---|
| datacoolie.yml | Project and environment build contract. | [source](source/projects/incremental/datacoolie.yml.md) · [raw](files/projects/incremental/datacoolie.yml) |
| data/ | Input and output fixture area. Generated output is excluded. | — |
| data/input/ | Input partition containing ordered source data. | — |
| data/input/orders/ | Orders source folder used by the file reader. | — |
| data/input/orders/orders.csv | Source rows with advancing sequence values. | [source](source/projects/incremental/data/input/orders/orders.csv.md) · [raw](files/projects/incremental/data/input/orders/orders.csv) |
| metadata/ | FileProvider metadata root. | — |
| metadata/connections.json | Connection definitions for the fixture. | [source](source/projects/incremental/metadata/connections.json.md) · [raw](files/projects/incremental/metadata/connections.json) |
| metadata/dataflows/ | Dataflow metadata with incremental watermark rules. | — |
| metadata/dataflows/orders_incremental.json | Incremental dataflow and watermark configuration. | [source](source/projects/incremental/metadata/dataflows/orders_incremental.json.md) · [raw](files/projects/incremental/metadata/dataflows/orders_incremental.json) |
| metadata/schema_hints.json | Explicit input schema hints. | [source](source/projects/incremental/metadata/schema_hints.json.md) · [raw](files/projects/incremental/metadata/schema_hints.json) |
| runners/ | Project-owned execution entrypoints. | — |
| runners/dev/ | Development environment runner. | — |
| runners/dev/run.py | Runner used for repeated incremental loads. | [source](source/projects/incremental/runners/dev/run.py.md) · [raw](files/projects/incremental/runners/dev/run.py) |
<!-- /dc-examples-section -->

### Transform project {#transform-project}

This is the smallest built-in transform example: one dataflow, one synthetic
input and one Parquet output. Its metadata focuses on column cleanup and
projection. The project runner creates the input when the extracted project
does not contain it. Open **Transform project**
([download](downloads/transform.zip)) for a complete checkout.

<!-- dc-examples-section: projects/transform -->
<!-- dc-generated-examples-tree: projects/transform -->

| File/Folder | Description | Links |
|---|---|---|
| datacoolie.yml | Project and environment build contract. | [source](source/projects/transform/datacoolie.yml.md) · [raw](files/projects/transform/datacoolie.yml) |
| metadata/ | FileProvider metadata root. | — |
| metadata/connections.json | Connection definitions for the fixture. | [source](source/projects/transform/metadata/connections.json.md) · [raw](files/projects/transform/metadata/connections.json) |
| metadata/dataflows/ | Focused transformer metadata. | — |
| metadata/dataflows/orders_clean.json | Dataflow that cleans and projects order columns. | [source](source/projects/transform/metadata/dataflows/orders_clean.json.md) · [raw](files/projects/transform/metadata/dataflows/orders_clean.json) |
| metadata/schema_hints.json | Explicit input schema hints. | [source](source/projects/transform/metadata/schema_hints.json.md) · [raw](files/projects/transform/metadata/schema_hints.json) |
| runners/ | Project-owned execution entrypoints. | — |
| runners/dev/ | Development environment runner. | — |
| runners/dev/run.py | Local transform project runner. | [source](source/projects/transform/runners/dev/run.py.md) · [raw](files/projects/transform/runners/dev/run.py) |
<!-- /dc-examples-section -->

### Platform smoke project {#platform-smoke-project}

Use this three-row fixture for the [local rehearsal and managed-platform handoff](runners.md#platform-smoke).
The [download](downloads/platform-smoke.zip) includes input and metadata; obtain the
selected runner through its separate raw action. CLI builds do not upload input
or execute notebooks. Cloud variants are setup contracts checked locally.

<!-- dc-examples-section: projects/platform-smoke -->
<!-- dc-generated-examples-tree: projects/platform-smoke -->

| File/Folder | Description | Links |
|---|---|---|
| data/ | Fixture source directory. | — |
| data/input/ | Fixture source directory. | — |
| data/input/orders/ | Fixture source directory. | — |
| data/input/orders/orders.csv | Three known input rows. | [source](source/projects/platform-smoke/data/input/orders/orders.csv.md) · [raw](files/projects/platform-smoke/data/input/orders/orders.csv) |
| datacoolie.yml | Project build/environment configuration. | [source](source/projects/platform-smoke/datacoolie.yml.md) · [raw](files/projects/platform-smoke/datacoolie.yml) |
| metadata/ | Fixture source directory. | — |
| metadata/connections.json | Shared metadata and type hints. | [source](source/projects/platform-smoke/metadata/connections.json.md) · [raw](files/projects/platform-smoke/metadata/connections.json) |
| metadata/dataflows/ | Fixture source directory. | — |
| metadata/dataflows/orders_platform_smoke.json | Shared metadata and type hints. | [source](source/projects/platform-smoke/metadata/dataflows/orders_platform_smoke.json.md) · [raw](files/projects/platform-smoke/metadata/dataflows/orders_platform_smoke.json) |
| metadata/environments/ | Fixture source directory. | — |
| metadata/environments/aws-iceberg.json | Environment-specific connection addressing. | [source](source/projects/platform-smoke/metadata/environments/aws-iceberg.json.md) · [raw](files/projects/platform-smoke/metadata/environments/aws-iceberg.json) |
| metadata/environments/aws.json | Environment-specific connection addressing. | [source](source/projects/platform-smoke/metadata/environments/aws.json.md) · [raw](files/projects/platform-smoke/metadata/environments/aws.json) |
| metadata/environments/databricks.json | Environment-specific connection addressing. | [source](source/projects/platform-smoke/metadata/environments/databricks.json.md) · [raw](files/projects/platform-smoke/metadata/environments/databricks.json) |
| metadata/environments/fabric.json | Environment-specific connection addressing. | [source](source/projects/platform-smoke/metadata/environments/fabric.json.md) · [raw](files/projects/platform-smoke/metadata/environments/fabric.json) |
| metadata/environments/local.json | Environment-specific connection addressing. | [source](source/projects/platform-smoke/metadata/environments/local.json.md) · [raw](files/projects/platform-smoke/metadata/environments/local.json) |
| metadata/schema_hints.json | Shared metadata and type hints. | [source](source/projects/platform-smoke/metadata/schema_hints.json.md) · [raw](files/projects/platform-smoke/metadata/schema_hints.json) |
<!-- /dc-examples-section -->

## Configuration {#configuration}

These focused files show the public configuration boundaries. A runner owns
engine/platform construction and passes metadata, SQL roots, runtime roots and
external run attributes to the Driver.

<!-- dc-examples-section: configuration -->
<!-- dc-generated-examples-tree: configuration -->

| File/Folder | Description | Links |
|---|---|---|
| logging_modes.py | Snapshot and JSON-record batch logging configuration. | [source](source/configuration/logging_modes.py.md) · [raw](files/configuration/logging_modes.py) |
| provider_construction.py | Artifact and explicit provider construction paths. | [source](source/configuration/provider_construction.py.md) · [raw](files/configuration/provider_construction.py) |
| provider_fixtures.py | Standalone database and API provider startup. | [source](source/configuration/provider_fixtures.py.md) · [raw](files/configuration/provider_fixtures.py) |
| run_attributes.py | External correlation values passed to a Driver session. | [source](source/configuration/run_attributes.py.md) · [raw](files/configuration/run_attributes.py) |
| sql_roots.py | Resolution through multiple explicit SQL roots. | [source](source/configuration/sql_roots.py.md) · [raw](files/configuration/sql_roots.py) |
| standalone_file_provider.py | FileProvider construction without a Driver. | [source](source/configuration/standalone_file_provider.py.md) · [raw](files/configuration/standalone_file_provider.py) |
<!-- /dc-examples-section -->

## Dataflows {#dataflows}

Each dataflow sample focuses on one metadata or execution feature. Inline SQL,
SQL files, schema hints, load strategies and transform metadata remain separate
authoring references instead of one large pipeline.

<!-- dc-examples-section: dataflows -->
<!-- dc-generated-examples-tree: dataflows -->

| File/Folder | Description | Links |
|---|---|---|
| format_connections.json | Alternative connection metadata representations. | [source](source/dataflows/format_connections.json.md) · [raw](files/dataflows/format_connections.json) |
| inline_sql.json | Inline SQL kept directly in source metadata. | [source](source/dataflows/inline_sql.json.md) · [raw](files/dataflows/inline_sql.json) |
| load_strategies.json | Append, overwrite and merge load patterns. | [source](source/dataflows/load_strategies.json.md) · [raw](files/dataflows/load_strategies.json) |
| sql/ | SQL files kept next to focused metadata examples. | — |
| sql/orders.sql | Qualified SQL used by the SQL-file example. | [source](source/dataflows/sql/orders.sql.md) · [raw](files/dataflows/sql/orders.sql) |
| sql_file.json | File-backed SQL query metadata. | [source](source/dataflows/sql_file.json.md) · [raw](files/dataflows/sql_file.json) |
| transform_patterns.json | Column and row transformation patterns. | [source](source/dataflows/transform_patterns.json.md) · [raw](files/dataflows/transform_patterns.json) |
<!-- /dc-examples-section -->

## Runners {#runners}

Runners are project code, not a universal dc run command. They register engine
relations, adapt host credentials and choose Driver options for a particular
environment.

<!-- dc-examples-section: runners -->
<!-- dc-generated-examples-tree: runners -->

| File/Folder | Description | Links |
|---|---|---|
| aws/ | AWS Glue and S3 runner contracts. | — |
| aws/run_glue_spark.py | AWS Glue Spark runner. | [source](source/runners/aws/run_glue_spark.py.md) · [raw](files/runners/aws/run_glue_spark.py) |
| aws/run_polars_s3.py | Polars runner with S3 roots. | [source](source/runners/aws/run_polars_s3.py.md) · [raw](files/runners/aws/run_polars_s3.py) |
| databricks/ | Databricks notebook and SDK runner contracts. | — |
| databricks/maintenance_spark.ipynb | Databricks Spark maintenance notebook. | [source](source/runners/databricks/maintenance_spark.ipynb.md) · [raw](files/runners/databricks/maintenance_spark.ipynb) |
| databricks/replay_spark.ipynb | Databricks Spark replay notebook. | [source](source/runners/databricks/replay_spark.ipynb.md) · [raw](files/runners/databricks/replay_spark.ipynb) |
| databricks/run_polars_sdk.py | Databricks Polars SDK runner. | [source](source/runners/databricks/run_polars_sdk.py.md) · [raw](files/runners/databricks/run_polars_sdk.py) |
| databricks/run_spark.ipynb | Databricks Spark runner notebook. | [source](source/runners/databricks/run_spark.ipynb.md) · [raw](files/runners/databricks/run_spark.ipynb) |
| fabric/ | Fabric Spark and Polars runner contracts. | — |
| fabric/run_polars.ipynb | Fabric Polars runner notebook. | [source](source/runners/fabric/run_polars.ipynb.md) · [raw](files/runners/fabric/run_polars.ipynb) |
| fabric/run_polars_azure_sdk.py | Fabric Polars Azure SDK runner. | [source](source/runners/fabric/run_polars_azure_sdk.py.md) · [raw](files/runners/fabric/run_polars_azure_sdk.py) |
| fabric/run_spark.ipynb | Fabric Spark runner. | [source](source/runners/fabric/run_spark.ipynb.md) · [raw](files/runners/fabric/run_spark.ipynb) |
| local/ | Local Python, Spark, replay and maintenance runners. | — |
| local/maintenance.py | Local Polars maintenance runner. | [source](source/runners/local/maintenance.py.md) · [raw](files/runners/local/maintenance.py) |
| local/replay.py | Local Polars replay runner. | [source](source/runners/local/replay.py.md) · [raw](files/runners/local/replay.py) |
| local/run.py | Configurable local Polars runner. | [source](source/runners/local/run.py.md) · [raw](files/runners/local/run.py) |
| local/run_artifact_minimal.py | Minimal artifact-root runner. | [source](source/runners/local/run_artifact_minimal.py.md) · [raw](files/runners/local/run_artifact_minimal.py) |
| local/run_spark.py | Local Spark runner. | [source](source/runners/local/run_spark.py.md) · [raw](files/runners/local/run_spark.py) |
<!-- /dc-examples-section -->

## Operations {#operations}

Operational wrappers demonstrate replay, interrupted-session recovery, maintenance
confirmation and structured logging without changing the Driver contract.

<!-- dc-examples-section: operations -->
<!-- dc-generated-examples-tree: operations -->

| File/Folder | Description | Links |
|---|---|---|
| replay_recovery.py | Repeatable replay wrapper with explicit watermark-save confirmation. | [source](source/operations/replay_recovery.py.md) · [raw](files/operations/replay_recovery.py) |
<!-- /dc-examples-section -->

## Plugins {#plugins}

Plugin samples show the smallest extension boundary and its packaging metadata.
Follow the [transformer tutorial](../extensions/transformer-tutorial.md) to
install this package and verify its output through a Driver. The
[transformer guide](../extensions/writing-a-transformer.md) explains adaptation
and pipeline ordering.

<!-- dc-examples-section: plugins -->
<!-- dc-generated-examples-tree: plugins -->

| File/Folder | Description | Links |
|---|---|---|
| pii_masker.py | Example transformer plugin that masks sensitive values. | [source](source/plugins/pii_masker.py.md) · [raw](files/plugins/pii_masker.py) |
| pyproject.toml | Build metadata, framework dependency and transformer entry point. | [source](source/plugins/pyproject.toml.md) · [raw](files/plugins/pyproject.toml) |
<!-- /dc-examples-section -->

## Using the catalog with the CLI {#cli}

The examples are documentation sources, not a project template. To inspect or
build one of the complete projects locally:

Follow the [CLI preparation walkthrough](../guide/cli/quickstart.md) when you
want a clean download-and-verify sequence. The commands below are the shortest
checkout-based equivalent:

~~~bash
dc --project-dir docs/examples/files/projects/artifact validate --format json
dc --project-dir docs/examples/files/projects/artifact build --dry-run --format json
~~~

The equivalent executable names are dc and datacoolie. The [project and CLI
guide](../guide/cli/project.md) explains adaptation, runtime roots and runner
ownership.
