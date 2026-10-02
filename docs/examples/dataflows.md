---
title: Dataflow examples
description: Focused DataCoolie metadata, inline SQL, SQL-file and artifact-relative dataflow examples.
---

# Dataflow examples

Each focused dataflow demonstrates one primary behavior. The framework accepts
inline SQL or a file reference; preparation resolves the reference before the
reader is built, while metadata logging keeps the original `source.query`
value.

The extracted project recipes require Python 3.11+. Their pinned `0.2.0`
profiles and the matching source-wheel handoff are documented in the
[installation guide](../guide/getting-started/installation.md).
When using that preview/source-wheel handoff, apply the profile named by each
recipe to the matching wheel for a minimal install. Artifact additionally
needs `polars-sql` (and its SQLGlot dependency); the guide's generic
`cli,polars-delta` profile already supplies Polars for Function and Transform.

## Artifact project fixture {#artifact-project-fixture}

The fixture below is intentionally small:

```text
artifact/
├── datacoolie.yml
├── metadata/
│   ├── connections.json
│   ├── schema_hints.json
│   └── dataflows/orders_query.json
└── queries/orders.sql
```

Open **Artifact SQL project** ([project-files](index.md#artifact-project) ·
[download](downloads/artifact.zip)). The dataflow is
**metadata/dataflows/orders_query.json**
([source](source/projects/artifact/metadata/dataflows/orders_query.json.md) ·
[raw](files/projects/artifact/metadata/dataflows/orders_query.json)), and the
companion query is **queries/orders.sql**
([source](source/projects/artifact/queries/orders.sql.md) ·
[raw](files/projects/artifact/queries/orders.sql)).

### SQL file resolution {#sql-file-resolution}

The metadata uses `artifact:/queries/orders.sql`, so the SQL file is resolved
relative to the artifact root. A relative reference such as
`queries/orders.sql` can instead be resolved through one or more explicit SQL
roots. The framework does not reserve a fixed `sql/` folder.

The project runner registers `orders` and `order_categories` as Polars
relations before calling the Driver. The SQL file joins the two qualified
relations. This is the important boundary for qualified SQL: table
registration is project/engine code, while query-file resolution is framework
preparation.

### Artifact project recipe {#artifact-project-recipe}

For a complete extracted run, use Python 3.11+ with the qualified-SQL profile,
then change to the extracted `artifact/` root (the directory
containing `metadata/`, `queries/` and `runners/`):

```bash
python -m pip install "datacoolie[polars-sql]==0.2.0"
python runners/dev/run.py --state-base-path .runtime
```

The project runner reports one succeeded dataflow and writes
`data/output/orders/orders.parquet` plus runtime records under `.runtime/`.
Read the business result independently:

```python
import polars as pl

rows = (
    pl.read_parquet("data/output/orders/orders.parquet")
    .select("order_id", "amount", "category_group")
    .sort("order_id")
)
assert rows.rows() == [
    (1, 19.99, "physical"),
    (2, 29.0, "digital"),
    (3, 5.5, "physical"),
]
```

The destination uses overwrite, so rerunning the command keeps three business
rows. To adapt the SQL, change `queries/orders.sql` and register every new
relation in `runners/dev/run.py`; changing the reference CSV in `data/input/`
alone does not change this in-memory relation example. To reset without a
destructive cleanup command, extract a fresh ZIP copy into another directory;
generated output and `.runtime/` belong to each extracted project.

## Function project {#function-project}

### Function project recipe {#function-project-recipe}

Open **Function project** ([project-files](index.md#function-project) ·
[download](downloads/function.zip)). Its metadata is
**metadata/dataflows/orders_function.json**
([source](source/projects/function/metadata/dataflows/orders_function.json.md) ·
[raw](files/projects/function/metadata/dataflows/orders_function.json)) and its
function package is **functions/sources.py**
([source](source/projects/function/functions/sources.py.md) ·
[raw](files/projects/function/functions/sources.py)).
The `python_function` path is metadata, while packaging/import setup remains
project runner code. Extract the ZIP and change to its `function/` root:

```bash
python -m pip install "datacoolie[polars]==0.2.0"
python runners/dev/run.py --state-base-path .runtime
```

`functions/sources.py` returns three rows without external input. The runner
keeps the `functions` prefix explicit and writes
`data/output/orders/orders.parquet`:

```python
import polars as pl

rows = (
    pl.read_parquet("data/output/orders/orders.parquet")
    .select("order_id", "amount", "category")
    .sort("order_id")
)
assert rows.rows() == [
    (1, 19.99, "hardware"),
    (2, 29.0, "software"),
    (3, 5.5, "hardware"),
]
```

Rerunning keeps the three-row overwrite result. To adapt the function, edit
`functions/sources.py` and the `python_function` value in the dataflow; keep
`functions/__init__.py` and install any function dependencies in the execution
environment. For the automatic packaging check, add the `cli` extra and run
`python -m pip install "datacoolie[cli,polars]==0.2.0"`, then
`dc --project-dir . build --format json` from the extracted root. A fresh
extraction resets generated output and `.runtime/`.

The archive also includes `functions/range_source.py`, a separate
[custom-reader extension fixture](../extensions/writing-a-source.md#testing).
The CLI packages it with the function root, but this project's metadata selects
`functions.sources.load_orders`; the normal Function recipe does not execute
that custom reader.

## One focused transform project {#transform-project}

The **Transform project** ([project-files](index.md#transform-project) ·
[download](downloads/transform.zip)) keeps one dataflow per feature: it
trims and lowercases `category`, projects the three business columns, and
overwrites a Parquet destination. Its runner creates a
tiny synthetic CSV only when the input is absent, so both the project archive and a
CLI-built environment run without a checked-in runtime directory.

### Transform project recipe {#transform-project-recipe}

Extract the ZIP and change to its `transform/` root. Install the Polars
profile, then run the project-owned runner:

```bash
python -m pip install "datacoolie[polars]==0.2.0"
python runners/dev/run.py --state-base-path .runtime
```

The runner creates the three-row CSV when needed. The output keeps exactly the
three business columns and normalizes `category`:

```python
import polars as pl

rows = pl.read_parquet("data/output/orders/orders.parquet").sort("order_id")
assert rows.columns[:3] == ["order_id", "category", "amount"]
assert rows.select("category").to_series().to_list() == [
    "hardware",
    "software",
    "hardware",
]
```

Rerunning the overwrite flow keeps three rows. To adapt it, edit the source
fixture or `metadata/dataflows/orders_clean.json` and keep `select_columns`
aligned with the columns produced by the value rules. Extract a fresh copy to
reset the generated `data/output/` and `.runtime/` directories.

## Focused metadata references {#focused-metadata-references}

The catalog owns the complete metadata inventory. The focused source files are
**inline_sql.json** ([source](source/dataflows/inline_sql.json.md) ·
[raw](files/dataflows/inline_sql.json)), **sql_file.json**
([source](source/dataflows/sql_file.json.md) ·
[raw](files/dataflows/sql_file.json)), **format_connections.json**
([source](source/dataflows/format_connections.json.md) ·
[raw](files/dataflows/format_connections.json)), **transform_patterns.json**
([source](source/dataflows/transform_patterns.json.md) ·
[raw](files/dataflows/transform_patterns.json)) and **load_strategies.json**
([source](source/dataflows/load_strategies.json.md) ·
[raw](files/dataflows/load_strategies.json)).

These compact files are authoring references rather than one combined runnable
pipeline. Keep each production dataflow focused on one behavior, then validate
the complete project with the CLI before a runner executes it.

## Validate and build without execution

```bash
dc --project-dir docs/examples/files/projects/artifact validate --only config --only metadata --format json
dc --project-dir docs/examples/files/projects/artifact build --dry-run --format json
```

These commands validate the project and show the build digest. They do not run a
Driver, read business data or write runtime state.

## Authoring patterns

| Pattern | Use when | Reference |
|---|---|---|
| Inline SQL | The query is short and belongs with the dataflow metadata | [Source patterns](../guide/metadata/source-patterns.md) |
| SQL file | The query is medium/complex, reviewed as code or shared by a project | [Query preparation guide](../guide/operations/runtime-configuration.md#query-preparation) |
| Qualified Polars SQL | A runner registers tables before calling the Driver | [Polars SQL guide](../reference/concepts/engines.md#qualified-sql-relations-in-polars) |
| Python function source | Business logic needs a packaged callable | [Python functions contract](../guide/metadata/source-patterns.md#python-function-source) |

Function packages are prepared before Driver startup. The dataflow metadata
declares the import path; the runner only supplies the fixed allowed prefix and
does not discover or package source files at runtime.
