# Polars Qualified SQL Registration

## Scope

- Read when a Polars runner executes Delta or Iceberg metadata `source.query` against lakehouse
  tables.
- This reference owns the agent decision boundary, same-process bootstrap gate, and evidence
  needed to adapt a project runner. The public [Polars qualified-SQL reference](https://datacoolie.github.io/datacoolie/reference/concepts/engines/#qualified-sql-relations-in-polars),
  [source patterns guide](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/),
  [dataflows guide](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/), and
  [dataflow examples](https://datacoolie.github.io/datacoolie/examples/dataflows/) own API
  parameters, logical-name rules, dependency profiles, and project recipe details.

## Decision boundary

Use this route only when all of these are true:

1. The selected engine is Polars.
2. The source is Delta or Iceberg.
3. A normal metadata `source.query` must resolve lakehouse table relations through Polars SQL.

Keep direct whole-table reads on the normal `source.table` or path route. Do not add relation
registration or SQLGlot to a DataFrame/LazyFrame workflow that does not need indexed SQL. Database
`source.query` is sent to the source database and is not this feature.

## Framework-first runner boundary

Use the framework-first source order from `framework-boundary.md`: direct source addressing, then
one bounded query, then a narrow metadata-addressed function only when the first two cannot express
verified behavior. A qualified Delta/Iceberg query remains `source.query`; relation discovery is
engine bootstrap, not a Python-function fallback.

Registration belongs in project-owned bootstrap code that uses the same active Polars engine as the
Driver. Keep the sequence explicit:

```text
platform/catalog bootstrap
  -> construct PolarsEngine
  -> register the selected Delta/Iceberg relations on that engine
  -> construct DataCoolieDriver with the same engine
  -> call the selected driver operation
```

Do not move registration into metadata, a source function, or a setup subprocess. A subprocess
cannot register tables into the Driver's process-local SQL context. Keep physical roots, catalog
properties, namespace mappings, and credentials in the project/environment bootstrap selected by
the public recipe; do not invent generic metadata or runtime-selector fields.

## Public implementation route

Read the public [qualified-SQL reference](https://datacoolie.github.io/datacoolie/reference/concepts/engines/#qualified-sql-relations-in-polars)
for Delta path discovery, Iceberg catalog/path mode, logical names, suffix qualification, filters,
lazy binding, error handling, and optional dependency profiles. Use the matching
[example source/project action](https://datacoolie.github.io/datacoolie/examples/) for a complete
runner and pin its revision through `references/public-examples.md`.

Keep direct source paths and query-capable portions native even when one narrow registration step
is needed. Use the narrowest verified physical root and the public registration options; never add
an unbounded discovery scan merely to avoid selecting a project-owned root.

## Outcome-scoped verification

Apply [Build's outcome checks](../SKILL.md#build-and-local-verification). For requested runtime use, verify registered names and reconcile output schema/rows in the exact runner process; report ambiguous names.


When the framework or public example changes, recheck the public qualified-SQL page, installed
capability evidence, the same-process mapping, and the exact retrieved source before changing a
runner. Do not silently replace a qualified query with bespoke I/O.
