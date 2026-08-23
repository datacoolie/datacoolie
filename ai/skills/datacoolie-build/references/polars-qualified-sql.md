# Polars Qualified SQL Registration

## Scope

- Read when a Polars runner executes Delta or Iceberg metadata `source.query` against one or more
  lakehouse tables.
- Owns the source-native discovery, logical SQL naming, same-process registration, dependency, and
  verification contract for indexed Polars SQL relations.
- Does not change metadata fields, decide whether a query is needed, define generic runner
  parameters, or apply to database SQL. Route those concerns to `schema-quick-reference.md`,
  `framework-boundary.md`, and `runner-contract.md`.

## Decision boundary

Use this feature only when all of these are true:

1. The selected engine is `PolarsEngine`.
2. The source is Delta or Iceberg.
3. A normal metadata `source.query` must resolve lakehouse table relations through Polars SQL.

Keep direct whole-table reads on the normal `source.table`/path route. Do not add relation
registration or SQLGlot to a Polars DataFrame/LazyFrame workflow that does not need indexed SQL.
Database `source.query` is sent to the source database and is not this feature.

## Metadata and runner ownership

The SQL belongs in canonical metadata. Registration belongs in project-owned bootstrap code that
uses the same active engine as the driver:

```json
{
  "source": {
    "connection_name": "delta_lake",
    "query": "SELECT * FROM catalog_A.database_B.schema_C.orders"
  }
}
```

Do not put `logical_prefix`, `recursive`, `max_depth`, `include`, `exclude`, `max_tables`,
`preload`, registration case IDs, or expected names in `source.configure`. Do not wrap registration
in a Python-function source. A setup subprocess cannot register tables into the driver's
process-local `SQLContext`.

Use this order in the selected runner:

```text
platform/catalog bootstrap
  -> construct PolarsEngine
  -> register Delta/Iceberg relation descriptors on that engine
  -> construct DataCoolieDriver with the same engine
  -> driver.run(stage=stage)
```

Keep physical roots, catalog properties, and namespace mappings in the fixed project/environment
bootstrap selected by the runner. Resolve credentials through the platform or secret mechanism;
do not invent generic metadata or runtime-selector fields.

## Delta path discovery

Delta registration starts from a physical path. Relative table-directory components below that
root are appended to `logical_prefix`. Recursive traversal stops when `_delta_log` identifies a
table, so it does not descend into its data or partition folders.

```python
platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
engine.register_delta_tables(
    "/lake/database_B",
    logical_prefix=("catalog_A", "database_B"),
    recursive=True,
    include="database_B.**.d_*",
    exclude=("**.tmp.**", "**.*_backup"),
    max_tables=1_000,
)
```

If `/lake/database_B/schema_C/d_orders` is a Delta root, its canonical logical name is
`catalog_A.database_B.schema_C.d_orders`. Prefer the narrowest useful physical root before adding
filters; discovery must enumerate the selected root before logical patterns can select results.

## Iceberg catalog and path discovery

Catalog mode is the primary Iceberg route. Attach a configured PyIceberg catalog to the engine,
then use `namespace` to bound enumeration. With `logical_prefix=None` (the default), the catalog
name and selected root namespace are preserved in the canonical name.

```python
catalog = load_catalog("catalog_A", **catalog_properties)
engine = PolarsEngine(platform=platform, iceberg_catalog=catalog)
engine.register_iceberg_tables(
    namespace=("database_B",),
    recursive=True,
    include="database_B.**.d_*",
    max_tables=1_000,
)
```

For catalog table `database_B.schema_C.d_orders`, the default canonical logical name is
`catalog_A.database_B.schema_C.d_orders`. A supplied `logical_prefix` replaces the catalog/root
namespace mapping; relative child namespaces and the table are then appended. Use
`logical_prefix=()` only when the canonical name should contain solely the identifier below the
selected namespace.

Iceberg path mode is available with `base_path=...` when no catalog is attached. It is mutually
exclusive with `namespace` and follows path discovery using the Iceberg `metadata` marker. Use it
only when the project's verified source is path-addressed.

## Logical names and filters

Every indexed table has one canonical logical name with one to four non-empty components. There is
no configurable separator and no `max_sql_name_levels`. A query may omit only leading components;
the resulting suffix must be unique:

| Canonical name | Valid SQL forms when unique |
|---|---|
| `catalog.database.schema.table` | 4, 3, 2, or 1 trailing components |
| `database.schema.table` | 3, 2, or 1 trailing components |
| `schema.table` | 2 or 1 trailing components |
| `table` | 1 component |

If a suffix matches more than one canonical name, execution raises an ambiguity error with the
candidates. Qualify the SQL reference further; never pick a candidate implicitly.

`include` and `exclude` accept one component glob or a sequence. They match canonical names and
their suffixes case-insensitively. `*` stays inside one component; `**` crosses zero or more whole
components. Any matching exclude wins.

| Intent | Pattern |
|---|---|
| One catalog/database subtree | `catalog_A.database_B.**` |
| Database B under any catalog | `database_B.**` |
| `d_` tables below database B | `database_B.**.d_*` |
| `d_` tables anywhere | `d_*` |
| Remove temporary namespaces/tables | `**.tmp.**`, `**.*_tmp` |

Use `max_depth` to bound traversal relative to the physical root and `max_tables` as a fail-fast
safety ceiling. These do not control how many SQL name components are accepted.

## Lazy binding, reuse, and errors

The default `preload=False` only enumerates and indexes descriptors. It does not create scans or
bind tables to `SQLContext`. `PolarsEngine.execute_sql` resolves the relations needed by a query,
creates their `LazyFrame`s, binds private aliases once, and reuses those bindings for later queries
on the same engine. Collection remains lazy until a downstream boundary needs data.

Keep `preload=False` for metadata-driven execution through `DataCoolieDriver`. Use `preload=True`
only when a verified caller must bypass `execute_sql` and call `engine.sql_context.execute`
directly. Default `on_error="raise"` is fail-fast. When best-effort discovery is explicitly
required, use `on_error="skip"` and inspect `engine.last_registration_report`; never hide partial
registration.

Registration methods return sorted accepted logical names. Check `engine.registered_tables()` in
bootstrap tests. Re-registering the same logical name and source is idempotent; a conflicting source
or ambiguous query is an error.

## Dependencies

Install only the selected source path:

```text
Delta qualified SQL:   datacoolie[polars-sql,polars-delta]
Iceberg qualified SQL: datacoolie[polars-sql,polars-iceberg]
Both:                  datacoolie[polars-sql,polars-delta,polars-iceberg]
```

`polars-sql` provides the optional SQLGlot resolver. Do not add it to projects that use only normal
Polars DataFrame/LazyFrame APIs, direct table reads, or one-part frames registered with
`register_table`.

## Verification

- Parse and validate the normal metadata; every qualified-SQL source uses `source.query` and its
  Delta/Iceberg connection.
- Unit-test each bootstrap mapping against the exact sorted names returned by registration.
- Test at least one full canonical reference and every shorter suffix form the project intends to
  expose.
- Add a collision fixture and assert that an intentionally short reference fails with all
  candidates rather than selecting one.
- Run at least two queries through one engine when reuse matters; do not use separate processes as
  proof of cached registration.
- Execute the exact generated runner and metadata through `DataCoolieDriver`, then reconcile output
  schema, row count, and measures.

## Unresolved Questions

- None. Project-specific physical roots, catalog properties, namespaces, and intended public SQL
  names must come from the approved project evidence and environment binding.
